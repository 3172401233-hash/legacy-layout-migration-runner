"""Layout migration executor with incremental catch-up and dual-read checks."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Union

from .errors import (
    CheckpointError,
    FieldMappingError,
    MigrationError,
    PartitionConflictError,
    ValidationError,
)
from .layout import (
    LegacyFile,
    PartitionFn,
    Record,
    Transform,
    as_transform,
    batch_filename,
    batch_id_from_filename,
    partition_relpath,
    read_jsonl,
    resolve_partition,
    scan_legacy_files,
    write_jsonl,
)
from .state import (
    BatchState,
    MigrationState,
    ROLLED_BACK,
    VALIDATED,
    WRITTEN,
)
from .watermark import WriteWatermark, parse_event_time


def _make_batch_id(source_name: str, source_path: str,
                   start_offset: int, end_offset: int, raw_chunk: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(source_name.encode("utf-8"))
    digest.update(b"\0")
    digest.update(os.path.abspath(source_path).encode("utf-8"))
    digest.update(b"\0")
    digest.update(str(start_offset).encode("ascii"))
    digest.update(b":")
    digest.update(str(end_offset).encode("ascii"))
    digest.update(b"\0")
    digest.update(raw_chunk)
    return digest.hexdigest()[:16]


def _index_records(records: List[Record], primary_key: str, scope: str
                   ) -> Dict[str, str]:
    indexed: Dict[str, str] = {}
    for record in records:
        if primary_key not in record:
            raise ValidationError(
                f"{scope}: record missing primary key {primary_key!r}"
            )
        key = str(record[primary_key])
        if key in indexed:
            raise ValidationError(f"{scope}: duplicate primary key {key!r}")
        indexed[key] = json.dumps(record, sort_keys=True,
                                  ensure_ascii=False, separators=(",", ":"))
    return indexed


@dataclass
class DualReadReport:
    """Outcome of a whole-lake dual-read comparison."""

    source_records: int
    target_records: int
    missing_keys: List[str]
    mismatched_keys: List[str]
    extra_keys: List[str]

    @property
    def ok(self) -> bool:
        return not (self.missing_keys or self.mismatched_keys or self.extra_keys)


class MigrationExecutor:
    """Migrate year/month legacy buckets into date/region lake partitions.

    Parameters
    ----------
    old_path_mapping:
        Legacy root directory, sequence of roots, or ``{root: source_name}``.
    target_root:
        Root of the new data lake.
    partition_fn:
    field_mapping:
    write_watermark:
    event_time_field:
        Field read on the transformed record to evaluate the watermark.
    primary_key:
        Field used for dual-read record identity.
    batch_size:
        Maximum number of legacy records per batch / target file.
    state_dir:
        Directory holding ``migration-state.json``; defaults to ``target_root``.
    """

    def __init__(
        self,
        old_path_mapping: Union[str, Sequence[str], Mapping[str, str]],
        target_root: str,
        partition_fn: PartitionFn,
        field_mapping: Union[Mapping[str, str], Transform],
        write_watermark: WriteWatermark,
        event_time_field: str = "biz_date",
        primary_key: str = "id",
        batch_size: int = 1000,
        state_dir: Optional[str] = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self.old_path_mapping = old_path_mapping
        self.target_root = os.path.abspath(target_root)
        self.partition_fn = partition_fn
        self.transform = as_transform(field_mapping)
        self.write_watermark = write_watermark
        self.event_time_field = event_time_field
        self.primary_key = primary_key
        self.batch_size = batch_size
        self.state = MigrationState.load(state_dir or self.target_root)
        self.state.watermark = write_watermark.to_dict()
        self.recover_interrupted()

    # ------------------------------------------------------------------
    # Checkpoint recovery
    # ------------------------------------------------------------------
    def recover_interrupted(self) -> None:
        """Clean temp files and reconcile batches left in ``written`` state."""
        owned_files = set()
        for batch in self.state.batches.values():
            owned_files.update(os.path.abspath(path) for path in batch.target_files)
        if os.path.isdir(self.target_root):
            for dirpath, _dirnames, filenames in os.walk(self.target_root):
                for filename in filenames:
                    path = os.path.join(dirpath, filename)
                    if filename.startswith(".") and filename.endswith(".tmp"):
                        os.unlink(os.path.join(dirpath, filename))
                        continue
                    if batch_id_from_filename(filename) and \
                            os.path.abspath(path) not in owned_files:
                        raise CheckpointError(
                            f"orphan migration file {path} is absent from the "
                            "checkpoint; move it aside or rebuild the state "
                            "before resuming"
                        )
        for batch in self.state.batches.values():
            if batch.status == WRITTEN:
                self._rollback_batch(batch)

    # ------------------------------------------------------------------
    # Main migration loop
    # ------------------------------------------------------------------
    def run(self) -> MigrationState:
        """Run one catch-up round over every legacy file."""
        for legacy_file in scan_legacy_files(self.old_path_mapping):
            self._migrate_file(legacy_file)
        self.state.save()
        return self.state

    def _migrate_file(self, legacy_file: LegacyFile) -> None:
        if not os.path.exists(legacy_file.path):
            return
        offset = self.state.offset_for(legacy_file.path)
        chunk_records: List[Record] = []
        chunk_raw = bytearray()
        start_offset = offset

        def flush(end_offset: int) -> None:
            nonlocal chunk_records, chunk_raw, start_offset
            if not chunk_records:
                start_offset = end_offset
                return
            batch_id = _make_batch_id(
                legacy_file.source_name, legacy_file.path,
                start_offset, end_offset, bytes(chunk_raw),
            )
            self._migrate_batch(
                legacy_file, batch_id, list(chunk_records),
                start_offset, end_offset,
            )
            chunk_records = []
            chunk_raw = bytearray()
            start_offset = end_offset

        with open(legacy_file.path, "rb") as handle:
            handle.seek(offset)
            for raw_line in handle:
                if not raw_line.strip():
                    offset += len(raw_line)
                    continue
                record = json.loads(raw_line.decode("utf-8"))
                event_time = record.get(self.event_time_field)
                if event_time is None:
                    raise FieldMappingError(
                        f"record in {legacy_file.path} has no field "
                        f"{self.event_time_field!r}"
                    )
                if not self.write_watermark.allows(parse_event_time(str(event_time))):
                    break
                chunk_records.append(record)
                chunk_raw.extend(raw_line)
                offset += len(raw_line)
                if len(chunk_records) >= self.batch_size:
                    flush(offset)
            flush(offset)

    def _migrate_batch(self, legacy_file: LegacyFile, batch_id: str,
                       source_records: List[Record],
                       start_offset: int, end_offset: int) -> None:
        existing = self.state.get_batch(batch_id)
        if existing is not None and existing.status == VALIDATED:
            # Deterministic re-run: verified data must never be overwritten.
            self.state.advance_offset(legacy_file.path, end_offset)
            return

        batch = BatchState(
            batch_id=batch_id,
            source_name=legacy_file.source_name,
            source_path=legacy_file.path,
            start_offset=start_offset,
            end_offset=end_offset,
            record_count=len(source_records),
            primary_key=self.primary_key,
        )
        snapshot = self.state.snapshot()
        self.state.upsert_batch(batch)
        try:
            transformed = [self.transform(record) for record in source_records]
            grouped: Dict[str, List[Record]] = {}
            for record in transformed:
                if self.primary_key not in record:
                    raise FieldMappingError(
                        f"transformed record missing primary key {self.primary_key!r}"
                    )
                partition = resolve_partition(self.partition_fn, record)
                relpath = partition_relpath(partition)
                grouped.setdefault(relpath, []).append(record)

            target_paths: List[str] = []
            batch.partition_keys = sorted(grouped)
            for relpath, records in grouped.items():
                target_path = os.path.join(
                    self.target_root, relpath, batch_filename(batch_id)
                )
                self._ensure_no_foreign_file(target_path, batch_id)
                target_paths.append(target_path)
                self.state.touch_partition(relpath, batch_id)
                self.state.save()

            batch.target_files = target_paths
            batch.status = WRITTEN
            self.state.save()
            try:
                for relpath, target_path in zip(batch.partition_keys, target_paths):
                    write_jsonl(target_path, grouped[relpath])

                self._dual_read_batch(batch, transformed)
            except Exception:
                # Unexpected crash: persist the in-flight checkpoint so a new
                # process can reconcile the batch and keep the partition
                # unready instead of silently promoting it.
                self.state.save()
                raise
            self.state.mark_batch_validated(batch)
            self.state.advance_offset(legacy_file.path, end_offset)
            self.state.save()
        except MigrationError:
            self.state.restore(snapshot)
            self._rollback_batch(batch)
            raise

    def _ensure_no_foreign_file(self, target_path: str, batch_id: str) -> None:
        """Reject files in the target partition this migration does not own."""
        partition_dir = os.path.dirname(target_path)
        if not os.path.isdir(partition_dir):
            return
        for filename in os.listdir(partition_dir):
            path = os.path.join(partition_dir, filename)
            if not os.path.isfile(path):
                continue
            if filename.endswith(".tmp"):
                continue
            owner = batch_id_from_filename(filename)
            if owner and (owner == batch_id or owner in self.state.batches):
                continue
            raise PartitionConflictError(
                f"refusing to write into partition {partition_dir}: foreign "
                f"file {path} is owned by another writer or earlier migration"
            )

    # ------------------------------------------------------------------
    # Dual-read validation
    # ------------------------------------------------------------------
    def _dual_read_batch(self, batch: BatchState, expected: List[Record]) -> None:
        actual: List[Record] = []
        for target_path in batch.target_files:
            actual.extend(read_jsonl(target_path))
        self._compare_records(expected, actual, f"batch {batch.batch_id}")

    def _compare_records(self, expected: List[Record], actual: List[Record],
                         scope: str) -> None:
        expected_index = _index_records(expected, self.primary_key, scope)
        actual_index = _index_records(actual, self.primary_key, scope)
        missing = sorted(set(expected_index) - set(actual_index))
        extra = sorted(set(actual_index) - set(expected_index))
        mismatched = sorted(
            key for key in expected_index.keys() & actual_index.keys()
            if expected_index[key] != actual_index[key]
        )
        if missing or extra or mismatched:
            raise ValidationError(
                f"{scope} dual-read failed: missing={missing[:5]} "
                f"extra={extra[:5]} mismatched={mismatched[:5]}"
            )

    def dual_read_check(self) -> DualReadReport:
        """Compare every eligible legacy record with every validated target."""
        expected: List[Record] = []
        for legacy_file in scan_legacy_files(self.old_path_mapping):
            for record in read_jsonl(legacy_file.path):
                event_time = record.get(self.event_time_field)
                if event_time is None:
                    raise FieldMappingError(
                        f"record in {legacy_file.path} has no field "
                        f"{self.event_time_field!r}"
                    )
                if self.write_watermark.allows(parse_event_time(str(event_time))):
                    expected.append(self.transform(record))

        actual: List[Record] = []
        for batch in self.state.batches.values():
            if batch.status != VALIDATED:
                continue
            for target_path in batch.target_files:
                actual.extend(read_jsonl(target_path))

        expected_index = _index_records(expected, self.primary_key, "lake dual-read")
        actual_index = _index_records(actual, self.primary_key, "lake dual-read")
        missing = sorted(set(expected_index) - set(actual_index))
        extra = sorted(set(actual_index) - set(expected_index))
        mismatched = sorted(
            key for key in expected_index.keys() & actual_index.keys()
            if expected_index[key] != actual_index[key]
        )
        report = DualReadReport(
            source_records=len(expected),
            target_records=len(actual),
            missing_keys=missing,
            mismatched_keys=mismatched,
            extra_keys=extra,
        )
        if not report.ok:
            raise ValidationError(
                f"lake dual-read failed: missing={missing[:5]} extra={extra[:5]} "
                f"mismatched={mismatched[:5]}"
            )
        return report

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------
    def _rollback_batch(self, batch: BatchState) -> None:
        for target_path in batch.target_files:
            if os.path.exists(target_path):
                os.unlink(target_path)
            parent = os.path.dirname(target_path)
            if os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
        batch.target_files = []
        batch.status = ROLLED_BACK

    def rollback_migration(self, *, force: bool = False) -> List[str]:
        """Remove every migration-owned target file and reset the checkpoint.

        Foreign files are left untouched. ``force`` is required because the
        operation also removes validated data; validated data is never
        overwritten by a normal re-run.
        """
        if not force:
            raise MigrationError("pass force=True to roll back validated data")
        foreign: List[str] = []
        if os.path.isdir(self.target_root):
            for dirpath, _dirnames, filenames in os.walk(self.target_root):
                for filename in filenames:
                    path = os.path.join(dirpath, filename)
                    if os.path.abspath(path) == os.path.abspath(self.state.path):
                        continue
                    if not batch_id_from_filename(filename):
                        foreign.append(path)
                        continue
                    os.unlink(path)
        for dirpath, _dirnames, _filenames in os.walk(self.target_root, topdown=False):
            if os.path.isdir(dirpath) and not os.listdir(dirpath) and \
                    os.path.abspath(dirpath) != os.path.abspath(self.target_root):
                os.rmdir(dirpath)
        self.state.batches.clear()
        self.state.partitions.clear()
        self.state.offsets.clear()
        self.state.save()
        return sorted(foreign)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def ready_partitions(self) -> List[str]:
        return sorted(relpath for relpath, partition in self.state.partitions.items()
                      if partition.ready)

    def pending_records(self) -> int:
        """Count eligible legacy records still behind the checkpoint offset."""
        pending = 0
        for legacy_file in scan_legacy_files(self.old_path_mapping):
            if not os.path.exists(legacy_file.path):
                continue
            with open(legacy_file.path, "rb") as handle:
                handle.seek(self.state.offset_for(legacy_file.path))
                for raw_line in handle:
                    if not raw_line.strip():
                        continue
                    record = json.loads(raw_line.decode("utf-8"))
                    event_time = record.get(self.event_time_field)
                    if event_time is None:
                        raise FieldMappingError(
                            f"record in {legacy_file.path} has no field "
                            f"{self.event_time_field!r}"
                        )
                    if self.write_watermark.allows(
                            parse_event_time(str(event_time))):
                        pending += 1
                    else:
                        break
        return pending
