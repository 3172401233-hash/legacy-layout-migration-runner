"""Persistent migration state with checkpoint and rollback support."""

from __future__ import annotations

import copy
import json
import os
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

from .errors import CheckpointError

STATE_FILENAME = "migration-state.json"

# Batch lifecycle.
PENDING = "pending"
WRITTEN = "written"
VALIDATED = "validated"
FAILED = "failed"
ROLLED_BACK = "rolled_back"


@dataclass
class BatchState:
    batch_id: str
    source_name: str
    source_path: str
    start_offset: int
    end_offset: int
    record_count: int
    primary_key: str
    status: str = PENDING
    partition_keys: List[str] = field(default_factory=list)
    target_files: List[str] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def finished(self) -> bool:
        return self.status == VALIDATED


@dataclass
class PartitionState:
    relpath: str
    ready: bool = True
    validated_batches: List[str] = field(default_factory=list)
    pending_batches: List[str] = field(default_factory=list)


class MigrationState:
    """In-memory state mirror of ``migration-state.json``."""

    def __init__(self, state_dir: str):
        self.state_dir = os.path.abspath(state_dir)
        self.path = os.path.join(self.state_dir, STATE_FILENAME)
        self.batches: Dict[str, BatchState] = {}
        self.partitions: Dict[str, PartitionState] = {}
        self.offsets: Dict[str, int] = {}
        self.watermark: Optional[dict] = None

    @classmethod
    def load(cls, state_dir: str) -> "MigrationState":
        state = cls(state_dir)
        if not os.path.exists(state.path):
            return state
        try:
            with open(state.path, encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise CheckpointError(f"cannot read checkpoint {state.path}: {exc}") from exc
        state.offsets = dict(payload.get("offsets", {}))
        state.watermark = payload.get("watermark")
        for item in payload.get("batches", []):
            batch = BatchState(**item)
            state.batches[batch.batch_id] = batch
        for item in payload.get("partitions", []):
            partition = PartitionState(**item)
            state.partitions[partition.relpath] = partition
        return state

    def save(self) -> None:
        os.makedirs(self.state_dir, exist_ok=True)
        payload = {
            "version": 1,
            "watermark": self.watermark,
            "offsets": dict(sorted(self.offsets.items())),
            "batches": [asdict(batch) for batch in
                        sorted(self.batches.values(), key=lambda b: b.batch_id)],
            "partitions": [asdict(partition) for partition in
                           sorted(self.partitions.values(), key=lambda p: p.relpath)],
        }
        tmp = os.path.join(self.state_dir, f".{STATE_FILENAME}.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def get_batch(self, batch_id: str) -> Optional[BatchState]:
        return self.batches.get(batch_id)

    def upsert_batch(self, batch: BatchState) -> None:
        self.batches[batch.batch_id] = batch

    def offset_for(self, source_path: str) -> int:
        return self.offsets.get(source_path, 0)

    def advance_offset(self, source_path: str, offset: int) -> None:
        if offset > self.offsets.get(source_path, 0):
            self.offsets[source_path] = offset

    def partition(self, relpath: str) -> PartitionState:
        partition = self.partitions.get(relpath)
        if partition is None:
            partition = PartitionState(relpath=relpath)
            self.partitions[relpath] = partition
        return partition

    def touch_partition(self, relpath: str, batch_id: str) -> None:
        partition = self.partition(relpath)
        if batch_id not in partition.pending_batches and \
                batch_id not in partition.validated_batches:
            partition.pending_batches.append(batch_id)
            partition.ready = False

    def mark_batch_validated(self, batch: BatchState) -> None:
        batch.status = VALIDATED
        batch.error = None
        for relpath in batch.partition_keys:
            partition = self.partition(relpath)
            if batch.batch_id in partition.pending_batches:
                partition.pending_batches.remove(batch.batch_id)
            if batch.batch_id not in partition.validated_batches:
                partition.validated_batches.append(batch.batch_id)
            partition.ready = not partition.pending_batches

    def mark_batch_failed(self, batch: BatchState, error: str) -> None:
        batch.status = FAILED
        batch.error = error

    def snapshot(self) -> dict:
        return copy.deepcopy({
            "batches": self.batches,
            "partitions": self.partitions,
            "offsets": self.offsets,
            "watermark": self.watermark,
        })

    def restore(self, snapshot: dict) -> None:
        self.batches = copy.deepcopy(snapshot["batches"])
        self.partitions = copy.deepcopy(snapshot["partitions"])
        self.offsets = copy.deepcopy(snapshot["offsets"])
        self.watermark = copy.deepcopy(snapshot["watermark"])
