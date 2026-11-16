import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

from migration import (
    CheckpointError,
    FieldMappingError,
    MigrationError,
    MigrationExecutor,
    PartitionConflictError,
    ValidationError,
    WriteWatermark,
)
from migration import executor as executor_module
from migration.layout import read_jsonl, write_jsonl
from migration.state import MigrationState, VALIDATED


def make_record(record_id, biz_date, region="cn", amount=10):
    return {
        "id": record_id,
        "biz_date": biz_date,
        "region": region,
        "amount": amount,
    }


FIELD_MAPPING = {
    "id": "id",
    "biz_date": "biz_date",
    "region": "region",
    "amount": "amount",
}


def partition_fn(record):
    return {
        "biz_date": record["biz_date"][:10],
        "region": record["region"],
    }


def write_legacy(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def append_legacy(path, records):
    with open(path, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def partition_relpath(record):
    return os.path.join(
        f"biz_date={record['biz_date'][:10]}",
        f"region={record['region']}",
    )


class MigrationTestBase(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix="migration-")
        self.addCleanup(shutil.rmtree, self.workdir)
        self.legacy_root = os.path.join(self.workdir, "legacy")
        self.target_root = os.path.join(self.workdir, "lake")
        self.legacy_file = os.path.join(self.legacy_root, "2024", "01", "data.jsonl")
        self.watermark = WriteWatermark(
            datetime(2024, 1, 5, tzinfo=timezone.utc)
        )

    def make_executor(self, watermark=None, field_mapping=None, batch_size=1):
        return MigrationExecutor(
            old_path_mapping={self.legacy_root: "src"},
            target_root=self.target_root,
            partition_fn=partition_fn,
            field_mapping=field_mapping or FIELD_MAPPING,
            write_watermark=watermark or self.watermark,
            event_time_field="biz_date",
            primary_key="id",
            batch_size=batch_size,
        )

    def target_files(self):
        result = []
        for dirpath, _dirnames, filenames in os.walk(self.target_root):
            for filename in filenames:
                if filename.endswith(".jsonl"):
                    result.append(os.path.join(dirpath, filename))
        return sorted(result)


class IncrementalArrivalTest(MigrationTestBase):
    def test_catches_up_new_arrivals_without_overwriting_verified_data(self):
        records = [
            make_record("r1", "2024-01-01T00:00:00Z"),
            make_record("r2", "2024-01-02T00:00:00Z", region="us"),
        ]
        write_legacy(self.legacy_file, records)

        executor = self.make_executor()
        state = executor.run()
        self.assertEqual(len(self.target_files()), 2)
        self.assertEqual(
            sorted(os.path.relpath(os.path.dirname(path), self.target_root)
                   for path in self.target_files()),
            sorted(partition_relpath(record) for record in records),
        )
        self.assertTrue(
            all(batch.status == VALIDATED for batch in state.batches.values())
        )
        report = executor.dual_read_check()
        self.assertTrue(report.ok)
        self.assertEqual(report.source_records, 2)
        self.assertEqual(report.target_records, 2)

        mtimes = {path: os.stat(path).st_mtime_ns for path in self.target_files()}

        # Legacy writer keeps producing: r3 is stable, r4 is beyond the
        # watermark and must stay behind for the next round.
        append_legacy(self.legacy_file, [
            make_record("r3", "2024-01-03T00:00:00Z"),
            make_record("r4", "2024-01-10T00:00:00Z"),
        ])
        executor = self.make_executor()
        executor.run()
        self.assertEqual(len(self.target_files()), 3)
        for path, mtime in mtimes.items():
            self.assertEqual(os.stat(path).st_mtime_ns, mtime,
                             "validated target file was rewritten")

        report = executor.dual_read_check()
        self.assertEqual(report.source_records, 3)
        self.assertEqual(report.target_records, 3)

        # A deterministic re-run is a no-op and still does not touch files.
        self.make_executor().run()
        for path, mtime in mtimes.items():
            self.assertEqual(os.stat(path).st_mtime_ns, mtime)

        # Advance the watermark and catch the previously blocked record.
        later = WriteWatermark(datetime(2024, 1, 12, tzinfo=timezone.utc))
        executor = self.make_executor(watermark=later)
        executor.run()
        report = executor.dual_read_check()
        self.assertTrue(report.ok)
        self.assertEqual(report.source_records, 4)
        self.assertEqual(report.target_records, 4)

    def test_checkpoint_resume_skips_migrated_offsets(self):
        write_legacy(self.legacy_file, [
            make_record("r1", "2024-01-01T00:00:00Z"),
        ])
        self.make_executor().run()
        reloaded = MigrationState.load(self.target_root)
        self.assertEqual(reloaded.offsets[self.legacy_file],
                         os.path.getsize(self.legacy_file))

    def test_pending_records_drives_catch_up_loop(self):
        write_legacy(self.legacy_file, [
            make_record("r1", "2024-01-01T00:00:00Z"),
        ])
        executor = self.make_executor()
        self.assertEqual(executor.pending_records(), 1)
        executor.run()
        self.assertEqual(executor.pending_records(), 0)
        self.assertEqual(len(self.target_files()), 1)

        # A record beyond the watermark is blocked and stays pending until the
        # watermark moves; it must not poison the checkpoint offset.
        append_legacy(self.legacy_file, [
            make_record("r4", "2024-01-10T00:00:00Z"),
        ])
        self.assertEqual(executor.pending_records(), 0)  # r4 beyond watermark

        append_legacy(self.legacy_file, [
            make_record("r2", "2024-01-02T00:00:00Z"),
        ])
        # r2 sits behind the blocked r4 and is itself within the watermark,
        # but the blocked prefix means it cannot be claimed in this round.
        self.assertEqual(executor.pending_records(), 0)

        later = WriteWatermark(datetime(2024, 1, 12, tzinfo=timezone.utc))
        executor = self.make_executor(watermark=later)
        self.assertEqual(executor.pending_records(), 2)
        executor.run()
        self.assertEqual(executor.pending_records(), 0)


class PartitionConflictTest(MigrationTestBase):
    def test_foreign_file_at_target_path_blocks_the_batch(self):
        records = [make_record("r1", "2024-01-01T00:00:00Z")]
        write_legacy(self.legacy_file, records)
        foreign_path = os.path.join(
            self.target_root, partition_relpath(records[0]),
            "foreign-owner.jsonl",
        )
        write_jsonl(foreign_path, [{"id": "foreign-owner"}])

        with self.assertRaises(PartitionConflictError):
            self.make_executor().run()

        self.assertEqual(
            read_jsonl(foreign_path),
            [{"id": "foreign-owner"}],
        )
        reloaded = MigrationState.load(self.target_root)
        self.assertEqual(reloaded.offsets, {})

        # Once the foreign owner moves away, the same batch can run.
        quarantine = os.path.join(self.workdir, "quarantine.jsonl")
        shutil.move(foreign_path, quarantine)
        executor = self.make_executor()
        executor.run()
        self.assertEqual(executor.dual_read_check().target_records, 1)

    def test_stale_file_after_state_loss_is_reported_by_dual_read(self):
        records = [make_record("r1", "2024-01-01T00:00:00Z")]
        write_legacy(self.legacy_file, records)
        self.make_executor().run()

        # Simulate losing the checkpoint while target data remains.
        os.unlink(os.path.join(self.target_root, "migration-state.json"))
        append_legacy(self.legacy_file, [
            make_record("r2", "2024-01-02T00:00:00Z"),
        ])
        with self.assertRaises(CheckpointError):
            self.make_executor()

        # Manual reconciliation: move the orphan aside, then resume safely.
        orphan = self.target_files()[0]
        quarantine = os.path.join(self.workdir, "quarantine.jsonl")
        shutil.move(orphan, quarantine)
        executor = self.make_executor()
        executor.run()
        self.assertTrue(executor.dual_read_check().ok)
        self.assertTrue(os.path.exists(quarantine))


class FieldMappingTest(MigrationTestBase):
    def test_missing_source_field_aborts_and_keeps_target_empty(self):
        write_legacy(self.legacy_file, [
            make_record("r1", "2024-01-01T00:00:00Z"),
        ])
        bad_mapping = dict(FIELD_MAPPING, amount="does_not_exist")
        with self.assertRaises(FieldMappingError):
            self.make_executor(field_mapping=bad_mapping).run()
        self.assertEqual(self.target_files(), [])

        self.make_executor().run()
        self.assertEqual(len(self.target_files()), 1)

    def test_callable_exception_is_normalized(self):
        write_legacy(self.legacy_file, [
            make_record("r1", "2024-01-01T00:00:00Z"),
        ])

        def explode(record):
            raise ValueError("boom")

        with self.assertRaises(FieldMappingError):
            self.make_executor(field_mapping=explode).run()


class ValidationFailureTest(MigrationTestBase):
    def test_corrupted_write_rolls_back_and_recovers_from_checkpoint(self):
        write_legacy(self.legacy_file, [
            make_record("r1", "2024-01-01T00:00:00Z"),
        ])
        real_write = executor_module.write_jsonl

        def corrupt_write(path, records):
            real_write(path, [
                {key: value for key, value in record.items() if key != "amount"}
                for record in records
            ])

        with mock.patch.object(executor_module, "write_jsonl",
                               side_effect=corrupt_write):
            with self.assertRaises(ValidationError):
                self.make_executor().run()

        self.assertEqual(self.target_files(), [])
        # The persisted checkpoint still records the interrupted write; a new
        # executor reconciles it before doing anything else.
        stale = MigrationState.load(self.target_root)
        self.assertTrue(any(batch.status == "written"
                            for batch in stale.batches.values()))

        executor = self.make_executor()
        executor.run()
        self.assertEqual(executor.dual_read_check().target_records, 1)


class MidwayRollbackTest(MigrationTestBase):
    def test_partition_not_ready_until_every_source_batch_is_validated(self):
        records = [
            make_record("r1", "2024-01-01T00:00:00Z"),
            make_record("r2", "2024-01-01T12:00:00Z"),
            make_record("r3", "2024-01-02T00:00:00Z"),
        ]
        write_legacy(self.legacy_file, records)

        # The first batch validates; simulate the process being killed after
        # the second batch's write but before its dual-read check.
        executor = self.make_executor(
            field_mapping=lambda record: dict(record)
        )
        from migration.layout import LegacyFile
        legacy_file = LegacyFile("src", self.legacy_root, self.legacy_file)

        first = [records[0]]
        first_id = executor_module._make_batch_id(
            "src", self.legacy_file, 0,
            len(json.dumps(records[0], ensure_ascii=False)) + 1,
            (json.dumps(records[0], ensure_ascii=False) + "\n").encode(),
        )
        executor._migrate_batch(
            legacy_file, first_id, first, 0,
            len(json.dumps(records[0], ensure_ascii=False)) + 1,
        )

        second_line = json.dumps(records[1], ensure_ascii=False) + "\n"
        first_line_len = len(json.dumps(records[0], ensure_ascii=False)) + 1
        second_id = executor_module._make_batch_id(
            "src", self.legacy_file, first_line_len,
            first_line_len + len(second_line), second_line.encode(),
        )
        real_write = executor_module.write_jsonl

        class StopMigration(Exception):
            pass

        with mock.patch.object(executor_module, "write_jsonl") as patched:
            def stop_after_write(path, batch_records):
                real_write(path, batch_records)
                raise StopMigration("process killed after write")
            patched.side_effect = stop_after_write
            with self.assertRaises(StopMigration):
                executor._migrate_batch(
                    legacy_file, second_id, [records[1]],
                    first_line_len, first_line_len + len(second_line),
                )

        state = MigrationState.load(self.target_root)
        relpath = partition_relpath(records[0])
        partition = state.partitions[relpath]
        self.assertFalse(partition.ready,
                         "partition still has an unfinished source batch")
        self.assertEqual(len(partition.validated_batches), 1)
        self.assertEqual(len(partition.pending_batches), 1)
        on_disk = self.target_files()
        self.assertEqual(len(on_disk), 2)
        validated_id = partition.validated_batches[0]
        first_file = next(
            path for path in on_disk
            if validated_id in os.path.basename(path)
        )
        surviving_mtime = os.stat(first_file).st_mtime_ns

        # A new process reconciles the interrupted write and resumes.
        executor = self.make_executor(field_mapping=lambda record: dict(record))
        executor.run()
        self.assertEqual(os.stat(first_file).st_mtime_ns, surviving_mtime)
        report = executor.dual_read_check()
        self.assertTrue(report.ok)
        self.assertEqual(report.target_records, 3)
        reloaded = MigrationState.load(self.target_root)
        self.assertTrue(all(state_partition.ready
                            for state_partition in reloaded.partitions.values()))

    def test_failed_validation_rolls_back_and_resumes(self):
        records = [
            make_record("r1", "2024-01-01T00:00:00Z"),
            make_record("r2", "2024-01-01T12:00:00Z"),
            make_record("r3", "2024-01-02T00:00:00Z"),
        ]
        write_legacy(self.legacy_file, records)
        real_write = executor_module.write_jsonl
        attempts = {"count": 0}

        def fail_second_batch(path, batch_records):
            if "r2" in [record["id"] for record in batch_records]:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    real_write(path, [
                        {key: value for key, value in record.items()
                         if key != "amount"}
                        for record in batch_records
                    ])
                    return
            real_write(path, batch_records)

        with self.assertRaises(ValidationError):
            self._run_with_writer(fail_second_batch)

        self.assertEqual(len(self.target_files()), 1)
        surviving = self.target_files()[0]
        surviving_mtime = os.stat(surviving).st_mtime_ns
        executor = self.make_executor(field_mapping=lambda record: dict(record))
        executor.run()
        self.assertEqual(os.stat(surviving).st_mtime_ns, surviving_mtime)
        report = executor.dual_read_check()
        self.assertTrue(report.ok)
        self.assertEqual(report.target_records, 3)

    def _run_with_writer(self, writer):
        with mock.patch.object(executor_module, "write_jsonl",
                               side_effect=writer):
            self.make_executor(field_mapping=lambda record: dict(record)).run()


class FullRollbackTest(MigrationTestBase):
    def test_rollback_removes_owned_data_but_keeps_foreign_files(self):
        write_legacy(self.legacy_file, [
            make_record("r1", "2024-01-01T00:00:00Z"),
        ])
        executor = self.make_executor()
        executor.run()

        foreign = os.path.join(self.target_root, "manual", "foreign-owner.jsonl")
        write_jsonl(foreign, [{"id": "foreign"}])

        with self.assertRaises(MigrationError):
            executor.rollback_migration()

        kept = executor.rollback_migration(force=True)

        self.assertEqual(kept, [foreign])
        self.assertTrue(os.path.exists(foreign))
        owned = [path for path in self.target_files()
                 if os.path.basename(path).startswith("batch-")]
        self.assertEqual(owned, [])
        reloaded = MigrationState.load(self.target_root)
        self.assertEqual(reloaded.offsets, {})
        self.assertEqual(reloaded.batches, {})

        # The lake can be rebuilt from scratch after the rollback.
        executor = self.make_executor()
        executor.run()
        self.assertTrue(executor.dual_read_check().ok)


if __name__ == "__main__":
    unittest.main()
