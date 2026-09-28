"""Silver compaction: the commit and recovery logic, with an in-memory storage (no Spark).

The previous in-DAG compaction read a partition and overwrote it, which deleted the data and
was then hidden by a catch-all `except`. What matters here is that no interruption can lose data.
"""
import json
import os
import unittest

import helpers  # noqa: F401  (puts processing/spark_streaming on sys.path)
import compact_silver as cs

BASE = "gs://bucket/silver"
DATASET, DAY = "vessel_positions", "2026-09-27"
PART = cs.partition_dir(BASE, DATASET, DAY)
ROOT = cs.staging_root(BASE, DATASET, DAY)


class Crash(Exception):
    pass


class FakeStorage:
    def __init__(self):
        self.files = {}

    def add(self, path, content="x"):
        self.files[path] = content

    def exists(self, p):
        return p in self.files or any(f.startswith(p.rstrip("/") + "/") for f in self.files)

    def list_files(self, d):
        return sorted((f, len(str(c))) for f, c in self.files.items() if f.rsplit("/", 1)[0] == d.rstrip("/"))

    def list_dirs(self, d):
        prefix, dirs = d.rstrip("/") + "/", set()
        for f in self.files:
            if f.startswith(prefix) and "/" in f[len(prefix):]:
                dirs.add(prefix + f[len(prefix):].split("/")[0])
        return sorted(dirs)

    def rename(self, src, dst):
        if src not in self.files:
            raise RuntimeError(f"no such file: {src}")
        self.files[dst] = self.files.pop(src)

    def delete(self, p, recursive=False):
        if p in self.files:
            del self.files[p]
        elif recursive:
            for f in [f for f in self.files if f.startswith(p.rstrip("/") + "/")]:
                del self.files[f]

    def write_text(self, p, text):
        self.files[p] = text

    def read_text(self, p):
        return self.files[p]


class CrashingStorage(FakeStorage):
    """Raises on the n-th call of one method, to simulate the process dying at that point."""

    def __init__(self, method, nth):
        super().__init__()
        self.method, self.nth, self.calls = method, nth, 0

    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if name != super().__getattribute__("method"):
            return attr

        def wrapper(*args, **kwargs):
            self.calls += 1
            if self.calls == self.nth:
                raise Crash(f"{name} call #{self.nth}")
            return attr(*args, **kwargs)
        return wrapper


def stage_run(storage, run_id="r1", n_staged=1, n_originals=6, late=True):
    """A verified run that has just written its manifest: the state right before the commit."""
    originals = [f"{PART}/part-{i}.snappy.parquet" for i in range(n_originals)]
    for path in originals:
        storage.add(path)
    run_dir = f"{ROOT}/{run_id}"
    staged = [f"{run_dir}/data/part-{i}.snappy.parquet" for i in range(n_staged)]
    for path in staged:
        storage.add(path, "compacted")
    moves = [[s, f"{PART}/compacted-{run_id}-{i:04d}.snappy.parquet"] for i, s in enumerate(staged)]
    manifest = {"run_id": run_id, "dataset": DATASET, "day": DAY, "originals": originals, "moves": moves}
    storage.add(f"{run_dir}/manifest.json", json.dumps(manifest))
    if late:
        storage.add(f"{PART}/part-late.snappy.parquet", "late")   # arrived after the snapshot
    return manifest, run_dir


class HelpersTest(unittest.TestCase):
    def test_is_data_file(self):
        self.assertTrue(cs.is_data_file("gs://b/silver/x/part-0000.snappy.parquet"))
        for name in ("_SUCCESS", ".part-0.parquet.crc", "part-0.parquet.crc", "_hidden.parquet", "notes.txt"):
            with self.subTest(name=name):
                self.assertFalse(cs.is_data_file(f"gs://b/silver/x/{name}"))

    def test_output_file_count(self):
        mb = 1024 * 1024
        self.assertEqual(cs.output_file_count(0, 256 * mb), 1)
        self.assertEqual(cs.output_file_count(100 * mb, 256 * mb), 1)
        self.assertEqual(cs.output_file_count(257 * mb, 256 * mb), 2)
        self.assertEqual(cs.output_file_count(1024 * mb, 256 * mb), 4)

    def test_staging_is_outside_every_table_prefix(self):
        """BigQuery reads gs://bucket/silver/<dataset>/*, so staging must not live under it."""
        for dataset in cs.DATASETS:
            self.assertFalse(cs.staging_root(BASE, dataset, DAY).startswith(f"{BASE}/{dataset}/"))
            self.assertFalse(cs.staging_root(BASE, "vessel_positions", DAY).startswith(f"{BASE}/vessel_metadata/"))

    def test_only_closed_days_are_compacted(self):
        with self.assertRaises(ValueError):
            cs.compact(None, FakeStorage(), BASE, DATASET, cs.utc_today())

    def test_main_refuses_to_run_without_a_silver_location(self):
        saved = os.environ.pop("GCS_BUCKET", None)
        try:
            self.assertEqual(cs.main(["--date", "2020-01-01"]), 2)     # returns before starting Spark
        finally:
            if saved is not None:
                os.environ["GCS_BUCKET"] = saved

    def test_defaults(self):
        self.assertLess(cs.utc_yesterday(), cs.utc_today())
        self.assertEqual(cs.parse_args([]).dataset, "all")
        self.assertFalse(cs.parse_args([]).dry_run)


class CommitTest(unittest.TestCase):
    def test_commit_replaces_originals_and_keeps_late_files(self):
        storage = FakeStorage()
        manifest, run_dir = stage_run(storage, n_staged=2)
        cs.apply_manifest(storage, manifest, run_dir)
        names = sorted(f.rsplit("/", 1)[1] for f, _ in storage.list_files(PART))
        self.assertEqual(names, ["compacted-r1-0000.snappy.parquet", "compacted-r1-0001.snappy.parquet",
                                 "part-late.snappy.parquet"])
        self.assertFalse(storage.exists(run_dir), "staging must be removed after the commit")

    def test_commit_is_idempotent(self):
        storage = FakeStorage()
        manifest, run_dir = stage_run(storage)
        cs.apply_manifest(storage, manifest, run_dir)
        before = dict(storage.files)
        cs.apply_manifest(storage, manifest, run_dir)
        self.assertEqual(storage.files, before)

    def test_missing_source_and_destination_raises_before_deleting_anything(self):
        storage = FakeStorage()
        manifest, run_dir = stage_run(storage)
        storage.delete(manifest["moves"][0][0])          # the staged file vanished, and was never moved
        with self.assertRaises(RuntimeError):
            cs.apply_manifest(storage, manifest, run_dir)
        for original in manifest["originals"]:
            self.assertTrue(storage.exists(original), "originals must survive a failed commit")


class RecoveryTest(unittest.TestCase):
    def assert_committed_once(self, storage, manifest):
        compacted = [f for f, _ in storage.list_files(PART) if "/compacted-" in f]
        self.assertEqual(len(compacted), len(manifest["moves"]), "each compacted file exactly once (no duplicates)")
        for original in manifest["originals"]:
            self.assertFalse(storage.exists(original))
        self.assertTrue(storage.exists(f"{PART}/part-late.snappy.parquet"), "late data must survive")
        self.assertEqual(storage.list_dirs(ROOT), [], "staging removed")

    def test_crash_while_deleting_originals_is_finished_by_the_next_run(self):
        storage = CrashingStorage("delete", 3)            # dies deleting the 3rd original
        manifest, run_dir = stage_run(storage)
        with self.assertRaises(Crash):
            cs.apply_manifest(storage, manifest, run_dir)
        storage.nth = -1                                   # the process restarts: no more crashes
        cs.recover(storage, ROOT)
        self.assert_committed_once(storage, manifest)

    def test_crash_between_moves_is_finished_by_the_next_run(self):
        storage = CrashingStorage("rename", 2)            # first staged file moved, dies on the second
        manifest, run_dir = stage_run(storage, n_staged=3)
        with self.assertRaises(Crash):
            cs.apply_manifest(storage, manifest, run_dir)
        for original in manifest["originals"]:
            self.assertTrue(storage.exists(original), "nothing may be deleted before every file is moved")
        storage.nth = -1
        cs.recover(storage, ROOT)
        self.assert_committed_once(storage, manifest)

    def test_staging_without_manifest_is_discarded_and_the_partition_untouched(self):
        storage = FakeStorage()
        manifest, run_dir = stage_run(storage)
        storage.delete(f"{run_dir}/manifest.json")         # died before the manifest was written
        cs.recover(storage, ROOT)
        self.assertEqual(storage.list_dirs(ROOT), [])
        for original in manifest["originals"]:
            self.assertTrue(storage.exists(original))
        self.assertFalse([f for f, _ in storage.list_files(PART) if "/compacted-" in f])

    def test_recover_with_nothing_to_do(self):
        cs.recover(FakeStorage(), ROOT)


if __name__ == "__main__":
    unittest.main()
