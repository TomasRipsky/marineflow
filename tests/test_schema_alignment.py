"""The BigQuery external tables (Terraform) must describe exactly what the Spark jobs write to GCS.

A column the job writes but Terraform lacks is invisible in BigQuery; a column Terraform declares
but the job never writes is always null. The Hive partition columns (partition_date, partition_hour)
are supplied by the partitioning, not by the schema list.
"""
import re
import unittest

from helpers import ROOT, SPARK_DIR

TF = (ROOT / "infra/terraform/modules/bigquery/main.tf").read_text()


HIVE_COLUMNS = {"partition_date", "partition_hour"}


def terraform_columns(table):
    block = TF.split(f'resource "google_bigquery_table" "{table}"', 1)[1]
    block = re.split(r'\nresource "', block, 1)[0]
    return set(re.findall(r'\{\s*name\s*=\s*"(\w+)"', block)) - HIVE_COLUMNS


def terraform_hive_columns(table):
    block = TF.split(f'resource "google_bigquery_table" "{table}"', 1)[1]
    block = re.split(r'\nresource "', block, 1)[0]
    return dict(re.findall(r'\{\s*name\s*=\s*"(partition_\w+)",\s*type\s*=\s*"(\w+)"', block))


def source(job):
    return (SPARK_DIR / job).read_text()


def between(text, start, end):
    return text.split(start, 1)[1].split(end, 1)[0]


def bronze_columns(job, extract, lineage_fn, next_fn):
    text = source(job)
    columns = set(re.findall(r'\.alias\("(\w+)"\)', between(text, f"def {extract}", "def validate" if job == "bronze_positions.py" else f"def {lineage_fn}")))
    columns |= set(re.findall(r'^\s+"(\w+)":', between(text, f"def {lineage_fn}", f"def {next_fn}"), flags=re.M))
    return columns - {"partition_date", "partition_hour"}


class BronzeTablesTest(unittest.TestCase):
    def test_positions_raw(self):
        job = bronze_columns("bronze_positions.py", "extract_fields", "add_partition_and_lineage", "process_micro_batch")
        self.assertEqual(terraform_columns("vessel_positions_raw"), job)

    def test_metadata_raw(self):
        job = bronze_columns("bronze_metadata.py", "extract_fields", "add_lineage", "process_micro_batch")
        self.assertEqual(terraform_columns("vessel_metadata_raw"), job)


class SilverTablesTest(unittest.TestCase):
    def test_positions_clean(self):
        text = source("silver_positions.py")
        select = between(text, "return df.select(", "\n    )\n")
        columns = set(re.findall(r'^\s+"(\w+)",', select, flags=re.M)) | set(re.findall(r'\.alias\("(\w+)"\)', select))
        columns.add("_silver_batch_id")          # added in process_micro_batch
        columns.discard("_batch_id")             # renamed to _bronze_batch_id
        self.assertEqual(terraform_columns("vessel_positions_clean"), columns)

    def test_metadata(self):
        text = source("silver_metadata.py")
        select = between(text, "return df.select(", ").filter")
        columns = set(re.findall(r'\.alias\("(\w+)"\)', select)) | {"_silver_batch_id"}
        self.assertEqual(terraform_columns("vessel_metadata"), columns)



class HivePartitionColumnsTest(unittest.TestCase):
    """BigQuery adds the hive partition columns to the table-level schema; the Terraform provider sends that schema on every
    update and the API rejects it unless the external schema declares the same columns (terraform apply failed with
    'schemas must be the same' on vessel_metadata_raw)."""

    TYPES = {"partition_date": "DATE", "partition_hour": "INTEGER"}

    def test_every_external_table_declares_the_columns_its_writer_partitions_by(self):
        writers = {"vessel_positions_raw": "bronze_positions.py", "vessel_metadata_raw": "bronze_metadata.py",
                   "vessel_positions_clean": "silver_positions.py", "vessel_metadata": "silver_metadata.py"}
        for table, job in writers.items():
            with self.subTest(table=table):
                # the writer's partitionBy("partition_date"[, "partition_hour"]), not a Window.partitionBy("mmsi")
                partitioned_by = re.search(r'\.partitionBy\(("partition_[^)]*)\)', source(job)).group(1)
                expected = {name: self.TYPES[name] for name in re.findall(r'"(partition_\w+)"', partitioned_by)}
                self.assertEqual(terraform_hive_columns(table), expected)


if __name__ == "__main__":
    unittest.main()