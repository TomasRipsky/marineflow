"""The BigQuery external tables (Terraform) must describe exactly what the Spark jobs write to GCS.

A column the job writes but Terraform lacks is invisible in BigQuery; a column Terraform declares
but the job never writes is always null. The Hive partition columns (partition_date, partition_hour)
are supplied by the partitioning, not by the schema list.
"""
import re
import unittest

from helpers import ROOT, SPARK_DIR

TF = (ROOT / "infra/terraform/modules/bigquery/main.tf").read_text()


def terraform_columns(table):
    block = TF.split(f'resource "google_bigquery_table" "{table}"', 1)[1]
    block = re.split(r'\nresource "', block, 1)[0]
    return set(re.findall(r'\{\s*name\s*=\s*"(\w+)"', block))


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


if __name__ == "__main__":
    unittest.main()
