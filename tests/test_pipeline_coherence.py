"""Cross-file wiring checks: the kind of drift that has already caused bugs here."""
import re
import unittest

import yaml

from helpers import DBT_DIR, ROOT, SPARK_DIR

COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
ENV_EXAMPLE = (ROOT / ".env.example").read_text()


class DagTest(unittest.TestCase):
    def setUp(self):
        self.dag = (ROOT / "orchestration/dags/marineflow_pipeline.py").read_text()

    def test_every_gold_model_is_run_by_the_dag(self):
        """vessel_erratic_course once had schema tests but was never built by the DAG."""
        gold = {p.stem for p in (DBT_DIR / "models/gold").glob("*.sql")}
        selected = set()
        for selection in re.findall(r'run --select ([a-z_ ]+)"', self.dag):
            selected |= set(selection.split())
        self.assertEqual(gold - selected, set())

    def test_every_dbt_model_selected_by_the_dag_exists(self):
        models = {p.stem for p in (DBT_DIR / "models").rglob("*.sql")}
        for selection in re.findall(r'run --select ([a-z_ ]+)"', self.dag):
            for model in selection.split():
                with self.subTest(model=model):
                    self.assertIn(model, models)

    def test_dag_holds_no_spark_and_never_overwrites_silver(self):
        """The Airflow image has no Spark, and the compaction that used to live here read a partition
        and overwrote it (deleting the data). Compaction is compact_silver.py, run by hand."""
        self.assertNotIn("pyspark", self.dag)
        self.assertNotIn('mode("overwrite")', self.dag)
        self.assertNotIn("compact_silver_partition", self.dag)
        self.assertNotIn("PythonOperator", self.dag.replace("ShortCircuitOperator", ""))

    def test_staging_view_is_rebuilt_before_any_gold_model(self):
        """dbt does not rebuild a view that is only referenced: the DAG once ran gold models against a stale
        stg_vessel_positions (still selecting a renamed Silver column) and every one of them failed."""
        self.assertIn('DBT_CMD.format("run --select stg_vessel_positions")', self.dag)
        self.assertIn("check_silver >> dbt_staging >> [dbt_batch, dbt_dark, dbt_speed, dbt_loitering]", self.dag)

    def test_erratic_course_waits_for_the_activity_summary(self):
        self.assertIn("dbt_batch >> dbt_erratic", self.dag)
        self.assertIn("[dbt_erratic, dbt_risk] >> dbt_test", self.dag)


class DbtModelsTest(unittest.TestCase):
    def test_the_removed_sts_flag_stays_removed(self):
        """potential_sts_transfer could never be true (distance_to_port_km is null outside the 15 port
        boxes). Do not re-add it without a real port catalogue."""
        for path in (DBT_DIR / "models").rglob("*"):
            if path.suffix in {".sql", ".yml"}:
                text = path.read_text()
                for name in ("potential_sts_transfer", "sts_signals", "avg_distance_to_port_km"):
                    with self.subTest(file=path.name, name=name):
                        self.assertNotIn(name, text)

    def test_loitering_carries_no_always_null_port_columns(self):
        """vessel_loitering only looks outside port zones, where Silver leaves these three null."""
        sql = (DBT_DIR / "models/gold/vessel_loitering.sql").read_text()
        code = "\n".join(line.split("--")[0] for line in sql.splitlines())   # ignore comments
        for column in ("port_name", "port_country", "distance_to_port_km"):
            with self.subTest(column=column):
                self.assertNotIn(column, code)

    def test_models_that_lost_columns_sync_their_schema(self):
        """dbt's incremental merge inserts the target table's columns; a column removed from the model
        would break every run until a full refresh, unless the schema is synced."""
        for model in ("vessel_loitering", "vessel_risk_score"):
            with self.subTest(model=model):
                sql = (DBT_DIR / f"models/gold/{model}.sql").read_text()
                self.assertIn("on_schema_change='sync_all_columns'", sql)

    def test_risk_score_still_weights_the_remaining_signals(self):
        sql = (DBT_DIR / "models/gold/vessel_risk_score.sql").read_text()
        for term in ("dark_score", "port_change_gaps", "spoofing_signals", "speed_anomalies", "loiter_events"):
            self.assertIn(term, sql)


class ComposeWiringTest(unittest.TestCase):
    def container_env(self):
        env = {}
        for service in COMPOSE["services"].values():
            for entry in service.get("environment", []) or []:
                name, _, value = str(entry).partition("=")
                env[name] = value
        return env

    def test_batch_size_variables_read_by_the_jobs_reach_the_containers(self):
        """The *_MAX_OFFSETS_PER_TRIGGER variables were read by the code but never passed to Docker."""
        read = set()
        for job in SPARK_DIR.glob("*.py"):
            read |= set(re.findall(r'os\.getenv\("([A-Z_]*MAX_OFFSETS_PER_TRIGGER)"', job.read_text()))
        self.assertGreaterEqual(len(read), 4)
        self.assertEqual(read - set(self.container_env()), set())

    def test_batch_size_variables_are_documented_in_env_example(self):
        for name in re.findall(r"^([A-Z_]*MAX_OFFSETS_PER_TRIGGER)=", ENV_EXAMPLE, flags=re.M):
            self.assertIn(name, self.container_env())
        for name in self.container_env():
            if name.endswith("MAX_OFFSETS_PER_TRIGGER"):
                with self.subTest(var=name):
                    self.assertRegex(ENV_EXAMPLE, rf"(?m)^{name}=", msg="missing from .env.example")

    def test_variables_without_default_are_documented_in_env_example(self):
        compose_text = (ROOT / "docker-compose.yml").read_text()
        required = set(re.findall(r"\$\{([A-Z_]+)\}", compose_text))
        for name in required:
            with self.subTest(var=name):
                self.assertRegex(ENV_EXAMPLE, rf"(?m)^{name}=", msg="missing from .env.example")

    def test_env_example_holds_no_secret(self):
        match = re.search(r"^AIS_API_KEY=(.*)$", ENV_EXAMPLE, flags=re.M)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1).strip(), "")


class SparkImageTest(unittest.TestCase):
    """hot_alerts uses applyInPandasWithState, which needs pandas and pyarrow inside the image."""

    def setUp(self):
        self.requirements = (SPARK_DIR / "requirements.txt").read_text()

    def test_pandas_and_pyarrow_are_pinned(self):
        for package in ("pandas", "pyarrow", "numpy"):
            with self.subTest(package=package):
                self.assertRegex(self.requirements, rf"(?m)^{package}==\d")

    def test_hot_alerts_really_uses_pandas_state(self):
        self.assertIn("applyInPandasWithState", (SPARK_DIR / "hot_alerts.py").read_text())


if __name__ == "__main__":
    unittest.main()
