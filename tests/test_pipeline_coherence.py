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

    def test_erratic_course_waits_for_the_activity_summary(self):
        self.assertIn("dbt_batch >> dbt_erratic", self.dag)
        self.assertIn("[dbt_erratic, dbt_risk] >> dbt_test", self.dag)


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
