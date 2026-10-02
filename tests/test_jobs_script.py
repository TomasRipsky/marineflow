"""scripts/jobs.sh runs the Spark jobs supervised; it must stay in line with the compose file and the README."""
import os
import re
import subprocess
import unittest

from helpers import ROOT, SPARK_DIR

SCRIPT = ROOT / "scripts" / "jobs.sh"
TEXT = SCRIPT.read_text()
README = (ROOT / "README.md").read_text()
COMPOSE = (ROOT / "docker-compose.yml").read_text()


def flags(command):
    """The spark-submit options of a command line, as a set of 'flag value' strings."""
    found = set(re.findall(r"--conf [\w.]+=[\w.:/\-]+", command))
    found |= set(re.findall(r"--(?:master|driver-memory|packages|jars|driver-class-path) [\w.:/\-\[\]]+", command))
    return found


class JobsScriptTest(unittest.TestCase):
    def test_script_is_valid_bash_and_executable(self):
        self.assertTrue(os.access(SCRIPT, os.X_OK))
        self.assertEqual(subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode, 0)

    def test_every_job_has_a_script_and_a_container(self):
        jobs = re.search(r"ALL_JOBS=\(([^)]*)\)", TEXT).group(1).split()
        self.assertEqual(len(jobs), 5)
        for job in jobs:
            with self.subTest(job=job):
                self.assertIn(f"container_name: marineflow-spark-{job}", COMPOSE)
                script = re.search(rf"{re.escape(job)}\)\s+echo (\w+\.py)", TEXT).group(1)
                self.assertTrue((SPARK_DIR / script).exists())

    def test_gcs_jobs_use_the_flags_of_the_readme_template(self):
        template = re.search(r"docker exec -it marineflow-spark-bronze (/opt/spark/bin/spark-submit [^\n]+)", README).group(1)
        # the script substitutes the driver memory; the README template spells it out
        in_script = flags(TEXT.replace("$memory", "1g"))
        self.assertTrue(flags(template) <= in_script, flags(template) - in_script)
        self.assertIn("spark.sql.streaming.metricsEnabled=true", TEXT)

    def test_metadata_jobs_default_to_a_smaller_driver_than_the_positions_jobs(self):
        """Measured live: 512m cuts the two low-volume metadata containers from about 1.6 to 1.1 GiB, which was the margin
        that kept a 10.7 GiB Docker VM from the OOM killer."""
        body = TEXT.split("memory_of() {", 1)[1].split("\n}\n", 1)[0]
        self.assertIn("bronze-metadata|silver-metadata) echo 512m", body)
        self.assertIn("*) echo 1g", body)

    def test_hot_alerts_gets_no_gcs_jars(self):
        hot = TEXT.split('if [ "$script" = hot_alerts.py ]; then', 1)[1].split("else", 1)[0]
        self.assertNotIn("gcs-connector", hot)
        self.assertIn("spark-sql-kafka", TEXT.split("common=", 1)[1].split("\n", 1)[0])

    def test_the_readme_documents_every_command_and_job(self):
        doc = (ROOT / "scripts" / "README.md").read_text()
        for command in re.findall(r"^  (start|stop|status|logs)\)", TEXT, re.M):
            with self.subTest(command=command):
                self.assertIn(f"scripts/jobs.sh {command}", doc)
        for job in re.search(r"ALL_JOBS=\(([^)]*)\)", TEXT).group(1).split():
            with self.subTest(job=job):
                self.assertIn(f"`{job}`", doc)
        for variable in re.findall(r'^(\w+)="\$\{\1:-', TEXT, re.M):
            with self.subTest(variable=variable):
                self.assertIn(f"`{variable}`", doc)


if __name__ == "__main__":
    unittest.main()
