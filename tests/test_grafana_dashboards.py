"""The Grafana dashboards are generated code: check they are current, well formed and wired to real datasources."""
import json
import re
import subprocess
import sys
import unittest

from helpers import ROOT

GRAFANA = ROOT / "monitoring" / "grafana"
DASHBOARDS = sorted((GRAFANA / "dashboards" / "json").glob("*.json"))
DATASOURCE_UIDS = set(re.findall(r"^\s+uid: (\S+)", (GRAFANA / "datasources" / "datasource.yml").read_text(), re.M))
PROMETHEUS_JOBS = set(re.findall(r'job_name: "([^"]+)"', (ROOT / "monitoring/prometheus/prometheus.yml").read_text()))


def load(path):
    return json.loads(path.read_text())


class GeneratedFilesTest(unittest.TestCase):
    def test_committed_json_matches_the_generator(self):
        result = subprocess.run([sys.executable, str(GRAFANA / "generate_dashboards.py"), "--check"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_the_four_dashboards_exist(self):
        self.assertEqual([p.stem for p in DASHBOARDS],
                         ["marineflow-kafka", "marineflow-live", "marineflow-overview", "marineflow-spark"])


class DashboardShapeTest(unittest.TestCase):
    def test_uid_matches_the_file_name_and_titles_are_unique(self):
        titles = []
        for path in DASHBOARDS:
            data = load(path)
            self.assertEqual(data["uid"], path.stem)
            titles.append(data["title"])
        self.assertEqual(len(titles), len(set(titles)))

    def test_panel_ids_are_unique_and_fit_the_24_column_grid(self):
        for path in DASHBOARDS:
            panels = load(path)["panels"]
            ids = [p["id"] for p in panels]
            self.assertEqual(len(ids), len(set(ids)), path.name)
            for p in panels:
                grid = p["gridPos"]
                self.assertLessEqual(grid["x"] + grid["w"], 24, f"{path.name}: {p['title']}")

    def test_every_datasource_reference_is_provisioned(self):
        for path in DASHBOARDS:
            for p in load(path)["panels"]:
                refs = [p["datasource"]] + [t["datasource"] for t in p.get("targets", []) if "datasource" in t]
                for ref in refs:
                    self.assertIn(ref["uid"], DATASOURCE_UIDS, f"{path.name}: {p['title']}")

    def test_every_literal_job_selector_matches_a_scraped_job(self):
        for path in DASHBOARDS:
            for job in re.findall(r'job=~?\\?"([^"\\$]+)', path.read_text()):  # "$job" is a dashboard variable
                for name in job.split("|"):
                    self.assertTrue(any(re.fullmatch(name, j) for j in PROMETHEUS_JOBS), f"{path.name}: {name}")


class ComposeWiringTest(unittest.TestCase):
    COMPOSE = (ROOT / "docker-compose.yml").read_text()

    def test_grafana_image_is_pinned(self):
        self.assertRegex(self.COMPOSE, r"image: grafana/grafana:\d+\.\d+\.\d+")

    def test_home_dashboard_is_a_generated_file(self):
        path = re.search(r"GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/provisioning/(\S+)", self.COMPOSE).group(1)
        self.assertTrue((GRAFANA / path).exists(), path)


if __name__ == "__main__":
    unittest.main()
