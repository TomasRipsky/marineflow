"""The project site (site/) must not rot: its headline numbers match the repo and its data stays anonymous."""
import json
import re
import unittest

import yaml

from helpers import DBT_DIR, ROOT

SITE = ROOT / "site"
HTML = (SITE / "index.html").read_text()
README = (ROOT / "README.md").read_text()


def facts():
    return {name: int(value) for name, value in re.findall(r'data-fact="(\w+)">(\d+)<', HTML)}


def data():
    text = (SITE / "assets" / "data.js").read_text()
    return json.loads(text[text.index("{"):text.rindex("}") + 1])


class FactsTest(unittest.TestCase):
    """Every number the site states about the repository is recomputed here, like the README's."""

    def test_counts_match_the_repository(self):
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
        init = (ROOT / "docker-compose.yml").read_text().split("kafka-init-topics:", 1)[1].split("spark-bronze:", 1)[0]
        dag = (ROOT / "orchestration/dags/marineflow_pipeline.py").read_text()
        expected = {
            "services": len(compose),
            "topics": len(set(re.findall(r"--topic ([\w-]+)", init))),
            "gold_models": len(list((DBT_DIR / "models/gold").glob("*.sql"))),
            "dag_tasks": len(re.findall(r'task_id="(\w+)"', dag)),
            "smoke_tests": len(list((ROOT / "tests/smoke").glob("*_smoke.py"))),
            "unit_tests": unittest.defaultTestLoader.discover(str(ROOT / "tests")).countTestCases(),
            "dbt_tests": int(re.search(r"\*\*(\d+) data tests\*\*", README).group(1)),
        }
        found = facts()
        for name, value in expected.items():
            with self.subTest(fact=name):
                self.assertIn(name, found)
                self.assertEqual(found[name], value)

    def test_every_fact_is_known(self):
        names = set(facts())
        self.assertLessEqual(names, {"services", "topics", "gold_models", "dag_tasks", "smoke_tests", "unit_tests", "dbt_tests"})


class FilesTest(unittest.TestCase):
    def test_local_references_exist(self):
        for ref in re.findall(r'(?:src|href)="(assets/[^"?]+)(?:\?[^"]*)?"', HTML):
            with self.subTest(ref=ref):
                self.assertTrue((SITE / ref).exists())

    def test_diagrams_are_copies_of_the_ones_in_the_readme(self):
        for name in ("architecture.svg", "medallion.svg"):
            with self.subTest(name=name):
                self.assertEqual((SITE / "assets/img" / name).read_text(), (ROOT / "docs/img" / name).read_text())

    def test_every_gallery_screenshot_is_documented(self):
        js = (SITE / "assets/app.js").read_text()
        notes = (SITE / "assets/shots/README.md").read_text()
        for name in re.findall(r'\["([\w-]+)", "', js):
            with self.subTest(shot=name):
                self.assertIn(f"{name}.png", notes)

    def test_gallery_images_are_attached_to_the_page_and_not_lazy(self):
        """A detached <img> with loading="lazy" can stay unloaded for ever in some browsers: the placeholder
        frames stayed on screen on the live site although the PNGs were there."""
        js = (SITE / "assets/app.js").read_text()
        gallery = js.split("function gallery()", 1)[1].split("})();", 1)[0]
        self.assertNotIn('"lazy"', gallery)
        self.assertNotIn("new Image()", gallery)
        self.assertIn("fig.appendChild(img)", gallery)

    def test_screenshots_open_in_a_closable_lightbox(self):
        js = (SITE / "assets/app.js").read_text()
        css = (SITE / "assets/style.css").read_text()
        for needle in ('"Escape"', 'role", "dialog"', "lb-close", "lightbox.open(img", 'e.key === "Enter"'):
            with self.subTest(needle=needle):
                self.assertIn(needle, js)
        for selector in (".lightbox {", ".lightbox.full", "html.lb-open", ".lightbox[hidden]"):
            with self.subTest(selector=selector):
                self.assertIn(selector, css)

    def test_hero_numbers_have_room(self):
        """A seven-digit number in the monospace face is about 12rem wide; the columns must be at least that and may shrink."""
        css = (SITE / "assets/style.css").read_text()
        stats = css.split(".stats {", 1)[1].split("}", 1)[0]
        self.assertRegex(stats, r"minmax\(min\(100%, 1[3-9]rem\), 1fr\)")
        self.assertIn("min-width: 0", css.split(".stat {", 1)[1].split("}", 1)[0])

    def test_capture_script_covers_every_gallery_screenshot(self):
        script = (SITE / "tools/capture_shots.mjs").read_text()
        js = (SITE / "assets/app.js").read_text()
        captured = set(re.findall(r'^\s+"?([\w-]+)"?: \{ app: "', script, re.M))
        self.assertEqual(captured, set(re.findall(r'\["([\w-]+)", "', js)))
        self.assertIn("blurMmsi: true", script)      # the alerts table shows MMSI numbers

    def test_pages_workflow_publishes_the_site_folder(self):
        workflow = (ROOT / ".github/workflows/pages.yml").read_text()
        self.assertIn("path: site", workflow)
        self.assertIn("__VERSION__", workflow)                       # cache busting is stamped at deploy time
        self.assertGreaterEqual(HTML.count("?v=__VERSION__"), 3)     # style, data and app
        self.assertIn("deploy-pages", workflow)


class DataTest(unittest.TestCase):
    """data.js is read from BigQuery by site/tools/refresh_data.py: it must stay consistent and anonymous."""

    def test_the_numbers_agree_with_each_other(self):
        d = data()
        self.assertEqual(sum(cell[2] for cell in d["grid"]), d["silver"]["positions"])
        self.assertEqual(sum(m["n"] for m in d["minutes"]), d["silver"]["positions"])
        self.assertEqual(sum(b["n"] for b in d["anomaly_buckets"]), d["anomaly_total"])
        self.assertEqual(d["anomaly_total"], d["rows"]["vessel_speed_anomalies"])
        self.assertEqual(d["gold"]["vessel_days"], d["rows"]["vessel_activity_summary"])

    def test_vessels_are_not_identifiable(self):
        d = data()
        masked = re.compile(r"^\d{6}\*\*\*$")
        for row in d["top_risk"]:
            self.assertRegex(row["mmsi"], masked)
            self.assertNotIn("vessel_name", row)
        self.assertRegex(d["case"]["mmsi"], masked)
        self.assertNotIn("name", d["case"])
        self.assertNotRegex((SITE / "assets/data.js").read_text(), r'"\d{9}"')


if __name__ == "__main__":
    unittest.main()
