"""The README must not rot: its links, images, anchors and headline numbers are checked against the repo."""
import re
import unittest

import yaml

from helpers import DBT_DIR, ROOT

README = (ROOT / "README.md").read_text()


def slug(heading):
    """GitHub's anchor for a heading: lower case, punctuation dropped, spaces to hyphens."""
    text = re.sub(r"[^\w\s-]", "", heading.strip().lower())
    return re.sub(r"\s", "-", text)


class LinksTest(unittest.TestCase):
    def test_images_and_relative_links_point_to_existing_files(self):
        targets = re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", README)
        checked = 0
        for target in targets:
            if target.startswith(("http://", "https://", "#", "mailto:")):
                continue
            with self.subTest(target=target):
                self.assertTrue((ROOT / target).exists(), f"{target} does not exist")
            checked += 1
        self.assertGreaterEqual(checked, 2)      # at least the two diagrams

    def test_every_anchor_link_resolves_to_a_heading(self):
        headings = {slug(h) for h in re.findall(r"^#{1,6}\s+(.+)$", README, flags=re.M)}
        for anchor in re.findall(r"\]\(#([^)]+)\)", README):
            with self.subTest(anchor=anchor):
                self.assertIn(anchor, headings)

    def test_diagrams_are_well_formed_xml(self):
        import xml.dom.minidom
        for image in (ROOT / "docs/img").glob("*.svg"):
            with self.subTest(image=image.name):
                xml.dom.minidom.parse(str(image))


class NumbersTest(unittest.TestCase):
    def test_docker_services(self):
        services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
        self.assertIn(f"{len(services)} Docker Compose services", README)
        self.assertIn(f"{len(services)} services", README)

    def test_kafka_topics(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        topics = set(re.findall(r"--topic ([\w-]+)", compose.split("kafka-init-topics:", 1)[1].split("spark-bronze:", 1)[0]))
        self.assertIn(f"{len(topics)} Kafka topics", README)

    def test_gold_models(self):
        gold = list((DBT_DIR / "models/gold").glob("*.sql"))
        self.assertIn(f"{len(gold)} gold models", README)

    def test_unit_test_count(self):
        """The headline number appears in the README and in the architecture diagram."""
        count = unittest.defaultTestLoader.discover(str(ROOT / "tests")).countTestCases()
        self.assertIn(f"**{count} unit tests**", README)
        self.assertIn(f"{count} unit tests", (ROOT / "docs/img/architecture.svg").read_text())
        self.assertIn(f"{count} unit tests", README.split("## 10. Quality and CI", 1)[1])

    def test_dag_tasks(self):
        dag = (ROOT / "orchestration/dags/marineflow_pipeline.py").read_text()
        tasks = re.findall(r'task_id="(\w+)"', dag)
        self.assertIn(f"{len(tasks)} tasks", README)

    def test_the_documented_reference_sizes(self):
        import reference_data
        self.assertIn(f"{len(reference_data.MID_MAP)} MID codes → {len(set(reference_data.MID_MAP.values()))} flag states", README)


if __name__ == "__main__":
    unittest.main()
