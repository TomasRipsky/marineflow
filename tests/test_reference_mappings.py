"""Reference mappings must be correct and consistent across Silver, dbt and the docs."""
import re
import unittest

import yaml

from helpers import DBT_DIR, ROOT, SPARK_DIR, const, defines

SILVER_POSITIONS = SPARK_DIR / "silver_positions.py"
SILVER_METADATA = SPARK_DIR / "silver_metadata.py"


class FlagMappingTest(unittest.TestCase):
    def setUp(self):
        import reference_data
        self.mid = reference_data.MID_MAP

    def test_shape(self):
        for mid, country in self.mid.items():
            self.assertRegex(mid, r"^\d{3}$")
            self.assertRegex(country, r"^[A-Z]{2}$")

    def test_no_duplicate_literals_hidden_by_dict_overwrite(self):
        literals = re.findall(r'"(\d{3})"', (SPARK_DIR / "reference_data.py").read_text())
        self.assertEqual(len(literals), len(set(literals)))
        self.assertEqual(len(literals), len(self.mid))

    def test_known_countries(self):
        expected = {"211": "DE", "227": "FR", "246": "NL", "255": "PT", "275": "LV", "276": "EE",
                    "277": "LT", "278": "SI", "512": "NZ", "518": "CK", "632": "GN", "657": "NG",
                    "667": "SL", "674": "TZ", "701": "AR", "720": "BO", "366": "US", "477": "HK"}
        for mid, country in expected.items():
            with self.subTest(mid=mid):
                self.assertEqual(self.mid.get(mid), country)

    def test_single_source_of_truth(self):
        for path in (SILVER_POSITIONS, SILVER_METADATA):
            with self.subTest(job=path.name):
                self.assertFalse(defines(path, "MID_MAP"))
                self.assertIn("from reference_data import MID_MAP", path.read_text())


class VesselTypeMappingTest(unittest.TestCase):
    def setUp(self):
        self.types = const(SILVER_METADATA, "VESSEL_TYPE_MAP")

    def test_codes(self):
        expected = {0: "unknown", 20: "wing_in_ground", 29: "wing_in_ground", 30: "fishing", 31: "tug",
                    32: "tug", 33: "special_craft", 34: "special_craft", 35: "special_craft",
                    36: "sailing_or_pleasure", 37: "sailing_or_pleasure", 40: "high_speed_craft",
                    49: "high_speed_craft", 50: "special_craft", 51: "special_craft", 52: "tug",
                    53: "special_craft", 55: "special_craft", 59: "special_craft", 60: "passenger",
                    70: "cargo", 79: "cargo", 80: "tanker", 89: "tanker", 90: "other", 99: "other"}
        for code, category in expected.items():
            with self.subTest(code=code):
                self.assertEqual(self.types.get(code), category)

    def test_reserved_codes_fall_back(self):
        for code in (1, 19, 38, 39):
            self.assertNotIn(code, self.types)

    def test_tug_codes(self):
        self.assertEqual(sorted(k for k, v in self.types.items() if v == "tug"), [31, 32, 52])


class CrossLayerCategoryTest(unittest.TestCase):
    """Every category Silver can emit must be handled by dbt."""

    def setUp(self):
        self.silver = set(const(SILVER_METADATA, "VESSEL_TYPE_MAP").values()) | {"other"}  # 'other' = fallback
        sql = (DBT_DIR / "models/gold/vessel_speed_anomalies.sql").read_text()
        self.limits = set(re.findall(r"struct\('([a-z_]+)'", sql))
        sources = yaml.safe_load((DBT_DIR / "models/staging/sources.yml").read_text())
        column = next(c for t in sources["sources"][0]["tables"] if t["name"] == "vessel_metadata"
                      for c in t["columns"] if c["name"] == "vessel_type_normalized")
        self.accepted = set(column["tests"][0]["accepted_values"]["values"])
        self.port_sql = (DBT_DIR / "models/gold/port_traffic.sql").read_text()

    def test_every_category_has_a_speed_limit(self):
        self.assertEqual(self.silver - self.limits, set())
        self.assertIn("unknown", self.limits)  # dbt staging default for vessels without metadata

    def test_accepted_values_match_silver_and_limits(self):
        self.assertEqual((self.silver | {"unknown"}) - self.accepted, set())
        self.assertEqual(self.accepted - self.limits, set())

    def test_port_traffic_counts_every_category_or_the_catch_all(self):
        named = set(re.findall(r"vessel_type_normalized = '([a-z_]+)'", self.port_sql))
        self.assertEqual(self.silver - named, {"other", "unknown", "wing_in_ground"})  # folded into other_vessels

    def test_port_traffic_breakdown_is_tested(self):
        schema = (DBT_DIR / "models/gold/schema.yml").read_text()
        self.assertIn("vessel_type_breakdown_sums_to_unique_vessels", schema)


class NavigationalStatusTest(unittest.TestCase):
    def test_labels_match_the_loitering_filter(self):
        nav = const(SPARK_DIR / "silver_positions.py", "NAV_STATUS_MAP")
        self.assertEqual((nav[1], nav[5]), ("at_anchor", "moored"))
        loitering = (DBT_DIR / "models/gold/vessel_loitering.sql").read_text()
        self.assertIn("'at_anchor', 'moored'", loitering)
        self.assertIn("navigational_status is null", loitering)  # NOT IN would drop NULL statuses

    def test_reserved_codes_are_left_to_the_unknown_fallback(self):
        nav = const(SPARK_DIR / "silver_positions.py", "NAV_STATUS_MAP")
        self.assertTrue({11, 12, 14} <= set(nav))
        self.assertFalse({9, 10, 13} & set(nav))


def classify(regions, lat, lon):
    """Same construction as enrich_geospatial(): when() wrappers built over reversed(regions)."""
    expr = lambda la, lo: "open_ocean"
    for la0, la1, lo0, lo1, name in reversed(regions):
        prev = expr
        expr = (lambda la, lo, la0=la0, la1=la1, lo0=lo0, lo1=lo1, name=name, prev=prev:
                name if (la0 <= la <= la1 and lo0 <= lo <= lo1) else prev(la, lo))
    return expr(lat, lon)


class OceanRegionTest(unittest.TestCase):
    def setUp(self):
        self.regions = const(SPARK_DIR / "silver_positions.py", "OCEAN_REGIONS")

    def test_chain_is_built_in_reverse_so_first_match_wins(self):
        self.assertIn("in reversed(OCEAN_REGIONS)", SILVER_POSITIONS.read_text())

    def test_reference_points(self):
        cases = {
            "Barcelona": (41.3, 2.1, "mediterranean"), "Marseille": (43.3, 5.4, "mediterranean"),
            "Piraeus": (37.9, 23.7, "mediterranean"), "Port Said": (31.26, 32.3, "mediterranean"),
            "Gibraltar": (36.1, -5.35, "mediterranean"), "Valencia": (39.5, -0.3, "mediterranean"),
            "Genoa": (44.4, 8.9, "mediterranean"), "Bilbao offshore": (43.6, -3.0, "atlantic_ocean"),
            "Bay of Biscay": (45.0, -3.0, "atlantic_ocean"), "Rotterdam": (51.9, 4.1, "atlantic_ocean"),
            "Santos": (-23.9, -46.3, "atlantic_ocean"), "Black Sea": (43.5, 34.0, "open_ocean"),
            "Arctic": (70.0, 20.0, "arctic_ocean"), "Southern Ocean": (-70.0, 0.0, "southern_ocean"),
            "Los Angeles": (33.7, -118.2, "pacific_east"), "Mumbai": (18.9, 72.8, "indian_ocean"),
            "New York": (40.7, -74.0, "atlantic_ocean"), "Gulf of Mexico": (28.5, -90.0, "atlantic_ocean"),
            "Miami": (25.8, -80.1, "atlantic_ocean"), "Colon": (9.4, -79.9, "atlantic_ocean"),
            "Halifax": (44.6, -63.6, "atlantic_ocean"), "Acapulco": (16.8, -99.9, "pacific_east"),
            "Callao": (-12.0, -77.2, "pacific_east"), "Lazaro Cardenas": (17.9, -102.2, "pacific_east"),
            "Shanghai": (31.2, 121.5, "pacific_west"), "Tokyo Bay": (35.4, 139.7, "pacific_west"),
            "Hong Kong": (22.3, 114.2, "pacific_west"), "Singapore": (1.26, 103.8, "pacific_west"),
            "Malacca Strait": (3.0, 100.5, "indian_ocean"), "Colombo": (6.9, 79.8, "indian_ocean"),
        }
        for name, (lat, lon, expected) in cases.items():
            with self.subTest(place=name):
                self.assertEqual(classify(self.regions, lat, lon), expected)

    def test_every_major_port_is_in_a_named_region(self):
        for lat, lon, _radius, name, _country in const(SILVER_POSITIONS, "MAJOR_PORTS"):
            with self.subTest(port=name):
                self.assertNotEqual(classify(self.regions, lat, lon), "open_ocean")

    def test_port_said_is_not_suez(self):
        port = next(p for p in const(SILVER_POSITIONS, "MAJOR_PORTS") if p[3] == "Port Said")
        self.assertAlmostEqual(port[0], 31.26, delta=0.05)


if __name__ == "__main__":
    unittest.main()
