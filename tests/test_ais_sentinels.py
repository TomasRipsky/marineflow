"""AIS 'not available' values must never look like real data, in Silver or in the hot path."""
import unittest

import pandas as pd

from helpers import FakeState, SPARK_DIR, const, load_hot_alerts, state_schema_fields

T0 = "2026-09-28T10:00:00+00:00"
T30 = "2026-09-28T10:30:00+00:00"
NEAR = (10.00, 10.00), (10.02, 10.02)   # ~1.7 nm apart in 30 min: no speed anomaly
FAR = (10.00, 10.00), (11.50, 10.00)    # ~90 nm apart in 30 min: ~180 kn calculated


def run(ns, positions):
    """Feed positions of one vessel, one micro-batch each; return the alert types raised."""
    state, alerts = FakeState(), []
    for lat, lon, sog, ts in positions:
        frame = pd.DataFrame([{"MMSI": "123456789", "Latitude": lat, "Longitude": lon, "Sog": sog, "time_utc": ts}])
        for out in ns["process_vessel"](("123456789",), iter([frame]), state):
            alerts += out["alert_type"].tolist()
    return alerts, state


class CleanSogTest(unittest.TestCase):
    def setUp(self):
        self.clean_sog = load_hot_alerts()["clean_sog"]

    def test_valid_speeds_are_kept(self):
        for value in (0, 12.5, 102.2):
            with self.subTest(value=value):
                self.assertEqual(self.clean_sog(value), float(value))

    def test_not_available_and_missing_become_none(self):
        for value in (102.3, 150, None, float("nan")):
            with self.subTest(value=value):
                self.assertIsNone(self.clean_sog(value))


class HotPathSentinelTest(unittest.TestCase):
    def setUp(self):
        self.ns = load_hot_alerts()

    def test_sentinel_sog_is_not_a_speed_change(self):
        alerts, state = run(self.ns, [(*NEAR[0], 12.0, T0), (*NEAR[1], 102.3, T30)])
        self.assertEqual(alerts, [])
        self.assertIsNone(self.ns["VesselState"](*state.value).last_sog)

    def test_missing_sog_raises_nothing(self):
        alerts, _ = run(self.ns, [(*NEAR[0], 12.0, T0), (*NEAR[1], float("nan"), T30)])
        self.assertEqual(alerts, [])

    def test_real_acceleration_still_alerts(self):
        alerts, _ = run(self.ns, [(*NEAR[0], 5.0, T0), (*NEAR[1], 20.0, T30)])
        self.assertEqual(alerts, ["SUDDEN_ACCELERATION"])

    def test_impossible_jump_with_real_sog_change_is_spoofing(self):
        alerts, _ = run(self.ns, [(*FAR[0], 8.0, T0), (*FAR[1], 20.0, T30)])
        self.assertEqual(alerts, ["GPS_SPOOFING"])

    def test_impossible_jump_with_steady_sog_is_impossible_speed(self):
        alerts, _ = run(self.ns, [(*FAR[0], 10.0, T0), (*FAR[1], 12.0, T30)])
        self.assertEqual(alerts, ["IMPOSSIBLE_SPEED"])

    def test_impossible_jump_with_unknown_sog_is_impossible_speed_not_spoofing(self):
        alerts, _ = run(self.ns, [(*FAR[0], 10.0, T0), (*FAR[1], 102.3, T30)])
        self.assertEqual(alerts, ["IMPOSSIBLE_SPEED"])


class HotPathStateTest(unittest.TestCase):
    """Spark returns the per-vessel state as a plain tuple. process_vessel once read it as an object
    (prev.last_time_utc) and would have crashed on the second position of any vessel."""

    def setUp(self):
        self.ns = load_hot_alerts()

    def test_state_fields_follow_the_state_schema(self):
        self.assertEqual(list(self.ns["VesselState"]._fields), state_schema_fields())

    def test_second_position_of_a_vessel_does_not_crash(self):
        alerts, state = run(self.ns, [(*NEAR[0], 10.0, T0), (*NEAR[1], 10.0, T30)])
        self.assertEqual(alerts, [])
        self.assertIsInstance(state.value, tuple)

    def test_gap_alert_when_the_vessel_times_out(self):
        _, state = run(self.ns, [(*NEAR[0], 10.0, T0)])
        timed_out = FakeState(has_timed_out=True)
        timed_out.exists, timed_out.value = True, state.value
        frames = list(self.ns["process_vessel"](("123456789",), iter([]), timed_out))
        self.assertEqual(frames[0]["alert_type"].tolist(), ["AIS_GAP"])
        self.assertEqual(frames[0].iloc[0]["latitude"], NEAR[0][0])
        self.assertFalse(timed_out.exists)


class SilverSentinelTest(unittest.TestCase):
    """Spark is not available in the test environment, so check the source and the shared constants."""

    def setUp(self):
        self.path = SPARK_DIR / "silver_positions.py"
        self.source = self.path.read_text()

    def test_thresholds(self):
        self.assertEqual(const(self.path, "AIS_SOG_NOT_AVAILABLE"), 102.3)
        self.assertEqual(const(self.path, "AIS_COG_NOT_AVAILABLE"), 360.0)

    def test_silver_and_hot_path_use_the_same_sog_sentinel(self):
        self.assertEqual(const(self.path, "AIS_SOG_NOT_AVAILABLE"), load_hot_alerts()["SOG_NOT_AVAILABLE"])

    def test_sentinels_become_null(self):
        self.assertIn('F.when(F.col("Sog") < AIS_SOG_NOT_AVAILABLE, F.col("Sog"))', self.source)
        self.assertIn('F.when(F.col("Cog") < AIS_COG_NOT_AVAILABLE, F.col("Cog"))', self.source)
        self.assertIn('F.when(F.col("TrueHeading") != 511', self.source)


if __name__ == "__main__":
    unittest.main()
