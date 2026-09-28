"""Shared helpers for the test suite.

The Spark job modules call load_dotenv() at import time and need PySpark, so the
tests never import them. Constants and pure functions are pulled out of the
source with ast instead.
"""
import ast
import math
import re
import sys
from collections import namedtuple
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SPARK_DIR = ROOT / "processing" / "spark_streaming"
DBT_DIR = ROOT / "transformation" / "dbt"

if str(SPARK_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_DIR))  # for `import reference_data`


def _top_level(path):
    return ast.parse(Path(path).read_text()).body


def const(path, name):
    """Evaluate a top-level `NAME = <literal expression>` without importing the module."""
    for node in _top_level(path):
        if isinstance(node, ast.Assign) and node.targets[0].id == name:
            return eval(compile(ast.Expression(node.value), name, "eval"))
    raise KeyError(f"{name} not found in {path}")


def defines(path, name):
    return any(isinstance(n, ast.Assign) and n.targets[0].id == name for n in _top_level(path))


# --- hot path ---------------------------------------------------------------
OUTPUT_FIELDS = ["mmsi", "alert_type", "severity", "description", "latitude", "longitude", "detected_at"]


def load_hot_alerts():
    """Namespace with the hot path constants and functions (haversine_nm, clean_sog, process_vessel)."""
    functions = {"haversine_nm", "clean_sog", "process_vessel"}
    constants = {"MAX_SPEED_KNOTS", "SPEED_CHANGE_SPOOFING_THRESHOLD", "SPEED_CHANGE_ACCEL_THRESHOLD",
                 "GAP_TIMEOUT_MINUTES", "SOG_NOT_AVAILABLE", "VesselState"}
    ns = {
        "math": math,
        "namedtuple": namedtuple,
        "timedelta": timedelta,
        "get_output_schema": lambda: SimpleNamespace(fields=[SimpleNamespace(name=n) for n in OUTPUT_FIELDS]),
    }
    path = SPARK_DIR / "hot_alerts.py"
    for node in _top_level(path):
        wanted = (isinstance(node, ast.FunctionDef) and node.name in functions) or (
            isinstance(node, ast.Assign) and node.targets[0].id in constants)
        if wanted:
            exec(compile(ast.Module([node], []), str(path), "exec"), ns)
    return ns


def state_schema_fields():
    """Field names of get_state_schema(), in order, read from the source."""
    source = (SPARK_DIR / "hot_alerts.py").read_text()
    body = source.split("def get_state_schema():", 1)[1].split("def get_output_schema", 1)[0]
    return re.findall(r'StructField\("(\w+)"', body)


class FakeState:
    """Stand-in for Spark's GroupState. Like the real one, it stores and returns a plain tuple
    in state-schema order (not an object with attributes)."""

    def __init__(self, has_timed_out=False):
        self.hasTimedOut = has_timed_out
        self.exists, self.value, self.timeout_ms = False, None, None

    @property
    def get(self):
        return self.value

    def update(self, row):
        assert len(row) == len(state_schema_fields()), "state must match get_state_schema()"
        self.exists, self.value = True, tuple(row)

    def setTimeoutDuration(self, ms):
        self.timeout_ms = ms

    def remove(self):
        self.exists, self.value = False, None
