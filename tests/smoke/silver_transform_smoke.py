"""Smoke test of the real Silver positions transformation on Spark, inside the project's Spark image.

Feeds hand-made Bronze rows (a Mediterranean port, New York, Shanghai, open ocean, an unmapped
MID) through transform_to_silver() and checks what the unit tests can only approximate: the
enrichment columns (port_name, port_country, flag_country, ocean_region), the AIS "not available"
values becoming null, the navigation status labels and the per-vessel deltas. No Kafka, no GCP.

Run from the repo root (needs Docker):

    docker run --rm --entrypoint /bin/sh \
      -v "$PWD/processing:/opt/spark/processing:ro" \
      -v "$PWD/tests/smoke:/smoke:ro" \
      marineflow-spark:3.5.0 \
      -c "python3 /smoke/silver_transform_smoke.py"
"""
import sys
from datetime import datetime

sys.path.insert(0, "/opt/spark/processing/spark_streaming")
import silver_positions as sp
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

T = lambda s: datetime.fromisoformat(s)
# (MMSI, name, lat, lon, Sog, Cog, TrueHeading, NavigationalStatus, time_utc)
rows = [
    ("224123456", "BARCELONA SHIP", 41.3, 2.1, 12.5, 90.0, 90, 0, "2026-09-27T10:00:00"),
    ("366123456", "NEW YORK SHIP", 40.7, -74.0, 102.3, 360.0, 511, 5, "2026-09-27T10:00:00"),
    ("412123456", "SHANGHAI SHIP", 31.2, 121.5, 8.0, 45.0, 45, 1, "2026-09-27T10:00:00"),
    ("277123456", "OPEN OCEAN A", 0.0, -30.0, 10.0, 10.0, 10, 14, "2026-09-27T10:00:00"),
    ("277123456", "OPEN OCEAN A", 0.1, -30.0, 25.0, 20.0, 60, 0, "2026-09-27T10:01:00"),      # 2nd row: speed and heading deltas
    ("999999999", "UNKNOWN FLAG", 51.9, 4.1, 5.0, 5.0, 5, 11, "2026-09-27T10:00:00"),        # Rotterdam, MID 999 not mapped
]
now = T("2026-09-27T10:05:00")
data = [(m, n, lat, lon, sog, cog, hd, ns, T(t), now) for m, n, lat, lon, sog, cog, hd, ns, t in rows]

schema = sp.get_bronze_schema()
def build(row):
    m, n, lat, lon, sog, cog, hd, ns, t, ing = row
    d = {f.name: None for f in schema.fields}
    d.update(MMSI=m, ShipName=n, Latitude=lat, Longitude=lon, Sog=sog, Cog=cog, TrueHeading=hd, NavigationalStatus=ns,
             time_utc=t, ingestion_timestamp=ing, _batch_id="b1", _pipeline_version="test", _source_system="aisstream_live")
    return tuple(d[f.name] for f in schema.fields)

df = spark.createDataFrame([build(r) for r in data], schema)
out = sp.transform_to_silver(df).orderBy("mmsi", "event_timestamp")
cols = out.columns
print("COLUMNS:", cols)
for r in out.collect():
    d = r.asDict()
    print(f"{d['mmsi']} | flag={d['flag_country']} | region={d['ocean_region']} | port_name={d['port_name']} | "
          f"port_country={d['port_country']} | in_port={d['is_in_port_zone']} | dist_km={None if d['distance_to_port_km'] is None else round(d['distance_to_port_km'], 1)} | "
          f"sog={d['speed_over_ground']} | cog={d['course_over_ground']} | heading={d['heading']} | nav={d['navigational_status']} | "
          f"dspeed={d['speed_change_rate']} | dheading={d['heading_change_degrees']}")

ok = True
def check(c, m):
    global ok
    print(("PASS " if c else "FAIL ") + m); ok = ok and c

rows = {(r["mmsi"], str(r["event_timestamp"])[:16]): r.asDict() for r in out.collect()}
b = rows[("224123456", "2026-09-27 10:00")]; ny = rows[("366123456", "2026-09-27 10:00")]; sh = rows[("412123456", "2026-09-27 10:00")]
oc1 = rows[("277123456", "2026-09-27 10:00")]; oc2 = rows[("277123456", "2026-09-27 10:01")]; ro = rows[("999999999", "2026-09-27 10:00")]
check("port_name" in cols and "port_country" in cols and "nearest_port" not in cols and "eez_country" not in cols, "renamed columns present, old ones gone")
check((b["port_name"], b["port_country"], b["flag_country"], b["ocean_region"]) == ("Barcelona", "ES", "ES", "mediterranean"), "Barcelona: port, country, flag, Mediterranean")
check((ny["port_name"], ny["port_country"], ny["flag_country"], ny["ocean_region"]) == ("New York", "US", "US", "atlantic_ocean"), "New York: Atlantic, not Pacific")
check((sh["port_name"], sh["flag_country"], sh["ocean_region"]) == ("Shanghai", "CN", "pacific_west"), "Shanghai: Pacific")
check(ny["speed_over_ground"] is None and ny["course_over_ground"] is None and ny["heading"] is None, "AIS not-available SOG 102.3 / COG 360 / heading 511 -> null")
check(b["speed_over_ground"] == 12.5 and b["course_over_ground"] == 90.0, "valid speed and course kept")
check(oc1["port_name"] is None and oc1["port_country"] is None and oc1["is_in_port_zone"] is False and oc1["distance_to_port_km"] is None, "open ocean: no port columns")
check(oc1["flag_country"] == "LT" and oc1["ocean_region"] == "atlantic_ocean", "MID 277 -> LT (was wrongly LV)")
check(oc2["speed_change_rate"] == 15.0 and oc2["heading_change_degrees"] == 50.0 and oc1["speed_change_rate"] is None, "deltas: 10->25 kn = 15, 10->60 deg = 50, first row null")
check(ro["flag_country"] is None and ro["port_name"] == "Rotterdam" and ro["port_country"] == "NL", "unmapped MID -> null flag; Rotterdam port zone")
check((b["navigational_status"], ny["navigational_status"], sh["navigational_status"], oc1["navigational_status"], ro["navigational_status"]) ==
      ("under_way_engine", "moored", "at_anchor", "ais_sart_active", "towing_astern"), "navigation status labels (incl. new codes 11 and 14)")
print("\nALL CHECKS PASSED" if ok else "\nSOME CHECKS FAILED")
sys.exit(0 if ok else 1)
