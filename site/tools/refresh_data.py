"""Refresh site/assets/data.js from the real BigQuery tables (read-only queries).

    python3 site/tools/refresh_data.py            # needs the `bq` CLI and gcloud credentials
    python3 site/tools/refresh_data.py --project my-project

Every number on the site's "results" sections comes from here, so each one can be traced to a query below.
Vessels are shown without names and with the MMSI masked (first six digits only): the site shows what the
pipeline found, not a list of ships to look up, and a risk score is a pipeline output, not an accusation.
"""
import argparse
import datetime
import json
import pathlib
import subprocess
import sys

OUT = pathlib.Path(__file__).resolve().parents[1] / "assets" / "data.js"


def make_runner(project):
    def run(sql):
        cmd = ["bq", f"--project_id={project}", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=5000", sql]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            sys.exit(f"bq failed:\n{result.stderr}\nSQL: {sql}")
        return json.loads(result.stdout or "[]")
    return run


def num(rows, *keys):
    """bq returns every value as a string; convert the numeric ones."""
    out = []
    for row in rows:
        item = dict(row)
        for key in keys:
            if item.get(key) is not None:
                item[key] = float(item[key]) if "." in item[key] else int(item[key])
        out.append(item)
    return out


def mask(mmsi):
    return str(mmsi)[:6] + "***"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="marineflow-489815")
    args = parser.parse_args()
    run = make_runner(args.project)
    silver = f"`{args.project}.marineflow_silver.vessel_positions_clean`"
    gold = f"`{args.project}.marineflow_gold"

    data = {"generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}

    data["silver"] = num(run(f"""
        SELECT COUNT(*) positions, COUNT(DISTINCT mmsi) vessels, COUNT(DISTINCT flag_country) flags,
               COUNTIF(is_in_port_zone) in_port,
               CAST(MIN(event_timestamp) AS STRING) first_ts, CAST(MAX(event_timestamp) AS STRING) last_ts
        FROM {silver}"""), "positions", "vessels", "flags", "in_port")[0]

    data["gold"] = num(run(f"""
        SELECT COUNT(*) vessel_days, ROUND(SUM(estimated_distance_km)) km, SUM(sharp_turns) sharp_turns,
               SUM(sudden_speed_changes) sudden_changes
        FROM {gold}.vessel_activity_summary`"""), "vessel_days", "km", "sharp_turns", "sudden_changes")[0]

    data["rows"] = {row["t"]: int(row["n"]) for row in run(f"""
        SELECT 'port_traffic' t, COUNT(*) n FROM {gold}.port_traffic` UNION ALL
        SELECT 'vessel_activity_summary', COUNT(*) FROM {gold}.vessel_activity_summary` UNION ALL
        SELECT 'vessel_dark_events', COUNT(*) FROM {gold}.vessel_dark_events` UNION ALL
        SELECT 'vessel_erratic_course', COUNT(*) FROM {gold}.vessel_erratic_course` UNION ALL
        SELECT 'vessel_loitering', COUNT(*) FROM {gold}.vessel_loitering` UNION ALL
        SELECT 'vessel_risk_score', COUNT(*) FROM {gold}.vessel_risk_score` UNION ALL
        SELECT 'vessel_speed_anomalies', COUNT(*) FROM {gold}.vessel_speed_anomalies`""")}

    data["flags"] = num(run(f"""
        SELECT flag_country k, COUNT(DISTINCT mmsi) n FROM {silver}
        WHERE flag_country IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10"""), "n")
    data["regions"] = num(run(f"SELECT ocean_region k, COUNT(*) n FROM {silver} GROUP BY 1 ORDER BY 2 DESC"), "n")
    data["types"] = num(run(f"""
        SELECT vessel_type k, COUNT(*) n FROM {gold}.vessel_activity_summary`
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8"""), "n")
    data["minutes"] = num(run(f"""
        SELECT FORMAT_TIMESTAMP('%H:%M', TIMESTAMP_TRUNC(event_timestamp, MINUTE)) t, COUNT(*) n
        FROM {silver} GROUP BY 1 ORDER BY 1"""), "n")
    data["grid"] = [[int(r["la"]), int(r["lo"]), int(r["n"])] for r in run(f"""
        SELECT CAST(ROUND(latitude / 2) * 2 AS INT64) la, CAST(ROUND(longitude / 2) * 2 AS INT64) lo, COUNT(*) n
        FROM {silver} WHERE latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 GROUP BY 1, 2""")]

    data["ports"] = num(run(f"""
        SELECT port_name, port_country, unique_vessels, total_position_reports, cargo_vessels, tanker_vessels,
               passenger_vessels, ROUND(avg_speed_in_port, 1) avg_speed
        FROM {gold}.port_traffic` ORDER BY total_position_reports DESC LIMIT 8"""),
        "unique_vessels", "total_position_reports", "cargo_vessels", "tanker_vessels", "passenger_vessels", "avg_speed")

    top = num(run(f"""
        SELECT mmsi, flag_country, vessel_type_normalized t, dark_events_30d dark,
               speed_anomalies_30d speed, spoofing_signals_30d spoof, loiter_events_30d loiter, risk_score
        FROM {gold}.vessel_risk_score` ORDER BY risk_score DESC, mmsi LIMIT 8"""),
        "dark", "speed", "spoof", "loiter", "risk_score")
    for row in top:
        row["mmsi"] = mask(row["mmsi"])
    data["top_risk"] = top
    data["risk_distribution"] = num(run(f"SELECT risk_score k, COUNT(*) n FROM {gold}.vessel_risk_score` GROUP BY 1 ORDER BY 1"), "k", "n")

    data["anomaly_buckets"] = num(run(f"""
        SELECT CASE WHEN calculated_speed_knots < 35 THEN '14-35 kn'
                    WHEN calculated_speed_knots < 100 THEN '35-100 kn'
                    WHEN calculated_speed_knots < 1000 THEN '100-1,000 kn'
                    ELSE 'over 1,000 kn' END k, COUNT(*) n, MIN(calculated_speed_knots) lo
        FROM {gold}.vessel_speed_anomalies` GROUP BY 1 ORDER BY lo"""), "n")
    data["anomaly_total"] = int(run(f"SELECT COUNT(*) n FROM {gold}.vessel_speed_anomalies`")[0]["n"])

    # The case file: the fastest "impossible speed" and the two positions that produced it.
    worst = run(f"""
        SELECT mmsi, CAST(event_timestamp AS STRING) ts, calculated_speed_knots
        FROM {gold}.vessel_speed_anomalies` ORDER BY calculated_speed_knots DESC LIMIT 1""")[0]
    pair = run(f"""
        SELECT CAST(event_timestamp AS STRING) ts, latitude, longitude, speed_over_ground sog, navigational_status status
        FROM {silver} WHERE mmsi = '{worst['mmsi']}'
          AND event_timestamp BETWEEN TIMESTAMP_SUB(TIMESTAMP '{worst['ts']}', INTERVAL 60 SECOND) AND TIMESTAMP '{worst['ts']}'
        ORDER BY event_timestamp DESC LIMIT 2""")
    data["case"] = {"mmsi": mask(worst["mmsi"]), "speed_knots": round(float(worst["calculated_speed_knots"])),
                    "positions": [{"ts": p["ts"], "lat": round(float(p["latitude"]), 3), "lon": round(float(p["longitude"]), 3),
                                   "sog": float(p["sog"]) if p["sog"] is not None else None, "status": p["status"]} for p in reversed(pair)]}

    OUT.write_text("// Generated by site/tools/refresh_data.py from the real BigQuery tables. Do not edit by hand.\n"
                   "window.MARINEFLOW = " + json.dumps(data, indent=1, ensure_ascii=False) + ";\n")
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes), {data['silver']['positions']} positions")


if __name__ == "__main__":
    main()
