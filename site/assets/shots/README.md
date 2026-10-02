# Dashboard screenshots

**Regenerate them with one command** (Chrome and the running stack, with data flowing so no panel says "no data"):

```bash
node site/tools/capture_shots.mjs              # all five
node site/tools/capture_shots.mjs kafka spark  # only some
```

The script signs in with the local development logins, resizes the viewport to the full height of each page (a dashboard is taller than any screen, so a manual capture cuts it), waits for the panels to load and writes the PNGs here. The MMSI column of the Live traffic alerts table is blurred first: the site never shows a full vessel identifier. Settings (`WIDTH`, `SETTLE_MS`, logins, Chrome path) are documented at the top of the script.

The site's monitoring section shows a file from this folder when it exists and a "screenshot pending" frame when it does not. If you capture by hand, drop real captures here (PNG, about 1600 px wide, full page height, no full MMSI visible) with exactly these names:

| File | What to capture |
|---|---|
| `overview.png` | Grafana, *MarineFlow: Overview* |
| `live-traffic.png` | Grafana, *MarineFlow: Live traffic* (with the producer running) |
| `spark.png` | Grafana, *MarineFlow: Spark streaming* |
| `kafka.png` | Grafana, *MarineFlow: Kafka* (topics, rates, retained messages, topic inventory table) |
| `airflow.png` | Airflow, the `marineflow_pipeline` graph view with a green run |

Capture them from a run with all five Spark jobs and the producer up, so no panel says "no data".
