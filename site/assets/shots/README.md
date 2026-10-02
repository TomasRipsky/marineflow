# Dashboard screenshots

The site's *Dashboards* section shows a file from this folder when it exists and a "screenshot pending" frame when it does not.
Drop real captures here (PNG, about 1600 px wide) with exactly these names:

| File | What to capture |
|---|---|
| `overview.png` | Grafana, *MarineFlow: Overview* |
| `live-traffic.png` | Grafana, *MarineFlow: Live traffic* (with the producer running) |
| `spark.png` | Grafana, *MarineFlow: Spark streaming* |
| `airflow.png` | Airflow, the `marineflow_pipeline` graph view with a green run |

Capture them from a run with all five Spark jobs and the producer up, so no panel says "no data".
