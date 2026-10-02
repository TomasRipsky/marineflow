# scripts/

## `jobs.sh` — run the five Spark jobs detached and supervised

Instead of five terminals with `docker exec -it ... spark-submit`, one command starts every streaming job in the background, one at a time, and restarts any job that dies. Run everything from the repository root, with the stack already up (`docker compose up -d`).

| Job name | Container | Script | Reads | Writes |
|---|---|---|---|---|
| `bronze` | `marineflow-spark-bronze` | `bronze_positions.py` | `vessel-positions` | GCS bronze + `vessel-positions-bronze` |
| `bronze-metadata` | `marineflow-spark-bronze-metadata` | `bronze_metadata.py` | `vessel-metadata` | GCS bronze + `vessel-metadata-bronze` |
| `silver-positions` | `marineflow-spark-silver-positions` | `silver_positions.py` | `vessel-positions-bronze` | GCS silver |
| `silver-metadata` | `marineflow-spark-silver-metadata` | `silver_metadata.py` | `vessel-metadata-bronze` | GCS silver |
| `hot-alerts` | `marineflow-spark-hot-alerts` | `hot_alerts.py` | `vessel-positions-bronze` | `vessel-alerts` |

### Commands

```bash
scripts/jobs.sh start                      # all five, one every 20 s
scripts/jobs.sh start silver-positions     # only the jobs you name
scripts/jobs.sh status                     # who is alive, memory, OOM kills, restarts, producer
scripts/jobs.sh logs bronze                # follow one job's log (Ctrl+C stops watching, not the job)
scripts/jobs.sh stop                       # clean stop of everything
scripts/jobs.sh stop hot-alerts            # clean stop of one job
```

Optional environment variables, written before the command:

| Variable | Default | Meaning |
|---|---|---|
| `STAGGER` | `20` | Seconds between job starts. Five JVMs starting together spike memory and CPU |
| `RESTART_DELAY` | `15` | Seconds before a dead job is restarted |
| `DRIVER_MEMORY` | `1g` | `--driver-memory` for the jobs you start |

```bash
DRIVER_MEMORY=512m scripts/jobs.sh start bronze-metadata silver-metadata
```

### Reading `status`

```
Docker VM memory: 11.7 GiB total
JOB                SUPERVISOR SPARK      MEMORY    OOMKILLED RESTARTS
bronze             yes        up         1.2GiB    false     0
...
Producer: running
```

| Column | Meaning |
|---|---|
| `SUPERVISOR` | `yes` when the restart loop is running. `no` after `stop`, or if you never started it |
| `SPARK` | `up` when the Spark JVM is alive, `DOWN` while it is dead or being restarted |
| `MEMORY` | What `docker stats` reports for the container |
| `OOMKILLED` | Docker's flag for the container. `true` means the kernel killed something for lack of memory |
| `RESTARTS` | How many times the job has been restarted since the container was created |

A healthy run shows `yes / up / false` for all five. A job that flickers between `up` and `DOWN` with a growing `RESTARTS` is being killed again and again: the Docker VM is too small.

### Where the logs are

Inside each container, in `/tmp/job.log` (`scripts/jobs.sh logs <job>` tails it). The supervisor writes one marker line per event:

```
--- supervisor started 2026-10-02T12:24:18Z
--- bronze_positions.py exited with 137 at 2026-10-02T12:24:23Z; restarting in 15s
```

Exit code **137** means the process received SIGKILL, which is what the Linux OOM killer sends. A job that exits with another code is crashing on its own: read the lines above the marker. The log lives as long as the container; recreating the container (`docker compose up --force-recreate`) clears it.

### How it works

1. `start` checks that the container is running and that nothing is supervised or hand-started there.
2. It runs `docker exec -d <container> sh -c '...'` with a small loop: run `spark-submit`, append its output to `/tmp/job.log`, wait `RESTART_DELAY`, run it again. The loop carries a `marineflow-loop:<job>` marker so `status` and `stop` can find it.
3. The `spark-submit` flags are exactly the ones in the README template (GCS jars and Prometheus metrics for the four GCS-writing jobs; only the Kafka package for `hot-alerts`). `tests/test_jobs_script.py` keeps them identical.
4. Restarting is safe because every job resumes from its checkpoint (GCS for the four GCS-writing jobs, a Docker volume for `hot-alerts`).
5. `stop` kills the loop first (otherwise it would restart the job) and then sends SIGTERM to the Spark process, the same clean stop as Ctrl+C.

### What it does not do

- **It does not fix a VM that is too small.** All five jobs plus Kafka, Airflow and Grafana need roughly 9 GiB; Docker Desktop defaults to 7.75 GiB. Raise it first (Docker Desktop → Settings → Resources → Memory, 11–12 GiB on a 16 GiB Mac). See "Before you start" in [FRESH_START.md](../FRESH_START.md).
- **It does not start the producer.** That one runs on the host: `cd ingestion/ais_producer && python main.py`. `status` only reports whether it is running.
- **It does not survive `docker compose down` or a Docker restart.** The containers go back to `sleep infinity`; run `start` again.
- **It does not mix with hand-started jobs.** If a job is already running in a terminal, `start` refuses; stop it with Ctrl+C first.

### Troubleshooting

| Symptom | Likely cause and what to do |
|---|---|
| `container ... is not running` | The stack is down. `docker compose up -d` and wait for the containers |
| `a spark-submit started by hand is running` | Ctrl+C that terminal, then `start` again |
| `RESTARTS` keeps growing for one job | Look at its log: exit 137 means out of memory (raise the VM memory, or `DRIVER_MEMORY=...` for the small jobs); another code is a crash, read the traceback above the marker |
| `status` shows `Producer: NOT running` | Start it on the host. The jobs run fine without it, but there is nothing to process |
| Grafana says fewer than 4 "Spark jobs up" although `status` shows `up` | The Spark UI port that Prometheus scrapes opens 30 to 90 s after the JVM starts (it first resolves the Kafka package). The tile recovers by itself |
