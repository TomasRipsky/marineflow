# Working on MarineFlow

How changes are made here, and how the documentation is kept true. Short on purpose: the reasons are in the [README](README.md).

## The routine

1. **One branch per change** (`feat/…`, `fix/…`, `docs/…`), cut from an up-to-date `main`.
2. **Validate before merging.** Run the checks below. Say plainly what could not be verified (for example anything that needs the real GCP project).
3. **Merge with `--no-ff` into `main`, then delete the branch.** `main` is always green: CI runs on every push (`ci.yml`), and a push to `main` that touches `site/` is published by `pages.yml`.
4. **Ask before anything destructive or costly.** `docker compose down -v`, deleting Kafka topics, `gcloud storage rm`, `terraform apply`, `dbt run --full-refresh`. GCS and BigQuery are a real project. [FRESH_START.md](FRESH_START.md) is a runbook to read, not a script to paste blindly.
5. **Never commit secrets** and do not read `.env`, `terraform.tfvars` or the credentials file in tooling. `.env.example` documents every variable.

## The checks

```bash
python -m unittest discover -s tests                     # or: python -m pytest -q
python -m compileall -q processing ingestion orchestration tests
(cd transformation/dbt && ../../venv/bin/dbt parse --profiles-dir .)
(cd infra/terraform && terraform validate)
python3 monitoring/grafana/generate_dashboards.py --check
node --check site/tools/capture_shots.mjs
```

The Spark smoke tests (`tests/smoke/`) need Docker and the project image; run them when a streaming job or the compaction changes. Each file's docstring has the exact command.

## Documentation standards

- **English** for everything in the repository: code, comments, logs, READMEs.
- **The code is the source of truth.** A document that disagrees with the code is the bug. Re-read a job's header and docstrings when you edit it.
- **Numbers are tested, not typed.** The unit-test count, services, topics, gold models, DAG tasks, dbt tests and reference-table sizes appear in the README, the architecture diagram and the site; `tests/test_readme.py` and `tests/test_site.py` recompute them. Adding a test means updating those counts (the failing test prints the new number).
- **Generated files are never edited by hand:** the Grafana JSON (`monitoring/grafana/generate_dashboards.py`), the site's data (`site/tools/refresh_data.py`) and its screenshots (`site/tools/capture_shots.mjs`). Change the generator, rerun it, commit both.
- **Say what a thing measures, and what it does not.** Name a tile after the number it shows, not the cause you suspect. State limits in the README's *Known limitations* and what is verified in *What is verified, and what is not*.
- **Vessels stay anonymous in anything public:** no names, MMSI masked (first six digits), the MMSI column blurred in screenshots.
- **An open problem gets a written plan** in `docs/plans/` with its evidence, cause, ordered steps and acceptance check, and a line in *Known limitations*.
- **A problem found by testing goes in the README's engineering log** with how it was found and how it was fixed.
- **One README per area that has commands** (`scripts/`, `site/assets/shots/`); the top-level README links to them from *Where to read what*.

## Adding things

- **A Spark job:** follow the skeleton the four streaming jobs share (structlog console logger, `load_dotenv()`, config at the top, lazy `pyspark` imports, `validate_config()`, `foreachBatch` with a 30-second trigger, a `maxOffsetsPerTrigger` read from the environment, a GCS checkpoint). A job whose source changes gets a **new checkpoint path**. Add it to `scripts/jobs.sh` and `docker-compose.yml`.
- **A gold model:** give it a surrogate key and tests in `models/gold/schema.yml`, and add it to the DAG (a test fails if the DAG does not run every model). If it is incremental, the cutoff over its own table needs a `coalesce(…, timestamp('1970-01-01'))` fallback.
- **A column in a Silver table:** add it to the job, to the Terraform external schema (`terraform apply` is the owner's), and to the dbt source; the schema-alignment test fails if one side is missing.
- **A dashboard panel:** edit the generator. Guard every division (`(x > 0)`), exclude Kafka's internal topics, and avoid anything with its own scrollbar.
