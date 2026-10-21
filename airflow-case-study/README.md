# Automobile Claim Triage — Airflow Case Study

A small, reproducible Airflow workflow that turns synthetic vehicle, repair and sensor records into a review queue for unusual insurance claims.

This is a clean rebuild of the inherited prototype in the parent repository. It is designed to demonstrate data orchestration practices, not production-scale MWAA operations or a validated fraud model.

## What the DAG does

1. **Validate and stage** source CSVs. Schema errors fail fast; malformed rows are written to a quarantine file. Cross-table identifiers are checked, input SHA-256 hashes are recorded, and staged data is isolated by Airflow run.
2. **Build features** by joining repairs to vehicle attributes and sensor alerts, and counting prior repairs in a 30-day window.
3. **Score claims** with a seeded Isolation Forest trained on the historical sample, plus explicit rules for unusually high costs, repeated repairs and sensor alerts.
4. **Publish a review report** with scores, reason codes and a summary. The model is a triage signal; a person decides what to investigate.

XCom carries only a run key. Data moves through run-scoped files, so retries are repeatable and parallel DAG runs do not overwrite one another. Writes use temporary files and atomic replacement; the quarantine artifact is replaced even when empty, preventing stale output on a same-run rerun.

## Start locally

Requires Docker Compose.

```bash
cd airflow-case-study
mkdir -p logs data/output
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up --build airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Open http://localhost:8080 and sign in with `airflow` / `airflow` (local demo credentials only). Trigger `automobile_claim_triage` manually.

Outputs appear under `data/output/<run-key>/`:
- `staged/`: validated source tables and a manifest
- `features/`: historical and incoming feature tables
- `scores/claim_scores.csv`: score and reason codes for each incoming claim
- `report/alerts.csv` and `report/summary.json`: review queue and run summary

Stop the stack with `docker compose down`. Remove the local database with `docker compose down -v`.

## Run tests

```bash
docker compose run --rm airflow-webserver bash -c "python -m pytest -q"
```

The unit tests cover input validation, quarantine behavior, deterministic retries, scoring output and run isolation. The DAG import test uses Airflow's DagBag. The root-level GitHub Actions workflow builds the same container and runs these checks for pull requests that change this case study.

## Configuration

The demo reads from `data/input/` and writes to `data/output/` inside the container. Change these paths with `PIPELINE_INPUT_DIR` and `PIPELINE_OUTPUT_DIR` in Compose.

The bundled input is synthetic and deterministic. The model is trained on the small historical sample on each run to keep the demo self-contained. For real use, model training and model deployment would be separate governed workflows, with a versioned model artifact, representative labeled data, monitoring and human review.

## Portfolio and interview framing

This is a fork-based case study with a clean rebuild under `airflow-case-study/`. Describe it as a workflow you rebuilt and tested, and be precise about which changes are yours. It is useful for discussing Airflow task boundaries, data quality, run isolation, retries, and testability. It does not prove production MWAA operations, large-scale scheduler tuning, or fraud-model performance. See [interview-notes.md](interview-notes.md) for a concise walkthrough and honest scope boundaries.

## Architecture boundary

This case demonstrates DAG structure, task boundaries, run-scoped artifacts, basic data quality, retries, tests and local reproducibility. It does not demonstrate MWAA provisioning, IAM/SQS/SNS integration, multi-tenant isolation, high availability, 200+ DAG performance, cost benchmarks or production observability. See [architecture.md](architecture.md) for the design and limitations.

## Provenance

The parent repository is a fork. This directory is a separate rebuild of the scenario and does not claim the inherited implementation as original work. Preserve the upstream project's attribution and license when redistributing the fork.
