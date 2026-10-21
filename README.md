# Automobile Repair Airflow Case Study

This repository contains the inherited prototype and a rebuilt, testable Airflow case study.

| Directory | Purpose |
|---|---|
| [`airflow-case-study/`](airflow-case-study/README.md) | Primary runnable case study: an Airflow claim-triage workflow using synthetic data, with tests and a local Docker Compose stack. |
| [`prototype/`](prototype/README.md) | Original forked prototype, preserved for context and comparison. |

## Run the case study

See [`airflow-case-study/README.md`](airflow-case-study/README.md) for setup, execution, outputs and tests. The local stack uses Airflow 2.10.3, PostgreSQL and LocalExecutor.

## Scope and provenance

The rebuilt workflow is a learning and interview demo. Its inputs are synthetic, and its scores are triage signals for human review—not validated fraud findings. It does not demonstrate production MWAA operations or large-scale platform tuning.

The repository began as a fork. The original prototype files are preserved under `prototype/`; the earlier repository overview is retained as [`prototype/README.original.md`](prototype/README.original.md). The rebuild is a separate implementation and does not claim the inherited code as original work.
