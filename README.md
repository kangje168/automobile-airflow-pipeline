# Automobile Airflow Pipeline

This repository contains a forked Airflow prototype and a cleaner, runnable case study built around the same scenario.

- `automobile-airflow-pipeline/` contains the inherited prototype.
- `airflow-case-v2/` is a clean rebuild for demonstrating a complete, testable Airflow workflow.

The rebuilt case study processes synthetic vehicle, repair and sensor data. It validates inputs, stages them per DAG run, builds features, scores unusual claims with a reproducible Isolation Forest plus transparent review rules, and writes a report for human review. It is a learning/interview demo, not a production fraud decision system.

## Run the rebuilt case

See [airflow-case-v2/README.md](airflow-case-v2/README.md). The local stack uses Airflow 2.10.3, PostgreSQL and LocalExecutor.

## Provenance and scope

This repository began as a fork. The inherited code remains available for comparison; the rebuilt case is a separate implementation and does not claim the inherited code as original work. Preserve the upstream attribution and license when redistributing the fork. The demo uses synthetic data and must not be used to make real insurance decisions.
