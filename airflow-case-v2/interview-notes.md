# Interview walkthrough

## 30-second summary

I rebuilt a small inherited Airflow prototype into a reproducible claim-triage workflow. The DAG validates four input tables, quarantines bad rows, builds time-aware features, scores new claims with a seeded anomaly model plus visible rules, and publishes a review queue. I focused on run isolation, safe retries, data quality, and tests. The data and model are synthetic; this is a platform-engineering case study, not a production fraud system.

## What I can demonstrate

- Airflow task boundaries and a small `TaskGroup`, with business logic kept outside the DAG file.
- Lazy imports in task callables so DAG parsing does not import pandas or scikit-learn.
- Schema checks, row quarantine, referential checks, and input hashes in a manifest.
- Run-specific artifact paths, atomic file replacement, and deterministic same-run retries.
- A local Airflow 2.10.3 + PostgreSQL + LocalExecutor stack, unit tests, and a DagBag import test.
- A review report that retains the model signal, rule signals, thresholds, and reason codes for human inspection.

## Scope to state clearly

- The source repository was forked; the `airflow-case-v2/` rebuild is the work to present.
- The example does not demonstrate MWAA in production, AWS IAM/SQS/CloudWatch operations, a 200+ DAG estate, migration leadership, or cost savings.
- The model has no labeled evaluation set and must not be described as a validated fraud detector.
- I have Airflow experience from data-engineering projects, including GCP pipelines and Luxembourg's Ministry of the Interior Odoo platform; the AWS/MWAA specifics would need to be validated with the client.

## How I would extend this for a production platform

1. Establish workload facts first: DAG count, parse time, task volume, peak concurrency, SLAs, failure history, data size, and current operating costs.
2. Replace shared local files with the client's approved durable storage and define retention, encryption, and access policies.
3. Integrate the platform's secrets, logs, metrics, alerts, deployment pipeline, and incident runbooks using the client's AWS standards.
4. Benchmark the current managed service and any alternatives against the same representative workload before recommending a migration.
5. Roll out incrementally with a rollback path, production acceptance criteria, and hands-on transfer to the Dev and Ops teams.

These are proposed next steps, not features implemented by this demo.
