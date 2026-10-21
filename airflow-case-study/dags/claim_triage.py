"""Airflow DAG for the synthetic automobile claim-triage case study."""

from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def _run_key(context):
    from claim_pipeline.pipeline import make_run_key

    return make_run_key(context["run_id"])


def _validate(**context):
    from claim_pipeline.pipeline import validate_sources

    key = _run_key(context)
    return validate_sources(
        input_dir=os.environ["PIPELINE_INPUT_DIR"],
        output_dir=os.environ["PIPELINE_OUTPUT_DIR"],
        run_key=key,
    )


def _features(**context):
    from claim_pipeline.pipeline import build_features

    key = _run_key(context)
    return build_features(
        output_dir=os.environ["PIPELINE_OUTPUT_DIR"],
        run_key=key,
    )


def _score(**context):
    from claim_pipeline.pipeline import score_claims

    key = _run_key(context)
    return score_claims(
        output_dir=os.environ["PIPELINE_OUTPUT_DIR"],
        run_key=key,
    )


def _publish(**context):
    from claim_pipeline.pipeline import publish_report

    key = _run_key(context)
    return publish_report(
        output_dir=os.environ["PIPELINE_OUTPUT_DIR"],
        run_key=key,
    )


with DAG(
    dag_id="automobile_claim_triage",
    description="Validate, score and publish a human-review queue for synthetic claims.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    default_args={
        "owner": "data-platform-demo",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["demo", "claims", "data-quality", "ml"],
) as dag:
    validate = PythonOperator(
        task_id="validate_and_stage_sources",
        python_callable=_validate,
    )

    with TaskGroup(group_id="claim_triage") as triage:
        features = PythonOperator(
            task_id="build_features",
            python_callable=_features,
        )
        score = PythonOperator(
            task_id="score_claims",
            python_callable=_score,
        )
        features >> score

    publish = PythonOperator(
        task_id="publish_review_report",
        python_callable=_publish,
    )

    validate >> triage >> publish
