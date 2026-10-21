from pathlib import Path

from airflow.models import DagBag


def test_dag_imports_without_errors() -> None:
    dags_dir = Path(__file__).resolve().parents[1] / "dags"
    dag_bag = DagBag(dag_folder=str(dags_dir), include_examples=False)

    assert dag_bag.import_errors == {}
    assert "automobile_claim_triage" in dag_bag.dags
