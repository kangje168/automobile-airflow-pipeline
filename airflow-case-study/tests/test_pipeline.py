from pathlib import Path
import shutil

import pandas as pd
import pytest

from claim_pipeline.pipeline import (
    build_features,
    make_run_key,
    publish_report,
    score_claims,
    validate_sources,
)


PROJECT_DIR = Path(__file__).resolve().parents[1]
SAMPLE_INPUT = PROJECT_DIR / "data" / "input"


def run_case(input_dir: Path, output_dir: Path, run_id: str) -> Path:
    key = make_run_key(run_id)
    validate_sources(str(input_dir), str(output_dir), key)
    build_features(str(output_dir), key)
    score_claims(str(output_dir), key)
    report_dir = Path(publish_report(str(output_dir), key))
    return report_dir


def test_end_to_end_creates_reviewable_outputs(tmp_path: Path) -> None:
    report_dir = run_case(SAMPLE_INPUT, tmp_path / "out", "manual__test-1")

    scores = pd.read_csv(report_dir.parent / "scores" / "claim_scores.csv")
    alerts = pd.read_csv(report_dir / "alerts.csv")
    summary = pd.read_json(report_dir / "summary.json", typ="series")

    assert len(scores) == 5
    assert {"model_anomaly_score", "reason_codes", "review_score"} <= set(scores.columns)
    assert not alerts.empty
    assert set(alerts["repair_id"]) == {"C102", "C103", "C104"}
    reasons = alerts.set_index("repair_id")["reason_codes"]
    assert "repeated_repairs_30d" in reasons["C102"]
    assert "sensor_threshold_breach" in reasons["C103"]
    assert "unusual_cost" in reasons["C104"]
    assert summary["incoming_claims"] == 5
    assert summary["claims_recommended_for_review"] == len(alerts)
    assert "human must review" in summary["decision"]


def test_invalid_repair_is_quarantined(tmp_path: Path) -> None:
    input_copy = tmp_path / "input"
    shutil.copytree(SAMPLE_INPUT, input_copy)
    incoming = pd.read_csv(input_copy / "incoming_repairs.csv")
    incoming.loc[0, "parts_cost"] = -10
    incoming.to_csv(input_copy / "incoming_repairs.csv", index=False)

    key = make_run_key("manual__quarantine")
    validate_sources(str(input_copy), str(tmp_path / "out"), key)

    staged = pd.read_csv(tmp_path / "out" / key / "staged" / "incoming_repairs.csv")
    quarantine = pd.read_csv(tmp_path / "out" / key / "quarantine.csv")
    manifest = pd.read_json(tmp_path / "out" / key / "staged" / "manifest.json", typ="series")

    assert len(staged) == 4
    assert len(quarantine) == 1
    assert "negative_value" in quarantine.loc[0, "rejection_reasons"]
    assert manifest["quarantined_rows"] == 1


def test_quarantine_is_replaced_when_same_run_is_reprocessed(tmp_path: Path) -> None:
    input_copy = tmp_path / "input"
    shutil.copytree(SAMPLE_INPUT, input_copy)
    incoming_path = input_copy / "incoming_repairs.csv"
    incoming = pd.read_csv(incoming_path)
    incoming.loc[0, "parts_cost"] = -10
    incoming.to_csv(incoming_path, index=False)

    key = make_run_key("manual__retry")
    output = tmp_path / "out"
    validate_sources(str(input_copy), str(output), key)
    assert len(pd.read_csv(output / key / "quarantine.csv")) == 1

    incoming.loc[0, "parts_cost"] = 135
    incoming.to_csv(incoming_path, index=False)
    validate_sources(str(input_copy), str(output), key)
    quarantine = pd.read_csv(output / key / "quarantine.csv")
    manifest = pd.read_json(output / key / "staged" / "manifest.json", typ="series")

    assert quarantine.empty
    assert manifest["quarantined_rows"] == 0


def test_duplicate_repair_ids_across_sources_fail(tmp_path: Path) -> None:
    input_copy = tmp_path / "input"
    shutil.copytree(SAMPLE_INPUT, input_copy)
    incoming_path = input_copy / "incoming_repairs.csv"
    incoming = pd.read_csv(incoming_path)
    incoming.loc[0, "repair_id"] = "H001"
    incoming.to_csv(incoming_path, index=False)

    with pytest.raises(ValueError, match="both historical and incoming"):
        validate_sources(str(input_copy), str(tmp_path / "out"), make_run_key("duplicate"))


def test_missing_required_column_fails_fast(tmp_path: Path) -> None:
    input_copy = tmp_path / "input"
    shutil.copytree(SAMPLE_INPUT, input_copy)
    incoming = pd.read_csv(input_copy / "incoming_repairs.csv").drop(columns=["labor_cost"])
    incoming.to_csv(input_copy / "incoming_repairs.csv", index=False)

    key = make_run_key("manual__bad-schema")
    with pytest.raises(ValueError, match="missing required columns"):
        validate_sources(str(input_copy), str(tmp_path / "out"), key)


def test_run_keys_are_safe_and_isolated() -> None:
    first = make_run_key("manual__2026-04-01T12:00:00+00:00")
    second = make_run_key("manual__2026-04-02T12:00:00+00:00")

    assert "/" not in first
    assert first != second


def test_separate_runs_do_not_overwrite_each_other(tmp_path: Path) -> None:
    first_dir = run_case(SAMPLE_INPUT, tmp_path / "out", "manual__run-A")
    second_dir = run_case(SAMPLE_INPUT, tmp_path / "out", "manual__run-B")

    assert first_dir != second_dir
    assert (first_dir / "summary.json").exists()
    assert (second_dir / "summary.json").exists()


def test_retrying_same_run_is_deterministic(tmp_path: Path) -> None:
    first = run_case(SAMPLE_INPUT, tmp_path / "out", "manual__stable-run")
    first_scores = (first.parent / "scores" / "claim_scores.csv").read_bytes()
    second = run_case(SAMPLE_INPUT, tmp_path / "out", "manual__stable-run")
    second_scores = (second.parent / "scores" / "claim_scores.csv").read_bytes()

    assert first == second
    assert first_scores == second_scores
