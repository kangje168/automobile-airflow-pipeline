"""Run-scoped data preparation, anomaly scoring and reporting.

Business data is exchanged through files, not XCom. All writes use a temporary
file followed by os.replace so task retries do not expose partial artifacts.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd


INPUT_FILES = {
    "historical_repairs": "historical_repairs.csv",
    "incoming_repairs": "incoming_repairs.csv",
    "vehicles": "vehicles.csv",
    "sensor_readings": "sensor_readings.csv",
}
REPAIR_COLUMNS = {
    "repair_id",
    "vehicle_id",
    "repair_date",
    "odometer_km",
    "parts_cost",
    "labor_cost",
}
FEATURE_COLUMNS = [
    "total_cost",
    "cost_per_1000km",
    "vehicle_age_years",
    "prior_repairs_30d",
    "sensor_alert_count",
]


def make_run_key(run_id: str) -> str:
    """Return a safe, stable directory key without exposing the full run id."""
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", run_id).strip("-")[:48] or "run"
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:10]
    return f"{slug}-{digest}"


def _atomic_csv(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", suffix=".tmp",
            dir=destination.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            frame.to_csv(handle, index=False)
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _atomic_json(payload: dict[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".tmp",
            dir=destination.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _read_csv(input_dir: Path, filename: str, required: set[str]) -> pd.DataFrame:
    path = input_dir / filename
    if not path.is_file():
        raise FileNotFoundError(f"Required input file not found: {path}")
    frame = pd.read_csv(path)
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{filename} is missing required columns: {missing}")
    if frame.empty:
        raise ValueError(f"{filename} contains no rows")
    return frame


def _validate_repairs(frame: pd.DataFrame, source_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quarantine invalid records while failing fast on schema errors."""
    clean = frame.copy()
    for column in ("repair_id", "vehicle_id"):
        clean[column] = clean[column].astype("string").str.strip()
    clean["repair_date"] = pd.to_datetime(clean["repair_date"], errors="coerce")
    for column in ("odometer_km", "parts_cost", "labor_cost"):
        clean[column] = pd.to_numeric(clean[column], errors="coerce")

    checks = {
        "missing_identifier": (
            clean["repair_id"].isna() | clean["repair_id"].eq("")
            | clean["vehicle_id"].isna() | clean["vehicle_id"].eq("")
        ),
        "invalid_date": clean["repair_date"].isna(),
        "invalid_numeric": (
            clean[["odometer_km", "parts_cost", "labor_cost"]].isna().any(axis=1)
            | ~clean[["odometer_km", "parts_cost", "labor_cost"]].apply(
                lambda column: column.map(math.isfinite)
            ).all(axis=1)
        ),
        "negative_value": (
            (clean[["odometer_km", "parts_cost", "labor_cost"]] < 0).any(axis=1)
        ),
    }
    reasons = []
    invalid = pd.Series(False, index=clean.index)
    for reason, mask in checks.items():
        invalid |= mask
        reasons.append((reason, mask))

    quarantine = clean.loc[invalid].copy()
    if not quarantine.empty:
        quarantine.insert(0, "source", source_name)
        quarantine["rejection_reasons"] = [
            "|".join(reason for reason, mask in reasons if bool(mask.loc[index]))
            for index in quarantine.index
        ]

    accepted = clean.loc[~invalid].copy()
    if accepted["repair_id"].duplicated().any():
        duplicates = accepted.loc[accepted["repair_id"].duplicated(keep=False), "repair_id"].tolist()
        raise ValueError(f"{source_name} contains duplicate repair_id values: {duplicates}")
    return accepted, quarantine


def _validate_dimensions(
    vehicles: pd.DataFrame, sensors: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    vehicles = vehicles.copy()
    vehicles["vehicle_id"] = vehicles["vehicle_id"].astype("string").str.strip()
    vehicles["manufacture_year"] = pd.to_numeric(vehicles["manufacture_year"], errors="coerce")
    if (
        vehicles["vehicle_id"].isna().any()
        or vehicles["vehicle_id"].eq("").any()
        or vehicles["vehicle_id"].duplicated().any()
    ):
        raise ValueError("vehicles.csv must have unique, non-null vehicle_id values")
    if (
        vehicles["manufacture_year"].isna().any()
        or not vehicles["manufacture_year"].map(math.isfinite).all()
        or (vehicles["manufacture_year"] < 1950).any()
    ):
        raise ValueError("vehicles.csv contains an invalid manufacture_year")

    sensors = sensors.copy()
    sensors["vehicle_id"] = sensors["vehicle_id"].astype("string").str.strip()
    for column in ("value", "threshold_min", "threshold_max"):
        sensors[column] = pd.to_numeric(sensors[column], errors="coerce")
    invalid = (
        sensors["vehicle_id"].isna()
        | sensors["vehicle_id"].eq("")
        | sensors["value"].isna()
        | sensors["threshold_min"].isna()
        | sensors["threshold_max"].isna()
        | ~sensors[["value", "threshold_min", "threshold_max"]].apply(
            lambda column: column.map(math.isfinite)
        ).all(axis=1)
        | (sensors["threshold_min"] >= sensors["threshold_max"])
        | ~sensors["vehicle_id"].isin(set(vehicles["vehicle_id"]))
    )
    quarantine = sensors.loc[invalid].copy()
    if not quarantine.empty:
        quarantine.insert(0, "source", "sensor_readings")
        quarantine["rejection_reasons"] = "missing_or_invalid_sensor_value_or_threshold"
    return vehicles, sensors.loc[~invalid].copy(), quarantine


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_dir(output_dir: Path, run_key: str) -> Path:
    return output_dir / run_key


def validate_sources(input_dir: str, output_dir: str, run_key: str) -> str:
    input_path = Path(input_dir)
    run_path = _run_dir(Path(output_dir), run_key)
    stage_path = run_path / "staged"

    history_raw = _read_csv(input_path, INPUT_FILES["historical_repairs"], REPAIR_COLUMNS)
    incoming_raw = _read_csv(input_path, INPUT_FILES["incoming_repairs"], REPAIR_COLUMNS)
    vehicles_raw = _read_csv(input_path, INPUT_FILES["vehicles"], {"vehicle_id", "manufacture_year"})
    sensors_raw = _read_csv(
        input_path,
        INPUT_FILES["sensor_readings"],
        {"vehicle_id", "sensor_type", "value", "threshold_min", "threshold_max"},
    )

    history, history_bad = _validate_repairs(history_raw, "historical_repairs")
    incoming, incoming_bad = _validate_repairs(incoming_raw, "incoming_repairs")
    duplicate_ids = set(history["repair_id"]) & set(incoming["repair_id"])
    if duplicate_ids:
        raise ValueError(
            "repair_id values appear in both historical and incoming data: "
            f"{sorted(duplicate_ids)}"
        )
    vehicles, sensors, sensor_bad = _validate_dimensions(vehicles_raw, sensors_raw)
    if history.empty or incoming.empty:
        raise ValueError("At least one valid historical and incoming repair is required")

    valid_vehicles = set(vehicles["vehicle_id"].astype(str))
    unknown = set(pd.concat([history["vehicle_id"], incoming["vehicle_id"]]).astype(str)) - valid_vehicles
    if unknown:
        raise ValueError(f"Repairs reference unknown vehicle_id values: {sorted(unknown)}")

    for key, frame in (
        ("historical_repairs", history),
        ("incoming_repairs", incoming),
        ("vehicles", vehicles),
        ("sensor_readings", sensors),
    ):
        _atomic_csv(frame, stage_path / INPUT_FILES[key])

    quarantined = pd.concat([history_bad, incoming_bad, sensor_bad], ignore_index=True, sort=False)
    if "source" not in quarantined.columns:
        quarantined["source"] = pd.Series(dtype="string")
    if "rejection_reasons" not in quarantined.columns:
        quarantined["rejection_reasons"] = pd.Series(dtype="string")
    # Replace this artifact even when there are no rejected rows, so a rerun
    # cannot leave stale quarantine data from an earlier attempt.
    _atomic_csv(quarantined, run_path / "quarantine.csv")

    _atomic_json(
        {
            "run_key": run_key,
            "accepted_rows": {
                "historical_repairs": len(history),
                "incoming_repairs": len(incoming),
                "vehicles": len(vehicles),
                "sensor_readings": len(sensors),
            },
            "quarantined_rows": len(quarantined),
            "input_sha256": {
                filename: _sha256(input_path / filename)
                for filename in INPUT_FILES.values()
            },
        },
        stage_path / "manifest.json",
    )
    return run_key


def _prior_repairs_count(row: pd.Series, all_repairs: pd.DataFrame) -> int:
    date = row["repair_date"]
    lower_bound = date - pd.Timedelta(days=30)
    mask = (
        (all_repairs["vehicle_id"].astype(str) == str(row["vehicle_id"]))
        & (all_repairs["repair_date"] < date)
        & (all_repairs["repair_date"] >= lower_bound)
    )
    return int(mask.sum())


def _make_features(
    repairs: pd.DataFrame,
    all_repairs: pd.DataFrame,
    vehicles: pd.DataFrame,
    sensors: pd.DataFrame,
) -> pd.DataFrame:
    result = repairs.copy()
    result["repair_date"] = pd.to_datetime(result["repair_date"])
    result["total_cost"] = result["parts_cost"] + result["labor_cost"]

    vehicle_lookup = vehicles.assign(vehicle_id=vehicles["vehicle_id"].astype(str)).set_index("vehicle_id")
    result["vehicle_id"] = result["vehicle_id"].astype(str)
    if not set(result["vehicle_id"]).issubset(set(vehicle_lookup.index)):
        raise ValueError("A repair references a vehicle absent from vehicles.csv")
    result["manufacture_year"] = result["vehicle_id"].map(vehicle_lookup["manufacture_year"])
    result["vehicle_age_years"] = (
        result["repair_date"].dt.year - result["manufacture_year"]
    ).clip(lower=0)

    sensor_frame = sensors.copy()
    sensor_frame["vehicle_id"] = sensor_frame["vehicle_id"].astype(str)
    alert_mask = (
        (sensor_frame["value"] < sensor_frame["threshold_min"])
        | (sensor_frame["value"] > sensor_frame["threshold_max"])
    )
    alert_counts = sensor_frame.loc[alert_mask].groupby("vehicle_id").size()
    result["sensor_alert_count"] = result["vehicle_id"].map(alert_counts).fillna(0).astype(int)

    all_repairs = all_repairs.copy()
    all_repairs["vehicle_id"] = all_repairs["vehicle_id"].astype(str)
    all_repairs["repair_date"] = pd.to_datetime(all_repairs["repair_date"])
    result["prior_repairs_30d"] = result.apply(
        lambda row: _prior_repairs_count(row, all_repairs), axis=1
    )
    result["cost_per_1000km"] = result["total_cost"] / (
        result["odometer_km"].astype(float).div(1000).clip(lower=1)
    )
    return result


def build_features(output_dir: str, run_key: str) -> str:
    run_path = _run_dir(Path(output_dir), run_key)
    stage = run_path / "staged"
    history = pd.read_csv(stage / INPUT_FILES["historical_repairs"])
    incoming = pd.read_csv(stage / INPUT_FILES["incoming_repairs"])
    vehicles = pd.read_csv(stage / INPUT_FILES["vehicles"])
    sensors = pd.read_csv(stage / INPUT_FILES["sensor_readings"])
    all_repairs = pd.concat([history, incoming], ignore_index=True)

    # Keep future incoming records out of historical rolling features.
    history_features = _make_features(history, history, vehicles, sensors)
    incoming_features = _make_features(incoming, all_repairs, vehicles, sensors)
    feature_path = run_path / "features"
    _atomic_csv(history_features, feature_path / "historical_features.csv")
    _atomic_csv(incoming_features, feature_path / "incoming_features.csv")
    return run_key


def score_claims(output_dir: str, run_key: str) -> str:
    from sklearn.ensemble import IsolationForest

    run_path = _run_dir(Path(output_dir), run_key)
    history = pd.read_csv(run_path / "features" / "historical_features.csv")
    incoming = pd.read_csv(run_path / "features" / "incoming_features.csv")
    if len(history) < 5:
        raise ValueError("Isolation Forest needs at least five historical examples in this demo")

    model = IsolationForest(
        n_estimators=100,
        contamination="auto",
        random_state=42,
    )
    model.fit(history[FEATURE_COLUMNS])
    scored = incoming.copy()
    scored["model_anomaly_score"] = -model.decision_function(incoming[FEATURE_COLUMNS])
    scored["model_flagged"] = model.predict(incoming[FEATURE_COLUMNS]) == -1

    median_cost = float(history["total_cost"].median())
    median_absolute_deviation = float((history["total_cost"] - median_cost).abs().median())
    high_cost_limit = median_cost + max(4 * median_absolute_deviation, median_cost, 500.0)
    scored["high_cost_limit"] = high_cost_limit

    scored["unusual_cost"] = scored["total_cost"] > high_cost_limit
    scored["repeated_repairs"] = scored["prior_repairs_30d"] >= 3
    scored["sensor_alerts"] = scored["sensor_alert_count"] > 0

    scored["review_score"] = (
        scored["model_flagged"].astype(int) * 40
        + scored["unusual_cost"].astype(int) * 30
        + scored["repeated_repairs"].astype(int) * 20
        + scored["sensor_alerts"].astype(int) * 10
    ).clip(upper=100)
    scored["review_recommended"] = scored["review_score"] > 0

    def reasons(row: pd.Series) -> str:
        labels = []
        if row["model_flagged"]:
            labels.append("model_outlier")
        if row["unusual_cost"]:
            labels.append("unusual_cost")
        if row["repeated_repairs"]:
            labels.append("repeated_repairs_30d")
        if row["sensor_alerts"]:
            labels.append("sensor_threshold_breach")
        return "|".join(labels)

    scored["reason_codes"] = scored.apply(reasons, axis=1)
    scored = scored.sort_values(
        ["review_score", "model_anomaly_score"], ascending=[False, False]
    )
    score_dir = run_path / "scores"
    _atomic_csv(scored, score_dir / "claim_scores.csv")
    return run_key


def publish_report(output_dir: str, run_key: str) -> str:
    run_path = _run_dir(Path(output_dir), run_key)
    scored = pd.read_csv(run_path / "scores" / "claim_scores.csv")
    alerts = scored.loc[scored["review_recommended"]].copy()
    report_path = run_path / "report"
    _atomic_csv(alerts, report_path / "alerts.csv")
    _atomic_json(
        {
            "run_key": run_key,
            "incoming_claims": int(len(scored)),
            "claims_recommended_for_review": int(len(alerts)),
            "high_priority_claims": int((alerts["review_score"] >= 60).sum()),
            "model": "IsolationForest(random_state=42)",
            "features": FEATURE_COLUMNS,
            "high_cost_limit": float(scored["high_cost_limit"].iloc[0]),
            "review_rules": {
                "repeated_repairs_30d": "prior_repairs_30d >= 3",
                "sensor_threshold_breach": "at least one reading outside configured bounds",
                "unusual_cost": "historical median + max(4 * MAD, median, 500)",
            },
            "decision": "Triage only; a human must review the evidence.",
        },
        report_path / "summary.json",
    )
    return str(report_path)
