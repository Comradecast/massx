from __future__ import annotations

from pathlib import Path

import pandas as pd

from .io_utils import clean_optional_str, normalize_whitespace

HUMAN_REVIEW_RESULTS_COLUMNS = [
    "incident_id",
    "review_status",
    "final_category",
    "final_confidence",
    "notes",
    "source_override",
]


def ensure_human_review_results_file(path: str | Path) -> Path:
    resolved_path = Path(path)
    if not resolved_path.exists():
        pd.DataFrame(columns=HUMAN_REVIEW_RESULTS_COLUMNS).to_csv(resolved_path, index=False)
    return resolved_path


def read_human_review_results_frame(path: str | Path) -> pd.DataFrame:
    resolved_path = ensure_human_review_results_file(path)
    frame = pd.read_csv(resolved_path, dtype=str, keep_default_na=False)
    _validate_human_review_results_frame(frame)
    return frame[HUMAN_REVIEW_RESULTS_COLUMNS].copy()


def write_human_review_results_frame(path: str | Path, frame: pd.DataFrame) -> None:
    missing_columns = [column for column in HUMAN_REVIEW_RESULTS_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(
            "Human review results file must have exactly these columns: "
            f"{', '.join(HUMAN_REVIEW_RESULTS_COLUMNS)}. "
            f"Missing columns: {', '.join(missing_columns)}."
        )
    normalized = frame[HUMAN_REVIEW_RESULTS_COLUMNS].copy()
    _validate_human_review_results_frame(normalized)
    normalized.to_csv(path, index=False)


def validate_review_result_values(
    *,
    incident_id: str,
    review_status: str,
    final_confidence: str,
) -> float | None:
    normalized_incident_id = normalize_whitespace(incident_id)
    if not normalized_incident_id:
        raise ValueError("Human review results row cannot have a blank incident_id")

    normalized_status = normalize_whitespace(review_status)
    if not normalized_status:
        raise ValueError(f"Human review results row for incident_id={normalized_incident_id} is missing review_status")

    confidence_text = normalize_whitespace(final_confidence)
    if not confidence_text:
        return None
    try:
        return float(confidence_text)
    except ValueError as exc:
        raise ValueError(
            f"Human review results for incident_id={normalized_incident_id} has invalid final_confidence: {confidence_text}"
        ) from exc


def _validate_human_review_results_frame(frame: pd.DataFrame) -> None:
    found_columns = list(frame.columns)
    if found_columns != HUMAN_REVIEW_RESULTS_COLUMNS:
        missing_columns = [column for column in HUMAN_REVIEW_RESULTS_COLUMNS if column not in found_columns]
        unexpected_columns = [column for column in found_columns if column not in HUMAN_REVIEW_RESULTS_COLUMNS]
        raise ValueError(
            "Human review results file must have exactly these columns: "
            f"{', '.join(HUMAN_REVIEW_RESULTS_COLUMNS)}. "
            f"Missing columns: {', '.join(missing_columns) or 'none'}. "
            f"Unexpected columns: {', '.join(unexpected_columns) or 'none'}. "
            f"Found columns: {', '.join(found_columns)}."
        )

    seen_incident_ids: set[str] = set()
    for row in frame.to_dict(orient="records"):
        incident_id = normalize_whitespace(str(row.get("incident_id", "")))
        if not incident_id:
            continue
        if incident_id in seen_incident_ids:
            raise ValueError(f"Human review results file contains duplicate incident_id: {incident_id}")
        seen_incident_ids.add(incident_id)
        validate_review_result_values(
            incident_id=incident_id,
            review_status=str(row.get("review_status", "")),
            final_confidence=str(row.get("final_confidence", "")),
        )


def build_review_result_row(
    *,
    incident_id: str,
    review_status: str,
    final_category: str,
    final_confidence: str,
    notes: str,
    source_override: str,
) -> dict[str, str]:
    normalized_incident_id = normalize_whitespace(incident_id)
    normalized_status = normalize_whitespace(review_status)
    normalized_confidence = normalize_whitespace(final_confidence)
    validate_review_result_values(
        incident_id=normalized_incident_id,
        review_status=normalized_status,
        final_confidence=normalized_confidence,
    )
    return {
        "incident_id": normalized_incident_id,
        "review_status": normalized_status,
        "final_category": clean_optional_str(final_category) or "",
        "final_confidence": normalized_confidence,
        "notes": clean_optional_str(notes) or "",
        "source_override": clean_optional_str(source_override) or "",
    }


def upsert_human_review_result_row(frame: pd.DataFrame, row: dict[str, str]) -> pd.DataFrame:
    _validate_human_review_results_frame(frame)
    remaining = frame.loc[frame["incident_id"] != row["incident_id"]].copy()
    updated = pd.concat([remaining, pd.DataFrame([row])], ignore_index=True)
    _validate_human_review_results_frame(updated)
    return updated[HUMAN_REVIEW_RESULTS_COLUMNS].sort_values("incident_id", kind="stable").reset_index(drop=True)
