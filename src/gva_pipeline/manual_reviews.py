from __future__ import annotations

"""Manual review ingestion for acquisition-layer source curation.

Expected CSV columns:
- incident_id (required)
- review_status
- decision_type
- preferred_source_url
- added_source_candidates  # JSON array string or pipe-delimited URLs
- rejected_candidates      # JSON array string or pipe-delimited URLs
- review_notes
- reviewer
- review_timestamp

This file is optional. When absent, acquisition behavior remains unchanged.
"""

import json
from pathlib import Path

import pandas as pd

from .io_utils import clean_optional_str, normalize_whitespace, parse_source_candidates_value
from .models import HumanReviewResultRecord, IncidentRecord, ManualReviewRecord
from .review_results_io import read_human_review_results_frame

VALID_DECISION_TYPES = {
    "add_source_candidates",
    "reject_source_candidates",
    "set_preferred_source",
    "mark_no_viable_source_found",
    "mark_irrelevant_incident",
    "needs_more_research",
}
def get_default_manual_review_path() -> Path:
    return Path("data") / "manual_reviews.csv"


def read_manual_reviews_csv(path: str | Path) -> dict[str, ManualReviewRecord]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "incident_id" not in frame.columns:
        raise ValueError("Manual review file is missing required column: incident_id")

    reviews: dict[str, ManualReviewRecord] = {}
    for row in frame.to_dict(orient="records"):
        incident_id = normalize_whitespace(str(row.get("incident_id", "")))
        if not incident_id:
            continue
        if incident_id in reviews:
            raise ValueError(f"Manual review file contains duplicate incident_id: {incident_id}")

        decision_type = clean_optional_str(row.get("decision_type"))
        if decision_type and decision_type not in VALID_DECISION_TYPES:
            raise ValueError(
                f"Manual review for incident_id={incident_id} has invalid decision_type: {decision_type}"
            )

        reviews[incident_id] = ManualReviewRecord(
            incident_id=incident_id,
            review_status=clean_optional_str(row.get("review_status")),
            decision_type=decision_type,
            preferred_source_url=clean_optional_str(row.get("preferred_source_url")),
            added_source_candidates=tuple(
                _parse_manual_review_url_list(
                    row.get("added_source_candidates"),
                    incident_id=incident_id,
                    column_name="added_source_candidates",
                )
            ),
            rejected_candidates=tuple(
                _parse_manual_review_url_list(
                    row.get("rejected_candidates"),
                    incident_id=incident_id,
                    column_name="rejected_candidates",
                )
            ),
            review_notes=clean_optional_str(row.get("review_notes")),
            reviewer=clean_optional_str(row.get("reviewer")),
            review_timestamp=clean_optional_str(row.get("review_timestamp")),
        )

    return reviews


def read_human_review_results_csv(path: str | Path) -> dict[str, HumanReviewResultRecord]:
    frame = read_human_review_results_frame(path)
    resolved_reviews: dict[str, HumanReviewResultRecord] = {}
    for row in frame.to_dict(orient="records"):
        review_status = normalize_whitespace(str(row.get("review_status", "")))
        if review_status != "resolved":
            continue

        incident_id = normalize_whitespace(str(row.get("incident_id", "")))
        if incident_id in resolved_reviews:
            raise ValueError(f"Human review results file contains duplicate resolved incident_id: {incident_id}")

        final_confidence = _parse_review_confidence(
            row.get("final_confidence"),
            incident_id=incident_id,
        )
        resolved_reviews[incident_id] = HumanReviewResultRecord(
            incident_id=incident_id,
            review_status=review_status,
            final_category=clean_optional_str(row.get("final_category")),
            final_confidence=final_confidence,
            notes=clean_optional_str(row.get("notes")),
            source_override=clean_optional_str(row.get("source_override")),
        )

    return resolved_reviews


def attach_manual_reviews(
    incidents: list[IncidentRecord],
    reviews_by_incident_id: dict[str, ManualReviewRecord],
) -> list[IncidentRecord]:
    attached: list[IncidentRecord] = []
    for incident in incidents:
        review = reviews_by_incident_id.get(incident.incident_id)
        attached.append(
            IncidentRecord(
                incident_id=incident.incident_id,
                incident_date=incident.incident_date,
                state=incident.state,
                city_or_county=incident.city_or_county,
                address=incident.address,
                victims_killed=incident.victims_killed,
                victims_injured=incident.victims_injured,
                suspects_killed=incident.suspects_killed,
                suspects_injured=incident.suspects_injured,
                suspects_arrested=incident.suspects_arrested,
                incident_url=incident.incident_url,
                source_url=incident.source_url,
                source_candidates=incident.source_candidates,
                source_candidate_origins=incident.source_candidate_origins,
                manual_review=review,
            )
        )
    return attached


def _parse_manual_review_url_list(
    value: object,
    *,
    incident_id: str,
    column_name: str,
) -> list[str]:
    text = clean_optional_str(value)
    if text is None:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Manual review for incident_id={incident_id} has invalid JSON in {column_name}: {exc.msg}"
            ) from exc
        if not isinstance(parsed, list):
            raise ValueError(
                f"Manual review for incident_id={incident_id} must provide a JSON array in {column_name}"
            )
        return parse_source_candidates_value(text)
    return parse_source_candidates_value(text)


def _parse_review_confidence(value: object, *, incident_id: str) -> float | None:
    text = clean_optional_str(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(
            f"Human review results for incident_id={incident_id} has invalid final_confidence: {text}"
        ) from exc
