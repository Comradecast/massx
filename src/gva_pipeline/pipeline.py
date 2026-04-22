from __future__ import annotations

from collections import Counter
from pathlib import Path
import threading
import time
from typing import Callable

import pandas as pd
from openpyxl.utils import get_column_letter

from .classify import classify_incident, extract_context_flags
from .demographics import extract_suspect_demographics
from .fetch import build_session, fetch_source, save_raw_html
from .io_utils import (
    deduplicate_incidents_frame,
    ensure_directory,
    frame_to_incident_records,
    read_incidents_csv,
    serialize_value,
    write_json_records,
)
from .manual_reviews import (
    attach_manual_reviews,
    get_default_manual_review_path,
    read_human_review_results_csv,
    read_manual_reviews_csv,
)
from .models import FetchResult, HumanReviewResultRecord, IncidentAcquisitionResult, PipelineReport
from .source_policy import extract_source_domain
from .source_acquisition import acquire_incident_sources

FetchFunction = Callable[..., FetchResult]
EXCEL_MAX_AUTOFIT_WIDTH = 80
REVIEW_RULE_PRIORITIES = {
    "fetch_failed": 100,
    "no_article_text": 95,
    "rule_conflict_domestic_context": 90,
    "unknown_category": 80,
    "rule_conflict_school_context": 78,
    "rule_conflict_party_context": 75,
    "low_confidence": 70,
}


def _log(message: str) -> None:
    print(message, flush=True)


def _excel_display_length(value: object) -> int:
    if value is None or value is pd.NA:
        return 0
    if isinstance(value, float) and pd.isna(value):
        return 0
    return max(len(part) for part in str(value).splitlines()) if str(value) else 0


def _write_excel_with_autofit(frame: pd.DataFrame, output_path: Path, *, sheet_name: str = "Sheet1") -> None:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]

        for index, column_name in enumerate(frame.columns, start=1):
            header_width = len(str(column_name))
            value_width = max((_excel_display_length(value) for value in frame[column_name]), default=0)
            adjusted_width = min(max(header_width, value_width) + 2, EXCEL_MAX_AUTOFIT_WIDTH)
            worksheet.column_dimensions[get_column_letter(index)].width = max(adjusted_width, 8)


def _write_tabular_outputs(
    output_directory: Path,
    csv_name: str,
    frame: pd.DataFrame,
    *,
    write_excel_autofit: bool,
) -> None:
    csv_path = output_directory / csv_name
    frame.to_csv(csv_path, index=False)
    if write_excel_autofit:
        _write_excel_with_autofit(frame, csv_path.with_suffix(".xlsx"))


def _format_heartbeat(status: dict[str, int | float | str | None]) -> str:
    current_incident = str(status["current_incident_id"] or "n/a")
    elapsed_seconds = float(status["elapsed_seconds"])
    return (
        "[heartbeat] "
        f"completed={status['completed_count']}/{status['total_count']} "
        f"current_incident={current_incident} "
        f"fetch_success={status['fetch_success_count']} "
        f"fetch_failure={status['fetch_failure_count']} "
        f"elapsed={elapsed_seconds:.1f}s"
    )


def _heartbeat_worker(
    *,
    stop_event: threading.Event,
    status_lock: threading.Lock,
    status: dict[str, int | float | str | None],
    heartbeat_seconds: float,
) -> None:
    while not stop_event.wait(timeout=heartbeat_seconds):
        with status_lock:
            snapshot = dict(status)
        snapshot["elapsed_seconds"] = time.monotonic() - float(snapshot["started_at"])
        _log(_format_heartbeat(snapshot))


def _record_to_output(
    incident,
    acquisition_result: IncidentAcquisitionResult,
    article_text: str,
    html_path: Path | None,
    human_review_result: HumanReviewResultRecord | None = None,
) -> dict[str, object]:
    fetch_result = acquisition_result.fetch_result
    context_flags = extract_context_flags(article_text)
    classification = classify_incident(article_text, context_flags)
    demographics = extract_suspect_demographics(article_text)
    acquisition_status, failure_stage, failure_reason = _coalesce_failure_metadata(fetch_result)
    selected_source_url = acquisition_result.selected_source_url
    domain_metadata = _build_fetch_domain_metadata(
        incident_source_url=incident.source_url,
        selected_source_url=selected_source_url,
        fetch_result=fetch_result,
    )
    output_record = {
        **incident.to_dict(),
        "selected_source_url": selected_source_url,
        "selected_source_origin": acquisition_result.selected_source_origin,
        "source_attempt_count": acquisition_result.source_attempt_count,
        "source_candidates_count": acquisition_result.source_candidates_count,
        "source_attempt_history": acquisition_result.source_attempt_history,
        "manual_review_applied": acquisition_result.manual_review_applied,
        "manual_review_status": acquisition_result.manual_review_status,
        "manual_review_decision_type": acquisition_result.manual_review_decision_type,
        "manual_review_preferred_source_url": acquisition_result.manual_review_preferred_source_url,
        "manual_review_added_candidates": acquisition_result.manual_review_added_candidates,
        "manual_review_rejected_candidates": acquisition_result.manual_review_rejected_candidates,
        "manual_review_notes": acquisition_result.manual_review_notes,
        "manual_review_reviewer": acquisition_result.manual_review_reviewer,
        "manual_review_timestamp": acquisition_result.manual_review_timestamp,
        **domain_metadata,
        "source_category": fetch_result.source_category,
        "source_action": fetch_result.source_action,
        "fetch_ok": fetch_result.ok,
        "fetch_error": fetch_result.error,
        "fetch_status_code": fetch_result.status_code,
        "fetch_final_url": fetch_result.final_url,
        "acquisition_status": acquisition_status,
        "failure_stage": failure_stage,
        "failure_reason": failure_reason,
        "fetch_retryable": fetch_result.retryable,
        "fetch_attempts": fetch_result.attempts,
        "article_text": article_text,
        "article_text_length": len(article_text),
        "raw_html_path": str(html_path) if html_path else None,
        "category": classification.category,
        "category_confidence": classification.confidence,
        "category_rule": classification.matched_rule,
        "category_explanation": classification.explanation,
        **context_flags.to_dict(),
        "suspect_age": demographics.suspect_age,
        "suspect_age_confidence": demographics.suspect_age_confidence,
        "suspect_gender": demographics.suspect_gender,
        "suspect_gender_confidence": demographics.suspect_gender_confidence,
        "suspect_race": demographics.suspect_race,
        "suspect_race_confidence": demographics.suspect_race_confidence,
        "suspect_count_estimate": demographics.suspect_count_estimate,
        "suspect_demographics_notes": demographics.suspect_demographics_notes,
        "suspect_demographics_snippet": demographics.suspect_demographics_snippet,
    }
    output_record.update(_build_review_metadata(output_record))
    output_record.update(_apply_human_review_result(output_record, human_review_result))

    return output_record


def _coalesce_failure_metadata(fetch_result: FetchResult) -> tuple[str, str | None, str | None]:
    if fetch_result.ok:
        return ("fetched", None, None)
    if fetch_result.failure_reason or fetch_result.acquisition_status != "fetched":
        return (
            fetch_result.acquisition_status,
            fetch_result.failure_stage,
            fetch_result.failure_reason,
        )

    error = fetch_result.error or ""
    if error == "article_text_not_found":
        return ("extraction_failed", "extraction", error)
    if error == "http_404":
        return ("permanent_not_found", "fetch", error)
    if error == "http_429":
        return ("rate_limited", "fetch", error)
    if "Timeout" in error:
        return ("timeout", "fetch", error)
    if error in {"missing_source_url", "malformed_source_url"} or error.endswith("_unsupported"):
        return ("source_not_supported", "source_policy", error)
    return ("fetch_failed", "fetch", error or None)


def _summarize(enriched_frame: pd.DataFrame) -> PipelineReport:
    total = len(enriched_frame.index)
    category_counts = dict(Counter(enriched_frame["category"].fillna("unknown")))

    usable_age_count = int(enriched_frame["suspect_age"].notna().sum())
    usable_gender_count = int((enriched_frame["suspect_gender"] != "unknown").sum())
    usable_race_count = int((enriched_frame["suspect_race"] != "unknown").sum())

    unknown_age_percentage = ((total - usable_age_count) / total * 100.0) if total else 0.0
    unknown_gender_percentage = ((total - usable_gender_count) / total * 100.0) if total else 0.0
    unknown_race_percentage = ((total - usable_race_count) / total * 100.0) if total else 0.0

    warnings = [
        "Warning: article-based demographics are incomplete and subject to reporting bias.",
        "Warning: missing demographic values should not be interpreted as evidence of absence.",
    ]
    return PipelineReport(
        total_unique_incidents=total,
        category_counts=category_counts,
        usable_age_count=usable_age_count,
        usable_gender_count=usable_gender_count,
        usable_race_count=usable_race_count,
        unknown_age_percentage=unknown_age_percentage,
        unknown_gender_percentage=unknown_gender_percentage,
        unknown_race_percentage=unknown_race_percentage,
        warning_messages=warnings,
    )


def build_console_report(report: PipelineReport, summary_by_category: pd.DataFrame) -> str:
    lines = [f"Total unique incidents: {report.total_unique_incidents}", "Counts by category:"]
    for _, row in summary_by_category.sort_values("incident_count", ascending=False).iterrows():
        lines.append(
            f"  {row['category']}: {row['incident_count']} incidents, "
            f"{row['victims_killed']} killed, {row['victims_injured']} injured"
        )
    lines.extend(
        [
            f"Usable suspect age data: {report.usable_age_count}",
            f"Usable suspect gender data: {report.usable_gender_count}",
            f"Explicit suspect race data: {report.usable_race_count}",
            f"Unknown age percentage: {report.unknown_age_percentage:.1f}%",
            f"Unknown gender percentage: {report.unknown_gender_percentage:.1f}%",
            f"Unknown race percentage: {report.unknown_race_percentage:.1f}%",
        ]
    )
    lines.extend(report.warning_messages)
    return "\n".join(lines)


def _normalize_source_domain(value: object) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip().lower()
    if not text or text in {"nan", "none"}:
        return "unknown"
    extracted = extract_source_domain(text)
    if extracted != "unknown":
        return extracted
    if "://" not in text and "/" not in text and " " not in text and "." in text:
        return text
    return "unknown"


def _domains_differ(request_domain: str, final_domain: str) -> bool:
    return (
        request_domain != "unknown"
        and final_domain != "unknown"
        and request_domain != final_domain
    )


def _build_fetch_domain_metadata(
    *,
    incident_source_url: str | None,
    selected_source_url: str | None,
    fetch_result: FetchResult,
) -> dict[str, object]:
    request_domain = _normalize_source_domain(fetch_result.requested_url)
    final_domain = _normalize_source_domain(fetch_result.final_url)
    source_domain = _normalize_source_domain(
        selected_source_url or fetch_result.final_url or fetch_result.requested_url or incident_source_url
    )
    return {
        "source_domain": source_domain,
        "fetch_request_domain": request_domain,
        "fetch_final_domain": final_domain,
        "fetch_domain_changed": _domains_differ(request_domain, final_domain),
    }


def _select_review_reason(candidates: list[str]) -> str | None:
    if not candidates:
        return None
    return max(candidates, key=lambda reason: REVIEW_RULE_PRIORITIES[reason])


def _build_review_metadata(row: dict[str, object]) -> dict[str, object]:
    category = str(row.get("category") or "")
    category_confidence = float(row.get("category_confidence") or 0.0)
    fetch_ok = bool(row.get("fetch_ok"))
    article_text_length = int(row.get("article_text_length") or 0)
    mentions_party = bool(row.get("mentions_party"))
    mentions_domestic = bool(row.get("mentions_domestic"))
    mentions_school = bool(row.get("mentions_school"))

    source_review_reasons: list[str] = []
    category_review_reasons: list[str] = []

    if not fetch_ok:
        source_review_reasons.append("fetch_failed")
    if article_text_length == 0:
        source_review_reasons.append("no_article_text")
    if category == "unknown":
        category_review_reasons.append("unknown_category")
    if category_confidence < 0.6:
        category_review_reasons.append("low_confidence")
    if mentions_domestic and category != "domestic_family":
        category_review_reasons.append("rule_conflict_domestic_context")
    if mentions_school and category != "school_campus":
        category_review_reasons.append("rule_conflict_school_context")
    if mentions_party and not (mentions_domestic and category == "domestic_family") and category not in {
        "party_social_event",
        "school_campus",
    }:
        category_review_reasons.append("rule_conflict_party_context")

    all_reasons = [*source_review_reasons, *category_review_reasons]
    review_reason = _select_review_reason(all_reasons)
    review_priority = REVIEW_RULE_PRIORITIES[review_reason] if review_reason else 0
    needs_source_review = bool(source_review_reasons)
    needs_category_review = bool(category_review_reasons)

    return {
        "review_required": bool(review_reason),
        "review_reason": review_reason,
        "review_priority": review_priority,
        "needs_category_review": needs_category_review,
        "needs_source_review": needs_source_review,
    }


def _build_human_review_queue(enriched_frame: pd.DataFrame) -> pd.DataFrame:
    review_frame = enriched_frame.copy()
    review_frame = review_frame[
        review_frame["review_required"].fillna(False) & ~review_frame["review_applied"].fillna(False)
    ].copy()
    review_frame["incident_date_sort"] = pd.to_datetime(review_frame["incident_date"], errors="coerce")
    review_frame = review_frame.sort_values(
        ["review_priority", "incident_date_sort", "incident_id"],
        ascending=[False, False, True],
        kind="stable",
    ).drop(columns=["incident_date_sort"])

    review_columns = [
        "incident_id",
        "incident_date",
        "state",
        "city_or_county",
        "address",
        "victims_killed",
        "victims_injured",
        "category",
        "category_confidence",
        "original_category",
        "original_category_confidence",
        "selected_source_url",
        "selected_source_overridden",
        "original_selected_source_url",
        "selected_source_origin",
        "source_candidates_count",
        "source_attempt_count",
        "fetch_ok",
        "acquisition_status",
        "failure_stage",
        "failure_reason",
        "fetch_status_code",
        "fetch_error",
        "incident_url",
        "source_url",
        "source_domain",
        "source_category",
        "mentions_party",
        "mentions_domestic",
        "mentions_school",
        "manual_review_applied",
        "manual_review_decision_type",
        "suspect_age",
        "suspect_gender",
        "suspect_race",
        "suspect_demographics_snippet",
        "review_applied",
        "review_applied_fields",
        "review_notes",
        "review_status",
        "review_required",
        "review_reason",
        "review_priority",
        "needs_category_review",
        "needs_source_review",
    ]
    return review_frame[review_columns].copy()


def _build_domain_fetch_summary(enriched_frame: pd.DataFrame) -> pd.DataFrame:
    working = enriched_frame.copy()
    working["source_domain"] = working["source_domain"].map(_normalize_source_domain)
    working["fetch_error"] = working["fetch_error"].fillna("")
    working["fetch_ok"] = working["fetch_ok"].fillna(False)
    working["acquisition_status"] = working["acquisition_status"].fillna("")
    working["failure_reason"] = working["failure_reason"].fillna("")

    working["fetch_success_count"] = working["fetch_ok"].map(lambda value: 1 if bool(value) else 0)
    working["fetch_failure_count"] = working["fetch_ok"].map(lambda value: 0 if bool(value) else 1)
    working["http_403_count"] = working["failure_reason"].map(lambda value: 1 if value == "http_403" else 0)
    working["http_404_count"] = working["failure_reason"].map(lambda value: 1 if value == "http_404" else 0)
    working["http_429_count"] = working["failure_reason"].map(lambda value: 1 if value == "http_429" else 0)
    working["timeout_count"] = working["acquisition_status"].map(
        lambda value: 1 if value == "timeout" else 0
    )
    working["article_text_not_found_count"] = working["failure_reason"].map(
        lambda value: 1 if value == "article_text_not_found" else 0
    )
    working["source_not_supported_count"] = working["acquisition_status"].map(
        lambda value: 1 if value == "source_not_supported" else 0
    )
    working["rejected_source_count"] = working["acquisition_status"].map(
        lambda value: 1 if value == "rejected_source" else 0
    )
    working["other_error_count"] = working.apply(
        lambda row: 1
        if row["failure_reason"]
        and row["failure_reason"] not in {"http_403", "http_404", "http_429", "article_text_not_found"}
        and row["acquisition_status"] not in {"timeout", "source_not_supported", "rejected_source"}
        else 0,
        axis=1,
    )

    summary = (
        working.groupby("source_domain", dropna=False)
        .agg(
            incident_count=("incident_id", "count"),
            fetch_success_count=("fetch_success_count", "sum"),
            fetch_failure_count=("fetch_failure_count", "sum"),
            http_403_count=("http_403_count", "sum"),
            http_404_count=("http_404_count", "sum"),
            http_429_count=("http_429_count", "sum"),
            timeout_count=("timeout_count", "sum"),
            article_text_not_found_count=("article_text_not_found_count", "sum"),
            source_not_supported_count=("source_not_supported_count", "sum"),
            rejected_source_count=("rejected_source_count", "sum"),
            other_error_count=("other_error_count", "sum"),
        )
        .reset_index()
    )
    summary["success_rate"] = summary.apply(
        lambda row: row["fetch_success_count"] / row["incident_count"] if row["incident_count"] else 0.0,
        axis=1,
    )
    summary = summary[
        [
            "source_domain",
            "incident_count",
            "fetch_success_count",
            "fetch_failure_count",
            "success_rate",
            "http_403_count",
            "http_404_count",
            "http_429_count",
            "timeout_count",
            "article_text_not_found_count",
            "source_not_supported_count",
            "rejected_source_count",
            "other_error_count",
        ]
    ].copy()
    return summary.sort_values(["incident_count", "source_domain"], ascending=[False, True]).reset_index(drop=True)


def _resolve_review_domain(row: pd.Series) -> str:
    source_domain = _normalize_source_domain(row.get("source_domain"))
    if source_domain != "unknown":
        return source_domain
    fetch_request_domain = _normalize_source_domain(row.get("fetch_request_domain"))
    if fetch_request_domain != "unknown":
        return fetch_request_domain
    return "unknown"


def _review_applied_field_contains(value: object, field_name: str) -> bool:
    text = str(value or "")
    return field_name in {item for item in text.split("|") if item}


def _build_domain_review_summary(enriched_frame: pd.DataFrame) -> pd.DataFrame:
    working = enriched_frame.copy()
    working["domain"] = working.apply(_resolve_review_domain, axis=1)
    working["fetch_ok"] = working["fetch_ok"].fillna(False)
    working["review_required"] = working["review_required"].fillna(False)
    working["review_applied"] = working["review_applied"].fillna(False)
    working["selected_source_overridden"] = working["selected_source_overridden"].fillna(False)
    working["category"] = working["category"].fillna("")
    working["review_applied_fields"] = working["review_applied_fields"].fillna("")
    working["article_text_length"] = pd.to_numeric(working["article_text_length"], errors="coerce").fillna(0)

    working["total_incidents"] = 1
    working["fetched_ok_count"] = working["fetch_ok"].map(lambda value: 1 if bool(value) else 0)
    working["fetch_failed_count"] = working["fetch_ok"].map(lambda value: 0 if bool(value) else 1)
    working["no_article_text_count"] = working["article_text_length"].map(lambda value: 1 if int(value) == 0 else 0)
    working["review_required_count"] = working["review_required"].map(lambda value: 1 if bool(value) else 0)
    working["review_applied_count"] = working["review_applied"].map(lambda value: 1 if bool(value) else 0)
    working["category_override_count"] = working["review_applied_fields"].map(
        lambda value: 1 if _review_applied_field_contains(value, "category") else 0
    )
    working["confidence_override_count"] = working["review_applied_fields"].map(
        lambda value: 1 if _review_applied_field_contains(value, "category_confidence") else 0
    )
    working["source_override_count"] = working["review_applied_fields"].map(
        lambda value: 1 if _review_applied_field_contains(value, "selected_source_url") else 0
    )
    working["selected_source_overridden_count"] = working["selected_source_overridden"].map(
        lambda value: 1 if bool(value) else 0
    )
    working["unknown_category_count"] = working["category"].map(lambda value: 1 if str(value) == "unknown" else 0)

    summary = (
        working.groupby("domain", dropna=False)
        .agg(
            total_incidents=("total_incidents", "sum"),
            fetched_ok_count=("fetched_ok_count", "sum"),
            fetch_failed_count=("fetch_failed_count", "sum"),
            no_article_text_count=("no_article_text_count", "sum"),
            review_required_count=("review_required_count", "sum"),
            review_applied_count=("review_applied_count", "sum"),
            category_override_count=("category_override_count", "sum"),
            confidence_override_count=("confidence_override_count", "sum"),
            source_override_count=("source_override_count", "sum"),
            selected_source_overridden_count=("selected_source_overridden_count", "sum"),
            unknown_category_count=("unknown_category_count", "sum"),
        )
        .reset_index()
    )
    summary["fetch_failure_rate"] = summary.apply(
        lambda row: row["fetch_failed_count"] / row["total_incidents"] if row["total_incidents"] else 0.0,
        axis=1,
    )
    summary["review_required_rate"] = summary.apply(
        lambda row: row["review_required_count"] / row["total_incidents"] if row["total_incidents"] else 0.0,
        axis=1,
    )
    summary["review_applied_rate"] = summary.apply(
        lambda row: row["review_applied_count"] / row["total_incidents"] if row["total_incidents"] else 0.0,
        axis=1,
    )
    summary["source_override_rate"] = summary.apply(
        lambda row: row["source_override_count"] / row["total_incidents"] if row["total_incidents"] else 0.0,
        axis=1,
    )
    summary = summary[
        [
            "domain",
            "total_incidents",
            "fetched_ok_count",
            "fetch_failed_count",
            "no_article_text_count",
            "review_required_count",
            "review_applied_count",
            "category_override_count",
            "confidence_override_count",
            "source_override_count",
            "selected_source_overridden_count",
            "unknown_category_count",
            "fetch_failure_rate",
            "review_required_rate",
            "review_applied_rate",
            "source_override_rate",
        ]
    ].copy()
    return summary.sort_values(
        ["review_required_count", "review_applied_count", "fetch_failed_count", "total_incidents", "domain"],
        ascending=[False, False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _resolve_review_reason_group(row: pd.Series) -> str:
    if not bool(row.get("review_required")):
        return "not_queued"
    review_reason = str(row.get("review_reason") or "").strip()
    return review_reason or "not_queued"


def _build_review_reason_summary(enriched_frame: pd.DataFrame) -> pd.DataFrame:
    working = enriched_frame.copy()
    working["review_reason"] = working.apply(_resolve_review_reason_group, axis=1)
    working["review_required"] = working["review_required"].fillna(False)
    working["review_applied"] = working["review_applied"].fillna(False)
    working["fetch_ok"] = working["fetch_ok"].fillna(False)
    working["selected_source_overridden"] = working["selected_source_overridden"].fillna(False)
    working["category"] = working["category"].fillna("")
    working["article_text_length"] = pd.to_numeric(working["article_text_length"], errors="coerce").fillna(0)

    working["total_incidents"] = 1
    working["queued_incidents"] = working["review_required"].map(lambda value: 1 if bool(value) else 0)
    working["review_applied_count"] = working["review_applied"].map(lambda value: 1 if bool(value) else 0)
    working["fetch_failed_count"] = working["fetch_ok"].map(lambda value: 0 if bool(value) else 1)
    working["no_article_text_count"] = working["article_text_length"].map(lambda value: 1 if int(value) == 0 else 0)
    working["unknown_category_count"] = working["category"].map(lambda value: 1 if str(value) == "unknown" else 0)
    working["selected_source_overridden_count"] = working["selected_source_overridden"].map(
        lambda value: 1 if bool(value) else 0
    )

    summary = (
        working.groupby("review_reason", dropna=False)
        .agg(
            total_incidents=("total_incidents", "sum"),
            queued_incidents=("queued_incidents", "sum"),
            review_applied_count=("review_applied_count", "sum"),
            fetch_failed_count=("fetch_failed_count", "sum"),
            no_article_text_count=("no_article_text_count", "sum"),
            unknown_category_count=("unknown_category_count", "sum"),
            selected_source_overridden_count=("selected_source_overridden_count", "sum"),
        )
        .reset_index()
    )
    summary["queued_rate"] = summary.apply(
        lambda row: row["queued_incidents"] / row["total_incidents"] if row["total_incidents"] else 0.0,
        axis=1,
    )
    summary["review_applied_rate"] = summary.apply(
        lambda row: row["review_applied_count"] / row["total_incidents"] if row["total_incidents"] else 0.0,
        axis=1,
    )
    summary = summary[
        [
            "review_reason",
            "total_incidents",
            "queued_incidents",
            "review_applied_count",
            "fetch_failed_count",
            "no_article_text_count",
            "unknown_category_count",
            "selected_source_overridden_count",
            "queued_rate",
            "review_applied_rate",
        ]
    ].copy()
    return summary.sort_values(
        ["queued_incidents", "review_applied_count", "total_incidents", "review_reason"],
        ascending=[False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _apply_human_review_result(
    row: dict[str, object],
    human_review_result: HumanReviewResultRecord | None,
) -> dict[str, object]:
    original_category = row.get("category")
    original_category_confidence = row.get("category_confidence")
    original_selected_source_url = row.get("selected_source_url")

    overrides: dict[str, object] = {
        "original_category": original_category,
        "original_category_confidence": original_category_confidence,
        "original_selected_source_url": original_selected_source_url,
        "selected_source_overridden": False,
        "review_applied": False,
        "review_applied_fields": "",
        "review_notes": "",
        "review_status": "",
    }
    if human_review_result is None:
        return overrides

    applied_fields: list[str] = []
    if human_review_result.final_category is not None:
        row["category"] = human_review_result.final_category
        applied_fields.append("category")
    if human_review_result.final_confidence is not None:
        row["category_confidence"] = human_review_result.final_confidence
        applied_fields.append("category_confidence")
    if human_review_result.source_override is not None:
        row["selected_source_url"] = human_review_result.source_override
        overrides["selected_source_overridden"] = True
        applied_fields.append("selected_source_url")

    overrides["review_applied"] = True
    overrides["review_applied_fields"] = "|".join(applied_fields)
    overrides["review_notes"] = human_review_result.notes or ""
    overrides["review_status"] = human_review_result.review_status
    return overrides


def run_pipeline(
    *,
    input_path: str | Path,
    output_dir: str | Path,
    manual_review_path: str | Path | None = None,
    human_review_results_path: str | Path | None = None,
    save_html: bool = False,
    write_excel_autofit: bool = False,
    timeout_seconds: float = 8.0,
    limit: int | None = None,
    progress_interval: int = 100,
    heartbeat_seconds: float = 10.0,
    verbose_lifecycle: bool = False,
    fetch_fn: FetchFunction = fetch_source,
) -> PipelineReport:
    output_directory = ensure_directory(output_dir)
    html_directory = ensure_directory(output_directory / "raw_html") if save_html else None

    raw_frame = read_incidents_csv(input_path)
    deduped_frame = deduplicate_incidents_frame(raw_frame)
    incidents = frame_to_incident_records(deduped_frame)
    resolved_manual_review_path = Path(manual_review_path) if manual_review_path else get_default_manual_review_path()
    manual_reviews_by_incident_id = (
        read_manual_reviews_csv(resolved_manual_review_path)
        if resolved_manual_review_path.exists()
        else {}
    )
    resolved_human_review_results_path = Path(human_review_results_path) if human_review_results_path else None
    if resolved_human_review_results_path and not resolved_human_review_results_path.exists():
        raise ValueError(f"Human review results file does not exist: {resolved_human_review_results_path}")
    human_review_results_by_incident_id = (
        read_human_review_results_csv(resolved_human_review_results_path)
        if resolved_human_review_results_path and resolved_human_review_results_path.exists()
        else {}
    )
    if manual_reviews_by_incident_id:
        incidents = attach_manual_reviews(incidents, manual_reviews_by_incident_id)
    if limit is not None:
        incidents = incidents[:limit]

    session = build_session()
    fetch_cache: dict[str, FetchResult] = {}
    enriched_records: list[dict[str, object]] = []
    failure_records: list[dict[str, object]] = []
    total_incidents = len(incidents)
    started_at = time.monotonic()
    status_lock = threading.Lock()
    heartbeat_stop_event = threading.Event()
    heartbeat_status: dict[str, int | float | str | None] = {
        "completed_count": 0,
        "total_count": total_incidents,
        "current_incident_id": None,
        "fetch_success_count": 0,
        "fetch_failure_count": 0,
        "started_at": started_at,
        "elapsed_seconds": 0.0,
    }
    heartbeat_thread = threading.Thread(
        target=_heartbeat_worker,
        kwargs={
            "stop_event": heartbeat_stop_event,
            "status_lock": status_lock,
            "status": heartbeat_status,
            "heartbeat_seconds": heartbeat_seconds,
        },
        daemon=True,
        name="massx-heartbeat",
    )

    _log(f"Loaded {total_incidents} incident(s) from {input_path}")
    if manual_reviews_by_incident_id:
        _log(
            f"Loaded {len(manual_reviews_by_incident_id)} manual review record(s) from "
            f"{resolved_manual_review_path}"
        )
    if human_review_results_by_incident_id and resolved_human_review_results_path:
        _log(
            f"Loaded {len(human_review_results_by_incident_id)} resolved human review result(s) from "
            f"{resolved_human_review_results_path}"
        )
    if limit is not None:
        _log(f"Limit mode active: processing first {total_incidents} incident(s)")
    heartbeat_thread.start()

    try:
        for index, incident in enumerate(incidents, start=1):
            with status_lock:
                heartbeat_status["current_incident_id"] = incident.incident_id

            if verbose_lifecycle:
                _log(f"[incident-start] {index}/{total_incidents} incident_id={incident.incident_id}")

            acquisition_result = acquire_incident_sources(
                incident,
                fetch_fn=fetch_fn,
                session=session,
                timeout_seconds=timeout_seconds,
                store_raw_html=save_html,
                fetch_cache=fetch_cache,
            )
            fetch_result = acquisition_result.fetch_result

            with status_lock:
                if fetch_result.ok:
                    heartbeat_status["fetch_success_count"] = int(heartbeat_status["fetch_success_count"]) + 1
                else:
                    heartbeat_status["fetch_failure_count"] = int(heartbeat_status["fetch_failure_count"]) + 1

            if verbose_lifecycle:
                _log(
                    "[fetch-complete] "
                    f"incident_id={incident.incident_id} ok={fetch_result.ok} "
                    f"status_code={fetch_result.status_code} error={fetch_result.error or 'none'}"
                )

            html_path = None
            if save_html and html_directory and fetch_result.raw_html:
                html_path = save_raw_html(fetch_result, html_directory, incident_id=incident.incident_id)

            article_text = fetch_result.article_text or ""
            enriched_records.append(
                _record_to_output(
                    incident,
                    acquisition_result,
                    article_text,
                    html_path,
                    human_review_results_by_incident_id.get(incident.incident_id),
                )
            )

            if not fetch_result.ok:
                acquisition_status, failure_stage, failure_reason = _coalesce_failure_metadata(fetch_result)
                failure_records.append(
                    {
                        "incident_id": incident.incident_id,
                        "source_url": incident.source_url,
                        "selected_source_url": acquisition_result.selected_source_url,
                        "selected_source_origin": acquisition_result.selected_source_origin,
                        "source_attempt_count": acquisition_result.source_attempt_count,
                        "source_candidates_count": acquisition_result.source_candidates_count,
                        "source_attempt_history": acquisition_result.source_attempt_history,
                        "manual_review_applied": acquisition_result.manual_review_applied,
                        "manual_review_status": acquisition_result.manual_review_status,
                        "manual_review_decision_type": acquisition_result.manual_review_decision_type,
                        "manual_review_preferred_source_url": acquisition_result.manual_review_preferred_source_url,
                        "manual_review_added_candidates": acquisition_result.manual_review_added_candidates,
                        "manual_review_rejected_candidates": acquisition_result.manual_review_rejected_candidates,
                        "manual_review_notes": acquisition_result.manual_review_notes,
                        "manual_review_reviewer": acquisition_result.manual_review_reviewer,
                        "manual_review_timestamp": acquisition_result.manual_review_timestamp,
                        **_build_fetch_domain_metadata(
                            incident_source_url=incident.source_url,
                            selected_source_url=acquisition_result.selected_source_url,
                            fetch_result=fetch_result,
                        ),
                        "status_code": fetch_result.status_code,
                        "final_url": fetch_result.final_url,
                        "acquisition_status": acquisition_status,
                        "failure_stage": failure_stage,
                        "failure_reason": failure_reason,
                        "source_category": fetch_result.source_category,
                        "attempts": fetch_result.attempts,
                        "error": fetch_result.error,
                    }
                )

            with status_lock:
                heartbeat_status["completed_count"] = index

            if verbose_lifecycle:
                _log(f"[incident-complete] {index}/{total_incidents} incident_id={incident.incident_id}")

            if progress_interval > 0 and (index % progress_interval == 0 or index == total_incidents):
                _log(f"Processed {index}/{total_incidents} incident(s)")
    finally:
        heartbeat_stop_event.set()
        heartbeat_thread.join(timeout=max(heartbeat_seconds, 0.1) + 1.0)
        if hasattr(session, "close"):
            session.close()

    enriched_frame = pd.DataFrame(enriched_records)
    _write_tabular_outputs(
        output_directory,
        "enriched_incidents.csv",
        enriched_frame,
        write_excel_autofit=write_excel_autofit,
    )
    write_json_records(
        output_directory / "enriched_incidents.json",
        [serialize_value(record) for record in enriched_records],
    )

    summary_by_category = (
        enriched_frame.groupby("category", dropna=False)
        .agg(
            incident_count=("incident_id", "count"),
            victims_killed=("victims_killed", lambda values: pd.to_numeric(values, errors="coerce").fillna(0).sum()),
            victims_injured=("victims_injured", lambda values: pd.to_numeric(values, errors="coerce").fillna(0).sum()),
        )
        .reset_index()
    )
    _write_tabular_outputs(
        output_directory,
        "summary_by_category.csv",
        summary_by_category,
        write_excel_autofit=write_excel_autofit,
    )

    total = max(len(enriched_frame.index), 1)
    summary_demographics = pd.DataFrame(
        [
            {
                "field": "suspect_age",
                "usable_count": int(enriched_frame["suspect_age"].notna().sum()),
                "unknown_count": int(total - enriched_frame["suspect_age"].notna().sum()),
                "unknown_percentage": (total - enriched_frame["suspect_age"].notna().sum()) / total * 100.0,
            },
            {
                "field": "suspect_gender",
                "usable_count": int((enriched_frame["suspect_gender"] != "unknown").sum()),
                "unknown_count": int(total - (enriched_frame["suspect_gender"] != "unknown").sum()),
                "unknown_percentage": (total - (enriched_frame["suspect_gender"] != "unknown").sum()) / total * 100.0,
            },
            {
                "field": "suspect_race",
                "usable_count": int((enriched_frame["suspect_race"] != "unknown").sum()),
                "unknown_count": int(total - (enriched_frame["suspect_race"] != "unknown").sum()),
                "unknown_percentage": (total - (enriched_frame["suspect_race"] != "unknown").sum()) / total * 100.0,
            },
        ]
    )
    _write_tabular_outputs(
        output_directory,
        "summary_demographics.csv",
        summary_demographics,
        write_excel_autofit=write_excel_autofit,
    )

    _write_tabular_outputs(
        output_directory,
        "fetch_failures.csv",
        pd.DataFrame(failure_records),
        write_excel_autofit=write_excel_autofit,
    )
    human_review_queue = _build_human_review_queue(enriched_frame)
    _write_tabular_outputs(
        output_directory,
        "human_review_queue.csv",
        human_review_queue,
        write_excel_autofit=write_excel_autofit,
    )
    domain_fetch_summary = _build_domain_fetch_summary(enriched_frame)
    _write_tabular_outputs(
        output_directory,
        "domain_fetch_summary.csv",
        domain_fetch_summary,
        write_excel_autofit=write_excel_autofit,
    )
    domain_review_summary = _build_domain_review_summary(enriched_frame)
    _write_tabular_outputs(
        output_directory,
        "domain_review_summary.csv",
        domain_review_summary,
        write_excel_autofit=write_excel_autofit,
    )
    review_reason_summary = _build_review_reason_summary(enriched_frame)
    _write_tabular_outputs(
        output_directory,
        "review_reason_summary.csv",
        review_reason_summary,
        write_excel_autofit=write_excel_autofit,
    )

    report = _summarize(enriched_frame)
    console_text = build_console_report(report, summary_by_category)
    _log(console_text)
    return report
