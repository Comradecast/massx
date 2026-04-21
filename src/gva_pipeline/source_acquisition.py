from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable
from urllib.parse import ParseResult, parse_qsl, urlencode, urlparse

import requests

from .manual_reviews import VALID_DECISION_TYPES
from .models import FetchResult, IncidentAcquisitionResult, IncidentRecord, ManualReviewRecord
from .source_policy import SourcePolicy, classify_source_url, extract_source_domain

FetchFunction = Callable[..., FetchResult]

SOURCE_CATEGORY_PRIORITY = {
    "OFFICIAL": 0,
    "NEWS": 1,
    "PAYWALL_OR_HIGH_FRICTION": 2,
}
KNOWN_OFFICIAL_DOMAIN_VARIANTS = {
    ("alabama", "birmingham"): ("police.birminghamal.gov",),
    ("california", "san francisco"): ("sanfranciscopolice.org", "www.sanfranciscopolice.org"),
    ("california", "san jose"): ("sjpd.org", "www.sjpd.org"),
    ("georgia", "atlanta"): ("atlantapd.org", "www.atlantapd.org"),
}
TRACKING_QUERY_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
}
TERMINAL_MANUAL_REVIEW_DECISIONS = {
    "mark_no_viable_source_found": ("no_viable_source_found", "manual_review_no_viable_source_found"),
    "mark_irrelevant_incident": ("rejected_source", "manual_review_irrelevant_incident"),
    "needs_more_research": ("needs_more_research", "manual_review_needs_more_research"),
}


@dataclass(slots=True, frozen=True)
class PreparedSourceCandidate:
    source_url: str
    source_policy: SourcePolicy
    origin: str
    original_index: int


@dataclass(slots=True, frozen=True)
class CandidateEntry:
    source_url: str
    origin: str


@dataclass(slots=True, frozen=True)
class CandidatePreparation:
    prepared_candidates: tuple[PreparedSourceCandidate, ...]
    manual_review_applied: bool
    manual_review_status: str | None
    manual_review_decision_type: str | None
    manual_review_preferred_source_url: str | None
    manual_review_added_candidates: tuple[str, ...]
    manual_review_rejected_candidates: tuple[str, ...]
    manual_review_notes: str | None
    manual_review_reviewer: str | None
    manual_review_timestamp: str | None
    short_circuit_status: str | None = None
    short_circuit_reason: str | None = None

    def __iter__(self):
        return iter(self.prepared_candidates)

    def __getitem__(self, index):
        return self.prepared_candidates[index]

    def __len__(self) -> int:
        return len(self.prepared_candidates)


def build_source_candidates(
    source_url: str | None,
    source_candidates: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()

    for candidate in [source_url, *(source_candidates or ())]:
        if not candidate:
            continue
        normalized = _normalize_candidate_url(candidate)
        if not normalized:
            continue
        dedupe_key = _candidate_dedupe_key(normalized)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        ordered.append(normalized)

    return tuple(ordered)


def expand_source_candidates(
    incident: IncidentRecord,
    source_candidates: tuple[str, ...],
) -> tuple[str, ...]:
    trusted_hosts = _known_official_hosts_for_incident(incident)
    expanded: list[str] = list(source_candidates)

    if trusted_hosts:
        for candidate in source_candidates:
            inferred_variants = _infer_official_candidate_variants(candidate, trusted_hosts)
            expanded.extend(inferred_variants)

    deduped = build_source_candidates(None, expanded)
    return _promote_trusted_candidates(deduped, trusted_hosts)


def prepare_source_candidates(incident: IncidentRecord) -> CandidatePreparation:
    candidate_entries = _build_base_candidate_entries(incident)
    (
        candidate_entries,
        manual_review_applied,
        manual_review_preferred_source_url,
        manual_review_added_candidates,
        manual_review_rejected_candidates,
        short_circuit_status,
        short_circuit_reason,
    ) = _apply_manual_review(candidate_entries, incident.manual_review)
    expanded_entries = _expand_candidate_entries(incident, candidate_entries)

    prepared = tuple(
        PreparedSourceCandidate(
            source_url=entry.source_url,
            source_policy=classify_source_url(entry.source_url),
            origin=entry.origin,
            original_index=index,
        )
        for index, entry in enumerate(expanded_entries)
    )
    ranked = tuple(
        sorted(
            prepared,
            key=lambda candidate: (
                _source_category_priority(candidate.source_policy.category),
                candidate.original_index,
            ),
        )
    )

    manual_review = incident.manual_review
    return CandidatePreparation(
        prepared_candidates=ranked,
        manual_review_applied=manual_review_applied,
        manual_review_status=manual_review.review_status if manual_review else None,
        manual_review_decision_type=manual_review.decision_type if manual_review else None,
        manual_review_preferred_source_url=manual_review_preferred_source_url,
        manual_review_added_candidates=manual_review_added_candidates,
        manual_review_rejected_candidates=manual_review_rejected_candidates,
        manual_review_notes=manual_review.review_notes if manual_review else None,
        manual_review_reviewer=manual_review.reviewer if manual_review else None,
        manual_review_timestamp=manual_review.review_timestamp if manual_review else None,
        short_circuit_status=short_circuit_status,
        short_circuit_reason=short_circuit_reason,
    )


def acquire_incident_sources(
    incident: IncidentRecord,
    *,
    fetch_fn: FetchFunction,
    session: requests.Session,
    timeout_seconds: float,
    store_raw_html: bool,
    fetch_cache: dict[str, FetchResult],
) -> IncidentAcquisitionResult:
    preparation = prepare_source_candidates(incident)
    prepared_candidates = preparation.prepared_candidates
    attempt_history: list[dict[str, object]] = []
    final_result: FetchResult | None = None
    final_relevant_result: FetchResult | None = None
    selected_source_url: str | None = None
    selected_source_origin = "unknown"

    if preparation.short_circuit_status and preparation.short_circuit_reason:
        short_circuit_result = FetchResult(
            requested_url=None,
            final_url=None,
            status_code=None,
            ok=False,
            error=preparation.short_circuit_reason,
            article_text=None,
            acquisition_status=preparation.short_circuit_status,
            failure_stage="manual_review",
            failure_reason=preparation.short_circuit_reason,
            source_category="REJECT" if preparation.short_circuit_status == "rejected_source" else "UNKNOWN",
            source_action="skip",
            attempts=0,
        )
        return IncidentAcquisitionResult(
            fetch_result=short_circuit_result,
            selected_source_url=None,
            selected_source_origin="unknown",
            source_candidates_count=len(prepared_candidates),
            source_attempt_count=0,
            source_attempt_history="[]",
            manual_review_applied=preparation.manual_review_applied,
            manual_review_status=preparation.manual_review_status,
            manual_review_decision_type=preparation.manual_review_decision_type,
            manual_review_preferred_source_url=preparation.manual_review_preferred_source_url,
            manual_review_added_candidates=_serialize_urls(preparation.manual_review_added_candidates),
            manual_review_rejected_candidates=_serialize_urls(preparation.manual_review_rejected_candidates),
            manual_review_notes=preparation.manual_review_notes,
            manual_review_reviewer=preparation.manual_review_reviewer,
            manual_review_timestamp=preparation.manual_review_timestamp,
        )

    for ranked_index, prepared_candidate in enumerate(prepared_candidates, start=1):
        candidate = prepared_candidate.source_url
        from_cache = candidate in fetch_cache
        if from_cache:
            fetch_result = fetch_cache[candidate]
        else:
            fetch_result = fetch_fn(
                candidate,
                session=session,
                timeout_seconds=timeout_seconds,
                store_raw_html=store_raw_html,
            )
            fetch_cache[candidate] = fetch_result

        acquisition_status, failure_stage, failure_reason = _coalesce_failure_metadata(fetch_result)

        attempt_history.append(
            {
                "candidate_index": ranked_index,
                "source_url": candidate,
                "source_domain": prepared_candidate.source_policy.domain,
                "source_origin": prepared_candidate.origin,
                "success": fetch_result.ok,
                "from_cache": from_cache,
                "ranked_source_category": prepared_candidate.source_policy.category,
                "ranked_source_priority": _source_category_priority(prepared_candidate.source_policy.category),
                "original_candidate_index": prepared_candidate.original_index + 1,
                "acquisition_status": acquisition_status,
                "failure_stage": failure_stage,
                "failure_reason": failure_reason,
                "fetch_status_code": fetch_result.status_code,
                "fetch_request_domain": extract_source_domain(fetch_result.requested_url or candidate),
                "fetch_final_domain": extract_source_domain(fetch_result.final_url),
                "fetch_domain_changed": _domains_differ(
                    extract_source_domain(fetch_result.requested_url or candidate),
                    extract_source_domain(fetch_result.final_url),
                ),
                "source_category": fetch_result.source_category,
                "source_action": fetch_result.source_action,
                "fetch_attempts": fetch_result.attempts,
            }
        )

        final_result = fetch_result
        if acquisition_status not in {"source_not_supported", "rejected_source"}:
            final_relevant_result = fetch_result

        if fetch_result.ok:
            selected_source_url = candidate
            selected_source_origin = prepared_candidate.origin
            break

    chosen_result = final_result
    if selected_source_url is None and final_relevant_result is not None:
        chosen_result = final_relevant_result

    if chosen_result is None:
        chosen_result = FetchResult(
            requested_url=None,
            final_url=None,
            status_code=None,
            ok=False,
            error="missing_source_url",
            article_text=None,
            acquisition_status="source_not_supported",
            failure_stage="source_policy",
            failure_reason="missing_source_url",
            source_category="REJECT",
            source_action="skip",
            attempts=0,
        )

    return IncidentAcquisitionResult(
        fetch_result=chosen_result,
        selected_source_url=selected_source_url,
        selected_source_origin=selected_source_origin,
        source_candidates_count=len(prepared_candidates),
        source_attempt_count=len(attempt_history),
        source_attempt_history=json.dumps(attempt_history, ensure_ascii=False),
        manual_review_applied=preparation.manual_review_applied,
        manual_review_status=preparation.manual_review_status,
        manual_review_decision_type=preparation.manual_review_decision_type,
        manual_review_preferred_source_url=preparation.manual_review_preferred_source_url,
        manual_review_added_candidates=_serialize_urls(preparation.manual_review_added_candidates),
        manual_review_rejected_candidates=_serialize_urls(preparation.manual_review_rejected_candidates),
        manual_review_notes=preparation.manual_review_notes,
        manual_review_reviewer=preparation.manual_review_reviewer,
        manual_review_timestamp=preparation.manual_review_timestamp,
    )


def _source_category_priority(category: str) -> int:
    return SOURCE_CATEGORY_PRIORITY.get(category, 3)


def _normalize_candidate_url(candidate: str) -> str | None:
    value = candidate.strip()
    if not value:
        return None
    try:
        parsed = urlparse(value)
    except ValueError:
        return value
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return value

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return value

    port = parsed.port
    if port and not ((parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"

    normalized = ParseResult(
        scheme=parsed.scheme.lower(),
        netloc=netloc,
        path=path,
        params=parsed.params,
        query=parsed.query,
        fragment="",
    )
    return normalized.geturl()


def _candidate_dedupe_key(candidate: str) -> str:
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return candidate

    if not parsed.query:
        return candidate

    filtered_query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_PARAMS
    ]
    filtered_query = urlencode(filtered_query_pairs, doseq=True)

    dedupe_url = ParseResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=parsed.path,
        params=parsed.params,
        query=filtered_query,
        fragment="",
    )
    return dedupe_url.geturl()


def _build_base_candidate_entries(incident: IncidentRecord) -> tuple[CandidateEntry, ...]:
    base_candidates = build_source_candidates(incident.source_url, incident.source_candidates)
    origin_by_url = dict(incident.source_candidate_origins)
    return tuple(
        CandidateEntry(
            source_url=candidate,
            origin=origin_by_url.get(candidate, "unknown"),
        )
        for candidate in base_candidates
    )


def _apply_manual_review(
    candidate_entries: tuple[CandidateEntry, ...],
    manual_review: ManualReviewRecord | None,
) -> tuple[
    tuple[CandidateEntry, ...],
    bool,
    str | None,
    tuple[str, ...],
    tuple[str, ...],
    str | None,
    str | None,
]:
    if manual_review is None:
        return (candidate_entries, False, None, (), (), None, None)

    if manual_review.decision_type and manual_review.decision_type not in VALID_DECISION_TYPES:
        raise ValueError(
            f"Unsupported manual review decision_type for incident_id={manual_review.incident_id}: "
            f"{manual_review.decision_type}"
        )

    entries = list(candidate_entries)
    added_candidates = build_source_candidates(None, manual_review.added_source_candidates)
    rejected_candidates = build_source_candidates(None, manual_review.rejected_candidates)
    preferred_candidates = build_source_candidates(manual_review.preferred_source_url)
    preferred_source_url = preferred_candidates[0] if preferred_candidates else None

    rejected_keys = {_canonical_candidate_key(candidate) for candidate in rejected_candidates}
    if rejected_keys:
        entries = [
            entry
            for entry in entries
            if _canonical_candidate_key(entry.source_url) not in rejected_keys
        ]

    existing_keys = {_canonical_candidate_key(entry.source_url) for entry in entries}
    for candidate in added_candidates:
        candidate_key = _canonical_candidate_key(candidate)
        if candidate_key in existing_keys:
            continue
        entries.append(CandidateEntry(source_url=candidate, origin="manual_review"))
        existing_keys.add(candidate_key)

    if preferred_source_url:
        preferred_key = _canonical_candidate_key(preferred_source_url)
        if preferred_key not in existing_keys:
            entries.append(CandidateEntry(source_url=preferred_source_url, origin="manual_review"))
        preferred_entries = [
            entry for entry in entries if _canonical_candidate_key(entry.source_url) == preferred_key
        ]
        other_entries = [
            entry for entry in entries if _canonical_candidate_key(entry.source_url) != preferred_key
        ]
        entries = [*preferred_entries, *other_entries]

    short_circuit_status = None
    short_circuit_reason = None
    if manual_review.decision_type in TERMINAL_MANUAL_REVIEW_DECISIONS:
        short_circuit_status, short_circuit_reason = TERMINAL_MANUAL_REVIEW_DECISIONS[manual_review.decision_type]

    manual_review_applied = bool(
        added_candidates
        or rejected_candidates
        or preferred_source_url
        or short_circuit_status
    )
    return (
        tuple(entries),
        manual_review_applied,
        preferred_source_url,
        added_candidates,
        rejected_candidates,
        short_circuit_status,
        short_circuit_reason,
    )


def _expand_candidate_entries(
    incident: IncidentRecord,
    candidate_entries: tuple[CandidateEntry, ...],
) -> tuple[CandidateEntry, ...]:
    expanded_urls = build_source_candidates(
        None,
        expand_source_candidates(incident, tuple(entry.source_url for entry in candidate_entries)),
    )
    existing_origins = {entry.source_url: entry.origin for entry in candidate_entries}
    return tuple(
        CandidateEntry(
            source_url=source_url,
            origin=existing_origins.get(source_url, "expanded"),
        )
        for source_url in expanded_urls
    )


def _canonical_candidate_key(candidate: str) -> str:
    normalized = _normalize_candidate_url(candidate)
    if not normalized:
        return candidate
    return _candidate_dedupe_key(normalized)


def _normalize_location_token(value: str | None) -> str:
    if not value:
        return ""
    token = " ".join(value.lower().split())
    if "(" in token:
        token = token.split("(", 1)[0].strip()
    return token


def _known_official_hosts_for_incident(incident: IncidentRecord) -> tuple[str, ...]:
    state = _normalize_location_token(incident.state)
    city = _normalize_location_token(incident.city_or_county)
    return KNOWN_OFFICIAL_DOMAIN_VARIANTS.get((state, city), ())


def _infer_official_candidate_variants(candidate: str, trusted_hosts: tuple[str, ...]) -> tuple[str, ...]:
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return ()
    hostname = (parsed.hostname or "").lower()
    if not hostname or not trusted_hosts or hostname not in trusted_hosts:
        return ()

    variants: list[str] = []
    path = parsed.path or "/"
    if path == "/":
        return ()

    for trusted_host in trusted_hosts:
        if trusted_host == hostname:
            continue
        variant = ParseResult(
            scheme=parsed.scheme or "https",
            netloc=trusted_host,
            path=path,
            params=parsed.params,
            query=parsed.query,
            fragment="",
        ).geturl()
        variants.append(variant)

    return tuple(variants)


def _promote_trusted_candidates(
    source_candidates: tuple[str, ...],
    trusted_hosts: tuple[str, ...],
) -> tuple[str, ...]:
    if not trusted_hosts:
        return source_candidates

    trusted: list[str] = []
    others: list[str] = []
    trusted_host_set = set(trusted_hosts)
    for candidate in source_candidates:
        try:
            hostname = (urlparse(candidate).hostname or "").lower()
        except ValueError:
            hostname = ""
        if hostname in trusted_host_set:
            trusted.append(candidate)
        else:
            others.append(candidate)
    return tuple([*trusted, *others])


def _domains_differ(request_domain: str, final_domain: str) -> bool:
    return (
        request_domain != "unknown"
        and final_domain != "unknown"
        and request_domain != final_domain
    )


def _serialize_urls(values: tuple[str, ...]) -> str:
    if not values:
        return ""
    return json.dumps(list(values), ensure_ascii=False)


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
