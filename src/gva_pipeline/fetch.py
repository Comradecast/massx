from __future__ import annotations

from pathlib import Path
import time
from urllib.parse import urlparse

import requests

from .fetch_policy import classify_extraction_failure, classify_http_failure, classify_request_exception, get_attempt_policy
from .models import FetchResult
from .parse_articles import extract_main_article_text
from .source_policy import classify_source_url

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def fetch_source(
    source_url: str | None,
    *,
    session: requests.Session,
    timeout_seconds: float = 8.0,
    store_raw_html: bool = False,
    sleep_fn=time.sleep,
) -> FetchResult:
    source_policy = classify_source_url(source_url)
    if not source_policy.should_fetch:
        return FetchResult(
            requested_url=source_url,
            final_url=None,
            status_code=None,
            ok=False,
            error=source_policy.reason,
            article_text=None,
            acquisition_status=source_policy.status,
            failure_stage="source_policy",
            failure_reason=source_policy.reason,
            source_category=source_policy.category,
            source_action=source_policy.action,
            attempts=0,
        )

    attempt_policy = get_attempt_policy(source_policy)
    normalized_url = source_policy.normalized_url or source_url
    attempt_timeout_seconds = timeout_seconds

    for attempt_number in range(1, attempt_policy.max_attempts + 1):
        headers = _build_attempt_headers(attempt_number)

        try:
            response = session.get(
                normalized_url,
                timeout=attempt_timeout_seconds,
                allow_redirects=True,
                headers=headers,
            )
        except requests.RequestException as exc:
            decision = classify_request_exception(
                source_policy,
                exc.__class__.__name__,
                attempt_number=attempt_number,
                attempt_policy=attempt_policy,
            )
            if decision.retry:
                if decision.status == "timeout":
                    attempt_timeout_seconds += attempt_policy.timeout_retry_increment_seconds
                sleep_fn(decision.backoff_seconds)
                continue
            return FetchResult(
                requested_url=normalized_url,
                final_url=None,
                status_code=None,
                ok=False,
                error=f"request_error: {exc.__class__.__name__}",
                article_text=None,
                acquisition_status=decision.status,
                failure_stage=decision.stage,
                failure_reason=decision.reason,
                source_category=source_policy.category,
                source_action=source_policy.action,
                retryable=decision.retryable,
                attempts=attempt_number,
            )

        raw_html = response.text if store_raw_html else None
        final_url = str(response.url)
        status_code = response.status_code

        if status_code >= 400:
            decision = classify_http_failure(
                source_policy,
                status_code,
                attempt_number=attempt_number,
                attempt_policy=attempt_policy,
                retry_after_seconds=_parse_retry_after_seconds(response.headers.get("Retry-After")),
            )
            if decision.retry:
                sleep_fn(decision.backoff_seconds)
                continue
            return FetchResult(
                requested_url=normalized_url,
                final_url=final_url,
                status_code=status_code,
                ok=False,
                error=f"http_{status_code}",
                article_text=None,
                raw_html=raw_html,
                acquisition_status=decision.status,
                failure_stage=decision.stage,
                failure_reason=decision.reason,
                source_category=source_policy.category,
                source_action=source_policy.action,
                retryable=decision.retryable,
                attempts=attempt_number,
            )

        article_text = extract_main_article_text(response.text)
        if not article_text:
            decision = classify_extraction_failure()
            return FetchResult(
                requested_url=normalized_url,
                final_url=final_url,
                status_code=status_code,
                ok=False,
                error="article_text_not_found",
                article_text=None,
                raw_html=raw_html,
                acquisition_status=decision.status,
                failure_stage=decision.stage,
                failure_reason=decision.reason,
                source_category=source_policy.category,
                source_action=source_policy.action,
                retryable=decision.retryable,
                attempts=attempt_number,
            )

        return FetchResult(
            requested_url=normalized_url,
            final_url=final_url,
            status_code=status_code,
            ok=True,
            error=None,
            article_text=article_text,
            raw_html=raw_html,
            acquisition_status="fetched",
            failure_stage=None,
            failure_reason=None,
            source_category=source_policy.category,
            source_action=source_policy.action,
            retryable=False,
            attempts=attempt_number,
        )

    return FetchResult(
        requested_url=normalized_url,
        final_url=None,
        status_code=None,
        ok=False,
        error="request_error: exhausted_attempts",
        article_text=None,
        acquisition_status="fetch_failed",
        failure_stage="fetch",
        failure_reason="request_error_exhausted_attempts",
        source_category=source_policy.category,
        source_action=source_policy.action,
        retryable=True,
        attempts=attempt_policy.max_attempts,
    )


def _build_attempt_headers(attempt_number: int) -> dict[str, str] | None:
    if attempt_number == 1:
        return None
    return {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
    }


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return seconds if seconds > 0 else None


def save_raw_html(fetch_result: FetchResult, destination: str | Path, *, incident_id: str) -> Path | None:
    if not fetch_result.raw_html:
        return None
    directory = Path(destination)
    directory.mkdir(parents=True, exist_ok=True)
    suffix = urlparse(fetch_result.final_url or fetch_result.requested_url or "").netloc.replace(".", "_")
    filename = f"{incident_id}_{suffix or 'source'}.html"
    path = directory / filename
    path.write_text(fetch_result.raw_html, encoding="utf-8")
    return path
