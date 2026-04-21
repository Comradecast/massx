from __future__ import annotations

from dataclasses import dataclass

from .source_policy import RECOVERABLE_403_CATEGORIES, RETRYABLE_EXCEPTION_NAMES, SourcePolicy


@dataclass(slots=True, frozen=True)
class AttemptPolicy:
    max_attempts: int
    timeout_backoff_seconds: tuple[float, ...]
    rate_limit_backoff_seconds: tuple[float, ...]
    http_403_backoff_seconds: tuple[float, ...]
    timeout_retry_increment_seconds: float


@dataclass(slots=True, frozen=True)
class FailureDecision:
    status: str
    reason: str
    stage: str
    retryable: bool
    retry: bool
    backoff_seconds: float = 0.0


DEFAULT_ATTEMPT_POLICY = AttemptPolicy(
    max_attempts=3,
    timeout_backoff_seconds=(1.0, 2.0),
    rate_limit_backoff_seconds=(2.0, 5.0),
    http_403_backoff_seconds=(1.0,),
    timeout_retry_increment_seconds=2.0,
)

HTTP_STATUS_TO_FINAL_STATUS = {
    400: "fetch_failed",
    403: "fetch_failed",
    404: "permanent_not_found",
    429: "rate_limited",
}

REQUEST_EXCEPTION_TO_FINAL_STATUS = {
    "ConnectTimeout": "timeout",
    "ReadTimeout": "timeout",
}


def get_attempt_policy(source_policy: SourcePolicy) -> AttemptPolicy:
    if source_policy.category == "PAYWALL_OR_HIGH_FRICTION":
        return AttemptPolicy(
            max_attempts=2,
            timeout_backoff_seconds=(1.5,),
            rate_limit_backoff_seconds=(3.0,),
            http_403_backoff_seconds=(1.5,),
            timeout_retry_increment_seconds=2.0,
        )
    return DEFAULT_ATTEMPT_POLICY


def classify_request_exception(
    source_policy: SourcePolicy,
    exception_name: str,
    *,
    attempt_number: int,
    attempt_policy: AttemptPolicy,
) -> FailureDecision:
    status = REQUEST_EXCEPTION_TO_FINAL_STATUS.get(exception_name, "fetch_failed")
    retryable = exception_name in RETRYABLE_EXCEPTION_NAMES
    retry = retryable and attempt_number < attempt_policy.max_attempts
    backoff = 0.0
    if retry:
        backoff = attempt_policy.timeout_backoff_seconds[attempt_number - 1]
    return FailureDecision(
        status=status,
        reason=f"request_error_{exception_name.lower()}",
        stage="fetch",
        retryable=retryable,
        retry=retry,
        backoff_seconds=backoff,
    )


def classify_http_failure(
    source_policy: SourcePolicy,
    status_code: int,
    *,
    attempt_number: int,
    attempt_policy: AttemptPolicy,
    retry_after_seconds: float | None = None,
) -> FailureDecision:
    status = HTTP_STATUS_TO_FINAL_STATUS.get(status_code, "fetch_failed")
    retryable = False
    retry = False
    backoff = 0.0

    if status_code == 429:
        retryable = True
        retry = attempt_number < attempt_policy.max_attempts
        if retry:
            backoff = retry_after_seconds or attempt_policy.rate_limit_backoff_seconds[attempt_number - 1]
    elif status_code == 403 and source_policy.category in RECOVERABLE_403_CATEGORIES:
        retryable = True
        retry = attempt_number < attempt_policy.max_attempts
        if retry:
            backoff = attempt_policy.http_403_backoff_seconds[min(attempt_number - 1, len(attempt_policy.http_403_backoff_seconds) - 1)]

    return FailureDecision(
        status=status,
        reason=f"http_{status_code}",
        stage="fetch",
        retryable=retryable,
        retry=retry,
        backoff_seconds=backoff,
    )


def classify_extraction_failure() -> FailureDecision:
    return FailureDecision(
        status="extraction_failed",
        reason="article_text_not_found",
        stage="extraction",
        retryable=False,
        retry=False,
    )
