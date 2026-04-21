from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

SOCIAL_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "nextdoor.com",
    "threads.net",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}

FUNDRAISING_DOMAINS = {
    "gofundme.com",
    "givebutter.com",
    "givesendgo.com",
}

HIGH_FRICTION_DOMAINS = {
    "charlotteobserver.com",
    "islandpacket.com",
    "kansascity.com",
    "star-telegram.com",
    "thenewsstar.com",
}

OFFICIAL_HOST_SUFFIXES = {
    "nixle.us",
    "nopdnews.com",
    "sanfranciscopolice.org",
    "sjpd.org",
    "atlantapd.org",
}

OFFICIAL_HOST_TOKENS = {
    "police",
    "publicsafety",
    "public-safety",
    "sheriff",
    "statepatrol",
}

REJECT_PATH_TOKENS = {
    "accident",
    "collision",
    "crash",
    "multi-vehicle",
    "vehicle-crash",
    "wreck",
}

PRIMARY_SOURCE_CATEGORIES = {"OFFICIAL", "NEWS", "PAYWALL_OR_HIGH_FRICTION"}
RECOVERABLE_403_CATEGORIES = {"OFFICIAL", "NEWS", "PAYWALL_OR_HIGH_FRICTION"}
RETRYABLE_EXCEPTION_NAMES = {"ConnectTimeout", "ReadTimeout"}


@dataclass(slots=True, frozen=True)
class SourcePolicy:
    normalized_url: str | None
    domain: str
    category: str
    should_fetch: bool
    preferred: bool
    action: str
    status: str
    reason: str


def extract_source_domain(source_url: object) -> str:
    if not source_url:
        return "unknown"
    try:
        parsed = urlparse(str(source_url))
    except ValueError:
        return "unknown"
    hostname = parsed.hostname
    return hostname.lower() if hostname else "unknown"


def _domain_matches(hostname: str, candidates: set[str]) -> bool:
    return any(hostname == candidate or hostname.endswith(f".{candidate}") for candidate in candidates)


def _has_official_signal(hostname: str, path: str) -> bool:
    if hostname.endswith(".gov") or _domain_matches(hostname, OFFICIAL_HOST_SUFFIXES):
        return True
    if any(token in hostname for token in OFFICIAL_HOST_TOKENS):
        return True
    return ".gov/" in path


def _has_reject_signal(path: str) -> bool:
    lowered = path.lower()
    return any(token in lowered for token in REJECT_PATH_TOKENS)


def _normalize_url(source_url: str | None) -> str | None:
    if not source_url:
        return None
    value = source_url.strip()
    if not value:
        return None
    try:
        parsed = urlparse(value)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return value


def classify_source_url(source_url: str | None) -> SourcePolicy:
    normalized_url = _normalize_url(source_url)
    if normalized_url is None:
        reason = "missing_source_url" if not source_url else "malformed_source_url"
        return SourcePolicy(
            normalized_url=None,
            domain="unknown",
            category="REJECT",
            should_fetch=False,
            preferred=False,
            action="skip",
            status="source_not_supported",
            reason=reason,
        )

    parsed = urlparse(normalized_url)
    hostname = (parsed.hostname or "unknown").lower()
    path = parsed.path.lower()

    if _has_reject_signal(path):
        return SourcePolicy(
            normalized_url=normalized_url,
            domain=hostname,
            category="REJECT",
            should_fetch=False,
            preferred=False,
            action="reject",
            status="rejected_source",
            reason="rejected_irrelevant_source",
        )

    if _domain_matches(hostname, SOCIAL_DOMAINS):
        return SourcePolicy(
            normalized_url=normalized_url,
            domain=hostname,
            category="SOCIAL_SUPPLEMENTAL",
            should_fetch=False,
            preferred=False,
            action="skip",
            status="source_not_supported",
            reason="social_primary_source_unsupported",
        )

    if _domain_matches(hostname, FUNDRAISING_DOMAINS):
        return SourcePolicy(
            normalized_url=normalized_url,
            domain=hostname,
            category="FUNDRAISING_SUPPLEMENTAL",
            should_fetch=False,
            preferred=False,
            action="skip",
            status="source_not_supported",
            reason="fundraising_primary_source_unsupported",
        )

    if _has_official_signal(hostname, path):
        return SourcePolicy(
            normalized_url=normalized_url,
            domain=hostname,
            category="OFFICIAL",
            should_fetch=True,
            preferred=True,
            action="fetch",
            status="fetch",
            reason="official_primary_source",
        )

    if _domain_matches(hostname, HIGH_FRICTION_DOMAINS):
        return SourcePolicy(
            normalized_url=normalized_url,
            domain=hostname,
            category="PAYWALL_OR_HIGH_FRICTION",
            should_fetch=True,
            preferred=False,
            action="fetch",
            status="fetch",
            reason="high_friction_primary_source",
        )

    return SourcePolicy(
        normalized_url=normalized_url,
        domain=hostname,
        category="NEWS",
        should_fetch=True,
        preferred=False,
        action="fetch",
        status="fetch",
        reason="news_primary_source",
    )
