from __future__ import annotations

from dataclasses import dataclass, field

import requests

from gva_pipeline.fetch import fetch_source
from gva_pipeline.source_policy import classify_source_url


ARTICLE_HTML = """
<html>
  <body>
    <article>
      <p>Police said four people were shot during a late-night gathering and investigators are still reviewing video from the scene.</p>
      <p>Detectives said the suspect was arrested after witnesses identified the shooter.</p>
    </article>
  </body>
</html>
"""


@dataclass
class FakeResponse:
    status_code: int
    text: str = ""
    url: str = "https://example.com/story"
    headers: dict[str, str] = field(default_factory=dict)


class SequencedSession:
    def __init__(self, outcomes: list[FakeResponse | Exception]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        url: str,
        *,
        timeout: float,
        allow_redirects: bool,
        headers: dict[str, str] | None = None,
    ) -> FakeResponse:
        self.calls.append(
            {
                "url": url,
                "timeout": timeout,
                "allow_redirects": allow_redirects,
                "headers": headers or {},
            }
        )
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_fetch_source_retries_read_timeout_then_recovers() -> None:
    sleeps: list[float] = []
    session = SequencedSession(
        [
            requests.ReadTimeout("slow upstream"),
            FakeResponse(status_code=200, text=ARTICLE_HTML),
        ]
    )

    result = fetch_source(
        "https://example.com/story",
        session=session,
        timeout_seconds=8.0,
        sleep_fn=sleeps.append,
    )

    assert result.ok is True
    assert result.attempts == 2
    assert sleeps == [1.0]
    assert [call["timeout"] for call in session.calls] == [8.0, 10.0]
    assert session.calls[1]["headers"] == {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
    }


def test_fetch_source_retries_429_with_retry_after() -> None:
    sleeps: list[float] = []
    session = SequencedSession(
        [
            FakeResponse(status_code=429, headers={"Retry-After": "7"}),
            FakeResponse(status_code=200, text=ARTICLE_HTML),
        ]
    )

    result = fetch_source(
        "https://example.com/story",
        session=session,
        timeout_seconds=8.0,
        sleep_fn=sleeps.append,
    )

    assert result.ok is True
    assert result.attempts == 2
    assert sleeps == [7.0]
    assert [call["timeout"] for call in session.calls] == [8.0, 8.0]


def test_fetch_source_marks_404_as_permanent_not_found() -> None:
    session = SequencedSession([FakeResponse(status_code=404, url="https://example.com/missing")])

    result = fetch_source("https://example.com/missing", session=session, sleep_fn=lambda _: None)

    assert result.ok is False
    assert result.acquisition_status == "permanent_not_found"
    assert result.failure_reason == "http_404"
    assert result.retryable is False
    assert result.attempts == 1


def test_fetch_source_distinguishes_extraction_failure_from_fetch_failure() -> None:
    session = SequencedSession([FakeResponse(status_code=200, text="<html><body></body></html>")])

    result = fetch_source("https://example.com/empty", session=session, sleep_fn=lambda _: None)

    assert result.ok is False
    assert result.status_code == 200
    assert result.acquisition_status == "extraction_failed"
    assert result.failure_stage == "extraction"
    assert result.failure_reason == "article_text_not_found"


def test_source_policy_classifies_domains() -> None:
    official = classify_source_url(
        "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west/"
    )
    news = classify_source_url("https://www.wlbt.com/2026/04/19/story/")
    social = classify_source_url("https://x.com/LAPDPIO/status/2039893572577792419")
    fundraising = classify_source_url("https://www.gofundme.com/f/help-feliciana-family")
    reject = classify_source_url(
        "https://abc3340.com/news/local/alabama-three-injured-in-late-night-multi-vehicle-crash-in-moody-police-department-december-31-2025-at-us-411-and-verbena"
    )

    assert official.category == "OFFICIAL"
    assert official.should_fetch is True
    assert news.category == "NEWS"
    assert social.category == "SOCIAL_SUPPLEMENTAL"
    assert social.status == "source_not_supported"
    assert fundraising.category == "FUNDRAISING_SUPPLEMENTAL"
    assert fundraising.should_fetch is False
    assert reject.category == "REJECT"
    assert reject.status == "rejected_source"
