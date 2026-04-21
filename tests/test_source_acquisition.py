from __future__ import annotations

import json

import requests

from gva_pipeline.models import FetchResult, IncidentRecord
from gva_pipeline.source_acquisition import (
    acquire_incident_sources,
    expand_source_candidates,
    prepare_source_candidates,
)


def _incident_with_candidates(*source_candidates: str) -> IncidentRecord:
    return IncidentRecord(
        incident_id="rank-1",
        incident_date=None,
        state="TX",
        city_or_county="Austin",
        address="1 Main St",
        victims_killed=0,
        victims_injured=4,
        suspects_killed=0,
        suspects_injured=0,
        suspects_arrested=1,
        incident_url="https://example.com/incidents/rank-1",
        source_url=None,
        source_candidates=tuple(source_candidates),
    )


def test_prepare_source_candidates_ranks_by_category_and_preserves_stable_order() -> None:
    incident = _incident_with_candidates(
        "https://www.wlbt.com/news-story-a",
        "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west/",
        "https://www.charlotteobserver.com/high-friction-story",
        "https://www.wdbj7.com/news-story-b",
        "https://x.com/example/status/1",
    )

    prepared = prepare_source_candidates(incident)

    assert [candidate.source_url for candidate in prepared] == [
        "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west",
        "https://www.wlbt.com/news-story-a",
        "https://www.wdbj7.com/news-story-b",
        "https://www.charlotteobserver.com/high-friction-story",
        "https://x.com/example/status/1",
    ]
    assert [candidate.source_policy.category for candidate in prepared] == [
        "OFFICIAL",
        "NEWS",
        "NEWS",
        "PAYWALL_OR_HIGH_FRICTION",
        "SOCIAL_SUPPLEMENTAL",
    ]


def test_default_candidate_expansion_hook_is_identity() -> None:
    incident = _incident_with_candidates("https://www.wlbt.com/news-story-a")
    assert expand_source_candidates(incident, incident.source_candidates) == incident.source_candidates


def test_candidate_enrichment_adds_known_official_domain_variants() -> None:
    incident = IncidentRecord(
        incident_id="rank-1b",
        incident_date=None,
        state="Georgia",
        city_or_county="Atlanta",
        address="1 Main St",
        victims_killed=0,
        victims_injured=4,
        suspects_killed=0,
        suspects_injured=0,
        suspects_arrested=1,
        incident_url="https://example.com/incidents/rank-1b",
        source_url="https://atlantapd.org/Home/Components/News/News/7551/631",
        source_candidates=(),
    )

    expanded = expand_source_candidates(
        incident,
        ("https://atlantapd.org/Home/Components/News/News/7551/631",),
    )

    assert expanded == (
        "https://atlantapd.org/Home/Components/News/News/7551/631",
        "https://www.atlantapd.org/Home/Components/News/News/7551/631",
    )


def test_build_source_candidates_normalizes_and_deduplicates() -> None:
    from gva_pipeline.source_acquisition import build_source_candidates

    candidates = build_source_candidates(
        " HTTPS://WWW.WLBT.COM/news-story-a/#top ",
        (
            "https://www.wlbt.com/news-story-a",
            "https://www.wlbt.com/news-story-a/",
            "https://www.wdbj7.com/news-story-b",
        ),
    )

    assert candidates == (
        "https://www.wlbt.com/news-story-a",
        "https://www.wdbj7.com/news-story-b",
    )


def test_build_source_candidates_deduplicates_tracking_only_query_variants() -> None:
    from gva_pipeline.source_acquisition import build_source_candidates

    candidates = build_source_candidates(
        None,
        (
            "https://www.wlbt.com/news-story-a?utm_source=newsletter",
            "https://www.wlbt.com/news-story-a?fbclid=abc123",
            "https://www.wlbt.com/news-story-a?utm_medium=social&gclid=xyz",
        ),
    )

    assert candidates == ("https://www.wlbt.com/news-story-a?utm_source=newsletter",)


def test_build_source_candidates_keeps_meaningful_query_params_distinct() -> None:
    from gva_pipeline.source_acquisition import build_source_candidates

    candidates = build_source_candidates(
        None,
        (
            "https://example.com/story?id=123",
            "https://example.com/story?id=456",
        ),
    )

    assert candidates == (
        "https://example.com/story?id=123",
        "https://example.com/story?id=456",
    )


def test_build_source_candidates_mixed_tracking_and_meaningful_query_behavior() -> None:
    from gva_pipeline.source_acquisition import build_source_candidates

    candidates = build_source_candidates(
        None,
        (
            "https://example.com/story?id=123&utm_source=newsletter",
            "https://example.com/story?id=123&fbclid=abc123",
            "https://example.com/story?id=456&utm_campaign=summer",
        ),
    )

    assert candidates == (
        "https://example.com/story?id=123&utm_source=newsletter",
        "https://example.com/story?id=456&utm_campaign=summer",
    )


def test_ranking_still_respected_after_enrichment() -> None:
    incident = IncidentRecord(
        incident_id="rank-1c",
        incident_date=None,
        state="Georgia",
        city_or_county="Atlanta",
        address="1 Main St",
        victims_killed=0,
        victims_injured=4,
        suspects_killed=0,
        suspects_injured=0,
        suspects_arrested=1,
        incident_url="https://example.com/incidents/rank-1c",
        source_url="https://www.wlbt.com/news-story-a",
        source_candidates=(
            "https://www.wlbt.com/news-story-a",
            "https://atlantapd.org/Home/Components/News/News/7551/631",
        ),
    )

    prepared = prepare_source_candidates(incident)

    assert [candidate.source_url for candidate in prepared[:3]] == [
        "https://atlantapd.org/Home/Components/News/News/7551/631",
        "https://www.atlantapd.org/Home/Components/News/News/7551/631",
        "https://www.wlbt.com/news-story-a",
    ]
    assert [candidate.source_policy.category for candidate in prepared[:3]] == [
        "OFFICIAL",
        "OFFICIAL",
        "NEWS",
    ]


def test_ranked_first_candidate_is_selected_when_successful() -> None:
    incident = IncidentRecord(
        incident_id="rank-2",
        incident_date=None,
        state="TX",
        city_or_county="Austin",
        address="1 Main St",
        victims_killed=0,
        victims_injured=4,
        suspects_killed=0,
        suspects_injured=0,
        suspects_arrested=1,
        incident_url="https://example.com/incidents/rank-2",
        source_url="https://www.wlbt.com/news-story-a",
        source_candidates=(
            "https://www.wlbt.com/news-story-a",
            "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west/",
        ),
    )
    seen_urls: list[str] = []

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        seen_urls.append(source_url or "")
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said investigators responded to the shooting.",
            source_category="OFFICIAL" if "police." in (source_url or "") else "NEWS",
        )

    result = acquire_incident_sources(
        incident,
        fetch_fn=fake_fetch,
        session=requests.Session(),
        timeout_seconds=8.0,
        store_raw_html=False,
        fetch_cache={},
    )

    assert seen_urls == [
        "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west"
    ]
    assert result.selected_source_url == seen_urls[0]
    history = json.loads(result.source_attempt_history)
    assert history[0]["ranked_source_category"] == "OFFICIAL"
    assert history[0]["original_candidate_index"] == 2


def test_acquisition_results_do_not_regress_after_enrichment_variants_added() -> None:
    incident = IncidentRecord(
        incident_id="rank-2b",
        incident_date=None,
        state="Georgia",
        city_or_county="Atlanta",
        address="1 Main St",
        victims_killed=0,
        victims_injured=4,
        suspects_killed=0,
        suspects_injured=0,
        suspects_arrested=1,
        incident_url="https://example.com/incidents/rank-2b",
        source_url="https://www.wlbt.com/news-story-a",
        source_candidates=(
            "https://www.wlbt.com/news-story-a",
            "https://atlantapd.org/Home/Components/News/News/7551/631",
        ),
    )
    seen_urls: list[str] = []

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        seen_urls.append(source_url or "")
        if source_url and "atlantapd.org" in source_url:
            return FetchResult(
                requested_url=source_url,
                final_url=source_url,
                status_code=404,
                ok=False,
                error="http_404",
                article_text=None,
                acquisition_status="permanent_not_found",
                failure_stage="fetch",
                failure_reason="http_404",
                source_category="OFFICIAL",
            )
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said witnesses identified a suspect after the shooting.",
            source_category="NEWS",
        )

    result = acquire_incident_sources(
        incident,
        fetch_fn=fake_fetch,
        session=requests.Session(),
        timeout_seconds=8.0,
        store_raw_html=False,
        fetch_cache={},
    )

    assert seen_urls == [
        "https://atlantapd.org/Home/Components/News/News/7551/631",
        "https://www.atlantapd.org/Home/Components/News/News/7551/631",
        "https://www.wlbt.com/news-story-a",
    ]
    assert result.selected_source_url == "https://www.wlbt.com/news-story-a"
    history = json.loads(result.source_attempt_history)
    assert history[-1]["success"] is True


def test_fallback_still_works_after_candidate_reordering() -> None:
    incident = _incident_with_candidates(
        "https://x.com/example/status/1",
        "https://www.wlbt.com/news-story-a",
        "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west/",
    )
    seen_urls: list[str] = []

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        seen_urls.append(source_url or "")
        if source_url and "police." in source_url:
            return FetchResult(
                requested_url=source_url,
                final_url=source_url,
                status_code=404,
                ok=False,
                error="http_404",
                article_text=None,
                acquisition_status="permanent_not_found",
                failure_stage="fetch",
                failure_reason="http_404",
                source_category="OFFICIAL",
            )
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said witnesses identified a suspect after the shooting.",
            source_category="NEWS",
        )

    result = acquire_incident_sources(
        incident,
        fetch_fn=fake_fetch,
        session=requests.Session(),
        timeout_seconds=8.0,
        store_raw_html=False,
        fetch_cache={},
    )

    assert seen_urls == [
        "https://police.birminghamal.gov/media-release-183-double-homicide-investigation-3rd-avenue-west",
        "https://www.wlbt.com/news-story-a",
    ]
    assert result.selected_source_url == "https://www.wlbt.com/news-story-a"
    history = json.loads(result.source_attempt_history)
    assert history[0]["failure_reason"] == "http_404"
    assert history[1]["success"] is True
