from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import requests

from gva_pipeline.manual_reviews import read_human_review_results_csv, read_manual_reviews_csv
from gva_pipeline.models import FetchResult
from gva_pipeline.pipeline import run_pipeline


def _write_incident_csv(path: Path, rows: list[str]) -> None:
    path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url,source_candidates",
                *rows,
            ]
        ),
        encoding="utf-8",
    )


def _write_human_review_results_csv(path: Path, rows: list[str]) -> None:
    path.write_text(
        "\n".join(
            [
                "incident_id,review_status,final_category,final_confidence,notes,source_override",
                *rows,
            ]
        ),
        encoding="utf-8",
    )


def test_pipeline_unchanged_when_manual_review_file_absent(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    missing_review_path = tmp_path / "manual_reviews.csv"
    _write_incident_csv(
        input_path,
        [
            'mr0,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/mr0,https://example.com/story-mr0,"[""https://example.com/story-mr0""]"',
        ],
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said investigators arrested one suspect after the shooting.",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        manual_review_path=missing_review_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert enriched.loc[0, "selected_source_url"] == "https://example.com/story-mr0"
    assert enriched.loc[0, "selected_source_origin"] == "original"
    assert bool(enriched.loc[0, "manual_review_applied"]) is False


def test_manual_review_added_source_candidates_are_considered(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    manual_review_path = tmp_path / "manual_reviews.csv"
    _write_incident_csv(
        input_path,
        [
            'mr1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/mr1,https://example.com/blocked,"[""https://example.com/blocked""]"',
        ],
    )
    manual_review_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,decision_type,preferred_source_url,added_source_candidates,rejected_candidates,review_notes,reviewer,review_timestamp",
                'mr1,complete,add_source_candidates,,"[""https://manual.example.com/story""]",,"Added a local article",analyst1,2026-04-20T21:00:00Z',
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        if source_url == "https://example.com/blocked":
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
                source_category="NEWS",
            )
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said the manual-review source confirmed the shooting details.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        manual_review_path=manual_review_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    history = json.loads(enriched.loc[0, "source_attempt_history"])

    assert enriched.loc[0, "manual_review_applied"]
    assert enriched.loc[0, "selected_source_url"] == "https://manual.example.com/story"
    assert enriched.loc[0, "selected_source_origin"] == "manual_review"
    assert json.loads(enriched.loc[0, "manual_review_added_candidates"]) == ["https://manual.example.com/story"]
    assert history[0]["source_url"] == "https://example.com/blocked"
    assert history[1]["source_origin"] == "manual_review"


def test_manual_review_rejected_candidates_removed_before_acquisition(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    manual_review_path = tmp_path / "manual_reviews.csv"
    _write_incident_csv(
        input_path,
        [
            'mr2,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/mr2,https://example.com/reject-me,"[""https://example.com/reject-me"",""https://example.com/works""]"',
        ],
    )
    manual_review_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,decision_type,preferred_source_url,added_source_candidates,rejected_candidates,review_notes,reviewer,review_timestamp",
                'mr2,complete,reject_source_candidates,,,"[""https://example.com/reject-me""]","Rejected dead source",analyst2,2026-04-20T21:05:00Z',
            ]
        ),
        encoding="utf-8",
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
            article_text="Police said witnesses identified the suspect after the shooting.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        manual_review_path=manual_review_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert seen_urls == ["https://example.com/works"]
    assert enriched.loc[0, "selected_source_url"] == "https://example.com/works"
    assert json.loads(enriched.loc[0, "manual_review_rejected_candidates"]) == ["https://example.com/reject-me"]


def test_manual_review_preferred_source_promoted_to_front(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    manual_review_path = tmp_path / "manual_reviews.csv"
    _write_incident_csv(
        input_path,
        [
            'mr3,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/mr3,https://example.com/primary,"[""https://example.com/primary"",""https://example.com/preferred""]"',
        ],
    )
    manual_review_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,decision_type,preferred_source_url,added_source_candidates,rejected_candidates,review_notes,reviewer,review_timestamp",
                'mr3,complete,set_preferred_source,https://example.com/preferred,,,Preferred the clearer article,analyst3,2026-04-20T21:10:00Z',
            ]
        ),
        encoding="utf-8",
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
            article_text="Police said officers responded to the shooting late Friday night.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        manual_review_path=manual_review_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert seen_urls == ["https://example.com/preferred"]
    assert enriched.loc[0, "selected_source_url"] == "https://example.com/preferred"
    assert enriched.loc[0, "manual_review_preferred_source_url"] == "https://example.com/preferred"
    assert enriched.loc[0, "manual_review_decision_type"] == "set_preferred_source"


def test_invalid_manual_review_json_is_explicit(tmp_path: Path) -> None:
    manual_review_path = tmp_path / "manual_reviews.csv"
    manual_review_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,decision_type,preferred_source_url,added_source_candidates,rejected_candidates,review_notes,reviewer,review_timestamp",
                'mr4,complete,add_source_candidates,,[not-json],,,analyst4,2026-04-20T21:15:00Z',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        read_manual_reviews_csv(manual_review_path)

    message = str(exc_info.value)
    assert "incident_id=mr4" in message
    assert "added_source_candidates" in message
    assert "invalid JSON" in message


def test_single_source_acquisition_behavior_still_works_with_manual_review_path(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    manual_review_path = tmp_path / "manual_reviews.csv"
    _write_incident_csv(
        input_path,
        [
            'mr5,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/mr5,https://example.com/story-mr5,"[""https://example.com/story-mr5""]"',
        ],
    )
    manual_review_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,decision_type,preferred_source_url,added_source_candidates,rejected_candidates,review_notes,reviewer,review_timestamp",
                "other,complete,add_source_candidates,,,,""Different incident"",analyst5,2026-04-20T21:20:00Z",
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said officers made an arrest after the shooting.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        manual_review_path=manual_review_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert enriched.loc[0, "selected_source_url"] == "https://example.com/story-mr5"
    assert enriched.loc[0, "selected_source_origin"] == "original"
    assert bool(enriched.loc[0, "manual_review_applied"]) is False


def test_resolved_category_override_applied_and_removed_from_review_queue(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    human_review_results_path = tmp_path / "human_review_results.csv"
    _write_incident_csv(
        input_path,
        [
            'hr1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/hr1,https://example.com/story-hr1,"[""https://example.com/story-hr1""]"',
        ],
    )
    _write_human_review_results_csv(
        human_review_results_path,
        [
            "hr1,resolved,domestic_family,,Reviewed by analyst,",
        ],
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Details remain limited and investigators have not described the circumstances.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        human_review_results_path=human_review_results_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    review_queue = pd.read_csv(output_dir / "human_review_queue.csv")

    assert enriched.loc[0, "original_category"] == "unknown"
    assert enriched.loc[0, "category"] == "domestic_family"
    assert enriched.loc[0, "original_category_confidence"] == 0.0
    assert enriched.loc[0, "original_selected_source_url"] == "https://example.com/story-hr1"
    assert bool(enriched.loc[0, "selected_source_overridden"]) is False
    assert bool(enriched.loc[0, "review_applied"]) is True
    assert enriched.loc[0, "review_applied_fields"] == "category"
    assert enriched.loc[0, "review_notes"] == "Reviewed by analyst"
    assert enriched.loc[0, "review_status"] == "resolved"
    assert review_queue.empty


def test_resolved_source_override_applied(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    human_review_results_path = tmp_path / "human_review_results.csv"
    _write_incident_csv(
        input_path,
        [
            'hr2,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/hr2,https://example.com/story-hr2,"[""https://example.com/story-hr2""]"',
        ],
    )
    _write_human_review_results_csv(
        human_review_results_path,
        [
            "hr2,resolved,,,,https://override.example.com/story-hr2",
        ],
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Details remain limited and investigators have not described the circumstances.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        human_review_results_path=human_review_results_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert enriched.loc[0, "original_selected_source_url"] == "https://example.com/story-hr2"
    assert bool(enriched.loc[0, "selected_source_overridden"]) is True
    assert enriched.loc[0, "selected_source_url"] == "https://override.example.com/story-hr2"
    assert enriched.loc[0, "review_applied_fields"] == "selected_source_url"


def test_resolved_confidence_override_applied(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    human_review_results_path = tmp_path / "human_review_results.csv"
    _write_incident_csv(
        input_path,
        [
            'hr3,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/hr3,https://example.com/story-hr3,"[""https://example.com/story-hr3""]"',
        ],
    )
    _write_human_review_results_csv(
        human_review_results_path,
        [
            "hr3,resolved,,0.99,,",
        ],
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Details remain limited and investigators have not described the circumstances.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        human_review_results_path=human_review_results_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert enriched.loc[0, "original_category_confidence"] == 0.0
    assert bool(enriched.loc[0, "selected_source_overridden"]) is False
    assert enriched.loc[0, "category_confidence"] == 0.99
    assert enriched.loc[0, "review_applied_fields"] == "category_confidence"


def test_unresolved_human_review_does_not_apply(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    human_review_results_path = tmp_path / "human_review_results.csv"
    _write_incident_csv(
        input_path,
        [
            'hr4,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/hr4,https://example.com/story-hr4,"[""https://example.com/story-hr4""]"',
        ],
    )
    _write_human_review_results_csv(
        human_review_results_path,
        [
            "hr4,pending,domestic_family,0.99,Still reviewing,https://override.example.com/story-hr4",
        ],
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Details remain limited and investigators have not described the circumstances.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        human_review_results_path=human_review_results_path,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    review_queue = pd.read_csv(output_dir / "human_review_queue.csv")

    assert bool(enriched.loc[0, "review_applied"]) is False
    assert bool(enriched.loc[0, "selected_source_overridden"]) is False
    assert pd.isna(enriched.loc[0, "review_applied_fields"]) or enriched.loc[0, "review_applied_fields"] == ""
    assert pd.isna(enriched.loc[0, "review_status"])
    assert enriched.loc[0, "category"] == "unknown"
    assert list(review_queue["incident_id"]) == ["hr4"]


def test_duplicate_resolved_human_review_results_fail_cleanly(tmp_path: Path) -> None:
    human_review_results_path = tmp_path / "human_review_results.csv"
    _write_human_review_results_csv(
        human_review_results_path,
        [
            "dup1,resolved,domestic_family,,,",
            "dup1,resolved,party_social_event,,,",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        read_human_review_results_csv(human_review_results_path)

    assert "duplicate resolved incident_id: dup1" in str(exc_info.value)


def test_missing_required_human_review_results_columns_fail_cleanly(tmp_path: Path) -> None:
    human_review_results_path = tmp_path / "human_review_results.csv"
    human_review_results_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,final_category,notes,source_override",
                "hr5,resolved,domestic_family,Reviewed,",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        read_human_review_results_csv(human_review_results_path)

    message = str(exc_info.value)
    assert "final_confidence" in message
    assert "must have exactly these columns" in message


def test_human_review_results_excel_outputs_still_work(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    human_review_results_path = tmp_path / "human_review_results.csv"
    _write_incident_csv(
        input_path,
        [
            'hr6,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/hr6,https://example.com/story-hr6,"[""https://example.com/story-hr6""]"',
        ],
    )
    _write_human_review_results_csv(
        human_review_results_path,
        [
            "hr6,resolved,domestic_family,0.97,Reviewed,https://override.example.com/story-hr6",
        ],
    )

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Details remain limited and investigators have not described the circumstances.",
            source_category="NEWS",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        human_review_results_path=human_review_results_path,
        write_excel_autofit=True,
        fetch_fn=fake_fetch,
    )

    assert (output_dir / "enriched_incidents.xlsx").exists()
    assert (output_dir / "human_review_queue.xlsx").exists()
