from __future__ import annotations

import json
from pathlib import Path
import time

from openpyxl import load_workbook
import pandas as pd
import pytest
import requests

from gva_pipeline import cli
from gva_pipeline.models import FetchResult
from gva_pipeline.pipeline import _build_human_review_queue, _build_review_metadata, run_pipeline


def test_pipeline_smoke_and_deduplication(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "1001,2024-01-10,TX,Austin,123 Main St,1,4,0,0,1,https://example.com/incidents/1001,https://example.com/story-1001",
                "1001,2024-01-10,TX,Austin,123 Main St,1,4,0,0,1,https://example.com/incidents/1001,https://example.com/story-1001",
                "1002,2024-02-14,FL,Miami,456 Ocean Ave,0,5,0,0,0,https://example.com/incidents/1002,",
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
        if not source_url:
            return FetchResult(
                requested_url=None,
                final_url=None,
                status_code=None,
                ok=False,
                error="missing_source_url",
                article_text=None,
            )
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text=(
                "Police said the shooting happened during a birthday party. "
                "The suspect, a 24-year-old man, was arrested."
            ),
            raw_html="<html></html>" if store_raw_html else None,
        )

    report = run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    failures = pd.read_csv(output_dir / "fetch_failures.csv")
    summary = pd.read_csv(output_dir / "summary_by_category.csv")

    assert report.total_unique_incidents == 2
    assert len(enriched.index) == 2
    assert len(failures.index) == 1
    assert summary.loc[summary["category"] == "party_social_event", "incident_count"].iloc[0] == 1


def test_pipeline_processes_all_30_rows_by_default(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents_30.csv"
    output_dir = tmp_path / "out_30"
    rows = [
        "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url"
    ]
    for index in range(30):
        incident_id = f"{1000 + index}"
        rows.append(
            ",".join(
                [
                    incident_id,
                    "2024-01-10",
                    "TX",
                    "Austin",
                    f"{index} Main St",
                    "0",
                    "4",
                    "0",
                    "0",
                    "1",
                    f"https://example.com/incidents/{incident_id}",
                    f"https://example.com/story-{incident_id}",
                ]
            )
        )
    input_path.write_text("\n".join(rows), encoding="utf-8")

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        time.sleep(0.01)
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said the shooting happened at a birthday party.",
            raw_html=None,
        )

    report = run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        progress_interval=10,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert report.total_unique_incidents == 30
    assert len(enriched.index) == 30
    assert enriched["incident_id"].nunique() == 30


def test_pipeline_can_write_excel_companions_with_autofit(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "out_excel"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "1001,2024-01-10,TX,Austin,123 Main St,1,4,0,0,1,https://example.com/incidents/1001,https://example.com/story-1001",
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
            article_text="Investigators said they are still trying to determine what happened.",
            raw_html=None,
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        write_excel_autofit=True,
        fetch_fn=fake_fetch,
    )

    workbook = load_workbook(output_dir / "enriched_incidents.xlsx")
    worksheet = workbook.active

    assert (output_dir / "summary_by_category.xlsx").exists()
    assert (output_dir / "human_review_queue.xlsx").exists()
    assert worksheet.column_dimensions["A"].width is not None
    assert worksheet.column_dimensions["A"].width > 8


def test_pipeline_succeeds_with_supported_incidents_canonical_input(tmp_path: Path) -> None:
    input_path = tmp_path / "incidents_canonical.csv"
    output_dir = tmp_path / "out_canonical"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "1001,2024-01-10,TX,Austin,123 Main St,1,4,0,0,1,https://example.com/incidents/1001,https://example.com/story-1001",
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
            article_text="Police said the shooting happened during a birthday party.",
            raw_html=None,
        )

    report = run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    assert report.total_unique_incidents == 1
    assert str(enriched.loc[0, "incident_id"]) == "1001"
    assert enriched.loc[0, "fetch_ok"]


@pytest.mark.parametrize(
    ("row", "expected_reason", "expected_priority", "needs_category_review", "needs_source_review"),
    [
        (
            {
                "fetch_ok": False,
                "article_text_length": 120,
                "category": "party_social_event",
                "category_confidence": 0.88,
            },
            "fetch_failed",
            100,
            False,
            True,
        ),
        (
            {
                "fetch_ok": True,
                "article_text_length": 0,
                "category": "party_social_event",
                "category_confidence": 0.88,
            },
            "no_article_text",
            95,
            False,
            True,
        ),
        (
            {
                "fetch_ok": True,
                "article_text_length": 50,
                "category": "unknown",
                "category_confidence": 0.9,
            },
            "unknown_category",
            80,
            True,
            False,
        ),
        (
            {
                "fetch_ok": True,
                "article_text_length": 50,
                "category": "interpersonal_dispute",
                "category_confidence": 0.5,
            },
            "low_confidence",
            70,
            True,
            False,
        ),
        (
            {
                "fetch_ok": True,
                "article_text_length": 50,
                "category": "interpersonal_dispute",
                "category_confidence": 0.82,
                "mentions_party": True,
            },
            "rule_conflict_party_context",
            75,
            True,
            False,
        ),
        (
            {
                "fetch_ok": True,
                "article_text_length": 50,
                "category": "unknown",
                "category_confidence": 0.0,
                "mentions_domestic": True,
            },
            "rule_conflict_domestic_context",
            90,
            True,
            False,
        ),
        (
            {
                "fetch_ok": True,
                "article_text_length": 50,
                "category": "public_space_nonrandom",
                "category_confidence": 0.78,
                "mentions_school": True,
            },
            "rule_conflict_school_context",
            78,
            True,
            False,
        ),
    ],
)
def test_build_review_metadata_applies_deterministic_review_rules(
    row: dict[str, object],
    expected_reason: str,
    expected_priority: int,
    needs_category_review: bool,
    needs_source_review: bool,
) -> None:
    metadata = _build_review_metadata(row)

    assert metadata["review_required"] is True
    assert metadata["review_reason"] == expected_reason
    assert metadata["review_priority"] == expected_priority
    assert metadata["needs_category_review"] is needs_category_review
    assert metadata["needs_source_review"] is needs_source_review


def test_cli_default_behavior_does_not_pass_a_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    input_path = tmp_path / "input.csv"
    output_dir = tmp_path / "out"
    input_path.write_text("", encoding="utf-8")

    def fake_run_pipeline(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    exit_code = cli.main(["--input", str(input_path), "--output-dir", str(output_dir)])

    assert exit_code == 0
    assert "limit" in captured
    assert captured["limit"] is None
    assert captured["timeout_seconds"] == 8.0


def test_cli_help_points_to_canonical_pipeline_input() -> None:
    parser = cli.build_parser()
    help_text = parser.format_help()

    assert "data/incidents_canonical.csv" in help_text
    assert "canonical incident CSV" in help_text


def test_cli_passes_heartbeat_and_verbose_lifecycle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    input_path = tmp_path / "input.csv"
    output_dir = tmp_path / "out"
    input_path.write_text("", encoding="utf-8")

    def fake_run_pipeline(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    exit_code = cli.main(
        [
            "--input",
            str(input_path),
            "--output-dir",
            str(output_dir),
            "--heartbeat-seconds",
            "3",
            "--verbose-lifecycle",
        ]
    )

    assert exit_code == 0
    assert captured["heartbeat_seconds"] == 3.0
    assert captured["verbose_lifecycle"] is True


def test_cli_passes_human_review_results_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    input_path = tmp_path / "input.csv"
    output_dir = tmp_path / "out"
    review_results_path = tmp_path / "human_review_results.csv"
    input_path.write_text("", encoding="utf-8")

    def fake_run_pipeline(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    exit_code = cli.main(
        [
            "--input",
            str(input_path),
            "--output-dir",
            str(output_dir),
            "--human-review-results",
            str(review_results_path),
        ]
    )

    assert exit_code == 0
    assert captured["human_review_results_path"] == str(review_results_path)


def test_readme_usage_examples_point_to_canonical_input() -> None:
    readme_text = Path("README.md").read_text(encoding="utf-8")

    assert "python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out" in readme_text
    assert "python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out --save-html" in readme_text
    assert "python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out --excel-autofit" in readme_text


def test_heartbeat_capable_code_path_does_not_change_row_counts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "incidents_30_heartbeat.csv"
    output_dir = tmp_path / "out_heartbeat"
    rows = [
        "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url"
    ]
    for index in range(30):
        incident_id = f"{2000 + index}"
        rows.append(
            ",".join(
                [
                    incident_id,
                    "2024-01-10",
                    "TX",
                    "Austin",
                    f"{index} Main St",
                    "0",
                    "4",
                    "0",
                    "0",
                    "1",
                    f"https://example.com/incidents/{incident_id}",
                    f"https://example.com/story-{incident_id}",
                ]
            )
        )
    input_path.write_text("\n".join(rows), encoding="utf-8")

    def fake_fetch(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        time.sleep(0.01)
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said the shooting happened at a birthday party.",
            raw_html=None,
        )

    report = run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        heartbeat_seconds=0.01,
        progress_interval=10,
        verbose_lifecycle=False,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    captured = capsys.readouterr()

    assert report.total_unique_incidents == 30
    assert len(enriched.index) == 30
    assert "[heartbeat]" in captured.out


def test_human_review_queue_includes_only_flagged_rows(tmp_path: Path) -> None:
    input_path = tmp_path / "review_cases.csv"
    output_dir = tmp_path / "out_review"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "r1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/r1,https://example.com/story-r1",
                "r2,2024-01-11,TX,Austin,2 Main St,0,4,0,0,1,https://example.com/incidents/r2,https://example.com/story-r2",
                "r3,2024-01-12,TX,Austin,3 Main St,0,4,0,0,1,https://example.com/incidents/r3,https://example.com/story-r3",
                "r4,2024-01-13,TX,Austin,4 Main St,0,4,0,0,1,https://example.com/incidents/r4,not-a-url",
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
        if source_url and source_url.endswith("story-r1"):
            return FetchResult(
                requested_url=source_url,
                final_url=source_url,
                status_code=200,
                ok=True,
                error=None,
                article_text="Police said the shooting happened during a birthday party.",
                raw_html=None,
            )
        if source_url and source_url.endswith("story-r2"):
            return FetchResult(
                requested_url=source_url,
                final_url=source_url,
                status_code=200,
                ok=True,
                error=None,
                article_text="Details remain limited and investigators have not described the circumstances.",
                raw_html=None,
            )
        if source_url and source_url.endswith("story-r3"):
            return FetchResult(
                requested_url=source_url,
                final_url=source_url,
                status_code=404,
                ok=False,
                error="http_404",
                article_text=None,
                raw_html=None,
            )
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="The shooting happened in a parking lot and police said the people involved knew each other.",
            raw_html=None,
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        fetch_fn=fake_fetch,
    )

    review_queue = pd.read_csv(output_dir / "human_review_queue.csv")
    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    domain_summary = pd.read_csv(output_dir / "domain_fetch_summary.csv")

    assert set(review_queue["incident_id"]) == {"r2", "r3"}
    assert set(enriched.loc[enriched["review_required"], "incident_id"]) == {"r2", "r3"}
    assert "r1" not in set(review_queue["incident_id"])
    reasons = dict(zip(review_queue["incident_id"], review_queue["review_reason"]))
    assert reasons["r2"] == "unknown_category"
    assert reasons["r3"] == "fetch_failed"
    priorities = dict(zip(review_queue["incident_id"], review_queue["review_priority"]))
    assert priorities["r2"] == 80
    assert priorities["r3"] == 100
    flags = {
        row["incident_id"]: (
            bool(row["needs_category_review"]),
            bool(row["needs_source_review"]),
        )
        for row in review_queue.to_dict(orient="records")
    }
    assert flags["r2"] == (True, False)
    assert flags["r3"] == (True, True)

    domains = dict(zip(enriched["incident_id"], enriched["source_domain"]))
    assert domains["r1"] == "example.com"
    assert domains["r4"] == "unknown"

    summary_by_domain = {row["source_domain"]: row for row in domain_summary.to_dict(orient="records")}
    example_summary = summary_by_domain["example.com"]
    unknown_summary = summary_by_domain["unknown"]
    assert example_summary["incident_count"] == 3
    assert example_summary["fetch_success_count"] == 2
    assert example_summary["fetch_failure_count"] == 1
    assert example_summary["http_404_count"] == 1
    assert example_summary["article_text_not_found_count"] == 0
    assert abs(example_summary["success_rate"] - (2 / 3)) < 1e-9
    assert unknown_summary["incident_count"] == 1
    assert unknown_summary["fetch_success_count"] == 1
    assert unknown_summary["fetch_failure_count"] == 0


def test_human_review_queue_sorts_by_priority_then_newest_date_then_incident_id() -> None:
    review_queue = _build_human_review_queue(
        pd.DataFrame(
            [
                {
                    "incident_id": "a-older",
                    "incident_date": "2024-01-01",
                    "state": "TX",
                    "city_or_county": "Austin",
                    "address": "1 Main St",
                    "victims_killed": 0,
                    "victims_injured": 4,
                    "category": "unknown",
                    "category_confidence": 0.0,
                    "original_category": "unknown",
                    "original_category_confidence": 0.0,
                    "selected_source_url": "https://example.com/a",
                    "selected_source_overridden": False,
                    "original_selected_source_url": "https://example.com/a",
                    "selected_source_origin": "original",
                    "source_candidates_count": 1,
                    "source_attempt_count": 1,
                    "fetch_ok": True,
                    "acquisition_status": "fetched",
                    "failure_stage": None,
                    "failure_reason": None,
                    "fetch_status_code": 200,
                    "fetch_error": None,
                    "incident_url": "https://example.com/incidents/a",
                    "source_url": "https://example.com/a",
                    "source_domain": "example.com",
                    "source_category": "NEWS",
                    "mentions_party": False,
                    "mentions_domestic": False,
                    "mentions_school": False,
                    "manual_review_applied": False,
                    "manual_review_decision_type": None,
                    "suspect_age": None,
                    "suspect_gender": "unknown",
                    "suspect_race": "unknown",
                    "suspect_demographics_snippet": "",
                    "review_applied": False,
                    "review_applied_fields": "",
                    "review_notes": "",
                    "review_status": "",
                    "review_required": True,
                    "review_reason": "unknown_category",
                    "review_priority": 80,
                    "needs_category_review": True,
                    "needs_source_review": False,
                },
                {
                    "incident_id": "b-newer",
                    "incident_date": "2024-02-01",
                    "state": "TX",
                    "city_or_county": "Austin",
                    "address": "2 Main St",
                    "victims_killed": 0,
                    "victims_injured": 4,
                    "category": "party_social_event",
                    "category_confidence": 0.88,
                    "original_category": "party_social_event",
                    "original_category_confidence": 0.88,
                    "selected_source_url": "https://example.com/b",
                    "selected_source_overridden": False,
                    "original_selected_source_url": "https://example.com/b",
                    "selected_source_origin": "original",
                    "source_candidates_count": 1,
                    "source_attempt_count": 1,
                    "fetch_ok": False,
                    "acquisition_status": "fetch_failed",
                    "failure_stage": "fetch",
                    "failure_reason": "http_404",
                    "fetch_status_code": 404,
                    "fetch_error": "http_404",
                    "incident_url": "https://example.com/incidents/b",
                    "source_url": "https://example.com/b",
                    "source_domain": "example.com",
                    "source_category": "NEWS",
                    "mentions_party": False,
                    "mentions_domestic": False,
                    "mentions_school": False,
                    "manual_review_applied": False,
                    "manual_review_decision_type": None,
                    "suspect_age": None,
                    "suspect_gender": "unknown",
                    "suspect_race": "unknown",
                    "suspect_demographics_snippet": "",
                    "review_applied": False,
                    "review_applied_fields": "",
                    "review_notes": "",
                    "review_status": "",
                    "review_required": True,
                    "review_reason": "fetch_failed",
                    "review_priority": 100,
                    "needs_category_review": False,
                    "needs_source_review": True,
                },
                {
                    "incident_id": "a-newer",
                    "incident_date": "2024-02-01",
                    "state": "TX",
                    "city_or_county": "Austin",
                    "address": "3 Main St",
                    "victims_killed": 0,
                    "victims_injured": 4,
                    "category": "party_social_event",
                    "category_confidence": 0.88,
                    "original_category": "party_social_event",
                    "original_category_confidence": 0.88,
                    "selected_source_url": "https://example.com/c",
                    "selected_source_overridden": False,
                    "original_selected_source_url": "https://example.com/c",
                    "selected_source_origin": "original",
                    "source_candidates_count": 1,
                    "source_attempt_count": 1,
                    "fetch_ok": False,
                    "acquisition_status": "fetch_failed",
                    "failure_stage": "fetch",
                    "failure_reason": "http_404",
                    "fetch_status_code": 404,
                    "fetch_error": "http_404",
                    "incident_url": "https://example.com/incidents/c",
                    "source_url": "https://example.com/c",
                    "source_domain": "example.com",
                    "source_category": "NEWS",
                    "mentions_party": False,
                    "mentions_domestic": False,
                    "mentions_school": False,
                    "manual_review_applied": False,
                    "manual_review_decision_type": None,
                    "suspect_age": None,
                    "suspect_gender": "unknown",
                    "suspect_race": "unknown",
                    "suspect_demographics_snippet": "",
                    "review_applied": False,
                    "review_applied_fields": "",
                    "review_notes": "",
                    "review_status": "",
                    "review_required": True,
                    "review_reason": "fetch_failed",
                    "review_priority": 100,
                    "needs_category_review": False,
                    "needs_source_review": True,
                },
            ]
        )
    )

    assert list(review_queue["incident_id"]) == ["a-newer", "b-newer", "a-older"]


def test_pipeline_output_preserves_core_schema_and_adds_acquisition_fields(tmp_path: Path) -> None:
    input_path = tmp_path / "schema.csv"
    output_dir = tmp_path / "out_schema"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "s1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/s1,https://example.com/story-s1",
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
            article_text="Police said the shooting happened during a large gathering and officers arrested one suspect.",
        )

    run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        fetch_fn=fake_fetch,
    )

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")

    expected_columns = {
        "incident_id",
        "incident_url",
        "source_url",
        "selected_source_url",
        "selected_source_origin",
        "source_attempt_count",
        "source_candidates_count",
        "source_attempt_history",
        "manual_review_applied",
        "manual_review_status",
        "manual_review_decision_type",
        "manual_review_preferred_source_url",
        "manual_review_added_candidates",
        "manual_review_rejected_candidates",
        "manual_review_notes",
        "manual_review_reviewer",
        "manual_review_timestamp",
        "fetch_ok",
        "fetch_error",
        "fetch_status_code",
        "fetch_final_url",
        "source_domain",
        "source_category",
        "source_action",
        "acquisition_status",
        "failure_stage",
        "failure_reason",
        "fetch_retryable",
        "fetch_attempts",
        "article_text",
        "review_required",
        "review_reason",
        "review_priority",
        "needs_category_review",
        "needs_source_review",
        "original_category",
        "original_category_confidence",
        "selected_source_overridden",
        "original_selected_source_url",
        "review_applied",
        "review_applied_fields",
        "review_notes",
        "review_status",
    }
    assert expected_columns.issubset(set(enriched.columns))
    assert {"fetch_request_domain", "fetch_final_domain", "fetch_domain_changed"}.issubset(set(enriched.columns))


def test_pipeline_normalizes_blank_source_domain_to_unknown(tmp_path: Path) -> None:
    input_path = tmp_path / "unknown_domain.csv"
    output_dir = tmp_path / "out_unknown_domain"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "u1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/u1,not-a-url",
            ]
        ),
        encoding="utf-8",
    )

    run_pipeline(input_path=input_path, output_dir=output_dir)

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    domain_summary = pd.read_csv(output_dir / "domain_fetch_summary.csv")

    assert enriched.loc[0, "source_domain"] == "unknown"
    assert enriched.loc[0, "fetch_request_domain"] == "unknown"
    assert enriched.loc[0, "fetch_final_domain"] == "unknown"
    assert not domain_summary["source_domain"].isna().any()
    assert "" not in set(domain_summary["source_domain"])
    assert "unknown" in set(domain_summary["source_domain"])


def test_pipeline_keeps_403_rows_attributable_by_domain(tmp_path: Path) -> None:
    input_path = tmp_path / "forbidden.csv"
    output_dir = tmp_path / "out_forbidden"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url",
                "f403,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/f403,https://blocked.example.com/story",
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
            final_url="https://edge.blocked.example.com/challenge/story",
            status_code=403,
            ok=False,
            error="http_403",
            article_text=None,
            acquisition_status="fetch_failed",
            failure_stage="fetch",
            failure_reason="http_403",
            source_category="NEWS",
        )

    run_pipeline(input_path=input_path, output_dir=output_dir, fetch_fn=fake_fetch)

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    failures = pd.read_csv(output_dir / "fetch_failures.csv")
    domain_summary = pd.read_csv(output_dir / "domain_fetch_summary.csv")
    history = json.loads(enriched.loc[0, "source_attempt_history"])

    assert enriched.loc[0, "source_domain"] == "edge.blocked.example.com"
    assert enriched.loc[0, "fetch_request_domain"] == "blocked.example.com"
    assert enriched.loc[0, "fetch_final_domain"] == "edge.blocked.example.com"
    assert bool(enriched.loc[0, "fetch_domain_changed"]) is True
    assert failures.loc[0, "source_domain"] == "edge.blocked.example.com"
    assert failures.loc[0, "fetch_request_domain"] == "blocked.example.com"
    assert failures.loc[0, "fetch_final_domain"] == "edge.blocked.example.com"
    assert history[0]["source_domain"] == "blocked.example.com"
    assert history[0]["fetch_request_domain"] == "blocked.example.com"
    assert history[0]["fetch_final_domain"] == "edge.blocked.example.com"
    assert history[0]["fetch_domain_changed"] is True

    summary_row = domain_summary.loc[domain_summary["source_domain"] == "edge.blocked.example.com"].iloc[0]
    assert summary_row["http_403_count"] == 1


def test_pipeline_uses_first_successful_candidate(tmp_path: Path) -> None:
    input_path = tmp_path / "first_success.csv"
    output_dir = tmp_path / "out_first_success"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url,source_candidates",
                'fs1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/fs1,https://example.com/primary,"[""https://example.com/primary"",""https://example.com/fallback""]"',
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
            article_text="Police said the shooting happened at a large gathering.",
        )

    run_pipeline(input_path=input_path, output_dir=output_dir, fetch_fn=fake_fetch)

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    history = json.loads(enriched.loc[0, "source_attempt_history"])

    assert seen_urls == ["https://example.com/primary"]
    assert enriched.loc[0, "selected_source_url"] == "https://example.com/primary"
    assert enriched.loc[0, "source_attempt_count"] == 1
    assert enriched.loc[0, "source_candidates_count"] == 2
    assert history[0]["success"] is True


def test_pipeline_falls_back_to_second_candidate_after_failure(tmp_path: Path) -> None:
    input_path = tmp_path / "fallback_success.csv"
    output_dir = tmp_path / "out_fallback_success"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url,source_candidates",
                'fb1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/fb1,https://example.com/blocked,"[""https://example.com/blocked"",""https://example.com/works""]"',
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
                status_code=403,
                ok=False,
                error="http_403",
                article_text=None,
                acquisition_status="fetch_failed",
                failure_stage="fetch",
                failure_reason="http_403",
                source_category="NEWS",
            )
        return FetchResult(
            requested_url=source_url,
            final_url=source_url,
            status_code=200,
            ok=True,
            error=None,
            article_text="Police said detectives arrested one suspect after the shooting.",
            source_category="NEWS",
        )

    run_pipeline(input_path=input_path, output_dir=output_dir, fetch_fn=fake_fetch)

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    history = json.loads(enriched.loc[0, "source_attempt_history"])

    assert enriched.loc[0, "selected_source_url"] == "https://example.com/works"
    assert enriched.loc[0, "source_attempt_count"] == 2
    assert history[0]["failure_reason"] == "http_403"
    assert history[1]["success"] is True


def test_pipeline_skips_unsupported_candidate_then_uses_news_source(tmp_path: Path) -> None:
    input_path = tmp_path / "skip_unsupported.csv"
    output_dir = tmp_path / "out_skip_unsupported"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url,source_candidates",
                'su1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/su1,https://x.com/example/status/1,"[""https://x.com/example/status/1"",""https://example.com/news-story""]"',
            ]
        ),
        encoding="utf-8",
    )

    def fetch_with_policy(
        source_url: str | None,
        *,
        session: requests.Session,
        timeout_seconds: float,
        store_raw_html: bool,
    ) -> FetchResult:
        if source_url == "https://example.com/news-story":
            return FetchResult(
                requested_url=source_url,
                final_url=source_url,
                status_code=200,
                ok=True,
                error=None,
                article_text="Police said four people were hospitalized after the shooting.",
                source_category="NEWS",
            )
        from gva_pipeline.fetch import fetch_source

        return fetch_source(
            source_url,
            session=session,
            timeout_seconds=timeout_seconds,
            store_raw_html=store_raw_html,
            sleep_fn=lambda _: None,
        )

    run_pipeline(input_path=input_path, output_dir=output_dir, fetch_fn=fetch_with_policy)

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    history = json.loads(enriched.loc[0, "source_attempt_history"])

    assert enriched.loc[0, "selected_source_url"] == "https://example.com/news-story"
    assert enriched.loc[0, "source_attempt_count"] == 1
    assert history[0]["success"] is True
    assert history[0]["ranked_source_category"] == "NEWS"


def test_pipeline_records_final_failure_when_all_candidates_fail(tmp_path: Path) -> None:
    input_path = tmp_path / "all_fail.csv"
    output_dir = tmp_path / "out_all_fail"
    input_path.write_text(
        "\n".join(
            [
                "incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url,source_candidates",
                'af1,2024-01-10,TX,Austin,1 Main St,0,4,0,0,1,https://example.com/incidents/af1,https://x.com/example/status/1,"[""https://x.com/example/status/1"",""https://example.com/missing""]"',
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
        if source_url == "https://example.com/missing":
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
        from gva_pipeline.fetch import fetch_source

        return fetch_source(
            source_url,
            session=session,
            timeout_seconds=timeout_seconds,
            store_raw_html=store_raw_html,
            sleep_fn=lambda _: None,
        )

    run_pipeline(input_path=input_path, output_dir=output_dir, fetch_fn=fake_fetch)

    enriched = pd.read_csv(output_dir / "enriched_incidents.csv")
    failures = pd.read_csv(output_dir / "fetch_failures.csv")
    history = json.loads(enriched.loc[0, "source_attempt_history"])

    assert pd.isna(enriched.loc[0, "selected_source_url"])
    assert enriched.loc[0, "acquisition_status"] == "permanent_not_found"
    assert enriched.loc[0, "failure_reason"] == "http_404"
    assert enriched.loc[0, "source_attempt_count"] == 2
    assert failures.loc[0, "source_attempt_count"] == 2
    assert history[0]["failure_reason"] == "http_404"
    assert history[0]["ranked_source_category"] == "NEWS"
    assert history[1]["acquisition_status"] == "source_not_supported"
