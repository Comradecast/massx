from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gva_pipeline.review_results_io import (
    HUMAN_REVIEW_RESULTS_COLUMNS,
    build_review_result_row,
    delete_human_review_result_row,
    ensure_human_review_results_file,
    read_human_review_results_frame,
    upsert_human_review_result_row,
    write_human_review_results_frame,
)


def test_ensure_human_review_results_file_creates_exact_header(tmp_path: Path) -> None:
    results_path = tmp_path / "human_review_results.csv"

    ensure_human_review_results_file(results_path)

    frame = pd.read_csv(results_path, dtype=str, keep_default_na=False)
    assert list(frame.columns) == HUMAN_REVIEW_RESULTS_COLUMNS
    assert frame.empty


def test_read_human_review_results_frame_fails_on_missing_columns(tmp_path: Path) -> None:
    results_path = tmp_path / "human_review_results.csv"
    results_path.write_text(
        "\n".join(
            [
                "incident_id,review_status,final_category,notes,source_override",
                "123,resolved,domestic_family,Reviewed,",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        read_human_review_results_frame(results_path)

    assert "must have exactly these columns" in str(exc_info.value)
    assert "final_confidence" in str(exc_info.value)


def test_build_review_result_row_validates_confidence_and_normalizes_values() -> None:
    row = build_review_result_row(
        incident_id=" 123 ",
        review_status=" resolved ",
        final_category=" domestic_family ",
        final_confidence=" 0.75 ",
        notes=" reviewed ",
        source_override=" https://example.com/source ",
    )

    assert row == {
        "incident_id": "123",
        "review_status": "resolved",
        "final_category": "domestic_family",
        "final_confidence": "0.75",
        "notes": "reviewed",
        "source_override": "https://example.com/source",
    }


def test_build_review_result_row_allows_blank_confidence() -> None:
    row = build_review_result_row(
        incident_id="123",
        review_status="resolved",
        final_category="",
        final_confidence="",
        notes="",
        source_override="",
    )

    assert row["final_confidence"] == ""


def test_build_review_result_row_rejects_invalid_confidence() -> None:
    with pytest.raises(ValueError) as exc_info:
        build_review_result_row(
            incident_id="123",
            review_status="resolved",
            final_category="domestic_family",
            final_confidence="not-a-float",
            notes="",
            source_override="",
        )

    assert "invalid final_confidence" in str(exc_info.value)


def test_upsert_human_review_result_row_preserves_other_incidents_and_exact_schema() -> None:
    frame = pd.DataFrame(
        [
            {
                "incident_id": "a1",
                "review_status": "pending",
                "final_category": "",
                "final_confidence": "",
                "notes": "first",
                "source_override": "",
            },
            {
                "incident_id": "b2",
                "review_status": "resolved",
                "final_category": "party_social_event",
                "final_confidence": "0.88",
                "notes": "second",
                "source_override": "https://example.com/source-b2",
            },
        ]
    )
    updated = upsert_human_review_result_row(
        frame,
        {
            "incident_id": "a1",
            "review_status": "resolved",
            "final_category": "domestic_family",
            "final_confidence": "0.92",
            "notes": "updated",
            "source_override": "",
        },
    )

    assert list(updated.columns) == HUMAN_REVIEW_RESULTS_COLUMNS
    assert len(updated.index) == 2
    assert updated.loc[updated["incident_id"] == "a1", "final_category"].iloc[0] == "domestic_family"
    assert updated.loc[updated["incident_id"] == "b2", "source_override"].iloc[0] == "https://example.com/source-b2"


def test_write_human_review_results_frame_does_not_write_extra_columns(tmp_path: Path) -> None:
    results_path = tmp_path / "human_review_results.csv"
    frame = pd.DataFrame(
        [
            {
                "incident_id": "123",
                "review_status": "resolved",
                "final_category": "",
                "final_confidence": "",
                "notes": "",
                "source_override": "",
                "extra_column": "ignore-me",
            }
        ]
    )

    write_human_review_results_frame(results_path, frame)

    written = pd.read_csv(results_path, dtype=str, keep_default_na=False)
    assert list(written.columns) == HUMAN_REVIEW_RESULTS_COLUMNS
    assert "extra_column" not in written.columns


def test_delete_human_review_result_row_removes_only_selected_incident() -> None:
    frame = pd.DataFrame(
        [
            {
                "incident_id": "a1",
                "review_status": "resolved",
                "final_category": "domestic_family",
                "final_confidence": "0.91",
                "notes": "alpha",
                "source_override": "",
            },
            {
                "incident_id": "b2",
                "review_status": "pending",
                "final_category": "",
                "final_confidence": "",
                "notes": "beta",
                "source_override": "https://example.com/b2",
            },
        ]
    )

    updated = delete_human_review_result_row(frame, "a1")

    assert list(updated.columns) == HUMAN_REVIEW_RESULTS_COLUMNS
    assert len(updated.index) == 1
    assert updated.loc[0, "incident_id"] == "b2"
    assert updated.loc[0, "source_override"] == "https://example.com/b2"


def test_delete_human_review_result_row_preserves_schema_and_remaining_rows_on_write(tmp_path: Path) -> None:
    results_path = tmp_path / "human_review_results.csv"
    frame = pd.DataFrame(
        [
            {
                "incident_id": "a1",
                "review_status": "resolved",
                "final_category": "domestic_family",
                "final_confidence": "0.91",
                "notes": "alpha",
                "source_override": "",
            },
            {
                "incident_id": "c3",
                "review_status": "resolved",
                "final_category": "party_social_event",
                "final_confidence": "0.88",
                "notes": "charlie",
                "source_override": "",
            },
        ]
    )

    updated = delete_human_review_result_row(frame, "a1")
    write_human_review_results_frame(results_path, updated)

    written = pd.read_csv(results_path, dtype=str, keep_default_na=False)
    assert list(written.columns) == HUMAN_REVIEW_RESULTS_COLUMNS
    assert written["incident_id"].tolist() == ["c3"]
