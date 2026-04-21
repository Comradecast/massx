from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from gva_pipeline.io_utils import EXPECTED_COLUMNS, read_incidents_csv


def test_read_incidents_csv_accepts_normalized_schema(tmp_path: Path) -> None:
    input_path = tmp_path / "normalized.csv"
    frame = pd.DataFrame(
        [
            {
                "incident_id": "1001",
                "incident_date": "2024-01-10",
                "state": "TX",
                "city_or_county": "Austin",
                "address": "123 Main St",
                "victims_killed": "1",
                "victims_injured": "4",
                "suspects_killed": "0",
                "suspects_injured": "0",
                "suspects_arrested": "1",
                "incident_url": "https://www.gunviolencearchive.org/incident/1001",
                "source_url": "https://example.com/story-1001",
            }
        ]
    )
    frame.to_csv(input_path, index=False)

    loaded = read_incidents_csv(input_path)

    assert list(loaded.columns) == EXPECTED_COLUMNS
    assert loaded.loc[0, "incident_id"] == "1001"


def test_read_incidents_csv_normalizes_raw_gva_headers_and_extracts_urls(tmp_path: Path) -> None:
    input_path = tmp_path / "raw_with_operations.csv"
    frame = pd.DataFrame(
        [
            {
                "Incident ID": "2001",
                "Incident Date": "2024-02-20",
                "State": "FL",
                "City Or County": "Miami",
                "Address": "456 Ocean Ave",
                "Victims Killed": "0",
                "Victims Injured": "5",
                "Suspects Killed": "0",
                "Suspects Injured": "0",
                "Suspects Arrested": "0",
                "Operations": (
                    '<a href="https://www.gunviolencearchive.org/incident/2001">Incident</a> '
                    '<a href="https://localnews.example.com/story-2001">Source</a>'
                ),
            }
        ]
    )
    frame.to_csv(input_path, index=False)

    loaded = read_incidents_csv(input_path)

    assert list(loaded.columns) == EXPECTED_COLUMNS + ["source_candidates"]
    assert loaded.loc[0, "incident_id"] == "2001"
    assert loaded.loc[0, "incident_url"] == "https://www.gunviolencearchive.org/incident/2001"
    assert loaded.loc[0, "source_url"] == "https://localnews.example.com/story-2001"
    assert json.loads(loaded.loc[0, "source_candidates"]) == ["https://localnews.example.com/story-2001"]


def test_read_incidents_csv_preserves_multiple_source_candidates(tmp_path: Path) -> None:
    input_path = tmp_path / "multiple_sources.csv"
    frame = pd.DataFrame(
        [
            {
                "incident_id": "2002",
                "incident_date": "2024-02-20",
                "state": "FL",
                "city_or_county": "Miami",
                "address": "456 Ocean Ave",
                "victims_killed": "0",
                "victims_injured": "5",
                "suspects_killed": "0",
                "suspects_injured": "0",
                "suspects_arrested": "0",
                "incident_url": "https://www.gunviolencearchive.org/incident/2002",
                "source_url": "https://example.com/primary-story",
                "source_candidates": json.dumps(
                    [
                        "https://example.com/primary-story",
                        "https://example.com/fallback-story",
                    ]
                ),
            }
        ]
    )
    frame.to_csv(input_path, index=False)

    loaded = read_incidents_csv(input_path)

    assert loaded.loc[0, "source_url"] == "https://example.com/primary-story"
    assert json.loads(loaded.loc[0, "source_candidates"]) == [
        "https://example.com/primary-story",
        "https://example.com/fallback-story",
    ]


def test_read_incidents_csv_fails_cleanly_when_urls_cannot_be_recovered(tmp_path: Path) -> None:
    input_path = tmp_path / "raw_without_urls.csv"
    frame = pd.DataFrame(
        [
            {
                "Incident ID": "3001",
                "Incident Date": "2024-03-01",
                "State": "GA",
                "City Or County": "Atlanta",
                "Address": "789 Peachtree St",
                "Victims Killed": "1",
                "Victims Injured": "3",
                "Suspects Killed": "0",
                "Suspects Injured": "0",
                "Suspects Arrested": "1",
                "Operations": "",
            }
        ]
    )
    frame.to_csv(input_path, index=False)

    with pytest.raises(ValueError) as exc_info:
        read_incidents_csv(input_path)

    message = str(exc_info.value)
    assert "incident_url, source_url" in message
    assert "Operations inspected: yes" in message
    assert "missing or has unusable required URL field(s)" in message
    assert "Raw GVA exports are unsupported pipeline inputs" in message
    assert "data/incidents_canonical.csv" in message


def test_read_incidents_csv_reports_found_columns_for_missing_required_fields(tmp_path: Path) -> None:
    input_path = tmp_path / "missing_columns.csv"
    frame = pd.DataFrame([{"Incident ID": "4001", "State": "LA"}])
    frame.to_csv(input_path, index=False)

    with pytest.raises(ValueError) as exc_info:
        read_incidents_csv(input_path)

    message = str(exc_info.value)
    assert "incident_date" in message
    assert "Found columns: Incident ID, State" in message
