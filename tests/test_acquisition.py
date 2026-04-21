from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gva_pipeline.acquisition import convert_pasted_rows_file, parse_pasted_rows_text


def test_parse_pasted_json_export(tmp_path: Path) -> None:
    payload = """
    [
      {
        "incident_id": "5001",
        "incident_date": "2024-05-01",
        "state": "TX",
        "city_or_county": "Dallas",
        "address": "123 Elm St",
        "victims_killed": "1",
        "victims_injured": "4",
        "suspects_killed": "0",
        "suspects_injured": "0",
        "suspects_arrested": "1",
        "incident_url": "https://www.gunviolencearchive.org/incident/5001",
        "source_url": "https://example.com/story-5001"
      }
    ]
    """

    frame = parse_pasted_rows_text(payload)

    assert frame.loc[0, "incident_id"] == "5001"
    assert frame.loc[0, "source_url"] == "https://example.com/story-5001"


def test_parse_pasted_json_export_with_leading_page_url() -> None:
    payload = """https://www.gunviolencearchive.org/mass-shooting?page=79[
      {
        "incident_id": "5002",
        "incident_date": "2024-05-02",
        "state": "FL",
        "city_or_county": "Miami",
        "address": "456 Ocean Dr",
        "victims_killed": "0",
        "victims_injured": "4",
        "suspects_killed": "0",
        "suspects_injured": "0",
        "suspects_arrested": "0",
        "incident_url": "https://www.gunviolencearchive.org/incident/5002",
        "source_url": "https://example.com/story-5002"
      }
    ]"""

    frame = parse_pasted_rows_text(payload)

    assert frame.loc[0, "incident_id"] == "5002"


def test_parse_pasted_csv_export_with_quoted_address() -> None:
    payload = """incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url
6001,2024-06-01,IL,Chicago,"123 W Main St, Apt 4",0,4,0,0,0,https://www.gunviolencearchive.org/incident/6001,https://example.com/story-6001
"""

    frame = parse_pasted_rows_text(payload)

    assert frame.loc[0, "address"] == "123 W Main St, Apt 4"


def test_convert_paste_deduplicates_by_incident_id(tmp_path: Path) -> None:
    input_path = tmp_path / "pasted_rows.txt"
    output_path = tmp_path / "canonical.csv"
    input_path.write_text(
        """
        [
          {
            "incident_id": "7001",
            "incident_date": "2024-07-01",
            "state": "GA",
            "city_or_county": "Atlanta",
            "address": "1 Peachtree St",
            "victims_killed": "1",
            "victims_injured": "2",
            "suspects_killed": "0",
            "suspects_injured": "0",
            "suspects_arrested": "1",
            "incident_url": "https://www.gunviolencearchive.org/incident/7001",
            "source_url": "https://example.com/story-7001"
          },
          {
            "incident_id": "7001",
            "incident_date": "2024-07-01",
            "state": "GA",
            "city_or_county": "Atlanta",
            "address": "1 Peachtree St",
            "victims_killed": "1",
            "victims_injured": "2",
            "suspects_killed": "0",
            "suspects_injured": "0",
            "suspects_arrested": "1",
            "incident_url": "https://www.gunviolencearchive.org/incident/7001",
            "source_url": "https://example.com/story-7001"
          }
        ]
        """,
        encoding="utf-8",
    )

    converted = convert_pasted_rows_file(input_path, output_path)
    written = pd.read_csv(output_path)

    assert len(converted.index) == 1
    assert len(written.index) == 1


def test_parse_pasted_rows_rejects_bad_input_format() -> None:
    with pytest.raises(ValueError) as exc_info:
        parse_pasted_rows_text("this is not json or csv")

    assert "supported JSON array or CSV" in str(exc_info.value)


def test_parse_pasted_rows_rejects_missing_required_field() -> None:
    payload = """
    [
      {
        "incident_id": "8001",
        "incident_date": "2024-08-01",
        "state": "LA",
        "city_or_county": "New Orleans",
        "address": "9 Canal St",
        "victims_killed": "0",
        "victims_injured": "4",
        "suspects_killed": "0",
        "suspects_injured": "0",
        "suspects_arrested": "0",
        "incident_url": "https://www.gunviolencearchive.org/incident/8001"
      }
    ]
    """

    with pytest.raises(ValueError) as exc_info:
        parse_pasted_rows_text(payload)

    assert "source_url" in str(exc_info.value)
