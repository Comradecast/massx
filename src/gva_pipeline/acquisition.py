from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import EXPECTED_COLUMNS, deduplicate_incidents_frame, ensure_directory, normalize_incidents_frame

TRAILING_NOISE_TOKENS = {"undefined", "null"}
SUPPORTED_INPUT_HEADERS = set(EXPECTED_COLUMNS) | {
    "Incident ID",
    "Incident Date",
    "State",
    "City Or County",
    "Address",
    "Victims Killed",
    "Victims Injured",
    "Suspects Killed",
    "Suspects Injured",
    "Suspects Arrested",
    "Operations",
}


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _clean_pasted_text(text: str) -> str:
    cleaned = text.lstrip("\ufeff")
    lines = cleaned.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and (not lines[-1].strip() or lines[-1].strip().lower() in TRAILING_NOISE_TOKENS):
        lines.pop()
    collapsed = "\n".join(lines).strip()
    json_start = collapsed.find("[")
    if json_start > 0:
        prefix = collapsed[:json_start].strip()
        if prefix and ("http://" in prefix or "https://" in prefix):
            return collapsed[json_start:].strip()
    return collapsed


def _parse_json_rows(text: str) -> pd.DataFrame:
    decoder = json.JSONDecoder()
    try:
        payload, end_index = decoder.raw_decode(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Could not parse pasted JSON: {exc.msg}") from exc

    trailing_text = text[end_index:].strip()
    if trailing_text:
        raise ValueError("Could not parse pasted JSON: unexpected trailing content after the JSON array.")
    if not isinstance(payload, list):
        raise ValueError("Could not parse pasted JSON: expected a top-level JSON array.")

    rows: list[dict[str, str]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"Could not parse pasted JSON: array item {index} is not an object.")
        rows.append({str(key): _stringify_value(value) for key, value in item.items()})
    return pd.DataFrame(rows)


def _parse_csv_rows(text: str) -> pd.DataFrame:
    try:
        frame = pd.read_csv(StringIO(text), dtype=str, keep_default_na=False, skip_blank_lines=True)
    except pd.errors.ParserError as exc:
        raise ValueError(f"Could not parse pasted CSV: {exc}") from exc

    if frame.empty and not list(frame.columns):
        raise ValueError("Pasted input is empty.")

    if not any(column in SUPPORTED_INPUT_HEADERS for column in frame.columns):
        raise ValueError(
            "Could not parse pasted input as a supported JSON array or CSV with recognized headers."
        )
    return frame


def parse_pasted_rows_text(text: str) -> pd.DataFrame:
    cleaned = _clean_pasted_text(text)
    if not cleaned:
        raise ValueError("Pasted input is empty.")

    if cleaned.startswith("["):
        frame = _parse_json_rows(cleaned)
    else:
        frame = _parse_csv_rows(cleaned)

    return normalize_incidents_frame(frame, require_url_values=True)


def convert_pasted_rows_file(input_path: str | Path, output_path: str | Path) -> pd.DataFrame:
    input_file = Path(input_path)
    output_file = Path(output_path)
    text = input_file.read_text(encoding="utf-8")
    normalized = parse_pasted_rows_text(text)
    deduped = deduplicate_incidents_frame(normalized)
    ensure_directory(output_file.parent)
    deduped.to_csv(output_file, index=False)
    return deduped[EXPECTED_COLUMNS].copy()
