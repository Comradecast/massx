from __future__ import annotations

import json
import re
from dataclasses import asdict, is_dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

from .models import IncidentRecord
from .patterns import TEXT_WHITESPACE_RE

EXPECTED_COLUMNS = [
    "incident_id",
    "incident_date",
    "state",
    "city_or_county",
    "address",
    "victims_killed",
    "victims_injured",
    "suspects_killed",
    "suspects_injured",
    "suspects_arrested",
    "incident_url",
    "source_url",
]
OPTIONAL_COLUMNS = ["source_candidates"]

RAW_GVA_COLUMN_MAP = {
    "Incident ID": "incident_id",
    "Incident Date": "incident_date",
    "State": "state",
    "City Or County": "city_or_county",
    "Address": "address",
    "Victims Killed": "victims_killed",
    "Victims Injured": "victims_injured",
    "Suspects Killed": "suspects_killed",
    "Suspects Injured": "suspects_injured",
    "Suspects Arrested": "suspects_arrested",
    "Operations": "operations",
}

NUMERIC_COLUMNS = [
    "victims_killed",
    "victims_injured",
    "suspects_killed",
    "suspects_injured",
    "suspects_arrested",
]

URL_RE = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)


def normalize_whitespace(text: str | None) -> str:
    if not text:
        return ""
    return TEXT_WHITESPACE_RE.sub(" ", text).strip()


def clean_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = normalize_whitespace(str(value))
    return text or None


def coerce_int(value: Any) -> int | None:
    text = clean_optional_str(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_date(value: Any) -> date | None:
    text = clean_optional_str(value)
    if text is None:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def extract_provenance_snippet(text: str, start: int, end: int, *, window: int = 160) -> str:
    normalized = normalize_whitespace(text)
    if not normalized:
        return ""
    left = max(0, start - window)
    right = min(len(normalized), end + window)
    snippet = normalized[left:right].strip()
    if left > 0:
        snippet = f"...{snippet}"
    if right < len(normalized):
        snippet = f"{snippet}..."
    return snippet


def _is_probable_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _is_gva_url(value: str) -> bool:
    return "gunviolencearchive.org" in urlparse(value).netloc.lower()


def _ordered_unique_urls(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_whitespace(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _source_candidates_to_text(candidates: list[str]) -> str:
    ordered = _ordered_unique_urls(candidates)
    if not ordered:
        return ""
    return json.dumps(ordered, ensure_ascii=False)


def parse_source_candidates_value(value: Any) -> list[str]:
    text = clean_optional_str(value)
    if text is None:
        return []

    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return _ordered_unique_urls([str(item) for item in parsed])

    candidates = [normalize_whitespace(match.group(0)) for match in URL_RE.finditer(text)]
    if candidates:
        return _ordered_unique_urls(candidates)

    return _ordered_unique_urls([part for part in text.split("|")])


def _extract_operations_urls(value: Any) -> tuple[str | None, list[str]]:
    text = clean_optional_str(value)
    if text is None:
        return None, []

    soup = BeautifulSoup(text, "html.parser")
    candidates: list[tuple[str, str]] = []

    for anchor in soup.find_all("a", href=True):
        href = normalize_whitespace(anchor.get("href"))
        label = normalize_whitespace(anchor.get_text(" ", strip=True)).lower()
        if href and _is_probable_url(href):
            candidates.append((href, label))

    for match in URL_RE.finditer(text):
        href = normalize_whitespace(match.group(0))
        if href and _is_probable_url(href) and href not in {url for url, _ in candidates}:
            candidates.append((href, ""))

    incident_url: str | None = None
    labeled_source_urls: list[str] = []
    fallback_source_urls: list[str] = []

    for href, label in candidates:
        if incident_url is None and ("incident" in label or "gva" in label):
            incident_url = href
            continue
        if "source" in label and not _is_gva_url(href):
            labeled_source_urls.append(href)

    for href, _ in candidates:
        if incident_url is None and _is_gva_url(href):
            incident_url = href
            continue
        if not _is_gva_url(href):
            fallback_source_urls.append(href)

    if incident_url is None and len(candidates) == 1 and _is_gva_url(candidates[0][0]):
        incident_url = candidates[0][0]
    if not fallback_source_urls and len(candidates) == 1 and not _is_gva_url(candidates[0][0]):
        fallback_source_urls = [candidates[0][0]]

    source_urls = _ordered_unique_urls([*labeled_source_urls, *fallback_source_urls])
    return incident_url, source_urls


def _normalize_input_columns(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str], bool]:
    original_columns = frame.columns.tolist()
    normalized = frame.rename(columns=RAW_GVA_COLUMN_MAP).copy()
    operations_inspected = "operations" in normalized.columns
    has_source_candidates_column = "source_candidates" in normalized.columns

    if "incident_url" not in normalized.columns:
        normalized["incident_url"] = ""
    if "source_url" not in normalized.columns:
        normalized["source_url"] = ""
    if has_source_candidates_column:
        normalized["source_candidates"] = normalized["source_candidates"].map(
            lambda value: _source_candidates_to_text(parse_source_candidates_value(value))
        )

    if operations_inspected:
        extracted = normalized["operations"].map(_extract_operations_urls)
        incident_candidates = extracted.map(lambda item: item[0] or "")
        source_candidate_lists = extracted.map(lambda item: item[1])
        source_candidates = source_candidate_lists.map(lambda item: item[0] if item else "")

        normalized["incident_url"] = normalized["incident_url"].where(
            normalized["incident_url"].map(bool),
            incident_candidates,
        )
        normalized["source_url"] = normalized["source_url"].where(
            normalized["source_url"].map(bool),
            source_candidates,
        )
        operations_source_candidates = source_candidate_lists.map(_source_candidates_to_text)
        if has_source_candidates_column:
            normalized["source_candidates"] = normalized["source_candidates"].where(
                normalized["source_candidates"].map(bool),
                operations_source_candidates,
            )
        elif operations_source_candidates.map(bool).any():
            normalized["source_candidates"] = operations_source_candidates

    if "source_candidates" in normalized.columns:
        normalized["source_url"] = normalized.apply(
            lambda row: row["source_url"]
            if normalize_whitespace(str(row.get("source_url", "")))
            else (parse_source_candidates_value(row.get("source_candidates")) or [""])[0],
            axis=1,
        )
        normalized["source_candidates"] = normalized.apply(
            lambda row: _source_candidates_to_text(
                [row.get("source_url", ""), *parse_source_candidates_value(row.get("source_candidates"))]
            ),
            axis=1,
        )

    return normalized, original_columns, operations_inspected


def _raise_missing_columns_error(original_columns: list[str], missing_columns: list[str], *, operations_inspected: bool) -> None:
    found_text = ", ".join(original_columns)
    missing_text = ", ".join(missing_columns)
    operations_text = "yes" if operations_inspected else "no"
    raise ValueError(
        "Missing required column(s) after normalization: "
        f"{missing_text}. Found columns: {found_text}. Operations inspected: {operations_text}."
    )


def _raise_missing_url_values_error(
    original_columns: list[str],
    missing_url_fields: list[str],
    *,
    operations_inspected: bool,
) -> None:
    found_text = ", ".join(original_columns)
    missing_text = ", ".join(missing_url_fields)
    operations_text = "yes" if operations_inspected else "no"
    raise ValueError(
        "Input file is missing or has unusable required URL field(s): "
        f"{missing_text}. Found columns: {found_text}. Operations inspected: {operations_text}. "
        "Raw GVA exports are unsupported pipeline inputs unless they already provide usable incident_url and "
        "source_url values. Use the canonical pipeline input file instead, for example "
        "data/incidents_canonical.csv."
    )


def normalize_incidents_frame(frame: pd.DataFrame, *, require_url_values: bool = True) -> pd.DataFrame:
    normalized_frame, original_columns, operations_inspected = _normalize_input_columns(frame)
    missing_columns = [column for column in EXPECTED_COLUMNS if column not in normalized_frame.columns]
    if missing_columns:
        _raise_missing_columns_error(original_columns, missing_columns, operations_inspected=operations_inspected)

    if require_url_values:
        missing_url_fields = [
            column
            for column in ("incident_url", "source_url")
            if normalized_frame[column].map(normalize_whitespace).eq("").all()
        ]
        if missing_url_fields:
            _raise_missing_url_values_error(
                original_columns,
                missing_url_fields,
                operations_inspected=operations_inspected,
            )

    output_columns = EXPECTED_COLUMNS.copy()
    for column in OPTIONAL_COLUMNS:
        if column in normalized_frame.columns:
            output_columns.append(column)

    return normalized_frame[output_columns].copy()


def read_incidents_csv(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    return normalize_incidents_frame(frame, require_url_values=True)


def deduplicate_incidents_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    working["incident_id"] = working["incident_id"].map(lambda value: normalize_whitespace(str(value)))
    working = working[working["incident_id"] != ""].copy()

    def choose_value(series: pd.Series) -> str:
        for raw_value in series.tolist():
            value = normalize_whitespace(str(raw_value))
            if value:
                return value
        return ""

    aggregated = (
        working.groupby("incident_id", sort=False, dropna=False)
        .agg({column: choose_value for column in working.columns if column != "incident_id"})
        .reset_index()
    )
    output_columns = EXPECTED_COLUMNS.copy()
    for column in OPTIONAL_COLUMNS:
        if column in aggregated.columns:
            output_columns.append(column)
    return aggregated[output_columns].copy()


def frame_to_incident_records(frame: pd.DataFrame) -> list[IncidentRecord]:
    records: list[IncidentRecord] = []
    for row in frame.to_dict(orient="records"):
        source_candidates = tuple(
            parse_source_candidates_value(row.get("source_candidates", row.get("source_url")))
        )
        source_url = clean_optional_str(row["source_url"])
        candidate_origins: list[tuple[str, str]] = []
        seen_candidates: set[str] = set()
        if source_url:
            candidate_origins.append((source_url, "original"))
            seen_candidates.add(source_url)
        for candidate in source_candidates:
            if candidate in seen_candidates:
                continue
            candidate_origins.append((candidate, "unknown"))
            seen_candidates.add(candidate)
        records.append(
            IncidentRecord(
                incident_id=normalize_whitespace(row["incident_id"]),
                incident_date=parse_date(row["incident_date"]),
                state=clean_optional_str(row["state"]),
                city_or_county=clean_optional_str(row["city_or_county"]),
                address=clean_optional_str(row["address"]),
                victims_killed=coerce_int(row["victims_killed"]),
                victims_injured=coerce_int(row["victims_injured"]),
                suspects_killed=coerce_int(row["suspects_killed"]),
                suspects_injured=coerce_int(row["suspects_injured"]),
                suspects_arrested=coerce_int(row["suspects_arrested"]),
                incident_url=clean_optional_str(row["incident_url"]),
                source_url=source_url,
                source_candidates=source_candidates,
                source_candidate_origins=tuple(candidate_origins),
            )
        )
    return records


def ensure_directory(path: str | Path) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_json_records(path: str | Path, records: list[dict[str, Any]]) -> None:
    with Path(path).open("w", encoding="utf-8") as handle:
        json.dump(records, handle, ensure_ascii=False, indent=2)


def serialize_value(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return {key: serialize_value(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {key: serialize_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [serialize_value(item) for item in value]
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    return value
