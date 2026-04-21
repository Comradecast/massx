from __future__ import annotations

from dataclasses import dataclass

from .io_utils import extract_provenance_snippet, normalize_whitespace
from .models import DemographicsResult
from .patterns import (
    AGE_PATTERNS,
    GENDER_PATTERNS,
    NUMBER_WORDS,
    RACE_PATTERNS,
    SENTENCE_SPLIT_RE,
    SUSPECT_ANCHOR_RE,
    SUSPECT_COUNT_PATTERNS,
)


@dataclass(slots=True)
class Candidate:
    value: str
    confidence: float
    snippet: str


def _split_sentences(text: str) -> list[str]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []
    return [sentence.strip() for sentence in SENTENCE_SPLIT_RE.split(normalized) if sentence.strip()]


def _suspect_sentences(text: str) -> list[str]:
    return [sentence for sentence in _split_sentences(text) if SUSPECT_ANCHOR_RE.search(sentence)]


def _count_candidates(text: str) -> list[int]:
    counts: list[int] = []
    normalized = normalize_whitespace(text)
    for pattern in SUSPECT_COUNT_PATTERNS:
        for match in pattern.finditer(normalized):
            raw_count = match.group("count").lower()
            if raw_count.isdigit():
                count = int(raw_count)
            else:
                count = NUMBER_WORDS.get(raw_count)
            if count is not None and count not in counts:
                counts.append(count)
    if not counts and " suspects " not in f" {normalized.lower()} " and SUSPECT_ANCHOR_RE.search(normalized):
        counts.append(1)
    return counts


def _collect_age_candidates(text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    normalized = normalize_whitespace(text)
    for sentence in _suspect_sentences(normalized):
        for pattern in AGE_PATTERNS:
            for match in pattern.finditer(sentence):
                snippet = extract_provenance_snippet(sentence, match.start(), match.end())
                value = match.group("age")
                if value not in {candidate.value for candidate in candidates}:
                    confidence = 0.98 if "-year-old" in match.group(0).lower() else 0.93
                    candidates.append(Candidate(value=value, confidence=confidence, snippet=snippet))
    return candidates


def _collect_gender_candidates(text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    normalized = normalize_whitespace(text)
    for sentence in _suspect_sentences(normalized):
        for gender, patterns in GENDER_PATTERNS.items():
            for pattern in patterns:
                for match in pattern.finditer(sentence):
                    snippet = extract_provenance_snippet(sentence, match.start(), match.end())
                    if gender not in {candidate.value for candidate in candidates}:
                        candidates.append(Candidate(value=gender, confidence=0.9, snippet=snippet))
    return candidates


def _collect_race_candidates(text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    normalized = normalize_whitespace(text)
    for sentence in _suspect_sentences(normalized):
        for race, patterns in RACE_PATTERNS.items():
            for pattern in patterns:
                for match in pattern.finditer(sentence):
                    snippet = extract_provenance_snippet(sentence, match.start(), match.end())
                    if race not in {candidate.value for candidate in candidates}:
                        candidates.append(Candidate(value=race, confidence=0.92, snippet=snippet))
    return candidates


def _resolve_single_value(candidates: list[Candidate], *, unknown_value: str = "unknown") -> tuple[str | None, float, str, str]:
    if not candidates:
        return unknown_value, 0.0, "", ""

    distinct_values = {candidate.value for candidate in candidates}
    if len(distinct_values) > 1:
        notes = f"Conflicting explicit values in article text: {', '.join(sorted(distinct_values))}"
        return unknown_value, 0.0, notes, candidates[0].snippet

    candidate = candidates[0]
    return candidate.value, candidate.confidence, "", candidate.snippet


def extract_suspect_demographics(text: str) -> DemographicsResult:
    normalized = normalize_whitespace(text)
    result = DemographicsResult()
    notes: list[str] = []

    suspect_count_candidates = _count_candidates(normalized)
    if len(suspect_count_candidates) == 1:
        result.suspect_count_estimate = suspect_count_candidates[0]
    elif len(suspect_count_candidates) > 1:
        notes.append(
            "Conflicting suspect-count references in article text: "
            + ", ".join(str(count) for count in sorted(set(suspect_count_candidates)))
        )

    age_candidates = _collect_age_candidates(normalized)
    age_value, age_confidence, age_notes, age_snippet = _resolve_single_value(age_candidates, unknown_value="unknown")
    if age_value != "unknown":
        result.suspect_age = int(age_value)
        result.suspect_age_confidence = age_confidence
    elif age_notes:
        notes.append(age_notes)

    gender_candidates = _collect_gender_candidates(normalized)
    gender_value, gender_confidence, gender_notes, gender_snippet = _resolve_single_value(gender_candidates)
    result.suspect_gender = gender_value or "unknown"
    result.suspect_gender_confidence = gender_confidence
    if gender_notes:
        notes.append(gender_notes)

    race_candidates = _collect_race_candidates(normalized)
    race_value, race_confidence, race_notes, race_snippet = _resolve_single_value(race_candidates)
    result.suspect_race = race_value or "unknown"
    result.suspect_race_confidence = race_confidence
    if race_notes:
        notes.append(race_notes)

    if result.suspect_count_estimate and result.suspect_count_estimate > 1:
        notes.append("Multiple suspects may be involved; single-value demographics may be incomplete.")

    if not result.suspect_age and age_candidates:
        notes.append("Age mentioned, but multiple explicit suspect ages conflicted.")

    best_snippet = next(
        (snippet for snippet in [age_snippet, gender_snippet, race_snippet] if snippet),
        "",
    )
    result.suspect_demographics_snippet = best_snippet
    result.suspect_demographics_notes = " ".join(dict.fromkeys(notes)).strip()
    return result
