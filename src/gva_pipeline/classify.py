from __future__ import annotations

from typing import Iterable

from .io_utils import normalize_whitespace
from .models import ClassificationResult, ContextFlags
from .patterns import CATEGORY_PATTERNS, CONTEXT_PATTERNS, PRIVATE_PARTY_PATTERNS, PUBLIC_TARGETING_PATTERNS


def _find_matches(text: str, patterns: Iterable[object]) -> list[str]:
    matches: list[str] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            value = normalize_whitespace(match.group(0))
            if value and value.lower() not in {item.lower() for item in matches}:
                matches.append(value)
    return matches


def _coerce_count(value: int | None) -> int:
    return value if isinstance(value, int) and value > 0 else 0


def extract_context_flags(text: str) -> ContextFlags:
    normalized = normalize_whitespace(text)
    return ContextFlags(
        mentions_party=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_party"])),
        mentions_argument=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_argument"])),
        mentions_domestic=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_domestic"])),
        mentions_bar_or_nightclub=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_bar_or_nightclub"])),
        mentions_school=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_school"])),
        mentions_store_or_restaurant=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_store_or_restaurant"])),
        mentions_drive_by=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_drive_by"])),
        mentions_street_takeover=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_street_takeover"])),
        mentions_family_relationship=bool(_find_matches(normalized, CONTEXT_PATTERNS["mentions_family_relationship"])),
    )


def classify_incident(
    text: str,
    context_flags: ContextFlags | None = None,
    *,
    victims_killed: int | None = None,
    victims_injured: int | None = None,
) -> ClassificationResult:
    normalized = normalize_whitespace(text)
    context = context_flags or extract_context_flags(normalized)
    workplace_matches = _find_matches(normalized, CATEGORY_PATTERNS["workplace_business"])
    private_party_matches = _find_matches(normalized, PRIVATE_PARTY_PATTERNS)
    total_victims = _coerce_count(victims_killed) + _coerce_count(victims_injured)

    domestic_strong = _find_matches(normalized, CATEGORY_PATTERNS["domestic_strong"])
    if domestic_strong:
        explanation = f"Matched domestic/family rule from article text: {', '.join(domestic_strong[:3])}"
        return ClassificationResult("domestic_family", 0.97, "domestic_strong", explanation)

    if context.mentions_domestic or context.mentions_family_relationship:
        explanation = "Matched domestic/family context helper based on relationship or domestic wording."
        return ClassificationResult("domestic_family", 0.88, "domestic_context_helper", explanation)

    school_matches = _find_matches(normalized, CATEGORY_PATTERNS["school_campus"])
    if school_matches or context.mentions_school:
        explanation = f"Matched school/campus setting: {', '.join((school_matches or ['school/campus context'])[:3])}"
        return ClassificationResult("school_campus", 0.95, "school_context", explanation)

    drive_by_matches = _find_matches(normalized, CONTEXT_PATTERNS["mentions_drive_by"])
    if drive_by_matches or context.mentions_drive_by:
        explanation = (
            "Matched drive-by or vehicle-fire language; treated as a targeted public-space incident: "
            f"{', '.join((drive_by_matches or ['drive-by context'])[:3])}"
        )
        return ClassificationResult("public_space_nonrandom", 0.91, "drive_by_public_nonrandom", explanation)

    public_event_matches = _find_matches(normalized, CATEGORY_PATTERNS["public_event_gathering"])
    if (
        public_event_matches
        and not context.mentions_domestic
        and not context.mentions_bar_or_nightclub
        and not private_party_matches
        and not workplace_matches
    ):
        explanation = f"Matched public event or gathering language: {', '.join(public_event_matches[:3])}"
        return ClassificationResult("public_event_gathering", 0.86, "public_event_terms", explanation)

    if workplace_matches:
        explanation = f"Matched explicit workplace indicators: {', '.join(workplace_matches[:3])}"
        return ClassificationResult("workplace_business", 0.92, "workplace_terms", explanation)

    nightlife_matches = _find_matches(normalized, CATEGORY_PATTERNS["nightlife_bar_district"])
    if nightlife_matches or context.mentions_bar_or_nightclub:
        explanation = f"Matched nightlife/bar context: {', '.join((nightlife_matches or ['bar/nightclub context'])[:3])}"
        return ClassificationResult("nightlife_bar_district", 0.9, "nightlife_terms", explanation)

    party_matches = _find_matches(normalized, CATEGORY_PATTERNS["party_social_event"])
    if party_matches or context.mentions_party:
        explanation = f"Matched party/social-event language: {', '.join((party_matches or ['party/social context'])[:3])}"
        return ClassificationResult("party_social_event", 0.88, "party_terms", explanation)

    gang_matches = _find_matches(normalized, CATEGORY_PATTERNS["gang_criminal_activity"])
    if gang_matches:
        explanation = f"Matched gang/criminal-activity language: {', '.join(gang_matches[:3])}"
        return ClassificationResult("gang_criminal_activity", 0.9, "gang_or_criminal_terms", explanation)

    dispute_matches = _find_matches(normalized, CATEGORY_PATTERNS["interpersonal_dispute"])
    if dispute_matches or context.mentions_argument:
        explanation = f"Matched interpersonal-dispute language: {', '.join((dispute_matches or ['argument/dispute context'])[:3])}"
        return ClassificationResult("interpersonal_dispute", 0.82, "dispute_terms", explanation)

    public_matches = _find_matches(normalized, CATEGORY_PATTERNS["public_space_nonrandom"])
    targeted_matches = _find_matches(normalized, PUBLIC_TARGETING_PATTERNS)
    if public_matches and targeted_matches:
        explanation = (
            "Matched public-space and targeted/nonrandom indicators: "
            f"{', '.join((public_matches + targeted_matches)[:4])}"
        )
        return ClassificationResult("public_space_nonrandom", 0.78, "public_targeted_terms", explanation)

    if public_matches or context.mentions_street_takeover or context.mentions_store_or_restaurant:
        explanation = "Matched public-facing location indicators without a stronger category."
        return ClassificationResult("public_space_nonrandom", 0.68, "public_location_terms", explanation)

    if (
        total_victims >= 3
        and not context.mentions_domestic
        and not context.mentions_party
        and not context.mentions_school
        and not context.mentions_bar_or_nightclub
        and not context.mentions_store_or_restaurant
        and not workplace_matches
    ):
        return ClassificationResult(
            "public_multi_victim_unclear",
            0.8,
            "public_multi_victim_fallback",
            "Multi-victim public incident with no stronger contextual signals",
        )

    return ClassificationResult(
        "unknown",
        0.0,
        "no_rule_match",
        "No transparent rule matched the available article text.",
    )
