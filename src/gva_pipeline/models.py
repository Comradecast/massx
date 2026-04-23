from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any


@dataclass(slots=True)
class ManualReviewRecord:
    incident_id: str
    review_status: str | None = None
    decision_type: str | None = None
    preferred_source_url: str | None = None
    added_source_candidates: tuple[str, ...] = ()
    rejected_candidates: tuple[str, ...] = ()
    review_notes: str | None = None
    reviewer: str | None = None
    review_timestamp: str | None = None


@dataclass(slots=True)
class HumanReviewResultRecord:
    incident_id: str
    review_status: str
    final_category: str | None = None
    final_confidence: float | None = None
    notes: str | None = None
    source_override: str | None = None


@dataclass(slots=True)
class IncidentRecord:
    incident_id: str
    incident_date: date | None
    state: str | None
    city_or_county: str | None
    address: str | None
    victims_killed: int | None
    victims_injured: int | None
    suspects_killed: int | None
    suspects_injured: int | None
    suspects_arrested: int | None
    incident_url: str | None
    source_url: str | None
    source_candidates: tuple[str, ...] = ()
    source_candidate_origins: tuple[tuple[str, str], ...] = ()
    manual_review: ManualReviewRecord | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "incident_date": self.incident_date.isoformat() if self.incident_date else None,
            "state": self.state,
            "city_or_county": self.city_or_county,
            "address": self.address,
            "victims_killed": self.victims_killed,
            "victims_injured": self.victims_injured,
            "suspects_killed": self.suspects_killed,
            "suspects_injured": self.suspects_injured,
            "suspects_arrested": self.suspects_arrested,
            "incident_url": self.incident_url,
            "source_url": self.source_url,
        }


@dataclass(slots=True)
class FetchResult:
    requested_url: str | None
    final_url: str | None
    status_code: int | None
    ok: bool
    error: str | None
    article_text: str | None
    raw_html: str | None = None
    acquisition_status: str = "fetched"
    failure_stage: str | None = None
    failure_reason: str | None = None
    source_category: str = "UNKNOWN"
    source_action: str = "fetch"
    retryable: bool = False
    attempts: int = 1


@dataclass(slots=True)
class IncidentAcquisitionResult:
    fetch_result: FetchResult
    selected_source_url: str | None
    selected_source_origin: str
    source_candidates_count: int
    source_attempt_count: int
    source_attempt_history: str
    manual_review_applied: bool = False
    manual_review_status: str | None = None
    manual_review_decision_type: str | None = None
    manual_review_preferred_source_url: str | None = None
    manual_review_added_candidates: str = ""
    manual_review_rejected_candidates: str = ""
    manual_review_notes: str | None = None
    manual_review_reviewer: str | None = None
    manual_review_timestamp: str | None = None


@dataclass(slots=True)
class ContextFlags:
    mentions_party: bool = False
    mentions_argument: bool = False
    mentions_domestic: bool = False
    mentions_bar_or_nightclub: bool = False
    mentions_school: bool = False
    mentions_public_location: bool = False
    mentions_store_or_restaurant: bool = False
    mentions_drive_by: bool = False
    mentions_street_takeover: bool = False
    mentions_family_relationship: bool = False

    def to_dict(self) -> dict[str, bool]:
        return asdict(self)


@dataclass(slots=True)
class ClassificationResult:
    category: str
    confidence: float
    matched_rule: str
    explanation: str


@dataclass(slots=True)
class DemographicsResult:
    suspect_age: int | None = None
    suspect_age_confidence: float = 0.0
    suspect_gender: str = "unknown"
    suspect_gender_confidence: float = 0.0
    suspect_race: str = "unknown"
    suspect_race_confidence: float = 0.0
    suspect_count_estimate: int | None = None
    suspect_demographics_notes: str = ""
    suspect_demographics_snippet: str = ""


@dataclass(slots=True)
class PipelineReport:
    total_unique_incidents: int
    category_counts: dict[str, int] = field(default_factory=dict)
    usable_age_count: int = 0
    usable_gender_count: int = 0
    usable_race_count: int = 0
    unknown_age_percentage: float = 0.0
    unknown_gender_percentage: float = 0.0
    unknown_race_percentage: float = 0.0
    warning_messages: list[str] = field(default_factory=list)
