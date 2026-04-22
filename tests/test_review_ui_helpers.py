from __future__ import annotations

import pandas as pd

from gva_pipeline.review_ui_helpers import (
    get_next_unresolved_incident_id,
    get_selected_source_link_target,
    summarize_filtered_queue,
)


def test_get_next_unresolved_incident_id_uses_current_filtered_queue() -> None:
    filtered_queue = pd.DataFrame(
        [
            {"incident_id": "a1", "saved_review_status": "resolved"},
            {"incident_id": "b2", "saved_review_status": ""},
            {"incident_id": "c3", "saved_review_status": "resolved"},
            {"incident_id": "d4", "saved_review_status": "pending"},
        ]
    )

    assert get_next_unresolved_incident_id(filtered_queue, "a1") == "b2"
    assert get_next_unresolved_incident_id(filtered_queue, "b2") == "d4"
    assert get_next_unresolved_incident_id(filtered_queue, "d4") == "b2"


def test_get_selected_source_link_target_handles_blank_and_present_values() -> None:
    assert get_selected_source_link_target("") is None
    assert get_selected_source_link_target("   ") is None
    assert get_selected_source_link_target("https://example.com/story") == "https://example.com/story"


def test_summarize_filtered_queue_counts_saved_and_unresolved_rows() -> None:
    filtered_queue = pd.DataFrame(
        [
            {"incident_id": "a1", "saved_review_status": ""},
            {"incident_id": "b2", "saved_review_status": "resolved"},
            {"incident_id": "c3", "saved_review_status": "pending"},
        ]
    )

    summary = summarize_filtered_queue(filtered_queue)

    assert summary == {
        "total_queued": 3,
        "saved_review_count": 2,
        "unresolved_count": 2,
    }
