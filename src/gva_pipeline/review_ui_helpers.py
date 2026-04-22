from __future__ import annotations

import pandas as pd


def summarize_filtered_queue(queue_frame: pd.DataFrame) -> dict[str, int]:
    saved_count = int((queue_frame["saved_review_status"] != "").sum()) if not queue_frame.empty else 0
    unresolved_count = int((queue_frame["saved_review_status"] != "resolved").sum()) if not queue_frame.empty else 0
    return {
        "total_queued": int(len(queue_frame.index)),
        "saved_review_count": saved_count,
        "unresolved_count": unresolved_count,
    }


def get_next_unresolved_incident_id(
    queue_frame: pd.DataFrame,
    current_incident_id: str,
) -> str | None:
    if queue_frame.empty or "incident_id" not in queue_frame.columns:
        return None

    incident_ids = queue_frame["incident_id"].astype(str).tolist()
    if not incident_ids:
        return None

    unresolved_ids = set(
        queue_frame.loc[queue_frame["saved_review_status"] != "resolved", "incident_id"].astype(str).tolist()
    )
    if not unresolved_ids:
        return None

    try:
        start_index = incident_ids.index(str(current_incident_id))
    except ValueError:
        start_index = -1

    for offset in range(1, len(incident_ids) + 1):
        candidate = incident_ids[(start_index + offset) % len(incident_ids)]
        if candidate in unresolved_ids:
            return candidate
    return None


def get_selected_source_link_target(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
