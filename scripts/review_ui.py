from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import streamlit as st

from gva_pipeline.review_results_io import (
    HUMAN_REVIEW_RESULTS_COLUMNS,
    build_review_result_row,
    delete_human_review_result_row,
    ensure_human_review_results_file,
    read_human_review_results_frame,
    upsert_human_review_result_row,
    write_human_review_results_frame,
)
from gva_pipeline.review_ui_helpers import (
    get_next_unresolved_incident_id,
    get_selected_source_link_target,
    summarize_filtered_queue,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local Streamlit reviewer for MassX human review results.")
    parser.add_argument("--queue", required=True, help="Path to human_review_queue.csv")
    parser.add_argument("--results", required=True, help="Path to human_review_results.csv")
    return parser.parse_args()


def load_queue(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "incident_id" not in frame.columns:
        raise ValueError(f"Review queue is missing required column: incident_id ({path})")
    return frame


def merge_review_status(queue_frame: pd.DataFrame, results_frame: pd.DataFrame) -> pd.DataFrame:
    working = queue_frame.copy()
    results_lookup = (
        results_frame.set_index("incident_id")[["review_status"]].rename(columns={"review_status": "saved_review_status"})
        if not results_frame.empty
        else pd.DataFrame(columns=["saved_review_status"])
    )
    merged = working.merge(results_lookup, how="left", left_on="incident_id", right_index=True)
    merged["saved_review_status"] = merged["saved_review_status"].fillna("")
    return merged


def incident_label(row: pd.Series) -> str:
    priority = row.get("review_priority", "")
    date = row.get("incident_date", "")
    location = ", ".join([part for part in [row.get("city_or_county", ""), row.get("state", "")] if part])
    return f"{row['incident_id']} | {date} | P{priority} | {location or 'unknown location'}"


def filter_queue(queue_frame: pd.DataFrame, filter_value: str) -> pd.DataFrame:
    if filter_value == "unresolved only":
        return queue_frame[queue_frame["saved_review_status"] != "resolved"].copy()
    if filter_value == "resolved only":
        return queue_frame[queue_frame["saved_review_status"] == "resolved"].copy()
    return queue_frame.copy()


def get_existing_result(results_frame: pd.DataFrame, incident_id: str) -> dict[str, str]:
    if results_frame.empty:
        return {column: "" for column in HUMAN_REVIEW_RESULTS_COLUMNS}
    matches = results_frame.loc[results_frame["incident_id"] == incident_id]
    if matches.empty:
        return {column: "" for column in HUMAN_REVIEW_RESULTS_COLUMNS}
    return matches.iloc[0].to_dict()


def save_review_result(
    *,
    results_path: Path,
    results_frame: pd.DataFrame,
    incident_id: str,
    review_status: str,
    final_category: str,
    final_confidence: str,
    notes: str,
    source_override: str,
) -> pd.DataFrame:
    new_row = build_review_result_row(
        incident_id=incident_id,
        review_status=review_status,
        final_category=final_category,
        final_confidence=final_confidence,
        notes=notes,
        source_override=source_override,
    )
    updated = upsert_human_review_result_row(results_frame, new_row)
    write_human_review_results_frame(results_path, updated)
    return updated


def clear_review_result(
    *,
    results_path: Path,
    results_frame: pd.DataFrame,
    incident_id: str,
) -> pd.DataFrame:
    updated = delete_human_review_result_row(results_frame, incident_id)
    write_human_review_results_frame(results_path, updated)
    return updated


def main() -> None:
    args = parse_args()
    queue_path = Path(args.queue)
    results_path = ensure_human_review_results_file(args.results)

    st.set_page_config(page_title="MassX Review UI", layout="wide")
    st.title("MassX Human Review")
    st.caption("Local-only reviewer for human_review_queue.csv -> human_review_results.csv")

    queue_frame = load_queue(queue_path)
    results_frame = read_human_review_results_frame(results_path)
    merged_queue = merge_review_status(queue_frame, results_frame)

    filter_value = st.sidebar.radio("Queue filter", ["all queued", "unresolved only", "resolved only"], index=1)
    filtered_queue = filter_queue(merged_queue, filter_value)
    queue_summary = summarize_filtered_queue(filtered_queue)

    st.sidebar.markdown(f"Queue path: `{queue_path}`")
    st.sidebar.markdown(f"Results path: `{results_path}`")
    st.sidebar.markdown(f"Visible incidents: `{len(filtered_queue.index)}`")

    if filtered_queue.empty:
        st.info("No incidents match the current filter.")
        return

    label_by_id = {
        str(row["incident_id"]): incident_label(row)
        for _, row in filtered_queue.iterrows()
    }
    visible_incident_ids = list(label_by_id.keys())
    if st.session_state.get("selected_incident_id") not in visible_incident_ids:
        st.session_state["selected_incident_id"] = visible_incident_ids[0]

    st.sidebar.selectbox(
        "Select incident",
        visible_incident_ids,
        format_func=lambda value: label_by_id[value],
        key="selected_incident_id",
    )
    incident_id = str(st.session_state["selected_incident_id"])
    selected_row = filtered_queue.loc[filtered_queue["incident_id"] == incident_id].iloc[0]
    existing_result = get_existing_result(results_frame, incident_id)
    has_saved_review = bool(existing_result["incident_id"])
    source_link_target = get_selected_source_link_target(selected_row.get("selected_source_url", ""))
    next_unresolved_incident_id = get_next_unresolved_incident_id(filtered_queue, incident_id)

    st.subheader(f"Incident {incident_id}")
    status_text = "Saved review exists" if has_saved_review else "No saved review yet"
    st.write(status_text)

    summary_col_1, summary_col_2, summary_col_3 = st.columns(3)
    summary_col_1.metric("Queued in current filter", queue_summary["total_queued"])
    summary_col_2.metric("Saved review rows", queue_summary["saved_review_count"])
    summary_col_3.metric("Still unresolved", queue_summary["unresolved_count"])

    details_col_1, details_col_2 = st.columns(2)
    with details_col_1:
        st.write(f"Incident date: {selected_row.get('incident_date', '')}")
        st.write(f"State: {selected_row.get('state', '')}")
        st.write(f"City / county: {selected_row.get('city_or_county', '')}")
        st.write(f"Current category: {selected_row.get('category', '')}")
        st.write(f"Category confidence: {selected_row.get('category_confidence', '')}")
    with details_col_2:
        st.write(f"Review reason: {selected_row.get('review_reason', '')}")
        st.write(f"Review priority: {selected_row.get('review_priority', '')}")
        if source_link_target:
            st.markdown(f"Selected source URL: [{source_link_target}]({source_link_target})")
        else:
            st.write("Selected source URL:")

    action_col_1, action_col_2 = st.columns(2)
    with action_col_1:
        if st.button("Next unresolved incident"):
            if next_unresolved_incident_id is None:
                st.info("No unresolved incidents remain in the current filter.")
            elif next_unresolved_incident_id == incident_id:
                st.info("This is the only unresolved incident in the current filter.")
            else:
                st.session_state["selected_incident_id"] = next_unresolved_incident_id
                st.rerun()
    with action_col_2:
        if st.button("Clear saved review for this incident", disabled=not has_saved_review):
            try:
                updated_results = clear_review_result(
                    results_path=results_path,
                    results_frame=results_frame,
                    incident_id=incident_id,
                )
            except ValueError as exc:
                st.error(str(exc))
            else:
                st.success(f"Cleared saved review row for incident_id={incident_id}")
                st.caption(f"Results file now contains {len(updated_results.index)} row(s).")
                st.rerun()

    st.markdown("### Queue view")
    st.dataframe(
        filtered_queue[
            [
                "incident_id",
                "incident_date",
                "state",
                "city_or_county",
                "review_reason",
                "review_priority",
                "saved_review_status",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Article text")
    st.text_area(
        "article_text",
        value=str(selected_row.get("article_text", "")),
        height=320,
        disabled=True,
        label_visibility="collapsed",
    )

    st.markdown("### Review entry")
    review_status_options = ["", "pending", "resolved"]
    existing_status = existing_result["review_status"]
    if existing_status and existing_status not in review_status_options:
        review_status_options.append(existing_status)

    with st.form(f"review-form-{incident_id}"):
        review_status = st.selectbox(
            "review_status",
            review_status_options,
            index=review_status_options.index(existing_status) if existing_status in review_status_options else 0,
        )
        final_category = st.text_input("final_category", value=existing_result["final_category"])
        final_confidence = st.text_input("final_confidence", value=existing_result["final_confidence"])
        source_override = st.text_input("source_override", value=existing_result["source_override"])
        notes = st.text_area("notes", value=existing_result["notes"], height=140)
        saved = st.form_submit_button("Save review")

    if saved:
        try:
            updated_results = save_review_result(
                results_path=results_path,
                results_frame=results_frame,
                incident_id=incident_id,
                review_status=review_status,
                final_category=final_category,
                final_confidence=final_confidence,
                notes=notes,
                source_override=source_override,
            )
        except ValueError as exc:
            st.error(str(exc))
        else:
            st.success(f"Saved review row for incident_id={incident_id}")
            st.caption(f"Results file now contains {len(updated_results.index)} row(s).")
            st.rerun()


if __name__ == "__main__":
    main()
