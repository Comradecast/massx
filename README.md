# GVA Pipeline

A local, repeatable Python pipeline for enriching Gun Violence Archive "Mass Shootings" CSV exports with fetched source article text, deterministic incident context classification, and conservative suspect-demographics extraction.

## What It Does

The pipeline:

1. Loads a CSV export from the GVA mass-shootings table.
2. Deduplicates rows by `incident_id`.
3. Fetches `source_url` pages with `requests` using a browser-like user agent.
4. Extracts article text with transparent BeautifulSoup heuristics.
5. Applies rule-based context classification.
6. Extracts limited suspect demographics only when the article text states them explicitly.
7. Writes enriched CSV/JSON outputs and summary reports.

## Important Warnings

- Gun Violence Archive's "mass shooting" label is a threshold definition, not a behavioral category. This pipeline classifies context from source reporting; it does not reinterpret the GVA threshold itself.
- Article-based suspect demographics are incomplete, inconsistent, and subject to reporting bias. Missing values should not be treated as evidence that a demographic characteristic is absent.
- Race extraction is intentionally conservative and defaults to `unknown` unless the source text explicitly states the suspect's race.
- A fetched article may be inaccessible, incomplete, paywalled, dynamically rendered, or poorly structured for automated parsing.

## Requirements

- Python 3.11+
- Windows, macOS, or Linux

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pip install -e .
```

## Usage

```bash
python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out
```

Optional raw-HTML archiving:

```bash
python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out --save-html
```

Optional Excel companion exports with auto-fit columns:

```bash
python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out --excel-autofit
```

Optional resolved human review overrides:

```bash
python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out --human-review-results data/human_review_results.csv
```

## How To Acquire Enriched GVA Data

The recommended workflow is:

1. Open the GVA mass-shootings page in Chrome.
2. Open DevTools and paste the script from [tools/gva_dom_extract.js](c:/dev/MassX/tools/gva_dom_extract.js) into the Console.
3. Let the script extract the visible table rows. It will:
   - collect table fields and links
   - copy a JSON array to your clipboard
   - print a `console.table(...)` preview
4. Paste that clipboard output into a local text file, for example `data/pasted_rows.txt`.
5. Convert the pasted text into canonical CSV:

```bash
python -m gva_pipeline.cli convert-paste --input data/pasted_rows.txt --output data/incidents_canonical.csv
```

6. Run the main pipeline on the canonical CSV:

```bash
python -m gva_pipeline.cli --input data/incidents_canonical.csv --output-dir out
```

This acquisition path is deterministic and browser-assisted. It does not use Selenium, Playwright, or any hidden browser automation.

## Accepted Input Formats

The pipeline's supported input path is the canonical CSV, for example `data/incidents_canonical.csv`.

Two file shapes may appear during the workflow:

1. Canonical pipeline input:
   - `incident_id`
   - `incident_date`
   - `state`
   - `city_or_county`
   - `address`
   - `victims_killed`
   - `victims_injured`
   - `suspects_killed`
   - `suspects_injured`
   - `suspects_arrested`
   - `incident_url`
   - `source_url`

2. Raw GVA export:
   - `Incident ID`
   - `Incident Date`
   - `State`
   - `City Or County`
   - `Address`
   - `Victims Killed`
   - `Victims Injured`
   - `Suspects Killed`
   - `Suspects Injured`
   - `Suspects Arrested`
   - `Operations`

Use the canonical pipeline input for normal pipeline runs. A raw GVA export is not a supported default pipeline input because it may not contain usable `incident_url` and `source_url` values. The loader will attempt to recover those URLs from `Operations` when possible, but if they cannot be recovered the pipeline fails closed with a clear ingestion error instead of inventing missing links.

## Outputs

The pipeline writes:

- `enriched_incidents.csv`
- `enriched_incidents.json`
- `summary_by_category.csv`
- `summary_demographics.csv`
- `fetch_failures.csv`
- `human_review_queue.csv`
- `domain_review_summary.csv`
- `review_reason_summary.csv`
- `run_quality_summary.csv`

When `--excel-autofit` is enabled, it also writes `.xlsx` companions for each CSV output with Excel-friendly column widths. This is the closest automated equivalent to using `Alt`, `H`, `O`, `I` after opening the file in Excel. The CSV files themselves remain plain text and cannot store column widths.

## Human Review Queue

Each enriched incident row now includes deterministic review metadata so review routing stays transparent and auditable:

- `review_required`: whether the row should enter the human review queue
- `review_reason`: the single highest-priority review reason selected by rule
- `review_priority`: deterministic integer priority used for sorting the queue
- `needs_category_review`: whether any category-review rule fired
- `needs_source_review`: whether any source-review rule fired

`human_review_queue.csv` contains only rows where `review_required` is true. The queue is sorted by highest `review_priority` first, then newest `incident_date`, then `incident_id`.

## Domain Review Summary

`domain_review_summary.csv` shows which source domains are generating the most review and fetch friction after final override application. It groups enriched incidents by:

1. `source_domain` when present
2. otherwise `fetch_request_domain` when present
3. otherwise `unknown`

Metrics are built from final enriched rows after human review override application:

- `total_incidents`
- `fetched_ok_count`
- `fetch_failed_count`
- `no_article_text_count`
- `review_required_count`
- `review_applied_count`
- `category_override_count`
- `confidence_override_count`
- `source_override_count`
- `selected_source_overridden_count`
- `unknown_category_count`
- `fetch_failure_rate`
- `review_required_rate`
- `review_applied_rate`
- `source_override_rate`

Use this summary to spot high-friction domains, especially those with high review-required counts, repeated fetch failures, and frequent source/category overrides.

## Review Reason Summary

`review_reason_summary.csv` shows why incidents are entering the human review queue after final override application. It groups final enriched rows by the existing `review_reason` emitted by the pipeline. Rows where `review_required` is false, or where `review_reason` is blank, are grouped as `not_queued`.

Metrics are built from final enriched rows after human review override application:

- `total_incidents`
- `queued_incidents`
- `review_applied_count`
- `fetch_failed_count`
- `no_article_text_count`
- `unknown_category_count`
- `selected_source_overridden_count`
- `queued_rate`
- `review_applied_rate`

Use this summary to distinguish acquisition pain from classification pain. High `fetch_failed_count` or `no_article_text_count` points to source-acquisition problems, while high `unknown_category_count` under queued review reasons points to classification gaps that are still surfacing after fetch succeeds.

## Run Quality Summary

`run_quality_summary.csv` is a one-row snapshot of the current run after final override application. It captures fetch success/failure totals, queue and review counts, source-override counts, and the final category counts for the main incident buckets.

Use it as a lightweight drift check across repeated runs: if `fetch_failure_rate`, `unknown_category_rate`, `review_required_rate`, or one of the category counts moves unexpectedly, that is a quick signal that acquisition behavior, rule quality, or review load may have shifted.

## Human Review Results

You can optionally apply resolved human review outcomes with `--human-review-results`. The CSV schema must be exactly:

```csv
incident_id,review_status,final_category,final_confidence,notes,source_override
```

Only rows where `review_status` is exactly `resolved` are applied, and matching is exact by `incident_id`.

When a resolved review is applied:

- the machine-produced baseline values are preserved in `original_category`, `original_category_confidence`, and `original_selected_source_url`
- non-empty overrides replace `category`, `category_confidence`, and/or `selected_source_url`
- audit fields are populated: `review_applied`, `review_applied_fields`, `review_notes`, and `review_status`
- `selected_source_overridden` marks whether `selected_source_url` was replaced by a resolved human review result

`review_applied_fields` lists only the fields actually overridden, in deterministic order. When `review_applied` is true and `review_applied_fields` is empty, the incident was reviewed and resolved without changing the machine-produced values. Once a resolved review has been applied, that incident drops out of `human_review_queue.csv` on later runs even though the original machine review signals remain preserved in the enriched output.

## Reviewer UI

For local review entry, launch the Streamlit reviewer app against an existing queue and results file path:

```bash
streamlit run scripts/review_ui.py -- --queue out/human_review_queue.csv --results data/human_review_results.csv
```

The app reads `human_review_queue.csv`, optionally loads existing `human_review_results.csv`, and saves edits back using the exact required schema:

```csv
incident_id,review_status,final_category,final_confidence,notes,source_override
```

If the results file does not exist yet, the app creates it with that exact header before saving.

## Classification Categories

- `domestic_family`
- `party_social_event`
- `nightlife_bar_district`
- `interpersonal_dispute`
- `gang_criminal_activity`
- `public_space_nonrandom`
- `workplace_business`
- `school_campus`
- `unknown`

Each classification includes:

- a confidence score
- the matched rule name
- a human-readable explanation

School and party context is interpreted conservatively: `school_campus` now requires explicit school-location language such as `school`, `campus`, `on campus`, or `school grounds`, while student-only or prom-related context at a private residence is treated as party/social context unless a stronger rule applies.

## Sample Smoke-Test Dataset

```csv
incident_id,incident_date,state,city_or_county,address,victims_killed,victims_injured,suspects_killed,suspects_injured,suspects_arrested,incident_url,source_url
1001,2024-01-10,TX,Austin,123 Main St,1,4,0,0,1,https://example.com/incidents/1001,https://example.com/story-1001
1001,2024-01-10,TX,Austin,123 Main St,1,4,0,0,1,https://example.com/incidents/1001,https://example.com/story-1001
1002,2024-02-14,FL,Miami,456 Ocean Ave,0,5,0,0,0,https://example.com/incidents/1002,
```

## Limitations

- Some sites block requests or require JavaScript; the pipeline does not use Selenium.
- Article extraction uses deterministic HTML heuristics and may miss text on unusual layouts.
- The classification rules are intentionally transparent and auditable, but they are not exhaustive.
- When multiple suspects are described with conflicting demographics, the pipeline prefers `unknown` over collapsing them into a misleading single value.
- Source reporting may describe victims, witnesses, and suspects in the same sentence. Extraction logic errs toward missing data instead of false positives.

## Development

Run tests with:

```bash
python -m pytest
```
# massx
