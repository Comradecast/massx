from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .acquisition import convert_pasted_rows_file
from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MassX tools for acquiring and enriching GVA incident data.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the enrichment pipeline on a canonical CSV.")
    run_parser.add_argument("--input", required=True, help="Path to the input GVA CSV export.")
    run_parser.add_argument("--output-dir", required=True, help="Directory for pipeline outputs.")
    run_parser.add_argument(
        "--manual-review-file",
        default=None,
        help="Optional path to manual review CSV. Defaults to data/manual_reviews.csv when present.",
    )
    run_parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw HTML to output-dir/raw_html.",
    )
    run_parser.add_argument(
        "--excel-autofit",
        action="store_true",
        help="Also write .xlsx companions with Excel-style auto-fit column widths.",
    )
    run_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=8.0,
        help="HTTP request timeout in seconds.",
    )
    run_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional incident limit for testing or sampling. Defaults to processing all incidents.",
    )
    run_parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=10.0,
        help="Seconds between heartbeat status lines during long runs.",
    )
    run_parser.add_argument(
        "--verbose-lifecycle",
        action="store_true",
        help="Print per-incident lifecycle lines in addition to heartbeat status.",
    )

    convert_parser = subparsers.add_parser(
        "convert-paste",
        help="Convert pasted browser-extracted JSON or CSV text into canonical incident CSV.",
    )
    convert_parser.add_argument("--input", required=True, help="Path to pasted JSON or CSV text.")
    convert_parser.add_argument("--output", required=True, help="Path for the canonical CSV output.")
    return parser


def _normalize_argv(argv: list[str] | None) -> list[str]:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] not in {"run", "convert-paste", "-h", "--help"}:
        return ["run", *raw_args]
    return raw_args


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(_normalize_argv(argv))

    try:
        if args.command == "convert-paste":
            converted = convert_pasted_rows_file(args.input, args.output)
            print(f"Wrote {len(converted.index)} canonical incident rows to {Path(args.output)}", flush=True)
        else:
            run_pipeline(
                input_path=args.input,
                output_dir=args.output_dir,
                manual_review_path=args.manual_review_file,
                save_html=args.save_html,
                write_excel_autofit=args.excel_autofit,
                timeout_seconds=args.timeout_seconds,
                limit=args.limit,
                heartbeat_seconds=args.heartbeat_seconds,
                verbose_lifecycle=args.verbose_lifecycle,
            )
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
