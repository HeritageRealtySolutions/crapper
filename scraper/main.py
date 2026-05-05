"""Command line entry point for monthly foreclosure scraper runs."""

from __future__ import annotations

import argparse
import asyncio

from scraper.counties.harris.runner import run_harris_monthly


def month_arg(value: str) -> int:
    month = int(value)
    if not 1 <= month <= 12:
        raise argparse.ArgumentTypeError("month must be between 1 and 12")
    return month


def positive_limit_arg(value: str) -> int:
    limit = int(value)
    if limit < 1:
        raise argparse.ArgumentTypeError("limit must be 1 or greater")
    return limit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monthly foreclosure scraper runner")
    parser.add_argument("--county", required=True, help="County to scrape. Currently only 'harris' is supported.")
    parser.add_argument("--year", required=True, type=int, help="Sale year, for example 2026.")
    parser.add_argument("--month", required=True, type=month_arg, help="Sale month number, 1-12.")
    parser.add_argument("--limit", type=positive_limit_arg, default=None, help="Optional maximum records to process.")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resume", action="store_true", help="Resume from the monthly checkpoint.")
    mode.add_argument("--retry-failed", action="store_true", help="Retry records marked failed in the checkpoint.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    county = args.county.strip().lower()
    if county != "harris":
        parser.error("only county=harris is supported right now")

    result = asyncio.run(
        run_harris_monthly(
            year=args.year,
            month=args.month,
            limit=args.limit,
            resume=args.resume,
            retry_failed=args.retry_failed,
        )
    )

    print(f"Processed: {result.processed}")
    print(f"Skipped: {result.skipped}")
    print(f"Failed: {result.failed}")
    print(f"CSV: {result.paths.output_csv}")
    print(f"JSONL: {result.paths.output_jsonl}")
    print(f"Checkpoint: {result.paths.checkpoint_json}")
    print(f"Failed records: {result.paths.failed_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
