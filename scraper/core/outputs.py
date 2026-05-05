"""Reusable output path helpers for monthly scraper runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MonthlyPaths:
    output_csv: Path
    output_jsonl: Path
    checkpoint_json: Path
    failed_json: Path
    text_cache_dir: Path
    pdfs_dir: Path


def build_monthly_paths(county: str, year: int, month: int) -> MonthlyPaths:
    if not county:
        raise ValueError("county is required")
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")

    county_slug = county.strip().lower()
    month_token = f"{month:02d}"
    run_token = f"{county_slug}_{year}_{month_token}"

    return MonthlyPaths(
        output_csv=Path("data") / "outputs" / f"{run_token}_foreclosures.csv",
        output_jsonl=Path("data") / "outputs" / f"{run_token}_foreclosures.jsonl",
        checkpoint_json=Path("data") / "checkpoints" / f"{run_token}_checkpoint.json",
        failed_json=Path("data") / "failed" / f"{run_token}_failed.json",
        text_cache_dir=Path("data") / "text_cache" / county_slug / str(year) / month_token,
        pdfs_dir=Path("data") / "pdfs" / county_slug / str(year) / month_token,
    )
