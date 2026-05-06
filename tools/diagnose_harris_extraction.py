#!/usr/bin/env python3
"""Compare local Harris PDF/text/CSV extraction paths for one document."""

from __future__ import annotations

import argparse
import csv
from io import BytesIO
from pathlib import Path
import sys

import pdfplumber

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harris_foreclosure_scraper_free import parse_foreclosure_pdf
from scraper.core.pdfs import load_pdf_bytes, load_text
from scraper.counties.harris.parser import CSV_FIELDS, parse_foreclosure_text
from scraper.counties.harris.runner import extract_text_from_pdf_bytes


def read_csv_row(csv_path: Path, doc_id: str) -> dict | None:
    if not csv_path.exists():
        return None
    with csv_path.open(newline="", encoding="utf-8") as csvfile:
        for row in csv.DictReader(csvfile):
            if row.get("doc_id") == doc_id:
                return row
    return None


def selected_fields(row: dict | None) -> dict:
    if not row:
        return {}
    return {
        field: row.get(field)
        for field in CSV_FIELDS
        if field != "raw_text_snippet"
    }


def legacy_extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def print_section(title: str) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose local Harris extraction for one PDF/doc ID.")
    parser.add_argument("--doc-id", required=True)
    parser.add_argument("--pdf-dir", required=True)
    parser.add_argument("--text-dir", required=True)
    parser.add_argument("--csv", required=True, dest="csv_path")
    args = parser.parse_args()

    doc_id = args.doc_id
    pdf_dir = Path(args.pdf_dir)
    text_dir = Path(args.text_dir)
    csv_path = Path(args.csv_path)

    pdf_bytes = load_pdf_bytes(pdf_dir, doc_id)
    if not pdf_bytes:
        raise SystemExit(f"Missing PDF bytes for {doc_id} in {pdf_dir}")

    legacy_text = legacy_extract_text_from_pdf_bytes(pdf_bytes)
    fresh_text = extract_text_from_pdf_bytes(pdf_bytes)
    cached_text = load_text(text_dir, doc_id)
    legacy_parsed = parse_foreclosure_pdf(pdf_bytes, doc_id)
    fresh_parsed = parse_foreclosure_text(fresh_text, doc_id)
    cached_parsed = parse_foreclosure_text(cached_text or "", doc_id)
    csv_row = read_csv_row(csv_path, doc_id)

    print_section("Text Lengths")
    print(f"PDF bytes: {len(pdf_bytes)}")
    print(f"Legacy parse_foreclosure_pdf text length: {len(legacy_text)}")
    print(f"Fresh runner/pdfplumber text length: {len(fresh_text)}")
    print(f"Cached text length: {len(cached_text or '')}")

    print_section("Legacy Text First 500 Characters")
    print(repr(legacy_text[:500]))

    print_section("Fresh Text First 500 Characters")
    print(repr(fresh_text[:500]))

    print_section("Cached Text First 500 Characters")
    print(repr((cached_text or "")[:500]))

    print_section("Legacy parse_foreclosure_pdf Output")
    print(selected_fields(legacy_parsed))

    print_section("Monthly Fresh parse_foreclosure_text Output")
    print(selected_fields(fresh_parsed))

    print_section("Monthly Cached parse_foreclosure_text Output")
    print(selected_fields(cached_parsed))

    print_section("Existing CSV Row")
    print(selected_fields(csv_row))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
