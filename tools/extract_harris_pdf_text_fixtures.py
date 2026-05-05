#!/usr/bin/env python3
"""Extract Harris County raw text fixtures from already-downloaded PDFs.

This helper is for T0 golden-sample preparation only. It does not scrape,
download, parse, or call live services. It mirrors the current scraper's
pdfplumber text extraction approach and writes the full raw extracted text
to baseline and test fixture directories.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract raw Harris County text fixtures from local PDFs."
    )
    parser.add_argument(
        "--pdf-dir",
        default="foreclosure_pdfs",
        help="Directory containing already-downloaded Harris PDF files.",
    )
    parser.add_argument(
        "--doc-ids",
        required=True,
        help="Comma-separated selected Harris FRCL document IDs.",
    )
    parser.add_argument(
        "--output-root",
        default="baseline/golden_samples/harris/ocr_text_samples",
        help="Baseline raw text fixture output directory.",
    )
    parser.add_argument(
        "--fixtures-root",
        default="tests/fixtures/harris/ocr_text",
        help="Pytest raw text fixture output directory.",
    )
    return parser.parse_args()


def safe_name(doc_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", doc_id.strip())


def parse_doc_ids(raw_doc_ids: str) -> list[str]:
    seen = set()
    doc_ids = []
    for value in raw_doc_ids.split(","):
        doc_id = value.strip()
        if not doc_id or doc_id in seen:
            continue
        seen.add(doc_id)
        doc_ids.append(doc_id)
    if not doc_ids:
        raise ValueError("At least one doc_id is required.")
    return doc_ids


def find_pdf(pdf_dir: Path, doc_id: str) -> Path | None:
    names = [f"{doc_id}.pdf", f"{safe_name(doc_id)}.pdf"]
    for name in names:
        candidate = pdf_dir / name
        if candidate.exists():
            return candidate

    lower_names = {name.lower() for name in names}
    if pdf_dir.exists():
        for candidate in pdf_dir.iterdir():
            if candidate.is_file() and candidate.name.lower() in lower_names:
                return candidate
    return None


def extract_text_like_current_scraper(pdf_path: Path) -> str:
    """Match harris_foreclosure_scraper_free.py's pdfplumber extraction."""
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def write_failure_marker(path: Path, doc_id: str, pdf_path: Path, error: Exception) -> None:
    marker = (
        "Text extraction failed for this sample.\n\n"
        "This is not OCR output and must not be used as a parser fixture.\n\n"
        f"doc_id: {doc_id}\n"
        f"pdf_path: {pdf_path}\n"
        f"error_type: {type(error).__name__}\n"
        f"error_message: {error}\n"
    )
    path.write_text(marker, encoding="utf-8")


def main() -> int:
    args = parse_args()
    pdf_dir = Path(args.pdf_dir)
    output_root = Path(args.output_root)
    fixtures_root = Path(args.fixtures_root)
    doc_ids = parse_doc_ids(args.doc_ids)

    output_root.mkdir(parents=True, exist_ok=True)
    fixtures_root.mkdir(parents=True, exist_ok=True)

    pdfs_found = []
    pdfs_missing = []
    text_files_created = []
    extraction_failures = []

    for doc_id in doc_ids:
        safe_doc_id = safe_name(doc_id)
        pdf_path = find_pdf(pdf_dir, doc_id)

        if not pdf_path:
            pdfs_missing.append(doc_id)
            continue

        pdfs_found.append(doc_id)
        try:
            text = extract_text_like_current_scraper(pdf_path)
        except Exception as exc:
            marker_name = f"{safe_doc_id}.ocr_failed.txt"
            write_failure_marker(output_root / marker_name, doc_id, pdf_path, exc)
            write_failure_marker(fixtures_root / marker_name, doc_id, pdf_path, exc)
            extraction_failures.append(doc_id)
            continue

        output_path = output_root / f"{safe_doc_id}.txt"
        fixture_path = fixtures_root / f"{safe_doc_id}.txt"
        output_path.write_text(text, encoding="utf-8")
        fixture_path.write_text(text, encoding="utf-8")
        text_files_created.append(doc_id)

    print(f"Selected doc IDs: {len(doc_ids)}")
    print(f"PDFs found: {len(pdfs_found)}")
    print(f"PDFs missing: {len(pdfs_missing)}")
    print(f"Text files created: {len(text_files_created)}")
    print(f"Extraction failures: {len(extraction_failures)}")

    if pdfs_missing:
        print("Missing PDFs:")
        for doc_id in pdfs_missing:
            print(f"  - {doc_id}")

    if extraction_failures:
        print("Extraction failures:")
        for doc_id in extraction_failures:
            print(f"  - {doc_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
