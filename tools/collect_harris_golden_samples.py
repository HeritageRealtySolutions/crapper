#!/usr/bin/env python3
"""Organize Harris County golden samples from existing scraper outputs.

This helper does not scrape, download, OCR, parse, or call live services. It
copies and filters already-generated artifacts so the current scraper behavior
can be preserved before refactoring.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Harris County golden sample artifacts from existing scraper outputs."
    )
    parser.add_argument("--year", type=int, required=True, help="Sale year, for example 2026.")
    parser.add_argument("--month", type=int, required=True, help="Sale month number, 1-12.")
    parser.add_argument("--current-csv", required=True, help="Path to current scraper CSV output.")
    parser.add_argument("--pdf-dir", required=True, help="Path to current PDF folder.")
    parser.add_argument("--ocr-dir", help="Optional path to current OCR text folder.")
    parser.add_argument("--records-json", help="Optional path to existing search metadata JSON.")
    parser.add_argument(
        "--doc-ids",
        required=True,
        help="Comma-separated selected Harris FRCL document IDs.",
    )
    parser.add_argument(
        "--output-root",
        default="baseline/golden_samples/harris",
        help="Golden sample output root.",
    )
    parser.add_argument(
        "--fixtures-root",
        default="tests/fixtures/harris",
        help="Pytest fixture output root.",
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


def month_token(month: int) -> str:
    if not 1 <= month <= 12:
        raise ValueError("--month must be between 1 and 12.")
    return f"{month:02d}"


def ensure_dirs(output_root: Path, fixtures_root: Path) -> dict[str, Path]:
    paths = {
        "pdf_samples": output_root / "pdf_samples",
        "ocr_text_samples": output_root / "ocr_text_samples",
        "current_outputs": output_root / "current_outputs",
        "expected_outputs": output_root / "expected_outputs",
        "notes": output_root / "notes",
        "fixture_ocr_text": fixtures_root / "ocr_text",
        "fixture_expected": fixtures_root / "expected",
        "fixture_records": fixtures_root / "records",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def find_file(base_dir: Path | None, doc_id: str, extension: str) -> Path | None:
    if base_dir is None or not base_dir.exists():
        return None

    names = [f"{doc_id}{extension}", f"{safe_name(doc_id)}{extension}"]
    for name in names:
        candidate = base_dir / name
        if candidate.exists():
            return candidate

    lower_names = {name.lower() for name in names}
    for candidate in base_dir.iterdir():
        if candidate.is_file() and candidate.name.lower() in lower_names:
            return candidate
    return None


def read_csv_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Current CSV not found: {csv_path}")
    with csv_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames or []
        if "doc_id" not in fieldnames:
            raise ValueError(f"Current CSV must include a doc_id column: {csv_path}")
        return fieldnames, list(reader)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, str]]) -> int:
    rows = list(rows)
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_jsonl(path: Path, rows: Iterable[dict[str, str]]) -> int:
    rows = list(rows)
    with path.open("w", encoding="utf-8") as jsonl:
        for row in rows:
            jsonl.write(json.dumps(row, ensure_ascii=True) + "\n")
    return len(rows)


def load_records(records_path: Path | None) -> list[dict[str, str]]:
    if records_path is None:
        return []
    if not records_path.exists():
        raise FileNotFoundError(f"Records JSON not found: {records_path}")
    with records_path.open(encoding="utf-8") as records_file:
        data = json.load(records_file)
    if not isinstance(data, list):
        raise ValueError("Records JSON must contain a list of objects.")
    return data


def blank_record(doc_id: str) -> dict[str, str]:
    return {
        "doc_id": doc_id,
        "sale_date": "",
        "file_date": "",
        "pages": "",
        "href": "",
    }


def selected_records(
    doc_ids: list[str],
    loaded_records: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[str], list[str]]:
    by_doc_id = {
        str(record.get("doc_id", "")): record
        for record in loaded_records
        if isinstance(record, dict)
    }

    records = []
    completed = []
    missing = []
    for doc_id in doc_ids:
        record = by_doc_id.get(doc_id)
        if record:
            records.append(record)
            required_values = [
                record.get("sale_date"),
                record.get("file_date"),
                record.get("pages"),
                record.get("href"),
            ]
            if all(value not in (None, "") for value in required_values):
                completed.append(doc_id)
            else:
                missing.append(doc_id)
        else:
            records.append(blank_record(doc_id))
            missing.append(doc_id)
    return records, completed, missing


def write_text_if_missing(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.write_text(content, encoding="utf-8")
    return True


def placeholder_ocr_text(doc_id: str, observed_at: str) -> str:
    return (
        "OCR text still needs to be generated for this golden sample.\n\n"
        "This is not OCR output and must not be used as a parser fixture.\n\n"
        f"doc_id: {doc_id}\n"
        f"observed_at: {observed_at}\n"
        "next_step: Run the current OCR process and save the raw, unedited text.\n"
    )


def note_markdown(
    doc_id: str,
    safe_doc_id: str,
    record: dict[str, str],
    row: dict[str, str] | None,
    pdf_found: bool,
    ocr_found: bool,
    year: int,
    month: str,
) -> str:
    if row:
        parser_lines = [
            f"- grantor_borrower: {row.get('grantor_borrower', '')}",
            f"- property_address: {row.get('property_address', '')}",
            f"- original_loan_amount: {row.get('original_loan_amount', '')}",
            f"- parse_notes: {row.get('parse_notes', '')}",
        ]
    else:
        parser_lines = ["- No current CSV row found for this doc_id."]

    pdf_artifact = f"../pdf_samples/{safe_doc_id}.pdf" if pdf_found else "MISSING"
    ocr_artifact = f"../ocr_text_samples/{safe_doc_id}.txt" if ocr_found else "MISSING"

    return "\n".join(
        [
            f"# {doc_id}",
            "",
            "Categories:",
            "- TODO",
            "",
            "Reason selected:",
            "TODO",
            "",
            "Search metadata:",
            f"- sale_date: {record.get('sale_date', '')}",
            f"- file_date: {record.get('file_date', '')}",
            f"- pages: {record.get('pages', '')}",
            f"- href: {record.get('href', '')}",
            "",
            "Artifacts:",
            f"- PDF: {pdf_artifact}",
            f"- OCR text: {ocr_artifact}",
            f"- Expected CSV: ../expected_outputs/harris_{year}_{month}_expected.csv",
            f"- Expected JSONL: ../expected_outputs/harris_{year}_{month}_expected.jsonl",
            "",
            "Current parser outcome:",
            *parser_lines,
            "",
            "Manual review notes:",
            "- TODO",
            "",
            "Public release review:",
            "- Needs review before pushing to public GitHub.",
            "",
        ]
    )


def summary_markdown(
    year: int,
    month: str,
    doc_ids: list[str],
    pdf_found: list[str],
    pdf_missing: list[str],
    ocr_found: list[str],
    ocr_missing: list[str],
    expected_rows_count: int,
    records_completed: list[str],
    records_missing: list[str],
    notes_created: list[str],
    notes_existing: list[str],
) -> str:
    def bullet_list(values: list[str]) -> list[str]:
        return [f"- {value}" for value in values] if values else ["- None"]

    lines = [
        f"# Harris Golden Sample Collection Summary {year}-{month}",
        "",
        "Generated by `tools/collect_harris_golden_samples.py` from existing local outputs.",
        "No live scrape was performed by this helper.",
        "",
        "## Selected Doc IDs",
        *bullet_list(doc_ids),
        "",
        "## PDFs Found",
        *bullet_list(pdf_found),
        "",
        "## PDFs Missing",
        *bullet_list(pdf_missing),
        "",
        "## OCR Text Found",
        *bullet_list(ocr_found),
        "",
        "## OCR Text Missing",
        *bullet_list(ocr_missing),
        "",
        "## Expected Rows Created",
        f"- {expected_rows_count}",
        "",
        "## Records Metadata Completed",
        *bullet_list(records_completed),
        "",
        "## Records Metadata Missing Or Incomplete",
        *bullet_list(records_missing),
        "",
        "## Note Files Created",
        *bullet_list(notes_created),
        "",
        "## Existing Note Files Preserved",
        *bullet_list(notes_existing),
        "",
        "## Manual TODOs Remaining",
        "- Fill categories in each sample note.",
        "- Fill reason selected in each sample note.",
        "- Fill manual review notes.",
        "- Generate raw OCR text for any OCR-missing sample.",
        "- Complete blank search metadata fields.",
        "- Review public-record samples before pushing to public GitHub.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    month = month_token(args.month)
    doc_ids = parse_doc_ids(args.doc_ids)
    year = args.year
    prefix = f"harris_{year}_{month}"

    output_root = Path(args.output_root)
    fixtures_root = Path(args.fixtures_root)
    current_csv = Path(args.current_csv)
    pdf_dir = Path(args.pdf_dir)
    ocr_dir = Path(args.ocr_dir) if args.ocr_dir else None
    records_path = Path(args.records_json) if args.records_json else None

    paths = ensure_dirs(output_root, fixtures_root)
    observed_at = datetime.now().isoformat(timespec="seconds")

    fieldnames, all_rows = read_csv_rows(current_csv)
    selected_set = set(doc_ids)
    selected_rows = [row for row in all_rows if row.get("doc_id") in selected_set]
    rows_by_doc_id = {}
    for row in selected_rows:
        rows_by_doc_id.setdefault(row.get("doc_id", ""), row)

    write_csv(
        paths["current_outputs"] / f"{prefix}_current_selected.csv",
        fieldnames,
        selected_rows,
    )
    expected_count = write_csv(
        paths["expected_outputs"] / f"{prefix}_expected.csv",
        fieldnames,
        selected_rows,
    )
    write_csv(
        paths["fixture_expected"] / f"{prefix}_expected.csv",
        fieldnames,
        selected_rows,
    )
    write_jsonl(paths["expected_outputs"] / f"{prefix}_expected.jsonl", selected_rows)
    write_jsonl(paths["fixture_expected"] / f"{prefix}_expected.jsonl", selected_rows)

    loaded_records = load_records(records_path)
    records, records_completed, records_missing = selected_records(doc_ids, loaded_records)
    records_output = paths["fixture_records"] / f"{prefix}_sample_records.json"
    records_output.write_text(json.dumps(records, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    records_by_doc_id = {record["doc_id"]: record for record in records}

    pdf_found = []
    pdf_missing = []
    ocr_found = []
    ocr_missing = []
    notes_created = []
    notes_existing = []

    for doc_id in doc_ids:
        safe_doc_id = safe_name(doc_id)

        source_pdf = find_file(pdf_dir, doc_id, ".pdf")
        target_pdf = paths["pdf_samples"] / f"{safe_doc_id}.pdf"
        if source_pdf:
            shutil.copy2(source_pdf, target_pdf)
            pdf_found.append(doc_id)
            found_pdf = True
        else:
            pdf_missing.append(doc_id)
            found_pdf = False

        source_ocr = find_file(ocr_dir, doc_id, ".txt")
        target_ocr = paths["ocr_text_samples"] / f"{safe_doc_id}.txt"
        fixture_ocr = paths["fixture_ocr_text"] / f"{safe_doc_id}.txt"
        if source_ocr:
            shutil.copy2(source_ocr, target_ocr)
            shutil.copy2(source_ocr, fixture_ocr)
            ocr_found.append(doc_id)
            found_ocr = True
        else:
            placeholder = placeholder_ocr_text(doc_id, observed_at)
            write_text_if_missing(paths["ocr_text_samples"] / f"{safe_doc_id}.ocr_text_needed.txt", placeholder)
            write_text_if_missing(paths["fixture_ocr_text"] / f"{safe_doc_id}.ocr_text_needed.txt", placeholder)
            ocr_missing.append(doc_id)
            found_ocr = False

        note_path = paths["notes"] / f"{safe_doc_id}.md"
        note = note_markdown(
            doc_id=doc_id,
            safe_doc_id=safe_doc_id,
            record=records_by_doc_id.get(doc_id, blank_record(doc_id)),
            row=rows_by_doc_id.get(doc_id),
            pdf_found=found_pdf,
            ocr_found=found_ocr,
            year=year,
            month=month,
        )
        if write_text_if_missing(note_path, note):
            notes_created.append(doc_id)
        else:
            notes_existing.append(doc_id)

    summary_path = paths["notes"] / f"collection_summary_{year}_{month}.md"
    summary_path.write_text(
        summary_markdown(
            year=year,
            month=month,
            doc_ids=doc_ids,
            pdf_found=pdf_found,
            pdf_missing=pdf_missing,
            ocr_found=ocr_found,
            ocr_missing=ocr_missing,
            expected_rows_count=expected_count,
            records_completed=records_completed,
            records_missing=records_missing,
            notes_created=notes_created,
            notes_existing=notes_existing,
        ),
        encoding="utf-8",
    )

    print(f"Selected doc IDs: {len(doc_ids)}")
    print(f"Expected rows created: {expected_count}")
    print(f"PDFs found/missing: {len(pdf_found)}/{len(pdf_missing)}")
    print(f"OCR text found/missing: {len(ocr_found)}/{len(ocr_missing)}")
    print(f"Records metadata completed/missing: {len(records_completed)}/{len(records_missing)}")
    print(f"Summary report: {summary_path}")
    if len(selected_rows) != len(doc_ids):
        missing_rows = [doc_id for doc_id in doc_ids if doc_id not in rows_by_doc_id]
        if missing_rows:
            print("Warning: no current CSV row found for: " + ", ".join(missing_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
