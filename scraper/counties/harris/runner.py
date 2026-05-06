"""Monthly Harris County foreclosure runner."""

from __future__ import annotations

import asyncio
import calendar
import csv
import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pdfplumber
from playwright.async_api import async_playwright

from scraper.core.checkpoints import (
    atomic_write_json,
    clear_failed,
    get_completed_ids,
    get_failed_ids,
    load_checkpoint,
    mark_completed,
    mark_failed,
    new_checkpoint,
    save_checkpoint,
)
from scraper.core.outputs import MonthlyPaths, build_monthly_paths
from scraper.core.pdfs import load_pdf_bytes, load_text, save_pdf_bytes, save_text
from scraper.core.writers import append_csv_row, append_jsonl_row, write_csv_rows, write_jsonl_rows
from scraper.counties.harris.client import collect_all_doc_ids, download_pdf
from scraper.counties.harris.parser import CSV_FIELDS, parse_foreclosure_text
from scraper.counties.harris.settings import DELAY_BETWEEN_RECORDS, HEADLESS

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
MIN_USABLE_TEXT_CHARS = 50
UNUSABLE_TEXT_REASON = "PDF text extraction produced no usable text"


@dataclass(frozen=True)
class HarrisRunResult:
    county: str
    year: int
    month: int
    dry_run: bool
    processed: int
    skipped: int
    planned: int
    planned_doc_ids: list[str]
    completed: int
    failed: int
    paths: MonthlyPaths


def month_label(month: int) -> str:
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")
    return calendar.month_name[month]


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def is_usable_extracted_text(text: str | None, min_chars: int = MIN_USABLE_TEXT_CHARS) -> bool:
    return bool(text and len(text.strip()) >= min_chars)


def build_output_row(record: dict, parsed: dict) -> dict:
    row = {
        "doc_id": record.get("doc_id", ""),
        "sale_date_from_table": record.get("sale_date", ""),
        "file_date": record.get("file_date", ""),
        "pages": record.get("pages", ""),
    }

    for field in CSV_FIELDS:
        if field not in row:
            row[field] = parsed.get(field, "N/A")

    row["raw_text_snippet"] = parsed.get("raw_text_snippet", "")
    row["parse_notes"] = parsed.get("parse_notes", "")
    return row


def write_failed_records(path, checkpoint: dict) -> None:
    atomic_write_json(
        path,
        {
            "failed_ids": checkpoint.get("failed_ids", []),
            "failed_reasons": checkpoint.get("failed_reasons", {}),
        },
    )


def read_csv_doc_ids(path) -> set[str]:
    target = Path(path)
    if not target.exists():
        return set()

    with target.open(newline="", encoding="utf-8") as csvfile:
        return {
            row["doc_id"]
            for row in csv.DictReader(csvfile)
            if row.get("doc_id")
        }


def read_jsonl_doc_ids(path) -> set[str]:
    target = Path(path)
    if not target.exists():
        return set()

    doc_ids = set()
    with target.open(encoding="utf-8") as jsonl_file:
        for line in jsonl_file:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if row.get("doc_id"):
                doc_ids.add(row["doc_id"])
    return doc_ids


def append_output_row_once(paths: MonthlyPaths, row: dict, csv_doc_ids: set[str], jsonl_doc_ids: set[str]) -> None:
    doc_id = row.get("doc_id", "")
    if doc_id not in csv_doc_ids:
        append_csv_row(paths.output_csv, row, CSV_FIELDS)
        csv_doc_ids.add(doc_id)
    if doc_id not in jsonl_doc_ids:
        append_jsonl_row(paths.output_jsonl, row)
        jsonl_doc_ids.add(doc_id)


def select_target_records(
    all_records: list[dict],
    checkpoint: dict,
    *,
    resume: bool,
    retry_failed: bool,
    limit: int | None,
) -> list[dict]:
    completed_ids = get_completed_ids(checkpoint)
    failed_ids = get_failed_ids(checkpoint)
    considered_records = all_records[:limit] if limit is not None else all_records

    if retry_failed:
        targets = [record for record in considered_records if record.get("doc_id") in failed_ids]
    elif resume:
        targets = [
            record for record in considered_records
            if record.get("doc_id") not in completed_ids
            and record.get("doc_id") not in failed_ids
        ]
    else:
        targets = list(considered_records)

    return targets


def count_checkpoint_skips(
    all_records: list[dict],
    checkpoint: dict,
    *,
    resume: bool,
    retry_failed: bool,
    limit: int | None,
) -> int:
    completed_ids = get_completed_ids(checkpoint)
    failed_ids = get_failed_ids(checkpoint)
    considered_records = all_records[:limit] if limit is not None else all_records

    if retry_failed:
        return sum(1 for record in considered_records if record.get("doc_id") not in failed_ids)
    if resume:
        return sum(
            1 for record in considered_records
            if record.get("doc_id") in completed_ids or record.get("doc_id") in failed_ids
        )
    return 0


async def run_harris_monthly(
    *,
    year: int,
    month: int,
    limit: int | None = None,
    resume: bool = False,
    retry_failed: bool = False,
    dry_run: bool = False,
    paths: MonthlyPaths | None = None,
    playwright_factory=async_playwright,
    collect_records_func=collect_all_doc_ids,
    download_pdf_func=download_pdf,
    extract_text_func=extract_text_from_pdf_bytes,
    parse_text_func=parse_foreclosure_text,
    delay_between_records: float = DELAY_BETWEEN_RECORDS,
) -> HarrisRunResult:
    paths = paths or build_monthly_paths("harris", year, month)
    sale_month = month_label(month)
    sale_year = str(year)

    if resume or retry_failed:
        checkpoint = load_checkpoint(paths.checkpoint_json)
    else:
        checkpoint = new_checkpoint()
        if not dry_run:
            write_csv_rows(paths.output_csv, [], CSV_FIELDS)
            write_jsonl_rows(paths.output_jsonl, [])

    if not dry_run:
        write_failed_records(paths.failed_json, checkpoint)
    csv_doc_ids = read_csv_doc_ids(paths.output_csv)
    jsonl_doc_ids = read_jsonl_doc_ids(paths.output_jsonl)

    processed = 0
    skipped = 0
    planned_doc_ids = []

    async with playwright_factory() as playwright:
        browser = await playwright.chromium.launch(headless=HEADLESS)
        try:
            context = await browser.new_context(user_agent=USER_AGENT)

            if checkpoint.get("all_records") and (resume or retry_failed):
                all_records = checkpoint["all_records"]
            else:
                page = await context.new_page()
                try:
                    all_records = await collect_records_func(
                        page,
                        sale_year=sale_year,
                        sale_month=sale_month,
                    )
                finally:
                    await page.close()

                checkpoint["all_records"] = all_records
                if not dry_run:
                    save_checkpoint(paths.checkpoint_json, checkpoint)

            targets = select_target_records(
                all_records,
                checkpoint,
                resume=resume,
                retry_failed=retry_failed,
                limit=limit,
            )
            planned_doc_ids = [record.get("doc_id", "") for record in targets]
            skipped += count_checkpoint_skips(
                all_records,
                checkpoint,
                resume=resume,
                retry_failed=retry_failed,
                limit=limit,
            )

            if dry_run:
                return HarrisRunResult(
                    county="harris",
                    year=year,
                    month=month,
                    dry_run=True,
                    processed=0,
                    skipped=skipped,
                    planned=len(planned_doc_ids),
                    planned_doc_ids=planned_doc_ids,
                    completed=len(get_completed_ids(checkpoint)),
                    failed=len(get_failed_ids(checkpoint)),
                    paths=paths,
                )

            if targets:
                record_page = await context.new_page()
                try:
                    for record in targets:
                        doc_id = record.get("doc_id", "")
                        try:
                            if (
                                not retry_failed
                                and doc_id in csv_doc_ids
                                and doc_id in jsonl_doc_ids
                            ):
                                mark_completed(checkpoint, doc_id)
                                save_checkpoint(paths.checkpoint_json, checkpoint)
                                write_failed_records(paths.failed_json, checkpoint)
                                skipped += 1
                                continue

                            pdf_bytes = load_pdf_bytes(paths.pdfs_dir, doc_id)
                            if pdf_bytes is None:
                                pdf_bytes = await download_pdf_func(record_page, record, context)
                                if pdf_bytes:
                                    save_pdf_bytes(paths.pdfs_dir, doc_id, pdf_bytes)

                            if not pdf_bytes:
                                mark_failed(checkpoint, doc_id, "PDF download failed")
                                save_checkpoint(paths.checkpoint_json, checkpoint)
                                write_failed_records(paths.failed_json, checkpoint)
                                continue

                            text = load_text(paths.text_cache_dir, doc_id)
                            if not is_usable_extracted_text(text):
                                text = extract_text_func(pdf_bytes)
                                if not is_usable_extracted_text(text):
                                    mark_failed(checkpoint, doc_id, UNUSABLE_TEXT_REASON)
                                    save_checkpoint(paths.checkpoint_json, checkpoint)
                                    write_failed_records(paths.failed_json, checkpoint)
                                    continue
                                save_text(paths.text_cache_dir, doc_id, text)

                            parsed = parse_text_func(text, doc_id)
                            row = build_output_row(record, parsed)
                            append_output_row_once(paths, row, csv_doc_ids, jsonl_doc_ids)

                            mark_completed(checkpoint, doc_id)
                            clear_failed(checkpoint, doc_id)
                            save_checkpoint(paths.checkpoint_json, checkpoint)
                            write_failed_records(paths.failed_json, checkpoint)
                            processed += 1
                        except Exception as e:
                            mark_failed(checkpoint, doc_id, str(e))
                            save_checkpoint(paths.checkpoint_json, checkpoint)
                            write_failed_records(paths.failed_json, checkpoint)

                        if delay_between_records:
                            await asyncio.sleep(delay_between_records)
                finally:
                    await record_page.close()
        finally:
            await browser.close()

    return HarrisRunResult(
        county="harris",
        year=year,
        month=month,
        dry_run=False,
        processed=processed,
        skipped=skipped,
        planned=len(planned_doc_ids),
        planned_doc_ids=planned_doc_ids,
        completed=len(get_completed_ids(checkpoint)),
        failed=len(get_failed_ids(checkpoint)),
        paths=paths,
    )
