"""Monthly Harris County foreclosure runner."""

from __future__ import annotations

import asyncio
import calendar
from dataclasses import dataclass
from io import BytesIO

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


@dataclass(frozen=True)
class HarrisRunResult:
    county: str
    year: int
    month: int
    processed: int
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

    if retry_failed:
        targets = [record for record in all_records if record.get("doc_id") in failed_ids]
    elif resume:
        targets = [
            record for record in all_records
            if record.get("doc_id") not in completed_ids
            and record.get("doc_id") not in failed_ids
        ]
    else:
        targets = list(all_records)

    if limit is not None:
        targets = targets[:limit]
    return targets


async def run_harris_monthly(
    *,
    year: int,
    month: int,
    limit: int | None = None,
    resume: bool = False,
    retry_failed: bool = False,
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
        write_csv_rows(paths.output_csv, [], CSV_FIELDS)
        write_jsonl_rows(paths.output_jsonl, [])

    write_failed_records(paths.failed_json, checkpoint)

    processed = 0

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
                save_checkpoint(paths.checkpoint_json, checkpoint)

            targets = select_target_records(
                all_records,
                checkpoint,
                resume=resume,
                retry_failed=retry_failed,
                limit=limit,
            )

            if targets:
                record_page = await context.new_page()
                try:
                    for record in targets:
                        doc_id = record.get("doc_id", "")
                        try:
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
                            if text is None:
                                text = extract_text_func(pdf_bytes)
                                save_text(paths.text_cache_dir, doc_id, text)

                            parsed = parse_text_func(text, doc_id)
                            row = build_output_row(record, parsed)
                            append_csv_row(paths.output_csv, row, CSV_FIELDS)
                            append_jsonl_row(paths.output_jsonl, row)

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
        processed=processed,
        completed=len(get_completed_ids(checkpoint)),
        failed=len(get_failed_ids(checkpoint)),
        paths=paths,
    )
