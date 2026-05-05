#!/usr/bin/env python3
"""
Harris County Foreclosure Scraper — FREE VERSION (No API Key)
=============================================================
Scrapes all 741 foreclosure records from Harris County Clerk portal.
Downloads each PDF directly, extracts text with pdfplumber, parses
with regex. Zero AI API calls. Zero cost.

Setup:
    pip install -r requirements_free.txt
    playwright install chromium

Run:
    python harris_foreclosure_scraper_free.py

Resume after interruption:
    python harris_foreclosure_scraper_free.py        (auto-resumes)

Retry only failed:
    python harris_foreclosure_scraper_free.py --retry-failed
"""

import asyncio
import csv
import json
import os
import sys
import argparse
import time
from pathlib import Path

import pdfplumber
from playwright.async_api import async_playwright
from scraper.counties.harris.client import (
    collect_all_doc_ids,
    download_pdf,
    log_download_diagnostics,
    read_download_bytes,
    resolve_document_url,
    sanitize_diagnostic_text,
    save_download_diagnostic_snapshot,
)
from scraper.counties.harris.parser import (
    CSV_FIELDS,
    clean,
    extract_field,
    parse_foreclosure_text,
)
from scraper.counties.harris.settings import (
    BASE_URL,
    CHECKPOINT_FILE,
    DELAY_BETWEEN_RECORDS,
    DOCUMENT_URL_LOG_LIMIT,
    DOWNLOAD_DIAGNOSTIC_SNAPSHOT_LIMIT,
    DOWNLOAD_DIAGNOSTICS,
    DOWNLOAD_DIAGNOSTICS_DIR,
    HEADLESS,
    LOG_FILE,
    MAX_RETRIES,
    OUTPUT_CSV,
    PDFS_DIR,
    SALE_MONTH,
    SALE_YEAR,
    SITE_BASE_URL,
    WEBSEARCH_BASE_URL,
)

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

# ── LOGGING ───────────────────────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ── CHECKPOINT ────────────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"completed_ids": [], "failed_ids": [], "all_records": []}

def save_checkpoint(cp: dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)

# ── PHASE 3 — PARSE PDF WITH PDFPLUMBER + REGEX ───────────────────────────────

def parse_foreclosure_pdf(pdf_bytes: bytes, doc_id: str) -> dict:
    """
    Extract all structured fields from a Harris County foreclosure notice PDF.
    Returns a dict of field values.
    """
    try:
        with pdfplumber.open(pdf_bytes if hasattr(pdf_bytes, "read") else
                             __import__("io").BytesIO(pdf_bytes)) as pdf:
            # Concatenate all pages
            full_text = "\n".join(
                (page.extract_text() or "") for page in pdf.pages
            )
    except Exception as e:
        return {"parse_notes": f"PDF read error: {e}"}

    return parse_foreclosure_text(full_text, doc_id)


# ── MAIN ORCHESTRATOR ─────────────────────────────────────────────────────────

async def run(retry_failed: bool = False):
    PDFS_DIR.mkdir(exist_ok=True)

    checkpoint    = load_checkpoint()
    completed_ids = set(checkpoint["completed_ids"])
    failed_ids    = list(checkpoint["failed_ids"])
    cached_recs   = checkpoint.get("all_records", [])

    log("Harris County Foreclosure Scraper (FREE) — pdfplumber edition")
    log(f"Checkpoint: {len(completed_ids)} completed, {len(failed_ids)} failed")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )

        # Phase 1: Collect Doc IDs
        if cached_recs and not retry_failed:
            all_records = cached_recs
            log(f"Using cached list ({len(all_records)} records)")
        else:
            p1_page = await context.new_page()
            all_records = await collect_all_doc_ids(p1_page)
            await p1_page.close()
            checkpoint["all_records"] = all_records
            save_checkpoint(checkpoint)

        # Filter to unprocessed records
        if retry_failed:
            targets = [r for r in all_records if r["doc_id"] in set(failed_ids)]
            log(f"Retry mode — {len(targets)} failed records to retry")
            completed_ids = completed_ids - set(failed_ids)
            checkpoint["failed_ids"] = []
        else:
            targets = [r for r in all_records if r["doc_id"] not in completed_ids]
            log(f"Phase 2 — {len(targets)} records remaining")

        csv_mode = "a" if (completed_ids and not retry_failed) else "w"
        with open(OUTPUT_CSV, csv_mode, newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
            if csv_mode == "w":
                writer.writeheader()

            rec_page = await context.new_page()
            total    = len(targets)

            for idx, record in enumerate(targets, start=1):
                doc_id = record["doc_id"]
                log(f"[{idx}/{total}] {doc_id}")

                success = False

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        pdf_bytes = await download_pdf(rec_page, record, context)

                        if pdf_bytes:
                            # Save PDF locally
                            pdf_path = PDFS_DIR / f"{doc_id}.pdf"
                            pdf_path.write_bytes(pdf_bytes)

                            parsed = parse_foreclosure_pdf(pdf_bytes, doc_id)
                        else:
                            parsed = {"parse_notes": "PDF download failed"}

                        row = {
                            "doc_id":               doc_id,
                            "sale_date_from_table": record["sale_date"],
                            "file_date":            record["file_date"],
                            "pages":                record["pages"],
                            "grantor_borrower":     parsed.get("grantor_borrower",     "N/A"),
                            "property_address":     parsed.get("property_address",     "N/A"),
                            "deed_of_trust_date":   parsed.get("deed_of_trust_date",   "N/A"),
                            "original_loan_amount": parsed.get("original_loan_amount", "N/A"),
                            "original_mortgagee":   parsed.get("original_mortgagee",   "N/A"),
                            "current_mortgagee":    parsed.get("current_mortgagee",    "N/A"),
                            "servicer_name":        parsed.get("servicer_name",        "N/A"),
                            "servicer_address":     parsed.get("servicer_address",     "N/A"),
                            "sale_date_from_doc":   parsed.get("sale_date_from_doc",   "N/A"),
                            "earliest_sale_time":   parsed.get("earliest_sale_time",   "N/A"),
                            "legal_description":    parsed.get("legal_description",    "N/A"),
                            "recording_doc_number": parsed.get("recording_doc_number", "N/A"),
                            "raw_text_snippet":     parsed.get("raw_text_snippet",     ""),
                            "parse_notes":          parsed.get("parse_notes",          ""),
                        }
                        writer.writerow(row)
                        csvfile.flush()

                        checkpoint["completed_ids"].append(doc_id)
                        save_checkpoint(checkpoint)

                        status = parsed.get("parse_notes", "")
                        log(f"  ✓ {row['grantor_borrower']} | {row['original_loan_amount']} | {status}")
                        success = True
                        break

                    except Exception as e:
                        log(f"  Attempt {attempt} failed: {e}", "WARN")
                        await asyncio.sleep(4 * attempt)

                if not success:
                    if doc_id not in checkpoint["failed_ids"]:
                        checkpoint["failed_ids"].append(doc_id)
                    save_checkpoint(checkpoint)
                    log(f"  ✗ Failed after {MAX_RETRIES} attempts", "WARN")

                await asyncio.sleep(DELAY_BETWEEN_RECORDS)

            await rec_page.close()
        await browser.close()

    log("=" * 55)
    log("COMPLETE")
    log(f"  Processed : {len(checkpoint['completed_ids'])}")
    log(f"  Failed    : {len(checkpoint['failed_ids'])}")
    log(f"  CSV       : {OUTPUT_CSV}")
    log(f"  PDFs      : {PDFS_DIR}/")
    if checkpoint["failed_ids"]:
        log("  Re-run with --retry-failed to attempt failures again")
    log("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Harris County Foreclosure Scraper (Free)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Only re-process previously failed records")
    args = parser.parse_args()
    asyncio.run(run(retry_failed=args.retry_failed))
