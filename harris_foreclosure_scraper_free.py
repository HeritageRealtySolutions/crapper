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
import re
import sys
import argparse
import time
from pathlib import Path

import pdfplumber
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

BASE_URL        = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
SALE_YEAR       = "2026"
SALE_MONTH      = "May"

OUTPUT_CSV      = "harris_foreclosures_may2026.csv"
CHECKPOINT_FILE = "scrape_checkpoint.json"
PDFS_DIR        = Path("foreclosure_pdfs")
LOG_FILE        = "scraper.log"

DELAY_BETWEEN_RECORDS = 2.0   # seconds — respectful rate limiting
MAX_RETRIES           = 3
HEADLESS              = True   # set False to watch the browser

# ── CSV COLUMNS ───────────────────────────────────────────────────────────────

CSV_FIELDS = [
    "doc_id",
    "sale_date_from_table",
    "file_date",
    "pages",
    "grantor_borrower",
    "property_address",
    "deed_of_trust_date",
    "original_loan_amount",
    "original_mortgagee",
    "current_mortgagee",
    "servicer_name",
    "servicer_address",
    "sale_date_from_doc",
    "earliest_sale_time",
    "legal_description",
    "recording_doc_number",
    "raw_text_snippet",
    "parse_notes",
]

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

# ── PHASE 1 — COLLECT ALL DOC IDs ─────────────────────────────────────────────

async def collect_all_doc_ids(page) -> list[dict]:
    log("Phase 1 — Collecting all Doc IDs from summary table...")

    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(2000)

    # Select "Sale Date" radio button
    try:
        radios = await page.query_selector_all('input[type="radio"]')
        for r in radios:
            val = await r.get_attribute("value") or ""
            label_id = await r.get_attribute("id") or ""
            if "sale" in val.lower() or "sale" in label_id.lower():
                await r.check()
                break
        else:
            if radios:
                await radios[0].check()
    except Exception:
        pass

    # Set Year
    selects = await page.query_selector_all("select")
    if not selects:
        raise RuntimeError("No dropdowns found — page may not have loaded correctly.")
    await selects[0].select_option(SALE_YEAR)
    await page.wait_for_timeout(400)

    # Set Month
    if len(selects) >= 2:
        await selects[1].select_option(label=SALE_MONTH)
    await page.wait_for_timeout(400)

    # Click Search
    search_btn = await page.query_selector(
        'input[value="SEARCH"], input[value="Search"], button:has-text("SEARCH")'
    )
    if not search_btn:
        raise RuntimeError("Search button not found.")
    await search_btn.click()
    await page.wait_for_timeout(3000)

    all_records = []
    current_page = 1

    while True:
        log(f"  Summary page {current_page}...")

        rows = await page.query_selector_all("table tr")
        for row in rows:
            link = await row.query_selector("a")
            if not link:
                continue
            doc_id = (await link.inner_text()).strip()
            if not doc_id.startswith("FRCL-"):
                continue

            cells   = await row.query_selector_all("td")
            sale_dt = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""
            file_dt = (await cells[2].inner_text()).strip() if len(cells) > 2 else ""
            pages   = (await cells[3].inner_text()).strip() if len(cells) > 3 else ""
            href    = await link.get_attribute("href") or ""
            onclick = await link.get_attribute("onclick") or ""

            all_records.append({
                "doc_id":    doc_id,
                "sale_date": sale_dt,
                "file_date": file_dt,
                "pages":     pages,
                "href":      href,
                "onclick":   onclick,
            })

        # Next page
        next_num  = current_page + 1
        next_link = await page.query_selector(f'a:has-text("{next_num}")')
        if next_link and current_page < 10:
            await next_link.click()
            await page.wait_for_timeout(2500)
            current_page += 1
        else:
            break

    log(f"Phase 1 complete — {len(all_records)} Doc IDs collected.")
    return all_records


# ── PHASE 2 — DOWNLOAD PDF FOR EACH RECORD ────────────────────────────────────

async def download_pdf(page, record: dict, context) -> bytes | None:
    """
    Navigate to a foreclosure record and capture the PDF bytes.
    The site renders the document via ViewECdocs.aspx — we intercept
    the PDF response from the network.
    """
    doc_id  = record["doc_id"]
    href    = record.get("href", "")
    pdf_bytes = None

    async def handle_response(response):
        nonlocal pdf_bytes
        ct = response.headers.get("content-type", "")
        if "pdf" in ct or "application/octet" in ct:
            try:
                pdf_bytes = await response.body()
            except Exception:
                pass

    page.on("response", handle_response)

    try:
        # Strategy A: navigate directly if href is a real URL
        if href and not href.startswith("javascript"):
            target = href if href.startswith("http") else \
                     f"https://www.cclerk.hctx.net{href}"
            await page.goto(target, wait_until="domcontentloaded", timeout=20000)

        else:
            # Strategy B: use Doc ID input to pull up the record directly
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(1500)

            # Try filling the Document ID input
            doc_input = await page.query_selector(
                'input[id*="DocID"], input[id*="doc"], input[placeholder*="FRCL"]'
            )
            if doc_input:
                await doc_input.fill(doc_id)
                search_btn = await page.query_selector(
                    'input[value="SEARCH"], input[value="Search"], button:has-text("SEARCH")'
                )
                if search_btn:
                    await search_btn.click()
                    await page.wait_for_timeout(2000)

            # Click the doc link
            link = await page.query_selector(f'a:has-text("{doc_id}")')
            if link:
                await link.click()

        await page.wait_for_timeout(3000)

        # If response handler didn't capture PDF, try fetching the current URL
        if not pdf_bytes:
            current_url = page.url
            if current_url and current_url != BASE_URL:
                resp = await page.request.get(current_url)
                ct   = resp.headers.get("content-type", "")
                if "pdf" in ct or "octet" in ct:
                    pdf_bytes = await resp.body()

        # Last resort: look for an embedded PDF iframe/embed src
        if not pdf_bytes:
            for sel in ['embed[src]', 'iframe[src]', 'object[data]']:
                el = await page.query_selector(sel)
                if el:
                    src = await el.get_attribute("src") or \
                          await el.get_attribute("data") or ""
                    if src:
                        pdf_url = src if src.startswith("http") else \
                                  f"https://www.cclerk.hctx.net{src}"
                        resp = await page.request.get(pdf_url)
                        pdf_bytes = await resp.body()
                        break

    except PlaywrightTimeout:
        log(f"  Timeout on {doc_id}", "WARN")
    except Exception as e:
        log(f"  Download error for {doc_id}: {e}", "WARN")
    finally:
        page.remove_listener("response", handle_response)

    return pdf_bytes if pdf_bytes and len(pdf_bytes) > 500 else None


# ── PHASE 3 — PARSE PDF WITH PDFPLUMBER + REGEX ───────────────────────────────

def clean(s: str | None) -> str:
    """Collapse whitespace and strip a string."""
    if not s:
        return "N/A"
    return re.sub(r"\s+", " ", s).strip()

def extract_field(pattern: str, text: str, flags=re.IGNORECASE | re.DOTALL) -> str | None:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def parse_foreclosure_pdf(pdf_bytes: bytes, doc_id: str) -> dict:
    """
    Extract all structured fields from a Harris County foreclosure notice PDF.
    Returns a dict of field values.
    """
    notes = []
    result = {}

    try:
        with pdfplumber.open(pdf_bytes if hasattr(pdf_bytes, "read") else
                             __import__("io").BytesIO(pdf_bytes)) as pdf:
            # Concatenate all pages
            full_text = "\n".join(
                (page.extract_text() or "") for page in pdf.pages
            )
    except Exception as e:
        return {"parse_notes": f"PDF read error: {e}"}

    if not full_text.strip():
        return {"parse_notes": "PDF extracted but no text found — may be image-based"}

    # ── Store a short snippet for debugging ──
    result["raw_text_snippet"] = full_text[:300].replace("\n", " ")

    # ── Grantor / Borrower ──
    grantor = extract_field(r"Grantor\(?s?\)?\s*:\s*(.+?)(?=\n|Original Mortgagee)", full_text)
    if not grantor:
        grantor = extract_field(r"Mortgagor\(?s?\)?\s*:\s*(.+?)(?=\n)", full_text)
    result["grantor_borrower"] = clean(grantor)

    # ── Deed of Trust Date ──
    dot_date = extract_field(r"Deed of Trust Dated?\s*:\s*(.+?)(?=\n|Amount)", full_text)
    result["deed_of_trust_date"] = clean(dot_date)

    # ── Extract year from deed of trust date ──
    if dot_date:
        yr = re.search(r"\b(19|20)\d{2}\b", dot_date)
        result["deed_of_trust_year"] = yr.group(0) if yr else "N/A"
    else:
        result["deed_of_trust_year"] = "N/A"

    # ── Original Loan Amount ──
    amount = extract_field(r"Amount\s*:\s*\$?([\d,\.]+)", full_text)
    if not amount:
        amount = extract_field(r"original\s+(?:note|loan|principal)\s+(?:amount|balance)\s*(?:of|:)?\s*\$?([\d,\.]+)", full_text)
    if amount:
        result["original_loan_amount"] = amount.replace(",", "")
    else:
        result["original_loan_amount"] = "N/A"
        notes.append("amount not found")

    # ── Original Mortgagee (lender) ──
    orig_mort = extract_field(r"Original Mortgagee\s*:\s*(.+?)(?=\n|Current Mortgagee)", full_text)
    result["original_mortgagee"] = clean(orig_mort)

    # ── Current Mortgagee ──
    curr_mort = extract_field(r"Current Mortgagee\s*:\s*(.+?)(?=\n|Mortgagee Servicer)", full_text)
    result["current_mortgagee"] = clean(curr_mort)

    # ── Servicer name and address ──
    servicer_block = extract_field(
        r"Mortgagee Servicer and Address\s*:\s*(.+?)(?=\nPursuant|\nRecording|\nLegal|\n\n)",
        full_text
    )
    if servicer_block:
        # Split "c/o NAME, ADDRESS" or "NAME\nADDRESS"
        sb = clean(servicer_block)
        if sb.startswith("c/o "):
            sb = sb[4:]
        # First comma separates name from address in most cases
        parts = sb.split(",", 1)
        result["servicer_name"]    = clean(parts[0])
        result["servicer_address"] = clean(parts[1]) if len(parts) > 1 else "N/A"
    else:
        result["servicer_name"]    = "N/A"
        result["servicer_address"] = "N/A"

    # ── Property Address ──
    # Not always an explicit field — try common patterns first
    prop_addr = extract_field(
        r"(?:Property Address|Subject Property|Property Located at)\s*:\s*(.+?)(?=\n)",
        full_text
    )
    if not prop_addr:
        # Try to find address in servicer block or recording info
        # Fall back to "N/A" — legal description is the authoritative identifier
        prop_addr = extract_field(
            r"(?:located at|known as)\s+(\d+\s+[\w\s]+(?:Street|St|Avenue|Ave|Drive|Dr|"
            r"Lane|Ln|Road|Rd|Blvd|Boulevard|Court|Ct|Way|Circle|Cir)[\w\s,\.]*?)(?=\n|,\s*Harris)",
            full_text, re.IGNORECASE
        )
    result["property_address"] = clean(prop_addr) if prop_addr else "See Legal Description"

    # ── Legal Description ──
    legal = extract_field(
        r"Legal Description\s*:\s*(.+?)(?=\nWhereas|\nDate of Sale|\nEarliest|\n\n)",
        full_text
    )
    result["legal_description"] = clean(legal)

    # ── Date of Sale (from document body) ──
    sale_date = extract_field(
        r"Date of Sale\s*:\s*(.+?)(?=\n|Earliest)",
        full_text
    )
    result["sale_date_from_doc"] = clean(sale_date)

    # ── Earliest Sale Time ──
    earliest = extract_field(
        r"Earliest Time Sale Will Begin\s*:\s*(.+?)(?=\n|Place of Sale)",
        full_text
    )
    result["earliest_sale_time"] = clean(earliest)

    # ── Recording Document Number ──
    rec_num = extract_field(
        r"Recording Information\s*:.*?Document No\.?\s*([\d\-RP]+)",
        full_text
    )
    result["recording_doc_number"] = clean(rec_num)

    # ── Substitute Trustee (bonus field) ──
    trustee = extract_field(
        r"(?:Substitute Trustee|appointed as Substitute Trustee)\s*[:\(]?\s*(.+?)(?=\n|each acting)",
        full_text
    )
    result["substitute_trustee"] = clean(trustee)

    result["parse_notes"] = "; ".join(notes) if notes else "OK"
    return result


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
