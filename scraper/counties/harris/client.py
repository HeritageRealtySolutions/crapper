"""Harris County browser/search/download client helpers."""

from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import (
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeout,
)

from scraper.counties.harris.settings import (
    BASE_URL,
    DOCUMENT_URL_LOG_LIMIT,
    DOWNLOAD_DIAGNOSTIC_SNAPSHOT_LIMIT,
    DOWNLOAD_DIAGNOSTICS,
    DOWNLOAD_DIAGNOSTICS_DIR,
    LOG_FILE,
    SALE_MONTH,
    SALE_YEAR,
    WEBSEARCH_BASE_URL,
)

_document_url_log_count = 0
_download_diagnostic_snapshot_count = 0


def log(msg: str, level: str = "INFO"):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


async def collect_all_doc_ids(
    page,
    sale_year: str = SALE_YEAR,
    sale_month: str = SALE_MONTH,
) -> list[dict]:
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

    # Set Year. Use locators and re-query after selection because the form can
    # re-render the month dropdown when the year changes.
    await page.wait_for_selector("select", state="attached", timeout=10000)
    selects = page.locator("select")
    if await selects.count() < 1:
        raise RuntimeError("No dropdowns found — page may not have loaded correctly.")

    log(f"Selecting sale year: {sale_year}")
    await selects.nth(0).select_option(sale_year)
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=5000)
    except PlaywrightTimeout:
        pass
    await page.wait_for_selector("select", state="attached", timeout=10000)
    await page.wait_for_timeout(400)

    # Set Month using a fresh locator after the year selection updates the DOM.
    selects = page.locator("select")
    if await selects.count() < 2:
        raise RuntimeError("Month dropdown not found after selecting year.")

    log(f"Selecting sale month: {sale_month}")
    await selects.nth(1).select_option(label=sale_month)
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=5000)
    except PlaywrightTimeout:
        pass
    await page.wait_for_selector("select", state="attached", timeout=10000)
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

            cells = await row.query_selector_all("td")
            sale_dt = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""
            file_dt = (await cells[2].inner_text()).strip() if len(cells) > 2 else ""
            pages = (await cells[3].inner_text()).strip() if len(cells) > 3 else ""
            href = await link.get_attribute("href") or ""
            onclick = await link.get_attribute("onclick") or ""

            all_records.append({
                "doc_id": doc_id,
                "sale_date": sale_dt,
                "file_date": file_dt,
                "pages": pages,
                "href": href,
                "onclick": onclick,
            })

        # Next page
        next_num = current_page + 1
        next_link = await page.query_selector(f'a:has-text("{next_num}")')
        if next_link and current_page < 10:
            await next_link.click()
            await page.wait_for_timeout(2500)
            current_page += 1
        else:
            break

    log(f"Phase 1 complete — {len(all_records)} Doc IDs collected.")
    return all_records


def resolve_document_url(href: str) -> str:
    """Resolve Harris document links without malformed domain/path joins."""
    href = (href or "").strip()
    if not href:
        return ""

    href = re.sub(
        r"^https://www\.cclerk\.hctx\.net(?=[A-Za-z0-9])",
        "",
        href,
        flags=re.IGNORECASE,
    )
    if href.lower().startswith("/viewecdocs.aspx"):
        href = href.lstrip("/")

    return urljoin(WEBSEARCH_BASE_URL, href)


def sanitize_diagnostic_text(value: str | None, limit: int = 500) -> str:
    if not value:
        return ""
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]+", " ", value)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


async def log_download_diagnostics(page, doc_id: str, nav_response=None):
    """Log safe page-level diagnostics for Harris document download failures."""
    if not DOWNLOAD_DIAGNOSTICS:
        return

    status = nav_response.status if nav_response else "N/A"
    content_type = ""
    if nav_response:
        content_type = nav_response.headers.get("content-type", "")

    try:
        title = await page.title()
    except Exception as e:
        title = f"unavailable: {e}"

    log(f"  Diagnostic final URL for {doc_id}: {page.url}")
    log(f"  Diagnostic response status for {doc_id}: {status}")
    log(f"  Diagnostic content-type for {doc_id}: {content_type or 'N/A'}")
    log(f"  Diagnostic page title for {doc_id}: {sanitize_diagnostic_text(title, 200)}")

    lowered_ct = content_type.lower()
    if "pdf" in lowered_ct or "octet" in lowered_ct:
        log(f"  Diagnostic: document URL returned PDF-like content for {doc_id}")
    elif "html" in lowered_ct:
        log(f"  Diagnostic: document URL appears to be an HTML viewer page for {doc_id}")
    else:
        log(f"  Diagnostic: document URL content type is not clearly PDF or HTML for {doc_id}")

    iframe_count = await page.locator("iframe").count()
    embed_count = await page.locator("embed").count()
    object_count = await page.locator("object").count()
    log(f"  Diagnostic iframe/embed/object counts for {doc_id}: {iframe_count}/{embed_count}/{object_count}")

    body_text = ""
    try:
        body = await page.query_selector("body")
        if body:
            body_text = await body.inner_text()
    except Exception:
        try:
            body_text = await page.locator("body").text_content(timeout=2000) or ""
        except Exception as e:
            body_text = f"unavailable: {e}"
    log(f"  Diagnostic body text for {doc_id}: {sanitize_diagnostic_text(body_text)}")

    attrs = []
    for el in await page.query_selector_all("a[href], iframe[src], embed[src], object[data]"):
        attr = (
            await el.get_attribute("href") or
            await el.get_attribute("src") or
            await el.get_attribute("data") or
            ""
        )
        if attr:
            attrs.append(sanitize_diagnostic_text(attr, 250))
        if len(attrs) >= 8:
            break
    log(f"  Diagnostic href/src samples for {doc_id}: {attrs if attrs else 'none'}")


async def save_download_diagnostic_snapshot(page, doc_id: str):
    """Save one local HTML snapshot per run for the first failed download."""
    global _download_diagnostic_snapshot_count
    if (
        not DOWNLOAD_DIAGNOSTICS or
        _download_diagnostic_snapshot_count >= DOWNLOAD_DIAGNOSTIC_SNAPSHOT_LIMIT
    ):
        return

    try:
        DOWNLOAD_DIAGNOSTICS_DIR.mkdir(parents=True, exist_ok=True)
        safe_doc_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", doc_id)
        snapshot_path = DOWNLOAD_DIAGNOSTICS_DIR / f"{safe_doc_id}_download_page.html"
        snapshot_path.write_text(await page.content(), encoding="utf-8")
        _download_diagnostic_snapshot_count += 1
        log(f"  Diagnostic HTML snapshot saved: {snapshot_path}")
    except Exception as e:
        log(f"  Diagnostic HTML snapshot failed for {doc_id}: {e}", "WARN")


async def read_download_bytes(download, doc_id: str) -> bytes | None:
    """Read bytes from a Playwright download temp file."""
    failure = await download.failure()
    if failure:
        log(f"  Download failed for {doc_id}: {failure}", "WARN")
        return None

    path = await download.path()
    if not path:
        log(f"  Download path unavailable for {doc_id}", "WARN")
        return None

    pdf_bytes = Path(path).read_bytes()
    log(f"  Captured PDF download for {doc_id}: {len(pdf_bytes)} bytes")
    return pdf_bytes


async def download_pdf(page, record: dict, context) -> bytes | None:
    """
    Navigate to a foreclosure record and capture the PDF bytes.
    The site renders the document via ViewECdocs.aspx — we intercept
    the PDF response from the network.
    """
    doc_id = record["doc_id"]
    href = record.get("href", "")
    pdf_bytes = None
    global _document_url_log_count

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
            target = resolve_document_url(href)
            if _document_url_log_count < DOCUMENT_URL_LOG_LIMIT:
                log(f"Resolved document URL for {doc_id}: {target}")
                _document_url_log_count += 1
            nav_response = None
            try:
                async with page.expect_download(timeout=20000) as download_info:
                    try:
                        nav_response = await page.goto(
                            target,
                            wait_until="domcontentloaded",
                            timeout=20000,
                        )
                    except PlaywrightError as e:
                        if "Download is starting" not in str(e):
                            raise
                        log(f"  Document URL started a Playwright download for {doc_id}")
                download = await download_info.value
                pdf_bytes = await read_download_bytes(download, doc_id)
            except PlaywrightTimeout:
                await page.wait_for_timeout(1000)
                await log_download_diagnostics(page, doc_id, nav_response)

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
            await page.wait_for_timeout(1000)
            await log_download_diagnostics(page, doc_id)

        await page.wait_for_timeout(3000)

        # If response handler didn't capture PDF, try fetching the current URL
        if not pdf_bytes:
            current_url = page.url
            if current_url and current_url != BASE_URL:
                resp = await page.request.get(current_url)
                ct = resp.headers.get("content-type", "")
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
                        pdf_url = resolve_document_url(src)
                        resp = await page.request.get(pdf_url)
                        pdf_bytes = await resp.body()
                        break

        if not pdf_bytes:
            await save_download_diagnostic_snapshot(page, doc_id)

    except PlaywrightTimeout:
        log(f"  Timeout on {doc_id}", "WARN")
    except Exception as e:
        log(f"  Download error for {doc_id}: {e}", "WARN")
    finally:
        page.remove_listener("response", handle_response)

    return pdf_bytes if pdf_bytes and len(pdf_bytes) > 500 else None
