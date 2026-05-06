"""Supabase writer for foreclosure records.

Upserts parsed foreclosure rows into the `foreclosures` table.
Uses the service role key to bypass Row Level Security.

The writer is intentionally optional — if SUPABASE_URL or
SUPABASE_SERVICE_ROLE_KEY are not set, all write calls are
no-ops and the scraper continues normally. This means the
scraper never fails due to a Supabase outage or missing config.

Environment variables (set in .env locally, Railway for prod):
    SUPABASE_URL               https://xxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY  eyJ...
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Lazy client — only initialized when first write is attempted
_client = None
_client_attempted = False


def _get_client():
    """Return a Supabase client or None if not configured."""
    global _client, _client_attempted

    if _client_attempted:
        return _client

    _client_attempted = True
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if not url or not key:
        logger.warning(
            "Supabase writer: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set. "
            "Records will not be written to Supabase."
        )
        return None

    try:
        from supabase import create_client
        _client = create_client(url, key)
        logger.info("Supabase writer: client initialized — %s", url)
    except ImportError:
        logger.warning(
            "Supabase writer: `supabase` package not installed. "
            "Run: pip install supabase"
        )
    except Exception as e:
        logger.warning("Supabase writer: client init failed — %s", e)

    return _client


def upsert_foreclosure_row(
    row: dict[str, Any],
    *,
    county: str,
    sale_year: int,
    sale_month: int,
) -> bool:
    """
    Upsert a single foreclosure row into Supabase.

    Idempotent — safe to call on every run. If the doc_id already
    exists, the record is updated. If not, it is inserted.

    Returns True on success, False on failure or if Supabase is
    not configured. Never raises — the scraper must not fail here.
    """
    client = _get_client()
    if client is None:
        return False

    doc_id = row.get("doc_id", "")
    if not doc_id:
        logger.warning("Supabase writer: skipping row with no doc_id")
        return False

    payload = {
        "doc_id":                doc_id,
        "county":                county,
        "sale_year":             sale_year,
        "sale_month":            sale_month,
        "sale_date_from_table":  row.get("sale_date_from_table") or None,
        "file_date":             row.get("file_date") or None,
        "pages":                 row.get("pages") or None,
        "grantor_borrower":      _na_to_none(row.get("grantor_borrower")),
        "property_address":      _na_to_none(row.get("property_address")),
        "deed_of_trust_date":    _na_to_none(row.get("deed_of_trust_date")),
        "original_loan_amount":  _na_to_none(row.get("original_loan_amount")),
        "original_mortgagee":    _na_to_none(row.get("original_mortgagee")),
        "current_mortgagee":     _na_to_none(row.get("current_mortgagee")),
        "servicer_name":         _na_to_none(row.get("servicer_name")),
        "servicer_address":      _na_to_none(row.get("servicer_address")),
        "sale_date_from_doc":    _na_to_none(row.get("sale_date_from_doc")),
        "earliest_sale_time":    _na_to_none(row.get("earliest_sale_time")),
        "legal_description":     _na_to_none(row.get("legal_description")),
        "recording_doc_number":  _na_to_none(row.get("recording_doc_number")),
        "substitute_trustee":    _na_to_none(row.get("substitute_trustee")),
        "raw_text_snippet":      row.get("raw_text_snippet") or None,
        "parse_notes":           row.get("parse_notes") or None,
    }

    try:
        client.table("foreclosures").upsert(
            payload,
            on_conflict="doc_id",
        ).execute()
        logger.debug("Supabase writer: upserted %s", doc_id)
        return True
    except Exception as e:
        logger.warning("Supabase writer: upsert failed for %s — %s", doc_id, e)
        return False


def upsert_foreclosure_batch(
    rows: list[dict[str, Any]],
    *,
    county: str,
    sale_year: int,
    sale_month: int,
) -> tuple[int, int]:
    """
    Upsert a list of foreclosure rows in a single batch call.

    Returns (success_count, failure_count).
    Falls back to row-by-row if batch call fails.
    """
    client = _get_client()
    if client is None:
        return 0, 0

    if not rows:
        return 0, 0

    payloads = []
    for row in rows:
        doc_id = row.get("doc_id", "")
        if not doc_id:
            continue
        payloads.append({
            "doc_id":               doc_id,
            "county":               county,
            "sale_year":            sale_year,
            "sale_month":           sale_month,
            "sale_date_from_table": row.get("sale_date_from_table") or None,
            "file_date":            row.get("file_date") or None,
            "pages":                row.get("pages") or None,
            "grantor_borrower":     _na_to_none(row.get("grantor_borrower")),
            "property_address":     _na_to_none(row.get("property_address")),
            "deed_of_trust_date":   _na_to_none(row.get("deed_of_trust_date")),
            "original_loan_amount": _na_to_none(row.get("original_loan_amount")),
            "original_mortgagee":   _na_to_none(row.get("original_mortgagee")),
            "current_mortgagee":    _na_to_none(row.get("current_mortgagee")),
            "servicer_name":        _na_to_none(row.get("servicer_name")),
            "servicer_address":     _na_to_none(row.get("servicer_address")),
            "sale_date_from_doc":   _na_to_none(row.get("sale_date_from_doc")),
            "earliest_sale_time":   _na_to_none(row.get("earliest_sale_time")),
            "legal_description":    _na_to_none(row.get("legal_description")),
            "recording_doc_number": _na_to_none(row.get("recording_doc_number")),
            "substitute_trustee":   _na_to_none(row.get("substitute_trustee")),
            "raw_text_snippet":     row.get("raw_text_snippet") or None,
            "parse_notes":          row.get("parse_notes") or None,
        })

    if not payloads:
        return 0, 0

    try:
        client.table("foreclosures").upsert(
            payloads,
            on_conflict="doc_id",
        ).execute()
        logger.info("Supabase writer: batch upserted %d rows", len(payloads))
        return len(payloads), 0
    except Exception as e:
        logger.warning(
            "Supabase writer: batch upsert failed (%s) — falling back to row-by-row", e
        )
        success, failure = 0, 0
        for row in rows:
            ok = upsert_foreclosure_row(row, county=county, sale_year=sale_year, sale_month=sale_month)
            if ok:
                success += 1
            else:
                failure += 1
        return success, failure


def _na_to_none(value: str | None) -> str | None:
    """Convert parser 'N/A' sentinel to NULL for clean DB storage."""
    if value is None or value == "N/A":
        return None
    return value or None
