"""Harris County foreclosure parser.

This module preserves the parser behavior from harris_foreclosure_scraper_free.py
so it can be characterized independently before broader refactors.

Fixes applied:
- substitute_trustee added to CSV_FIELDS (was extracted but silently dropped)
- grantor_borrower regex hardened with multi-field boundary stop condition
"""

from __future__ import annotations

import re

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
    "substitute_trustee",  # FIX: was extracted but not in CSV_FIELDS — silently dropped before
    "raw_text_snippet",
    "parse_notes",
]

# Common field labels that signal the end of the grantor/borrower value.
# Using a character-class stop anchor is more robust than relying on any single
# field label appearing on the same line.
_FIELD_BOUNDARY = (
    r"(?=\n(?:"
    r"Original\s+Mortgagee"
    r"|Current\s+Mortgagee"
    r"|Deed\s+of\s+Trust"
    r"|Amount"
    r"|Recording"
    r"|Legal\s+Description"
    r"|Date\s+of\s+Sale"
    r"|Earliest"
    r"|Mortgagee\s+Servicer"
    r"|Substitute\s+Trustee"
    r"|Pursuant"
    r"|WHEREAS"
    r")|\n\n|\Z)"
)


def clean(s: str | None) -> str:
    """Collapse whitespace and strip a string."""
    if not s:
        return "N/A"
    return re.sub(r"\s+", " ", s).strip()


def extract_field(pattern: str, text: str, flags=re.IGNORECASE | re.DOTALL) -> str | None:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def parse_foreclosure_text(full_text: str, doc_id: str) -> dict:
    """
    Extract all structured fields from already-extracted Harris foreclosure text.
    Returns a dict of field values.
    """
    notes = []
    result = {}

    if not full_text.strip():
        return {"parse_notes": "PDF extracted but no text found — may be image-based"}

    # ── Store a short snippet for debugging ──
    result["raw_text_snippet"] = full_text[:300].replace("\n", " ")

    # ── Grantor / Borrower ──
    # FIX: old pattern used "Original Mortgagee" as the sole stop anchor, which
    # silently corrupted or dropped the field when field order varied across lenders.
    # New pattern uses _FIELD_BOUNDARY: a lookahead for any known next-field label,
    # a blank line, or end-of-string — whichever comes first.
    grantor = extract_field(
        r"Grantor\(?s?\)?\s*:\s*(.+?)" + _FIELD_BOUNDARY,
        full_text,
    )
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
        amount = extract_field(
            r"original\s+(?:note|loan|principal)\s+(?:amount|balance)\s*(?:of|:)?\s*\$?([\d,\.]+)",
            full_text,
        )
    if amount:
        result["original_loan_amount"] = amount.replace(",", "")
    else:
        result["original_loan_amount"] = "N/A"
        notes.append("amount not found")

    # ── Original Mortgagee (lender) ──
    orig_mort = extract_field(
        r"Original Mortgagee\s*:\s*(.+?)(?=\n|Current Mortgagee)", full_text
    )
    result["original_mortgagee"] = clean(orig_mort)

    # ── Current Mortgagee ──
    curr_mort = extract_field(
        r"Current Mortgagee\s*:\s*(.+?)(?=\n|Mortgagee Servicer)", full_text
    )
    result["current_mortgagee"] = clean(curr_mort)

    # ── Servicer name and address ──
    servicer_block = extract_field(
        r"Mortgagee Servicer and Address\s*:\s*(.+?)(?=\nPursuant|\nRecording|\nLegal|\n\n)",
        full_text,
    )
    if servicer_block:
        sb = clean(servicer_block)
        if sb.startswith("c/o "):
            sb = sb[4:]
        parts = sb.split(",", 1)
        result["servicer_name"] = clean(parts[0])
        result["servicer_address"] = clean(parts[1]) if len(parts) > 1 else "N/A"
    else:
        result["servicer_name"] = "N/A"
        result["servicer_address"] = "N/A"

    # ── Property Address ──
    prop_addr = extract_field(
        r"(?:Property Address|Subject Property|Property Located at)\s*:\s*(.+?)(?=\n)",
        full_text,
    )
    if not prop_addr:
        prop_addr = extract_field(
            r"(?:located at|known as)\s+(\d+\s+[\w\s]+(?:Street|St|Avenue|Ave|Drive|Dr|"
            r"Lane|Ln|Road|Rd|Blvd|Boulevard|Court|Ct|Way|Circle|Cir)[\w\s,\.]*?)(?=\n|,\s*Harris)",
            full_text,
            re.IGNORECASE,
        )
    result["property_address"] = clean(prop_addr) if prop_addr else "See Legal Description"

    # ── Legal Description ──
    legal = extract_field(
        r"Legal Description\s*:\s*(.+?)(?=\nWhereas|\nDate of Sale|\nEarliest|\n\n)",
        full_text,
    )
    result["legal_description"] = clean(legal)

    # ── Date of Sale (from document body) ──
    sale_date = extract_field(r"Date of Sale\s*:\s*(.+?)(?=\n|Earliest)", full_text)
    result["sale_date_from_doc"] = clean(sale_date)

    # ── Earliest Sale Time ──
    earliest = extract_field(
        r"Earliest Time Sale Will Begin\s*:\s*(.+?)(?=\n|Place of Sale)", full_text
    )
    result["earliest_sale_time"] = clean(earliest)

    # ── Recording Document Number ──
    rec_num = extract_field(
        r"Recording Information\s*:.*?Document No\.?\s*([\d\-RP]+)", full_text
    )
    result["recording_doc_number"] = clean(rec_num)

    # ── Substitute Trustee ──
    # FIX: previously extracted but not in CSV_FIELDS — value was discarded.
    trustee = extract_field(
        r"(?:Substitute Trustee|appointed as Substitute Trustee)\s*[:\(]?\s*(.+?)(?=\n|each acting)",
        full_text,
    )
    result["substitute_trustee"] = clean(trustee)

    result["parse_notes"] = "; ".join(notes) if notes else "OK"
    return result
