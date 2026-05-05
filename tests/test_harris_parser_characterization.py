import json
import unittest
from pathlib import Path

from harris_foreclosure_scraper_free import CSV_FIELDS, parse_foreclosure_text


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "harris"
OCR_TEXT_DIR = FIXTURES_DIR / "ocr_text"
EXPECTED_JSONL = FIXTURES_DIR / "expected" / "harris_2026_05_expected.jsonl"

EXPECTED_CSV_FIELDS = [
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

PARSED_ROW_DEFAULTS = {
    "grantor_borrower": "N/A",
    "property_address": "N/A",
    "deed_of_trust_date": "N/A",
    "original_loan_amount": "N/A",
    "original_mortgagee": "N/A",
    "current_mortgagee": "N/A",
    "servicer_name": "N/A",
    "servicer_address": "N/A",
    "sale_date_from_doc": "N/A",
    "earliest_sale_time": "N/A",
    "legal_description": "N/A",
    "recording_doc_number": "N/A",
    "raw_text_snippet": "",
    "parse_notes": "",
}


def load_expected_rows():
    if not EXPECTED_JSONL.exists():
        raise unittest.SkipTest(f"Missing local golden expected file: {EXPECTED_JSONL}")

    rows = []
    with EXPECTED_JSONL.open(encoding="utf-8") as expected_file:
        for line in expected_file:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def build_current_output_row(expected_row, parsed):
    row = {
        "doc_id": expected_row["doc_id"],
        "sale_date_from_table": expected_row["sale_date_from_table"],
        "file_date": expected_row["file_date"],
        "pages": expected_row["pages"],
    }
    for field, default in PARSED_ROW_DEFAULTS.items():
        row[field] = parsed.get(field, default)
    return row


class HarrisParserCharacterizationTest(unittest.TestCase):
    def test_csv_fields_order_is_unchanged(self):
        self.assertEqual(CSV_FIELDS, EXPECTED_CSV_FIELDS)

    def test_golden_sample_fixtures_exist(self):
        rows = load_expected_rows()

        self.assertGreaterEqual(len(rows), 11)
        for row in rows:
            fixture_path = OCR_TEXT_DIR / f"{row['doc_id']}.txt"
            self.assertTrue(fixture_path.exists(), f"Missing text fixture: {fixture_path}")

    def test_parser_rows_match_current_expected_outputs(self):
        rows = load_expected_rows()

        for expected_row in rows:
            with self.subTest(doc_id=expected_row["doc_id"]):
                fixture_path = OCR_TEXT_DIR / f"{expected_row['doc_id']}.txt"
                text = fixture_path.read_text(encoding="utf-8")

                parsed = parse_foreclosure_text(text, expected_row["doc_id"])
                actual_row = build_current_output_row(expected_row, parsed)

                self.assertEqual(list(actual_row.keys()), CSV_FIELDS)
                self.assertEqual(list(expected_row.keys()), CSV_FIELDS)
                self.assertEqual(actual_row, expected_row)


if __name__ == "__main__":
    unittest.main()
