import csv
import json
import tempfile
import unittest
from pathlib import Path

from scraper.core.writers import (
    append_csv_row,
    append_jsonl_row,
    ensure_parent_dir,
    read_csv_rows,
    write_csv_rows,
    write_jsonl_rows,
)


class WriterHelpersTest(unittest.TestCase):
    def test_ensure_parent_dir_creates_parent_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "deeper" / "rows.csv"

            returned = ensure_parent_dir(path)

            self.assertEqual(returned, path)
            self.assertTrue(path.parent.exists())

    def test_write_csv_rows_preserves_header_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.csv"
            fieldnames = ["doc_id", "amount", "name"]
            rows = [
                {"name": "Borrower", "doc_id": "FRCL-1", "amount": "100.00"},
            ]

            count = write_csv_rows(path, rows, fieldnames)

            self.assertEqual(count, 1)
            with path.open(newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                self.assertEqual(next(reader), fieldnames)

    def test_append_csv_row_writes_header_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.csv"
            fieldnames = ["doc_id", "status"]

            append_csv_row(path, {"doc_id": "FRCL-1", "status": "OK"}, fieldnames)
            append_csv_row(path, {"doc_id": "FRCL-2", "status": "FAILED"}, fieldnames)

            lines = path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines[0], "doc_id,status")
            self.assertEqual(lines.count("doc_id,status"), 1)
            self.assertEqual(read_csv_rows(path), [
                {"doc_id": "FRCL-1", "status": "OK"},
                {"doc_id": "FRCL-2", "status": "FAILED"},
            ])

    def test_write_jsonl_rows_writes_valid_json_objects(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.jsonl"
            rows = [{"doc_id": "FRCL-1"}, {"doc_id": "FRCL-2"}]

            count = write_jsonl_rows(path, rows)

            self.assertEqual(count, 2)
            loaded = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(loaded, rows)

    def test_append_jsonl_row_appends_one_row_at_a_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.jsonl"

            append_jsonl_row(path, {"doc_id": "FRCL-1"})
            append_jsonl_row(path, {"doc_id": "FRCL-2"})

            loaded = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(loaded, [{"doc_id": "FRCL-1"}, {"doc_id": "FRCL-2"}])


if __name__ == "__main__":
    unittest.main()
