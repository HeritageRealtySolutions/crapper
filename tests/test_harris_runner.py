import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from scraper.core.checkpoints import load_checkpoint, save_checkpoint
from scraper.core.outputs import MonthlyPaths
from scraper.core.pdfs import pdf_path_for, save_pdf_bytes, save_text, text_path_for
from scraper.core.writers import read_csv_rows
from scraper.counties.harris.parser import CSV_FIELDS
from scraper.counties.harris.runner import run_harris_monthly


SAMPLE_TEXT = """
Grantor(s): Jane Borrower
Deed of Trust Dated: January 1, 2024
Amount: $123,456.78
Original Mortgagee: Original Bank
Current Mortgagee: Current Bank
Mortgagee Servicer and Address: Servicer LLC, 123 Main Street
Property Address: 100 Main Street
Legal Description: Lot 1, Block 2
Date of Sale: May 5, 2026
Earliest Time Sale Will Begin: 10:00 AM
Recording Information: Document No. RP-2024-123
"""


class FakePage:
    async def close(self):
        pass


class FakeContext:
    async def new_page(self):
        return FakePage()


class FakeBrowser:
    async def new_context(self, user_agent):
        return FakeContext()

    async def close(self):
        pass


class FakeChromium:
    async def launch(self, headless):
        return FakeBrowser()


class FakePlaywright:
    def __init__(self):
        self.chromium = FakeChromium()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakePlaywrightFactory:
    def __call__(self):
        return FakePlaywright()


def monthly_paths(root: Path) -> MonthlyPaths:
    return MonthlyPaths(
        output_csv=root / "data" / "outputs" / "harris_2026_05_foreclosures.csv",
        output_jsonl=root / "data" / "outputs" / "harris_2026_05_foreclosures.jsonl",
        checkpoint_json=root / "data" / "checkpoints" / "harris_2026_05_checkpoint.json",
        failed_json=root / "data" / "failed" / "harris_2026_05_failed.json",
        text_cache_dir=root / "data" / "text_cache" / "harris" / "2026" / "05",
        pdfs_dir=root / "data" / "pdfs" / "harris" / "2026" / "05",
    )


def sample_records():
    return [
        {
            "doc_id": "FRCL-2026-1",
            "sale_date": "05/05/2026",
            "file_date": "04/01/2026",
            "pages": "3",
            "href": "ViewECdocs.aspx?ID=1",
        },
        {
            "doc_id": "FRCL-2026-2",
            "sale_date": "05/05/2026",
            "file_date": "04/02/2026",
            "pages": "4",
            "href": "ViewECdocs.aspx?ID=2",
        },
    ]


def read_jsonl_rows(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class HarrisRunnerTest(unittest.TestCase):
    def test_monthly_runner_writes_monthly_outputs_and_honors_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))

            async def collect_records(page, sale_year, sale_month):
                self.assertEqual(sale_year, "2026")
                self.assertEqual(sale_month, "May")
                return sample_records()

            async def download_pdf(page, record, context):
                return f"%PDF-{record['doc_id']}".encode("utf-8")

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    limit=1,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=download_pdf,
                    extract_text_func=lambda pdf_bytes: SAMPLE_TEXT,
                    delay_between_records=0,
                )
            )

            self.assertEqual(result.processed, 1)
            self.assertEqual(result.skipped, 0)
            self.assertEqual(result.completed, 1)
            self.assertEqual(result.failed, 0)

            rows = read_csv_rows(paths.output_csv)
            self.assertEqual(len(rows), 1)
            self.assertEqual(list(rows[0].keys()), CSV_FIELDS)
            self.assertEqual(rows[0]["doc_id"], "FRCL-2026-1")
            self.assertEqual(rows[0]["sale_date_from_table"], "05/05/2026")

            jsonl_rows = read_jsonl_rows(paths.output_jsonl)
            self.assertEqual(len(jsonl_rows), 1)
            self.assertEqual(jsonl_rows[0]["doc_id"], "FRCL-2026-1")

            self.assertTrue(pdf_path_for(paths.pdfs_dir, "FRCL-2026-1").exists())
            self.assertFalse(pdf_path_for(paths.pdfs_dir, "FRCL-2026-2").exists())
            self.assertTrue(text_path_for(paths.text_cache_dir, "FRCL-2026-1").exists())

            checkpoint = load_checkpoint(paths.checkpoint_json)
            self.assertEqual(len(checkpoint["all_records"]), 2)
            self.assertEqual(checkpoint["completed_ids"], ["FRCL-2026-1"])
            self.assertEqual(checkpoint["failed_ids"], [])

            failed = json.loads(paths.failed_json.read_text(encoding="utf-8"))
            self.assertEqual(failed["failed_ids"], [])
            self.assertEqual(failed["failed_reasons"], {})

    def test_failed_download_is_tracked_without_output_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))

            async def collect_records(page, sale_year, sale_month):
                return sample_records()[:1]

            async def download_pdf(page, record, context):
                return None

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=download_pdf,
                    extract_text_func=lambda pdf_bytes: self.fail("should not extract text"),
                    delay_between_records=0,
                )
            )

            self.assertEqual(result.processed, 0)
            self.assertEqual(result.skipped, 0)
            self.assertEqual(result.completed, 0)
            self.assertEqual(result.failed, 1)
            self.assertEqual(read_csv_rows(paths.output_csv), [])

            checkpoint = load_checkpoint(paths.checkpoint_json)
            self.assertEqual(checkpoint["failed_ids"], ["FRCL-2026-1"])
            self.assertEqual(
                checkpoint["failed_reasons"]["FRCL-2026-1"],
                "PDF download failed",
            )

            failed = json.loads(paths.failed_json.read_text(encoding="utf-8"))
            self.assertEqual(failed["failed_ids"], ["FRCL-2026-1"])

    def test_resume_uses_checkpoint_records_and_skips_completed_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))
            save_checkpoint(
                paths.checkpoint_json,
                {
                    "all_records": sample_records(),
                    "completed_ids": ["FRCL-2026-1"],
                    "failed_ids": [],
                    "failed_reasons": {},
                },
            )

            async def collect_records(page, sale_year, sale_month):
                self.fail("resume should use checkpoint records")

            async def download_pdf(page, record, context):
                return f"%PDF-{record['doc_id']}".encode("utf-8")

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    resume=True,
                    limit=1,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=download_pdf,
                    extract_text_func=lambda pdf_bytes: SAMPLE_TEXT,
                    delay_between_records=0,
                )
            )

            self.assertEqual(result.processed, 1)
            self.assertEqual(result.skipped, 1)
            rows = read_csv_rows(paths.output_csv)
            self.assertEqual([row["doc_id"] for row in rows], ["FRCL-2026-2"])

            checkpoint = load_checkpoint(paths.checkpoint_json)
            self.assertEqual(
                checkpoint["completed_ids"],
                ["FRCL-2026-1", "FRCL-2026-2"],
            )

    def test_fresh_limit_rerun_does_not_duplicate_rows_and_reuses_caches(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))
            download_calls = []
            extract_calls = []

            async def collect_records(page, sale_year, sale_month):
                return sample_records()[:1]

            async def download_pdf(page, record, context):
                download_calls.append(record["doc_id"])
                return f"%PDF-{record['doc_id']}".encode("utf-8")

            def extract_text(pdf_bytes):
                extract_calls.append(pdf_bytes)
                return SAMPLE_TEXT

            for _ in range(2):
                result = asyncio.run(
                    run_harris_monthly(
                        year=2026,
                        month=5,
                        limit=1,
                        paths=paths,
                        playwright_factory=FakePlaywrightFactory(),
                        collect_records_func=collect_records,
                        download_pdf_func=download_pdf,
                        extract_text_func=extract_text,
                        delay_between_records=0,
                    )
                )
                self.assertEqual(result.processed, 1)

            self.assertEqual(download_calls, ["FRCL-2026-1"])
            self.assertEqual(len(extract_calls), 1)
            self.assertEqual([row["doc_id"] for row in read_csv_rows(paths.output_csv)], ["FRCL-2026-1"])
            self.assertEqual([row["doc_id"] for row in read_jsonl_rows(paths.output_jsonl)], ["FRCL-2026-1"])

    def test_resume_rerun_with_completed_checkpoint_does_not_duplicate_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))

            async def collect_records(page, sale_year, sale_month):
                return sample_records()[:1]

            async def download_pdf(page, record, context):
                return f"%PDF-{record['doc_id']}".encode("utf-8")

            first = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    limit=1,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=download_pdf,
                    extract_text_func=lambda pdf_bytes: SAMPLE_TEXT,
                    delay_between_records=0,
                )
            )
            second = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    resume=True,
                    limit=1,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=lambda page, sale_year, sale_month: self.fail("should use checkpoint"),
                    download_pdf_func=lambda page, record, context: self.fail("completed record should be skipped"),
                    extract_text_func=lambda pdf_bytes: self.fail("completed record should be skipped"),
                    delay_between_records=0,
                )
            )

            self.assertEqual(first.processed, 1)
            self.assertEqual(second.processed, 0)
            self.assertEqual(second.skipped, 1)
            self.assertEqual([row["doc_id"] for row in read_csv_rows(paths.output_csv)], ["FRCL-2026-1"])
            self.assertEqual([row["doc_id"] for row in read_jsonl_rows(paths.output_jsonl)], ["FRCL-2026-1"])

    def test_retry_failed_only_targets_failed_records_and_clears_failure_on_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))
            save_checkpoint(
                paths.checkpoint_json,
                {
                    "all_records": sample_records(),
                    "completed_ids": ["FRCL-2026-1"],
                    "failed_ids": ["FRCL-2026-2"],
                    "failed_reasons": {"FRCL-2026-2": "PDF download failed"},
                },
            )
            downloaded = []

            async def collect_records(page, sale_year, sale_month):
                self.fail("retry-failed should use checkpoint records")

            async def download_pdf(page, record, context):
                downloaded.append(record["doc_id"])
                return f"%PDF-{record['doc_id']}".encode("utf-8")

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    retry_failed=True,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=download_pdf,
                    extract_text_func=lambda pdf_bytes: SAMPLE_TEXT,
                    delay_between_records=0,
                )
            )

            self.assertEqual(downloaded, ["FRCL-2026-2"])
            self.assertEqual(result.processed, 1)
            self.assertEqual(result.skipped, 1)
            self.assertEqual(result.failed, 0)

            checkpoint = load_checkpoint(paths.checkpoint_json)
            self.assertEqual(checkpoint["completed_ids"], ["FRCL-2026-1", "FRCL-2026-2"])
            self.assertEqual(checkpoint["failed_ids"], [])
            self.assertEqual(checkpoint["failed_reasons"], {})

    def test_failure_reason_from_exception_is_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))

            async def collect_records(page, sale_year, sale_month):
                return sample_records()[:1]

            async def download_pdf(page, record, context):
                return b"%PDF-bytes"

            def extract_text(pdf_bytes):
                raise RuntimeError("text extraction failed")

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=download_pdf,
                    extract_text_func=extract_text,
                    delay_between_records=0,
                )
            )

            self.assertEqual(result.failed, 1)
            checkpoint = load_checkpoint(paths.checkpoint_json)
            self.assertEqual(
                checkpoint["failed_reasons"]["FRCL-2026-1"],
                "text extraction failed",
            )
            failed = json.loads(paths.failed_json.read_text(encoding="utf-8"))
            self.assertEqual(
                failed["failed_reasons"]["FRCL-2026-1"],
                "text extraction failed",
            )

    def test_pdf_and_text_cache_are_reused_when_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))
            save_pdf_bytes(paths.pdfs_dir, "FRCL-2026-1", b"%PDF-cached")
            save_text(paths.text_cache_dir, "FRCL-2026-1", SAMPLE_TEXT)

            async def collect_records(page, sale_year, sale_month):
                return sample_records()[:1]

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=collect_records,
                    download_pdf_func=lambda page, record, context: self.fail("PDF cache should be reused"),
                    extract_text_func=lambda pdf_bytes: self.fail("text cache should be reused"),
                    delay_between_records=0,
                )
            )

            self.assertEqual(result.processed, 1)
            rows = read_csv_rows(paths.output_csv)
            self.assertEqual([row["doc_id"] for row in rows], ["FRCL-2026-1"])

    def test_resume_limit_applies_after_completed_ids_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = monthly_paths(Path(tmp))
            records = sample_records() + [
                {
                    "doc_id": "FRCL-2026-3",
                    "sale_date": "05/05/2026",
                    "file_date": "04/03/2026",
                    "pages": "5",
                    "href": "ViewECdocs.aspx?ID=3",
                }
            ]
            save_checkpoint(
                paths.checkpoint_json,
                {
                    "all_records": records,
                    "completed_ids": ["FRCL-2026-1"],
                    "failed_ids": [],
                    "failed_reasons": {},
                },
            )
            downloaded = []

            async def download_pdf(page, record, context):
                downloaded.append(record["doc_id"])
                return f"%PDF-{record['doc_id']}".encode("utf-8")

            result = asyncio.run(
                run_harris_monthly(
                    year=2026,
                    month=5,
                    resume=True,
                    limit=1,
                    paths=paths,
                    playwright_factory=FakePlaywrightFactory(),
                    collect_records_func=lambda page, sale_year, sale_month: self.fail("should use checkpoint"),
                    download_pdf_func=download_pdf,
                    extract_text_func=lambda pdf_bytes: SAMPLE_TEXT,
                    delay_between_records=0,
                )
            )

            self.assertEqual(downloaded, ["FRCL-2026-2"])
            self.assertEqual(result.processed, 1)
            self.assertEqual(result.skipped, 1)
            self.assertEqual([row["doc_id"] for row in read_csv_rows(paths.output_csv)], ["FRCL-2026-2"])


if __name__ == "__main__":
    unittest.main()
