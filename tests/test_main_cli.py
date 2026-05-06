import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from scraper import main as scraper_main


class MainCliTest(unittest.TestCase):
    def test_dispatches_harris_monthly_runner(self):
        result = SimpleNamespace(
            dry_run=False,
            processed=0,
            skipped=0,
            planned=0,
            planned_doc_ids=[],
            failed=0,
            paths=SimpleNamespace(
                output_csv=Path("data/outputs/harris_2026_05_foreclosures.csv"),
                output_jsonl=Path("data/outputs/harris_2026_05_foreclosures.jsonl"),
                checkpoint_json=Path("data/checkpoints/harris_2026_05_checkpoint.json"),
                failed_json=Path("data/failed/harris_2026_05_failed.json"),
            ),
        )

        stdout = io.StringIO()
        with patch("scraper.main.run_harris_monthly", new=AsyncMock(return_value=result)) as run:
            with patch("sys.stdout", new=stdout):
                exit_code = scraper_main.main([
                    "--county", "harris",
                    "--year", "2026",
                    "--month", "5",
                    "--limit", "10",
                ])

        self.assertEqual(exit_code, 0)
        run.assert_awaited_once_with(
            year=2026,
            month=5,
            limit=10,
            resume=False,
            retry_failed=False,
            dry_run=False,
            use_ocr=False,
            reprocess_existing=False,
        )
        output = stdout.getvalue()
        self.assertIn("Processed: 0", output)
        self.assertIn("Skipped: 0", output)
        self.assertIn("Failed: 0", output)
        self.assertIn("CSV: data/outputs/harris_2026_05_foreclosures.csv", output)
        self.assertIn("JSONL: data/outputs/harris_2026_05_foreclosures.jsonl", output)

    def test_accepts_dry_run_flag(self):
        result = SimpleNamespace(
            dry_run=True,
            processed=0,
            skipped=0,
            planned=2,
            planned_doc_ids=["FRCL-2026-1", "FRCL-2026-2"],
            failed=0,
            paths=SimpleNamespace(
                output_csv=Path("data/outputs/harris_2026_05_foreclosures.csv"),
                output_jsonl=Path("data/outputs/harris_2026_05_foreclosures.jsonl"),
                checkpoint_json=Path("data/checkpoints/harris_2026_05_checkpoint.json"),
                failed_json=Path("data/failed/harris_2026_05_failed.json"),
            ),
        )

        stdout = io.StringIO()
        with patch("scraper.main.run_harris_monthly", new=AsyncMock(return_value=result)) as run:
            with patch("sys.stdout", new=stdout):
                exit_code = scraper_main.main([
                    "--county", "harris",
                    "--year", "2026",
                    "--month", "5",
                    "--limit", "10",
                    "--dry-run",
                ])

        self.assertEqual(exit_code, 0)
        run.assert_awaited_once_with(
            year=2026,
            month=5,
            limit=10,
            resume=False,
            retry_failed=False,
            dry_run=True,
            use_ocr=False,
            reprocess_existing=False,
        )
        output = stdout.getvalue()
        self.assertIn("DRY RUN", output)
        self.assertIn("Planned records: 2", output)
        self.assertIn("FRCL-2026-1", output)

    def test_accepts_ocr_and_reprocess_existing_flags(self):
        result = SimpleNamespace(
            dry_run=False,
            processed=0,
            skipped=0,
            planned=0,
            planned_doc_ids=[],
            failed=0,
            extraction_methods={},
            extraction_notes={},
            paths=SimpleNamespace(
                output_csv=Path("data/outputs/harris_2026_06_foreclosures.csv"),
                output_jsonl=Path("data/outputs/harris_2026_06_foreclosures.jsonl"),
                checkpoint_json=Path("data/checkpoints/harris_2026_06_checkpoint.json"),
                failed_json=Path("data/failed/harris_2026_06_failed.json"),
            ),
        )

        with patch("scraper.main.run_harris_monthly", new=AsyncMock(return_value=result)) as run:
            exit_code = scraper_main.main([
                "--county", "harris",
                "--year", "2026",
                "--month", "6",
                "--limit", "1",
                "--ocr",
                "--reprocess-existing",
            ])

        self.assertEqual(exit_code, 0)
        run.assert_awaited_once_with(
            year=2026,
            month=6,
            limit=1,
            resume=False,
            retry_failed=False,
            dry_run=False,
            use_ocr=True,
            reprocess_existing=True,
        )

    def test_bad_county_fails_clearly(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            with self.assertRaises(SystemExit) as raised:
                scraper_main.main(["--county", "bad", "--year", "2026", "--month", "5"])

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("only county=harris is supported right now", stderr.getvalue())

    def test_resume_and_retry_failed_are_mutually_exclusive(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            with self.assertRaises(SystemExit) as raised:
                scraper_main.main([
                    "--county", "harris",
                    "--year", "2026",
                    "--month", "5",
                    "--resume",
                    "--retry-failed",
                ])

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("not allowed with argument", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
