import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from scraper import main as scraper_main


class MainCliTest(unittest.TestCase):
    def test_dispatches_harris_monthly_runner(self):
        result = SimpleNamespace(
            processed=0,
            paths=SimpleNamespace(
                output_csv=Path("data/outputs/harris_2026_05_foreclosures.csv"),
                output_jsonl=Path("data/outputs/harris_2026_05_foreclosures.jsonl"),
                checkpoint_json=Path("data/checkpoints/harris_2026_05_checkpoint.json"),
                failed_json=Path("data/failed/harris_2026_05_failed.json"),
            ),
        )

        with patch("scraper.main.run_harris_monthly", new=AsyncMock(return_value=result)) as run:
            with patch("sys.stdout", new=io.StringIO()):
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
