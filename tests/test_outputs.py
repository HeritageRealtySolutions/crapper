import unittest
from pathlib import Path

from scraper.core.outputs import build_monthly_paths


class MonthlyOutputPathsTest(unittest.TestCase):
    def test_builds_harris_may_2026_paths(self):
        paths = build_monthly_paths(county="harris", year=2026, month=5)

        self.assertEqual(
            paths.output_csv,
            Path("data/outputs/harris_2026_05_foreclosures.csv"),
        )
        self.assertEqual(
            paths.output_jsonl,
            Path("data/outputs/harris_2026_05_foreclosures.jsonl"),
        )
        self.assertEqual(
            paths.checkpoint_json,
            Path("data/checkpoints/harris_2026_05_checkpoint.json"),
        )
        self.assertEqual(
            paths.failed_json,
            Path("data/failed/harris_2026_05_failed.json"),
        )
        self.assertEqual(
            paths.text_cache_dir,
            Path("data/text_cache/harris/2026/05"),
        )
        self.assertEqual(
            paths.pdfs_dir,
            Path("data/pdfs/harris/2026/05"),
        )

    def test_normalizes_county_case(self):
        paths = build_monthly_paths(county="Harris", year=2026, month=5)

        self.assertEqual(
            paths.output_csv,
            Path("data/outputs/harris_2026_05_foreclosures.csv"),
        )

    def test_rejects_invalid_month(self):
        with self.assertRaises(ValueError):
            build_monthly_paths(county="harris", year=2026, month=13)


if __name__ == "__main__":
    unittest.main()
