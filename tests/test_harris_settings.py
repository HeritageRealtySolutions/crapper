import unittest
from pathlib import Path

import harris_foreclosure_scraper_free as legacy
from scraper.counties.harris import settings


class HarrisSettingsTest(unittest.TestCase):
    def test_search_urls_are_unchanged(self):
        self.assertEqual(
            settings.BASE_URL,
            "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx",
        )
        self.assertEqual(settings.SEARCH_URL, settings.BASE_URL)
        self.assertEqual(settings.SITE_BASE_URL, "https://www.cclerk.hctx.net/")
        self.assertEqual(
            settings.WEBSEARCH_BASE_URL,
            "https://www.cclerk.hctx.net/applications/websearch/",
        )

    def test_legacy_month_selection_is_unchanged(self):
        self.assertEqual(settings.SALE_YEAR, "2026")
        self.assertEqual(settings.SALE_MONTH, "May")

    def test_legacy_output_paths_are_unchanged(self):
        self.assertEqual(settings.OUTPUT_CSV, "harris_foreclosures_may2026.csv")
        self.assertEqual(settings.CHECKPOINT_FILE, "scrape_checkpoint.json")
        self.assertEqual(settings.PDFS_DIR, Path("foreclosure_pdfs"))
        self.assertEqual(settings.LOG_FILE, "scraper.log")

    def test_legacy_script_exposes_unchanged_output_paths(self):
        self.assertEqual(legacy.OUTPUT_CSV, "harris_foreclosures_may2026.csv")
        self.assertEqual(legacy.CHECKPOINT_FILE, "scrape_checkpoint.json")
        self.assertEqual(legacy.PDFS_DIR, Path("foreclosure_pdfs"))
        self.assertEqual(legacy.LOG_FILE, "scraper.log")

    def test_legacy_runtime_settings_are_unchanged(self):
        self.assertEqual(settings.DELAY_BETWEEN_RECORDS, 2.0)
        self.assertEqual(settings.MAX_RETRIES, 3)
        self.assertTrue(settings.HEADLESS)
        self.assertEqual(settings.DOCUMENT_URL_LOG_LIMIT, 5)
        self.assertTrue(settings.DOWNLOAD_DIAGNOSTICS)
        self.assertEqual(settings.DOWNLOAD_DIAGNOSTIC_SNAPSHOT_LIMIT, 1)
        self.assertEqual(
            settings.DOWNLOAD_DIAGNOSTICS_DIR,
            Path("data/local/diagnostics"),
        )


if __name__ == "__main__":
    unittest.main()
