import unittest

import harris_foreclosure_scraper_free as legacy_scraper
from scraper.counties.harris.client import resolve_document_url


class HarrisUrlResolutionTest(unittest.TestCase):
    def test_resolves_relative_viewecdocs_url(self):
        self.assertEqual(
            resolve_document_url("viewecdocs.aspx/?ID=abc123"),
            "https://www.cclerk.hctx.net/applications/websearch/viewecdocs.aspx/?ID=abc123",
        )

    def test_resolves_root_viewecdocs_url_to_websearch_app(self):
        self.assertEqual(
            resolve_document_url("/ViewECdocs.aspx?ID=abc123"),
            "https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx?ID=abc123",
        )

    def test_preserves_already_absolute_urls(self):
        self.assertEqual(
            resolve_document_url("https://www.cclerk.hctx.net/Applications/WebSearch/ViewECdocs.aspx?ID=abc123"),
            "https://www.cclerk.hctx.net/Applications/WebSearch/ViewECdocs.aspx?ID=abc123",
        )

    def test_repairs_missing_slash_after_domain(self):
        self.assertEqual(
            resolve_document_url("https://www.cclerk.hctx.netviewecdocs.aspx/?ID=abc123"),
            "https://www.cclerk.hctx.net/applications/websearch/viewecdocs.aspx/?ID=abc123",
        )

    def test_resolves_raw_href_seen_in_checkpoint(self):
        self.assertEqual(
            resolve_document_url("ViewECdocs.aspx?ID=checkpoint-sample"),
            "https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx?ID=checkpoint-sample",
        )

    def test_legacy_script_reexports_resolver(self):
        self.assertIs(legacy_scraper.resolve_document_url, resolve_document_url)


if __name__ == "__main__":
    unittest.main()
