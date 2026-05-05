import unittest

from harris_foreclosure_scraper_free import resolve_document_url


class HarrisUrlResolutionTest(unittest.TestCase):
    def test_resolves_relative_viewecdocs_url(self):
        self.assertEqual(
            resolve_document_url("viewecdocs.aspx/?ID=abc123"),
            "https://www.cclerk.hctx.net/viewecdocs.aspx/?ID=abc123",
        )

    def test_resolves_root_relative_url(self):
        self.assertEqual(
            resolve_document_url("/Applications/WebSearch/ViewECdocs.aspx?ID=abc123"),
            "https://www.cclerk.hctx.net/Applications/WebSearch/ViewECdocs.aspx?ID=abc123",
        )

    def test_repairs_missing_slash_after_domain(self):
        self.assertEqual(
            resolve_document_url("https://www.cclerk.hctx.netviewecdocs.aspx/?ID=abc123"),
            "https://www.cclerk.hctx.net/viewecdocs.aspx/?ID=abc123",
        )


if __name__ == "__main__":
    unittest.main()
