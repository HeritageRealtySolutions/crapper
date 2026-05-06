import unittest
from unittest.mock import Mock, patch

from scraper.core.text_extraction import (
    OCR_UNAVAILABLE_REASON,
    TextExtractionResult,
    extract_text_with_fallback,
    extract_text_with_ocr,
    is_usable_text,
)


USABLE_TEXT = "Grantor(s): Jane Borrower\n" * 4


class TextExtractionTest(unittest.TestCase):
    def test_is_usable_text(self):
        self.assertFalse(is_usable_text(None))
        self.assertFalse(is_usable_text(""))
        self.assertFalse(is_usable_text("short"))
        self.assertTrue(is_usable_text(USABLE_TEXT))

    def test_fallback_uses_pdfplumber_text_when_usable(self):
        ocr = Mock()
        with patch("scraper.core.text_extraction.extract_text_pdfplumber", return_value=USABLE_TEXT):
            with patch("scraper.core.text_extraction.extract_text_with_ocr", ocr):
                result = extract_text_with_fallback(b"%PDF", use_ocr=True)

        self.assertEqual(result.text, USABLE_TEXT)
        self.assertEqual(result.extraction_method, "pdfplumber")
        self.assertFalse(result.used_ocr)
        ocr.assert_not_called()

    def test_blank_pdfplumber_text_triggers_ocr_when_enabled(self):
        ocr_result = TextExtractionResult(
            text=USABLE_TEXT,
            extraction_method="ocr",
            text_length=len(USABLE_TEXT),
            used_ocr=True,
            extraction_notes="OK",
        )

        with patch("scraper.core.text_extraction.extract_text_pdfplumber", return_value="\n\n"):
            with patch("scraper.core.text_extraction.extract_text_with_ocr", return_value=ocr_result) as ocr:
                result = extract_text_with_fallback(b"%PDF", use_ocr=True)

        self.assertEqual(result.text, USABLE_TEXT)
        self.assertEqual(result.extraction_method, "ocr")
        self.assertTrue(result.used_ocr)
        ocr.assert_called_once_with(b"%PDF")

    def test_blank_pdfplumber_text_does_not_trigger_ocr_when_disabled(self):
        ocr = Mock()
        with patch("scraper.core.text_extraction.extract_text_pdfplumber", return_value="\n\n"):
            with patch("scraper.core.text_extraction.extract_text_with_ocr", ocr):
                result = extract_text_with_fallback(b"%PDF", use_ocr=False)

        self.assertEqual(result.extraction_method, "pdfplumber")
        self.assertFalse(result.used_ocr)
        self.assertIn("no usable text", result.extraction_notes)
        ocr.assert_not_called()

    def test_ocr_unavailable_returns_clear_failure(self):
        result = extract_text_with_ocr(b"not a real pdf")

        if result.text:
            self.assertTrue(is_usable_text(result.text))
        else:
            self.assertTrue(result.extraction_notes.startswith(OCR_UNAVAILABLE_REASON))


if __name__ == "__main__":
    unittest.main()
