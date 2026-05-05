import tempfile
import unittest
from pathlib import Path

from scraper.core.pdfs import (
    ensure_dir,
    load_pdf_bytes,
    load_text,
    pdf_exists,
    pdf_path_for,
    save_pdf_bytes,
    save_text,
    safe_doc_id,
    text_exists,
    text_path_for,
)


class PdfCacheHelpersTest(unittest.TestCase):
    def test_ensure_dir_creates_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "nested" / "pdfs"

            returned = ensure_dir(directory)

            self.assertEqual(returned, directory)
            self.assertTrue(directory.is_dir())

    def test_pdf_bytes_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdf_dir = Path(tmp) / "pdfs"
            pdf_bytes = b"%PDF-test-bytes"

            path = save_pdf_bytes(pdf_dir, "FRCL-2026-12345", pdf_bytes)

            self.assertEqual(path, pdf_dir / "FRCL-2026-12345.pdf")
            self.assertTrue(pdf_exists(pdf_dir, "FRCL-2026-12345"))
            self.assertEqual(load_pdf_bytes(pdf_dir, "FRCL-2026-12345"), pdf_bytes)

    def test_text_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            text_dir = Path(tmp) / "text"

            path = save_text(text_dir, "FRCL-2026-12345", "raw extracted text")

            self.assertEqual(path, text_dir / "FRCL-2026-12345.txt")
            self.assertTrue(text_exists(text_dir, "FRCL-2026-12345"))
            self.assertEqual(load_text(text_dir, "FRCL-2026-12345"), "raw extracted text")

    def test_missing_files_return_safe_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)

            self.assertFalse(pdf_exists(cache_dir, "FRCL-2026-MISSING"))
            self.assertFalse(text_exists(cache_dir, "FRCL-2026-MISSING"))
            self.assertIsNone(load_pdf_bytes(cache_dir, "FRCL-2026-MISSING"))
            self.assertIsNone(load_text(cache_dir, "FRCL-2026-MISSING"))

    def test_unsafe_filename_characters_are_sanitized(self):
        doc_id = "FRCL-2026/12:345?"

        self.assertEqual(safe_doc_id(doc_id), "FRCL-2026_12_345_")
        self.assertEqual(
            pdf_path_for("pdfs", doc_id),
            Path("pdfs") / "FRCL-2026_12_345_.pdf",
        )
        self.assertEqual(
            text_path_for("text", doc_id),
            Path("text") / "FRCL-2026_12_345_.txt",
        )


if __name__ == "__main__":
    unittest.main()
