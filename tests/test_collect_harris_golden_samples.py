import importlib.util
import tempfile
import unittest
from pathlib import Path


HELPER_PATH = Path(__file__).parents[1] / "tools" / "collect_harris_golden_samples.py"
SPEC = importlib.util.spec_from_file_location("collect_harris_golden_samples", HELPER_PATH)
collect_helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collect_helper)


class CollectHarrisGoldenSamplesTest(unittest.TestCase):
    def test_copy_file_if_needed_skips_same_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "sample.txt"
            source.write_text("raw text", encoding="utf-8")

            copied = collect_helper.copy_file_if_needed(source, source)

            self.assertFalse(copied)
            self.assertEqual(source.read_text(encoding="utf-8"), "raw text")

    def test_copy_file_if_needed_copies_different_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source.txt"
            target = Path(tmp) / "target.txt"
            source.write_text("raw text", encoding="utf-8")

            copied = collect_helper.copy_file_if_needed(source, target)

            self.assertTrue(copied)
            self.assertEqual(target.read_text(encoding="utf-8"), "raw text")


if __name__ == "__main__":
    unittest.main()
