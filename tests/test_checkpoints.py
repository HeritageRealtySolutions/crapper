import json
import tempfile
import unittest
from pathlib import Path

from scraper.core.checkpoints import (
    atomic_write_json,
    clear_failed,
    get_completed_ids,
    get_failed_ids,
    load_checkpoint,
    mark_completed,
    mark_failed,
    save_checkpoint,
)


class CheckpointHelpersTest(unittest.TestCase):
    def test_missing_checkpoint_loads_safely(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = load_checkpoint(Path(tmp) / "missing.json")

        self.assertEqual(checkpoint["all_records"], [])
        self.assertEqual(checkpoint["completed_ids"], [])
        self.assertEqual(checkpoint["failed_ids"], [])
        self.assertEqual(checkpoint["failed_reasons"], {})

    def test_completed_ids_are_tracked_without_duplicates(self):
        checkpoint = {}

        mark_completed(checkpoint, "FRCL-1")
        mark_completed(checkpoint, "FRCL-1")

        self.assertEqual(checkpoint["completed_ids"], ["FRCL-1"])
        self.assertEqual(get_completed_ids(checkpoint), {"FRCL-1"})

    def test_failed_ids_and_reasons_are_tracked(self):
        checkpoint = {}

        mark_failed(checkpoint, "FRCL-1", "PDF download failed")

        self.assertEqual(checkpoint["failed_ids"], ["FRCL-1"])
        self.assertEqual(get_failed_ids(checkpoint), {"FRCL-1"})
        self.assertEqual(checkpoint["failed_reasons"]["FRCL-1"], "PDF download failed")

    def test_retry_success_clears_failed_id(self):
        checkpoint = {}

        mark_failed(checkpoint, "FRCL-1", "OCR failed")
        mark_completed(checkpoint, "FRCL-1")

        self.assertEqual(checkpoint["completed_ids"], ["FRCL-1"])
        self.assertEqual(checkpoint["failed_ids"], [])
        self.assertNotIn("FRCL-1", checkpoint["failed_reasons"])

    def test_clear_failed_removes_failed_id_and_reason(self):
        checkpoint = {}

        mark_failed(checkpoint, "FRCL-1", "PDF download failed")
        clear_failed(checkpoint, "FRCL-1")

        self.assertEqual(checkpoint["failed_ids"], [])
        self.assertEqual(checkpoint["failed_reasons"], {})

    def test_atomic_save_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "checkpoint.json"
            data = {
                "all_records": [{"doc_id": "FRCL-1"}],
                "completed_ids": ["FRCL-1", "FRCL-1"],
                "failed_ids": [],
            }

            save_checkpoint(path, data)

            loaded = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(loaded["all_records"], [{"doc_id": "FRCL-1"}])
            self.assertEqual(loaded["completed_ids"], ["FRCL-1"])
            self.assertEqual(loaded["failed_ids"], [])
            self.assertEqual(loaded["failed_reasons"], {})

    def test_atomic_write_json_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "checkpoint.json"

            atomic_write_json(path, {"completed_ids": ["FRCL-1"]})

            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                {"completed_ids": ["FRCL-1"]},
            )


if __name__ == "__main__":
    unittest.main()
