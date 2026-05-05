import tempfile
import unittest
from pathlib import Path

from app.output_management import (
    can_clear_outputs,
    clear_monthly_outputs,
    monthly_output_targets,
    validate_clear_targets,
)
from scraper.core.outputs import MonthlyPaths, build_monthly_paths


class AppOutputManagementTest(unittest.TestCase):
    def test_monthly_output_targets_for_harris_2026_05_are_exact(self):
        paths = build_monthly_paths("harris", 2026, 5)

        self.assertEqual(
            monthly_output_targets(paths),
            [
                Path("data/outputs/harris_2026_05_foreclosures.csv"),
                Path("data/outputs/harris_2026_05_foreclosures.jsonl"),
                Path("data/checkpoints/harris_2026_05_checkpoint.json"),
                Path("data/failed/harris_2026_05_failed.json"),
                Path("data/pdfs/harris/2026/05"),
                Path("data/text_cache/harris/2026/05"),
            ],
        )

    def test_can_clear_outputs_requires_checkbox_and_reset_phrase(self):
        self.assertTrue(can_clear_outputs(True, "RESET"))
        self.assertFalse(can_clear_outputs(False, "RESET"))
        self.assertFalse(can_clear_outputs(True, "reset"))

    def test_validate_clear_targets_refuses_paths_outside_data(self):
        unsafe = MonthlyPaths(
            output_csv=Path("README.md"),
            output_jsonl=Path("data/outputs/harris_2026_05_foreclosures.jsonl"),
            checkpoint_json=Path("data/checkpoints/harris_2026_05_checkpoint.json"),
            failed_json=Path("data/failed/harris_2026_05_failed.json"),
            text_cache_dir=Path("data/text_cache/harris/2026/05"),
            pdfs_dir=Path("data/pdfs/harris/2026/05"),
        )

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                validate_clear_targets(unsafe, repo_root=Path(tmp))

    def test_clear_monthly_outputs_handles_missing_paths_safely(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_monthly_paths("harris", 2026, 5)

            summary = clear_monthly_outputs(paths, repo_root=Path(tmp))

            self.assertEqual(summary.deleted_files, [])
            self.assertEqual(summary.deleted_folders, [])
            self.assertEqual(
                summary.missing_paths,
                [
                    "data/outputs/harris_2026_05_foreclosures.csv",
                    "data/outputs/harris_2026_05_foreclosures.jsonl",
                    "data/checkpoints/harris_2026_05_checkpoint.json",
                    "data/failed/harris_2026_05_failed.json",
                    "data/pdfs/harris/2026/05",
                    "data/text_cache/harris/2026/05",
                ],
            )

    def test_clear_monthly_outputs_deletes_only_selected_monthly_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = build_monthly_paths("harris", 2026, 5)
            keep_file = root / "data" / "outputs" / "keep.txt"
            keep_file.parent.mkdir(parents=True)
            keep_file.write_text("keep", encoding="utf-8")

            for file_path in [
                root / paths.output_csv,
                root / paths.output_jsonl,
                root / paths.checkpoint_json,
                root / paths.failed_json,
            ]:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text("delete", encoding="utf-8")

            for folder_path in [
                root / paths.pdfs_dir,
                root / paths.text_cache_dir,
            ]:
                folder_path.mkdir(parents=True, exist_ok=True)
                (folder_path / "sample.txt").write_text("delete", encoding="utf-8")

            summary = clear_monthly_outputs(paths, repo_root=root)

            self.assertEqual(len(summary.deleted_files), 4)
            self.assertEqual(len(summary.deleted_folders), 2)
            self.assertTrue(keep_file.exists())
            self.assertFalse((root / paths.output_csv).exists())
            self.assertFalse((root / paths.pdfs_dir).exists())


if __name__ == "__main__":
    unittest.main()
