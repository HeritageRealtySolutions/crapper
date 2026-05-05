"""App-focused helpers for clearing selected monthly outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from scraper.core.outputs import MonthlyPaths

CLEAR_CONFIRMATION_PHRASE = "RESET"


@dataclass(frozen=True)
class ClearOutputSummary:
    deleted_files: list[str]
    deleted_folders: list[str]
    missing_paths: list[str]


def monthly_output_targets(paths: MonthlyPaths) -> list[Path]:
    return [
        paths.output_csv,
        paths.output_jsonl,
        paths.checkpoint_json,
        paths.failed_json,
        paths.pdfs_dir,
        paths.text_cache_dir,
    ]


def can_clear_outputs(confirm_checked: bool, typed_phrase: str) -> bool:
    return confirm_checked and typed_phrase == CLEAR_CONFIRMATION_PHRASE


def _resolve_target(path: Path, repo_root: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def validate_clear_targets(paths: MonthlyPaths, repo_root: Path | str = ".") -> list[Path]:
    root = Path(repo_root).resolve()
    data_root = (root / "data").resolve()
    targets = [_resolve_target(path, root) for path in monthly_output_targets(paths)]

    for target in targets:
        if target == data_root or not target.is_relative_to(data_root):
            raise ValueError(f"Refusing to clear unsafe path: {_display_path(target, root)}")

    return targets


def clear_monthly_outputs(paths: MonthlyPaths, repo_root: Path | str = ".") -> ClearOutputSummary:
    root = Path(repo_root).resolve()
    targets = validate_clear_targets(paths, root)

    deleted_files = []
    deleted_folders = []
    missing_paths = []

    for target in targets:
        display = _display_path(target, root)
        if not target.exists():
            missing_paths.append(display)
        elif target.is_file() or target.is_symlink():
            target.unlink()
            deleted_files.append(display)
        elif target.is_dir():
            shutil.rmtree(target)
            deleted_folders.append(display)
        else:
            missing_paths.append(display)

    return ClearOutputSummary(
        deleted_files=deleted_files,
        deleted_folders=deleted_folders,
        missing_paths=missing_paths,
    )
