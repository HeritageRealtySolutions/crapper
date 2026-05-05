"""Reusable CSV and JSONL writer helpers."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable


def ensure_parent_dir(path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def write_csv_rows(path, rows: Iterable[dict], fieldnames: list[str]) -> int:
    target = ensure_parent_dir(path)
    rows = list(rows)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def append_csv_row(path, row: dict, fieldnames: list[str]) -> None:
    target = ensure_parent_dir(path)
    should_write_header = not target.exists() or target.stat().st_size == 0

    with target.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if should_write_header:
            writer.writeheader()
        writer.writerow(row)


def read_csv_rows(path) -> list[dict]:
    target = Path(path)
    with target.open(newline="", encoding="utf-8") as csvfile:
        return list(csv.DictReader(csvfile))


def write_jsonl_rows(path, rows: Iterable[dict]) -> int:
    target = ensure_parent_dir(path)
    rows = list(rows)

    with target.open("w", encoding="utf-8") as jsonl_file:
        for row in rows:
            jsonl_file.write(json.dumps(row, ensure_ascii=True) + "\n")

    return len(rows)


def append_jsonl_row(path, row: dict) -> None:
    target = ensure_parent_dir(path)
    with target.open("a", encoding="utf-8") as jsonl_file:
        jsonl_file.write(json.dumps(row, ensure_ascii=True) + "\n")
