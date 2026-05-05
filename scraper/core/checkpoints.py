"""Reusable checkpoint helpers for scraper runs."""

from __future__ import annotations

import json
from pathlib import Path


DEFAULT_CHECKPOINT = {
    "all_records": [],
    "completed_ids": [],
    "failed_ids": [],
    "failed_reasons": {},
}


def new_checkpoint() -> dict:
    return {
        "all_records": [],
        "completed_ids": [],
        "failed_ids": [],
        "failed_reasons": {},
    }


def normalize_checkpoint(checkpoint: dict | None) -> dict:
    normalized = new_checkpoint()
    if not checkpoint:
        return normalized

    normalized["all_records"] = checkpoint.get("all_records", [])
    normalized["completed_ids"] = list(dict.fromkeys(checkpoint.get("completed_ids", [])))
    normalized["failed_ids"] = list(dict.fromkeys(checkpoint.get("failed_ids", [])))
    normalized["failed_reasons"] = checkpoint.get("failed_reasons", {})
    return normalized


def load_checkpoint(path) -> dict:
    checkpoint_path = Path(path)
    if not checkpoint_path.exists():
        return new_checkpoint()

    with checkpoint_path.open(encoding="utf-8") as checkpoint_file:
        return normalize_checkpoint(json.load(checkpoint_file))


def atomic_write_json(path, data) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f".{target.name}.tmp")

    with temp_path.open("w", encoding="utf-8") as temp_file:
        json.dump(data, temp_file, indent=2)
        temp_file.write("\n")

    temp_path.replace(target)


def save_checkpoint(path, data) -> None:
    atomic_write_json(path, normalize_checkpoint(data))


def get_completed_ids(checkpoint) -> set[str]:
    return set(normalize_checkpoint(checkpoint)["completed_ids"])


def get_failed_ids(checkpoint) -> set[str]:
    return set(normalize_checkpoint(checkpoint)["failed_ids"])


def _append_unique(values: list, value) -> None:
    if value not in values:
        values.append(value)


def mark_completed(checkpoint: dict, doc_id: str) -> dict:
    normalized = normalize_checkpoint(checkpoint)
    _append_unique(normalized["completed_ids"], doc_id)
    clear_failed(normalized, doc_id)
    checkpoint.clear()
    checkpoint.update(normalized)
    return checkpoint


def mark_failed(checkpoint: dict, doc_id: str, reason: str) -> dict:
    normalized = normalize_checkpoint(checkpoint)
    _append_unique(normalized["failed_ids"], doc_id)
    normalized["failed_reasons"][doc_id] = reason
    checkpoint.clear()
    checkpoint.update(normalized)
    return checkpoint


def clear_failed(checkpoint: dict, doc_id: str) -> dict:
    normalized = normalize_checkpoint(checkpoint)
    normalized["failed_ids"] = [
        failed_doc_id for failed_doc_id in normalized["failed_ids"]
        if failed_doc_id != doc_id
    ]
    normalized["failed_reasons"].pop(doc_id, None)
    checkpoint.clear()
    checkpoint.update(normalized)
    return checkpoint
