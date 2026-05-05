"""Reusable PDF and extracted-text cache helpers."""

from __future__ import annotations

import re
from pathlib import Path


def ensure_dir(path) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def safe_doc_id(doc_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", doc_id.strip())


def pdf_path_for(pdf_dir, doc_id: str) -> Path:
    return Path(pdf_dir) / f"{safe_doc_id(doc_id)}.pdf"


def text_path_for(text_dir, doc_id: str) -> Path:
    return Path(text_dir) / f"{safe_doc_id(doc_id)}.txt"


def save_pdf_bytes(pdf_dir, doc_id: str, pdf_bytes: bytes) -> Path:
    ensure_dir(pdf_dir)
    path = pdf_path_for(pdf_dir, doc_id)
    path.write_bytes(pdf_bytes)
    return path


def load_pdf_bytes(pdf_dir, doc_id: str) -> bytes | None:
    path = pdf_path_for(pdf_dir, doc_id)
    if not path.exists():
        return None
    return path.read_bytes()


def pdf_exists(pdf_dir, doc_id: str) -> bool:
    return pdf_path_for(pdf_dir, doc_id).exists()


def save_text(text_dir, doc_id: str, text: str) -> Path:
    ensure_dir(text_dir)
    path = text_path_for(text_dir, doc_id)
    path.write_text(text, encoding="utf-8")
    return path


def load_text(text_dir, doc_id: str) -> str | None:
    path = text_path_for(text_dir, doc_id)
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def text_exists(text_dir, doc_id: str) -> bool:
    return text_path_for(text_dir, doc_id).exists()
