"""PDF text extraction helpers with optional OCR fallback."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import pdfplumber


DEFAULT_MIN_USABLE_TEXT_CHARS = 50
UNUSABLE_TEXT_REASON = "PDF text extraction produced no usable text"
OCR_UNAVAILABLE_REASON = "OCR unavailable: install tesseract and required Python packages."
OCR_DPI = 300


@dataclass(frozen=True)
class TextExtractionResult:
    text: str
    extraction_method: str
    text_length: int
    used_ocr: bool
    extraction_notes: str


def is_usable_text(text: str | None, min_chars: int = DEFAULT_MIN_USABLE_TEXT_CHARS) -> bool:
    return bool(text and len(text.strip()) >= min_chars)


def build_text_result(
    text: str | None,
    *,
    extraction_method: str,
    used_ocr: bool,
    extraction_notes: str,
) -> TextExtractionResult:
    safe_text = text or ""
    return TextExtractionResult(
        text=safe_text,
        extraction_method=extraction_method,
        text_length=len(safe_text),
        used_ocr=used_ocr,
        extraction_notes=extraction_notes,
    )


def extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def extract_text_with_ocr(pdf_bytes: bytes) -> TextExtractionResult:
    try:
        from pdf2image import convert_from_bytes
        import pytesseract
    except ImportError as e:
        return build_text_result(
            "",
            extraction_method="ocr_unavailable",
            used_ocr=False,
            extraction_notes=f"{OCR_UNAVAILABLE_REASON} Missing Python dependency: {e.name}.",
        )

    try:
        images = convert_from_bytes(pdf_bytes, dpi=OCR_DPI)
        text = "\n\n".join(pytesseract.image_to_string(image) for image in images)
    except Exception as e:
        return build_text_result(
            "",
            extraction_method="ocr_unavailable",
            used_ocr=False,
            extraction_notes=f"{OCR_UNAVAILABLE_REASON} {e}",
        )

    notes = "OK" if is_usable_text(text) else "OCR produced no usable text"
    return build_text_result(
        text,
        extraction_method="ocr",
        used_ocr=True,
        extraction_notes=notes,
    )


def extract_text_with_fallback(pdf_bytes: bytes, use_ocr: bool = True) -> TextExtractionResult:
    try:
        pdfplumber_text = extract_text_pdfplumber(pdf_bytes)
    except Exception as e:
        pdfplumber_text = ""
        pdfplumber_note = f"pdfplumber extraction failed: {e}"
    else:
        pdfplumber_note = "OK" if is_usable_text(pdfplumber_text) else UNUSABLE_TEXT_REASON

    if is_usable_text(pdfplumber_text):
        return build_text_result(
            pdfplumber_text,
            extraction_method="pdfplumber",
            used_ocr=False,
            extraction_notes="OK",
        )

    if not use_ocr:
        return build_text_result(
            pdfplumber_text,
            extraction_method="pdfplumber",
            used_ocr=False,
            extraction_notes=pdfplumber_note,
        )

    ocr_result = extract_text_with_ocr(pdf_bytes)
    if is_usable_text(ocr_result.text):
        return build_text_result(
            ocr_result.text,
            extraction_method="ocr",
            used_ocr=True,
            extraction_notes="pdfplumber text was unusable; OCR fallback succeeded",
        )
    return ocr_result
