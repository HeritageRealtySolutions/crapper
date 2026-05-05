# T0 Harris Golden Sample Helper

This repository includes a T0-only helper:

```text
tools/collect_harris_golden_samples.py
```

The helper organizes 10-20 Harris County golden samples from already-generated local scraper outputs. It does not scrape Harris County, does not download PDFs, does not OCR PDFs, does not parse documents, and does not modify scraper behavior.

## Purpose

Use this before parser tests or refactoring to preserve the current behavior of `harris_foreclosure_scraper_free.py`.

Golden samples let us compare future parser and runner output against the current unrefactored scraper output. This is a behavior baseline, not a claim that the current parser is correct.

## Expected Inputs

You should already have local outputs from the current scraper process:

```text
harris_foreclosures_may2026.csv
foreclosure_pdfs/
```

Optional inputs:

```text
ocr text folder, if you already generated raw OCR text
records JSON, if you already captured search-result metadata
```

## Example

```bash
python3 tools/collect_harris_golden_samples.py \
  --year 2026 \
  --month 5 \
  --current-csv harris_foreclosures_may2026.csv \
  --pdf-dir foreclosure_pdfs \
  --doc-ids FRCL-2026-12345,FRCL-2026-67890
```

With OCR text and records metadata:

```bash
python3 tools/collect_harris_golden_samples.py \
  --year 2026 \
  --month 5 \
  --current-csv harris_foreclosures_may2026.csv \
  --pdf-dir foreclosure_pdfs \
  --ocr-dir current_ocr_text \
  --records-json current_records.json \
  --doc-ids FRCL-2026-12345,FRCL-2026-67890
```

## What It Creates

The helper creates these folders if needed:

```text
baseline/golden_samples/harris/pdf_samples/
baseline/golden_samples/harris/ocr_text_samples/
baseline/golden_samples/harris/current_outputs/
baseline/golden_samples/harris/expected_outputs/
baseline/golden_samples/harris/notes/
tests/fixtures/harris/ocr_text/
tests/fixtures/harris/expected/
tests/fixtures/harris/records/
```

It writes:

```text
baseline/golden_samples/harris/current_outputs/harris_YYYY_MM_current_selected.csv
baseline/golden_samples/harris/expected_outputs/harris_YYYY_MM_expected.csv
baseline/golden_samples/harris/expected_outputs/harris_YYYY_MM_expected.jsonl
tests/fixtures/harris/expected/harris_YYYY_MM_expected.csv
tests/fixtures/harris/expected/harris_YYYY_MM_expected.jsonl
tests/fixtures/harris/records/harris_YYYY_MM_sample_records.json
baseline/golden_samples/harris/notes/<doc_id>.md
baseline/golden_samples/harris/notes/collection_summary_YYYY_MM.md
```

When OCR text is missing, it creates a clear `*.ocr_text_needed.txt` placeholder. That placeholder is not OCR output and must not be treated as a parser fixture.

## Public Repo Warning

Review before pushing any real generated/sample artifacts to GitHub.

Real PDFs, OCR text, current outputs, logs, notes, expected outputs, and public-record samples may contain names, addresses, loan details, document links, or other public-record data. The `.gitignore` is configured to keep the highest-risk generated artifacts out of Git by default, but you are still responsible for reviewing anything staged before committing.

Do not commit:

- real PDFs unless intentionally approved
- raw OCR text unless intentionally approved
- generated CSV/JSONL outputs unless intentionally approved
- logs
- `.env` files
- credentials, cookies, tokens, or secrets

## Current Boundaries

- Harris County only.
- T0 sample collection only.
- No parser changes.
- No regex changes.
- No CSV field changes.
- No Playwright/browser changes.
- No live scrape is performed by this helper.
- No monthly runner or app logic is included here.
