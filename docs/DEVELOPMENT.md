# Development

This project currently supports Harris County foreclosure notices only. Other counties are future work. A Streamlit or app layer is also future work and should call the scraper runner rather than duplicate scraper logic.

## Architecture Overview

The code is being moved from a one-off script toward a reusable monthly runner while preserving legacy behavior.

Key files:

- `harris_foreclosure_scraper_free.py` - legacy script, still runnable.
- `scraper/main.py` - CLI entry point for monthly runs.
- `scraper/counties/harris/runner.py` - Harris monthly runner orchestration.
- `scraper/counties/harris/client.py` - Harris browser/search/download behavior.
- `scraper/counties/harris/parser.py` - Harris parser and `CSV_FIELDS`.
- `scraper/counties/harris/settings.py` - Harris URLs, legacy paths, and runtime settings.
- `scraper/core/outputs.py` - monthly output path construction.
- `scraper/core/checkpoints.py` - checkpoint and failed-ID helpers.
- `scraper/core/writers.py` - CSV and JSONL writers.
- `scraper/core/pdfs.py` - PDF and text cache helpers.
- `tools/collect_harris_golden_samples.py` - helper for organizing local golden samples.
- `tools/extract_harris_pdf_text_fixtures.py` - helper for extracting local PDF text fixtures.

## Module Responsibilities

`scraper/main.py`

- Parses CLI arguments.
- Enforces `county=harris` for now.
- Calls the Harris monthly runner.
- Prints processed, skipped, failed, and output paths.

`scraper/counties/harris/runner.py`

- Builds monthly paths.
- Loads and saves checkpoints.
- Coordinates Harris search, PDF download, text extraction, parsing, CSV/JSONL writing, PDF cache, and text cache.
- Applies `--limit`, `--resume`, and `--retry-failed` behavior.
- Does not contain parser regex logic.

`scraper/counties/harris/client.py`

- Owns Harris-specific Playwright browser behavior.
- Selects sale year/month.
- Collects Harris FRCL records.
- Resolves Harris document URLs.
- Downloads PDFs and writes download diagnostics.

`scraper/counties/harris/parser.py`

- Owns the current parser behavior.
- Defines `CSV_FIELDS`.
- Must not be changed casually. Parser behavior should be protected by characterization tests before any parser edit.

`scraper/core/*`

- Contains reusable helpers that are not Harris-specific.
- Should stay independent of county-specific scraping details.

## Testing

Run all tests:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p 'test*.py'
```

Compile key modules:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m py_compile harris_foreclosure_scraper_free.py scraper/main.py scraper/counties/harris/runner.py
```

Unit tests must not call live Harris County servers. Use mocks for runner and CLI behavior. Live smoke checks should be explicit and limited to commands such as:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 1
```

## Parser Characterization Rule

Parser logic is intentionally protected.

Before changing parser regexes or output fields:

1. Add or update golden samples.
2. Add characterization tests that show current behavior.
3. Confirm the current expected output.
4. Make the parser change.
5. Update expected outputs only when the behavior change is deliberate and reviewed.

Do not change `CSV_FIELDS` order unless the downstream output contract is intentionally changed and tests/docs are updated.

## Golden Samples

Golden samples are local examples used to lock down parser behavior before refactoring. They can include PDFs, extracted text, expected CSV/JSONL rows, records metadata, and sample notes.

Important locations:

- `baseline/golden_samples/harris/pdf_samples/`
- `baseline/golden_samples/harris/ocr_text_samples/`
- `baseline/golden_samples/harris/current_outputs/`
- `baseline/golden_samples/harris/expected_outputs/`
- `baseline/golden_samples/harris/notes/`
- `tests/fixtures/harris/ocr_text/`
- `tests/fixtures/harris/expected/`
- `tests/fixtures/harris/records/`

Most real sample artifacts are ignored from Git because this is a public repo. Review any public-record data before pushing.

## Adding Future Counties

Do not add other counties yet. The intended direction is:

- Add a new county package under `scraper/counties/<county>/`.
- Keep county-specific browser and parser logic inside that package.
- Reuse `scraper/core` helpers.
- Add characterization tests before parser changes.
- Keep app/UI code separate from scraper engine logic.

## App Direction

Streamlit or other app work is future work. When it happens, the app should call `scraper.counties.harris.runner.run_harris_monthly()` or a small wrapper around it. It should not reimplement search, download, parse, checkpoint, or output writing logic.
