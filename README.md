# crapper

Foreclosure scraper tooling for county public-record foreclosure notices.

Current status: Harris County, Texas only. Other counties are planned as a future architecture direction, but they are not implemented yet. A Streamlit or simple app wrapper is also future work; app code should call the scraper runner instead of duplicating scraper logic.

## What This Does

The current Harris workflow can:

- Search the Harris County Clerk foreclosure records for a sale month.
- Download foreclosure notice PDFs.
- Extract text from PDFs.
- Parse the current Harris fields.
- Write monthly CSV and JSONL outputs.
- Save monthly PDFs and extracted text cache.
- Maintain checkpoint, resume, and retry-failed state.

The legacy script is still available and runnable, but the preferred repeatable workflow is the monthly runner.

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install Python dependencies:

```bash
python -m pip install -r requirements_free.txt
```

Install Playwright Chromium:

```bash
python -m playwright install chromium
```

## Legacy Script

The legacy script is preserved for compatibility:

```bash
python harris_foreclosure_scraper_free.py
```

It is hardcoded for the legacy May 2026 Harris workflow and writes legacy root-level outputs such as:

- `harris_foreclosures_may2026.csv`
- `scrape_checkpoint.json`
- `scraper.log`
- `foreclosure_pdfs/`

## Monthly Runner

Preferred command shape:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 10
```

Only `--county harris` is supported right now.

Safe limit-1 smoke test:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 1
```

Resume an interrupted or already-started monthly run:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --resume
```

Resume while considering only the first record in the monthly list:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 1 --resume
```

Retry failed records:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --retry-failed
```

The `--limit` flag is a safety cap on records considered from the start of the monthly record list. With `--resume`, already-completed records inside that considered set are skipped rather than replaced by later records.

## Monthly Outputs

The monthly runner writes:

- `data/outputs/harris_YYYY_MM_foreclosures.csv`
- `data/outputs/harris_YYYY_MM_foreclosures.jsonl`
- `data/checkpoints/harris_YYYY_MM_checkpoint.json`
- `data/failed/harris_YYYY_MM_failed.json`
- `data/text_cache/harris/YYYY/MM/`
- `data/pdfs/harris/YYYY/MM/`

For May 2026, that means:

- `data/outputs/harris_2026_05_foreclosures.csv`
- `data/outputs/harris_2026_05_foreclosures.jsonl`
- `data/checkpoints/harris_2026_05_checkpoint.json`
- `data/failed/harris_2026_05_failed.json`
- `data/text_cache/harris/2026/05/`
- `data/pdfs/harris/2026/05/`

## Tests

Run the unit test suite:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p 'test*.py'
```

The unit tests must not call live Harris County servers. Live checks should be explicit and limited, usually with `--limit 1`.

## Generated Files Warning

Generated scraper data is local and ignored by Git where appropriate. Do not commit generated PDFs, extracted OCR/text, live CSV outputs, logs, checkpoints, failed-record files, or local diagnostics without manual review.

This repository is public. Even when source documents are public records, review any sample PDFs, extracted text, notes, expected outputs, and metadata before pushing. Public-record data can still contain names, addresses, loan details, and other sensitive personal or property information.

See also:

- [Operations runbook](docs/OPERATIONS.md)
- [Development notes](docs/DEVELOPMENT.md)
- [T0 helper guide](README_T0_HELPER.md)
