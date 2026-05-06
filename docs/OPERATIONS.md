# Operations

This runbook covers the Harris County monthly runner. Other counties are not implemented yet.

## One-Time Setup

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements_free.txt
python -m playwright install chromium
```

Optional OCR fallback for scanned/image-based PDFs also needs local system packages:

```bash
brew install tesseract
brew install poppler
```

Without those packages, `--ocr` will fail records gracefully with an OCR unavailable reason instead of crashing the full run.

## Safe Smoke Test

Use a limit before running a larger scrape:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 1
```

Expected result:

- CLI prints `Processed`, `Skipped`, and `Failed`.
- CSV appears under `data/outputs/`.
- JSONL appears under `data/outputs/`.
- Checkpoint appears under `data/checkpoints/`.
- Failed-record summary appears under `data/failed/`.
- PDF appears under `data/pdfs/harris/YYYY/MM/`.
- Extracted text appears under `data/text_cache/harris/YYYY/MM/`.

If you repeat a smoke test after one record is already completed, use resume:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 1 --resume
```

With `--limit 1 --resume`, the runner considers only the first record from the monthly list. If that record is already complete, it is skipped and no later record is processed.

## Dry Run

Preview a run without changing output state:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --limit 10 --dry-run
```

Dry-run mode plans records after applying `--limit`, `--resume`, or `--retry-failed`, then stops. It does not download PDFs, extract text, parse foreclosure text, write CSV/JSONL rows, mark records completed, or mutate checkpoint/failed-record state.

## OCR Fallback

Many Harris PDFs are scanned/image-based. The default monthly runner keeps OCR off unless explicitly requested. If pdfplumber extracts blank or tiny text and `--ocr` is not used, the record is failed with a clear unusable-text reason.

Use OCR for a limited smoke test first:

```bash
python -m scraper.main --county harris --year 2026 --month 6 --limit 1 --ocr
```

OCR is slower than the default pdfplumber path and requires Tesseract plus Poppler installed locally.
If a usable text cache already exists, normal runs reuse it. Use reprocess-existing mode when you intentionally want to rebuild text from local PDFs.

## Reprocess Existing Local PDFs

To rebuild text cache and CSV/JSONL rows from already-downloaded monthly PDFs without contacting the Harris site:

```bash
python -m scraper.main --county harris --year 2026 --month 6 --limit 1 --reprocess-existing --ocr
```

This mode reads PDFs from:

```text
data/pdfs/harris/YYYY/MM/
```

It writes rebuilt text under:

```text
data/text_cache/harris/YYYY/MM/
```

Use `--limit 1` first. Do not use this mode as a workaround for missing PDFs; it will not redownload anything.

## Monthly Run

Run the month with no limit:

```bash
python -m scraper.main --county harris --year 2026 --month 5
```

Use respectful operation. Do not add uncontrolled concurrency or aggressive retry loops. The runner is designed for repeatability and local caching, not maximum speed.

## Resume

Resume after interruption:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --resume
```

Resume uses the monthly checkpoint:

```text
data/checkpoints/harris_2026_05_checkpoint.json
```

Completed IDs are skipped. Failed IDs are not retried by normal resume; use `--retry-failed` for those.

## Retry Failed Records

Retry only records currently listed as failed:

```bash
python -m scraper.main --county harris --year 2026 --month 5 --retry-failed
```

Failed records and reasons are tracked in:

```text
data/failed/harris_2026_05_failed.json
```

The checkpoint also keeps `failed_ids` and `failed_reasons`.

## Local Streamlit App

Run the local app from the repo root:

```bash
streamlit run app/streamlit_app.py
```

The app is a simple wrapper around the Harris monthly runner. It provides controls for county, year, month, limit, resume, retry-failed mode, dry-run mode, OCR fallback, and local reprocess mode. It displays processed/skipped/failed counts, output paths, extraction methods, the CSV preview, a CSV download button, and failed records when available.

Keep the default limit of `1` for smoke testing. Do not use the app to run a full month repeatedly.

If the limit is disabled, the app requires explicit confirmation before the run button can execute. The app also has a dry-run checkbox for previewing planned records without changing output state.

## Inspect Outputs

Count CSV rows:

```bash
python -c "import csv; rows=list(csv.DictReader(open('data/outputs/harris_2026_05_foreclosures.csv'))); print(len(rows))"
```

Count JSONL rows:

```bash
python -c "from pathlib import Path; p=Path('data/outputs/harris_2026_05_foreclosures.jsonl'); print(sum(1 for line in p.open() if line.strip()))"
```

Inspect checkpoint counts:

```bash
python -c "import json; d=json.load(open('data/checkpoints/harris_2026_05_checkpoint.json')); print('all', len(d.get('all_records', [])), 'completed', len(d.get('completed_ids', [])), 'failed', len(d.get('failed_ids', [])))"
```

Inspect failed records:

```bash
python -c "import json; d=json.load(open('data/failed/harris_2026_05_failed.json')); print(d)"
```

List cached PDFs:

```bash
find data/pdfs/harris/2026/05 -type f -name '*.pdf' | sort
```

List cached extracted text:

```bash
find data/text_cache/harris/2026/05 -type f -name '*.txt' | sort
```

## What Not To Commit

Do not commit local generated outputs unless there has been an explicit public-release review:

- `data/`
- `foreclosure_pdfs/`
- `harris_foreclosures_*.csv`
- `scrape_checkpoint.json`
- `scraper.log`
- `baseline/golden_samples/harris/pdf_samples/`
- `baseline/golden_samples/harris/ocr_text_samples/`
- `baseline/golden_samples/harris/current_outputs/`
- `baseline/golden_samples/harris/expected_outputs/`
- generated Harris golden sample notes
- generated Harris sample records metadata
- local diagnostics under `data/local/`

This is a public repo. Public-record artifacts can still include names, addresses, loan amounts, document IDs, and other sensitive details. Review before pushing anything generated from a live run.

## Legacy Script

The legacy script remains available:

```bash
python harris_foreclosure_scraper_free.py
```

It is preserved for compatibility and should not be deleted while the monthly runner is being stabilized.
