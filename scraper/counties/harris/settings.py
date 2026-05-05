"""Harris County scraper settings used by the legacy runner."""

from pathlib import Path
from urllib.parse import urljoin

BASE_URL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
SEARCH_URL = BASE_URL
SITE_BASE_URL = "https://www.cclerk.hctx.net/"
WEBSEARCH_BASE_URL = urljoin(BASE_URL, "./")

SALE_YEAR = "2026"
SALE_MONTH = "May"

OUTPUT_CSV = "harris_foreclosures_may2026.csv"
CHECKPOINT_FILE = "scrape_checkpoint.json"
PDFS_DIR = Path("foreclosure_pdfs")
LOG_FILE = "scraper.log"

DELAY_BETWEEN_RECORDS = 2.0
MAX_RETRIES = 3
HEADLESS = True

DOCUMENT_URL_LOG_LIMIT = 5
DOWNLOAD_DIAGNOSTICS = True
DOWNLOAD_DIAGNOSTIC_SNAPSHOT_LIMIT = 1
DOWNLOAD_DIAGNOSTICS_DIR = Path("data/local/diagnostics")
