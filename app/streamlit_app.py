"""Lumen Capital — Intelligence Dashboard.

Full aesthetic redesign: Precision Dark / Amber Data Terminal.
- Fonts: Cormorant Garamond (display) + JetBrains Mono (data/terminal)
- Colors: near-black base, copper/amber accent, high-contrast text
- Layout: data-dense, editorial, purpose-built feel
"""

from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import streamlit as st

from app.output_management import (
    CLEAR_CONFIRMATION_PHRASE,
    can_clear_outputs,
    clear_monthly_outputs,
    monthly_output_targets,
)
from scraper.core.outputs import build_monthly_paths
from scraper.counties.harris.runner import run_harris_monthly

MONTH_OPTIONS = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


# ── THEME ─────────────────────────────────────────────────────────────────────

def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,500;0,600;0,700;1,400;1,600&family=JetBrains+Mono:wght@300;400;500;600&display=swap');

        :root {
            --black:        #07090c;
            --surface:      #0d0f13;
            --surface-2:    #13161c;
            --surface-3:    #191d25;
            --border:       rgba(200, 146, 42, 0.18);
            --border-soft:  rgba(200, 146, 42, 0.08);
            --amber:        #c8922a;
            --amber-bright: #e0a93a;
            --amber-dim:    rgba(200, 146, 42, 0.55);
            --text:         #eceef2;
            --text-2:       #a8adb8;
            --text-3:       #636978;
            --danger:       #c0524a;
            --danger-dim:   rgba(192, 82, 74, 0.14);
            --success:      #5a9e7c;
            --success-dim:  rgba(90, 158, 124, 0.14);
            --mono:         'JetBrains Mono', 'Courier New', monospace;
            --serif:        'Cormorant Garamond', Georgia, serif;
        }

        /* ── Reset & Base ── */
        html, body, [class*="css"] {
            font-family: var(--mono);
        }

        .stApp {
            background-color: var(--black);
            background-image:
                radial-gradient(ellipse 60% 40% at 50% -10%, rgba(200, 146, 42, 0.06), transparent),
                radial-gradient(ellipse 40% 30% at 100% 80%, rgba(200, 146, 42, 0.04), transparent);
            color: var(--text);
        }

        /* Top amber rule */
        .stApp::before {
            content: '';
            display: block;
            position: fixed;
            top: 0; left: 0; right: 0;
            height: 2px;
            background: linear-gradient(90deg, transparent 0%, var(--amber) 30%, var(--amber-bright) 60%, transparent 100%);
            z-index: 9999;
        }

        /* ── Sidebar ── */
        [data-testid="stSidebar"] {
            background-color: var(--surface) !important;
            border-right: 1px solid var(--border) !important;
        }

        [data-testid="stSidebar"] > div:first-child {
            padding-top: 2rem;
        }

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] .stMarkdown {
            color: var(--text) !important;
            font-family: var(--mono) !important;
            font-size: 0.78rem !important;
        }

        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stCheckbox label,
        [data-testid="stSidebar"] .stNumberInput label {
            color: var(--text-2) !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.68rem !important;
        }

        /* Sidebar select, inputs */
        [data-testid="stSidebar"] select,
        [data-testid="stSidebar"] input[type="number"] {
            background: var(--surface-2) !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
            font-family: var(--mono) !important;
            font-size: 0.8rem !important;
            border-radius: 4px !important;
        }

        /* ── Main content area ── */
        .block-container {
            padding-top: 3rem !important;
            padding-left: 2.5rem !important;
            padding-right: 2.5rem !important;
            max-width: 1400px !important;
        }

        /* ── Hero ── */
        .lumen-hero {
            position: relative;
            border: 1px solid var(--border);
            border-left: 3px solid var(--amber);
            border-radius: 0 6px 6px 0;
            padding: 32px 36px 28px;
            margin-bottom: 32px;
            background: linear-gradient(135deg, var(--surface-2) 0%, var(--surface) 100%);
            overflow: hidden;
        }

        .lumen-hero::after {
            content: 'LUMEN INTEL';
            position: absolute;
            right: 36px;
            top: 50%;
            transform: translateY(-50%);
            font-family: var(--mono);
            font-size: 5rem;
            font-weight: 600;
            color: rgba(200, 146, 42, 0.05);
            letter-spacing: 0.3em;
            pointer-events: none;
            white-space: nowrap;
        }

        .hero-eyebrow {
            font-family: var(--mono);
            font-size: 0.68rem;
            font-weight: 500;
            color: var(--amber);
            text-transform: uppercase;
            letter-spacing: 0.15em;
            margin-bottom: 10px;
        }

        .hero-title {
            font-family: var(--serif);
            font-size: 3.2rem;
            font-weight: 600;
            color: var(--text);
            line-height: 1.0;
            margin: 0 0 12px 0;
            letter-spacing: -0.01em;
        }

        .hero-sub {
            font-family: var(--mono);
            font-size: 0.78rem;
            color: var(--text-2);
            line-height: 1.6;
            max-width: 580px;
        }

        .hero-badges {
            display: flex;
            gap: 8px;
            margin-top: 18px;
        }

        .hero-badge {
            font-family: var(--mono);
            font-size: 0.62rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: var(--amber);
            border: 1px solid var(--border);
            border-radius: 2px;
            padding: 3px 8px;
            background: rgba(200, 146, 42, 0.06);
        }

        /* ── Section headings ── */
        .section-block {
            display: flex;
            align-items: baseline;
            gap: 12px;
            margin: 28px 0 10px 0;
        }

        .section-rule {
            flex: 1;
            height: 1px;
            background: var(--border-soft);
        }

        .section-label {
            font-family: var(--mono);
            font-size: 0.62rem;
            font-weight: 600;
            color: var(--amber);
            text-transform: uppercase;
            letter-spacing: 0.14em;
            white-space: nowrap;
        }

        .section-title {
            font-family: var(--serif);
            font-size: 1.35rem;
            font-weight: 600;
            color: var(--text);
            letter-spacing: 0.01em;
            white-space: nowrap;
        }

        /* ── Cards / Containers ── */
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--surface-2) !important;
            border: 1px solid var(--border) !important;
            border-radius: 6px !important;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3) !important;
        }

        /* ── Metrics ── */
        [data-testid="stMetric"] {
            background: var(--surface-3) !important;
            border: 1px solid var(--border-soft) !important;
            border-top: 2px solid var(--amber) !important;
            border-radius: 4px !important;
            padding: 14px 16px 12px !important;
        }

        [data-testid="stMetricLabel"] > div {
            font-family: var(--mono) !important;
            font-size: 0.62rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.1em !important;
            color: var(--text-3) !important;
        }

        [data-testid="stMetricValue"] > div {
            font-family: var(--mono) !important;
            font-size: 1.7rem !important;
            font-weight: 600 !important;
            color: var(--text) !important;
        }

        /* ── Buttons ── */
        div.stButton > button,
        div.stDownloadButton > button {
            font-family: var(--mono) !important;
            font-size: 0.72rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.1em !important;
            border-radius: 4px !important;
            border: 1px solid var(--amber-dim) !important;
            background: linear-gradient(180deg, rgba(200, 146, 42, 0.14) 0%, rgba(200, 146, 42, 0.06) 100%) !important;
            color: var(--amber-bright) !important;
            padding: 0.45rem 1.1rem !important;
            transition: all 0.15s ease !important;
        }

        div.stButton > button:hover,
        div.stDownloadButton > button:hover {
            border-color: var(--amber) !important;
            background: linear-gradient(180deg, rgba(200, 146, 42, 0.22) 0%, rgba(200, 146, 42, 0.1) 100%) !important;
            color: var(--amber-bright) !important;
            box-shadow: 0 0 14px rgba(200, 146, 42, 0.15) !important;
        }

        div.stButton > button[kind="primary"],
        div.stButton > button[data-testid*="primary"] {
            border-color: var(--amber) !important;
            background: linear-gradient(180deg, var(--amber) 0%, #a8781e 100%) !important;
            color: #07090c !important;
            font-weight: 600 !important;
        }

        div.stButton > button[kind="primary"]:hover {
            background: linear-gradient(180deg, var(--amber-bright) 0%, var(--amber) 100%) !important;
            box-shadow: 0 0 20px rgba(200, 146, 42, 0.3) !important;
        }

        /* ── DataFrames ── */
        [data-testid="stDataFrame"] {
            border: 1px solid var(--border) !important;
            border-radius: 4px !important;
        }

        .dvn-scroller {
            background: var(--surface) !important;
        }

        /* ── Code / Terminal ── */
        .stCodeBlock, pre, code {
            font-family: var(--mono) !important;
            background: var(--black) !important;
            border: 1px solid var(--border) !important;
            border-radius: 4px !important;
            font-size: 0.73rem !important;
            color: var(--text-2) !important;
        }

        /* ── Alerts / Notes ── */
        .lumen-note {
            font-family: var(--mono);
            font-size: 0.73rem;
            line-height: 1.6;
            color: var(--text-2);
            border: 1px solid var(--border-soft);
            border-left: 2px solid var(--amber-dim);
            border-radius: 0 4px 4px 0;
            padding: 12px 16px;
            background: rgba(200, 146, 42, 0.04);
            margin-bottom: 16px;
        }

        .lumen-danger {
            font-family: var(--mono);
            font-size: 0.73rem;
            line-height: 1.6;
            color: #ffccc9;
            border: 1px solid rgba(192, 82, 74, 0.4);
            border-left: 2px solid var(--danger);
            border-radius: 0 4px 4px 0;
            padding: 12px 16px;
            background: var(--danger-dim);
            margin-bottom: 16px;
        }

        .lumen-success {
            font-family: var(--mono);
            font-size: 0.73rem;
            line-height: 1.6;
            color: #b8e8d0;
            border: 1px solid rgba(90, 158, 124, 0.4);
            border-left: 2px solid var(--success);
            border-radius: 0 4px 4px 0;
            padding: 12px 16px;
            background: var(--success-dim);
            margin-bottom: 16px;
        }

        /* ── Sidebar run mode label ── */
        .sidebar-section {
            font-family: var(--mono);
            font-size: 0.6rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            color: var(--amber);
            border-bottom: 1px solid var(--border);
            padding-bottom: 6px;
            margin: 20px 0 12px 0;
        }

        /* ── Checkboxes ── */
        [data-testid="stCheckbox"] label {
            font-family: var(--mono) !important;
            font-size: 0.73rem !important;
            color: var(--text-2) !important;
        }

        /* ── Captions ── */
        .stCaption, [data-testid="stCaptionContainer"] {
            font-family: var(--mono) !important;
            font-size: 0.65rem !important;
            color: var(--text-3) !important;
        }

        /* ── Warning / Error / Success streamlit native ── */
        [data-testid="stNotification"],
        .stAlert {
            font-family: var(--mono) !important;
            font-size: 0.72rem !important;
            border-radius: 4px !important;
        }

        /* ── Spinners ── */
        [data-testid="stSpinner"] p {
            font-family: var(--mono) !important;
            font-size: 0.72rem !important;
            color: var(--amber) !important;
        }

        /* ── Scrollbar ── */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: var(--black); }
        ::-webkit-scrollbar-thumb { background: rgba(200, 146, 42, 0.3); border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--amber-dim); }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── COMPONENTS ────────────────────────────────────────────────────────────────

def show_header(county: str, year: int, month_name: str) -> None:
    st.markdown(
        f"""
        <div class="lumen-hero">
            <div class="hero-eyebrow">Lumen Capital · Acquisition Intelligence</div>
            <h1 class="hero-title">Foreclosure<br>Intelligence</h1>
            <div class="hero-sub">
                Collect, structure, and monitor county foreclosure records
                for distressed asset acquisition analysis.
            </div>
            <div class="hero-badges">
                <span class="hero-badge">{county.upper()} COUNTY</span>
                <span class="hero-badge">{month_name.upper()} {year}</span>
                <span class="hero-badge">Harris Clerk Portal</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_heading(label: str, title: str) -> None:
    st.markdown(
        f"""
        <div class="section-block">
            <span class="section-label">{label}</span>
            <span class="section-title">{title}</span>
            <div class="section-rule"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def note(msg: str, kind: str = "info") -> None:
    css_class = {"info": "lumen-note", "danger": "lumen-danger", "success": "lumen-success"}.get(kind, "lumen-note")
    st.markdown(f'<div class="{css_class}">{msg}</div>', unsafe_allow_html=True)


def sidebar_section(label: str) -> None:
    st.sidebar.markdown(f'<div class="sidebar-section">{label}</div>', unsafe_allow_html=True)


# ── DATA HELPERS ──────────────────────────────────────────────────────────────

def load_failed_records(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(pd.read_csv(path))
    except Exception:
        return 0


def show_output_paths(paths) -> None:
    rows = [
        {"Artifact": "CSV", "Path": str(paths.output_csv)},
        {"Artifact": "JSONL", "Path": str(paths.output_jsonl)},
        {"Artifact": "Checkpoint", "Path": str(paths.checkpoint_json)},
        {"Artifact": "Failed records", "Path": str(paths.failed_json)},
        {"Artifact": "PDF cache", "Path": str(paths.pdfs_dir)},
        {"Artifact": "Text cache", "Path": str(paths.text_cache_dir)},
    ]
    st.dataframe(rows, hide_index=True, use_container_width=True)


def show_csv(path: Path) -> None:
    if not path.exists():
        note("CSV output does not exist yet. Run the scraper to generate records.")
        return

    try:
        df = pd.read_csv(path)
    except Exception as e:
        note(f"Could not read CSV: {e}", "danger")
        return

    col1, col2 = st.columns([1, 5])
    col1.metric("Rows", len(df))

    st.dataframe(df, use_container_width=True, height=320)

    st.download_button(
        "↓ Download CSV",
        data=path.read_bytes(),
        file_name=path.name,
        mime="text/csv",
    )


def show_failed_records(path: Path) -> None:
    failed = load_failed_records(path)
    if not failed:
        note("No failed-record file found yet.")
        return

    failed_ids = failed.get("failed_ids", [])
    failed_reasons = failed.get("failed_reasons", {})

    if not failed_ids:
        note("No failed records. All processed records completed successfully.", "success")
        return

    st.metric("Failed Count", len(failed_ids))
    rows = [
        {"doc_id": doc_id, "reason": failed_reasons.get(doc_id, "—")}
        for doc_id in failed_ids
    ]
    st.dataframe(rows, use_container_width=True)


def build_run_log(result) -> list[str]:
    lines = []
    if result.dry_run:
        lines.append("── DRY RUN ── no output state was changed ──")
        lines.append(f"Planned records : {result.planned}")
        for doc_id in result.planned_doc_ids[:25]:
            lines.append(f"  · {doc_id}")
        remaining = len(result.planned_doc_ids) - 25
        if remaining > 0:
            lines.append(f"  ... {remaining} more planned records")
    else:
        lines.append("── RUN COMPLETE ──")

    lines.extend([
        f"Processed : {result.processed}",
        f"Skipped   : {result.skipped}",
        f"Failed    : {result.failed}",
        f"CSV       : {result.paths.output_csv}",
        f"JSONL     : {result.paths.output_jsonl}",
        f"Checkpoint: {result.paths.checkpoint_json}",
        f"Failed    : {result.paths.failed_json}",
    ])

    extraction_methods = getattr(result, "extraction_methods", {})
    extraction_notes = getattr(result, "extraction_notes", {})
    if extraction_methods:
        lines.append("")
        lines.append("── Extraction Methods ──")
        for doc_id, method in list(extraction_methods.items())[:25]:
            note_txt = extraction_notes.get(doc_id, "") if extraction_notes else ""
            lines.append(f"  {doc_id}: {method}" + (f"  ({note_txt})" if note_txt else ""))
        remaining = len(extraction_methods) - 25
        if remaining > 0:
            lines.append(f"  ... {remaining} more")
    return lines


def show_run_log() -> None:
    lines = st.session_state.get("run_log", ["No run started in this session."])
    st.code("\n".join(lines), language=None)


def show_clear_summary() -> None:
    summary = st.session_state.get("clear_summary")
    if not summary:
        return
    st.write("Deleted files:", summary.deleted_files or "None")
    st.write("Deleted folders:", summary.deleted_folders or "None")
    st.write("Missing paths:", summary.missing_paths or "None")


def show_danger_zone(paths) -> None:
    note(
        "⚠ This operation deletes only the selected monthly generated outputs listed below. "
        "Source code, checkpoints from other months, and PDFs outside this month are unaffected.",
        "danger",
    )
    for path in monthly_output_targets(paths):
        st.markdown(
            f'<code style="font-size:0.7rem;color:var(--text-3,#636978)">{path}</code>',
            unsafe_allow_html=True,
        )

    st.markdown("")
    confirm_checked = st.checkbox("I understand these selected monthly output files/folders will be deleted.")
    typed_phrase = st.text_input(f"Type  {CLEAR_CONFIRMATION_PHRASE}  to confirm")
    clear_allowed = can_clear_outputs(confirm_checked, typed_phrase)

    if st.button("Clear Selected Local Outputs", disabled=not clear_allowed):
        try:
            st.session_state["clear_summary"] = clear_monthly_outputs(paths, repo_root=REPO_ROOT)
            note("Selected local outputs cleared.", "success")
        except Exception as e:
            st.session_state["clear_summary"] = None
            note(f"Clear failed: {e}", "danger")

    show_clear_summary()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Lumen Intel · Foreclosure Intelligence",
        page_icon="◈",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_theme()

    # ── SIDEBAR ──
    with st.sidebar:
        st.markdown(
            """
            <div style="font-family:'JetBrains Mono',monospace;font-size:1.05rem;
                        font-weight:600;color:#c8922a;letter-spacing:0.05em;margin-bottom:4px">
                ◈ LUMEN INTEL
            </div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.62rem;
                        color:#636978;letter-spacing:0.1em;text-transform:uppercase;
                        border-bottom:1px solid rgba(200,146,42,0.18);padding-bottom:12px;margin-bottom:0">
                Foreclosure Scraper
            </div>
            """,
            unsafe_allow_html=True,
        )

        sidebar_section("Target")
        county = st.selectbox("County", ["harris"], index=0)
        year = st.number_input("Year", min_value=2000, max_value=2100, value=2026, step=1)
        month_name = st.selectbox("Month", list(MONTH_OPTIONS.keys()), index=4)
        month = MONTH_OPTIONS[month_name]

        sidebar_section("Scope")
        use_limit = st.checkbox("Use limit", value=True)
        limit = st.number_input(
            "Max records",
            min_value=1,
            value=1,
            step=1,
            disabled=not use_limit,
        )

        sidebar_section("Run Mode")
        resume = st.checkbox("Resume from checkpoint", value=False)
        retry_failed = st.checkbox("Retry failed only", value=False)
        dry_run = st.checkbox("Dry run", value=False)

        sidebar_section("Extraction")
        use_ocr = st.checkbox("OCR fallback (scanned PDFs)", value=False)
        reprocess_existing = st.checkbox("Reprocess local PDFs only", value=False)

        if use_ocr:
            st.caption("Requires Tesseract + poppler installed locally.")
        if reprocess_existing:
            st.caption("Does not contact Harris County portal.")

        confirm_no_limit = True
        if not use_limit:
            st.warning("No limit set — may process the full month.")
            confirm_no_limit = st.checkbox(
                "Confirm full-month run (creates public-record files)",
                value=False,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        if resume and retry_failed:
            st.error("Choose Resume or Retry failed — not both.")
            run_disabled = True
        else:
            run_disabled = not confirm_no_limit

        run_clicked = st.button(
            "▶  Run Scraper",
            type="primary",
            disabled=run_disabled,
            use_container_width=True,
        )

    # ── SELECTED PARAMS ──
    selected_limit = int(limit) if use_limit else None
    paths = build_monthly_paths(county, int(year), month)
    last_result = st.session_state.get("last_result")

    # ── HEADER ──
    show_header(county, int(year), month_name)

    note(
        "Start with limit = 1. Generated outputs may contain public-record data including "
        "names, addresses, and loan details. Review before sharing or committing to version control."
    )

    # ── RUN ──
    if run_clicked:
        with st.spinner("Running Harris monthly runner…"):
            try:
                result = asyncio.run(
                    run_harris_monthly(
                        year=int(year),
                        month=month,
                        limit=selected_limit,
                        resume=resume,
                        retry_failed=retry_failed,
                        dry_run=dry_run,
                        use_ocr=use_ocr,
                        reprocess_existing=reprocess_existing,
                    )
                )
            except Exception as e:
                note(f"Run failed: {e}", "danger")
                st.session_state["run_log"] = [f"Run failed: {e}"]
            else:
                if result.dry_run:
                    note("Dry run complete — no output state was changed.", "success")
                else:
                    note("Run complete.", "success")
                paths = result.paths
                st.session_state["run_log"] = build_run_log(result)
                st.session_state["last_result"] = result
                last_result = result

    # ── EXECUTION SUMMARY ──
    section_heading("01 · Status", "Execution Summary")
    with st.container(border=True):
        processed    = last_result.processed if last_result else 0
        skipped      = last_result.skipped if last_result else 0
        failed_count = last_result.failed if last_result else 0
        csv_rows     = csv_row_count(paths.output_csv)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Processed", processed)
        c2.metric("Skipped",   skipped)
        c3.metric("Failed",    failed_count)
        c4.metric("CSV Rows",  csv_rows)

        st.markdown("<br>", unsafe_allow_html=True)
        show_run_log()

        if last_result and last_result.dry_run and last_result.planned_doc_ids:
            st.markdown(
                '<div style="font-family:var(--mono,monospace);font-size:0.65rem;'
                'text-transform:uppercase;letter-spacing:0.1em;color:#636978;margin:12px 0 6px">Planned Records</div>',
                unsafe_allow_html=True,
            )
            st.dataframe(
                [{"doc_id": d} for d in last_result.planned_doc_ids],
                use_container_width=True,
                height=220,
            )

    # ── EXTRACTION METHODS ──
    extraction_methods = getattr(last_result, "extraction_methods", {}) if last_result else {}
    if extraction_methods:
        section_heading("02 · Extraction", "PDF Extraction Methods")
        with st.container(border=True):
            extraction_notes = getattr(last_result, "extraction_notes", {})
            st.dataframe(
                [
                    {
                        "doc_id": doc_id,
                        "method": method,
                        "notes": extraction_notes.get(doc_id, ""),
                    }
                    for doc_id, method in extraction_methods.items()
                ],
                use_container_width=True,
                height=260,
            )

    # ── CSV PREVIEW ──
    section_heading("03 · Output", "Structured CSV Preview")
    with st.container(border=True):
        show_csv(paths.output_csv)

    # ── FAILED RECORDS ──
    section_heading("04 · Exceptions", "Failed Records & Retry Queue")
    with st.container(border=True):
        show_failed_records(paths.failed_json)

    # ── OUTPUT PATHS ──
    section_heading("05 · Artifacts", "Monthly Output Paths")
    with st.container(border=True):
        show_output_paths(paths)

    # ── DANGER ZONE ──
    section_heading("06 · Danger Zone", "Clear Selected Local Output")
    with st.container(border=True):
        show_danger_zone(paths)


if __name__ == "__main__":
    main()
