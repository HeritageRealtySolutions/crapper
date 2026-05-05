"""Simple Streamlit wrapper for the Harris monthly runner."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys

# Streamlit runs this file from app/, so add the repo root for local package imports.
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
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --chrome: #d8dde6;
            --chrome-soft: #9ca6b7;
            --panel: #111318;
            --panel-2: #171a21;
            --line: rgba(216, 221, 230, 0.22);
            --line-strong: rgba(216, 221, 230, 0.38);
            --danger: #d66a61;
            --success: #9ed7bc;
        }

        .stApp {
            background:
                radial-gradient(circle at 18% 0%, rgba(165, 174, 190, 0.14), transparent 32%),
                linear-gradient(180deg, #050506 0%, #0b0d10 45%, #050506 100%);
            color: #f4f6f8;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #050506 0%, #111318 100%);
            border-right: 1px solid var(--line);
        }

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p {
            color: #f4f6f8;
        }

        .block-container {
            padding-top: 2.5rem;
            max-width: 1280px;
        }

        .brand-hero {
            border: 1px solid var(--line-strong);
            border-radius: 8px;
            padding: 28px 30px;
            margin-bottom: 24px;
            background:
                linear-gradient(135deg, rgba(255, 255, 255, 0.11), rgba(255, 255, 255, 0.03) 38%, rgba(255, 255, 255, 0.08)),
                linear-gradient(180deg, #151821 0%, #0b0d10 100%);
            box-shadow: 0 24px 70px rgba(0, 0, 0, 0.45);
        }

        .brand-kicker {
            color: var(--chrome-soft);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0;
            margin-bottom: 8px;
        }

        .brand-title {
            color: #ffffff;
            font-size: 2.75rem;
            line-height: 1.05;
            font-weight: 700;
            margin: 0;
        }

        .brand-subtitle {
            color: #c7ced9;
            max-width: 760px;
            margin-top: 12px;
            font-size: 1.02rem;
        }

        .section-heading {
            margin: 20px 0 8px 0;
        }

        .section-label {
            color: var(--chrome-soft);
            text-transform: uppercase;
            font-size: 0.72rem;
            letter-spacing: 0;
        }

        .section-title {
            color: #f6f7f9;
            font-size: 1.08rem;
            font-weight: 650;
            margin-top: 2px;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
            background: linear-gradient(180deg, rgba(23, 26, 33, 0.95), rgba(12, 14, 18, 0.95));
            border: 1px solid var(--line) !important;
            border-radius: 8px !important;
            box-shadow: 0 14px 45px rgba(0, 0, 0, 0.26);
        }

        [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.08), rgba(255, 255, 255, 0.02));
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 16px 16px 14px 16px;
        }

        [data-testid="stMetricLabel"] {
            color: var(--chrome-soft);
        }

        [data-testid="stMetricValue"] {
            color: #ffffff;
        }

        div.stButton > button,
        div.stDownloadButton > button {
            border-radius: 8px;
            border: 1px solid var(--line-strong);
            background: linear-gradient(180deg, #f6f7f9 0%, #aeb7c6 100%);
            color: #050506;
            font-weight: 650;
        }

        div.stButton > button:hover,
        div.stDownloadButton > button:hover {
            border-color: #ffffff;
            color: #050506;
        }

        .safe-note {
            border: 1px solid rgba(216, 221, 230, 0.18);
            border-radius: 8px;
            padding: 14px 16px;
            color: #cbd2dc;
            background: rgba(216, 221, 230, 0.07);
            margin-bottom: 12px;
        }

        .danger-note {
            border: 1px solid rgba(214, 106, 97, 0.55);
            border-radius: 8px;
            padding: 14px 16px;
            color: #ffe4e1;
            background: rgba(214, 106, 97, 0.11);
            margin-bottom: 12px;
        }

        code, pre {
            border-radius: 8px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def show_header() -> None:
    st.markdown(
        """
        <div class="brand-hero">
            <div class="brand-kicker">Presented by Lumen Capital</div>
            <h1 class="brand-title">Intelligence Dashboard</h1>
            <div class="brand-subtitle">
                Collect, monitor, and structure Harris County foreclosure data for acquisition analysis.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_heading(label: str, title: str) -> None:
    st.markdown(
        f"""
        <div class="section-heading">
            <div class="section-label">{label}</div>
            <div class="section-title">{title}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def load_failed_records(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


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


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(pd.read_csv(path))


def show_csv(path: Path) -> None:
    if not path.exists():
        st.markdown('<div class="safe-note">CSV output does not exist yet.</div>', unsafe_allow_html=True)
        return

    df = pd.read_csv(path)
    st.write(f"Rows: {len(df)}")
    st.dataframe(df, use_container_width=True)

    st.download_button(
        "Download CSV",
        data=path.read_bytes(),
        file_name=path.name,
        mime="text/csv",
    )


def show_failed_records(path: Path) -> None:
    failed = load_failed_records(path)
    if not failed:
        st.markdown('<div class="safe-note">No failed-record file found yet.</div>', unsafe_allow_html=True)
        return

    failed_ids = failed.get("failed_ids", [])
    failed_reasons = failed.get("failed_reasons", {})
    st.write(f"Failed count: {len(failed_ids)}")
    if failed_ids:
        rows = [
            {"doc_id": doc_id, "reason": failed_reasons.get(doc_id, "")}
            for doc_id in failed_ids
        ]
        st.dataframe(rows, use_container_width=True)
    else:
        st.success("No failed records.")


def build_run_log(result) -> list[str]:
    lines = []
    if result.dry_run:
        lines.append("DRY RUN: no output state was changed.")
        lines.append(f"Planned records: {result.planned}")
        for doc_id in result.planned_doc_ids[:25]:
            lines.append(f"- {doc_id}")
        remaining = len(result.planned_doc_ids) - 25
        if remaining > 0:
            lines.append(f"... {remaining} more planned records")
    else:
        lines.append("Run complete.")

    lines.extend([
        f"Processed: {result.processed}",
        f"Skipped: {result.skipped}",
        f"Failed: {result.failed}",
        f"CSV: {result.paths.output_csv}",
        f"JSONL: {result.paths.output_jsonl}",
        f"Checkpoint: {result.paths.checkpoint_json}",
        f"Failed records: {result.paths.failed_json}",
    ])
    return lines


def show_run_log() -> None:
    lines = st.session_state.get("run_log", ["No run has been started in this app session."])
    st.code("\n".join(lines))


def show_clear_summary() -> None:
    summary = st.session_state.get("clear_summary")
    if not summary:
        return

    st.write("Deleted files:", summary.deleted_files or "None")
    st.write("Deleted folders:", summary.deleted_folders or "None")
    st.write("Missing paths:", summary.missing_paths or "None")


def show_danger_zone(paths) -> None:
    st.markdown(
        '<div class="danger-note">This clears only the selected monthly generated outputs listed below.</div>',
        unsafe_allow_html=True,
    )
    for path in monthly_output_targets(paths):
        st.write(str(path))

    confirm_checked = st.checkbox("I understand these selected monthly output files/folders will be deleted.")
    typed_phrase = st.text_input(f'Type {CLEAR_CONFIRMATION_PHRASE} to confirm')
    clear_allowed = can_clear_outputs(confirm_checked, typed_phrase)

    if st.button("Clear Selected Local Outputs", disabled=not clear_allowed):
        try:
            st.session_state["clear_summary"] = clear_monthly_outputs(paths, repo_root=REPO_ROOT)
            st.success("Selected local outputs cleared.")
        except Exception as e:
            st.session_state["clear_summary"] = None
            st.error(f"Clear failed: {e}")

    show_clear_summary()


def main() -> None:
    st.set_page_config(page_title="Intelligence Dashboard", layout="wide")
    apply_theme()
    show_header()

    st.markdown(
        '<div class="safe-note">Start with a small limit, such as 1. Generated outputs may contain '
        'public-record information. Review PDFs, text cache, CSVs, JSONL, checkpoints, logs, and '
        'failed-record files before sharing or committing.</div>',
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("## Run Controls")
        county = st.selectbox("County", ["harris"], index=0)
        year = st.number_input("Year", min_value=2000, max_value=2100, value=2026, step=1)
        month_label = st.selectbox("Month", list(MONTH_OPTIONS.keys()), index=4)
        month = MONTH_OPTIONS[month_label]

        use_limit = st.checkbox("Use limit", value=True)
        limit = st.number_input(
            "Limit records considered",
            min_value=1,
            value=1,
            step=1,
            disabled=not use_limit,
        )

        resume = st.checkbox("Resume", value=False)
        retry_failed = st.checkbox("Retry failed only", value=False)
        dry_run = st.checkbox("Dry run only", value=False)

        confirm_no_limit = True
        if not use_limit:
            st.warning("Running without a limit may process the full month and create public-record output files.")
            confirm_no_limit = st.checkbox(
                "I understand this may run the full month and create public-record output files.",
                value=False,
            )

        run_clicked = st.button(
            "Run Harris Monthly Runner",
            type="primary",
            disabled=not confirm_no_limit,
        )

    selected_limit = int(limit) if use_limit else None
    paths = build_monthly_paths(county, int(year), month)

    last_result = st.session_state.get("last_result")

    if resume and retry_failed:
        st.error("Choose either Resume or Retry failed only, not both.")
        run_clicked = False

    if run_clicked:
        with st.spinner("Running Harris monthly runner..."):
            try:
                result = asyncio.run(
                    run_harris_monthly(
                        year=int(year),
                        month=month,
                        limit=selected_limit,
                        resume=resume,
                        retry_failed=retry_failed,
                        dry_run=dry_run,
                    )
                )
            except Exception as e:
                st.error(f"Run failed: {e}")
                st.session_state["run_log"] = [f"Run failed: {e}"]
            else:
                st.success("Dry run complete. No output state was changed." if result.dry_run else "Run complete.")
                paths = result.paths
                st.session_state["run_log"] = build_run_log(result)
                st.session_state["last_result"] = result
                last_result = result

    processed = last_result.processed if last_result else 0
    skipped = last_result.skipped if last_result else 0
    failed_count = last_result.failed if last_result else 0

    section_heading("Run Status", "Execution Summary")
    with st.container(border=True):
        metric_cols = st.columns(4)
        metric_cols[0].metric("Processed", processed)
        metric_cols[1].metric("Skipped", skipped)
        metric_cols[2].metric("Failed", failed_count)
        metric_cols[3].metric("CSV Rows", csv_row_count(paths.output_csv))

        st.markdown("#### Run Log")
        show_run_log()
        if last_result and last_result.dry_run:
            st.markdown("#### Planned Records")
            st.write(f"Planned count: {last_result.planned}")
            st.dataframe(
                [{"doc_id": doc_id} for doc_id in last_result.planned_doc_ids],
                use_container_width=True,
            )

    section_heading("Output Preview", "Structured CSV")
    with st.container(border=True):
        show_csv(paths.output_csv)

    section_heading("Failed Records", "Exceptions And Retry Queue")
    with st.container(border=True):
        show_failed_records(paths.failed_json)

    section_heading("Output Paths", "Monthly Artifacts")
    with st.container(border=True):
        show_output_paths(paths)

    section_heading("Danger Zone", "Clear Selected Local Output")
    with st.container(border=True):
        show_danger_zone(paths)


if __name__ == "__main__":
    main()
