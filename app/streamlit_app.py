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


def load_failed_records(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def show_output_paths(paths) -> None:
    st.write("CSV:", str(paths.output_csv))
    st.write("JSONL:", str(paths.output_jsonl))
    st.write("Checkpoint:", str(paths.checkpoint_json))
    st.write("Failed records:", str(paths.failed_json))
    st.write("PDF cache:", str(paths.pdfs_dir))
    st.write("Text cache:", str(paths.text_cache_dir))


def show_csv(path: Path) -> None:
    if not path.exists():
        st.info("CSV output does not exist yet.")
        return

    df = pd.read_csv(path)
    st.subheader("CSV Preview")
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
        st.info("No failed-record file found yet.")
        return

    st.subheader("Failed Records")
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
    st.subheader("Run Log")
    lines = st.session_state.get("run_log", ["No run has been started in this app session."])
    st.code("\n".join(lines))


def show_clear_summary() -> None:
    summary = st.session_state.get("clear_summary")
    if not summary:
        return

    st.subheader("Clear Summary")
    st.write("Deleted files:", summary.deleted_files or "None")
    st.write("Deleted folders:", summary.deleted_folders or "None")
    st.write("Missing paths:", summary.missing_paths or "None")


def show_danger_zone(paths) -> None:
    st.subheader("Danger Zone")
    st.warning("This clears only the selected monthly generated outputs listed below.")
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
    st.set_page_config(page_title="Harris Foreclosure Runner", layout="wide")
    st.title("Harris County Foreclosure Runner")

    st.warning(
        "Start with a small limit, such as 1. Generated outputs may contain "
        "public-record information. Review PDFs, text cache, CSVs, JSONL, "
        "checkpoints, logs, and failed-record files before sharing or committing."
    )

    with st.sidebar:
        st.header("Run Settings")
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

    st.subheader("Output Paths")
    show_output_paths(paths)

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
                col1, col2, col3 = st.columns(3)
                col1.metric("Processed", result.processed)
                col2.metric("Skipped", result.skipped)
                col3.metric("Failed", result.failed)
                if result.dry_run:
                    st.subheader("Planned Records")
                    st.write(f"Planned count: {result.planned}")
                    st.dataframe(
                        [{"doc_id": doc_id} for doc_id in result.planned_doc_ids],
                        use_container_width=True,
                    )
                paths = result.paths
                st.session_state["run_log"] = build_run_log(result)

    show_run_log()
    show_csv(paths.output_csv)
    show_failed_records(paths.failed_json)
    show_danger_zone(paths)


if __name__ == "__main__":
    main()
