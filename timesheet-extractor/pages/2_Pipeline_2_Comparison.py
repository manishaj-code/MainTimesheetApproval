"""
Pipeline 2 — Comparison

Compares Pipeline 1 extracted JSON with truth document extraction.
Truth document is fetched from truthfile/documents using the same filename
as selected in the Pipeline 1 merged record, then extracted to schema JSON.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# Go up one level so imports resolve from timesheet-extractor/
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from comparator import compare_timesheet
from extractor import extract_raw_content, invoke_bedrock_extraction


REPO_ROOT = Path(__file__).parent.parent.parent
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

TRUTH_DOCS_DIR = REPO_ROOT / "truthfile" / "documents"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _list_logs(prefix: str) -> List[Path]:
    return sorted(LOGS_DIR.glob(f"{prefix}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def _find_truth_doc(p1_filename: str) -> Optional[Path]:
    """
    Prefer exact filename match in truthfile/documents.
    Fallback to stem match (if extension changed).
    """
    exact = TRUTH_DOCS_DIR / p1_filename
    if exact.exists() and exact.is_file():
        return exact

    stem = Path(p1_filename).stem.lower()
    for p in TRUTH_DOCS_DIR.iterdir():
        if p.is_file() and p.stem.lower() == stem:
            return p
    return None


st.set_page_config(page_title="Pipeline 2 — Comparison", page_icon="🔎", layout="wide")
st.title("🔎 Pipeline 2 — Comparison")
st.caption("Compare Pipeline 1 extracted JSON with truthfile/documents extraction (same filename).")

with st.sidebar:
    st.header("📋 Pipeline 2 Logs")
    p2_logs = _list_logs("p2")
    if not p2_logs:
        st.info("No Pipeline 2 logs yet.")
    else:
        for lf in p2_logs[:20]:
            data = _load_json(lf)
            ts = data.get("timestamp", "")[:19].replace("T", " ")
            res = data.get("comparison", {}).get("comparison_result", "—")
            if st.button(f"{ts}  {res}", key=lf.stem, use_container_width=True):
                st.session_state["p2_loaded_log"] = data

st.divider()

st.subheader("Inputs")

col_l, col_r = st.columns(2)
prefill = st.session_state.get("p2_prefill") or {}

with col_l:
    st.markdown("### Pipeline 1 source")
    p1_logs = _list_logs("p1")
    if not p1_logs:
        st.warning("No Pipeline 1 logs found. Run Pipeline 1 first.")
        st.stop()

    p1_default_idx = 0
    p1_prefill_name = prefill.get("p1_log")
    if p1_prefill_name:
        for i, lp in enumerate(p1_logs):
            if lp.name == p1_prefill_name:
                p1_default_idx = i
                break

    p1_choice = st.selectbox(
        "Choose a Pipeline 1 log",
        options=p1_logs,
        index=p1_default_idx,
        format_func=lambda p: p.name,
    )
    p1_data = _load_json(p1_choice)
    merged = p1_data.get("results") or []
    if not merged:
        st.error("Selected Pipeline 1 log has no results.")
        st.stop()

    record_labels = []
    for item in merged:
        files = item.get("files") or []
        record_labels.append(" + ".join(files) if files else "unknown")

    rec_default_idx = 0
    prefill_files = set(prefill.get("p1_record_files") or [])
    if prefill_files:
        for i, item in enumerate(merged):
            files_i = set(item.get("files") or [])
            if files_i and files_i.intersection(prefill_files):
                rec_default_idx = i
                break

    rec_idx = st.selectbox(
        "Choose merged record",
        options=list(range(len(merged))),
        index=rec_default_idx,
        format_func=lambda i: record_labels[i],
    )
    extracted = merged[rec_idx].get("extracted") or {}
    quality_flags = []
    # Prefer precheck-derived quality flags if present
    prechecks = merged[rec_idx].get("prechecks") or []
    for pc in prechecks:
        if pc and pc.get("quality_flags"):
            quality_flags.extend(pc.get("quality_flags") or [])
    quality_flags = sorted(set(map(str, quality_flags)))

with col_r:
    st.markdown("### Truth document (auto-resolved)")
    if not TRUTH_DOCS_DIR.exists():
        st.warning(f"Truth documents folder not found: `{TRUTH_DOCS_DIR}`")
        st.stop()

    p1_files = merged[rec_idx].get("files") or []
    if not p1_files:
        st.warning("Selected Pipeline 1 record has no source file names.")
        st.stop()

    selected_source_file = st.selectbox(
        "Choose source file from selected Pipeline 1 record",
        options=p1_files,
    )
    truth_doc = _find_truth_doc(selected_source_file)
    if truth_doc:
        st.info(f"Matched truth doc: `{truth_doc.name}`")
    else:
        st.error(f"No matching file found in `truthfile/documents` for `{selected_source_file}`")
        st.stop()

st.divider()

run = st.button("▶ Run Comparison", type="primary")

if run:
    comparison_id = str(uuid.uuid4())
    with st.spinner("Extracting truth document and running comparison..."):
        truth_bytes = truth_doc.read_bytes()
        truth_raw = extract_raw_content(truth_doc.name, truth_bytes)
        truth_extracted, truth_audit = invoke_bedrock_extraction(truth_raw)
        comparison = compare_timesheet(extracted, truth_extracted, quality_flags=quality_flags)

    log_entry = {
        "pipeline": "2 — Comparison",
        "comparison_id": comparison_id,
        "timestamp": datetime.utcnow().isoformat(),
        "inputs": {
            "p1_log": p1_choice.name,
            "p1_record_files": merged[rec_idx].get("files", []),
            "source_file_for_match": selected_source_file,
            "truth_document_file": truth_doc.name,
        },
        "truth_extracted": truth_extracted,
        "truth_extraction_audit": truth_audit,
        "comparison": comparison,
        "log_file": f"p2_{comparison_id}.json",
    }
    (LOGS_DIR / log_entry["log_file"]).write_text(json.dumps(log_entry, indent=2, default=str), encoding="utf-8")
    st.session_state["p2_loaded_log"] = log_entry
    st.rerun()

log_data = st.session_state.get("p2_loaded_log")
if log_data:
    st.divider()
    st.subheader("Results")

    cmp_data = log_data.get("comparison") or {}
    res = cmp_data.get("comparison_result", "—")
    if res == "MATCH":
        st.success("Comparison result: MATCH")
    elif res == "PARTIAL_MATCH":
        st.warning("Comparison result: PARTIAL_MATCH")
    elif res == "MISMATCH":
        st.error("Comparison result: MISMATCH")
    else:
        st.info(f"Comparison result: {res}")
    st.metric("comparison_result", res)

    # Field results
    st.markdown("### Field results")
    st.json(cmp_data.get("field_results", {}))

    # Issues
    st.markdown("### Issues")
    issues = cmp_data.get("issues", [])
    if issues:
        df_issues = pd.DataFrame(issues)
        st.dataframe(df_issues, use_container_width=True, hide_index=True)
    else:
        st.success("No issues.")

    # Day-wise comparison
    st.markdown("### Day-wise comparison")
    st.caption("`delta = extracted_day_total - expected_day_total` (and in project breakdown: `extracted_hours - expected_hours`).")
    day_rows = cmp_data.get("day_wise_comparison", [])
    if day_rows:
        df_days = pd.DataFrame(
            [
                {
                    "work_date": r.get("work_date"),
                    "expected_day_total": r.get("expected_day_total"),
                    "extracted_day_total": r.get("extracted_day_total"),
                    "delta": r.get("delta"),
                    "status": r.get("status"),
                }
                for r in day_rows
            ]
        )
        st.dataframe(df_days, use_container_width=True, hide_index=True)

        with st.expander("Per-day project breakdown"):
            for r in day_rows:
                st.markdown(f"#### {r.get('work_date')} — {r.get('status')}")
                pb = r.get("project_breakdown", [])
                if pb:
                    st.dataframe(pd.DataFrame(pb), use_container_width=True, hide_index=True)
                else:
                    st.info("No project breakdown.")
    else:
        st.info("No day-wise comparison produced (missing truth period).")

    st.divider()
    c_left, c_mid, c_right = st.columns([1, 2, 1])
    with c_mid:
        if st.button("➡️ Next Step: Pipeline 3", type="primary", use_container_width=True):
            st.session_state["p3_prefill"] = {
                "p1_log": log_data.get("inputs", {}).get("p1_log"),
                "p1_record_files": log_data.get("inputs", {}).get("p1_record_files", []),
                "p2_log": log_data.get("log_file"),
            }
            st.switch_page("pages/3_Pipeline_3_Decision_Engine.py")

