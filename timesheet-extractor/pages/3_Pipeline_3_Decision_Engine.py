"""
Pipeline 3 — Decision Engine

Consumes Pipeline 1 extracted JSON + Pipeline 2 comparison output
and produces a decision record (AUTO_APPROVE / MANUAL_REVIEW / TRUSTED).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from decision_engine import make_decision
from trusted_template import derive_trusted_context


LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _list_logs(prefix: str) -> List[Path]:
    return sorted(LOGS_DIR.glob(f"{prefix}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def _matching_p2_logs(p2_logs: List[Path], p1_log_name: str, p1_record_files: List[str]) -> List[Path]:
    """
    Return Pipeline 2 logs that likely belong to selected Pipeline 1 record.
    Match rules:
    - Same p1_log name, and
    - source_file_for_match is one of the selected record files OR
      p1_record_files overlap.
    """
    matched: List[Path] = []
    files_set = set(p1_record_files or [])
    for p in p2_logs:
        try:
            d = _load_json(p)
            inp = d.get("inputs", {})
            if inp.get("p1_log") != p1_log_name:
                continue
            src = inp.get("source_file_for_match")
            p2_files = set(inp.get("p1_record_files") or [])
            if (src and src in files_set) or (files_set and p2_files and files_set.intersection(p2_files)):
                matched.append(p)
        except Exception:
            continue
    return matched


st.set_page_config(page_title="Pipeline 3 — Decision Engine", page_icon="✅", layout="wide")
st.title("✅ Pipeline 3 — Decision Engine")
st.caption("Generate decision output from Pipeline 1 + Pipeline 2 results.")

with st.sidebar:
    st.header("📋 Pipeline 3 Logs")
    p3_logs = _list_logs("p3")
    if not p3_logs:
        st.info("No Pipeline 3 logs yet.")
    else:
        for lf in p3_logs[:20]:
            data = _load_json(lf)
            ts = data.get("timestamp", "")[:19].replace("T", " ")
            dec = data.get("decision", {}).get("decision", "—")
            if st.button(f"{ts}  {dec}", key=lf.stem, use_container_width=True):
                st.session_state["p3_loaded_log"] = data

st.divider()
st.subheader("Inputs")

col_l, col_r = st.columns(2)
prefill = st.session_state.get("p3_prefill") or {}

with col_l:
    st.markdown("### Pipeline 1 extracted record")
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

    p1_choice = st.selectbox("Choose a Pipeline 1 log", options=p1_logs, index=p1_default_idx, format_func=lambda p: p.name)
    p1_data = _load_json(p1_choice)
    merged = p1_data.get("results") or []
    if not merged:
        st.error("Selected Pipeline 1 log has no results.")
        st.stop()

    rec_labels = []
    for item in merged:
        files = item.get("files") or []
        rec_labels.append(" + ".join(files) if files else "unknown")

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
        format_func=lambda i: rec_labels[i],
    )
    extracted = merged[rec_idx].get("extracted") or {}

with col_r:
    st.markdown("### Pipeline 2 comparison")
    p2_logs = _list_logs("p2")
    if not p2_logs:
        st.warning("No Pipeline 2 logs found. Run Pipeline 2 first.")
        st.stop()

    selected_files = merged[rec_idx].get("files", [])
    matched_p2 = _matching_p2_logs(p2_logs, p1_choice.name, selected_files)

    auto_link = st.toggle("Auto-link latest matching Pipeline 2 log", value=True)
    if auto_link:
        if matched_p2:
            p2_choice = matched_p2[0]
            st.success(f"Auto-linked: `{p2_choice.name}`")
            if len(matched_p2) > 1:
                st.caption(f"{len(matched_p2)} matching Pipeline 2 logs found; using latest.")
        else:
            st.warning("No matching Pipeline 2 log found for this Pipeline 1 record. Select manually.")
            p2_prefill_name = prefill.get("p2_log")
            p2_default_idx = 0
            if p2_prefill_name:
                for i, lp in enumerate(p2_logs):
                    if lp.name == p2_prefill_name:
                        p2_default_idx = i
                        break
            p2_choice = st.selectbox("Choose a Pipeline 2 log", options=p2_logs, index=p2_default_idx, format_func=lambda p: p.name)
    else:
        p2_prefill_name = prefill.get("p2_log")
        p2_default_idx = 0
        if p2_prefill_name:
            for i, lp in enumerate(p2_logs):
                if lp.name == p2_prefill_name:
                    p2_default_idx = i
                    break
        p2_choice = st.selectbox("Choose a Pipeline 2 log", options=p2_logs, index=p2_default_idx, format_func=lambda p: p.name)

    p2_data = _load_json(p2_choice)
    comparison = p2_data.get("comparison") or {}

st.divider()
st.subheader("Policy / Trusted (auto)")

policy = {
    "block_on_duplicate": st.toggle("Block auto-approve when duplicates detected", value=True),
    "trusted_template_enabled": st.toggle("Enable trusted-template auto-approve path", value=True),
}

trusted = derive_trusted_context(extracted, file_types=merged[rec_idx].get("file_types", []))
with st.expander("Trusted-template context (auto-derived)", expanded=True):
    st.json(trusted)

run = st.button("▶ Run Decision Engine", type="primary")

if run:
    decision_id = str(uuid.uuid4())
    decision = make_decision(extracted, comparison, policy=policy, trusted=trusted)

    log_entry = {
        "pipeline": "3 — Decision Engine",
        "decision_id": decision_id,
        "timestamp": datetime.utcnow().isoformat(),
        "inputs": {
            "p1_log": p1_choice.name,
            "p1_record_files": merged[rec_idx].get("files", []),
            "p2_log": p2_choice.name,
            "policy": policy,
            "trusted": trusted,
        },
        "decision": decision,
        "log_file": f"p3_{decision_id}.json",
    }
    (LOGS_DIR / log_entry["log_file"]).write_text(json.dumps(log_entry, indent=2, default=str), encoding="utf-8")
    st.session_state["p3_loaded_log"] = log_entry
    st.rerun()

log_data = st.session_state.get("p3_loaded_log")
if log_data:
    st.divider()
    st.subheader("Decision output")
    dec = log_data.get("decision") or {}
    st.metric("decision", dec.get("decision", "—"))
    st.metric("approval_type", dec.get("approval_type", "—"))
    st.write(dec.get("summary", ""))

    st.markdown("### Manual review reasons")
    reasons = dec.get("manual_review_reasons") or []
    if dec.get("decision") == "MANUAL_REVIEW":
        if reasons:
            st.error("This submission is routed to MANUAL_REVIEW for the reason(s) below.")
            for r in reasons:
                st.markdown(
                    f"- **{r.get('reason_code','UNKNOWN')}** ({r.get('severity','CRITICAL')}): "
                    f"{r.get('message','')}"
                )
        else:
            st.error("This submission is routed to MANUAL_REVIEW, but no explicit reasons were returned.")
    if reasons:
        st.dataframe(pd.DataFrame(reasons), use_container_width=True, hide_index=True)
    else:
        st.success("No manual review reasons.")

    st.markdown("### Audit")
    st.json(dec.get("audit", {}))

    st.divider()
    c_left, c_mid, c_right = st.columns([1, 2, 1])
    with c_mid:
        if st.button("➡️ Next Step: Review", type="primary", use_container_width=True):
            st.switch_page("pages/4_Review.py")

