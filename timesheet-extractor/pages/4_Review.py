"""
Pipeline 4 — Review

Manual review workflow:
- Ingests Pipeline 3 decisions from logs
- Shows 3 tabs: Manual Approval / Approved / Rejected
- Allows reviewer to Approve or Reject pending manual-review entries
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from trusted_template import record_manual_review_outcome

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
QUEUE_FILE = LOGS_DIR / "review_queue.json"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _list_p3_logs() -> List[Path]:
    return sorted(LOGS_DIR.glob("p3_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def _load_queue() -> Dict[str, Any]:
    if not QUEUE_FILE.exists():
        return {"updated_at": None, "items": {}}
    try:
        return _load_json(QUEUE_FILE)
    except Exception:
        return {"updated_at": None, "items": {}}


def _save_queue(queue: Dict[str, Any]) -> None:
    queue["updated_at"] = _now_iso()
    QUEUE_FILE.write_text(json.dumps(queue, indent=2, default=str), encoding="utf-8")


def _default_final_status(p3_decision: str) -> str:
    if p3_decision in ("AUTO_APPROVE", "AUTO_APPROVE_TRUSTED_TEMPLATE"):
        return "APPROVED"
    if p3_decision == "MANUAL_REVIEW":
        return "PENDING_MANUAL_REVIEW"
    return "PENDING_MANUAL_REVIEW"


def _sync_from_p3_logs(queue: Dict[str, Any]) -> Dict[str, Any]:
    items = queue.setdefault("items", {})
    for p in _list_p3_logs():
        try:
            d = _load_json(p)
            decision_id = d.get("decision_id") or p.stem
            decision_obj = d.get("decision", {})
            p3_decision = decision_obj.get("decision", "MANUAL_REVIEW")
            p3_audit = decision_obj.get("audit", {})
            if decision_id in items:
                # Backfill newer fields for older queue rows created by previous versions.
                item = items[decision_id]
                if not item.get("decision_audit"):
                    item["decision_audit"] = p3_audit
                if not item.get("trusted_context"):
                    item["trusted_context"] = d.get("inputs", {}).get("trusted", {})
                if not item.get("final_decision"):
                    item["final_decision"] = p3_decision
                continue
            final_status = _default_final_status(p3_decision)

            items[decision_id] = {
                "decision_id": decision_id,
                "p3_log": p.name,
                "created_at": d.get("timestamp"),
                "final_decision": p3_decision,
                "approval_type": decision_obj.get("approval_type"),
                "summary": decision_obj.get("summary"),
                "manual_review_reasons": decision_obj.get("manual_review_reasons", []),
                "decision_audit": p3_audit,
                "inputs": d.get("inputs", {}),
                "trusted_context": d.get("inputs", {}).get("trusted", {}),
                "final_status": final_status,
                "review_action": "SYSTEM_AUTO_APPROVE" if final_status == "APPROVED" else None,
                "review_action_by": "system" if final_status == "APPROVED" else None,
                "review_action_at": d.get("timestamp") if final_status == "APPROVED" else None,
                "review_comment": None,
            }
        except Exception:
            continue
    return queue


def _items_by_status(queue: Dict[str, Any], status: str) -> List[Dict[str, Any]]:
    items = list((queue.get("items") or {}).values())
    out = [i for i in items if i.get("final_status") == status]
    out.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return out


def _row_view(item: Dict[str, Any]) -> Dict[str, Any]:
    files = (item.get("inputs") or {}).get("p1_record_files") or []
    return {
        "decision_id": item.get("decision_id"),
        "created_at": item.get("created_at"),
        "file(s)": " + ".join(files) if files else "—",
        "final_decision": item.get("final_decision") or item.get("pipeline3_decision"),
        "final_status": item.get("final_status"),
        "summary": item.get("summary"),
        "review_action": item.get("review_action") or "—",
        "review_action_by": item.get("review_action_by") or "—",
        "review_action_at": item.get("review_action_at") or "—",
    }


st.set_page_config(page_title="Pipeline 4 — Review", page_icon="🗂️", layout="wide")
st.title("🗂️ Pipeline 4 — Review")
st.caption("Review MANUAL_REVIEW entries and move them to Approved or Rejected.")

if st.button("🏠 Home", type="primary"):
    st.switch_page("app.py")

queue = _load_queue()
queue = _sync_from_p3_logs(queue)
_save_queue(queue)

pending = _items_by_status(queue, "PENDING_MANUAL_REVIEW")
approved = _items_by_status(queue, "APPROVED")
rejected = _items_by_status(queue, "REJECTED")

c1, c2, c3 = st.columns(3)
c1.metric("Manual Approval", len(pending))
c2.metric("Approved", len(approved))
c3.metric("Rejected", len(rejected))

tab_manual, tab_approved, tab_rejected = st.tabs(["Manual Approval", "Approved", "Rejected"])

with tab_manual:
    if not pending:
        st.success("No pending manual approvals.")
    else:
        st.dataframe(pd.DataFrame([_row_view(i) for i in pending]), use_container_width=True, hide_index=True)
        st.divider()
        st.markdown("### Review actions")
        for item in pending:
            did = item.get("decision_id")
            files = (item.get("inputs") or {}).get("p1_record_files") or []
            title = " + ".join(files) if files else did
            with st.expander(f"{title} ({did})"):
                reasons = item.get("manual_review_reasons") or []
                if reasons:
                    st.markdown("**Manual review reasons**")
                    for r in reasons:
                        st.markdown(
                            f"- `{r.get('reason_code','UNKNOWN')}` ({r.get('severity','CRITICAL')}): {r.get('message','')}"
                        )
                else:
                    st.info("No explicit reasons captured.")

                comment_key = f"comment_{did}"
                comment = st.text_input("Reviewer comment (optional)", key=comment_key)
                corrected_key = f"corrected_{did}"
                data_corrected = st.checkbox("Data corrected during review", key=corrected_key, value=False)
                c_left, c_right = st.columns(2)
                if c_left.button("✅ Approve", key=f"approve_{did}", use_container_width=True):
                    queue["items"][did]["final_status"] = "APPROVED"
                    queue["items"][did]["review_action"] = "MANUAL_APPROVE"
                    queue["items"][did]["review_action_by"] = "reviewer"
                    queue["items"][did]["review_action_at"] = _now_iso()
                    queue["items"][did]["review_comment"] = comment or None
                    queue["items"][did]["data_corrected"] = bool(data_corrected)
                    tc = queue["items"][did].get("trusted_context") or {}
                    audit = queue["items"][did].get("decision_audit") or {}
                    accepted_missing_fields = audit.get("required_fields_missing") or []
                    identity = tc.get("identity_key")
                    template_hash = tc.get("template_hash")
                    if identity and template_hash:
                        record_manual_review_outcome(
                            identity=identity,
                            template_hash=template_hash,
                            action="MANUAL_APPROVE",
                            data_corrected=bool(data_corrected),
                            accepted_missing_fields=accepted_missing_fields,
                        )
                    _save_queue(queue)
                    st.rerun()
                if c_right.button("❌ Reject", key=f"reject_{did}", use_container_width=True):
                    queue["items"][did]["final_status"] = "REJECTED"
                    queue["items"][did]["review_action"] = "MANUAL_REJECT"
                    queue["items"][did]["review_action_by"] = "reviewer"
                    queue["items"][did]["review_action_at"] = _now_iso()
                    queue["items"][did]["review_comment"] = comment or None
                    queue["items"][did]["data_corrected"] = bool(data_corrected)
                    tc = queue["items"][did].get("trusted_context") or {}
                    audit = queue["items"][did].get("decision_audit") or {}
                    accepted_missing_fields = audit.get("required_fields_missing") or []
                    identity = tc.get("identity_key")
                    template_hash = tc.get("template_hash")
                    if identity and template_hash:
                        record_manual_review_outcome(
                            identity=identity,
                            template_hash=template_hash,
                            action="MANUAL_REJECT",
                            data_corrected=bool(data_corrected),
                            accepted_missing_fields=accepted_missing_fields,
                        )
                    _save_queue(queue)
                    st.rerun()

with tab_approved:
    if not approved:
        st.info("No approved entries.")
    else:
        st.dataframe(pd.DataFrame([_row_view(i) for i in approved]), use_container_width=True, hide_index=True)
        with st.expander("Approved details"):
            st.json(approved)

with tab_rejected:
    if not rejected:
        st.info("No rejected entries.")
    else:
        st.dataframe(pd.DataFrame([_row_view(i) for i in rejected]), use_container_width=True, hide_index=True)
        with st.expander("Rejected details"):
            st.json(rejected)

