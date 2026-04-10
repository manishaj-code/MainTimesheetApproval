"""
AutoStart — End-to-End pipeline runner

Runs Pipeline 1 -> Pipeline 2 -> Pipeline 3 in one click,
then redirects to Review.
"""

from __future__ import annotations

import hashlib
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from comparator import compare_timesheet
from decision_engine import make_decision
from extractor import extract_raw_content, invoke_bedrock_extraction, precheck_file, validate_required_fields
from merger import merge_extractions
from trusted_template import derive_trusted_context


REPO_ROOT = Path(__file__).parent.parent.parent
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
PROCESSED_INDEX_FILE = LOGS_DIR / "processed_submissions_index.json"
TRUTH_DOCS_DIR = REPO_ROOT / "truthfile" / "documents"


def _load_processed_index() -> dict:
    if not PROCESSED_INDEX_FILE.exists():
        return {"entries": []}
    try:
        return json.loads(PROCESSED_INDEX_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"entries": []}


def _save_processed_index(index_data: dict) -> None:
    PROCESSED_INDEX_FILE.write_text(json.dumps(index_data, indent=2, default=str), encoding="utf-8")


def _hash_json(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _details_fingerprint(extracted: dict) -> str:
    normalized_entries = []
    for proj in extracted.get("daily_entries") or []:
        p = (proj.get("project_name") or "").strip().lower()
        b = (proj.get("billing_type") or "").strip().lower()
        days = sorted(
            [
                {
                    "work_date": str(d.get("work_date") or ""),
                    "day_name": str(d.get("day_name") or "").strip().lower(),
                    "hours_worked": round(float(d.get("hours_worked") or 0), 2),
                }
                for d in (proj.get("daily_entries") or [])
            ],
            key=lambda x: (x["work_date"], x["day_name"]),
        )
        normalized_entries.append({"project_name": p, "billing_type": b, "daily_entries": days})

    normalized_entries = sorted(
        normalized_entries,
        key=lambda x: (x["project_name"], x["billing_type"], json.dumps(x["daily_entries"], sort_keys=True)),
    )

    base = {
        "employee_name": (extracted.get("employee_name") or "").strip().lower(),
        "client_name": (extracted.get("client_name") or "").strip().lower(),
        "vendor_name": (extracted.get("vendor_name") or "").strip().lower(),
        "timesheet_type": (extracted.get("timesheet_type") or "").strip().lower(),
        "start_date": str(extracted.get("start_date") or ""),
        "end_date": str(extracted.get("end_date") or ""),
        "total_hours": round(float(extracted.get("total_hours") or 0), 2),
        "timesheet_status": (extracted.get("timesheet_status") or "").strip().lower(),
        "approver_signature": (extracted.get("approver_signature") or "").strip().lower(),
        "daily_entries": normalized_entries,
    }
    return _hash_json(base)


def _document_fingerprint(prechecks: list) -> str:
    hashes = sorted(
        [
            str(pc.get("file_sha256"))
            for pc in (prechecks or [])
            if pc and pc.get("file_sha256")
        ]
    )
    return hashlib.sha256("|".join(hashes).encode("utf-8")).hexdigest() if hashes else ""


def _find_truth_doc(p1_filename: str) -> Optional[Path]:
    exact = TRUTH_DOCS_DIR / p1_filename
    if exact.exists() and exact.is_file():
        return exact

    stem = Path(p1_filename).stem.lower()
    for p in TRUTH_DOCS_DIR.iterdir():
        if p.is_file() and p.stem.lower() == stem:
            return p
    return None


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


st.set_page_config(page_title="AutoStart — End to End", page_icon="⚡", layout="wide")
st.title("⚡ AutoStart — End to End")
st.caption("Upload once, run Pipeline 1 -> 2 -> 3 automatically, then redirect to Review.")

uploaded_files = st.file_uploader(
    "Choose files",
    type=["pdf", "png", "jpg", "jpeg", "webp", "gif", "bmp", "tiff", "doc", "docx", "xlsx", "xls", "csv", "ppt", "pptx", "txt"],
    accept_multiple_files=True,
    label_visibility="collapsed",
    key="autostart_uploader",
)

if uploaded_files:
    st.write(f"**{len(uploaded_files)} file(s) selected:**")
    for f in uploaded_files:
        ext = f.name.rsplit(".", 1)[-1].upper()
        st.markdown(f"- `{ext}` {f.name} `{round(f.size / 1024, 1)} KB`")
    st.info("AutoStart is enabled: processing starts automatically after upload.")

if not uploaded_files:
    st.session_state.pop("autostart_processed_sig", None)

upload_sig = ""
if uploaded_files:
    sig_payload = "|".join(sorted([f"{f.name}:{f.size}" for f in uploaded_files]))
    upload_sig = hashlib.sha256(sig_payload.encode("utf-8")).hexdigest()

run = bool(uploaded_files) and st.session_state.get("autostart_processed_sig") != upload_sig

if uploaded_files:
    if st.button("↻ Allow rerun for same upload set"):
        st.session_state.pop("autostart_processed_sig", None)
        st.rerun()

if run and uploaded_files:
    st.session_state["autostart_processed_sig"] = upload_sig
    overall = st.progress(0, text="Starting AutoStart...")
    p1_box = st.empty()
    p2_box = st.empty()
    p3_box = st.empty()

    # Pipeline 1
    p1_box.info("Pipeline 1: Started")
    submission_id = str(uuid.uuid4())
    per_file_results: List[Dict[str, Any]] = []
    p1_errors: List[Dict[str, str]] = []
    processed_index = _load_processed_index()

    for upload in uploaded_files:
        file_bytes = upload.read()
        try:
            precheck = precheck_file(upload.name, file_bytes)
            precheck["file_sha256"] = hashlib.sha256(file_bytes).hexdigest()
        except Exception as e:
            p1_errors.append({"file": upload.name, "error": str(e)})
            continue

        try:
            raw_content = extract_raw_content(upload.name, file_bytes)
            llm_output, llm_audit = invoke_bedrock_extraction(raw_content)
            llm_audit["validated_response"] = llm_output
            per_file_results.append(
                {
                    "file": upload.name,
                    "file_type": raw_content["file_type"],
                    "status": "success",
                    "extracted": llm_output,
                    "schema_error": None,
                    "llm_audit": llm_audit,
                    "precheck": precheck,
                }
            )
        except Exception as e:
            p1_errors.append({"file": upload.name, "error": str(e)})

    merged_results = merge_extractions(per_file_results)
    for item in merged_results:
        missing = validate_required_fields(item["extracted"])
        item["extracted"]["required_fields_missing"] = missing
        prechecks = item.get("prechecks") or []
        quality_flags = sorted({str(flag) for pc in prechecks for flag in (pc.get("quality_flags") or []) if pc})

        p1_manual_reasons = []
        if missing:
            p1_manual_reasons.extend(missing)
        if item.get("conflicts"):
            p1_manual_reasons.append("hour_conflicts")
        if quality_flags:
            p1_manual_reasons.append("quality_flags")

        details_fp = _details_fingerprint(item["extracted"])
        doc_fp = _document_fingerprint(prechecks)
        matched_prev = None
        for prev in processed_index.get("entries", []):
            if prev.get("details_fingerprint") == details_fp and prev.get("document_fingerprint") == doc_fp:
                matched_prev = prev
                break
        if matched_prev:
            p1_manual_reasons.append("duplicate_details_previously_processed")
            item["extracted"]["duplicate_submission"] = {
                "detected": True,
                "previous_submission_id": matched_prev.get("submission_id"),
                "previous_timestamp": matched_prev.get("timestamp"),
            }
        else:
            item["extracted"]["duplicate_submission"] = {"detected": False}

        item["extracted"]["quality_flags"] = quality_flags
        item["extracted"]["p1_manual_review_reasons"] = p1_manual_reasons
        item["extracted"]["review_decision"] = "MANUAL_REVIEW" if p1_manual_reasons else "PASS"
        item["extracted"]["details_fingerprint"] = details_fp
        item["extracted"]["document_fingerprint"] = doc_fp

    p1_log_name = f"p1_{submission_id}.json"
    p1_log = {
        "pipeline": "1 — Upload / Extract / Normalize",
        "submission_id": submission_id,
        "timestamp": datetime.utcnow().isoformat(),
        "files_processed": len(per_file_results),
        "files_failed": len(p1_errors),
        "merged_records": len(merged_results),
        "results": merged_results,
        "errors": p1_errors,
        "log_file": p1_log_name,
    }
    _write_json(LOGS_DIR / p1_log_name, p1_log)
    for item in merged_results:
        extracted = item.get("extracted", {})
        processed_index.setdefault("entries", []).append(
            {
                "submission_id": submission_id,
                "timestamp": p1_log["timestamp"],
                "files": item.get("files", []),
                "details_fingerprint": extracted.get("details_fingerprint"),
                "document_fingerprint": extracted.get("document_fingerprint"),
            }
        )
    _save_processed_index(processed_index)
    overall.progress(33, text="Pipeline 1 completed")
    p1_box.success(f"Pipeline 1: Completed ({len(merged_results)} merged record(s), {len(p1_errors)} error(s))")

    if not merged_results:
        p2_box.error("Pipeline 2: Skipped (no Pipeline 1 results)")
        p3_box.error("Pipeline 3: Skipped (no Pipeline 1 results)")
        st.stop()

    # Pipeline 2
    p2_box.info("Pipeline 2: Started")
    p2_logs: List[Tuple[Dict[str, Any], Dict[str, Any], str]] = []
    for idx, rec in enumerate(merged_results):
        extracted = rec.get("extracted") or {}
        files = rec.get("files") or []
        source_file = files[0] if files else None

        comparison_id = str(uuid.uuid4())
        p2_log_name = f"p2_{comparison_id}.json"

        if not source_file:
            comparison = {
                "comparison_result": "MISMATCH",
                "issues": [{"code": "SOURCE_FILE_MISSING", "message": "No source file found in Pipeline 1 record.", "severity": "CRITICAL"}],
                "required_fields_missing": extracted.get("required_fields_missing") or [],
                "field_results": {},
                "day_wise_comparison": [],
                "quality_flags": extracted.get("quality_flags") or [],
                "duplicate_detected": bool((extracted.get("duplicate_submission") or {}).get("detected")),
            }
            truth_extracted = {}
            truth_audit = {"error": "No source file available"}
            truth_name = None
        else:
            truth_doc = _find_truth_doc(source_file)
            if truth_doc is None:
                comparison = {
                    "comparison_result": "MISMATCH",
                    "issues": [
                        {
                            "code": "TRUTH_DOC_NOT_FOUND",
                            "message": f"No matching truth document found for {source_file}.",
                            "severity": "CRITICAL",
                        }
                    ],
                    "required_fields_missing": extracted.get("required_fields_missing") or [],
                    "field_results": {},
                    "day_wise_comparison": [],
                    "quality_flags": extracted.get("quality_flags") or [],
                    "duplicate_detected": bool((extracted.get("duplicate_submission") or {}).get("detected")),
                }
                truth_extracted = {}
                truth_audit = {"error": "Truth document not found"}
                truth_name = None
            else:
                truth_bytes = truth_doc.read_bytes()
                truth_raw = extract_raw_content(truth_doc.name, truth_bytes)
                truth_extracted, truth_audit = invoke_bedrock_extraction(truth_raw)
                prechecks = rec.get("prechecks") or []
                quality_flags = sorted({str(flag) for pc in prechecks for flag in (pc.get("quality_flags") or []) if pc})
                comparison = compare_timesheet(extracted, truth_extracted, quality_flags=quality_flags)
                truth_name = truth_doc.name

        p2_log = {
            "pipeline": "2 — Comparison",
            "comparison_id": comparison_id,
            "timestamp": datetime.utcnow().isoformat(),
            "inputs": {
                "p1_log": p1_log_name,
                "p1_record_files": files,
                "source_file_for_match": source_file,
                "truth_document_file": truth_name,
            },
            "truth_extracted": truth_extracted,
            "truth_extraction_audit": truth_audit,
            "comparison": comparison,
            "log_file": p2_log_name,
        }
        _write_json(LOGS_DIR / p2_log_name, p2_log)
        p2_logs.append((rec, p2_log, p2_log_name))

        overall.progress(33 + int(((idx + 1) / max(1, len(merged_results))) * 33), text=f"Pipeline 2 progress {idx + 1}/{len(merged_results)}")

    p2_box.success(f"Pipeline 2: Completed ({len(p2_logs)} comparison log(s))")

    # Pipeline 3
    p3_box.info("Pipeline 3: Started")
    p3_count = 0
    for idx, (rec, p2_log, p2_log_name) in enumerate(p2_logs):
        decision_id = str(uuid.uuid4())
        extracted = rec.get("extracted") or {}
        comparison = p2_log.get("comparison") or {}

        trusted = derive_trusted_context(extracted, file_types=rec.get("file_types", []))
        policy = {"block_on_duplicate": True, "trusted_template_enabled": True}
        decision = make_decision(extracted, comparison, policy=policy, trusted=trusted)

        p3_log_name = f"p3_{decision_id}.json"
        p3_log = {
            "pipeline": "3 — Decision Engine",
            "decision_id": decision_id,
            "timestamp": datetime.utcnow().isoformat(),
            "inputs": {
                "p1_log": p1_log_name,
                "p1_record_files": rec.get("files", []),
                "p2_log": p2_log_name,
                "policy": policy,
                "trusted": trusted,
            },
            "decision": decision,
            "log_file": p3_log_name,
        }
        _write_json(LOGS_DIR / p3_log_name, p3_log)
        p3_count += 1
        overall.progress(66 + int(((idx + 1) / max(1, len(p2_logs))) * 34), text=f"Pipeline 3 progress {idx + 1}/{len(p2_logs)}")

    p3_box.success(f"Pipeline 3: Completed ({p3_count} decision log(s))")
    overall.progress(100, text="AutoStart completed")
    st.success("AutoStart completed. Redirecting to Review page...")
    st.switch_page("pages/4_Review.py")
