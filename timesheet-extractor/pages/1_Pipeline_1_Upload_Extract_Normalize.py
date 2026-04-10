"""
Pipeline 1 — Upload / Extract / Normalize
Steps covered:
  1. File upload & pre-check
  2. Pre-validation & quality check  → MANUAL_REVIEW on fail
  3. Data extraction (Vision LLM / Python libs)
  4. Normalization (AWS Bedrock → strict JSON schema)
"""

import json
import uuid
import hashlib
import pandas as pd
import streamlit as st
from datetime import datetime
from pathlib import Path

# Go up one level so imports resolve from timesheet-extractor/
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from extractor import extract_raw_content, invoke_bedrock_extraction, validate_required_fields, precheck_file
from schemas import TimesheetReviewResponseSchema
from merger import merge_extractions

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
PROCESSED_INDEX_FILE = LOGS_DIR / "processed_submissions_index.json"


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
    """
    Build a stable fingerprint for "same employee + same details".
    """
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


def _display_day_label(work_date_val, day_name_val) -> str | None:
    """
    Prefer extracted day_name; if missing, derive from work_date.
    Normalize short/long weekday forms for stable UI display.
    """
    raw = str(day_name_val or "").strip()
    day_map = {
        "mon": "Monday", "monday": "Monday",
        "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
        "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
        "thu": "Thursday", "thur": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
        "fri": "Friday", "friday": "Friday",
        "sat": "Saturday", "saturday": "Saturday",
        "sun": "Sunday", "sunday": "Sunday",
    }
    if raw:
        key = raw.lower().replace(".", "")
        return day_map.get(key, raw)
    try:
        return datetime.fromisoformat(str(work_date_val)).strftime("%A")
    except Exception:
        return None

st.set_page_config(page_title="Pipeline 1 — Upload / Extract / Normalize", page_icon="📂", layout="wide")

# If navigated via "Start"/"Start Upload", clear prior session state for a fresh run.
if st.session_state.pop("reset_to_p1", False):
    for k in [
        "p1_loaded_log",
        "p2_loaded_log",
        "p3_loaded_log",
        "p2_prefill",
        "p3_prefill",
    ]:
        st.session_state.pop(k, None)
    st.session_state["p1_uploader_version"] = st.session_state.get("p1_uploader_version", 0) + 1

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("📂 Pipeline 1 — Upload / Extract / Normalize")
st.caption("Steps 1 → 2 → 3 → 4 from the Zero-Touch Timesheet Approval flow.")

# Pipeline step indicator
cols = st.columns(4)
cols[0].success("**Step 1** — File Upload & Pre-check")
cols[1].success("**Step 2** — Pre-validation & Quality Check")
cols[2].success("**Step 3** — Data Extraction (Vision LLM)")
cols[3].success("**Step 4** — Normalization (AWS Bedrock)")

st.divider()

# ---------------------------------------------------------------------------
# Sidebar — logs
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("📋 Pipeline 1 Logs")
    log_files = sorted(LOGS_DIR.glob("p1_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not log_files:
        st.info("No logs yet.")
    else:
        for lf in log_files[:20]:
            data = json.loads(lf.read_text())
            data["_loaded_from"] = lf.name
            ts   = data.get("timestamp", "")[:19].replace("T", " ")
            n_ok = data.get("files_processed", 0)
            n_err= data.get("files_failed", 0)
            if st.button(f"{ts}  ✅{n_ok} ❌{n_err}", key=lf.stem, use_container_width=True):
                st.session_state["p1_loaded_log"] = data

# ---------------------------------------------------------------------------
# Step 1 — File Upload
# ---------------------------------------------------------------------------
st.subheader("Step 1 — File Upload")
st.caption("Upload one or more timesheet files. Multiple files for the same employee + period will be auto-merged.")

uploaded_files = st.file_uploader(
    "Choose files",
    type=["pdf","png","jpg","jpeg","webp","gif","bmp","tiff",
          "doc","docx","xlsx","xls","csv","ppt","pptx","txt"],
    accept_multiple_files=True,
    label_visibility="collapsed",
    key=f"p1_uploader_{st.session_state.get('p1_uploader_version', 0)}",
)

if uploaded_files:
    st.write(f"**{len(uploaded_files)} file(s) selected:**")
    for f in uploaded_files:
        ext = f.name.rsplit(".", 1)[-1].upper()
        st.markdown(f"- `{ext}` &nbsp; {f.name} &nbsp; `{round(f.size/1024, 1)} KB`")

run = st.button("▶ Run Pipeline 1", type="primary", disabled=not uploaded_files)

# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------
if run and uploaded_files:
    submission_id = str(uuid.uuid4())
    per_file_results = []
    errors = []
    processed_index = _load_processed_index()

    # Step 2 status container
    st.divider()
    st.subheader("Step 2 — Pre-validation & Quality Check")
    pre_check_log = st.container()

    # Step 3+4 status container
    st.subheader("Step 3+4 — Extraction & Normalization")
    progress = st.progress(0, text="Starting...")

    for i, upload in enumerate(uploaded_files):
        progress.progress(i / len(uploaded_files), text=f"Processing: {upload.name}")
        file_bytes = upload.read()
        ext = upload.name.rsplit(".", 1)[-1].lower()

        # --- Step 2: Pre-validation ---
        precheck = None
        with pre_check_log:
            try:
                precheck = precheck_file(upload.name, file_bytes)
                precheck["file_sha256"] = hashlib.sha256(file_bytes).hexdigest()
                msg = f"✅ `{upload.name}` — Pre-check passed"
                if precheck.get("page_count") is not None:
                    msg += f" | pages: `{precheck['page_count']}`"
                st.success(msg)

                qf = precheck.get("quality_flags") or []
                if qf:
                    st.warning(f"⚠️ `{upload.name}` — Quality flags: `{', '.join(qf)}`")
            except Exception as e:
                st.error(f"❌ `{upload.name}` — {e} → **MANUAL_REVIEW**")
                errors.append({"file": upload.name, "error": str(e)})
                continue

        # --- Step 3: Extraction ---
        try:
            raw_content = extract_raw_content(upload.name, file_bytes)

            # --- Step 4: Normalization via Bedrock ---
            llm_output, llm_audit = invoke_bedrock_extraction(raw_content)

            # llm_output is already Pydantic-validated via with_structured_output
            validated_dict = llm_output
            schema_error   = None

            # Store pydantic result in audit
            llm_audit["validated_response"] = validated_dict

            per_file_results.append({
                "file":        upload.name,
                "file_type":   raw_content["file_type"],
                "status":      "success",
                "extracted":   validated_dict,
                "schema_error": schema_error,
                "llm_audit":   llm_audit,
                "precheck":    precheck,
            })

        except Exception as e:
            errors.append({"file": upload.name, "error": str(e)})
            with pre_check_log:
                st.error(f"❌ `{upload.name}` — Extraction failed: {e} → **MANUAL_REVIEW**")

    progress.progress(0.9, text="Merging multi-file results...")

    # Merge files for same employee + period
    merged_results = merge_extractions(per_file_results)

    # Post-merge required field validation
    for item in merged_results:
        missing = validate_required_fields(item["extracted"])
        item["extracted"]["required_fields_missing"] = missing
        prechecks = item.get("prechecks") or []
        quality_flags = sorted(
            {
                str(flag)
                for pc in prechecks
                for flag in (pc.get("quality_flags") or [])
                if pc
            }
            | {str(f) for f in (item["extracted"].get("extraction_quality_flags") or [])}
        )
        p1_manual_reasons = []
        if missing:
            p1_manual_reasons.extend(missing)
        if item.get("conflicts"):
            p1_manual_reasons.append("hour_conflicts")
        if quality_flags:
            p1_manual_reasons.append("quality_flags")

        # Cross-submission duplicate check: same document + same extracted details
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

    progress.progress(1.0, text="Pipeline 1 complete.")

    # Save log
    log_file_name = f"p1_{submission_id}.json"
    log_entry = {
        "pipeline":       "1 — Upload / Extract / Normalize",
        "submission_id":  submission_id,
        "timestamp":      datetime.utcnow().isoformat(),
        "files_processed": len(per_file_results),
        "files_failed":   len(errors),
        "merged_records": len(merged_results),
        "results":        merged_results,
        "errors":         errors,
        "log_file":       log_file_name,
    }
    (LOGS_DIR / log_file_name).write_text(
        json.dumps(log_entry, indent=2, default=str)
    )
    # Update processed index after run
    for item in merged_results:
        extracted = item.get("extracted", {})
        processed_index.setdefault("entries", []).append(
            {
                "submission_id": submission_id,
                "timestamp": log_entry["timestamp"],
                "files": item.get("files", []),
                "details_fingerprint": extracted.get("details_fingerprint"),
                "document_fingerprint": extracted.get("document_fingerprint"),
            }
        )
    _save_processed_index(processed_index)
    st.session_state["p1_loaded_log"] = log_entry
    st.rerun()

# ---------------------------------------------------------------------------
# Render results
# ---------------------------------------------------------------------------
log_data = st.session_state.get("p1_loaded_log")

if log_data:
    st.divider()
    st.subheader("Results")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Submission ID",   log_data["submission_id"][:8] + "...")
    c2.metric("Files Processed", log_data.get("files_processed", 0))
    c3.metric("Merged Records",  log_data.get("merged_records", 0))
    c4.metric("Failed",          log_data.get("files_failed", 0))

    for err in log_data.get("errors", []):
        st.error(f"❌ **{err['file']}** — {err['error']} → MANUAL_REVIEW")

    merged_results = log_data.get("results", [])
    if not merged_results:
        st.warning("No results to display.")
        st.stop()

    tab_labels = []
    for item in merged_results:
        files     = item.get("files", [])
        label     = " + ".join(files) if len(files) > 1 else (files[0] if files else "unknown")
        missing   = item["extracted"].get("required_fields_missing", [])
        conflicts = item.get("conflicts", [])
        has_quality = bool(item["extracted"].get("quality_flags"))
        icon = "🔴" if (conflicts or has_quality) else ("⚠️" if missing else "✅")
        tab_labels.append(f"{icon} {label}")

    tabs = st.tabs(tab_labels)

    for tab, item in zip(tabs, merged_results):
        with tab:
            d         = item["extracted"]
            missing   = d.get("required_fields_missing", [])
            decision  = d.get("review_decision", "UNKNOWN")
            conflicts = item.get("conflicts", [])
            schema_err= item.get("schema_error")
            files     = item.get("files", [])

            if len(files) > 1:
                st.info(f"🔀 Merged from **{len(files)} files:** {', '.join(files)}")

            prechecks = item.get("prechecks", [])
            if prechecks:
                with st.expander("Pre-check details"):
                    st.json(prechecks)

            if conflicts:
                st.error(f"⚡ **{len(conflicts)} hour conflict(s)** — routed to MANUAL_REVIEW")
                with st.expander("View conflicts"):
                    for c in conflicts:
                        loc = c.get("work_date") or c.get("day_name") or "—"
                        st.write(
                            f"- `{loc}` | Project: `{c.get('project_name','—')}` | "
                            f"Hours found: `{c['hours_found']}`"
                        )

            qflags = d.get("quality_flags", [])
            if qflags:
                st.warning(f"⚠️ Quality issue(s) detected in Pipeline 1: `{', '.join(qflags)}` → MANUAL_REVIEW")

            dup = d.get("duplicate_submission") or {}
            if dup.get("detected"):
                st.error(
                    f"🚫 Duplicate detected: same document + same details previously processed "
                    f"(submission: `{dup.get('previous_submission_id')}` at `{dup.get('previous_timestamp')}`) "
                    f"→ MANUAL_REVIEW"
                )

            if decision == "PASS" and not conflicts:
                st.success("✅ Decision: **PASS** — All required fields present, no conflicts")
            else:
                reasons = d.get("p1_manual_review_reasons") or (missing + (["hour_conflicts"] if conflicts else []))
                st.warning(f"⚠️ Decision: **MANUAL_REVIEW** — {', '.join(reasons)}")

            if schema_err:
                st.error(f"Pydantic schema warning: {schema_err}")

            view = st.radio(
                "View mode", ["Structured", "Raw JSON", "LLM Audit"],
                horizontal=True, key=f"view_{'_'.join(files)}",
            )

            if view == "Raw JSON":
                st.json(d)
                continue

            if view == "LLM Audit":
                audits = item.get("llm_audits", [])
                if not audits:
                    st.info("No audit data in this log. Re-run the pipeline to capture LLM audit.")
                else:
                    for audit in audits:
                        st.markdown(f"#### 📄 {audit.get('file', '')}")

                        # Show schema enforcement status
                        mode = audit.get("schema_enforcement", "unknown")
                        if "✅" in mode:
                            st.success(f"Schema enforcement: {mode}")
                        elif "⚠️" in mode:
                            st.warning(f"Schema enforcement: {mode}")
                        else:
                            st.info(f"Schema enforcement: {mode}")

                        sent = audit.get("sent", {})
                        col_l, col_r = st.columns(2)
                        with col_l:
                            st.markdown("**📤 Sent to LLM**")
                            st.json({
                                "file_type":  sent.get("file_type"),
                                "is_image":   sent.get("is_image"),
                                "has_images": sent.get("has_images"),
                                "raw_text":   sent.get("raw_text", ""),
                            })
                        with col_r:
                            st.markdown("**✅ Validated Response (Pydantic Schema)**")
                            st.json(audit.get("validated_response", {}))
                        st.divider()
                continue

            # Core fields
            st.subheader("Core Fields")
            core_fields = [
                "employee_name", "client_name", "vendor_name",
                "timesheet_type", "start_date", "end_date",
                "total_hours", "timesheet_status", "approver_signature",
            ]
            cols = st.columns(3)
            for idx, field in enumerate(core_fields):
                val = d.get(field)
                is_crit = field in missing or (
                    field in ("timesheet_status", "approver_signature")
                    and "timesheet_status_or_approver_signature" in missing
                ) or (
                    field in ("start_date", "end_date")
                    and "start_date_or_end_date" in missing
                ) or (
                    field == "start_date"
                    and "day_only_weekday_rows" in missing
                )
                with cols[idx % 3]:
                    if (val is None or val == "") and field == "start_date" and "day_only_weekday_rows" in missing:
                        st.info(f"**{field}**\n\n`null` (not printed — weekday-only rows)")
                    elif val is None or val == "":
                        st.error(f"**{field}**\n\n`null`")
                    elif is_crit:
                        st.warning(f"**{field}**\n\n{val}")
                    else:
                        st.success(f"**{field}**\n\n{val}")

            # Day-wise table
            entries = d.get("daily_entries") or []
            if entries:
                st.subheader("Day-Wise Entries")
                rows = []
                for proj in entries:
                    for day in proj.get("daily_entries") or []:
                        disp_day = _display_day_label(day.get("work_date"), day.get("day_name"))
                        rows.append({
                            "Date":         day.get("work_date"),
                            "Day":          disp_day,
                            "Project":      proj.get("project_name") or "—",
                            "Billing Type": proj.get("billing_type"),
                            "Hours":        day.get("hours_worked"),
                        })
                if rows:
                    df = pd.DataFrame(rows)
                    has_cal = any(r.get("Date") not in (None, "") for r in rows)
                    if has_cal:
                        df = df.sort_values("Date").reset_index(drop=True)
                    else:
                        _order = [
                            "monday", "tuesday", "wednesday", "thursday",
                            "friday", "saturday", "sunday",
                        ]
                        df["_ord"] = df["Day"].astype(str).str.lower().map(
                            {d: i for i, d in enumerate(_order)}
                        ).fillna(99)
                        df = df.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)
                    df["Hours"] = df["Hours"].apply(lambda x: round(x, 2))

                    def color_billing(val):
                        return {
                            "Billable":     "background-color:#1e3a5f;color:#93c5fd",
                            "Non-Billable": "background-color:#1c2a1c;color:#86efac",
                            "OT":           "background-color:#3b2a0a;color:#fcd34d",
                            "DT":           "background-color:#2a1a3b;color:#c4b5fd",
                        }.get(val, "")

                    st.dataframe(
                        df.style.map(color_billing, subset=["Billing Type"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                    calc_total = round(sum(r["Hours"] for r in rows if r["Hours"]), 2)
                    st.caption(
                        f"Calculated from entries: **{calc_total}h** | "
                        f"Reported total_hours: **{d.get('total_hours')}h**"
                    )
            else:
                st.info("No daily entries extracted.")

            weekly_entries = d.get("weekly_entries") or []
            if weekly_entries:
                st.subheader("Weekly Entries")
                st.dataframe(pd.DataFrame(weekly_entries), use_container_width=True, hide_index=True)

            monthly_entries = d.get("monthly_entries") or []
            if monthly_entries:
                st.subheader("Monthly Entries")
                st.dataframe(pd.DataFrame(monthly_entries), use_container_width=True, hide_index=True)

            # Bottom-centered next-step action
            p1_log_name = log_data.get("log_file") or log_data.get("_loaded_from")
            if p1_log_name:
                st.divider()
                c_left, c_mid, c_right = st.columns([1, 2, 1])
                with c_mid:
                    if st.button("➡️ Next Step: Pipeline 2", key=f"next_to_p2_{'_'.join(files)}", type="primary", use_container_width=True):
                        st.session_state["p2_prefill"] = {
                            "p1_log": p1_log_name,
                            "p1_record_files": files,
                        }
                        st.switch_page("pages/2_Pipeline_2_Comparison.py")
