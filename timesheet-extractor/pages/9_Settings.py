"""
Application Settings

UI page to edit runtime configuration stored in .env.
Note: restart Streamlit after saving for changes to take effect.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import streamlit as st


ENV_PATH = Path(__file__).parent.parent / ".env"


def _read_env_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def _env_map(lines: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _upsert_env(lines: List[str], updates: Dict[str, str]) -> List[str]:
    remaining = dict(updates)
    new_lines: List[str] = []

    for ln in lines:
        s = ln.strip()
        if s and (not s.startswith("#")) and ("=" in s):
            k, _ = s.split("=", 1)
            key = k.strip()
            if key in remaining:
                new_lines.append(f"{key}={remaining.pop(key)}")
                continue
        new_lines.append(ln)

    if remaining:
        if new_lines and new_lines[-1].strip() != "":
            new_lines.append("")
        for k, v in remaining.items():
            new_lines.append(f"{k}={v}")
    return new_lines


def _to_int(value: str, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _to_float(value: str, default: float) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


st.set_page_config(page_title="Settings", page_icon="⚙️", layout="wide")
st.title("⚙️ Settings")
st.caption("Configure pipeline thresholds. After save, restart Streamlit to apply changes.")

env_lines = _read_env_lines(ENV_PATH)
env = _env_map(env_lines)

st.subheader("Pipeline 1 - Pre-check thresholds")
c1, c2 = st.columns(2)

with c1:
    max_file_size_mb = st.number_input(
        "MAX_FILE_SIZE_MB",
        min_value=1,
        max_value=200,
        value=_to_int(env.get("MAX_FILE_SIZE_MB", "20"), 20),
        step=1,
        help="Maximum upload file size in MB.",
    )
    max_page_count = st.number_input(
        "MAX_PAGE_COUNT",
        min_value=1,
        max_value=500,
        value=_to_int(env.get("MAX_PAGE_COUNT", "25"), 25),
        step=1,
        help="Maximum page/frame count allowed per file.",
    )

with c2:
    min_pdf_text_chars = st.number_input(
        "MIN_PDF_TEXT_CHARS_FOR_TEXT_MODE",
        min_value=0,
        max_value=5000,
        value=_to_int(env.get("MIN_PDF_TEXT_CHARS_FOR_TEXT_MODE", "250"), 250),
        step=10,
        help="If PDF extracted text is below this, pages are rendered to images for vision extraction.",
    )
    max_pdf_render_pages = st.number_input(
        "MAX_PDF_RENDER_PAGES",
        min_value=1,
        max_value=500,
        value=_to_int(env.get("MAX_PDF_RENDER_PAGES", env.get("MAX_PAGE_COUNT", "25")), _to_int(env.get("MAX_PAGE_COUNT", "25"), 25)),
        step=1,
        help="Max pages to render as images for scanned PDFs.",
    )

st.subheader("Image quality thresholds")
q1, q2, q3 = st.columns(3)

with q1:
    min_image_min_side = st.number_input(
        "MIN_IMAGE_MIN_SIDE_PX",
        min_value=1,
        max_value=10000,
        value=_to_int(env.get("MIN_IMAGE_MIN_SIDE_PX", "800"), 800),
        step=10,
        help="Flags low_resolution if min(width, height) is below this.",
    )

with q2:
    min_image_total_px = st.number_input(
        "MIN_IMAGE_TOTAL_PX",
        min_value=1,
        max_value=100_000_000,
        value=_to_int(env.get("MIN_IMAGE_TOTAL_PX", str(1200 * 800)), 1200 * 800),
        step=10000,
        help="Flags low_resolution if width*height is below this.",
    )

with q3:
    blur_sharpness_threshold = st.number_input(
        "BLUR_SHARPNESS_THRESHOLD",
        min_value=-10.0,
        max_value=100.0,
        value=_to_float(env.get("BLUR_SHARPNESS_THRESHOLD", "9.0"), 9.0),
        step=0.5,
        help="Flags blurry_or_low_detail if sharpness score is below this.",
    )

st.info(
    "Tip: Set image thresholds to very lenient values to reduce quality flags. "
    "Example: MIN_IMAGE_MIN_SIDE_PX=1, MIN_IMAGE_TOTAL_PX=1, BLUR_SHARPNESS_THRESHOLD=-1."
)

save = st.button("💾 Save Settings", type="primary")
if save:
    updates = {
        "MAX_FILE_SIZE_MB": str(int(max_file_size_mb)),
        "MAX_PAGE_COUNT": str(int(max_page_count)),
        "MIN_PDF_TEXT_CHARS_FOR_TEXT_MODE": str(int(min_pdf_text_chars)),
        "MAX_PDF_RENDER_PAGES": str(int(max_pdf_render_pages)),
        "MIN_IMAGE_MIN_SIDE_PX": str(int(min_image_min_side)),
        "MIN_IMAGE_TOTAL_PX": str(int(min_image_total_px)),
        "BLUR_SHARPNESS_THRESHOLD": str(float(blur_sharpness_threshold)),
    }
    new_lines = _upsert_env(env_lines, updates)
    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    st.success("Settings saved to .env")
    st.warning("Restart Streamlit app to apply new values.")

