"""
clean.py

Deterministic structural cleaning for GradCafe applicant data.

What this does (NO LLM here):
- trims whitespace
- normalizes empty strings to None
- parses/extracts required fields from status/comments/program text:
  - program (raw combined program+university string, for traceability)
  - comments
  - date_added
  - url (row-specific url from scraper)
  - status (Accepted/Rejected/Waitlisted/Interview if detectable)
  - acceptance/rejection/interview date (if present in text)
  - term (Fall/Spring/Summer/Winter + year if present)
  - us_or_international (American / International / Other if detectable)
  - gpa, gre, gre_v, gre_aw (if present)
  - degree (Masters / PhD if detectable)

Semantic normalization (clean program/university names) is handled by:
module_2/llm_hosting (LLM standardizer).
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# Regex helpers
# -------------------------
TERM_FULL_RE = re.compile(
    r"\b(Fall|Spring|Summer|Winter|Autumn)\s*['’]?\s*(20\d{2}|\d{2})\b",
    re.IGNORECASE,
)
TERM_SHORT_RE = re.compile(r"\b(F|S|SU|W)\s*['’]?\s*(\d{2})\b", re.IGNORECASE)
TERM_MAP = {"F": "Fall", "S": "Spring", "SU": "Summer", "W": "Winter"}

# Decision keyword (keep simple + explicit)
DECISION_RE = re.compile(r"\b(Accepted|Rejected|Waitlisted|Interview)\b", re.IGNORECASE)

# Dates written like "Accepted on 29 Jan", "Rejected on 1 Feb", etc.
# We don't force year here; we store it as raw string date_event_text for traceability.
EVENT_DATE_RE = re.compile(
    r"\b(Accepted|Rejected|Waitlisted|Interview)\s+on\s+([0-3]?\d)\s+([A-Za-z]{3,9})\b",
    re.IGNORECASE,
)

# Nationality
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE = re.compile(r"\bInternational\b", re.IGNORECASE)
OTHER_RE = re.compile(r"\bOther\b", re.IGNORECASE)

# Degree
PHD_RE = re.compile(r"\b(PhD|Ph\.D\.|Doctorate)\b", re.IGNORECASE)
MASTERS_RE = re.compile(r"\b(Masters|Master's|MS|M\.S\.|MSc|MEng|M\.Eng\.)\b", re.IGNORECASE)

# Metrics
# NOTE: Quant GRE is often written like "GRE 305" meaning total, or "GRE Q 165"
GPA_RE = re.compile(r"\bGPA\s*[:=]?\s*([0-4](?:\.\d{1,2})?)\b", re.IGNORECASE)

GRE_TOTAL_RE = re.compile(r"\bGRE\s*[:=]?\s*(\d{3})\b", re.IGNORECASE)
GRE_Q_RE = re.compile(r"\bGRE\s*Q(?:uant)?\s*[:=]?\s*(\d{3})\b", re.IGNORECASE)
GRE_V_RE = re.compile(r"\bGRE\s*V(?:erb)?\s*[:=]?\s*(\d{3})\b", re.IGNORECASE)
GRE_AW_RE = re.compile(r"\b(?:AWA|AW|Analytical Writing)\s*[:=]?\s*([0-6](?:\.\d)?)\b", re.IGNORECASE)


# -------------------------
# Basic cleaners
# -------------------------
def _clean_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _parse_date_added(raw: Optional[str]) -> Optional[str]:
    """
    Keep as ISO date string YYYY-MM-DD if parsable, else None.
    GradCafe dates often look like "February 01, 2026" or "Feb 1, 2026".
    """
    if not raw:
        return None
    s = raw.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _extract_term(text: str) -> Optional[str]:
    if not text:
        return None

    m = TERM_FULL_RE.search(text)
    if m:
        season = m.group(1).title()
        year = m.group(2)
        if len(year) == 2:
            year = f"20{year}"
        if season == "Autumn":
            season = "Fall"
        return f"{season} {year}"

    m = TERM_SHORT_RE.search(text)
    if m:
        code = m.group(1).upper()
        year2 = m.group(2)
        season = TERM_MAP.get(code)
        if season:
            return f"{season} 20{year2}"

    return None


def _to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except ValueError:
        return None


def _extract_metrics(text: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns (gpa, gre_q_or_total, gre_v, gre_aw).
    For gre field: if GRE Q is present use that; else if GRE total present use that.
    """
    if not text:
        return None, None, None, None

    gpa = None
    m = GPA_RE.search(text)
    if m:
        gpa = _to_float(m.group(1))

    gre = None
    m = GRE_Q_RE.search(text)
    if m:
        gre = _to_float(m.group(1))
    else:
        m = GRE_TOTAL_RE.search(text)
        if m:
            gre = _to_float(m.group(1))

    gre_v = None
    m = GRE_V_RE.search(text)
    if m:
        gre_v = _to_float(m.group(1))

    gre_aw = None
    m = GRE_AW_RE.search(text)
    if m:
        gre_aw = _to_float(m.group(1))

    return gpa, gre, gre_v, gre_aw


def _extract_status(text: str) -> Optional[str]:
    if not text:
        return None
    m = DECISION_RE.search(text)
    return m.group(1).title() if m else None


def _extract_event_date_text(text: str) -> Optional[str]:
    """
    Keep original event date snippet for traceability, e.g.:
    "Accepted on 29 Jan"
    """
    if not text:
        return None
    m = EVENT_DATE_RE.search(text)
    if not m:
        return None
    status = m.group(1).title()
    day = m.group(2)
    month = m.group(3)
    return f"{status} on {day} {month}"


def _extract_us_intl(text: str) -> Optional[str]:
    if not text:
        return None
    if INTL_RE.search(text):
        return "International"
    if AMERICAN_RE.search(text):
        return "American"
    if OTHER_RE.search(text):
        return "Other"
    return None


def _extract_degree(text: str) -> Optional[str]:
    if not text:
        return None
    if PHD_RE.search(text):
        return "PhD"
    if MASTERS_RE.search(text):
        return "Masters"
    return None


# -------------------------
# Cleaning logic
# -------------------------
REQUIRED_INPUT_FIELDS = [
    "program_university_raw",
    "status_raw",
    "date_added_raw",
    "comments_raw",
    "source_url",
]


def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Output schema used downstream:
    - We keep original raw fields for traceability
    - We add extracted structured fields needed by the assignment + module_3 load
    """
    raw_program = _clean_value(record.get("program_university_raw"))
    raw_status = _clean_value(record.get("status_raw"))
    raw_date_added = _clean_value(record.get("date_added_raw"))
    raw_comments = _clean_value(record.get("comments_raw"))
    raw_url = _clean_value(record.get("source_url"))

    # Combine text sources for extraction
    combined = " ".join([raw_program or "", raw_status or "", raw_comments or ""]).strip()

    # Extract fields
    status = _extract_status(combined) or raw_status
    term = _extract_term(combined)
    us_intl = _extract_us_intl(combined)
    degree = _extract_degree(combined)
    gpa, gre, gre_v, gre_aw = _extract_metrics(combined)
    event_date_text = _extract_event_date_text(combined)

    # Parse date_added into ISO when possible (keeps it consistent for SQL)
    date_added_iso = _parse_date_added(raw_date_added)

    # IMPORTANT:
    # LLM standardizer expects key "program" (it standardizes program/university)
    # So we provide it here as the raw combined string.
    cleaned = {
        # ---- original raw (traceability) ----
        "program_university_raw": raw_program,
        "status_raw": raw_status,
        "date_added_raw": raw_date_added,
        "comments_raw": raw_comments,
        "source_url": raw_url,

        # ---- structured fields required by assignment ----
        "program": raw_program,            # keep original for reproducibility (grader note)
        "comments": raw_comments,
        "date_added": date_added_iso,      # ISO yyyy-mm-dd if possible
        "url": raw_url,                    # row-specific URL from scrape.py
        "status": status,                  # Accepted/Rejected/Waitlisted/Interview if detectable
        "term": term,                      # e.g., "Fall 2026"
        "US/International": us_intl,        # American/International/Other
        "gpa": gpa,
        "gre": gre,
        "gre_v": gre_v,
        "gre_aw": gre_aw,
        "Degree": degree,

        # Optional but useful for traceability
        "decision_date_text": event_date_text,
    }

    # Ensure required input keys exist even if None
    for k in REQUIRED_INPUT_FIELDS:
        cleaned.setdefault(k, None)

    return cleaned


def clean_dataset(input_path: str, output_path: str) -> None:
    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    records = payload.get("records", [])
    cleaned_records = [clean_record(r) for r in records]

    output_payload = {
        **payload,
        "records": cleaned_records,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    clean_dataset(
        "module_2/applicant_data.json",
        "module_2/applicant_data_structural_clean.json",
    )
