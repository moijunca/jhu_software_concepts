# module_3/load_data.py
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import psycopg2

# -------------------------
# Module 3 self-contained data paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Liv's file (JSONL) MUST be here
LIV_LLM_JSONL = os.path.join(DATA_DIR, "llm_extend_applicant_data.json")

# -------------------------
# DB config
# -------------------------
DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

FALL_2026 = "Fall 2026"


# -------------------------
# DB helpers
# -------------------------
def get_conn():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)


# -------------------------
# Cleaning / parsing helpers
# -------------------------
def clean_text(x: Any) -> Optional[str]:
    """Convert to str, strip, and REMOVE NUL bytes that crash psycopg2."""
    if x is None:
        return None
    s = str(x).replace("\x00", "")  # critical: remove NULs
    s = s.strip()
    return s if s else None


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = clean_text(x)
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[datetime.date]:
    """
    Liv's file often has: "January 31, 2026"
    Also support: "Feb 1, 2026", "2026-02-01", "02/01/2026"
    """
    s = clean_text(value)
    if not s:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


# -------------------------
# Term extraction
# -------------------------
TERM_FULL_RE = re.compile(r"\b(Fall|Spring|Summer|Winter|Autumn)\s*['’]?\s*(20\d{2}|\d{2})\b", re.IGNORECASE)
TERM_SHORT_RE = re.compile(r"\b(F|S|SU|W)\s*['’]?\s*(\d{2})\b", re.IGNORECASE)
TERM_MAP = {"F": "Fall", "S": "Spring", "SU": "Summer", "W": "Winter"}


def extract_term(text: str) -> Optional[str]:
    text = text or ""

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


# -------------------------
# Status / nationality
# -------------------------
DECISION_RE = re.compile(r"\b(Accepted|Rejected|Waitlisted|Wait listed|Interview)\b", re.IGNORECASE)
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE = re.compile(r"\bInternational\b", re.IGNORECASE)


def extract_status(text: str) -> Optional[str]:
    t = text or ""
    m = DECISION_RE.search(t)
    if not m:
        return None
    raw = m.group(1).lower().replace(" ", "")
    if raw == "waitlisted":
        return "Waitlisted"
    return m.group(1).title()


def extract_us_intl(text: str) -> Optional[str]:
    t = text or ""
    if INTL_RE.search(t):
        return "International"
    if AMERICAN_RE.search(t):
        return "American"
    return None


# -------------------------
# Degree normalization
# -------------------------
def normalize_degree(x: Any) -> Optional[str]:
    s = clean_text(x)
    if not s:
        return None
    lo = s.lower()
    if "phd" in lo or "ph.d" in lo or "doctor" in lo:
        return "PhD"
    if "master" in lo or lo in ("ms", "m.s.", "msc", "mcs", "meng"):
        return "Masters"
    if "bachelor" in lo or lo in ("bs", "b.s."):
        return "Bachelors"
    return s


# -------------------------
# GPA / GRE extraction (fixes Q3–Q6)
# -------------------------
# GPA patterns:
# - "GPA 3.88"
# - "3.88 GPA"
# - "3.88 Master's GPA"
GPA_RE = re.compile(
    r"\b(?:GPA\s*[:=]?\s*)?([0-4]\.\d{1,2})\s*(?:master'?s\s*)?(?:GPA)?\b",
    re.IGNORECASE,
)

# GRE patterns: accept several forms
GRE_Q_RE = re.compile(
    r"\b(?:GRE\s*)?(?:Q(?:uant)?|Quant|Quantitative)\s*[:=]?\s*(\d{3})\b|\b(\d{3})\s*Q\b",
    re.IGNORECASE,
)
GRE_V_RE = re.compile(
    r"\b(?:GRE\s*)?(?:V(?:erb)?|Verbal)\s*[:=]?\s*(\d{3})\b|\b(\d{3})\s*V\b",
    re.IGNORECASE,
)
GRE_AW_RE = re.compile(
    r"\b(?:AWA|AW|Analytical Writing)\s*[:=]?\s*([0-6]\.\d)\b",
    re.IGNORECASE,
)


def extract_gpa_gre(text: str):
    t = text or ""

    gpa = None
    m = GPA_RE.search(t)
    if m:
        gpa = to_float(m.group(1))

    gre_q = None
    m = GRE_Q_RE.search(t)
    if m:
        gre_q = to_float(m.group(1) or m.group(2))

    gre_v = None
    m = GRE_V_RE.search(t)
    if m:
        gre_v = to_float(m.group(1) or m.group(2))

    gre_aw = None
    m = GRE_AW_RE.search(t)
    if m:
        gre_aw = to_float(m.group(1))

    return gpa, gre_q, gre_v, gre_aw


# -------------------------
# JSONL loader
# -------------------------
def load_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    """Stream JSONL (one object per line)."""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def ensure_index():
    """
    Ensure table is idempotent for repeated loads:
    - remove duplicates by (url, program, comments)
    - create unique index if missing
    """
    sql = """
    DO $$
    BEGIN
      WITH ranked AS (
        SELECT
          p_id,
          ROW_NUMBER() OVER (
            PARTITION BY
              COALESCE(url, ''),
              COALESCE(program, ''),
              COALESCE(comments, '')
            ORDER BY p_id
          ) AS rn
        FROM applicants
      )
      DELETE FROM applicants
      WHERE p_id IN (SELECT p_id FROM ranked WHERE rn > 1);

      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname='public' AND indexname='applicants_sig_unique'
      ) THEN
        CREATE UNIQUE INDEX applicants_sig_unique
        ON applicants (url, program, comments);
      END IF;
    END $$;
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def main():
    if not os.path.exists(LIV_LLM_JSONL):
        raise FileNotFoundError(
            f"Missing Liv JSONL at {LIV_LLM_JSONL}\n"
            f"Fix: cp module_2/llm_extend_applicant_data.json module_3/data/llm_extend_applicant_data.json"
        )

    ensure_index()

    insert_sql = """
    INSERT INTO applicants (
      program, comments, date_added, url,
      status, term, us_or_international,
      gpa, gre, gre_v, gre_aw,
      degree, llm_generated_program, llm_generated_university
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (url, program, comments) DO NOTHING;
    """

    inserted = 0
    read_rows = 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for r in load_jsonl(LIV_LLM_JSONL):
                read_rows += 1

                program = clean_text(r.get("program"))
                comments = clean_text(r.get("comments"))
                url = clean_text(r.get("url"))
                date_added = parse_date(r.get("date_added") or r.get("date_added_raw"))

                # degree (Liv key: masters_or_phd)
                deg = normalize_degree(r.get("masters_or_phd") or r.get("degree"))

                # llm fields (support both key styles)
                llm_prog = clean_text(r.get("llm-generated-program") or r.get("llm_generated_program"))
                llm_uni = clean_text(r.get("llm-generated-university") or r.get("llm_generated_university"))

                # status may exist in Liv file; otherwise derive from text
                status = clean_text(r.get("status"))
                combined = " ".join([program or "", comments or "", status or "", llm_prog or "", llm_uni or ""])

                # term
                term = extract_term(combined)

                # IMPORTANT fallback: if no term but date_added is in 2026 -> Fall 2026
                if term is None and date_added and date_added.year == 2026:
                    term = FALL_2026

                # derive status/us_intl if missing
                if not status:
                    status = extract_status(combined)
                us_intl = extract_us_intl(combined)

                # GPA/GRE extraction from combined text
                gpa, gre_q, gre_v, gre_aw = extract_gpa_gre(combined)

                cur.execute(
                    insert_sql,
                    (
                        program,
                        comments,
                        date_added,
                        url,
                        status,
                        term,
                        us_intl,
                        gpa,
                        gre_q,   # stored in column "gre" (Quant)
                        gre_v,
                        gre_aw,
                        deg,
                        llm_prog,
                        llm_uni,
                    ),
                )
                inserted += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    # Report
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            total = cur.fetchone()[0]

            cur.execute("""
              SELECT
                SUM(CASE WHEN llm_generated_university IS NOT NULL AND llm_generated_university<>'' THEN 1 ELSE 0 END),
                SUM(CASE WHEN llm_generated_program IS NOT NULL AND llm_generated_program<>'' THEN 1 ELSE 0 END),
                SUM(CASE WHEN gpa IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN gre IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN term IS NOT NULL AND term<>'' THEN 1 ELSE 0 END)
              FROM applicants;
            """)
            llm_uni_nonnull, llm_prog_nonnull, gpa_cnt, greq_cnt, term_cnt = cur.fetchone()
    finally:
        conn.close()

    print("=== load_data.py completed (Module 3 self-contained, Liv JSONL) ===")
    print(f"Read rows (JSONL): {read_rows}")
    print(f"Inserted into DB: {inserted}")
    print(f"DB total: {total}")
    print(f"DB llm uni non-null: {llm_uni_nonnull}")
    print(f"DB llm prog non-null: {llm_prog_nonnull}")
    print(f"DB GPA non-null: {gpa_cnt}")
    print(f"DB GRE-Q (gre) non-null: {greq_cnt}")
    print(f"DB term non-null: {term_cnt}")


if __name__ == "__main__":
    main()
