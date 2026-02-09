# module_3/load_data.py
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2


# -------------------------
# Module 3 self-contained data paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Liv's file (JSONL)
LIV_LLM_JSONL = os.path.join(DATA_DIR, "llm_extend_applicant_data.json")

# Optional structural file if you ever have it (not required for this loader)
STRUCTURAL_JSON = os.path.join(DATA_DIR, "applicant_data_structural_clean.json")


# -------------------------
# DB config
# -------------------------
DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))


# -------------------------
# Helpers
# -------------------------
def get_conn():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)


def clean_text(x: Any) -> Optional[str]:
    """Convert to str, strip, and REMOVE NUL bytes that crash psycopg2."""
    if x is None:
        return None
    s = str(x)
    s = s.replace("\x00", "")  # critical
    s = s.strip()
    return s if s else None


def parse_date(value: Any) -> Optional[datetime.date]:
    if not value:
        return None
    s = clean_text(value)
    if not s:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


# TERM extraction from any text we have
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


DECISION_RE = re.compile(r"\b(Accepted|Rejected|Waitlisted|Wait listed|Interview)\b", re.IGNORECASE)
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE = re.compile(r"\bInternational\b", re.IGNORECASE)


def extract_status(text: str) -> Optional[str]:
    if not text:
        return None
    m = DECISION_RE.search(text)
    if not m:
        return None
    s = m.group(1).lower().replace(" ", "")
    if s == "waitlisted" or s == "waitlisted":
        return "Waitlisted"
    if s == "waitlisted" or s == "waitlisted":
        return "Waitlisted"
    # normalize casing
    return m.group(1).title()


def extract_us_intl(text: str) -> Optional[str]:
    if not text:
        return None
    if INTL_RE.search(text):
        return "International"
    if AMERICAN_RE.search(text):
        return "American"
    return None


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
                # skip malformed line
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

    # We are using Liv as truth: insert directly, do NOT depend on module_2 structural.
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

                # Liv fields
                deg = normalize_degree(r.get("masters_or_phd") or r.get("degree"))
                llm_prog = clean_text(r.get("llm-generated-program") or r.get("llm_generated_program"))
                llm_uni = clean_text(r.get("llm-generated-university") or r.get("llm_generated_university"))

                # derive term/status/us_intl from whatever text we have
                combined = " ".join([program or "", comments or "", llm_prog or "", llm_uni or ""])
                term = extract_term(combined)
                status = extract_status(clean_text(r.get("status")) or combined)
                us_intl = extract_us_intl(combined)

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
                        None,  # gpa (Liv file usually doesn't have it)
                        None,  # gre q
                        None,  # gre v
                        None,  # gre aw
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
                SUM(CASE WHEN llm_generated_program IS NOT NULL AND llm_generated_program<>'' THEN 1 ELSE 0 END)
              FROM applicants;
            """)
            llm_uni_nonnull, llm_prog_nonnull = cur.fetchone()
    finally:
        conn.close()

    print("=== load_data.py completed (Module 3 self-contained, Liv JSONL) ===")
    print(f"Read rows (JSONL): {read_rows}")
    print(f"Inserted into DB: {inserted}")
    print(f"DB total: {total}")
    print(f"DB llm uni non-null: {llm_uni_nonnull}")
    print(f"DB llm prog non-null: {llm_prog_nonnull}")


if __name__ == "__main__":
    main()
