import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

# -------------------------
# Paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

M2_STRUCTURAL = os.path.join(PROJECT_DIR, "module_2", "applicant_data_structural_clean.json")
M2_LLM_JSONL = os.path.join(PROJECT_DIR, "module_2", "llm_extend_applicant_data.json")  # JSONL

# -------------------------
# DB config
# -------------------------
DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

# -------------------------
# Extractors
# -------------------------
TERM_FULL_RE = re.compile(r"\b(Fall|Spring|Summer|Winter|Autumn)\s*['’]?\s*(20\d{2}|\d{2})\b", re.IGNORECASE)
TERM_SHORT_RE = re.compile(r"\b(F|S|SU|W)\s*['’]?\s*(\d{2})\b", re.IGNORECASE)
TERM_MAP = {"F": "Fall", "S": "Spring", "SU": "Summer", "W": "Winter"}

GPA_RE = re.compile(r"\bGPA\s*([0-4]\.\d{1,2})\b", re.IGNORECASE)
GRE_Q_RE = re.compile(r"\bGRE\s*Q(?:uant)?\s*[:=]?\s*(\d{3})\b", re.IGNORECASE)
GRE_V_RE = re.compile(r"\bGRE\s*V(?:erb)?\s*[:=]?\s*(\d{3})\b", re.IGNORECASE)
GRE_AW_RE = re.compile(r"\b(?:AWA|AW|Analytical Writing)\s*[:=]?\s*([0-6]\.\d)\b", re.IGNORECASE)

DECISION_RE = re.compile(r"\b(Accepted|Rejected|Waitlisted|Interview)\b", re.IGNORECASE)
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE = re.compile(r"\bInternational\b", re.IGNORECASE)

DEGREE_RE = re.compile(
    r"\b(PhD|Ph\.D\.|Doctorate|Masters|Master's|MS|M\.S\.|MSc|Bachelors|Bachelor's|BS|B\.S\.)\b",
    re.IGNORECASE
)


def get_conn():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)


def parse_date(value: Any) -> Optional[datetime.date]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


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


def normalize_degree(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    d = raw.lower()
    if "ph" in d or "doctor" in d:
        return "PhD"
    if "master" in d or d in ("ms", "m.s.", "msc"):
        return "Masters"
    if "bachelor" in d or d in ("bs", "b.s."):
        return "Bachelors"
    return raw


def extract_fields(text: str) -> Tuple[
    Optional[str], Optional[str], Optional[str],
    Optional[float], Optional[float], Optional[float], Optional[float],
    Optional[str]
]:
    text = text or ""

    term = extract_term(text)

    gpa = None
    m = GPA_RE.search(text)
    if m:
        gpa = to_float(m.group(1))

    gre_q = None
    m = GRE_Q_RE.search(text)
    if m:
        gre_q = to_float(m.group(1))

    gre_v = None
    m = GRE_V_RE.search(text)
    if m:
        gre_v = to_float(m.group(1))

    gre_aw = None
    m = GRE_AW_RE.search(text)
    if m:
        gre_aw = to_float(m.group(1))

    status = None
    m = DECISION_RE.search(text)
    if m:
        status = m.group(1).title()

    us_intl = None
    if INTL_RE.search(text):
        us_intl = "International"
    elif AMERICAN_RE.search(text):
        us_intl = "American"

    degree_guess = None
    m = DEGREE_RE.search(text)
    if m:
        degree_guess = normalize_degree(m.group(1))

    return term, status, us_intl, gpa, gre_q, gre_v, gre_aw, degree_guess


def load_structural() -> List[Dict[str, Any]]:
    with open(M2_STRUCTURAL, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict) and "records" in payload:
        return payload["records"]
    if isinstance(payload, list):
        return payload
    raise ValueError("Unexpected format in applicant_data_structural_clean.json")


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def ensure_index():
    """
    Make the table idempotent for repeated loads:
    1) Remove duplicates that would violate the signature uniqueness
    2) Create the unique index if it does not exist
    """
    sql = """
    DO $$
    BEGIN
      -- 1) Remove duplicates (keep the smallest p_id)
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

      -- 2) Create unique index if missing
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
    if not os.path.exists(M2_STRUCTURAL):
        raise FileNotFoundError(f"Missing: {M2_STRUCTURAL}")
    if not os.path.exists(M2_LLM_JSONL):
        raise FileNotFoundError(f"Missing: {M2_LLM_JSONL}")

    structural = load_structural()
    llm_rows = load_jsonl(M2_LLM_JSONL)

    # Lookup LLM by (url, program, comments) because date_added is often blank in JSONL
    llm_lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in llm_rows:
        url = (r.get("url") or "").strip()
        program = (r.get("program") or "").strip()
        comments = (r.get("comments") or "").strip()
        if url and program:
            llm_lookup[(url, program, comments)] = r

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

    update_sql = """
    UPDATE applicants
    SET llm_generated_program = COALESCE(NULLIF(%s,''), llm_generated_program),
        llm_generated_university = COALESCE(NULLIF(%s,''), llm_generated_university)
    WHERE url=%s AND program=%s AND comments=%s;
    """

    inserted = 0
    updated = 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Insert structural rows
            for r in structural:
                program = (r.get("program_university_raw") or "").strip() or None
                comments = (r.get("comments_raw") or "").strip() or None
                url = (r.get("source_url") or "").strip() or None
                status_raw = (r.get("status_raw") or "").strip() or None
                date_added = parse_date(r.get("date_added_raw"))

                combined = " ".join([program or "", comments or "", status_raw or ""])
                term, status_ex, us_intl, gpa, gre_q, gre_v, gre_aw, degree_guess = extract_fields(combined)

                cur.execute(insert_sql, (
                    program, comments, date_added, url,
                    (status_ex or status_raw), term, us_intl,
                    gpa, gre_q, gre_v, gre_aw,
                    degree_guess, None, None
                ))
                inserted += cur.rowcount

            # Update LLM fields
            for (url, program, comments), lr in llm_lookup.items():
                llm_prog = (lr.get("llm-generated-program") or "").strip()
                llm_uni = (lr.get("llm-generated-university") or "").strip()
                cur.execute(update_sql, (llm_prog, llm_uni, url, program, comments))
                updated += cur.rowcount

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

    print("=== load_data.py completed ===")
    print(f"Structural read: {len(structural)}")
    print(f"LLM JSONL read: {len(llm_rows)}")
    print(f"Inserted: {inserted}")
    print(f"Updated with LLM fields: {updated}")
    print(f"DB total: {total}")
    print(f"DB llm uni non-null: {llm_uni_nonnull}")
    print(f"DB llm prog non-null: {llm_prog_nonnull}")


if __name__ == "__main__":
    main()

