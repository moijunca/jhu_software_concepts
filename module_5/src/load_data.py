"""
load_data.py – ETL: parse JSONL from the LLM-extended dataset and insert rows
into the PostgreSQL ``applicants`` table.

Changes from Module 3:
- Uses psycopg3 (``import psycopg``) instead of psycopg2.
- ``get_conn()`` / ``ensure_index()`` / ``main()`` accept an optional Flask
  ``app`` object so that tests can inject a ``DATABASE_URL`` via
  ``app.config`` without touching environment variables.
- ``_build_conninfo()`` mirrors the helper in ``app.py``.
"""

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import psycopg  # psycopg3

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")  # module_4/data/

LIV_LLM_JSONL = os.path.join(DATA_DIR, "llm_extend_applicant_data.json")

FALL_2026 = "Fall 2026"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _build_conninfo(app=None) -> str:
    """Return a psycopg3-compatible connection string."""
    url = (app.config.get("DATABASE_URL") if app else None) or os.getenv("DATABASE_URL")
    if url:
        return url
    db   = os.getenv("PGDATABASE", "gradcafe")
    user = os.getenv("PGUSER", os.getenv("USER", "postgres"))
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    return f"dbname={db} user={user} host={host} port={port}"


def get_conn(app=None):
    """Open and return a psycopg3 connection."""
    return psycopg.connect(_build_conninfo(app))


# ---------------------------------------------------------------------------
# Cleaning / parsing helpers
# ---------------------------------------------------------------------------

def clean_text(x: Any) -> Optional[str]:
    """Convert to str, strip, and remove NUL bytes that crash psycopg."""
    if x is None:
        return None
    s = str(x).replace("\x00", "").strip()
    return s if s else None


def to_float(x: Any) -> Optional[float]:
    """Coerce a value to float, returning None on failure."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = clean_text(x)
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[datetime]:
    """
    Parse various date strings.

    Supported formats: ``"January 31, 2026"``, ``"Feb 1, 2026"``,
    ``"2026-02-01"``, ``"02/01/2026"``.
    Returns a :class:`datetime.date` or ``None``.
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


# ---------------------------------------------------------------------------
# Term extraction
# ---------------------------------------------------------------------------
TERM_FULL_RE = re.compile(
    r"\b(Fall|Spring|Summer|Winter|Autumn)\s*['']?\s*(20\d{2}|\d{2})\b",
    re.IGNORECASE,
)
TERM_SHORT_RE = re.compile(r"\b(F|S|SU|W)\s*['']?\s*(\d{2})\b", re.IGNORECASE)
TERM_MAP = {"F": "Fall", "S": "Spring", "SU": "Summer", "W": "Winter"}


def extract_term(text: str) -> Optional[str]:
    """Extract a normalised semester/year term string from free text."""
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


# ---------------------------------------------------------------------------
# Status / nationality
# ---------------------------------------------------------------------------
DECISION_RE = re.compile(
    r"\b(Accepted|Rejected|Waitlisted|Wait listed|Interview)\b", re.IGNORECASE
)
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE     = re.compile(r"\bInternational\b", re.IGNORECASE)


def extract_status(text: str) -> Optional[str]:
    """Extract an admission decision string from free text."""
    t = text or ""
    m = DECISION_RE.search(t)
    if not m:
        return None
    raw = m.group(1).lower().replace(" ", "")
    if raw == "waitlisted":
        return "Waitlisted"
    return m.group(1).title()


def extract_us_intl(text: str) -> Optional[str]:
    """Return ``'International'``, ``'American'``, or ``None``."""
    t = text or ""
    if INTL_RE.search(t):
        return "International"
    if AMERICAN_RE.search(t):
        return "American"
    return None


# ---------------------------------------------------------------------------
# Degree normalisation
# ---------------------------------------------------------------------------

def normalize_degree(x: Any) -> Optional[str]:
    """Normalise a degree string to ``'PhD'``, ``'Masters'``, ``'Bachelors'``, or raw."""
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


# ---------------------------------------------------------------------------
# GPA / GRE extraction
# ---------------------------------------------------------------------------
GPA_RE = re.compile(
    r"\b(?:GPA\s*[:=]?\s*)?([0-4]\.\d{1,2})\s*(?:master'?s\s*)?(?:GPA)?\b",
    re.IGNORECASE,
)
GRE_Q_RE = re.compile(
    r"\b(?:GRE\s*)?(?:Q(?:uant)?|Quant|Quantitative)\s*[:=]?\s*(\d{3})\b"
    r"|\b(\d{3})\s*Q\b",
    re.IGNORECASE,
)
GRE_V_RE = re.compile(
    r"\b(?:GRE\s*)?(?:V(?:erb)?|Verbal)\s*[:=]?\s*(\d{3})\b"
    r"|\b(\d{3})\s*V\b",
    re.IGNORECASE,
)
GRE_AW_RE = re.compile(
    r"\b(?:AWA|AW|Analytical Writing)\s*[:=]?\s*([0-6]\.\d)\b",
    re.IGNORECASE,
)


def extract_gpa_gre(text: str):
    """
    Extract GPA, GRE Quant, GRE Verbal, and GRE AW from free text.

    Returns a 4-tuple ``(gpa, gre_q, gre_v, gre_aw)`` where each element is
    a ``float`` or ``None``.
    """
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


# ---------------------------------------------------------------------------
# JSONL loader
# ---------------------------------------------------------------------------

def load_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    """Stream JSONL (one JSON object per line), skipping malformed lines."""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


# ---------------------------------------------------------------------------
# Schema / index helpers
# ---------------------------------------------------------------------------

def ensure_table(app=None):
    """
    Create the ``applicants`` table and its unique index if they do not exist.

    Safe to call repeatedly (idempotent).
    """
    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS applicants (
                    p_id                    SERIAL PRIMARY KEY,
                    program                 TEXT,
                    comments                TEXT,
                    date_added              DATE,
                    url                     TEXT,
                    status                  TEXT,
                    term                    TEXT,
                    us_or_international     TEXT,
                    gpa                     NUMERIC,
                    gre                     NUMERIC,
                    gre_v                   NUMERIC,
                    gre_aw                  NUMERIC,
                    degree                  TEXT,
                    llm_generated_program   TEXT,
                    llm_generated_university TEXT
                );
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS applicants_sig_unique
                ON applicants (
                    COALESCE(url, ''),
                    COALESCE(program, ''),
                    COALESCE(comments, '')
                );
            """)
        conn.commit()
    finally:
        conn.close()


def ensure_index(app=None):
    """
    Deduplicate existing rows and ensure the unique index exists.

    Used by ``main()`` before bulk-inserting to guarantee idempotency on
    tables that may have been populated without the index.
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
        WHERE schemaname = 'public' AND indexname = 'applicants_sig_unique'
      ) THEN
        CREATE UNIQUE INDEX applicants_sig_unique
        ON applicants (
          COALESCE(url, ''),
          COALESCE(program, ''),
          COALESCE(comments, '')
        );
      END IF;
    END $$;
    """
    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main ETL entry-point
# ---------------------------------------------------------------------------

def main(app=None, jsonl_path: Optional[str] = None):
    """
    Load records from the LLM-extended JSONL file into PostgreSQL.

    Parameters
    ----------
    app :
        Optional Flask app whose ``config["DATABASE_URL"]`` overrides the
        default connection string.
    jsonl_path :
        Path to the JSONL file.  Defaults to ``LIV_LLM_JSONL``.
    """
    path = jsonl_path or LIV_LLM_JSONL
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing JSONL at {path}\n"
            "Fix: copy module_2/llm_extend_applicant_data.json → module_4/data/"
        )

    ensure_index(app)

    insert_sql = """
    INSERT INTO applicants (
        program, comments, date_added, url,
        status, term, us_or_international,
        gpa, gre, gre_v, gre_aw,
        degree, llm_generated_program, llm_generated_university
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    inserted = 0
    read_rows = 0

    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            for r in load_jsonl(path):
                read_rows += 1

                program  = clean_text(r.get("program"))
                comments = clean_text(r.get("comments"))
                url      = clean_text(r.get("url"))
                date_added = parse_date(r.get("date_added") or r.get("date_added_raw"))

                deg      = normalize_degree(r.get("masters_or_phd") or r.get("degree"))
                llm_prog = clean_text(r.get("llm-generated-program") or r.get("llm_generated_program"))
                llm_uni  = clean_text(r.get("llm-generated-university") or r.get("llm_generated_university"))

                status   = clean_text(r.get("status"))
                combined = " ".join(filter(None, [program, comments, status, llm_prog, llm_uni]))

                term = extract_term(combined)
                if term is None and date_added and date_added.year == 2026:
                    term = FALL_2026

                if not status:
                    status = extract_status(combined)
                us_intl = extract_us_intl(combined)
                gpa, gre_q, gre_v, gre_aw = extract_gpa_gre(combined)

                cur.execute(insert_sql, (
                    program, comments, date_added, url,
                    status, term, us_intl,
                    gpa, gre_q, gre_v, gre_aw,
                    deg, llm_prog, llm_uni,
                ))
                inserted += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    print("=== load_data.py completed ===")
    print(f"  Read rows  : {read_rows}")
    print(f"  Inserted   : {inserted}")


if __name__ == "__main__":
    main()
