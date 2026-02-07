import json
import os
import re
from datetime import datetime
import psycopg2

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Use the JSONL file that actually has records
DATA_FILE = os.path.join(BASE_DIR, "part1.json.jsonl")

DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

# --- regex extractors (since term/GPA/GRE often appear inside free text) ---
TERM_RE = re.compile(r"\b(Fall|Spring|Summer|Winter)\s*(20\d{2})\b", re.IGNORECASE)
GPA_RE = re.compile(r"\bGPA\s*([0-4]\.\d{1,2})\b", re.IGNORECASE)
GRE_RE = re.compile(r"\bGRE\s*(\d{3})\b", re.IGNORECASE)
DECISION_RE = re.compile(r"\b(Accepted|Rejected|Waitlisted|Interview)\b", re.IGNORECASE)
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE = re.compile(r"\bInternational\b", re.IGNORECASE)


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS applicants (
  p_id SERIAL PRIMARY KEY,
  program TEXT,
  comments TEXT,
  date_added DATE,
  url TEXT,
  status TEXT,
  term TEXT,
  us_or_international TEXT,
  gpa DOUBLE PRECISION,
  gre DOUBLE PRECISION,
  gre_v DOUBLE PRECISION,
  gre_aw DOUBLE PRECISION,
  degree TEXT,
  llm_generated_program TEXT,
  llm_generated_university TEXT
);
"""


def parse_date(value):
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def extract_fields(text: str):
    text = text or ""

    term = None
    m = TERM_RE.search(text)
    if m:
        term = f"{m.group(1).title()} {m.group(2)}"

    gpa = None
    m = GPA_RE.search(text)
    if m:
        gpa = to_float(m.group(1))

    gre = None
    m = GRE_RE.search(text)
    if m:
        gre = to_float(m.group(1))

    status = None
    m = DECISION_RE.search(text)
    if m:
        status = m.group(1).title()

    us_intl = None
    if INTL_RE.search(text):
        us_intl = "International"
    elif AMERICAN_RE.search(text):
        us_intl = "American"

    return {"term": term, "gpa": gpa, "gre": gre, "status": status, "us_intl": us_intl}


def load_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Missing data file: {DATA_FILE}")

    records = load_jsonl(DATA_FILE)

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )

    insert_sql = """
    INSERT INTO applicants (
        program, comments, date_added, url, status, term, us_or_international,
        gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    try:
        with conn.cursor() as cur:
            cur.execute(TABLE_SQL)
            cur.execute("TRUNCATE applicants;")

            inserted = 0
            for r in records:
                # Original fields from JSONL
                program = (r.get("program") or "").strip() or None
                comments = (r.get("comments") or "").strip() or None
                date_added = parse_date(r.get("date_added"))
                url = (r.get("url") or "").strip() or None

                status_raw = (r.get("status") or "").strip() or None
                term_raw = (r.get("term") or "").strip() or None
                us_intl_raw = (r.get("US/International") or "").strip() or None
                degree = (r.get("Degree") or "").strip() or None

                llm_prog = (r.get("llm-generated-program") or "").strip() or None
                llm_uni = (r.get("llm-generated-university") or "").strip() or None

                # Extract term/status/GPA/GRE/nationality from text (because many rows embed it)
                combined = " ".join([
                    program or "",
                    comments or "",
                    status_raw or "",
                    term_raw or "",
                ])
                ex = extract_fields(combined)

                status = ex["status"] or status_raw
                term = ex["term"] or term_raw
                us_intl = ex["us_intl"] or us_intl_raw
                gpa = ex["gpa"]
                gre = ex["gre"]

                # gre_v / gre_aw not reliably available in your JSONL sample
                gre_v = None
                gre_aw = None

                cur.execute(insert_sql, (
                    program, comments, date_added, url, status, term, us_intl,
                    gpa, gre, gre_v, gre_aw, degree, llm_prog, llm_uni
                ))
                inserted += 1

            conn.commit()
            print(f"Inserted {inserted} rows into applicants (reloaded).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
