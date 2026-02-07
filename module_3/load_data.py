import json
import os
import re
from datetime import datetime
import psycopg2

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "part1.json.jsonl")

DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

# -------------------------
# Extractors (text often embeds these)
# -------------------------

# Term patterns:
# 1) Full: "Fall 2026", "Spring 2026", "Fall '26", "Autumn 2026"
TERM_FULL_RE = re.compile(r"\b(Fall|Spring|Summer|Winter|Autumn)\s*['’]?\s*(20\d{2}|\d{2})\b", re.IGNORECASE)

# 2) Shorthand: "F26", "S26", "SU26", "W26", "F'26"
TERM_SHORT_RE = re.compile(r"\b(F|S|SU|W)\s*['’]?\s*(\d{2})\b", re.IGNORECASE)

TERM_MAP = {
    "F": "Fall",
    "S": "Spring",
    "SU": "Summer",
    "W": "Winter",
}

GPA_RE = re.compile(r"\bGPA\s*([0-4]\.\d{1,2})\b", re.IGNORECASE)
GRE_RE = re.compile(r"\bGRE\s*(\d{3})\b", re.IGNORECASE)

# Decisions: your data often says "Accepted on..." etc
DECISION_RE = re.compile(r"\b(Accepted|Rejected|Waitlisted|Interview)\b", re.IGNORECASE)

# Nationality markers
AMERICAN_RE = re.compile(r"\bAmerican\b", re.IGNORECASE)
INTL_RE = re.compile(r"\bInternational\b", re.IGNORECASE)


def parse_date(value):
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


def to_float(x):
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


def extract_term(text: str):
    text = text or ""

    # Full form first: "Fall 2026", "Fall '26", "Autumn 2026"
    m = TERM_FULL_RE.search(text)
    if m:
        season = m.group(1).title()
        year = m.group(2)

        # Normalize 2-digit year -> 20xx
        if len(year) == 2:
            year = f"20{year}"

        # Normalize Autumn -> Fall
        if season == "Autumn":
            season = "Fall"

        return f"{season} {year}"

    # Shorthand: "F26", "S26", "SU26", "W26"
    m = TERM_SHORT_RE.search(text)
    if m:
        code = m.group(1).upper()
        year2 = m.group(2)
        season = TERM_MAP.get(code)
        if season:
            return f"{season} 20{year2}"

    return None


def extract_fields(text: str):
    text = text or ""

    # term
    term = extract_term(text)

    # gpa
    gpa = None
    m = GPA_RE.search(text)
    if m:
        gpa = to_float(m.group(1))

    # gre
    gre = None
    m = GRE_RE.search(text)
    if m:
        gre = to_float(m.group(1))

    # status/decision
    status = None
    m = DECISION_RE.search(text)
    if m:
        status = m.group(1).title()

    # us/international
    us_intl = None
    if INTL_RE.search(text):
        us_intl = "International"
    elif AMERICAN_RE.search(text):
        us_intl = "American"

    return term, status, us_intl, gpa, gre


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
        raise FileNotFoundError(f"Missing {DATA_FILE}")

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
            # wipe old rows to ensure clean reload
            cur.execute("TRUNCATE applicants;")

            inserted = 0
            for r in records:
                program = (r.get("program") or "").strip() or None
                comments = (r.get("comments") or "").strip() or None
                date_added = parse_date(r.get("date_added"))
                url = (r.get("url") or "").strip() or None

                # raw fields (often empty or messy)
                status_raw = (r.get("status") or "").strip() or None
                term_raw = (r.get("term") or "").strip() or None
                us_intl_raw = (r.get("US/International") or "").strip() or None
                degree = (r.get("Degree") or "").strip() or None

                llm_prog = (r.get("llm-generated-program") or "").strip() or None
                llm_uni = (r.get("llm-generated-university") or "").strip() or None

                combined = " ".join([
                    program or "",
                    comments or "",
                    status_raw or "",
                    term_raw or "",
                    us_intl_raw or "",
                    degree or "",
                ])

                term_ex, status_ex, us_intl_ex, gpa_ex, gre_ex = extract_fields(combined)

                term = term_ex or term_raw
                status = status_ex or status_raw
                us_intl = us_intl_ex or us_intl_raw

                cur.execute(insert_sql, (
                    program,
                    comments,
                    date_added,
                    url,
                    status,
                    term,
                    us_intl,
                    gpa_ex,
                    gre_ex,
                    None,   # gre_v not reliably present
                    None,   # gre_aw not reliably present
                    degree,
                    llm_prog,
                    llm_uni,
                ))
                inserted += 1

            conn.commit()
            print(f"Inserted {inserted} rows into applicants (reloaded).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
