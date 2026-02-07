import json
import os
from datetime import datetime
import psycopg2

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_FILE = os.path.join(BASE_DIR, "applicant_data_structural_clean.json")

DB_NAME = "gradcafe"
DB_USER = os.getenv("USER")
DB_HOST = "localhost"
DB_PORT = 5432


def parse_date(value):
    if not value:
        return None
    value = value.strip()
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def load_json_records(path):
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if isinstance(obj, list):
        return obj

    if isinstance(obj, dict):
        for key in obj:
            if isinstance(obj[key], list):
                return obj[key]

    raise ValueError("Could not locate list of records in JSON file")


def main():
    data = load_json_records(DATA_FILE)

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )

    cur = conn.cursor()

    insert_sql = """
    INSERT INTO applicants (
        program, comments, date_added, url, status, term, us_or_international,
        gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    inserted = 0

    for row in data:
        vals = (
            row.get("program"),
            row.get("comments"),
            parse_date(row.get("date_added")),
            row.get("url"),
            row.get("status"),
            row.get("term"),
            row.get("US/International") or row.get("us_or_international"),
            to_float(row.get("gpa")),
            to_float(row.get("gre")),
            to_float(row.get("gre_v")),
            to_float(row.get("gre_aw")),
            row.get("degree"),
            row.get("llm-generated-program") or row.get("llm_generated_program"),
            row.get("llm-generated-university") or row.get("llm_generated_university"),
        )

        cur.execute(insert_sql, vals)
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {inserted} rows into applicants.")


if __name__ == "__main__":
    main()

