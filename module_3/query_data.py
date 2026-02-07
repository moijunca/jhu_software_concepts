import os
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

FALL_2026 = "Fall 2026"

DECISION_ACCEPTED = "%accepted%"
DECISION_REJECTED = "%rejected%"
DECISION_WAITLISTED = "%waitlisted%"
DECISION_INTERVIEW = "%interview%"

DECISION_ANY_SQL = """
(status ILIKE %s OR status ILIKE %s OR status ILIKE %s OR status ILIKE %s)
"""

def one(cur, label, sql, params=None):
    cur.execute(sql, params or ())
    val = cur.fetchone()[0]
    print(f"{label} {val}")

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT
    )

    try:
        with conn.cursor() as cur:
            print("=== Sanity ===")
            one(cur, "Total rows:", "SELECT COUNT(*) FROM applicants;")
            one(cur, "Rows with term:", "SELECT COUNT(*) FROM applicants WHERE term IS NOT NULL AND term<>'';")
            one(cur, "Rows with GPA:", "SELECT COUNT(*) FROM applicants WHERE gpa IS NOT NULL;")
            one(cur, "Rows with GRE:", "SELECT COUNT(*) FROM applicants WHERE gre IS NOT NULL;")
            one(cur, "Rows with any decision-word status:", f"SELECT COUNT(*) FROM applicants WHERE {DECISION_ANY_SQL};",
                (DECISION_ACCEPTED, DECISION_REJECTED, DECISION_WAITLISTED, DECISION_INTERVIEW))
            print()

            print("=== Module 3 Queries ===")

            # Q1: How many entries applied for Fall 2026?
            one(cur, "Q1 Fall 2026 applicants:",
                "SELECT COUNT(*) FROM applicants WHERE term = %s;",
                (FALL_2026,))

            # Q2: Percent international (based on extracted us_or_international)
            one(cur, "Q2 Percent International:",
                """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE %s THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*),0),
                    2
                )
                FROM applicants;
                """,
                ("%international%",))

            # Q3: Average GPA and GRE among those who reported them
            one(cur, "Q3 Avg GPA (non-null):",
                "SELECT ROUND(AVG(gpa)::numeric,3) FROM applicants WHERE gpa IS NOT NULL;")
            one(cur, "Q3 Avg GRE (non-null):",
                "SELECT ROUND(AVG(gre)::numeric,3) FROM applicants WHERE gre IS NOT NULL;")

            # Q4: Avg GPA for American applicants in Fall 2026
            one(cur, "Q4 Avg GPA American (Fall 2026):",
                """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE %s
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026, "%american%"))

            # Q5: Acceptance % in Fall 2026 (decision word Accepted)
            one(cur, "Q5 Acceptance % (Fall 2026):",
                """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN status ILIKE %s THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*),0),
                    2
                )
                FROM applicants
                WHERE term = %s;
                """,
                (DECISION_ACCEPTED, FALL_2026))

            # Q6: Avg GPA for Accepted in Fall 2026
            one(cur, "Q6 Avg GPA Accepted (Fall 2026):",
                """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE %s
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026, DECISION_ACCEPTED))

            # Curiosity 1: top 10 terms
            print("\nCuriosity 1: Top 10 terms")
            cur.execute("""
                SELECT term, COUNT(*) AS cnt
                FROM applicants
                WHERE term IS NOT NULL AND term <> ''
                GROUP BY term
                ORDER BY cnt DESC
                LIMIT 10;
            """)
            for term, cnt in cur.fetchall():
                print(f"  {term}: {cnt}")

            # Curiosity 2: decision distribution (only decision-like statuses)
            print("\nCuriosity 2: Decision distribution (Accepted/Rejected/Waitlisted/Interview)")
            cur.execute(f"""
                SELECT
                    CASE
                        WHEN status ILIKE %s THEN 'Accepted'
                        WHEN status ILIKE %s THEN 'Rejected'
                        WHEN status ILIKE %s THEN 'Waitlisted'
                        WHEN status ILIKE %s THEN 'Interview'
                        ELSE 'Other'
                    END AS decision,
                    COUNT(*) AS cnt
                FROM applicants
                GROUP BY decision
                ORDER BY cnt DESC;
            """, (DECISION_ACCEPTED, DECISION_REJECTED, DECISION_WAITLISTED, DECISION_INTERVIEW))
            for decision, cnt in cur.fetchall():
                print(f"  {decision}: {cnt}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
