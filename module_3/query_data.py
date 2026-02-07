import os
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

FALL_2026 = "Fall 2026"

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
            # Q1: How many entries applied for Fall 2026?
            one(cur, "Q1 Fall 2026 applicants:", "SELECT COUNT(*) FROM applicants WHERE term = %s;", (FALL_2026,))

            # Q2: Percent international (simple version)
            one(cur, "Q2 Percent International:", """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE %s THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*),0),
                    2
                )
                FROM applicants;
            """, ("%international%",))

            # Q3: Average GPA/GRE where present
            one(cur, "Q3 Avg GPA:", "SELECT ROUND(AVG(gpa)::numeric,3) FROM applicants WHERE gpa IS NOT NULL;")
            one(cur, "Q3 Avg GRE:", "SELECT ROUND(AVG(gre)::numeric,3) FROM applicants WHERE gre IS NOT NULL;")

            # Q4: Average GPA of American students in Fall 2026
            one(cur, "Q4 Avg GPA American (Fall 2026):", """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE %s
                  AND gpa IS NOT NULL;
            """, (FALL_2026, "%american%"))

            # Q5: Acceptance percent in Fall 2026 (based on extracted decision word)
            one(cur, "Q5 Acceptance % (Fall 2026):", """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN status ILIKE %s THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*),0),
                    2
                )
                FROM applicants
                WHERE term = %s;
            """, ("%accepted%", FALL_2026))

            # Q6: Avg GPA of accepted applicants in Fall 2026
            one(cur, "Q6 Avg GPA Accepted (Fall 2026):", """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE %s
                  AND gpa IS NOT NULL;
            """, (FALL_2026, "%accepted%"))

            # Two “curiosity” examples (you can change these later)
            one(cur, "Curiosity A: Top 10 terms:", """
                SELECT STRING_AGG(term || ':' || cnt, ', ')
                FROM (
                    SELECT term, COUNT(*) AS cnt
                    FROM applicants
                    WHERE term IS NOT NULL AND term <> ''
                    GROUP BY term
                    ORDER BY cnt DESC
                    LIMIT 10
                ) t;
            """)

            one(cur, "Curiosity B: Top 10 programs (raw):", """
                SELECT STRING_AGG(program || ':' || cnt, ' | ')
                FROM (
                    SELECT program, COUNT(*) AS cnt
                    FROM applicants
                    WHERE program IS NOT NULL AND program <> ''
                    GROUP BY program
                    ORDER BY cnt DESC
                    LIMIT 10
                ) t;
            """)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
