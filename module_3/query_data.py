import os
import psycopg2

# Database connection settings
DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

FALL_2026 = "Fall 2026"

# Decision word patterns
DECISION_ACCEPTED = "%accepted%"
DECISION_REJECTED = "%rejected%"
DECISION_WAITLISTED = "%waitlisted%"
DECISION_INTERVIEW = "%interview%"

# SQL snippet used to detect any decision-type status
DECISION_ANY_SQL = """
(status ILIKE %s OR status ILIKE %s OR status ILIKE %s OR status ILIKE %s)
"""


def one(cur, label, sql, params=None):
    """Execute a query expected to return a single value and print it."""
    cur.execute(sql, params or ())
    val = cur.fetchone()[0]
    print(f"{label} {val}")


def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )

    try:
        with conn.cursor() as cur:

            # -------- SANITY CHECKS --------
            print("=== Sanity ===")
            one(cur, "Total rows:", "SELECT COUNT(*) FROM applicants;")
            one(cur, "Rows with term:", "SELECT COUNT(*) FROM applicants WHERE term IS NOT NULL AND term<>'';")
            one(cur, "Rows with GPA:", "SELECT COUNT(*) FROM applicants WHERE gpa IS NOT NULL;")
            one(cur, "Rows with GRE:", "SELECT COUNT(*) FROM applicants WHERE gre IS NOT NULL;")
            one(
                cur,
                "Rows with any decision-word status:",
                f"SELECT COUNT(*) FROM applicants WHERE {DECISION_ANY_SQL};",
                (DECISION_ACCEPTED, DECISION_REJECTED, DECISION_WAITLISTED, DECISION_INTERVIEW),
            )
            print()

            print("=== Module 3 Queries ===")

            # Q1: How many entries applied for Fall 2026?
            one(
                cur,
                "Q1 Fall 2026 applicants:",
                "SELECT COUNT(*) FROM applicants WHERE term = %s;",
                (FALL_2026,),
            )

            # Q2: What percent of applicants are International?
            # (Only among rows where nationality is known)
            one(
                cur,
                "Q2 Percent International (known nationality only):",
                """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE %s THEN 1 ELSE 0 END)
                    / NULLIF(
                        SUM(
                            CASE WHEN us_or_international IS NOT NULL 
                                      AND us_or_international <> '' 
                                 THEN 1 ELSE 0 END
                        ),
                        0
                    ),
                    2
                )
                FROM applicants;
                """,
                ("%international%",),
            )

            # Q3: Average GPA and GRE for applicants who reported them
            one(
                cur,
                "Q3 Avg GPA (non-null):",
                "SELECT ROUND(AVG(gpa)::numeric,3) FROM applicants WHERE gpa IS NOT NULL;",
            )

            one(
                cur,
                "Q3 Avg GRE (non-null):",
                "SELECT ROUND(AVG(gre)::numeric,3) FROM applicants WHERE gre IS NOT NULL;",
            )

            # Q4: Average GPA of American students in Fall 2026
            one(
                cur,
                "Q4 Avg GPA American (Fall 2026):",
                """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE %s
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026, "%american%"),
            )

            # Q5: What percent of Fall 2026 applicants were accepted?
            # (Only considering rows with a decision word)
            one(
                cur,
                "Q5 Acceptance % (Fall 2026, decisions only):",
                f"""
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN status ILIKE %s THEN 1 ELSE 0 END)
                    / NULLIF(
                        SUM(
                            CASE WHEN {DECISION_ANY_SQL} THEN 1 ELSE 0 END
                        ),
                        0
                    ),
                    2
                )
                FROM applicants
                WHERE term = %s;
                """,
                (
                    DECISION_ACCEPTED,  # numerator
                    DECISION_ACCEPTED,  # part of decision-any
                    DECISION_REJECTED,
                    DECISION_WAITLISTED,
                    DECISION_INTERVIEW,
                    FALL_2026,
                ),
            )

            # Q6: Average GPA of accepted applicants in Fall 2026
            one(
                cur,
                "Q6 Avg GPA Accepted (Fall 2026):",
                """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE %s
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026, DECISION_ACCEPTED),
            )

            # -------- Additional Curiosity Queries --------
            # (These satisfy the requirement to create two extra interesting queries)

            # Curiosity 1: Top 10 most common application terms
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

            # Curiosity 2: Distribution of decision outcomes
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
