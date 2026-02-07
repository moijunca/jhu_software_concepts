import os
import psycopg2
from flask import Flask, render_template

# -------------------------
# Flask setup (template folder is inside module_3/templates)
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATES_DIR)


# -------------------------
# Flask setup (template folder is inside module_3/templates)
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATES_DIR)

# -------------------------
# Database config
# -------------------------
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

# SQL snippet for "any decision-like status"
DECISION_ANY_SQL = "(status ILIKE %s OR status ILIKE %s OR status ILIKE %s OR status ILIKE %s)"


def get_db():
    """Create a DB connection."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )


def one(cur, sql, params=None):
    """Run a SQL query that returns exactly one value."""
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


@app.route("/")
def index():
    """
    Main dashboard page. Runs the same queries as query_data.py and renders index.html.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Basic counts
            total = one(cur, "SELECT COUNT(*) FROM applicants;")

            fall2026 = one(
                cur,
                "SELECT COUNT(*) FROM applicants WHERE term = %s;",
                (FALL_2026,),
            )

            # Q2: Percent International (known nationality only)
            international = one(
                cur,
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

            # Q3: Average GPA and GRE (only among rows that reported them)
            avg_gpa = one(
                cur,
                "SELECT ROUND(AVG(gpa)::numeric,3) FROM applicants WHERE gpa IS NOT NULL;",
            )
            avg_gre = one(
                cur,
                "SELECT ROUND(AVG(gre)::numeric,3) FROM applicants WHERE gre IS NOT NULL;",
            )

            # Q4: Avg GPA American (Fall 2026)
            avg_gpa_american = one(
                cur,
                """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE %s
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026, "%american%"),
            )

            # Q5: Acceptance % (Fall 2026, decisions only)
            acceptance = one(
                cur,
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
                    DECISION_ACCEPTED,  # decision-any
                    DECISION_REJECTED,
                    DECISION_WAITLISTED,
                    DECISION_INTERVIEW,
                    FALL_2026,
                ),
            )

            # Q6: Avg GPA Accepted (Fall 2026)
            avg_gpa_accepted = one(
                cur,
                """
                SELECT ROUND(AVG(gpa)::numeric,3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE %s
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026, DECISION_ACCEPTED),
            )

            # Curiosity 1: Term distribution (include missing)
            cur.execute(
                """
                SELECT COALESCE(NULLIF(term,''),'No term detected') AS term_bucket, COUNT(*) AS cnt
                FROM applicants
                GROUP BY term_bucket
                ORDER BY cnt DESC
                LIMIT 10;
                """
            )
            terms = cur.fetchall()

            # Curiosity 2: Decision distribution
            cur.execute(
                """
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
                """,
                (DECISION_ACCEPTED, DECISION_REJECTED, DECISION_WAITLISTED, DECISION_INTERVIEW),
            )
            decisions = cur.fetchall()

        return render_template(
            "index.html",
            total=total,
            fall2026=fall2026,
            international=international,
            avg_gpa=avg_gpa,
            avg_gre=avg_gre,
            avg_gpa_american=avg_gpa_american,
            acceptance=acceptance,
            avg_gpa_accepted=avg_gpa_accepted,
            terms=terms,
            decisions=decisions,
        )

    except Exception as e:
        # This makes debugging much faster than a silent failure
        return f"Server error: {type(e).__name__}: {e}", 500

    finally:
        conn.close()


if __name__ == "__main__":
    # Deterministic local run (avoids the "starts then disappears" loop)
    app.run(host="127.0.0.1", port=8000, debug=False, use_reloader=False)
