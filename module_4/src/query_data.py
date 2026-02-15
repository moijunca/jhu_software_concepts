"""
query_data.py – Run all Module 3/4 analytical queries against the
``applicants`` PostgreSQL table and return the results as a dict.

Changes from Module 3:
- Uses psycopg3 (``import psycopg``) instead of psycopg2.
- ``fetch_metrics()`` accepts an optional Flask ``app`` object so tests can
  inject a ``DATABASE_URL`` without modifying environment variables.
"""

import os
from typing import Optional

import psycopg  # psycopg3

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


def _one(cur, sql, params=None):
    """Execute *sql* and return the first column of the first row, or None."""
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Core query function
# ---------------------------------------------------------------------------

def fetch_metrics(app=None) -> dict:
    """
    Execute all Module 3/4 queries and return results as a dictionary.

    Parameters
    ----------
    app :
        Optional Flask app.  When provided, ``app.config["DATABASE_URL"]`` is
        used to connect to the test database instead of the default one.

    Returns
    -------
    dict
        Keys include ``total``, ``fall_2026``, ``pct_intl``, ``avg_gpa``,
        ``avg_gre``, ``avg_gre_v``, ``avg_gre_aw``, ``avg_gpa_american_fall``,
        ``acceptance_pct``, ``avg_gpa_accepted``, ``q7_jhu_ms_cs``,
        ``q8_raw``, ``q9_llm``, ``q10a_rows``, ``q10b_rows``,
        ``term_dist``, ``decision_dist``.
    """
    metrics: dict = {
        "total": None,
        "fall_2026": None,
        "pct_intl": None,
        "avg_gpa": None,
        "avg_gre": None,
        "avg_gre_v": None,
        "avg_gre_aw": None,
        "avg_gpa_american_fall": None,
        "acceptance_pct": None,
        "avg_gpa_accepted": None,
        "q7_jhu_ms_cs": None,
        "q8_raw": None,
        "q9_llm": None,
        "q10a_rows": [],
        "q10b_rows": [],
        "term_dist": [],
        "decision_dist": [],
    }

    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            # Total rows
            metrics["total"] = _one(cur, "SELECT COUNT(*) FROM applicants;")

            # Q1: Fall 2026 count
            metrics["fall_2026"] = _one(
                cur,
                "SELECT COUNT(*) FROM applicants WHERE term = %s;",
                (FALL_2026,),
            )

            # Q2: percent international (known nationality only)
            metrics["pct_intl"] = _one(cur, """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE 'International%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN us_or_international IS NOT NULL
                                          AND us_or_international <> '' THEN 1 ELSE 0 END), 0),
                    2
                )
                FROM applicants;
            """)

            # Q3: average scores
            for col, key in [
                ("gpa",    "avg_gpa"),
                ("gre",    "avg_gre"),
                ("gre_v",  "avg_gre_v"),
                ("gre_aw", "avg_gre_aw"),
            ]:
                metrics[key] = _one(
                    cur,
                    f"SELECT ROUND(AVG({col})::numeric, 3) "
                    f"FROM applicants WHERE {col} IS NOT NULL;",
                )

            # Q4: avg GPA of American students in Fall 2026
            metrics["avg_gpa_american_fall"] = _one(cur, """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE 'American%'
                  AND gpa IS NOT NULL;
            """, (FALL_2026,))

            # Q5: acceptance % among decisions in Fall 2026
            metrics["acceptance_pct"] = _one(cur, """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN status ILIKE 'Accepted%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE
                        WHEN status ILIKE 'Accepted%'
                          OR status ILIKE 'Rejected%'
                          OR status ILIKE 'Waitlisted%'
                          OR status ILIKE 'Interview%'
                        THEN 1 ELSE 0 END), 0),
                    2
                )
                FROM applicants
                WHERE term = %s;
            """, (FALL_2026,))

            # Q6: avg GPA of accepted applicants in Fall 2026
            metrics["avg_gpa_accepted"] = _one(cur, """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE 'Accepted%'
                  AND gpa IS NOT NULL;
            """, (FALL_2026,))

            # Q7: JHU Masters in Computer Science
            CS_REGEX = r'(^|[^a-z])cs([^a-z]|$)'
            metrics["q7_jhu_ms_cs"] = _one(cur, f"""
                SELECT COUNT(*)
                FROM applicants
                WHERE (
                    program ILIKE '%johns hopkins%' OR program ILIKE '%hopkins%'
                    OR program ILIKE '%jhu%'
                    OR llm_generated_university ILIKE '%johns hopkins%'
                    OR llm_generated_university ILIKE '%hopkins%'
                    OR llm_generated_university ILIKE '%jhu%'
                  )
                  AND (
                    (program ILIKE '%computer%' AND program ILIKE '%science%')
                    OR (comments ILIKE '%computer%' AND comments ILIKE '%science%')
                    OR (llm_generated_program ILIKE '%computer%'
                        AND llm_generated_program ILIKE '%science%')
                    OR program ILIKE '%comp sci%'
                    OR llm_generated_program ILIKE '%comp sci%'
                    OR program ~* %s
                    OR comments ~* %s
                    OR llm_generated_program ~* %s
                  )
                  AND (
                    degree ILIKE 'Master%'
                    OR program ILIKE '%master%' OR comments ILIKE '%master%'
                    OR program ILIKE '%m.s%'    OR comments ILIKE '%m.s%'
                    OR program ILIKE '%msc%'    OR comments ILIKE '%msc%'
                    OR program ILIKE '%mcs%'    OR comments ILIKE '%mcs%'
                    OR program ILIKE '%meng%'   OR comments ILIKE '%meng%'
                  );
            """, (CS_REGEX, CS_REGEX, CS_REGEX))

            # Q8: 2026 PhD CS acceptances at Georgetown/MIT/Stanford/CMU (raw)
            metrics["q8_raw"] = _one(cur, f"""
                SELECT COUNT(*)
                FROM applicants
                WHERE status ILIKE 'Accepted%'
                  AND (
                    (date_added >= DATE '2026-01-01' AND date_added < DATE '2027-01-01')
                    OR term ILIKE '%2026%'
                  )
                  AND (
                    (program ILIKE '%computer%' AND program ILIKE '%science%')
                    OR (comments ILIKE '%computer%' AND comments ILIKE '%science%')
                    OR program ILIKE '%comp sci%'
                    OR program ~* %s
                    OR comments ~* %s
                  )
                  AND (
                    degree = 'PhD'
                    OR program ILIKE '%phd%' OR comments ILIKE '%phd%'
                    OR program ILIKE '%ph.d%' OR comments ILIKE '%ph.d%'
                    OR program ILIKE '%doctorate%' OR comments ILIKE '%doctorate%'
                  )
                  AND (
                    program ILIKE '%georgetown%'
                    OR program ILIKE '%massachusetts institute of technology%'
                    OR program ILIKE '%mit%'
                    OR program ILIKE '%stanford%'
                    OR program ILIKE '%carnegie mellon%'
                    OR program ILIKE '%cmu%'
                  );
            """, (CS_REGEX, CS_REGEX))

            # Q9: same as Q8 using LLM-normalised fields
            metrics["q9_llm"] = _one(cur, f"""
                SELECT COUNT(*)
                FROM applicants
                WHERE status ILIKE 'Accepted%'
                  AND (
                    (date_added >= DATE '2026-01-01' AND date_added < DATE '2027-01-01')
                    OR term ILIKE '%2026%'
                  )
                  AND (
                    (llm_generated_program ILIKE '%computer%'
                     AND llm_generated_program ILIKE '%science%')
                    OR llm_generated_program ILIKE '%comp sci%'
                    OR llm_generated_program ~* %s
                    OR (program ILIKE '%computer%' AND program ILIKE '%science%')
                    OR program ~* %s
                  )
                  AND (
                    degree = 'PhD'
                    OR program ILIKE '%phd%' OR comments ILIKE '%phd%'
                    OR program ILIKE '%ph.d%' OR comments ILIKE '%ph.d%'
                    OR program ILIKE '%doctorate%' OR comments ILIKE '%doctorate%'
                  )
                  AND (
                    llm_generated_university ILIKE '%georgetown%'
                    OR llm_generated_university ILIKE '%mit%'
                    OR llm_generated_university ILIKE '%stanford%'
                    OR llm_generated_university ILIKE '%carnegie mellon%'
                    OR llm_generated_university ILIKE '%cmu%'
                    OR program ILIKE '%georgetown%'
                    OR program ILIKE '%mit%'
                    OR program ILIKE '%stanford%'
                    OR program ILIKE '%carnegie mellon%'
                    OR program ILIKE '%cmu%'
                  );
            """, (CS_REGEX, CS_REGEX))

            # Q10a: top 10 universities by Fall 2026 CS applicants
            cur.execute(f"""
                SELECT
                  COALESCE(NULLIF(llm_generated_university, ''), 'Unknown') AS university,
                  COUNT(*) AS cnt
                FROM applicants
                WHERE term = %s
                  AND (
                    (program ILIKE '%computer%' AND program ILIKE '%science%')
                    OR (llm_generated_program ILIKE '%computer%'
                        AND llm_generated_program ILIKE '%science%')
                    OR program ILIKE '%comp sci%'
                    OR llm_generated_program ILIKE '%comp sci%'
                    OR program ~* %s
                    OR llm_generated_program ~* %s
                  )
                  AND COALESCE(NULLIF(llm_generated_university, ''), '') <> ''
                  AND llm_generated_university !~* '(gpa|gre|score|american gpas)'
                  AND llm_generated_university !~* '^international\\b'
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 10;
            """, (FALL_2026, CS_REGEX, CS_REGEX))
            metrics["q10a_rows"] = cur.fetchall()

            # Q10b: acceptance rate by term (top 5 terms by volume)
            cur.execute("""
                WITH decisions AS (
                  SELECT
                    COALESCE(NULLIF(term, ''), 'No term detected') AS term_label,
                    CASE WHEN status ILIKE 'Accepted%' THEN 1
                         WHEN status ILIKE 'Rejected%' THEN 0
                         ELSE NULL END AS accepted_flag,
                    CASE WHEN status ILIKE 'Accepted%'
                           OR status ILIKE 'Rejected%'
                           OR status ILIKE 'Waitlisted%'
                           OR status ILIKE 'Interview%'
                         THEN 1 ELSE 0 END AS is_decision
                  FROM applicants
                ),
                top_terms AS (
                  SELECT term_label, COUNT(*) AS term_cnt
                  FROM decisions
                  GROUP BY 1
                  ORDER BY term_cnt DESC
                  LIMIT 5
                )
                SELECT
                  d.term_label,
                  tt.term_cnt,
                  ROUND(
                    100.0 * SUM(CASE WHEN d.accepted_flag = 1 THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN d.is_decision = 1 THEN 1 ELSE 0 END), 0),
                    2
                  ) AS acceptance_pct
                FROM decisions d
                JOIN top_terms tt ON tt.term_label = d.term_label
                GROUP BY d.term_label, tt.term_cnt
                ORDER BY tt.term_cnt DESC;
            """)
            metrics["q10b_rows"] = cur.fetchall()

            # Term distribution (top 10)
            cur.execute("""
                SELECT COALESCE(NULLIF(term,''), 'No term detected') AS term_label,
                       COUNT(*) AS cnt
                FROM applicants
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 10;
            """)
            metrics["term_dist"] = cur.fetchall()

            # Decision distribution (top 10)
            cur.execute("""
                SELECT COALESCE(NULLIF(status,''), 'No decision detected') AS status_label,
                       COUNT(*) AS cnt
                FROM applicants
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 10;
            """)
            metrics["decision_dist"] = cur.fetchall()

    finally:
        conn.close()

    return metrics


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main():
    m = fetch_metrics()

    print("=== Module 4 Required Questions ===")
    print(f"Q0  Total applicants           : {m['total']}")
    print(f"Q1  Fall 2026 applicants        : {m['fall_2026']}")
    print(f"Q2  Percent International       : {m['pct_intl']}")
    print(f"Q3  Avg GPA                     : {m['avg_gpa']}")
    print(f"Q3  Avg GRE Quant               : {m['avg_gre']}")
    print(f"Q3  Avg GRE Verbal              : {m['avg_gre_v']}")
    print(f"Q3  Avg GRE AW                  : {m['avg_gre_aw']}")
    print(f"Q4  Avg GPA American (Fall 2026): {m['avg_gpa_american_fall']}")
    print(f"Q5  Acceptance % (Fall 2026)    : {m['acceptance_pct']}")
    print(f"Q6  Avg GPA Accepted (Fall 2026): {m['avg_gpa_accepted']}")
    print(f"Q7  JHU Masters CS              : {m['q7_jhu_ms_cs']}")
    print(f"Q8  2026 PhD CS raw             : {m['q8_raw']}")
    print(f"Q9  2026 PhD CS LLM             : {m['q9_llm']}")

    print("\n=== Q10a Top universities (Fall 2026 CS) ===")
    for uni, cnt in m["q10a_rows"]:
        print(f"  {uni}: {cnt}")

    print("\n=== Q10b Acceptance rate by term ===")
    for term_label, term_cnt, acc_pct in m["q10b_rows"]:
        print(f"  {term_label}: {term_cnt} rows | acceptance % = {acc_pct}")


if __name__ == "__main__":
    main()
