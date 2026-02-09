# module_3/query_data.py
import os
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

FALL_2026 = "Fall 2026"


def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )


def one(cur, sql, params=None):
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def fetch_metrics():
    """
    Returns a dict used by Flask for rendering the dashboard.
    Also usable for tests.
    """
    metrics = {
        "total": None,                 # Q0
        "fall_2026": None,             # Q1
        "pct_intl": None,              # Q2
        "avg_gpa": None,               # Q3
        "avg_gre": None,               # Q3
        "avg_gre_v": None,             # Q3
        "avg_gre_aw": None,            # Q3
        "avg_gpa_american_fall": None, # Q4
        "acceptance_pct": None,        # Q5
        "avg_gpa_accepted": None,      # Q6
        "q7_jhu_ms_cs": None,          # Q7
        "q8_raw": None,                # Q8
        "q9_llm": None,                # Q9
        "q10a_rows": [],               # Q10a list of (label,count)
        "q10b_rows": [],               # Q10b list of (term,term_cnt,acc_pct)
        "term_dist": [],               # list of (term,count)
        "decision_dist": [],           # list of (decision,count)
    }

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Q0
            metrics["total"] = one(cur, "SELECT COUNT(*) FROM applicants;")

            # Q1
            metrics["fall_2026"] = one(
                cur,
                "SELECT COUNT(*) FROM applicants WHERE term = %s;",
                (FALL_2026,),
            )

            # Q2
            metrics["pct_intl"] = one(cur, """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE 'International%%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN us_or_international IS NOT NULL AND us_or_international <> '' THEN 1 ELSE 0 END), 0),
                    2
                )
                FROM applicants;
            """)

            # Q3
            metrics["avg_gpa"] = one(cur, "SELECT ROUND(AVG(gpa)::numeric, 3) FROM applicants WHERE gpa IS NOT NULL;")
            metrics["avg_gre"] = one(cur, "SELECT ROUND(AVG(gre)::numeric, 3) FROM applicants WHERE gre IS NOT NULL;")
            metrics["avg_gre_v"] = one(cur, "SELECT ROUND(AVG(gre_v)::numeric, 3) FROM applicants WHERE gre_v IS NOT NULL;")
            metrics["avg_gre_aw"] = one(cur, "SELECT ROUND(AVG(gre_aw)::numeric, 3) FROM applicants WHERE gre_aw IS NOT NULL;")

            # Q4
            metrics["avg_gpa_american_fall"] = one(cur, """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE 'American%%'
                  AND gpa IS NOT NULL;
            """, (FALL_2026,))

            # Q5
            metrics["acceptance_pct"] = one(cur, """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN status ILIKE 'Accepted%%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE
                        WHEN status ILIKE 'Accepted%%'
                          OR status ILIKE 'Rejected%%'
                          OR status ILIKE 'Waitlisted%%'
                          OR status ILIKE 'Interview%%'
                        THEN 1 ELSE 0 END), 0),
                    2
                )
                FROM applicants
                WHERE term = %s;
            """, (FALL_2026,))

            # Q6
            metrics["avg_gpa_accepted"] = one(cur, """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE 'Accepted%%'
                  AND gpa IS NOT NULL;
            """, (FALL_2026,))

            # Q7
            metrics["q7_jhu_ms_cs"] = one(cur, r"""
                SELECT COUNT(*)
                FROM applicants
                WHERE
                (
                    program ILIKE '%%johns hopkins%%' OR program ILIKE '%%hopkins%%' OR program ILIKE '%%jhu%%'
                    OR llm_generated_university ILIKE '%%johns hopkins%%' OR llm_generated_university ILIKE '%%hopkins%%' OR llm_generated_university ILIKE '%%jhu%%'
                )
                AND (
                    program ILIKE '%%computer science%%' OR program ILIKE '%%comp sci%%'
                    OR comments ILIKE '%%computer science%%' OR comments ILIKE '%%comp sci%%'
                    OR llm_generated_program ILIKE '%%computer science%%' OR llm_generated_program ILIKE '%%comp sci%%'
                    OR program ~* '\mCS\M' OR comments ~* '\mCS\M' OR llm_generated_program ~* '\mCS\M'
                )
                AND (
                    program ILIKE '%%master%%' OR comments ILIKE '%%master%%' OR status ILIKE '%%master%%'
                    OR program ILIKE '%%msc%%' OR comments ILIKE '%%msc%%' OR status ILIKE '%%msc%%'
                    OR program ILIKE '%%m.s%%' OR comments ILIKE '%%m.s%%' OR status ILIKE '%%m.s%%'
                    OR program ILIKE '%%mscs%%' OR comments ILIKE '%%mscs%%' OR status ILIKE '%%mscs%%'
                    OR program ILIKE '%%meng%%' OR comments ILIKE '%%meng%%' OR status ILIKE '%%meng%%'
                    OR program ILIKE '%%mcs%%' OR comments ILIKE '%%mcs%%' OR status ILIKE '%%mcs%%'
                );
            """)

            # Q8
            metrics["q8_raw"] = one(cur, r"""
                SELECT COUNT(*)
                FROM applicants
                WHERE date_added >= DATE '2026-01-01'
                AND date_added <  DATE '2027-01-01'
                AND status ILIKE 'Accepted%%'
                AND (
                    program ILIKE '%%computer science%%' OR program ILIKE '%%comp sci%%'
                    OR comments ILIKE '%%computer science%%' OR comments ILIKE '%%comp sci%%'
                    OR program ~* '\mCS\M' OR comments ~* '\mCS\M'
                )
                AND (
                    program ILIKE '%%phd%%' OR comments ILIKE '%%phd%%'
                    OR program ILIKE '%%ph.d%%' OR comments ILIKE '%%ph.d%%'
                    OR program ILIKE '%%doctorate%%' OR comments ILIKE '%%doctorate%%'
                )
                AND (
                    program ILIKE '%%georgetown%%'
                    OR program ILIKE '%%massachusetts institute of technology%%' OR program ILIKE '%%mit%%'
                    OR program ILIKE '%%stanford%%'
                    OR program ILIKE '%%carnegie mellon%%' OR program ILIKE '%%cmu%%'
                );
            """)

            # Q9
            metrics["q9_llm"] = one(cur, r"""
                SELECT COUNT(*)
                FROM applicants
                WHERE date_added >= DATE '2026-01-01'
                AND date_added <  DATE '2027-01-01'
                AND status ILIKE 'Accepted%%'
                AND (
                    llm_generated_program ILIKE '%%computer science%%' OR llm_generated_program ILIKE '%%comp sci%%'
                    OR llm_generated_program ~* '\mCS\M'
                    OR program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%'
                )
                AND (
                    program ILIKE '%%phd%%' OR comments ILIKE '%%phd%%'
                    OR program ILIKE '%%ph.d%%' OR comments ILIKE '%%ph.d%%'
                    OR program ILIKE '%%doctorate%%' OR comments ILIKE '%%doctorate%%'
                )
                AND (
                    llm_generated_university ILIKE '%%georgetown%%'
                    OR llm_generated_university ILIKE '%%massachusetts institute of technology%%' OR llm_generated_university ILIKE '%%mit%%'
                    OR llm_generated_university ILIKE '%%stanford%%'
                    OR llm_generated_university ILIKE '%%carnegie mellon%%' OR llm_generated_university ILIKE '%%cmu%%'
                );
            """)

            # Q10a
            cur.execute("""
                SELECT
                  COALESCE(NULLIF(llm_generated_university, ''), 'Unknown') AS university,
                  COUNT(*) AS cnt
                FROM applicants
                WHERE term = %s
                  AND (program ILIKE '%%computer science%%' OR llm_generated_program ILIKE '%%computer science%%')
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 10;
            """, (FALL_2026,))
            metrics["q10a_rows"] = cur.fetchall()

            # Q10b
            cur.execute("""
                WITH decisions AS (
                  SELECT
                    COALESCE(NULLIF(term, ''), 'No term detected') AS term_label,
                    CASE
                      WHEN status ILIKE 'Accepted%%' THEN 1
                      WHEN status ILIKE 'Rejected%%' THEN 0
                      WHEN status ILIKE 'Waitlisted%%' THEN NULL
                      WHEN status ILIKE 'Interview%%' THEN NULL
                      ELSE NULL
                    END AS accepted_flag,
                    CASE
                      WHEN status ILIKE 'Accepted%%'
                        OR status ILIKE 'Rejected%%'
                        OR status ILIKE 'Waitlisted%%'
                        OR status ILIKE 'Interview%%'
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

            # Term Distribution (Top 10)
            cur.execute("""
                SELECT COALESCE(NULLIF(term,''), 'No term detected') AS term_label, COUNT(*) AS cnt
                FROM applicants
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 10;
            """)
            metrics["term_dist"] = cur.fetchall()

            # Decision Distribution
            cur.execute("""
                SELECT COALESCE(NULLIF(status,''), 'No decision detected') AS status_label, COUNT(*) AS cnt
                FROM applicants
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 10;
            """)
            metrics["decision_dist"] = cur.fetchall()

    finally:
        conn.close()

    return metrics


def main():
    m = fetch_metrics()

    print("=== Module 3 Required Questions ===")
    print(f"Q0 Total applicants: {m['total']}")
    print(f"Q1 Fall 2026 applicants: {m['fall_2026']}")
    print(f"Q2 Percent International (known nationality only): {m['pct_intl']}")
    print(f"Q3 Avg GPA (non-null): {m['avg_gpa']}")
    print(f"Q3 Avg GRE Quant (non-null): {m['avg_gre']}")
    print(f"Q3 Avg GRE Verbal (non-null): {m['avg_gre_v']}")
    print(f"Q3 Avg GRE AW (non-null): {m['avg_gre_aw']}")
    print(f"Q4 Avg GPA American (Fall 2026): {m['avg_gpa_american_fall']}")
    print(f"Q5 Acceptance % (Fall 2026, decisions only): {m['acceptance_pct']}")
    print(f"Q6 Avg GPA Accepted (Fall 2026): {m['avg_gpa_accepted']}")
    print(f"Q7 JHU Masters in Computer Science: {m['q7_jhu_ms_cs']}")
    print(f"Q8 2026 Acceptances PhD CS (raw fields): {m['q8_raw']}")
    print(f"Q9 2026 Acceptances PhD CS (LLM fields): {m['q9_llm']}")

    print("\n=== Q10 Two additional curiosity questions ===")
    print("\nQ10a Top 10 universities by Fall 2026 Computer Science applicants (LLM university):")
    for uni, cnt in m["q10a_rows"]:
        print(f"  {uni}: {cnt}")

    print("\nQ10b Acceptance rate by term (decisions only), top 5 terms by volume:")
    for term_label, term_cnt, acc_pct in m["q10b_rows"]:
        print(f"  {term_label}: {term_cnt} rows | acceptance % (decisions) = {acc_pct}")


if __name__ == "__main__":
    main()
