"""SQL query functions for GradCafe analytics."""
from typing import Optional
import os
from psycopg import sql
import psycopg

FALL_2026 = "Fall 2026"

def _build_conninfo(app=None) -> str:
    url = (app.config.get("DATABASE_URL") if app else None) or os.getenv("DATABASE_URL")
    if url:
        return url
    db   = os.getenv("PGDATABASE", "gradcafe")
    user = os.getenv("PGUSER", os.getenv("USER", "postgres"))
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    return f"dbname={db} user={user} host={host} port={port}"

def get_conn(app=None):
    """Execute SQL query and return first column of first row."""
    return psycopg.connect(_build_conninfo(app))

def _one(cur, query, params=None):
    cur.execute(query, params or ())
    row = cur.fetchone()
    return row[0] if row else None
def fetch_metrics(app=None) -> dict:
    """Fetch all analytics metrics from database."""
    metrics = {
        "total": None, "fall_2026": None, "pct_intl": None,
        "avg_gpa": None, "avg_gre": None, "avg_gre_v": None, "avg_gre_aw": None,
        "avg_gpa_american_fall": None, "acceptance_pct": None, "avg_gpa_accepted": None,
        "q7_jhu_ms_cs": None, "q8_raw": None, "q9_llm": None,
        "q10a_rows": [], "q10b_rows": [], "term_dist": [], "decision_dist": [],
    }
    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            metrics["total"] = _one(cur, "SELECT COUNT(*) FROM applicants LIMIT 1;")
            metrics["fall_2026"] = _one(cur,
                "SELECT COUNT(*) FROM applicants WHERE term = %s LIMIT 1;", (FALL_2026,))
            metrics["pct_intl"] = _one(cur, """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE 'International%%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN us_or_international IS NOT NULL
                                          AND us_or_international <> '' THEN 1 ELSE 0 END), 0), 2)
                FROM applicants LIMIT 1;
            """)
            for col, key in [("gpa","avg_gpa"),("gre","avg_gre"),("gre_v","avg_gre_v"),("gre_aw","avg_gre_aw")]:
                query = sql.SQL(
                    "SELECT ROUND(AVG({col})::numeric, 3) FROM applicants WHERE {col} IS NOT NULL LIMIT 1"
                ).format(col=sql.Identifier(col))
                metrics[key] = _one(cur, query)
            metrics["avg_gpa_american_fall"] = _one(cur, """
                SELECT ROUND(AVG(gpa)::numeric, 3) FROM applicants
                WHERE term = %s AND us_or_international ILIKE 'American%%' AND gpa IS NOT NULL;
            """, (FALL_2026,))
            metrics["acceptance_pct"] = _one(cur, """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN status ILIKE 'Accepted%%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN status ILIKE 'Accepted%%' OR status ILIKE 'Rejected%%'
                        OR status ILIKE 'Waitlisted%%' OR status ILIKE 'Interview%%'
                        THEN 1 ELSE 0 END), 0), 2)
                FROM applicants WHERE term = %s;
            """, (FALL_2026,))
            metrics["avg_gpa_accepted"] = _one(cur, """
                SELECT ROUND(AVG(gpa)::numeric, 3) FROM applicants
                WHERE term = %s AND status ILIKE 'Accepted%%' AND gpa IS NOT NULL LIMIT 1;
            """, (FALL_2026,))
            metrics["q7_jhu_ms_cs"] = _one(cur, """
                SELECT COUNT(*) FROM applicants
                WHERE (program ILIKE '%%johns hopkins%%' OR program ILIKE '%%jhu%%'
                    OR llm_generated_university ILIKE '%%johns hopkins%%'
                    OR llm_generated_university ILIKE '%%jhu%%')
                AND (program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%'
                    OR llm_generated_program ILIKE '%%computer science%%')
                AND (degree ILIKE 'Master%%' OR program ILIKE '%%master%%' OR comments ILIKE '%%master%%');
            """)
            metrics["q8_raw"] = _one(cur, """
                SELECT COUNT(*) FROM applicants
                WHERE status ILIKE 'Accepted%%'
                AND ((date_added >= DATE '2026-01-01' AND date_added < DATE '2027-01-01') OR term ILIKE '%%2026%%')
                AND (program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%')
                AND (degree = 'PhD' OR program ILIKE '%%phd%%' OR comments ILIKE '%%phd%%')
                AND (program ILIKE '%%georgetown%%' OR program ILIKE '%%mit%%'
                    OR program ILIKE '%%stanford%%' OR program ILIKE '%%carnegie mellon%%'
                    OR program ILIKE '%%cmu%%');
            """)
            metrics["q9_llm"] = _one(cur, """
                SELECT COUNT(*) FROM applicants
                WHERE status ILIKE 'Accepted%%'
                AND ((date_added >= DATE '2026-01-01' AND date_added < DATE '2027-01-01') OR term ILIKE '%%2026%%')
                AND (llm_generated_program ILIKE '%%computer science%%' OR program ILIKE '%%computer science%%')
                AND (degree = 'PhD' OR program ILIKE '%%phd%%' OR comments ILIKE '%%phd%%')
                AND (llm_generated_university ILIKE '%%georgetown%%' OR llm_generated_university ILIKE '%%mit%%'
                    OR llm_generated_university ILIKE '%%stanford%%'
                    OR llm_generated_university ILIKE '%%carnegie mellon%%'
                    OR program ILIKE '%%georgetown%%' OR program ILIKE '%%mit%%'
                    OR program ILIKE '%%stanford%%' OR program ILIKE '%%carnegie mellon%%');
            """)
            cur.execute("""
                SELECT COALESCE(NULLIF(term,''), 'No term detected'), COUNT(*)::int
                FROM applicants GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10;
            """)
            metrics["term_dist"] = cur.fetchall()
            cur.execute("""
                SELECT COALESCE(NULLIF(status,''), 'No decision detected'), COUNT(*)::int
                FROM applicants GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10;
            """)
            metrics["decision_dist"] = cur.fetchall()
            cur.execute("""
                SELECT COALESCE(NULLIF(llm_generated_university,''), 'Unknown'), COUNT(*)::int
                FROM applicants WHERE term = %s GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5;
            """, (FALL_2026,))
            metrics["q10a_rows"] = cur.fetchall()
            metrics["q10b_rows"] = metrics["q10a_rows"]
    finally:
        conn.close()
    return metrics

def main():
    """CLI entry point for running queries."""
    m = fetch_metrics()
    print(f"Total: {m['total']}, Fall 2026: {m['fall_2026']}")

if __name__ == "__main__":
    main()
