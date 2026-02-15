"""
app.py – GradCafe Analytics Flask application (Module 4).

Key changes from Module 3:
- create_app() factory for testability (tests can call create_app() directly).
- psycopg3 (import psycopg) replaces psycopg2.
- DATABASE_URL env-var is the primary DB connection string; individual
  PGDATABASE / PGUSER / … vars are kept as fallback for local dev.
- POST /pull-data and POST /update-analysis return HTTP 409 JSON
  {"busy": true} instead of a redirect when the server is busy,
  so tests can assert the status code without following redirects.
- Buttons carry data-testid="pull-data-btn" and
  data-testid="update-analysis-btn" for stable UI selectors.
"""

import os
import threading
import subprocess
from typing import Optional

import psycopg  # psycopg3
from flask import Flask, render_template, redirect, url_for, flash, request, jsonify

# ---------------------------------------------------------------------------
# Thread-safe busy state (module-level so it survives across requests)
# ---------------------------------------------------------------------------
_LOCK = threading.Lock()
_PULL_RUNNING: bool = False
_PULL_MESSAGE: str = "No load has been run yet."
_LAST_ANALYSIS: str = ""

FALL_2026 = "Fall 2026"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _build_conninfo(database_url: Optional[str] = None) -> str:
    """
    Return a psycopg3-compatible connection string.

    Priority:
      1. Explicit database_url argument (used by tests via app config).
      2. DATABASE_URL environment variable.
      3. Individual PG* environment variables.
    """
    url = database_url or os.getenv("DATABASE_URL")
    if url:
        return url
    db   = os.getenv("PGDATABASE", "gradcafe")
    user = os.getenv("PGUSER", os.getenv("USER", "postgres"))
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    return f"dbname={db} user={user} host={host} port={port}"


def get_conn(app=None):
    """Open and return a psycopg3 connection."""
    conninfo = _build_conninfo(
        app.config.get("DATABASE_URL") if app else None
    )
    return psycopg.connect(conninfo)


# ---------------------------------------------------------------------------
# Analysis / metrics
# ---------------------------------------------------------------------------

def fetch_metrics(app=None) -> dict:
    """
    Compute Q1–Q10 directly from PostgreSQL.

    Returns a dict whose keys match what index.html expects.
    """
    metrics = {
        "fall_2026": 0,
        "pct_intl": None,
        "avg_gpa": None,
        "avg_gre": None,
        "avg_gre_v": None,
        "avg_gre_aw": None,
        "avg_gpa_american_fall": None,
        "acceptance_pct": None,
        "avg_gpa_accepted": None,
        "q7": 0,
        "q8": 0,
        "q9": 0,
        "q10a_title": "Top 5 terms by volume",
        "q10a_rows": [],
        "q10b_title": "Top 5 universities in Fall 2026 (LLM university)",
        "q10b_rows": [],
        "term_dist": [],
        "decision_dist": [],
    }

    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            # Q1
            cur.execute(
                "SELECT COUNT(*) FROM applicants WHERE term = %s;",
                (FALL_2026,),
            )
            metrics["fall_2026"] = cur.fetchone()[0] or 0

            # Q2: percent international among known nationality only
            cur.execute("""
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE 'International%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN us_or_international IS NOT NULL
                                          AND us_or_international <> '' THEN 1 ELSE 0 END), 0),
                    2
                )
                FROM applicants;
            """)
            metrics["pct_intl"] = cur.fetchone()[0]

            # Q3: averages (non-null only)
            for col, key in [
                ("gpa",    "avg_gpa"),
                ("gre",    "avg_gre"),
                ("gre_v",  "avg_gre_v"),
                ("gre_aw", "avg_gre_aw"),
            ]:
                cur.execute(
                    f"SELECT ROUND(AVG({col})::numeric, 3) "
                    f"FROM applicants WHERE {col} IS NOT NULL;"
                )
                metrics[key] = cur.fetchone()[0]

            # Q4: avg GPA of American students in Fall 2026
            cur.execute("""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE 'American%'
                  AND gpa IS NOT NULL;
            """, (FALL_2026,))
            metrics["avg_gpa_american_fall"] = cur.fetchone()[0]

            # Q5: acceptance % among decisions only for Fall 2026
            cur.execute("""
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
            metrics["acceptance_pct"] = cur.fetchone()[0]

            # Q6: avg GPA of accepted applicants in Fall 2026
            cur.execute("""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE 'Accepted%'
                  AND gpa IS NOT NULL;
            """, (FALL_2026,))
            metrics["avg_gpa_accepted"] = cur.fetchone()[0]

            # Q7: JHU masters in CS
            cur.execute("""
                SELECT COUNT(*)
                FROM applicants
                WHERE (
                    program ILIKE '%johns hopkins%' OR program ILIKE '%jhu%'
                    OR llm_generated_university ILIKE '%johns hopkins%'
                    OR llm_generated_university ILIKE '%jhu%'
                  )
                  AND (
                    program ILIKE '%computer science%'
                    OR comments ILIKE '%computer science%'
                    OR llm_generated_program ILIKE '%computer science%'
                  )
                  AND (
                    degree ILIKE '%master%'
                    OR program ILIKE '%master%'
                    OR comments ILIKE '%master%'
                  );
            """)
            metrics["q7"] = cur.fetchone()[0] or 0

            # Q8: 2026 acceptances PhD CS at Georgetown/MIT/Stanford/CMU (raw)
            cur.execute("""
                SELECT COUNT(*)
                FROM applicants
                WHERE date_added >= DATE '2026-01-01'
                  AND date_added <  DATE '2027-01-01'
                  AND status ILIKE 'Accepted%'
                  AND (program ILIKE '%computer science%'
                       OR comments ILIKE '%computer science%')
                  AND (degree ILIKE '%phd%'
                       OR program ILIKE '%phd%'
                       OR comments ILIKE '%phd%')
                  AND (
                    program ILIKE '%georgetown%'
                    OR program ILIKE '%massachusetts institute of technology%'
                    OR program ILIKE '%mit%'
                    OR program ILIKE '%stanford%'
                    OR program ILIKE '%carnegie mellon%'
                    OR program ILIKE '%cmu%'
                  );
            """)
            metrics["q8"] = cur.fetchone()[0] or 0

            # Q9: same as Q8 using LLM fields
            cur.execute("""
                SELECT COUNT(*)
                FROM applicants
                WHERE date_added >= DATE '2026-01-01'
                  AND date_added <  DATE '2027-01-01'
                  AND status ILIKE 'Accepted%'
                  AND (
                    llm_generated_program ILIKE '%computer science%'
                    OR program ILIKE '%computer science%'
                    OR comments ILIKE '%computer science%'
                  )
                  AND (degree ILIKE '%phd%'
                       OR program ILIKE '%phd%'
                       OR comments ILIKE '%phd%')
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
            """)
            metrics["q9"] = cur.fetchone()[0] or 0

            # Q10a: top 5 terms by volume
            cur.execute("""
                SELECT COALESCE(NULLIF(term,''), 'No term detected') AS term_label,
                       COUNT(*)::int
                FROM applicants
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 5;
            """)
            metrics["q10a_rows"] = cur.fetchall()

            # Q10b: top 5 universities in Fall 2026 (LLM university)
            cur.execute("""
                SELECT COALESCE(NULLIF(llm_generated_university,''), 'Unknown') AS uni,
                       COUNT(*)::int
                FROM applicants
                WHERE term = %s
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 5;
            """, (FALL_2026,))
            metrics["q10b_rows"] = cur.fetchall()

            # Optional: term_dist top 10
            cur.execute("""
                SELECT COALESCE(NULLIF(term,''), 'No term detected') AS term_label,
                       COUNT(*)::int
                FROM applicants
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 10;
            """)
            metrics["term_dist"] = cur.fetchall()

            # Optional: decision_dist top 10
            cur.execute("""
                SELECT COALESCE(NULLIF(status,''), 'No status') AS status_label,
                       COUNT(*)::int
                FROM applicants
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 10;
            """)
            metrics["decision_dist"] = cur.fetchall()

    finally:
        conn.close()

    return metrics


# ---------------------------------------------------------------------------
# Background workers
# ---------------------------------------------------------------------------

def _pull_worker(app, module2_dir: str, module3_dir: str):
    """
    Run scrape → clean → load_data pipeline in a background thread.

    Accepts the Flask app object so it can read config (DATABASE_URL).
    Scraper / cleaner are injected via app.config["SCRAPER_FN"] so tests
    can substitute a fake without touching the file system.
    """
    global _PULL_RUNNING, _PULL_MESSAGE

    scraper_fn = app.config.get("SCRAPER_FN")

    def _run_subprocess(cmd, cwd):
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        out = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        print(f"\n=== RUN: {' '.join(cmd)} ===\n{out}\n")
        return proc.returncode, out

    try:
        if scraper_fn is not None:
            # Injected fake scraper (used by tests)
            rows = scraper_fn()
            _load_rows(app, rows)
        else:
            # Real pipeline: module_2 scrape + clean + load_data.py
            rc, _ = _run_subprocess(["python", "scrape.py"], module2_dir)
            if rc != 0:
                raise RuntimeError("scrape.py failed")
            rc, _ = _run_subprocess(["python", "clean.py"], module2_dir)
            if rc != 0:
                raise RuntimeError("clean.py failed")
            rc, _ = _run_subprocess(["python", "load_data.py"], module3_dir)
            if rc != 0:
                raise RuntimeError("load_data.py failed")

        msg = "Pull Data complete."
    except Exception as exc:
        msg = f"Pull Data failed: {exc}"
    finally:
        with _LOCK:
            _PULL_RUNNING = False
            _PULL_MESSAGE = msg


def _load_rows(app, rows: list):
    """
    Insert a list of dicts (from the fake scraper in tests) into the DB.

    Each dict must have the same keys as the applicants table columns.
    """
    if not rows:
        return

    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO applicants (
                        program, comments, date_added, url,
                        status, term, us_or_international,
                        gpa, gre, gre_v, gre_aw,
                        degree, llm_generated_program, llm_generated_university
                    )
                    VALUES (%(program)s, %(comments)s, %(date_added)s, %(url)s,
                            %(status)s, %(term)s, %(us_or_international)s,
                            %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s,
                            %(degree)s, %(llm_generated_program)s,
                            %(llm_generated_university)s)
                    ON CONFLICT (url, program, comments) DO NOTHING;
                """, row)
        conn.commit()
    finally:
        conn.close()


def _analysis_worker(app):
    """Refresh _LAST_ANALYSIS from query_data.fetch_metrics()."""
    global _LAST_ANALYSIS
    # Import lazily to avoid circular deps and allow module path overrides
    import importlib, sys
    src_dir = os.path.join(os.path.dirname(__file__))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    from query_data import fetch_metrics as qd_fetch
    m = qd_fetch(app)
    with _LOCK:
        _LAST_ANALYSIS = str(m)


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(config: Optional[dict] = None) -> Flask:
    """
    Flask application factory.

    Parameters
    ----------
    config : dict, optional
        Override default configuration. Useful keys:
          - DATABASE_URL : str  — psycopg3 connection string for the test DB.
          - SCRAPER_FN   : callable() -> list[dict]  — fake scraper for tests.
          - TESTING      : bool
    """
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

    # Resolve paths relative to this file's location
    this_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(this_dir)  # module_4/
    app.config["MODULE2_DIR"] = os.path.join(os.path.dirname(repo_root), "module_2")
    app.config["MODULE3_DIR"] = this_dir

    if config:
        app.config.update(config)

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.get("/")
    def index():
        with _LOCK:
            pull_running  = _PULL_RUNNING
            pull_message  = _PULL_MESSAGE
            last_analysis = _LAST_ANALYSIS

        try:
            metrics = fetch_metrics(app)
        except Exception:
            metrics = {}

        return render_template(
            "index.html",
            metrics=metrics,
            pull_running=pull_running,
            pull_message=pull_message,
            last_analysis=last_analysis,
        )

    @app.post("/pull-data")
    def pull_data():
        global _PULL_RUNNING, _PULL_MESSAGE

        with _LOCK:
            if _PULL_RUNNING:
                # Return 409 so tests can assert on status code directly
                return jsonify({"busy": True}), 409
            _PULL_RUNNING = True
            _PULL_MESSAGE = "Pull Data started…"

        threading.Thread(
            target=_pull_worker,
            args=(app, app.config["MODULE2_DIR"], app.config["MODULE3_DIR"]),
            daemon=True,
        ).start()

        return jsonify({"ok": True}), 200

    @app.post("/update-analysis")
    def update_analysis():
        with _LOCK:
            if _PULL_RUNNING:
                return jsonify({"busy": True}), 409

        threading.Thread(
            target=_analysis_worker,
            args=(app,),
            daemon=True,
        ).start()

        return jsonify({"ok": True}), 200

    return app


# ---------------------------------------------------------------------------
# Dev entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    flask_app = create_app()
    flask_app.run(host="127.0.0.1", port=8000, debug=False, use_reloader=False)
