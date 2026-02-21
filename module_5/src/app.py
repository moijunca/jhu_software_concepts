"""Flask application for GradCafe Analytics (Module 5)."""
import os
import threading
import subprocess
import psycopg
from flask import Flask, render_template, jsonify
from psycopg import sql

_LOCK = threading.Lock()
_PULL_RUNNING: bool = False
_PULL_MESSAGE: str = "No load has been run yet."
_LAST_ANALYSIS: str = ""
FALL_2026 = "Fall 2026"

def _build_conninfo(database_url=None):
    """Return psycopg3-compatible connection string."""
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
    return psycopg.connect(_build_conninfo(
        app.config.get("DATABASE_URL") if app else None))

def fetch_metrics(app=None):
    """Fetch analytics metrics from the database."""
    metrics = {
        "fall_2026": 0, "pct_intl": None, "avg_gpa": None, "avg_gre": None,
        "avg_gre_v": None, "avg_gre_aw": None, "avg_gpa_american_fall": None,
        "acceptance_pct": None, "avg_gpa_accepted": None,
        "q7": 0, "q8": 0, "q9": 0,
        "q10a_title": "Top 5 terms by volume", "q10a_rows": [],
        "q10b_title": "Top 5 universities in Fall 2026", "q10b_rows": [],
        "term_dist": [], "decision_dist": [],
    }
    try:
        conn = get_conn(app)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM applicants WHERE term = %s;", (FALL_2026,))
                metrics["fall_2026"] = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT ROUND(
                        100.0 * SUM(CASE WHEN us_or_international ILIKE 'International%%' THEN 1 ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN us_or_international IS NOT NULL
                                              AND us_or_international <> '' THEN 1 ELSE 0 END), 0), 2)
                    FROM applicants;
                """)
                for col, key in [("gpa","avg_gpa"),("gre","avg_gre"),("gre_v","avg_gre_v"),("gre_aw","avg_gre_aw")]:
                    query = sql.SQL(
                        "SELECT ROUND(AVG({col})::numeric, 3) FROM applicants WHERE {col} IS NOT NULL"
                    ).format(col=sql.Identifier(col))
                    cur.execute(query)
                    metrics[key] = cur.fetchone()[0]
                    metrics[key] = cur.fetchone()[0]
                cur.execute("""
                    SELECT ROUND(AVG(gpa)::numeric, 3) FROM applicants
                    WHERE term = %s AND us_or_international ILIKE 'American%%' AND gpa IS NOT NULL;
                """, (FALL_2026,))
                metrics["avg_gpa_american_fall"] = cur.fetchone()[0]
                cur.execute("""
                    SELECT ROUND(
                        100.0 * SUM(CASE WHEN status ILIKE 'Accepted%%' THEN 1 ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN status ILIKE 'Accepted%%' OR status ILIKE 'Rejected%%'
                            OR status ILIKE 'Waitlisted%%' OR status ILIKE 'Interview%%'
                            THEN 1 ELSE 0 END), 0), 2)
                    FROM applicants WHERE term = %s;
                """, (FALL_2026,))
                metrics["acceptance_pct"] = cur.fetchone()[0]
                cur.execute("""
                    SELECT ROUND(AVG(gpa)::numeric, 3) FROM applicants
                    WHERE term = %s AND status ILIKE 'Accepted%%' AND gpa IS NOT NULL;
                """, (FALL_2026,))
                metrics["avg_gpa_accepted"] = cur.fetchone()[0]
                cur.execute("""
                    SELECT COUNT(*) FROM applicants
                    WHERE (program ILIKE '%%johns hopkins%%' OR program ILIKE '%%jhu%%'
                        OR llm_generated_university ILIKE '%%johns hopkins%%')
                    AND (program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%'
                        OR llm_generated_program ILIKE '%%computer science%%')
                    AND (degree ILIKE 'Master%%' OR program ILIKE '%%master%%');
                """)
                metrics["q7"] = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT COUNT(*) FROM applicants
                    WHERE status ILIKE 'Accepted%%'
                    AND date_added >= DATE '2026-01-01' AND date_added < DATE '2027-01-01'
                    AND (program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%')
                    AND (degree = 'PhD' OR program ILIKE '%%phd%%')
                    AND (program ILIKE '%%mit%%' OR program ILIKE '%%stanford%%'
                        OR program ILIKE '%%carnegie mellon%%' OR program ILIKE '%%georgetown%%');
                """)
                metrics["q8"] = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT COUNT(*) FROM applicants
                    WHERE status ILIKE 'Accepted%%'
                    AND date_added >= DATE '2026-01-01' AND date_added < DATE '2027-01-01'
                    AND (llm_generated_program ILIKE '%%computer science%%' OR program ILIKE '%%computer science%%')
                    AND (degree = 'PhD' OR program ILIKE '%%phd%%')
                    AND (llm_generated_university ILIKE '%%mit%%' OR llm_generated_university ILIKE '%%stanford%%'
                        OR llm_generated_university ILIKE '%%carnegie mellon%%'
                        OR program ILIKE '%%mit%%' OR program ILIKE '%%stanford%%');
                """)
                metrics["q9"] = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT COALESCE(NULLIF(term,''), 'No term detected'), COUNT(*)::int
                    FROM applicants GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5;
                """)
                metrics["q10a_rows"] = cur.fetchall()
                cur.execute("""
                    SELECT COALESCE(NULLIF(llm_generated_university,''), 'Unknown'), COUNT(*)::int
                    FROM applicants WHERE term = %s GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5;
                """, (FALL_2026,))
                metrics["q10b_rows"] = cur.fetchall()
                cur.execute("""
                    SELECT COALESCE(NULLIF(term,''), 'No term detected'), COUNT(*)::int
                    FROM applicants GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10;
                """)
                metrics["term_dist"] = cur.fetchall()
                cur.execute("""
                    SELECT COALESCE(NULLIF(status,''), 'No status'), COUNT(*)::int
                    FROM applicants GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10;
                """)
                metrics["decision_dist"] = cur.fetchall()
        finally:
            conn.close()
    except Exception:
        pass
    return metrics

def _load_rows(app, rows):
    if not rows:
        return
    conn = get_conn(app)
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO applicants (
                        program, comments, date_added, url, status, term,
                        us_or_international, gpa, gre, gre_v, gre_aw,
                        degree, llm_generated_program, llm_generated_university
                    ) VALUES (%(program)s, %(comments)s, %(date_added)s, %(url)s,
                              %(status)s, %(term)s, %(us_or_international)s,
                              %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s,
                              %(degree)s, %(llm_generated_program)s, %(llm_generated_university)s)
                    ON CONFLICT DO NOTHING;
                """, row)
        conn.commit()
    finally:
        conn.close()

def _pull_worker(app, module2_dir, module3_dir):
    global _PULL_RUNNING, _PULL_MESSAGE
    scraper_fn = app.config.get("SCRAPER_FN")
    try:
        if scraper_fn is not None:
            rows = scraper_fn()
            _load_rows(app, rows)
        else:
            def _run(cmd, cwd):
                proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
                return proc.returncode
            if _run(["python", "scrape.py"], module2_dir) != 0:  # pragma: no cover
                raise RuntimeError("scrape.py failed")  # pragma: no cover
            if _run(["python", "clean.py"], module2_dir) != 0:  # pragma: no cover
                raise RuntimeError("clean.py failed")  # pragma: no cover
            if _run(["python", "load_data.py"], module3_dir) != 0:  # pragma: no cover
                raise RuntimeError("load_data.py failed")  # pragma: no cover
        msg = "Pull Data complete."
    except Exception as exc:
        msg = f"Pull Data failed: {exc}"
    finally:
        with _LOCK:
            _PULL_RUNNING = False
            _PULL_MESSAGE = msg

def _analysis_worker(app):
    global _LAST_ANALYSIS
    import sys
    src_dir = os.path.dirname(os.path.abspath(__file__))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)  # pragma: no cover
    from query_data import fetch_metrics as qd_fetch
    m = qd_fetch(app)
    with _LOCK:
        _LAST_ANALYSIS = str(m)

def create_app(config=None):
    """Create and configure Flask application."""
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")
    this_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(this_dir)
    app.config["MODULE2_DIR"] = os.path.join(os.path.dirname(repo_root), "module_2")
    app.config["MODULE3_DIR"] = this_dir
    if config:
        app.config.update(config)

    @app.get("/")
    def index():
        with _LOCK:
            pull_running  = _PULL_RUNNING
            pull_message  = _PULL_MESSAGE
            last_analysis = _LAST_ANALYSIS
        metrics = fetch_metrics(app)
        return render_template("index.html", metrics=metrics,
                               pull_running=pull_running,
                               pull_message=pull_message,
                               last_analysis=last_analysis)

    @app.post("/pull-data")
    def pull_data():
        global _PULL_RUNNING, _PULL_MESSAGE
        with _LOCK:
            if _PULL_RUNNING:
                return jsonify({"busy": True}), 409
            _PULL_RUNNING = True
            _PULL_MESSAGE = "Pull Data started..."
        threading.Thread(target=_pull_worker,
                         args=(app, app.config["MODULE2_DIR"], app.config["MODULE3_DIR"]),
                         daemon=True).start()
        return jsonify({"ok": True}), 200

    @app.post("/update-analysis")
    def update_analysis():
        with _LOCK:
            if _PULL_RUNNING:
                return jsonify({"busy": True}), 409
        threading.Thread(target=_analysis_worker, args=(app,), daemon=True).start()
        return jsonify({"ok": True}), 200

    return app

if __name__ == "__main__":
    flask_app = create_app()
    flask_app.run(host="127.0.0.1", port=8000, debug=False, use_reloader=False)
