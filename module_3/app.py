import os
import threading
import subprocess
import psycopg2
from flask import Flask, render_template, redirect, url_for, flash, request

# -------------------------
# Flask setup
# -------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

# -------------------------
# Paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))          # module_3/
PROJECT_ROOT = os.path.dirname(BASE_DIR)                       # repo root

MODULE2_DIR = os.path.join(PROJECT_ROOT, "module_2")
MODULE3_DIR = BASE_DIR

LOAD_SCRIPT = os.path.join(MODULE3_DIR, "load_data.py")
QUERY_SCRIPT = os.path.join(MODULE3_DIR, "query_data.py")

# -------------------------
# DB config
# -------------------------
DB_NAME = os.getenv("PGDATABASE", "gradcafe")
DB_USER = os.getenv("PGUSER", os.getenv("USER", "postgres"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

FALL_2026 = "Fall 2026"

# -------------------------
# Thread-safe state
# -------------------------
_LOCK = threading.Lock()
_PULL_RUNNING = False
_PULL_MESSAGE = "No load has been run yet."
_LAST_ANALYSIS = ""  # store latest query_data.py console output


def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )


def fetch_metrics():
    """
    Compute Q1–Q10 directly from PostgreSQL and return a dict with keys
    that index.html expects.
    """
    metrics = {
        # Q1–Q9
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
        # Q10 blocks
        "q10a_title": "Top 5 terms by volume",
        "q10a_rows": [],
        "q10b_title": "Top 5 universities in Fall 2026 (LLM university)",
        "q10b_rows": [],
        # Optional distributions
        "term_dist": [],
        "decision_dist": [],
    }

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Q1
            cur.execute("SELECT COUNT(*) FROM applicants WHERE term = %s;", (FALL_2026,))
            metrics["fall_2026"] = cur.fetchone()[0] or 0

            # Q2: percent international among known nationality only
            cur.execute(
                """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN us_or_international ILIKE 'International%%' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN us_or_international IS NOT NULL AND us_or_international <> '' THEN 1 ELSE 0 END), 0),
                    2
                )
                FROM applicants;
                """
            )
            metrics["pct_intl"] = cur.fetchone()[0]

            # Q3: averages (non-null only)
            cur.execute("SELECT ROUND(AVG(gpa)::numeric, 3) FROM applicants WHERE gpa IS NOT NULL;")
            metrics["avg_gpa"] = cur.fetchone()[0]

            cur.execute("SELECT ROUND(AVG(gre)::numeric, 3) FROM applicants WHERE gre IS NOT NULL;")
            metrics["avg_gre"] = cur.fetchone()[0]

            cur.execute("SELECT ROUND(AVG(gre_v)::numeric, 3) FROM applicants WHERE gre_v IS NOT NULL;")
            metrics["avg_gre_v"] = cur.fetchone()[0]

            cur.execute("SELECT ROUND(AVG(gre_aw)::numeric, 3) FROM applicants WHERE gre_aw IS NOT NULL;")
            metrics["avg_gre_aw"] = cur.fetchone()[0]

            # Q4: avg GPA of American students in Fall 2026
            cur.execute(
                """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND us_or_international ILIKE 'American%%'
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026,),
            )
            metrics["avg_gpa_american_fall"] = cur.fetchone()[0]

            # Q5: acceptance % among decisions only for Fall 2026
            cur.execute(
                """
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
                """,
                (FALL_2026,),
            )
            metrics["acceptance_pct"] = cur.fetchone()[0]

            # Q6: avg GPA of accepted applicants in Fall 2026
            cur.execute(
                """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = %s
                  AND status ILIKE 'Accepted%%'
                  AND gpa IS NOT NULL;
                """,
                (FALL_2026,),
            )
            metrics["avg_gpa_accepted"] = cur.fetchone()[0]

            # Q7: JHU masters in CS (use raw + LLM fields; keep tolerant)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE
                  (
                    program ILIKE '%%johns hopkins%%' OR program ILIKE '%%jhu%%'
                    OR llm_generated_university ILIKE '%%johns hopkins%%' OR llm_generated_university ILIKE '%%jhu%%'
                  )
                  AND (
                    program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%'
                    OR llm_generated_program ILIKE '%%computer science%%'
                  )
                  AND (
                    degree ILIKE '%%master%%'
                    OR program ILIKE '%%master%%' OR comments ILIKE '%%master%%' OR status ILIKE '%%master%%'
                  );
                """
            )
            metrics["q7"] = cur.fetchone()[0] or 0

            # Q8: 2026 acceptances PhD CS at Georgetown / MIT / Stanford / CMU (raw fields)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE date_added >= DATE '2026-01-01'
                  AND date_added <  DATE '2027-01-01'
                  AND status ILIKE 'Accepted%%'
                  AND (program ILIKE '%%computer science%%' OR comments ILIKE '%%computer science%%')
                  AND (
                        degree ILIKE '%%phd%%'
                     OR program ILIKE '%%phd%%' OR comments ILIKE '%%phd%%'
                  )
                  AND (
                    program ILIKE '%%georgetown%%'
                    OR program ILIKE '%%massachusetts institute of technology%%' OR program ILIKE '%%mit%%'
                    OR program ILIKE '%%stanford%%'
                    OR program ILIKE '%%carnegie mellon%%' OR program ILIKE '%%cmu%%'
                  );
                """
            )
            metrics["q8"] = cur.fetchone()[0] or 0

            # Q9: same as Q8 but use LLM university/program when available
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE date_added >= DATE '2026-01-01'
                  AND date_added <  DATE '2027-01-01'
                  AND status ILIKE 'Accepted%%'
                  AND (
                       llm_generated_program ILIKE '%%computer science%%'
                       OR program ILIKE '%%computer science%%'
                       OR comments ILIKE '%%computer science%%'
                  )
                  AND (
                        degree ILIKE '%%phd%%'
                     OR program ILIKE '%%phd%%' OR comments ILIKE '%%phd%%'
                  )
                  AND (
                    llm_generated_university ILIKE '%%georgetown%%'
                    OR llm_generated_university ILIKE '%%massachusetts institute of technology%%' OR llm_generated_university ILIKE '%%mit%%'
                    OR llm_generated_university ILIKE '%%stanford%%'
                    OR llm_generated_university ILIKE '%%carnegie mellon%%' OR llm_generated_university ILIKE '%%cmu%%'
                    OR (
                        llm_generated_university IS NULL OR llm_generated_university = ''
                      ) AND (
                        program ILIKE '%%georgetown%%'
                        OR program ILIKE '%%massachusetts institute of technology%%' OR program ILIKE '%%mit%%'
                        OR program ILIKE '%%stanford%%'
                        OR program ILIKE '%%carnegie mellon%%' OR program ILIKE '%%cmu%%'
                      )
                  );
                """
            )
            metrics["q9"] = cur.fetchone()[0] or 0

            # Q10a: term distribution top 5 (also reused as "curiosity" block)
            cur.execute(
                """
                SELECT COALESCE(NULLIF(term,''), 'No term detected') AS term_label, COUNT(*)::int
                FROM applicants
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 5;
                """
            )
            metrics["q10a_rows"] = cur.fetchall()

            # Q10b: top 5 universities in Fall 2026 using LLM uni (fallback Unknown)
            cur.execute(
                """
                SELECT COALESCE(NULLIF(llm_generated_university,''), 'Unknown') AS uni, COUNT(*)::int
                FROM applicants
                WHERE term = %s
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 5;
                """,
                (FALL_2026,),
            )
            metrics["q10b_rows"] = cur.fetchall()

            # Optional: term_dist top 10
            cur.execute(
                """
                SELECT COALESCE(NULLIF(term,''), 'No term detected') AS term_label, COUNT(*)::int
                FROM applicants
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 10;
                """
            )
            metrics["term_dist"] = cur.fetchall()

            # Optional: decision_dist
            cur.execute(
                """
                SELECT COALESCE(NULLIF(status,''), 'No status') AS status_label, COUNT(*)::int
                FROM applicants
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 10;
                """
            )
            metrics["decision_dist"] = cur.fetchall()

    finally:
        conn.close()

    return metrics


def _run_script(script_path: str) -> tuple[int, str]:
    """Run a python script and return (returncode, combined_output)."""
    proc = subprocess.run(
        ["python", script_path],
        cwd=os.path.dirname(script_path),
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode, out.strip()


def _pull_worker(max_records: int | None = None):
    """
    Pull Data should run Module 2 scraper + cleaner (+ LLM if you decide),
    then load into Postgres via module_3/load_data.py.
    """
    global _PULL_RUNNING, _PULL_MESSAGE

    def run(cmd: list[str], cwd: str):
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        out = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        print("\n=== RUN:", " ".join(cmd), "===\n", out, "\n")
        return proc.returncode

    try:
        # 1) Module 2 scrape
        code = run(["python", "scrape.py"], cwd=MODULE2_DIR)
        if code != 0:
            raise RuntimeError("module_2/scrape.py failed")

        # 2) Module 2 clean
        code = run(["python", "clean.py"], cwd=MODULE2_DIR)
        if code != 0:
            raise RuntimeError("module_2/clean.py failed")

        # 3) OPTIONAL: run Module 2 LLM standardizer (CLI) to generate JSONL
        # If you want this to run every time, point --file to your LLM input file.
        # (Many students create module_2/llm_in.json first.)
        llm_hosting_dir = os.path.join(MODULE2_DIR, "llm_hosting")
        llm_in = os.path.join(MODULE2_DIR, "llm_in.json")  # <-- change if your input file name differs
        llm_out = os.path.join(MODULE2_DIR, "llm_extend_applicant_data.json")

        if os.path.exists(llm_in):
            code = run(
                ["python", "app.py", "--file", llm_in, "--out", llm_out],
                cwd=llm_hosting_dir,
            )
            if code != 0:
                raise RuntimeError("module_2/llm_hosting/app.py CLI failed")
        else:
            print(f"Skipping LLM step (no input file found): {llm_in}")

        # 4) Module 3 load into Postgres
        code = run(["python", "load_data.py"], cwd=MODULE3_DIR)
        if code != 0:
            raise RuntimeError("module_3/load_data.py failed")

        msg = "Pull Data complete: scraped + cleaned (and LLM if available) then loaded into PostgreSQL."
    except Exception as e:
        msg = f"Pull Data failed: {e}"
    finally:
        with _LOCK:
            _PULL_RUNNING = False
            _PULL_MESSAGE = msg


def _analysis_worker():
    global _LAST_ANALYSIS
    code, out = _run_script(QUERY_SCRIPT)
    with _LOCK:
        _LAST_ANALYSIS = out
    print("\n=== query_data.py output ===\n", out, "\n")


@app.get("/")
def index():
    with _LOCK:
        pull_running = _PULL_RUNNING
        pull_message = _PULL_MESSAGE
        last_analysis = _LAST_ANALYSIS

    metrics = fetch_metrics()

    return render_template(
        "index.html",
        metrics=metrics,               # IMPORTANT: template expects this
        pull_running=pull_running,     # IMPORTANT: rename template vars accordingly
        pull_message=pull_message,
        last_analysis=last_analysis,
    )


@app.post("/pull-data")
def pull_data():
    global _PULL_RUNNING, _PULL_MESSAGE
    max_records = request.form.get("max_records")  # optional from UI

    with _LOCK:
        if _PULL_RUNNING:
            flash("Pull Data is already running. Please wait.", "warning")
            return redirect(url_for("index"))
        _PULL_RUNNING = True
        _PULL_MESSAGE = "Pull Data started: running Module 2 scrape+clean (and optional LLM) then loading into DB…"

    threading.Thread(target=_pull_worker, args=(max_records,), daemon=True).start()
    flash("Pull Data started. When it finishes, click Update Analysis.", "info")
    return redirect(url_for("index"))


@app.post("/update-analysis")
def update_analysis():
    with _LOCK:
        if _PULL_RUNNING:
            flash("Pull Data is running — Update Analysis is disabled until it finishes.", "warning")
            return redirect(url_for("index"))

    threading.Thread(target=_analysis_worker, daemon=True).start()
    flash("Update Analysis started: running query_data.py…", "info")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=False, use_reloader=False)
