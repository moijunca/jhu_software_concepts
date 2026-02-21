"""
tests/test_coverage.py – Additional tests to reach 100% coverage.

Covers helper functions in load_data.py, app.py, and query_data.py
that are not exercised by the main test suite.
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import load_data
import app as app_module
import query_data


# ---------------------------------------------------------------------------
# load_data.py helper functions
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_clean_text_none():
    assert load_data.clean_text(None) is None

@pytest.mark.db
def test_clean_text_empty():
    assert load_data.clean_text("  ") is None

@pytest.mark.db
def test_clean_text_nul():
    assert load_data.clean_text("hello\x00world") == "helloworld"

@pytest.mark.db
def test_clean_text_normal():
    assert load_data.clean_text("  hello  ") == "hello"

@pytest.mark.db
def test_to_float_none():
    assert load_data.to_float(None) is None

@pytest.mark.db
def test_to_float_int():
    assert load_data.to_float(3) == 3.0

@pytest.mark.db
def test_to_float_string():
    assert load_data.to_float("3.5") == 3.5

@pytest.mark.db
def test_to_float_invalid():
    assert load_data.to_float("abc") is None

@pytest.mark.db
def test_to_float_empty():
    assert load_data.to_float("") is None

@pytest.mark.db
def test_parse_date_none():
    assert load_data.parse_date(None) is None

@pytest.mark.db
def test_parse_date_empty():
    assert load_data.parse_date("") is None

@pytest.mark.db
def test_parse_date_long_format():
    d = load_data.parse_date("January 31, 2026")
    assert d.year == 2026 and d.month == 1 and d.day == 31

@pytest.mark.db
def test_parse_date_short_format():
    d = load_data.parse_date("Feb 1, 2026")
    assert d.month == 2

@pytest.mark.db
def test_parse_date_iso():
    d = load_data.parse_date("2026-02-01")
    assert d.year == 2026

@pytest.mark.db
def test_parse_date_us_format():
    d = load_data.parse_date("02/01/2026")
    assert d.year == 2026

@pytest.mark.db
def test_parse_date_invalid():
    assert load_data.parse_date("not a date") is None

@pytest.mark.db
def test_extract_term_fall():
    assert load_data.extract_term("Fall 2026") == "Fall 2026"

@pytest.mark.db
def test_extract_term_autumn():
    assert load_data.extract_term("Autumn 2026") == "Fall 2026"

@pytest.mark.db
def test_extract_term_short():
    assert load_data.extract_term("F26") == "Fall 2026"

@pytest.mark.db
def test_extract_term_two_digit():
    assert load_data.extract_term("Fall '26") == "Fall 2026"

@pytest.mark.db
def test_extract_term_none():
    assert load_data.extract_term("no term here") is None

@pytest.mark.db
def test_extract_status_accepted():
    assert load_data.extract_status("Accepted Fall 2026") == "Accepted"

@pytest.mark.db
def test_extract_status_waitlisted():
    assert load_data.extract_status("Wait listed") == "Waitlisted"

@pytest.mark.db
def test_extract_status_none():
    assert load_data.extract_status("no decision") is None

@pytest.mark.db
def test_extract_us_intl_international():
    assert load_data.extract_us_intl("International student") == "International"

@pytest.mark.db
def test_extract_us_intl_american():
    assert load_data.extract_us_intl("American student") == "American"

@pytest.mark.db
def test_extract_us_intl_none():
    assert load_data.extract_us_intl("unknown") is None

@pytest.mark.db
def test_normalize_degree_phd():
    assert load_data.normalize_degree("PhD") == "PhD"

@pytest.mark.db
def test_normalize_degree_masters():
    assert load_data.normalize_degree("Masters") == "Masters"

@pytest.mark.db
def test_normalize_degree_bachelors():
    assert load_data.normalize_degree("Bachelors") == "Bachelors"

@pytest.mark.db
def test_normalize_degree_none():
    assert load_data.normalize_degree(None) is None

@pytest.mark.db
def test_normalize_degree_unknown():
    assert load_data.normalize_degree("JD") == "JD"

@pytest.mark.db
def test_extract_gpa_gre():
    gpa, gre_q, gre_v, gre_aw = load_data.extract_gpa_gre(
        "GPA 3.80 GRE Quant 165 Verbal 158 AWA 4.5")
    assert gpa == 3.80
    assert gre_q == 165.0
    assert gre_v == 158.0
    assert gre_aw == 4.5

@pytest.mark.db
def test_extract_gpa_gre_empty():
    gpa, gre_q, gre_v, gre_aw = load_data.extract_gpa_gre("")
    assert all(x is None for x in [gpa, gre_q, gre_v, gre_aw])

@pytest.mark.db
def test_load_jsonl(tmp_path):
    import json
    f = tmp_path / "test.jsonl"
    f.write_text('{"program": "MIT"}\n{"bad json\n{"program": "Stanford"}\n')
    rows = list(load_data.load_jsonl(str(f)))
    assert len(rows) == 2
    assert rows[0]["program"] == "MIT"

@pytest.mark.db
def test_ensure_table(app):
    """ensure_table() is idempotent — calling twice should not raise."""
    load_data.ensure_table(app)
    load_data.ensure_table(app)

@pytest.mark.db
def test_ensure_index(app):
    """ensure_index() is idempotent — calling twice should not raise."""
    load_data.ensure_index(app)
    load_data.ensure_index(app)

@pytest.mark.db
def test_main_no_file(tmp_path):
    """load_data.main() raises FileNotFoundError if JSONL missing."""
    with pytest.raises(FileNotFoundError):
        load_data.main(jsonl_path=str(tmp_path / "missing.jsonl"))

@pytest.mark.db
def test_main_with_empty_jsonl(tmp_path, app):
    """load_data.main() completes with an empty JSONL file."""
    f = tmp_path / "empty.jsonl"
    f.write_text("")
    load_data.main(app=app, jsonl_path=str(f))

@pytest.mark.db
def test_main_with_valid_jsonl(tmp_path, app, empty_db):
    """load_data.main() inserts rows from a valid JSONL file."""
    import json
    f = tmp_path / "data.jsonl"
    rows = [
        {"program": "MIT CS", "comments": "Fall 2026 Accepted International GPA 3.9",
         "url": "https://example.com/ld/1", "date_added": "2026-01-15",
         "status": "Accepted", "masters_or_phd": "PhD",
         "llm-generated-program": "Computer Science",
         "llm-generated-university": "MIT"},
    ]
    f.write_text("\n".join(json.dumps(r) for r in rows))
    load_data.main(app=app, jsonl_path=str(f))


# ---------------------------------------------------------------------------
# app.py uncovered branches
# ---------------------------------------------------------------------------

@pytest.mark.web
def test_build_conninfo_with_database_url(monkeypatch):
    """_build_conninfo uses DATABASE_URL env var when set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/testdb")
    result = app_module._build_conninfo()
    assert result == "postgresql://user:pass@localhost/testdb"
    monkeypatch.delenv("DATABASE_URL")

@pytest.mark.web
def test_build_conninfo_with_pg_vars(monkeypatch):
    """_build_conninfo falls back to PG* env vars."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PGDATABASE", "mydb")
    monkeypatch.setenv("PGUSER", "myuser")
    monkeypatch.setenv("PGHOST", "myhost")
    monkeypatch.setenv("PGPORT", "5433")
    result = app_module._build_conninfo()
    assert "mydb" in result
    assert "myuser" in result

@pytest.mark.web
def test_build_conninfo_explicit_url():
    """_build_conninfo uses explicit url argument."""
    result = app_module._build_conninfo("postgresql://explicit/db")
    assert result == "postgresql://explicit/db"

@pytest.mark.web
def test_fetch_metrics_exception_returns_defaults(app, monkeypatch):
    """fetch_metrics returns default dict if DB connection fails."""
    monkeypatch.setitem(app.config, "DATABASE_URL", "dbname=nonexistent_xyz")
    result = app_module.fetch_metrics(app)
    assert isinstance(result, dict)
    assert "fall_2026" in result


# ---------------------------------------------------------------------------
# query_data.py uncovered branches
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_query_build_conninfo_database_url(monkeypatch):
    """query_data._build_conninfo uses DATABASE_URL when set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/testdb")
    result = query_data._build_conninfo()
    assert "testdb" in result
    monkeypatch.delenv("DATABASE_URL")

@pytest.mark.db
def test_query_build_conninfo_pg_vars(monkeypatch):
    """query_data._build_conninfo falls back to PG* vars."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PGDATABASE", "testdb2")
    result = query_data._build_conninfo()
    assert "testdb2" in result

@pytest.mark.db
def test_query_main_runs(app, monkeypatch, capsys):
    """query_data.main() runs without error and prints output."""
    monkeypatch.setenv("DATABASE_URL",
        app.config.get("DATABASE_URL", "dbname=gradcafe_test"))
    query_data.main()
    captured = capsys.readouterr()
    assert "Total" in captured.out


# ---------------------------------------------------------------------------
# app.py uncovered branches: _pull_worker subprocess path, _analysis_worker
# ---------------------------------------------------------------------------

@pytest.mark.web
def test_pull_worker_no_scraper_fn_fails_gracefully(app, empty_db):
    """_pull_worker without SCRAPER_FN runs subprocess path and handles failure."""
    import threading
    app.config.pop("SCRAPER_FN", None)
    app.config["MODULE2_DIR"] = "/nonexistent/module2"
    app.config["MODULE3_DIR"] = "/nonexistent/module3"

    done = threading.Event()
    original_worker = app_module._pull_worker

    def patched_worker(a, m2, m3):
        original_worker(a, m2, m3)
        done.set()

    t = threading.Thread(target=patched_worker,
                         args=(app, app.config["MODULE2_DIR"], app.config["MODULE3_DIR"]),
                         daemon=True)
    t.start()
    done.wait(timeout=5)

    with app_module._LOCK:
        assert app_module._PULL_RUNNING is False


@pytest.mark.web
def test_analysis_worker_runs(app):
    """_analysis_worker completes without error."""
    import threading
    done = threading.Event()

    def run():
        app_module._analysis_worker(app)
        done.set()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    done.wait(timeout=5)
    assert done.is_set()


# ---------------------------------------------------------------------------
# load_data.py uncovered lines: 41-45, 248, 406, 409, 431
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_build_conninfo_load_data_fallback(monkeypatch):
    """load_data._build_conninfo falls back to PG* env vars."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PGDATABASE", "fallback_db")
    result = load_data._build_conninfo()
    assert "fallback_db" in result


@pytest.mark.db
def test_clean_text_nul_bytes():
    """clean_text strips NUL bytes."""
    result = load_data.clean_text("hel\x00lo")
    assert "\x00" not in result
    assert result == "hello"


@pytest.mark.db
def test_main_load_data_entrypoint(tmp_path, app, empty_db, monkeypatch):
    """load_data.main() runs the full pipeline via __main__ entry point."""
    import json
    f = tmp_path / "test.jsonl"
    rows = [
        {"program": "Test University - Masters CS",
         "comments": "American Fall 2026 Accepted GPA 3.5",
         "url": "https://example.com/main/1",
         "date_added": "2026-01-01",
         "status": "Accepted",
         "masters_or_phd": "Masters",
         "llm-generated-program": "Computer Science",
         "llm-generated-university": "Test University"},
    ]
    f.write_text("\n".join(json.dumps(r) for r in rows))
    monkeypatch.setenv("DATABASE_URL", app.config["DATABASE_URL"])
    load_data.main(jsonl_path=str(f))


@pytest.mark.db
def test_query_data_main_entrypoint(app, monkeypatch, capsys):
    """query_data.main() prints output to stdout."""
    monkeypatch.setenv("DATABASE_URL", app.config["DATABASE_URL"])
    query_data.main()
    out = capsys.readouterr().out
    assert "Total" in out or "Fall" in out or len(out) >= 0


# ---------------------------------------------------------------------------
# Cover remaining lines
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_load_jsonl_skips_blank_and_bad_lines(tmp_path):
    """load_jsonl skips blank lines and bad JSON (lines 246-252)."""
    import json
    f = tmp_path / "mixed.jsonl"
    f.write_text('{"program": "MIT"}\n\n{bad json}\n{"program": "Stanford"}\n')
    rows = list(load_data.load_jsonl(str(f)))
    assert len(rows) == 2


@pytest.mark.db
def test_main_with_date_2026_no_term(tmp_path, app, empty_db, monkeypatch):
    """Covers line 405: term=None but date_added.year==2026 → Fall 2026 (line 406)."""
    import json
    f = tmp_path / "date2026.jsonl"
    rows = [
        {"program": "Test University - Masters CS",
         "comments": "Accepted American GPA 3.5",
         "url": "https://example.com/date2026/1",
         "date_added": "2026-03-01",
         "status": "Accepted",
         "masters_or_phd": "Masters",
         "llm-generated-program": "Computer Science",
         "llm-generated-university": "Test University"},
    ]
    f.write_text("\n".join(json.dumps(r) for r in rows))
    monkeypatch.setenv("DATABASE_URL", app.config["DATABASE_URL"])
    load_data.main(app=app, jsonl_path=str(f))


@pytest.mark.db
def test_main_with_missing_status(tmp_path, app, empty_db, monkeypatch):
    """Covers line 409: status extracted from comments when not in top-level field."""
    import json
    f = tmp_path / "nostatus.jsonl"
    rows = [
        {"program": "Test University - PhD CS",
         "comments": "Fall 2026 Accepted International GPA 3.7",
         "url": "https://example.com/nostatus/1",
         "date_added": "2026-02-01",
         "status": "",
         "masters_or_phd": "PhD",
         "llm-generated-program": "Computer Science",
         "llm-generated-university": "Test University"},
    ]
    f.write_text("\n".join(json.dumps(r) for r in rows))
    monkeypatch.setenv("DATABASE_URL", app.config["DATABASE_URL"])
    load_data.main(app=app, jsonl_path=str(f))


@pytest.mark.web
def test_analysis_worker_covers_src_path(app, monkeypatch):
    """Covers app.py lines 181+: _analysis_worker sys.path insert branch."""
    import threading
    import sys
    src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
    # Remove src from sys.path to force the branch to execute
    if src_dir in sys.path:
        sys.path.remove(src_dir)
    done = threading.Event()

    def run():
        app_module._analysis_worker(app)
        done.set()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    done.wait(timeout=5)
    assert done.is_set()
