"""
tests/test_db_insert.py – Database writes tests.

Covers:
- Test insert on pull:
    * Before: target table empty.
    * After POST /pull-data new rows exist with required (non-null) fields.
- Test idempotency / constraints:
    * Duplicate rows do not create duplicates in the database.
- Test simple query function:
    * fetch_metrics() returns a dict with all expected keys.
"""

import pytest
import threading

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import app as app_module
from query_data import fetch_metrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_KEYS = [
    "total", "fall_2026", "pct_intl", "avg_gpa", "avg_gre",
    "avg_gre_v", "avg_gre_aw", "avg_gpa_american_fall",
    "acceptance_pct", "avg_gpa_accepted", "q7_jhu_ms_cs",
    "q8_raw", "q9_llm", "q10a_rows", "q10b_rows",
    "term_dist", "decision_dist",
]

# Required non-null fields per the Module 3 schema
REQUIRED_COLUMNS = [
    "program", "url",
]


def _reset_busy_state():
    with app_module._LOCK:
        app_module._PULL_RUNNING = False
        app_module._PULL_MESSAGE = "No load has been run yet."


# ---------------------------------------------------------------------------
# Test insert on pull
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_table_is_empty_before_pull(db_conn, empty_db):
    """Target table should be empty before any pull (after truncation)."""
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count = cur.fetchone()[0]
    assert count == 0, f"Expected empty table before pull, got {count} rows"


@pytest.mark.db
def test_new_rows_exist_after_pull(client, empty_db, sample_rows, db_conn):
    """After POST /pull-data, new rows must exist in the applicants table."""
    _reset_busy_state()

    done_event = threading.Event()

    def fake_scraper():
        done_event.set()
        return sample_rows

    client.application.config["SCRAPER_FN"] = fake_scraper

    response = client.post("/pull-data")
    assert response.status_code == 200

    done_event.wait(timeout=5)
    import time; time.sleep(0.3)

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count = cur.fetchone()[0]

    assert count > 0, "No rows found in applicants table after pull"
    assert count == len(sample_rows), (
        f"Expected {len(sample_rows)} rows, got {count}"
    )

    _reset_busy_state()


@pytest.mark.db
def test_inserted_rows_have_required_fields(client, empty_db, sample_rows, db_conn):
    """
    After pull, inserted rows must have required non-null fields:
    program and url.
    """
    _reset_busy_state()

    done_event = threading.Event()

    def fake_scraper():
        done_event.set()
        return sample_rows

    client.application.config["SCRAPER_FN"] = fake_scraper
    client.post("/pull-data")

    done_event.wait(timeout=5)
    import time; time.sleep(0.3)

    with db_conn.cursor() as cur:
        for col in REQUIRED_COLUMNS:
            cur.execute(
                f"SELECT COUNT(*) FROM applicants WHERE {col} IS NULL;"
            )
            null_count = cur.fetchone()[0]
            assert null_count == 0, (
                f"Column '{col}' has {null_count} NULL values after insert"
            )

    _reset_busy_state()


@pytest.mark.db
def test_inserted_rows_match_schema(client, empty_db, sample_rows, db_conn):
    """Inserted rows must match the required Module 3 schema columns."""
    _reset_busy_state()

    done_event = threading.Event()

    def fake_scraper():
        done_event.set()
        return sample_rows

    client.application.config["SCRAPER_FN"] = fake_scraper
    client.post("/pull-data")

    done_event.wait(timeout=5)
    import time; time.sleep(0.3)

    expected_columns = {
        "p_id", "program", "comments", "date_added", "url",
        "status", "term", "us_or_international",
        "gpa", "gre", "gre_v", "gre_aw",
        "degree", "llm_generated_program", "llm_generated_university",
    }

    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'applicants'
              AND table_schema = 'public';
        """)
        actual_columns = {row[0] for row in cur.fetchall()}

    assert expected_columns.issubset(actual_columns), (
        f"Missing columns: {expected_columns - actual_columns}"
    )

    _reset_busy_state()


# ---------------------------------------------------------------------------
# Idempotency / constraints tests
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_duplicate_pull_does_not_duplicate_rows(client, empty_db, sample_rows, db_conn):
    """
    Running POST /pull-data twice with the same data must not duplicate rows.

    The unique index on (url, program, comments) enforces this.
    """
    _reset_busy_state()

    for _ in range(2):
        done_event = threading.Event()

        def fake_scraper():
            done_event.set()
            return sample_rows

        client.application.config["SCRAPER_FN"] = fake_scraper
        client.post("/pull-data")
        done_event.wait(timeout=5)
        import time; time.sleep(0.3)
        _reset_busy_state()

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count = cur.fetchone()[0]

    assert count == len(sample_rows), (
        f"Expected {len(sample_rows)} rows after two identical pulls, "
        f"got {count} (possible duplicates)"
    )


@pytest.mark.db
def test_unique_index_exists(db_conn):
    """The applicants_sig_unique index must exist on the applicants table."""
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'applicants'
              AND schemaname = 'public';
        """)
        indexes = {row[0] for row in cur.fetchall()}

    assert "applicants_sig_unique" in indexes, (
        f"Unique index 'applicants_sig_unique' not found. "
        f"Existing indexes: {indexes}"
    )


@pytest.mark.db
def test_direct_duplicate_insert_is_ignored(db_conn, empty_db):
    """
    Directly inserting the same row twice must result in only one row
    (ON CONFLICT DO NOTHING behaviour).
    """
    insert_sql = """
        INSERT INTO applicants (
            program, comments, url, status, term,
            us_or_international, gpa, degree,
            llm_generated_program, llm_generated_university
        ) VALUES (
            'Dup Test', 'Fall 2026 Accepted',
            'https://example.com/dup/1', 'Accepted', 'Fall 2026',
            'American', 3.70, 'Masters', 'CS', 'Test U'
        )
        ON CONFLICT DO NOTHING;
    """
    with db_conn.cursor() as cur:
        cur.execute(insert_sql)
        cur.execute(insert_sql)  # second insert — should be ignored
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count = cur.fetchone()[0]

    assert count == 1, (
        f"Expected 1 row after duplicate insert, got {count}"
    )


# ---------------------------------------------------------------------------
# Query function tests
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_fetch_metrics_returns_dict(app):
    """fetch_metrics() must return a dictionary."""
    result = fetch_metrics(app)
    assert isinstance(result, dict), (
        f"fetch_metrics() should return dict, got {type(result)}"
    )


@pytest.mark.db
def test_fetch_metrics_has_required_keys(app):
    """fetch_metrics() must return all expected keys."""
    result = fetch_metrics(app)
    for key in REQUIRED_KEYS:
        assert key in result, (
            f"Key '{key}' missing from fetch_metrics() result"
        )


@pytest.mark.db
def test_fetch_metrics_returns_correct_counts(app, empty_db, db_conn):
    """
    After inserting known rows, fetch_metrics() must return correct counts.
    """
    with db_conn.cursor() as cur:
        for i in range(3):
            cur.execute("""
                INSERT INTO applicants (
                    program, comments, url, status, term,
                    us_or_international, gpa, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (
                    'Query Test', %s, %s, 'Accepted', 'Fall 2026',
                    'American', 3.70, 'Masters', 'CS', 'Test U'
                )
                ON CONFLICT DO NOTHING;
            """, (f"Fall 2026 Accepted {i}", f"https://example.com/q/{i}"))
    db_conn.commit()

    result = fetch_metrics(app)

    assert result["total"] >= 3, (
        f"Expected total >= 3, got {result['total']}"
    )
    assert result["fall_2026"] >= 3, (
        f"Expected fall_2026 >= 3, got {result['fall_2026']}"
    )
