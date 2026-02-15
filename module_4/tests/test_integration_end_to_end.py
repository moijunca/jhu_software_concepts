"""
tests/test_integration_end_to_end.py – Integration Tests.

Covers:
- End-to-end (pull → update → render):
    * Inject a fake scraper that returns multiple records.
    * POST /pull-data succeeds and rows are in DB.
    * POST /update-analysis succeeds (when not busy).
    * GET / shows updated analysis with correctly formatted values.
- Multiple pulls:
    * Running POST /pull-data twice with overlapping data remains
      consistent with the uniqueness policy.
"""

import re
import threading
import time
import pytest
from bs4 import BeautifulSoup

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import app as app_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PERCENTAGE_RE = re.compile(r"\d+\.\d{2}%")


def _reset_busy_state():
    with app_module._LOCK:
        app_module._PULL_RUNNING = False
        app_module._PULL_MESSAGE = "No load has been run yet."


def _do_pull(client, rows):
    """Trigger POST /pull-data with a fake scraper and wait for completion."""
    _reset_busy_state()
    done_event = threading.Event()

    def fake_scraper():
        done_event.set()
        return rows

    client.application.config["SCRAPER_FN"] = fake_scraper
    response = client.post("/pull-data")
    done_event.wait(timeout=5)
    time.sleep(0.3)  # let _pull_worker finish DB writes
    _reset_busy_state()
    return response


# ---------------------------------------------------------------------------
# End-to-end: pull → update → render
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_e2e_pull_succeeds(client, empty_db, sample_rows):
    """POST /pull-data with fake scraper must return 200."""
    response = _do_pull(client, sample_rows)
    assert response.status_code == 200
    data = response.get_json()
    assert data.get("ok") is True


@pytest.mark.integration
def test_e2e_rows_in_db_after_pull(client, empty_db, sample_rows, db_conn):
    """After pull, rows from the fake scraper must be present in the DB."""
    _do_pull(client, sample_rows)

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count = cur.fetchone()[0]

    assert count == len(sample_rows), (
        f"Expected {len(sample_rows)} rows after pull, got {count}"
    )


@pytest.mark.integration
def test_e2e_update_analysis_succeeds_after_pull(client, empty_db, sample_rows):
    """POST /update-analysis must return 200 after a completed pull."""
    _do_pull(client, sample_rows)

    response = client.post("/update-analysis")
    assert response.status_code == 200
    data = response.get_json()
    assert data.get("ok") is True

    _reset_busy_state()


@pytest.mark.integration
def test_e2e_render_shows_analysis_after_pull(client, empty_db, sample_rows):
    """
    After pull → update, GET / must show updated analysis with
    correctly formatted values (Answer: labels present).
    """
    _do_pull(client, sample_rows)

    # Allow update-analysis worker to finish
    client.post("/update-analysis")
    time.sleep(0.3)
    _reset_busy_state()

    response = client.get("/")
    assert response.status_code == 200
    assert b"Answer:" in response.data, (
        "No 'Answer:' label found after pull → update → render"
    )


@pytest.mark.integration
def test_e2e_percentages_formatted_correctly_after_pull(client, empty_db, sample_rows):
    """
    After pull, any percentage on the rendered page must have exactly
    two decimal places.
    """
    _do_pull(client, sample_rows)

    response = client.get("/")
    text = response.data.decode("utf-8")

    all_pcts = re.findall(r"\d+\.\d+%", text)
    for pct in all_pcts:
        assert PERCENTAGE_RE.match(pct), (
            f"Percentage '{pct}' is not formatted with exactly two decimals"
        )


@pytest.mark.integration
def test_e2e_fall_2026_count_correct_after_pull(client, empty_db, sample_rows, db_conn):
    """
    After pull, fetch_metrics() fall_2026 count must match the number of
    sample rows with term='Fall 2026'.
    """
    from query_data import fetch_metrics

    _do_pull(client, sample_rows)

    expected = sum(1 for r in sample_rows if r.get("term") == "Fall 2026")
    result = fetch_metrics(client.application)

    assert result["fall_2026"] == expected, (
        f"Expected fall_2026={expected}, got {result['fall_2026']}"
    )


@pytest.mark.integration
def test_e2e_update_analysis_blocked_during_pull(client, empty_db):
    """
    POST /update-analysis must return 409 if called while pull is running.
    """
    with app_module._LOCK:
        app_module._PULL_RUNNING = True

    try:
        response = client.post("/update-analysis")
        assert response.status_code == 409
        data = response.get_json()
        assert data.get("busy") is True
    finally:
        _reset_busy_state()


# ---------------------------------------------------------------------------
# Multiple pulls — uniqueness consistency
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_multiple_pulls_no_duplicates(client, empty_db, sample_rows, db_conn):
    """
    Running POST /pull-data twice with the same (overlapping) data must
    not duplicate rows — uniqueness policy must hold across multiple pulls.
    """
    _do_pull(client, sample_rows)
    count_after_first = None
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count_after_first = cur.fetchone()[0]

    # Second pull with same data
    _do_pull(client, sample_rows)
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count_after_second = cur.fetchone()[0]

    assert count_after_first == count_after_second, (
        f"Row count changed after second identical pull: "
        f"{count_after_first} → {count_after_second} (duplicates inserted)"
    )


@pytest.mark.integration
def test_multiple_pulls_with_new_rows_increases_count(client, empty_db, sample_rows, db_conn):
    """
    Running POST /pull-data with additional new rows should increase the
    count correctly (only new unique rows added).
    """
    _do_pull(client, sample_rows)
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count_after_first = cur.fetchone()[0]

    # New row with a different URL (genuinely new)
    new_rows = sample_rows + [{
        "program": "Carnegie Mellon University - PhD Computer Science",
        "comments": "GPA 3.99 International Fall 2026 Accepted",
        "date_added": None,
        "url": "https://example.com/applicant/999",
        "status": "Accepted",
        "term": "Fall 2026",
        "us_or_international": "International",
        "gpa": 3.99,
        "gre": 169.0,
        "gre_v": 160.0,
        "gre_aw": 5.0,
        "degree": "PhD",
        "llm_generated_program": "Computer Science",
        "llm_generated_university": "Carnegie Mellon University",
    }]

    _do_pull(client, new_rows)
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count_after_second = cur.fetchone()[0]

    assert count_after_second == count_after_first + 1, (
        f"Expected {count_after_first + 1} rows after second pull with 1 new row, "
        f"got {count_after_second}"
    )
