"""
tests/test_buttons.py – Buttons & Busy-State Behavior tests.

Covers:
- POST /pull-data returns 200 when not busy and triggers the loader
  with rows from the (faked/mocked) scraper.
- POST /update-analysis returns 200 when not busy.
- Busy gating:
    * When a pull is "in progress", POST /update-analysis returns 409.
    * When busy, POST /pull-data returns 409.
"""

import pytest
import threading

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import app as app_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_busy_state():
    """Force the module-level busy flag back to False between tests."""
    with app_module._LOCK:
        app_module._PULL_RUNNING = False
        app_module._PULL_MESSAGE = "No load has been run yet."


# ---------------------------------------------------------------------------
# POST /pull-data tests
# ---------------------------------------------------------------------------

@pytest.mark.buttons
def test_pull_data_returns_200_when_not_busy(client, empty_db):
    """POST /pull-data should return 200 with {"ok": true} when not busy."""
    _reset_busy_state()

    # Inject a fake scraper that returns an empty list instantly
    client.application.config["SCRAPER_FN"] = lambda: []

    response = client.post("/pull-data")
    assert response.status_code == 200
    data = response.get_json()
    assert data is not None
    assert data.get("ok") is True

    _reset_busy_state()


@pytest.mark.buttons
def test_pull_data_triggers_loader_with_fake_rows(client, empty_db, sample_rows, db_conn):
    """
    POST /pull-data with a fake scraper should insert the fake rows into DB.

    We use a threading.Event to wait for the background worker to finish
    before asserting on the DB state.
    """
    _reset_busy_state()

    done_event = threading.Event()
    inserted_rows = []

    def fake_scraper():
        inserted_rows.extend(sample_rows)
        return sample_rows

    def fake_scraper_with_signal():
        rows = fake_scraper()
        done_event.set()
        return rows

    client.application.config["SCRAPER_FN"] = fake_scraper_with_signal

    response = client.post("/pull-data")
    assert response.status_code == 200

    # Wait for the background thread to finish (max 5 seconds)
    done_event.wait(timeout=5)
    # Give _pull_worker a moment to finish the DB insert after scraper returns
    import time; time.sleep(0.3)

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        count = cur.fetchone()[0]

    assert count == len(sample_rows), (
        f"Expected {len(sample_rows)} rows in DB after pull, got {count}"
    )

    _reset_busy_state()


# ---------------------------------------------------------------------------
# POST /update-analysis tests
# ---------------------------------------------------------------------------

@pytest.mark.buttons
def test_update_analysis_returns_200_when_not_busy(client):
    """POST /update-analysis should return 200 with {"ok": true} when not busy."""
    _reset_busy_state()

    response = client.post("/update-analysis")
    assert response.status_code == 200
    data = response.get_json()
    assert data is not None
    assert data.get("ok") is True

    _reset_busy_state()


# ---------------------------------------------------------------------------
# Busy-state gating tests
# ---------------------------------------------------------------------------

@pytest.mark.buttons
def test_update_analysis_returns_409_when_pull_running(client):
    """
    When a pull is in progress, POST /update-analysis must return 409
    with {"busy": true} and perform no update.
    """
    # Manually set the busy flag to simulate an in-progress pull
    with app_module._LOCK:
        app_module._PULL_RUNNING = True

    try:
        response = client.post("/update-analysis")
        assert response.status_code == 409
        data = response.get_json()
        assert data is not None
        assert data.get("busy") is True
    finally:
        _reset_busy_state()


@pytest.mark.buttons
def test_pull_data_returns_409_when_already_running(client):
    """
    When a pull is already in progress, POST /pull-data must return 409
    with {"busy": true}.
    """
    # Manually set the busy flag to simulate an in-progress pull
    with app_module._LOCK:
        app_module._PULL_RUNNING = True

    try:
        response = client.post("/pull-data")
        assert response.status_code == 409
        data = response.get_json()
        assert data is not None
        assert data.get("busy") is True
    finally:
        _reset_busy_state()


@pytest.mark.buttons
def test_busy_flag_resets_after_pull_completes(client, empty_db):
    """
    After a pull completes, _PULL_RUNNING should be False again.
    """
    _reset_busy_state()

    done_event = threading.Event()

    def fake_scraper():
        done_event.set()
        return []

    client.application.config["SCRAPER_FN"] = fake_scraper

    client.post("/pull-data")

    # Wait for the worker to finish
    done_event.wait(timeout=5)
    import time; time.sleep(0.2)

    with app_module._LOCK:
        still_running = app_module._PULL_RUNNING

    assert still_running is False, "Busy flag should be False after pull completes"

    _reset_busy_state()
