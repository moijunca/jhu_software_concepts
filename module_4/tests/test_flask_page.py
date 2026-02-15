"""
tests/test_flask_page.py – Flask App & Page Rendering tests.

Covers:
- App factory creates a valid Flask app with required routes.
- GET /analysis (index) returns 200 with required page components:
    * Status 200
    * Both "Pull Data" and "Update Analysis" buttons present
    * Page text includes "Analysis" and at least one "Answer:" label
"""

import pytest
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# App factory / config tests
# ---------------------------------------------------------------------------

@pytest.mark.web
def test_create_app_returns_flask_app(app):
    """create_app() should return a testable Flask application."""
    from flask import Flask
    assert isinstance(app, Flask)


@pytest.mark.web
def test_app_is_in_testing_mode(app):
    """App created with TESTING=True should have testing mode enabled."""
    assert app.config["TESTING"] is True


@pytest.mark.web
def test_app_has_index_route(app):
    """The app must expose a GET / route."""
    rules = [rule.rule for rule in app.url_map.iter_rules()]
    assert "/" in rules


@pytest.mark.web
def test_app_has_pull_data_route(app):
    """The app must expose a POST /pull-data route."""
    rules = [rule.rule for rule in app.url_map.iter_rules()]
    assert "/pull-data" in rules


@pytest.mark.web
def test_app_has_update_analysis_route(app):
    """The app must expose a POST /update-analysis route."""
    rules = [rule.rule for rule in app.url_map.iter_rules()]
    assert "/update-analysis" in rules


# ---------------------------------------------------------------------------
# GET / (index / analysis page) tests
# ---------------------------------------------------------------------------

@pytest.mark.web
def test_index_returns_200(client):
    """GET / should return HTTP 200."""
    response = client.get("/")
    assert response.status_code == 200


@pytest.mark.web
def test_index_contains_analysis_text(client):
    """Page title/text must include the word 'Analysis'."""
    response = client.get("/")
    assert b"Analysis" in response.data


@pytest.mark.web
def test_index_contains_pull_data_button(client):
    """Page must contain a 'Pull Data' button."""
    response = client.get("/")
    soup = BeautifulSoup(response.data, "html.parser")
    buttons = soup.find_all("button")
    button_texts = [b.get_text(strip=True) for b in buttons]
    assert any("Pull Data" in text for text in button_texts), (
        f"'Pull Data' button not found. Found buttons: {button_texts}"
    )


@pytest.mark.web
def test_index_contains_update_analysis_button(client):
    """Page must contain an 'Update Analysis' button."""
    response = client.get("/")
    soup = BeautifulSoup(response.data, "html.parser")
    buttons = soup.find_all("button")
    button_texts = [b.get_text(strip=True) for b in buttons]
    assert any("Update Analysis" in text for text in button_texts), (
        f"'Update Analysis' button not found. Found buttons: {button_texts}"
    )


@pytest.mark.web
def test_index_contains_pull_data_testid(client):
    """Pull Data button must have data-testid='pull-data-btn'."""
    response = client.get("/")
    soup = BeautifulSoup(response.data, "html.parser")
    btn = soup.find(attrs={"data-testid": "pull-data-btn"})
    assert btn is not None, "No element with data-testid='pull-data-btn' found"


@pytest.mark.web
def test_index_contains_update_analysis_testid(client):
    """Update Analysis button must have data-testid='update-analysis-btn'."""
    response = client.get("/")
    soup = BeautifulSoup(response.data, "html.parser")
    btn = soup.find(attrs={"data-testid": "update-analysis-btn"})
    assert btn is not None, "No element with data-testid='update-analysis-btn' found"


@pytest.mark.web
def test_index_contains_answer_label(client):
    """Page must contain at least one 'Answer:' label."""
    response = client.get("/")
    assert b"Answer:" in response.data, (
        "No 'Answer:' label found on the analysis page"
    )


@pytest.mark.web
def test_index_content_type_is_html(client):
    """GET / should return HTML content."""
    response = client.get("/")
    assert "text/html" in response.content_type
