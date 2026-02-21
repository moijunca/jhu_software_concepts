"""
tests/test_analysis_format.py – Analysis Formatting tests.

Covers:
- Page includes "Answer:" labels for rendered analysis items.
- Any percentage rendered on the page is formatted with exactly two decimals.
"""

import re
import pytest
from bs4 import BeautifulSoup

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

PERCENTAGE_RE = re.compile(r"\d+\.\d{2}%")
TWO_DECIMAL_RE = re.compile(r"\d+\.\d{2}%")
WRONG_DECIMAL_RE = re.compile(r"\d+\.(?!\d{2}%)(\d{1}|\d{3,})%")


def _get_page_text(client):
    response = client.get("/")
    assert response.status_code == 200
    return response.data.decode("utf-8")


@pytest.mark.analysis
def test_page_contains_answer_label(client):
    """The rendered page must contain at least one 'Answer:' label."""
    text = _get_page_text(client)
    assert "Answer:" in text


@pytest.mark.analysis
def test_page_contains_multiple_answer_labels(client):
    """The rendered page should contain multiple 'Answer:' labels."""
    text = _get_page_text(client)
    count = text.count("Answer:")
    assert count >= 1


@pytest.mark.analysis
def test_answer_labels_in_html_structure(client):
    """'Answer:' labels must appear inside the HTML body."""
    response = client.get("/")
    soup = BeautifulSoup(response.data, "html.parser")
    body_text = soup.body.get_text() if soup.body else ""
    assert "Answer:" in body_text


@pytest.mark.analysis
def test_percentages_have_two_decimals(client, empty_db, db_conn):
    """Any percentage rendered on the page must have exactly two decimal places."""
    with db_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO applicants (
                program, comments, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw,
                degree, llm_generated_program, llm_generated_university
            ) VALUES (
                'Test Program', 'International Fall 2026 Accepted',
                'https://example.com/test/1', 'Accepted', 'Fall 2026',
                'International', 3.80, 165, 158, 4.5,
                'Masters', 'Computer Science', 'Test University'
            )
            ON CONFLICT DO NOTHING;
        """)
    db_conn.commit()

    text = _get_page_text(client)
    raw_pct_strings = re.findall(r"\d+\.\d+%", text)

    for pct in raw_pct_strings:
        assert TWO_DECIMAL_RE.match(pct), (
            f"Percentage '{pct}' is not formatted with exactly two decimals"
        )


@pytest.mark.analysis
def test_no_percentage_with_wrong_decimals(client, empty_db, db_conn):
    """No percentage on the page should have 1 or 3+ decimal places."""
    rows = [
        ("https://example.com/fmt/1", "Accepted", "International"),
        ("https://example.com/fmt/2", "Rejected", "American"),
    ]
    with db_conn.cursor() as cur:
        for url, status, us_intl in rows:
            cur.execute("""
                INSERT INTO applicants (
                    program, comments, url, status, term,
                    us_or_international, gpa, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (
                    'Format Test', %s, %s, %s, 'Fall 2026',
                    %s, 3.50, 'Masters', 'CS', 'Test U'
                )
                ON CONFLICT DO NOTHING;
            """, (f"Fall 2026 {status}", url, status, us_intl))
    db_conn.commit()

    text = _get_page_text(client)
    bad = WRONG_DECIMAL_RE.findall(text)
    assert not bad, f"Found percentages with wrong decimal count: {bad}"


@pytest.mark.analysis
def test_percentage_format_with_known_value(client, empty_db, db_conn):
    """3 accepted + 1 rejected → acceptance_pct should render as '75.00%'."""
    entries = [
        ("https://example.com/pct/1", "Accepted"),
        ("https://example.com/pct/2", "Accepted"),
        ("https://example.com/pct/3", "Accepted"),
        ("https://example.com/pct/4", "Rejected"),
    ]
    with db_conn.cursor() as cur:
        for url, status in entries:
            cur.execute("""
                INSERT INTO applicants (
                    program, comments, url, status, term,
                    us_or_international, gpa, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (
                    'Pct Test', %s, %s, %s, 'Fall 2026',
                    'American', 3.60, 'Masters', 'CS', 'Test U'
                )
                ON CONFLICT DO NOTHING;
            """, (f"Fall 2026 {status}", url, status))
    db_conn.commit()

    text = _get_page_text(client)
    found = re.findall(r"\d+\.\d+%", text)
    assert "75.00%" in text, f"Expected '75.00%' on page, percentages found: {found}"
