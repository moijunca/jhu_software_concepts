"""
conftest.py – Shared pytest fixtures for the Module 4 test suite.

All fixtures live here so every test file can use them without imports.

Fixture summary
---------------
app         : Flask app wired to the isolated test DB (function-scoped).
client      : Flask test client for HTTP assertions (function-scoped).
db_conn     : Raw psycopg3 connection to the test DB (function-scoped).
empty_db    : Truncates the applicants table before the test (function-scoped).
sample_rows : A list of minimal applicant dicts for use as fake scraper data.
"""

import os
import pytest
import psycopg  # psycopg3

# ---------------------------------------------------------------------------
# Resolve the src/ directory so imports work regardless of where pytest runs
# ---------------------------------------------------------------------------
import sys
SRC_DIR = os.path.join(os.path.dirname(__file__), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from app import create_app
from load_data import ensure_table


# ---------------------------------------------------------------------------
# Test database URL
# ---------------------------------------------------------------------------
# Uses the same Postgres instance but a dedicated test database so we never
# touch the production "gradcafe" DB.  The CI workflow creates this DB;
# locally you can run:  createdb gradcafe_test
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "dbname=gradcafe_test user={user} host=localhost port=5432".format(
        user=os.getenv("PGUSER", os.getenv("USER", "postgres"))
    ),
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def app():
    """
    Create a Flask app instance pointed at the isolated test database.

    The applicants table is created (if missing) before yielding so every
    test starts with a known schema.
    """
    flask_app = create_app(
        {
            "TESTING": True,
            "DATABASE_URL": TEST_DATABASE_URL,
        }
    )

    # Ensure the schema exists in the test DB
    ensure_table(flask_app)

    yield flask_app


@pytest.fixture()
def client(app):
    """Flask test client — use this for all HTTP request assertions."""
    return app.test_client()


@pytest.fixture()
def db_conn(app):
    """
    Raw psycopg3 connection to the test database.

    Useful for direct INSERT / SELECT assertions that don't go through Flask.
    The connection is closed automatically after each test.
    """
    conn = psycopg.connect(TEST_DATABASE_URL)
    yield conn
    conn.close()


@pytest.fixture()
def empty_db(db_conn):
    """
    Truncate the applicants table before (and after) each test.

    Use this fixture in any test that needs to start with a clean slate,
    e.g. database insert tests and integration tests.
    """
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY CASCADE;")
    db_conn.commit()
    yield
    # Cleanup after test too
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY CASCADE;")
    db_conn.commit()


@pytest.fixture()
def sample_rows():
    """
    A list of minimal applicant dicts that satisfy the required schema.

    Use these as the return value of a fake scraper function injected via
    ``app.config["SCRAPER_FN"]``.  Each dict matches the column set that
    ``_load_rows()`` in app.py expects.
    """
    return [
        {
            "program": "Johns Hopkins University - Masters Computer Science",
            "comments": "GPA 3.80 GRE Quant 165 Verbal 158 AWA 4.5 American Fall 2026 Accepted",
            "date_added": None,
            "url": "https://example.com/applicant/1",
            "status": "Accepted",
            "term": "Fall 2026",
            "us_or_international": "American",
            "gpa": 3.80,
            "gre": 165.0,
            "gre_v": 158.0,
            "gre_aw": 4.5,
            "degree": "Masters",
            "llm_generated_program": "Computer Science",
            "llm_generated_university": "Johns Hopkins University",
        },
        {
            "program": "MIT - PhD Computer Science",
            "comments": "GPA 3.95 GRE Quant 170 Verbal 162 AWA 5.0 International Fall 2026 Accepted",
            "date_added": None,
            "url": "https://example.com/applicant/2",
            "status": "Accepted",
            "term": "Fall 2026",
            "us_or_international": "International",
            "gpa": 3.95,
            "gre": 170.0,
            "gre_v": 162.0,
            "gre_aw": 5.0,
            "degree": "PhD",
            "llm_generated_program": "Computer Science",
            "llm_generated_university": "Massachusetts Institute of Technology",
        },
        {
            "program": "Stanford University - PhD Computer Science",
            "comments": "GPA 3.88 International Fall 2026 Rejected",
            "date_added": None,
            "url": "https://example.com/applicant/3",
            "status": "Rejected",
            "term": "Fall 2026",
            "us_or_international": "International",
            "gpa": 3.88,
            "gre": None,
            "gre_v": None,
            "gre_aw": None,
            "degree": "PhD",
            "llm_generated_program": "Computer Science",
            "llm_generated_university": "Stanford University",
        },
    ]
