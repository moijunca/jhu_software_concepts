import os
import sys
import pytest
import psycopg

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from app import create_app
from load_data import ensure_table

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "dbname=gradcafe_test user={user} host=localhost port=5432".format(
        user=os.getenv("PGUSER", os.getenv("USER", "postgres"))
    ),
)

@pytest.fixture()
def app():
    flask_app = create_app({"TESTING": True, "DATABASE_URL": TEST_DATABASE_URL})
    ensure_table(flask_app)
    yield flask_app

@pytest.fixture()
def client(app):
    return app.test_client()

@pytest.fixture()
def db_conn(app):
    conn = psycopg.connect(TEST_DATABASE_URL)
    yield conn
    conn.close()

@pytest.fixture()
def empty_db(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY CASCADE;")
    db_conn.commit()
    yield
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY CASCADE;")
    db_conn.commit()

@pytest.fixture()
def sample_rows():
    return [
        {"program": "Johns Hopkins University - Masters Computer Science",
         "comments": "GPA 3.80 GRE Quant 165 Verbal 158 AWA 4.5 American Fall 2026 Accepted",
         "date_added": None, "url": "https://example.com/applicant/1",
         "status": "Accepted", "term": "Fall 2026", "us_or_international": "American",
         "gpa": 3.80, "gre": 165.0, "gre_v": 158.0, "gre_aw": 4.5, "degree": "Masters",
         "llm_generated_program": "Computer Science", "llm_generated_university": "Johns Hopkins University"},
        {"program": "MIT - PhD Computer Science",
         "comments": "GPA 3.95 GRE Quant 170 Verbal 162 AWA 5.0 International Fall 2026 Accepted",
         "date_added": None, "url": "https://example.com/applicant/2",
         "status": "Accepted", "term": "Fall 2026", "us_or_international": "International",
         "gpa": 3.95, "gre": 170.0, "gre_v": 162.0, "gre_aw": 5.0, "degree": "PhD",
         "llm_generated_program": "Computer Science", "llm_generated_university": "Massachusetts Institute of Technology"},
        {"program": "Stanford University - PhD Computer Science",
         "comments": "GPA 3.88 International Fall 2026 Rejected",
         "date_added": None, "url": "https://example.com/applicant/3",
         "status": "Rejected", "term": "Fall 2026", "us_or_international": "International",
         "gpa": 3.88, "gre": None, "gre_v": None, "gre_aw": None, "degree": "PhD",
         "llm_generated_program": "Computer Science", "llm_generated_university": "Stanford University"},
    ]
