# Module 4: GradCafe Analytics — Pytest & Sphinx

A test-driven, documented Grad Café data and web analysis service built with Flask, PostgreSQL (psycopg3), Pytest, and Sphinx.

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 15+

### Installation

```bash
# From the repo root
cd module_4

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create the database
createdb gradcafe

# Run the app
python src/app.py
```

The app will be available at http://127.0.0.1:8000.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | psycopg3 connection string | — |
| `PGDATABASE` | Database name (fallback) | `gradcafe` |
| `PGUSER` | PostgreSQL user (fallback) | current OS user |
| `PGHOST` | PostgreSQL host (fallback) | `localhost` |
| `PGPORT` | PostgreSQL port (fallback) | `5432` |
| `FLASK_SECRET_KEY` | Flask session secret | `dev-secret` |

## Project Structure

```
module_4/
  src/               # Application code (Flask, ETL, queries)
    app.py           # Flask app factory + routes
    load_data.py     # ETL: parse JSONL → PostgreSQL
    query_data.py    # Analytical SQL queries
    templates/       # Jinja2 HTML templates
    static/          # CSS
  tests/             # All test code
    conftest.py      # Shared fixtures
    test_flask_page.py
    test_buttons.py
    test_analysis_format.py
    test_db_insert.py
    test_integration_end_to_end.py
  docs/              # Sphinx documentation
    source/
      conf.py
      index.rst
      overview.rst
      architecture.rst
      api.rst
      testing.rst
      operational_notes.rst
  pytest.ini
  requirements.txt
  README.md
```

## Running Tests

```bash
# Create the test database (first time only)
createdb gradcafe_test

# Run the full suite
cd module_4
pytest tests/ -m "web or buttons or analysis or db or integration"

# Run with coverage
pytest tests/ -m "web or buttons or analysis or db or integration" \
  --cov=src --cov-report=term-missing
```

### Test Markers

| Marker | Description |
|--------|-------------|
| `web` | Flask route/page load tests |
| `buttons` | Pull Data and Update Analysis behavior |
| `analysis` | Formatting/rounding of analysis output |
| `db` | Database schema, inserts, selects |
| `integration` | End-to-end flows |

## CI

GitHub Actions runs the full test suite on every push to `main`.
See `.github/workflows/tests.yml`.

## Documentation

Sphinx documentation: https://jhu-gradcafe-module4.readthedocs.io

To build locally:

```bash
pip install sphinx
cd module_4/docs
make html
# Open docs/build/html/index.html in your browser
```

## Architecture

- **Web (Flask):** `src/app.py` — serves the analysis dashboard via `create_app()` factory. Routes return JSON (200/409) for testability.
- **ETL:** `src/load_data.py` — parses LLM-extended JSONL, cleans/normalises fields, inserts into PostgreSQL with idempotency via `ON CONFLICT DO NOTHING`.
- **DB/Queries:** `src/query_data.py` — executes Q1–Q10 analytical queries and returns results as a dictionary.

## Fresh Install Instructions

### Method 1: pip + venv (Traditional)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

### Method 2: uv (Fast Alternative)
```bash
pip install uv
uv venv
source .venv/bin/activate
uv pip sync requirements.txt
pip install -e .
```

### Running the Application
```bash
export DB_NAME=gradcafe
export DB_USER=gradcafe_user
export DB_PASSWORD=your_password
python src/app.py
```

### Running Tests
```bash
pytest tests/ --cov=src --cov-report=term-missing
```

### Running Pylint
```bash
pylint src/*.py
```
