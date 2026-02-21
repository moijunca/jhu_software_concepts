Testing Guide
=============

The test suite uses ``pytest`` with custom markers to organise tests by
concern. All tests must be marked — no unmarked tests are permitted.

Pytest Markers
--------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Marker
     - Description
   * - ``web``
     - Flask route/page load tests (``test_flask_page.py``)
   * - ``buttons``
     - Pull Data and Update Analysis button behavior (``test_buttons.py``)
   * - ``analysis``
     - Formatting/rounding of analysis output (``test_analysis_format.py``)
   * - ``db``
     - Database schema, inserts, and selects (``test_db_insert.py``)
   * - ``integration``
     - End-to-end flows (``test_integration_end_to_end.py``)

Running Tests
-------------

.. code-block:: bash

   # Run the entire suite (all markers)
   pytest tests/ -m "web or buttons or analysis or db or integration"

   # Run a single marker group
   pytest tests/ -m web
   pytest tests/ -m db
   pytest tests/ -m integration

   # Run with verbose output
   pytest tests/ -m "web or buttons or analysis or db or integration" -v

   # Run with coverage report
   pytest tests/ -m "web or buttons or analysis or db or integration" \
     --cov=src --cov-report=term-missing

HTML Selectors
--------------

The Flask template exposes stable ``data-testid`` attributes for UI tests:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Selector
     - Element
   * - ``data-testid="pull-data-btn"``
     - The Pull Data submit button
   * - ``data-testid="update-analysis-btn"``
     - The Update Analysis submit button

Use ``BeautifulSoup`` to find these elements:

.. code-block:: python

   from bs4 import BeautifulSoup

   response = client.get("/")
   soup = BeautifulSoup(response.data, "html.parser")
   btn = soup.find(attrs={"data-testid": "pull-data-btn"})
   assert btn is not None

Test Fixtures
-------------

All shared fixtures are defined in ``tests/conftest.py``:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Fixture
     - Description
   * - ``app``
     - Flask app pointed at the isolated ``gradcafe_test`` database
   * - ``client``
     - Flask test client for HTTP assertions
   * - ``db_conn``
     - Raw psycopg3 connection to the test database
   * - ``empty_db``
     - Truncates ``applicants`` before and after each test
   * - ``sample_rows``
     - List of 3 minimal applicant dicts for use as fake scraper data

Fake Scraper Injection
----------------------

Tests inject a fake scraper via ``app.config["SCRAPER_FN"]`` to avoid
hitting the network:

.. code-block:: python

   def fake_scraper():
       return [{"program": "MIT", "url": "https://...", ...}]

   client.application.config["SCRAPER_FN"] = fake_scraper
   response = client.post("/pull-data")
   assert response.status_code == 200

Test Database
-------------

Tests run against a dedicated ``gradcafe_test`` database. Create it once:

.. code-block:: bash

   createdb gradcafe_test

The ``TEST_DATABASE_URL`` environment variable overrides the default
connection string — used automatically by the GitHub Actions CI workflow.
