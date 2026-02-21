Operational Notes
=================

Busy-State Policy
-----------------

The application uses a module-level ``_PULL_RUNNING`` flag (protected by a
``threading.Lock``) to prevent concurrent operations:

- While a pull is in progress, ``POST /pull-data`` returns **409**
  ``{"busy": true}``.
- While a pull is in progress, ``POST /update-analysis`` returns **409**
  ``{"busy": true}``.
- Once the pull completes (success or failure), ``_PULL_RUNNING`` is reset
  to ``False`` in the worker's ``finally`` block — guaranteeing the flag
  always resets even if an exception occurs.

**Important:** Tests must never use ``sleep()`` to check busy state.
Instead, inject the busy flag directly via ``app_module._LOCK``:

.. code-block:: python

   with app_module._LOCK:
       app_module._PULL_RUNNING = True

   response = client.post("/update-analysis")
   assert response.status_code == 409

Idempotency Strategy
--------------------

The ETL layer is designed to be safe to run multiple times:

1. All inserts use ``ON CONFLICT DO NOTHING`` against the unique index.
2. The unique index ``applicants_sig_unique`` is defined on
   ``(COALESCE(url,''), COALESCE(program,''), COALESCE(comments,''))``.
3. Running ``POST /pull-data`` twice with the same data will not create
   duplicate rows.
4. ``ensure_table()`` / ``ensure_index()`` are idempotent — safe to call
   on every startup.

Uniqueness Keys
---------------

A row is considered a duplicate if it shares the same combination of:

- ``url`` — source URL of the applicant record
- ``program`` — raw program/university string
- ``comments`` — applicant free-text comments

This triple captures the semantic identity of a scraped record. Two records
from the same URL with the same program and comments are always the same
applicant entry, regardless of when they were scraped.

Troubleshooting
---------------

**Tests fail with** ``psycopg.OperationalError: connection refused``

The test database is not running. Start PostgreSQL and ensure
``gradcafe_test`` exists:

.. code-block:: bash

   pg_ctl start
   createdb gradcafe_test

**Tests fail with** ``relation "applicants" does not exist``

The schema has not been initialised. The ``app`` fixture calls
``ensure_table()`` automatically — make sure you are using the ``app``
fixture in your test.

**GitHub Actions workflow fails at "Install dependencies"**

Check that ``module_4/requirements.txt`` contains ``psycopg[binary]``
(not ``psycopg2``).

**Coverage is below 100%**

Run the coverage report locally to find uncovered lines:

.. code-block:: bash

   pytest tests/ -m "web or buttons or analysis or db or integration" \
     --cov=src --cov-report=term-missing

Then add additional tests to cover the missing lines.

**Pull Data never completes in tests**

Use a ``threading.Event`` to wait for the background worker:

.. code-block:: python

   done = threading.Event()

   def fake_scraper():
       done.set()
       return []

   client.application.config["SCRAPER_FN"] = fake_scraper
   client.post("/pull-data")
   done.wait(timeout=5)
