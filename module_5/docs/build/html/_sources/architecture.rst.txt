Architecture
============

The GradCafe Analytics system is divided into three layers:

Web Layer (Flask)
-----------------

**File:** ``src/app.py``

The web layer serves the analysis dashboard and exposes three HTTP endpoints:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Route
     - Method
     - Description
   * - ``/``
     - GET
     - Renders the analysis dashboard. Calls ``fetch_metrics()`` to
       compute Q1–Q10 directly from PostgreSQL and passes results to
       ``index.html``.
   * - ``/pull-data``
     - POST
     - Starts a background thread that runs the scrape → clean →
       load pipeline. Returns ``{"ok": true}`` (200) when not busy,
       or ``{"busy": true}`` (409) if a pull is already running.
   * - ``/update-analysis``
     - POST
     - Starts a background thread that refreshes the analysis output.
       Returns 409 if a pull is in progress.

The app uses a ``create_app()`` factory so it can be instantiated with
different configurations (e.g. a test database URL) without side effects.

**Busy-state policy:** A module-level ``_PULL_RUNNING`` flag (protected by
a ``threading.Lock``) prevents concurrent pulls and disables
``/update-analysis`` while a pull is in progress.

ETL Layer
---------

**File:** ``src/load_data.py``

Responsible for reading the LLM-extended JSONL dataset and inserting rows
into PostgreSQL. Key responsibilities:

- Parse and clean raw applicant records (GPA, GRE, term, status, nationality).
- Normalise degree strings (``PhD``, ``Masters``, ``Bachelors``).
- Extract structured fields from free-text comments using regex.
- Insert rows with ``ON CONFLICT DO NOTHING`` to ensure idempotency.
- Maintain a unique index on ``(url, program, comments)`` to prevent
  duplicate rows across multiple pulls.

DB / Query Layer
----------------

**File:** ``src/query_data.py``

Executes all analytical SQL queries against the ``applicants`` table and
returns results as a Python dictionary. Queries cover:

- Q1: Fall 2026 applicant count
- Q2: Percent international (known nationality only)
- Q3: Average GPA / GRE scores
- Q4: Average GPA of American students in Fall 2026
- Q5: Acceptance rate for Fall 2026 decisions
- Q6: Average GPA of accepted applicants in Fall 2026
- Q7: JHU Masters in Computer Science applicants
- Q8: 2026 PhD CS acceptances at Georgetown / MIT / Stanford / CMU (raw fields)
- Q9: Same as Q8 using LLM-normalised university/program fields
- Q10a/b: Two additional curiosity questions

Database Schema
---------------

Table: ``applicants``

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Column
     - Type
     - Description
   * - ``p_id``
     - SERIAL PK
     - Auto-incrementing primary key
   * - ``program``
     - TEXT
     - Raw program/university string
   * - ``comments``
     - TEXT
     - Applicant free-text comments
   * - ``date_added``
     - DATE
     - Date the record was added
   * - ``url``
     - TEXT
     - Source URL (part of unique key)
   * - ``status``
     - TEXT
     - Admission decision (Accepted/Rejected/Waitlisted/Interview)
   * - ``term``
     - TEXT
     - Normalised semester/year (e.g. ``Fall 2026``)
   * - ``us_or_international``
     - TEXT
     - ``American`` or ``International``
   * - ``gpa``
     - NUMERIC
     - GPA extracted from comments
   * - ``gre``
     - NUMERIC
     - GRE Quantitative score
   * - ``gre_v``
     - NUMERIC
     - GRE Verbal score
   * - ``gre_aw``
     - NUMERIC
     - GRE Analytical Writing score
   * - ``degree``
     - TEXT
     - Normalised degree (PhD/Masters/Bachelors)
   * - ``llm_generated_program``
     - TEXT
     - LLM-normalised program name
   * - ``llm_generated_university``
     - TEXT
     - LLM-normalised university name

**Unique index:** ``applicants_sig_unique`` on
``(COALESCE(url,''), COALESCE(program,''), COALESCE(comments,''))``
