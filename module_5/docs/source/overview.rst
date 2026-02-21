Overview & Setup
================

This document describes how to set up and run the GradCafe Analytics
application locally.

Prerequisites
-------------

- Python 3.11+
- PostgreSQL 15+
- A virtual environment (recommended)

Required Environment Variables
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Variable
     - Description
   * - ``DATABASE_URL``
     - Primary psycopg3 connection string, e.g.
       ``dbname=gradcafe user=postgres host=localhost port=5432``
   * - ``PGDATABASE``
     - Database name (fallback if ``DATABASE_URL`` not set). Default: ``gradcafe``
   * - ``PGUSER``
     - PostgreSQL user (fallback). Default: current OS user
   * - ``PGHOST``
     - PostgreSQL host (fallback). Default: ``localhost``
   * - ``PGPORT``
     - PostgreSQL port (fallback). Default: ``5432``
   * - ``FLASK_SECRET_KEY``
     - Flask session secret. Default: ``dev-secret`` (change in production)

Installation
------------

.. code-block:: bash

   # 1. Clone the repository
   git clone git@github.com:moijunca/jhu_software_concepts.git
   cd jhu_software_concepts/module_4

   # 2. Create and activate a virtual environment
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate

   # 3. Install dependencies
   pip install -r requirements.txt

   # 4. Create the database
   createdb gradcafe

   # 5. Run the application
   python src/app.py

The app will be available at http://127.0.0.1:8000.

Running the Application
-----------------------

.. code-block:: bash

   # Load data into PostgreSQL
   python src/load_data.py

   # Run analytical queries
   python src/query_data.py

   # Start the Flask web server
   python src/app.py

Running the Tests
-----------------

.. code-block:: bash

   # Create the test database (first time only)
   createdb gradcafe_test

   # Run the full test suite with coverage
   cd module_4
   pytest tests/ -m "web or buttons or analysis or db or integration"

   # Run a specific marker group
   pytest tests/ -m web
   pytest tests/ -m buttons
   pytest tests/ -m analysis
   pytest tests/ -m db
   pytest tests/ -m integration
