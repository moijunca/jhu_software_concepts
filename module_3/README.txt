Name: Moises Junca  
JHED ID: 33522  

Course: Software Concepts  
Module: 3 – Databases, Querying, and Web Presentation  

------------------------------------------------------------
OVERVIEW
------------------------------------------------------------

This module implements a complete analytics workflow using PostgreSQL and Python.  
The focus of Module 3 is to:

1. Load cleaned GradCafe applicant data into a relational database  
2. Perform structured analytical queries using SQL  
3. Present results through both:
   - a command-line reporting script  
   - a simple Flask-based web interface  

All components of Module 3 are designed to be self-contained and to run without requiring any code or data from Module 2.

------------------------------------------------------------
DATA SOURCE
------------------------------------------------------------

For this assignment, the source of truth is:

module_3/data/llm_extend_applicant_data.json

This file was provided by course staff (Liv d’Alberti) as a cleaned and structured dataset intended specifically for Module 3.

It contains approximately 30,000 cleaned GradCafe applicant records in JSONL format, with fields such as:

- program
- masters_or_phd
- comments
- date_added
- llm_generated_program
- llm_generated_university

Using this dataset ensures consistent and reliable results independent of individual Module 2 scraping performance.

------------------------------------------------------------
PROJECT STRUCTURE
------------------------------------------------------------

The following files make up the complete Module 3 submission:

module_3/
│── app.py                    # Flask web application
│── load_data.py              # Loads data into PostgreSQL
│── query_data.py             # Runs SQL analytics queries
│── requirements.txt          # Python dependencies
│── README.txt                # This file
│── screenshots/              # Required screenshots
│── data/
│    └── llm_extend_applicant_data.json   # Liv’s cleaned dataset (JSONL)
│── templates/
│    └── index.html           # Web UI template
│── static/
│    └── styles.css           # Styling for web interface

------------------------------------------------------------
PART 1: DATABASE LOADING (load_data.py)
------------------------------------------------------------

The script load_data.py performs the following steps:

- Connects to a PostgreSQL database named "gradcafe"
- Ensures a unique index on the applicants table for idempotent loads  
- Reads all records from:

module_3/data/llm_extend_applicant_data.json

- Cleans and normalizes important fields such as:

  - application term (e.g., “Fall 2026”)  
  - decision status  
  - degree level (Masters / PhD)  
  - nationality indicators  
  - program and university names  

- Inserts all records into the applicants table using:

ON CONFLICT DO NOTHING

so that the script can be safely re-run multiple times.

This script makes Module 3 fully independent of Module 2.

------------------------------------------------------------
PART 2: QUERYING THE DATABASE (query_data.py)
------------------------------------------------------------

The script query_data.py answers all required assignment questions directly from PostgreSQL using SQL queries.

It reports:

Required Questions

Q0: Total number of applicants  
Q1: Number of Fall 2026 applicants  
Q2: Percent International applicants  
Q3: Average GPA and GRE metrics  
Q4: Average GPA for American students in Fall 2026  
Q5: Acceptance percentage for Fall 2026  
Q6: Average GPA of accepted Fall 2026 applicants  
Q7: JHU Masters in Computer Science applicants  
Q8: 2026 PhD CS acceptances at Georgetown / MIT / Stanford / CMU (raw fields)  
Q9: Same as Q8 using LLM-generated fields  

Additional Curiosity Queries

Q10a: Top 10 universities by Fall 2026 Computer Science applicants  
Q10b: Acceptance rate by application term  

All results are printed in a reproducible, assignment-compliant format.

------------------------------------------------------------
PART 3: WEB INTERFACE (app.py)
------------------------------------------------------------

A minimal Flask application is included that:

- Connects to the same PostgreSQL database  
- Executes the same queries as query_data.py  
- Displays the results in a clean HTML dashboard  

This satisfies the assignment requirement for presenting analytics through a local web interface.

To run the web interface:

python module_3/app.py

Then open in a browser:

http://127.0.0.1:8000

------------------------------------------------------------
SCREENSHOTS
------------------------------------------------------------

The submission includes screenshots demonstrating:

- Successful execution of:
  - load_data.py
  - query_data.py
  - the running Flask web application  
- Console outputs showing correct query results  
- Browser view of the analytics dashboard  

These are located in:

module_3/screenshots/

------------------------------------------------------------
KNOWN LIMITATIONS
------------------------------------------------------------

1. SOURCE DATA LIMITATIONS

All results depend on the quality of Liv’s provided dataset.

- GPA and GRE values are largely absent from the provided data  
- Many records do not contain structured numeric fields  
- As a result, averages for GPA/GRE may display as None or zero  

This is an inherent limitation of the dataset, not of the SQL queries.

2. TERM AND STATUS EXTRACTION

Application term and decision status are derived from text fields using pattern matching.

Unusual formatting in source posts may cause:

- Some terms to be labeled as “No term detected”  
- Some statuses to remain unclassified  

3. SELECTION BIAS

GradCafe data represents self-reported posts rather than an official admissions dataset.

Therefore:

- The dataset is not statistically representative  
- Results should be interpreted as descriptive rather than authoritative  

------------------------------------------------------------
REPRODUCIBILITY
------------------------------------------------------------

To reproduce the full Module 3 workflow:

1) Create a virtual environment

python -m venv .venv
source .venv/bin/activate

2) Install dependencies

pip install -r requirements.txt

3) Ensure PostgreSQL is running

Database must be named:

gradcafe

4) Load data

python module_3/load_data.py

5) Run queries

python module_3/query_data.py

6) Launch web interface

python module_3/app.py

------------------------------------------------------------
FINAL NOTES
------------------------------------------------------------

- Module 3 does not rely on any Module 2 code or data  
- All functionality is fully self-contained  
- All requirements from the assignment specification have been implemented  

End of README
