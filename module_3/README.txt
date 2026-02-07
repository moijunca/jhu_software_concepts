Name: Moises Junca  
JHED ID: 33522  

Module Info:  
Module 3 – Databases, Querying, and Web Presentation  
Course: Software Concepts  
Due Date: (add official due date here)

------------------------------------------------------------
Overview
------------------------------------------------------------

This module builds on the data collected in Module 2 and focuses on:

1) Loading structured applicant data into a PostgreSQL database  
2) Performing analytical queries using SQL  
3) Presenting results through both command-line scripts and a simple web interface  

The goal of Module 3 is to transform previously scraped and cleaned data into a reproducible analytics workflow backed by a relational database.

------------------------------------------------------------
Data Source
------------------------------------------------------------

The input to this module is a JSONL file (`part1.json.jsonl`) generated from earlier scraping and cleaning work. Each line in this file represents a single GradCafe applicant record containing fields such as:

- program  
- comments  
- date_added  
- status  
- term  
- GPA  
- GRE  
- nationality  

Because the original GradCafe data is highly unstructured, most of these attributes must be extracted from free text using regular expressions.

------------------------------------------------------------
Part 1: Database Loading (load_data.py)
------------------------------------------------------------

The script `load_data.py` performs the following tasks:

- Connects to a local PostgreSQL database named “gradcafe”  
- Truncates any existing applicant records to ensure a clean reload  
- Reads records from `part1.json.jsonl`  
- Extracts structured attributes from free text, including:  
  - application term (e.g., “Fall 2026”, “F26”, “Fall ’26”)  
  - GPA values  
  - GRE scores  
  - decision status (Accepted, Rejected, etc.)  
  - nationality indicators (American / International)  
- Inserts the processed records into the `applicants` table  

This script ensures that messy raw text is converted into structured fields that can be queried reliably in SQL.

------------------------------------------------------------
Part 2: Querying the Database (query_data.py)
------------------------------------------------------------

The script `query_data.py` answers the required assignment questions directly from PostgreSQL using parameterized SQL queries.

The script reports:

Q1: Number of applicants for Fall 2026  
Q2: Percent of applicants who are International  
Q3: Average GPA and GRE among applicants who reported them  
Q4: Average GPA for American applicants in Fall 2026  
Q5: Acceptance percentage for Fall 2026 (based only on posts with a decision)  
Q6: Average GPA of accepted applicants in Fall 2026  

Additionally, two curiosity queries are implemented:

- Distribution of application terms (including “No term detected”)  
- Distribution of decision outcomes (Accepted, Rejected, Interview, etc.)

These outputs are printed to the console in a reproducible format.

------------------------------------------------------------
Part 3: Web Interface (app.py)
------------------------------------------------------------

A minimal Flask web application is provided in `app.py` which:

- Connects to the same PostgreSQL database  
- Executes the same queries used in `query_data.py`  
- Displays the results in a simple HTML webpage  

This satisfies the assignment requirement to present query results through a local web interface.

------------------------------------------------------------
Known Bugs / Limitations
------------------------------------------------------------

Selection Bias  
The scraper used to generate the source data relied on the parameter `sort=newest`.  
As a result, the dataset is intentionally biased toward the most recent GradCafe posts.  
This leads to an over-representation of the current admissions cycle (primarily “Fall 2026”) and an under-representation of older terms.

This bias affects analyses such as:

- “Top terms” distributions  
- Acceptance rates by term  
- Overall nationality percentages  

These results should therefore be interpreted as descriptive of recent posts rather than as representative of all historical GradCafe data.

Missing or Inconsistent Fields  
Many GradCafe posts do not include structured information such as GPA, GRE, or nationality.  
Although `load_data.py` attempts to extract these values using regular expressions, a significant portion of records still lack detectable values.

For example, in the current dataset:

- Total rows: 1,325  
- Rows with detected term: 525  
- Rows with GPA: 294  
- Rows with GRE: 42  

Averages and percentages are therefore calculated only from the subset of records where the relevant information could be extracted.

Extraction Accuracy  
Term detection relies on patterns such as “Fall 2026”, “F26”, or “Fall ’26”.  
Unusual formats or misspellings may still fail to be recognized.

------------------------------------------------------------
Files Included (Module 3)
------------------------------------------------------------

- load_data.py – loads JSONL data into PostgreSQL  
- query_data.py – performs SQL analysis  
- app.py – Flask web interface  
- part1.json.jsonl – cleaned input dataset  
- requirements.txt – Python dependencies  
- README.txt – this document  
- limitations.pdf – summary of limitations (submitted separately)  

------------------------------------------------------------
Reproducibility
------------------------------------------------------------

The full Module 3 workflow can be reproduced as follows:

1) Create a Python virtual environment  
2) Install dependencies using requirements.txt  
3) Ensure PostgreSQL is running with a database named “gradcafe”  
4) Execute:  
      python load_data.py  
      python query_data.py  
5) Run the web application:  
      python app.py  

No external APIs or paid services are required.