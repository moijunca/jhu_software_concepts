Name:
Moises Junca
JHED ID: 33522

Module Info:
Module 2 – Web Scraping, Data Cleaning, and Local LLM Normalization
Course: Software Concepts
Due Date: (add the official due date here)

------------------------------------------------------------
Overview
------------------------------------------------------------
This module implements a complete pipeline to collect, clean, and standardize
graduate admissions data from the GradCafe website. The project is divided
into three main phases:

1) Ethical web scraping using urllib3
2) Structuring and storing large-scale applicant data as JSON
3) Program and university name normalization using a self-hosted local LLM

The final output consists of both a raw scraped dataset and a cleaned,
LLM-standardized dataset suitable for downstream analysis.

------------------------------------------------------------
Approach
------------------------------------------------------------

Part 1: Web Scraping (scrape.py)

The scraper is implemented in scrape.py and uses only libraries covered in
Module 2 lectures, including urllib3, urllib.robotparser, BeautifulSoup,
and standard Python libraries.

Key aspects of the approach:

Robots.txt Verification:
The GradCafe robots.txt file was manually reviewed in a browser prior to
scraping. A screenshot of the robots.txt file confirming that User-agent "*"
is allowed to access the site is included as:
module_2/robots_txt_checked.png

- Ethical scraping:
  Requests are rate-limited using randomized sleep intervals between
  1.0 and 2.5 seconds per page to avoid overloading the server.

- Explicit academic identification:
  Requests use a clear academic User-Agent string
  ("JHU-Software-Concepts-Module2") rather than impersonating a browser.

- Pagination and scale:
  The scraper iterates through survey pages until at least 30,000 applicant
  records are collected. If more than 30,000 records are fetched, the result
  is trimmed to exactly 30,000 entries as required.

- Data extraction:
  HTML is parsed using BeautifulSoup. Each table row is converted into a
  structured Python dictionary containing:
    - program_university_raw
    - status_raw
    - date_added_raw
    - comments_raw
    - source_url

- Storage:
  The final output is written to module_2/applicant_data.json as a structured
  JSON object containing metadata (source, timestamp, record_count) and
  a list of applicant records.

Part 2: Local LLM Normalization (llm_hosting)

GradCafe program and university fields are highly inconsistent and often
contain abbreviations, typos, and mixed labels (e.g., “JHU”, “John Hopkins”,
“Johns Hopkins University”).

To address this, an instructor-provided local LLM package was added as a
sub-package under module_2/llm_hosting. This approach avoids third-party
APIs and keeps the entire pipeline local and reproducible.

The normalization process works as follows:

- A small local language model proposes standardized program and university
  names based on the raw scraped text.
- The model output is post-processed using canonical lists
  (canon_programs.txt and canon_universities.txt).
- Lightweight string normalization and fuzzy matching are applied to map
  near-matches and common abbreviations to canonical forms.

The LLM is executed locally using:
  python app.py --file applicant_data.json

Part 3: Cleaned Output

The normalized dataset is written to:
  module_2/llm_extend_applicant_data.json

This file contains the original scraped data along with additional fields
for cleaned and standardized program and university names, making the data
suitable for aggregation, grouping, and analysis.

------------------------------------------------------------
Known Bugs / Limitations
------------------------------------------------------------

- GradCafe survey formatting varies over time. If the HTML table structure
  changes significantly, the scraper may require small adjustments to the
  parsing logic.

- Some applicant comments contain free-form text with embedded metrics
  (e.g., GPA, GRE scores). These values are preserved as raw text rather
  than fully parsed into numeric fields.

- The LLM-based normalization is intentionally conservative. Rare or highly
  ambiguous program names may still require future additions to the
  canonical lists to improve accuracy.

------------------------------------------------------------
Files Included
------------------------------------------------------------

- scrape.py
- clean.py
- applicant_data.json
- llm_extend_applicant_data.json
- llm_hosting/ (local LLM sub-package, instructor-provided)
- requirements.txt
- README.txt
- robots.txt screenshot (showing permission to scrape)

------------------------------------------------------------
Reproducibility
------------------------------------------------------------

The entire pipeline can be reproduced locally using Python 3.10+ by:
1) Creating a virtual environment
2) Installing requirements.txt
3) Running scrape.py
4) Running the LLM standardizer in llm_hosting

No external APIs or paid services are required.