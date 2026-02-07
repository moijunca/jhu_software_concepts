import json
import time
import random
import urllib3
import urllib.robotparser
from bs4 import BeautifulSoup

# -------------------------
# Configuration
# -------------------------
BASE_URL = "https://www.thegradcafe.com"
SURVEY_URL = f"{BASE_URL}/survey/"
ROBOTS_URL = f"{BASE_URL}/robots.txt"

MAX_RECORDS_DEFAULT = 30000
PER_PAGE = 100  # If unsupported by site, it will still return a page; we'll keep parsing what we get.
DELAY_MIN = 1.0
DELAY_MAX = 2.5

USER_AGENT = "JHU-Software-Concepts-Module2"

# -------------------------
# Core Requirements
# -------------------------
def check_robots_txt() -> bool:
    """
    Confirm that robots.txt permits scraping the survey pages.
    """
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(ROBOTS_URL)
    rp.read()
    return rp.can_fetch("*", SURVEY_URL)


def scrape_data(max_records: int = MAX_RECORDS_DEFAULT) -> list[dict]:
    """
    Pull data from GradCafe survey pages until max_records is reached (or pages run out).
    Returns raw records; detailed cleaning happens in clean.py.
    """
    if not check_robots_txt():
        raise RuntimeError("robots.txt does not permit scraping the survey pages")

    http = urllib3.PoolManager(headers={"User-Agent": USER_AGENT})

    all_records: list[dict] = []
    page = 1

    while len(all_records) < max_records:
        url = _build_survey_url(page)
        resp = http.request("GET", url)

        if resp.status != 200:
            raise RuntimeError(f"Failed page fetch: page={page} HTTP {resp.status}")

        html = resp.data.decode("utf-8", errors="ignore")
        page_records = _parse_rows_from_html(html, source_url=url)

        # Stop if no records are found (site layout changed or end of data)
        if not page_records:
            print(f"No rows found on page {page}. Stopping.")
            break

        all_records.extend(page_records)
        print(f"Page {page}: +{len(page_records)} records | total={len(all_records)}")

        page += 1
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Ensure we do not exceed max_records
    return all_records[:max_records]


def save_data(records: list[dict], output_path: str = "module_2/applicant_data.json") -> None:
    """
    Save records to JSON under applicant_data.json with reasonable top-level keys.
    """
    payload = {
        "source": "thegradcafe_survey",
        "base_url": BASE_URL,
        "survey_url": SURVEY_URL,
        "scraped_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "record_count": len(records),
        "records": records,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_data(input_path: str = "module_2/applicant_data.json") -> dict:
    """
    Load JSON data from applicant_data.json.
    Returns the full payload dict with metadata + records.
    """
    with open(input_path, "r", encoding="utf-8") as f:
        return json.load(f)


# -------------------------
# Helper / Milestone Functions
# -------------------------
def parse_sample(limit: int = 10) -> list[dict]:
    """
    Fetch ONE survey page and parse a small sample of rows for debugging.
    Use this to demonstrate parsing works before scaling.
    """
    if not check_robots_txt():
        raise RuntimeError("robots.txt does not permit scraping the survey pages")

    http = urllib3.PoolManager(headers={"User-Agent": USER_AGENT})
    resp = http.request("GET", SURVEY_URL)

    if resp.status != 200:
        raise RuntimeError(f"Failed to fetch survey page: HTTP {resp.status}")

    html = resp.data.decode("utf-8", errors="ignore")
    records = _parse_rows_from_html(html, source_url=SURVEY_URL)
    return records[:limit]


def _build_survey_url(page: int) -> str:
    """
    Build the URL for a given survey page.
    Some parameters may or may not be supported; the page should still return.
    """
    # Common pattern seen on similar survey pages:
    return f"{SURVEY_URL}index.php?page={page}&pp={PER_PAGE}&sort=newest"


def _parse_rows_from_html(html: str, source_url: str) -> list[dict]:
    """
    Parse the survey results table into raw dict records.

    Keys are raw on purpose (clean.py will transform these into final schema).
    Missing fields are stored as None.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    records: list[dict] = []

    for row in rows[1:]:  # skip header row
        cols = row.find_all("td")
        if not cols:
            continue

        # Normalize text (no HTML remnants)
        def cell_text(i: int):
            return cols[i].get_text(" ", strip=True) if len(cols) > i else None

        record = {
            "source_url": source_url,                # URL of the page where this row was scraped
            "program_university_raw": cell_text(0),  # mixed field (program + university)
            "status_raw": cell_text(1),              # Accepted/Rejected/Waitlisted/etc + sometimes extra
            "date_added_raw": cell_text(2),          # date the information was added
            "comments_raw": cell_text(3),            # applicant comments; may include metrics
        }

        records.append(record)

    return records


# -------------------------
# CLI / Main
# -------------------------
if __name__ == "__main__":
    # Milestone: parse 10 rows from one page (quick verification)
    sample = parse_sample(limit=10)
    print(f"Parsed {len(sample)} sample records:\n")
    for i, rec in enumerate(sample, 1):
        print(f"Record {i}")
        for k, v in rec.items():
            print(f"  {k}: {v}")
        print()

    records = scrape_data(max_records=30000)
    save_data(records, "module_2/applicant_data.json")
    print(f"Saved {len(records)} records to module_2/applicant_data.json")