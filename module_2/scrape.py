import json
import random
import time
import urllib.request
import urllib.parse
import urllib.robotparser
from bs4 import BeautifulSoup

# -------------------------
# Configuration
# -------------------------
BASE_URL = "https://www.thegradcafe.com"
SURVEY_PATH = "/survey/index.php"
SURVEY_URL = f"{BASE_URL}/survey/"
ROBOTS_URL = f"{BASE_URL}/robots.txt"

MAX_RECORDS_DEFAULT = 30000
PER_PAGE = 100
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
    # Check the SURVEY_PATH specifically (not just /survey/)
    return rp.can_fetch(USER_AGENT, SURVEY_PATH) or rp.can_fetch("*", SURVEY_PATH)


def scrape_data(max_records: int = MAX_RECORDS_DEFAULT) -> list[dict]:
    """
    Pull data from GradCafe survey pages until max_records is reached (or pages run out).
    Returns raw records; cleaning happens in clean.py.
    """
    if not check_robots_txt():
        raise RuntimeError("robots.txt does not permit scraping the survey pages")

    all_records: list[dict] = []
    page = 1

    while len(all_records) < max_records:
        page_url = _build_survey_url(page)

        html = _fetch_html(page_url)
        page_records = _parse_rows_from_html(html, page_url=page_url, page_num=page)

        if not page_records:
            print(f"No rows found on page {page}. Stopping.")
            break

        all_records.extend(page_records)
        print(f"Page {page}: +{len(page_records)} records | total={len(all_records)}")

        page += 1
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

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
    """
    if not check_robots_txt():
        raise RuntimeError("robots.txt does not permit scraping the survey pages")

    url = _build_survey_url(page=1)
    html = _fetch_html(url)
    records = _parse_rows_from_html(html, page_url=url, page_num=1)
    return records[:limit]


def _build_survey_url(page: int) -> str:
    """
    Build the URL for a given survey page.
    """
    qs = urllib.parse.urlencode({"page": page, "pp": PER_PAGE, "sort": "newest"})
    return f"{BASE_URL}{SURVEY_PATH}?{qs}"


def _fetch_html(url: str) -> str:
    """
    Fetch HTML using urllib.request with a User-Agent header.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="ignore")


def _normalize_header(s: str) -> str:
    """
    Normalize header text to improve mapping stability.
    """
    return " ".join((s or "").strip().lower().split())


def _pick_first_link(row) -> str | None:
    """
    Try to find a row-specific link (best effort).
    If the table includes a link per row, use it.
    """
    a = row.find("a", href=True)
    if not a:
        return None
    href = (a.get("href") or "").strip()
    if not href:
        return None
    return urllib.parse.urljoin(BASE_URL, href)


def _parse_rows_from_html(html: str, page_url: str, page_num: int) -> list[dict]:
    """
    Parse the survey results table into raw dict records.

    IMPORTANT:
    - We map cells by reading header names (th) rather than assuming fixed indices.
    - We still keep "program_university_raw" because module_2 cleaning + llm_hosting expect it.
    - We also keep "source_url" as a row-specific URL when possible.
    """
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Build header map from first row with th cells
    header_cells = rows[0].find_all(["th", "td"])
    headers = [_normalize_header(h.get_text(" ", strip=True)) for h in header_cells]

    # Heuristics: identify likely columns by header keywords
    # (GradCafe has changed labels over time; we allow multiple possibilities)
    def find_col(*keywords: str) -> int | None:
        for i, h in enumerate(headers):
            for kw in keywords:
                if kw in h:
                    return i
        return None

    col_uni = find_col("institution", "school", "university")
    col_prog = find_col("program", "department", "subject")
    col_decision = find_col("decision", "result", "status")
    col_date = find_col("date added", "added", "date")
    col_notes = find_col("notes", "comment", "comments")

    records: list[dict] = []

    for r_i, row in enumerate(rows[1:], start=1):  # skip header
        cols = row.find_all("td")
        if not cols:
            continue

        def cell_text(idx: int | None) -> str | None:
            if idx is None:
                return None
            if idx < 0 or idx >= len(cols):
                return None
            txt = cols[idx].get_text(" ", strip=True)
            return txt if txt != "" else None

        uni = cell_text(col_uni)
        prog = cell_text(col_prog)

        # If we can't detect separate uni/prog, fall back:
        # many GradCafe layouts have the combined field in the first column.
        combined = None
        if prog and uni:
            combined = f"{prog}, {uni}"
        elif prog:
            combined = prog
        elif uni:
            combined = uni
        else:
            # last resort: first col text
            combined = cols[0].get_text(" ", strip=True) or None

        decision = cell_text(col_decision)
        date_added = cell_text(col_date)
        notes = cell_text(col_notes)

        # Row-specific URL (best effort)
        row_link = _pick_first_link(row)
        if not row_link:
            # At least make it unique to the row rather than “page 1 always”
            row_link = f"{page_url}#row-{page_num}-{r_i}"

        record = {
            # REQUIRED raw fields for your existing clean.py expectations
            "source_url": row_link,
            "program_university_raw": combined,
            "status_raw": decision,
            "date_added_raw": date_added,
            "comments_raw": notes,
        }

        records.append(record)

    return records


# -------------------------
# CLI / Main
# -------------------------
if __name__ == "__main__":
    sample = parse_sample(limit=10)
    print(f"Parsed {len(sample)} sample records:\n")
    for i, rec in enumerate(sample, 1):
        print(f"Record {i}")
        for k, v in rec.items():
            print(f"  {k}: {v}")
        print()

    records = scrape_data(max_records=200)
    save_data(records, "module_2/applicant_data.json")
    print(f"Saved {len(records)} records to module_2/applicant_data.json")
