"""
clean.py

Lightweight structural cleaning for GradCafe applicant data.

This script performs basic, deterministic cleaning steps only:
- trims whitespace
- normalizes empty strings to None
- ensures required keys exist

Semantic normalization of program and university names is intentionally
handled by the local LLM pipeline in module_2/llm_hosting.
"""

import json
from typing import Dict, Any


REQUIRED_FIELDS = [
    "program_university_raw",
    "status_raw",
    "date_added_raw",
    "comments_raw",
    "source_url",
]


def _clean_value(value):
    """Strip whitespace and normalize empty strings to None."""
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Clean a single applicant record."""
    cleaned = {}

    for field in REQUIRED_FIELDS:
        cleaned[field] = _clean_value(record.get(field))

    return cleaned


def clean_dataset(input_path: str, output_path: str) -> None:
    """Load raw data, apply structural cleaning, and write cleaned output."""
    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    records = payload.get("records", [])
    cleaned_records = [clean_record(r) for r in records]

    output_payload = {
        **payload,
        "records": cleaned_records,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    clean_dataset(
        "module_2/applicant_data.json",
        "module_2/applicant_data_structural_clean.json",
    )