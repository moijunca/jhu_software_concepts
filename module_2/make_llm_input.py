import argparse
import json
import os
from typing import Any, Dict, List, Optional


def _load_payload(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _pick_program(rec: Dict[str, Any]) -> str:
    # LLM-hosting expects a key named "program"
    # Prefer the structural raw program/university mix (what the LLM is meant to standardize)
    for k in ("program_university_raw", "program", "program_university"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _pick_url(rec: Dict[str, Any]) -> Optional[str]:
    for k in ("source_url", "url"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_status(rec: Dict[str, Any]) -> Optional[str]:
    for k in ("status_raw", "status"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_date_added(rec: Dict[str, Any]) -> Optional[str]:
    # Keep original string (LLM file is allowed to keep it as text)
    for k in ("date_added_raw", "date_added"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_comments(rec: Dict[str, Any]) -> Optional[str]:
    for k in ("comments_raw", "comments"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def transform_records(records: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rec in records[:limit]:
        row = {
            # REQUIRED by llm_hosting/app.py
            "program": _pick_program(rec),

            # Keep these for joining later (and for Module 3 table columns)
            "comments": _pick_comments(rec),
            "date_added": _pick_date_added(rec),
            "url": _pick_url(rec),
            "status": _pick_status(rec),

            # Provide placeholders (your pipeline can fill these later if you want)
            "term": rec.get("term") or "",
            "US/International": rec.get("US/International") or rec.get("us_or_international") or "",
            "Degree": rec.get("Degree") or rec.get("degree") or "",
        }
        out.append(row)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Input cleaned JSON (module_2/applicant_data_structural_clean.json)")
    ap.add_argument("--outdir", default="module_2/llm_chunks", help="Output folder for chunk JSON files")
    ap.add_argument("--chunk-size", type=int, default=1000, help="Rows per chunk file")
    ap.add_argument("--limit", type=int, default=30000, help="Max rows to transform")
    args = ap.parse_args()

    payload = _load_payload(args.infile)

    # your clean.py writes {"records":[...]} (plus metadata)
    records = payload.get("records", [])
    if not isinstance(records, list):
        raise ValueError("Expected 'records' to be a list in cleaned file.")

    rows = transform_records(records, limit=min(args.limit, len(records)))
    chunks = _chunk_list(rows, args.chunk_size)

    os.makedirs(args.outdir, exist_ok=True)
    for i, ch in enumerate(chunks, start=1):
        out_path = os.path.join(args.outdir, f"llm_in_chunk_{i:04d}.json")
        _write_json(out_path, {"rows": ch})
    print(f"Created {len(chunks)} chunk file(s) in {args.outdir}")
    print(f"Total rows prepared for LLM: {len(rows)}")


if __name__ == "__main__":
    main()
