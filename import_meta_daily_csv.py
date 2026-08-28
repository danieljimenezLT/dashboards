"""
import_meta_daily_csv.py — one-time import of a user-supplied CSV of real
daily Meta Ads spend per studio into meta-ads-baked.json's studio_daily.

Expected CSV columns: Studio, Date, Amount Spent
  Studio:       display name (aliased below where the export's label
                doesn't match studios.json's canonical name exactly)
  Date:         "Mon D, YYYY" (e.g. "Jan 10, 2026")
  Amount Spent: dollar amount for that studio on that day

Rows whose Studio doesn't match any studios.json entry (even after aliasing)
are skipped and logged with their total — e.g. franchise/corporate campaigns
not tied to a single studio.

Usage:
    python3 import_meta_daily_csv.py "path/to/export.csv"

After running, regenerate spend-data.json:
    python3 build_spend_data.py
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT    = Path(__file__).resolve().parent
BAKED_PATH   = REPO_ROOT / "meta-ads-baked.json"
STUDIOS_PATH = REPO_ROOT / "studios.json"

# CSV studio label -> studios.json canonical name, for labels that don't
# match exactly (case/naming differences in the export).
STUDIO_ALIAS = {
    "Capitol View":     "Nashville - Capitol View",
    "Charlotte - NODA": "Charlotte - Noda",
    "Midtown Miami":    "Miami - Midtown",
    "Coconut Grove":    "Miami - Coconut Grove",
    "Park Slope":       "NYC - Park Slope",
}


def run(csv_path: str) -> None:
    studios_raw = json.loads(STUDIOS_PATH.read_text(encoding="utf-8"))
    studios_list = studios_raw.get("studios", studios_raw) if isinstance(studios_raw, dict) else studios_raw
    name_to_code = {s["name"]: s["code"] for s in studios_list if s.get("name") and s.get("code")}

    spend_by_day: dict[tuple, float] = {}
    skipped: dict[str, float] = {}
    rows_read = 0

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_read += 1
            raw_studio = (row.get("Studio") or "").strip()
            date_str   = (row.get("Date") or "").strip()
            amount_str = (row.get("Amount Spent") or "").strip()
            if not raw_studio or not date_str or not amount_str:
                continue

            studio_name = STUDIO_ALIAS.get(raw_studio, raw_studio)
            code = name_to_code.get(studio_name)
            amount = round(float(amount_str), 2)
            if not code:
                skipped[raw_studio] = round(skipped.get(raw_studio, 0.0) + amount, 2)
                continue

            d = datetime.strptime(date_str, "%b %d, %Y").date().isoformat()
            k = (d, code)
            spend_by_day[k] = round(spend_by_day.get(k, 0.0) + amount, 2)

    print(f"Read {rows_read} CSV rows")
    if skipped:
        print("Skipped (no matching studio):")
        for label, total in sorted(skipped.items(), key=lambda x: -x[1]):
            print(f"  {label}: ${total:,.2f}")

    if not spend_by_day:
        print("Nothing to bake — exiting without touching meta-ads-baked.json.")
        return

    total_imported = round(sum(spend_by_day.values()), 2)
    dates = sorted({d for (d, _c) in spend_by_day})
    date_from, date_to = dates[0], dates[-1]
    print(f"Imported {len(spend_by_day)} (date, studio) rows, ${total_imported:,.2f} total, {date_from} to {date_to}")

    existing = json.loads(BAKED_PATH.read_text(encoding="utf-8")) if BAKED_PATH.exists() else {}
    # Drop any prior baked rows inside this range before re-adding (idempotent re-run)
    studio_daily = [
        r for r in existing.get("studio_daily", [])
        if not (date_from <= r.get("date", "") <= date_to)
    ]
    for (d, code), spend in sorted(spend_by_day.items()):
        studio_daily.append({"date": d, "studio_code": code, "spend": spend})
    studio_daily.sort(key=lambda r: (r["date"], r["studio_code"]))
    existing["studio_daily"] = studio_daily

    BAKED_PATH.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Baked {len(spend_by_day)} rows into {BAKED_PATH}")
    print("Now run: python3 build_spend_data.py")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 import_meta_daily_csv.py <path-to-csv>")
        sys.exit(1)
    run(sys.argv[1])
