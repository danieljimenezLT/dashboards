#!/usr/bin/env python3
"""
fetch_grassroots_spend_sheet.py

Reads Grassroots spend entries from the NSO Spend Form response sheet
and outputs nso_grassroots_spend.json, keyed by studio name with
Sun-Sat week entries that the scorecard (test) tab reads via date-range
overlap matching.
"""
import json
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

SHEET_ID   = "1oLvrjTfcGUmt-mi1bp2xnqmESgt4uu4QlLCfAOKoRd8"
OUTPUT     = Path(__file__).parent.parent / "nso_grassroots_spend.json"
CREDS_PATH = Path(__file__).parent.parent / "credentials" / "service_account.json"
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Sheet studio names → scorecard/scorecard2 studio names (where they differ)
NAME_OVERRIDES = {
    "Uptown": "Dallas - Uptown",
}


def parse_date(val: str) -> str | None:
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def main():
    creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
    gc    = gspread.authorize(creds)
    ws    = gc.open_by_key(SHEET_ID).get_worksheet(0)
    rows  = ws.get_all_values()

    if not rows:
        print("ERROR: destination sheet is empty")
        return

    header = rows[0]

    def _col(name, fallback):
        try:
            return header.index(name)
        except ValueError:
            return fallback

    c_studio   = _col("Studio", 2)
    c_start    = _col("Start date of spend", 3)
    c_end      = _col("End date of spend", 4)
    c_category = _col("Spend Category", 5)
    c_amount   = _col("Amount", 6)

    studios: dict[str, list[dict]] = {}

    for row in rows[1:]:
        if not row:
            continue
        def _cell(idx):
            return row[idx].strip() if idx < len(row) else ""

        studio   = _cell(c_studio)
        category = _cell(c_category)
        amount_s = _cell(c_amount)
        start_s  = _cell(c_start)
        end_s    = _cell(c_end)

        if not studio or category.lower() != "grassroots":
            continue

        try:
            amount = float(amount_s)
        except ValueError:
            continue
        if amount <= 0:
            continue

        week_start = parse_date(start_s)
        week_end   = parse_date(end_s)
        if not week_start or not week_end:
            continue

        canonical = NAME_OVERRIDES.get(studio, studio)
        studios.setdefault(canonical, []).append({
            "week_start": week_start,
            "week_end":   week_end,
            "amount":     round(amount, 2),
        })

    for name in studios:
        studios[name].sort(key=lambda x: x["week_start"])

    result = {
        "generated_at": datetime.now().isoformat(),
        "source_sheet": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}",
        "studios": studios,
    }

    OUTPUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    total = sum(len(v) for v in studios.values())
    print(f"Done. {len(studios)} studios, {total} entries -> {OUTPUT.name} "
          f"({OUTPUT.stat().st_size/1000:.1f} KB)")
    for name, entries in sorted(studios.items()):
        total_amt = sum(e["amount"] for e in entries)
        print(f"  {name:<28}  {len(entries):>3} weeks  ${total_amt:>8.2f}")


if __name__ == "__main__":
    main()
