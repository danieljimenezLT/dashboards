#!/usr/bin/env python3
"""
migrate_grassroots_spend.py

Reads Grassroots Spend ($) from the NSO Manual Metrics Google Sheet
(Weekly Events & Spend tab).  The source sheet uses Monday-Sunday weeks
anchored to each studio's Week 1 Start Date.

Process:
  1. Spread each source-week's spend evenly across its 7 days (daily = spend/7)
  2. Re-group daily amounts into Sunday-Saturday calendar weeks
  3. Write one row per non-zero Sun-Sat week to the destination Google Sheet

Destination columns (Google Form response sheet):
  Timestamp | Email Address | Studio | Start date of spend |
  End date of spend | Spend Category | Amount |
  Main SWEAT440 contact email | Notes

Re-running the script replaces all previously migrated rows
(identified by Notes = MIGRATION_TAG) without touching form responses.

Usage:
  python migrate_grassroots_spend.py [--dry-run]
"""

import argparse
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

# ── Config ────────────────────────────────────────────────────────────────────
SOURCE_SHEET_ID = "1Ku0VSwOY6HVXuqucduWlsbKIiNzb0ojL21rozaPpHHU"
DEST_SHEET_ID   = "1oLvrjTfcGUmt-mi1bp2xnqmESgt4uu4QlLCfAOKoRd8"
SOURCE_TAB      = "Weekly Events & Spend"
MIGRATION_TAG   = "migrated:grassroots"   # marker in Notes column to identify our rows

DEST_HEADER = [
    "Timestamp", "Email Address", "Studio",
    "Start date of spend", "End date of spend",
    "Spend Category", "Amount",
    "Main SWEAT440 contact email", "Notes",
]

CREDS_PATH = Path(__file__).parent.parent / "credentials" / "service_account.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Destination column positions (0-based after the header row)
COL_TIMESTAMP   = 0
COL_EMAIL       = 1
COL_STUDIO      = 2
COL_START       = 3
COL_END         = 4
COL_CATEGORY    = 5
COL_AMOUNT      = 6
COL_CONTACT     = 7
COL_NOTES       = 8


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_money(val: str) -> float:
    if not val:
        return 0.0
    cleaned = re.sub(r"[$,\s]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date(val: str) -> date | None:
    if not val:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            continue
    return None


def week_sunday(d: date) -> date:
    """Return the Sunday that opens the Sun-Sat week containing d."""
    # weekday(): Mon=0 … Sun=6  →  days-since-Sunday = (weekday+1) % 7
    return d - timedelta(days=(d.weekday() + 1) % 7)


# ── Read source ───────────────────────────────────────────────────────────────

def read_source(gc: gspread.Client) -> list[tuple[str, date, int, float]]:
    """
    Returns a list of (studio_name, week1_start, week_number, spend).
    Only rows where metric == 'Grassroots Spend ($)' and spend > 0.
    """
    ws   = gc.open_by_key(SOURCE_SHEET_ID).worksheet(SOURCE_TAB)
    rows = ws.get_all_values()

    # Find the header row: must have "metric" in col 2 AND actual week names in col 3+
    header_idx = None
    for i, row in enumerate(rows):
        if len(row) > 3 and row[2].strip().lower() == "metric" and re.match(r"week\s+\d+", row[3].strip(), re.IGNORECASE):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Header row not found in source sheet")

    header = rows[header_idx]

    # Map week number → column index
    week_cols: dict[int, int] = {}
    for j, cell in enumerate(header):
        m = re.match(r"Week\s+(\d+)$", cell.strip(), re.IGNORECASE)
        if m:
            week_cols[int(m.group(1))] = j

    if not week_cols:
        raise RuntimeError("No 'Week N' columns found in header")

    results = []
    current_studio  = None
    current_w1_start = None

    for row in rows[header_idx + 1:]:
        # Pad short rows
        row = row + [""] * (max(week_cols.values()) + 2 - len(row))

        studio_cell = row[0].strip()
        date_cell   = row[1].strip()
        metric_cell = row[2].strip()

        # Update running context (source rows have merged cells: only
        # first row of each studio group has a non-empty name/date).
        if studio_cell:
            current_studio = studio_cell
        if date_cell:
            parsed = parse_date(date_cell)
            if parsed:
                current_w1_start = parsed

        if metric_cell == "Grassroots Spend ($)" and current_studio and current_w1_start:
            for wk_num, col_idx in sorted(week_cols.items()):
                spend = parse_money(row[col_idx]) if col_idx < len(row) else 0.0
                if spend > 0:
                    results.append((current_studio, current_w1_start, wk_num, spend))

    return results


# ── Compute Sun-Sat buckets ───────────────────────────────────────────────────

def build_sunsat_rows(
    source_data: list[tuple[str, date, int, float]]
) -> list[tuple[str, date, date, str, float]]:
    """
    Returns list of (studio, sun, sat, category, amount).
    Weeks with total == 0 are dropped.
    """
    # daily_spend[studio][calendar_date] += daily_rate
    daily: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))

    for studio, w1_start, wk_num, spend in source_data:
        # Week N starts on the Monday that is (N-1) weeks after w1_start
        week_monday = w1_start + timedelta(weeks=wk_num - 1)
        daily_rate  = spend / 7
        for offset in range(7):
            day = week_monday + timedelta(days=offset)
            daily[studio][day] += daily_rate

    # Aggregate into Sun-Sat weeks
    output = []
    for studio, day_map in daily.items():
        bucket: dict[date, float] = defaultdict(float)
        for day, amt in day_map.items():
            bucket[week_sunday(day)] += amt

        for sun, total in sorted(bucket.items()):
            if total > 0.005:  # drop floating-point dust
                sat = sun + timedelta(days=6)
                output.append((studio, sun, sat, "Grassroots", round(total, 2)))

    output.sort(key=lambda x: (x[0], x[1]))
    return output


# ── Write destination ─────────────────────────────────────────────────────────

def write_destination(
    gc: gspread.Client,
    rows_to_write: list[tuple[str, date, date, str, float]],
    dry_run: bool,
) -> None:
    dest_ws = gc.open_by_key(DEST_SHEET_ID).get_worksheet(0)
    existing = dest_ws.get_all_values()

    header = existing[0] if existing else []
    data_rows = existing[1:] if len(existing) > 1 else []

    # Remove previously migrated rows (identified by MIGRATION_TAG in Notes col)
    rows_to_keep = [
        row for row in data_rows
        if len(row) <= COL_NOTES or row[COL_NOTES].strip() != MIGRATION_TAG
    ]

    migrated_removed = len(data_rows) - len(rows_to_keep)
    print(f"  Existing rows: {len(data_rows)}  |  Previously migrated (to replace): {migrated_removed}")

    new_rows = []
    for studio, sun, sat, category, amount in rows_to_write:
        new_rows.append([
            "",                          # Timestamp
            "",                          # Email Address
            studio,                      # Studio
            sun.strftime("%m/%d/%Y"),    # Start date of spend
            sat.strftime("%m/%d/%Y"),    # End date of spend
            category,                    # Spend Category
            amount,                      # Amount
            "",                          # Main SWEAT440 contact email
            MIGRATION_TAG,               # Notes — marker for idempotent re-runs
        ])

    print(f"  New migrated rows to write: {len(new_rows)}")

    if dry_run:
        print("\n[DRY RUN — nothing written]\n")
        for r in new_rows[:10]:
            print(f"  {r[COL_STUDIO]:<28} {r[COL_START]} – {r[COL_END]}  ${r[COL_AMOUNT]:>8.2f}")
        if len(new_rows) > 10:
            print(f"  … and {len(new_rows) - 10} more rows")
        return

    # Rewrite: clear all data rows, then write kept rows + new migrated rows
    # Using batch_clear + update is safer than delete_rows loops.
    all_rows = rows_to_keep + new_rows

    # Write header explicitly (restores it if clear ever wiped it)
    dest_ws.update(values=[DEST_HEADER], range_name="A1:I1")

    # Clear only the data area (row 2 onward), then re-append
    last_row = max(len(existing) + 1, 2)
    dest_ws.batch_clear([f"A2:I{last_row}"])
    if all_rows:
        dest_ws.append_rows(all_rows, value_input_option="USER_ENTERED")

    print(f"\nDone. Wrote {len(new_rows)} migrated rows "
          f"({len(rows_to_keep)} non-migrated rows preserved).")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without touching the sheet")
    args = parser.parse_args()

    creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
    gc    = gspread.authorize(creds)

    print("Reading source sheet (Weekly Events & Spend) …")
    source_data = read_source(gc)
    print(f"  Found {len(source_data)} non-zero Grassroots Spend entries across "
          f"{len({s for s, *_ in source_data})} studio(s)\n")

    print("Computing Sunday-Saturday buckets …")
    output_rows = build_sunsat_rows(source_data)
    print(f"  {len(output_rows)} Sun-Sat week rows generated\n")

    # Summary by studio
    by_studio: dict[str, float] = defaultdict(float)
    for studio, _, __, ___, amt in output_rows:
        by_studio[studio] += amt
    for studio, total in sorted(by_studio.items()):
        print(f"  {studio:<28}  ${total:>8.2f}")

    print(f"\nWriting to destination sheet …")
    write_destination(gc, output_rows, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
