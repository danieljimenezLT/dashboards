#!/usr/bin/env python3
"""
fetch_brand_offenses.py

Reads the "Offenses record" tab of the Brand Offenses Tracker sheet and
writes brand_offenses.json, grouped by studio name and month, in the shape
build_audit_data.py merges into each studio's offBrandPosts list.

Columns (as of 2026-09): Date | Studio | Type of offense | Escalated |
Penalized | Message | Link to Creative | Timestamp | Studio follow-up
action taken. Looked up by header name (not position) so a reordered or
added column doesn't silently break this script.

Also reads the optional "Collab posts" tab — one row per collab post a
studio was invited to by an account outside SWEAT440 (Date | Studio | Link to
post). The Instagram API can't see those (it only returns posts the studio
owns), so they're entered by hand and added to the studio's post count by
build_audit_data.py. Collabs between SWEAT440 studios are detected
automatically and don't need a row (a duplicate row is ignored anyway).

Run once a month, ahead of build_audit_data.py — not on the daily refresh.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

SHEET_ID   = "13u_-vs8SOQgQBvIt1Oo5rX1wLlNLTNYDp5zyAFWhsXg"
WORKSHEET  = "Offenses record"
COLLABS_WORKSHEET = "Collab posts"
OUTPUT     = Path(__file__).parent.parent / "data" / "brand_offenses.json"
CREDS_PATH = Path(__file__).parent.parent.parent / "nso-dashboard" / "credentials" / "service_account.json"
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _col(header, name, required=True):
    try:
        return header.index(name)
    except ValueError:
        if required:
            print(f"ERROR: expected column '{name}' not found in sheet header: {header}", file=sys.stderr)
            sys.exit(1)
        return None


def main():
    creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SHEET_ID).worksheet(WORKSHEET)
    rows = ws.get_all_values()

    if not rows:
        print("ERROR: Offenses record sheet is empty")
        sys.exit(1)

    header = rows[0]
    c_date    = _col(header, "Date")
    c_studio  = _col(header, "Studio")
    c_type    = _col(header, "Type of offense")
    c_esc     = _col(header, "Escalated")
    c_pen     = _col(header, "Penalized")
    c_msg     = _col(header, "Message")
    c_link    = _col(header, "Link to Creative")
    c_action  = _col(header, "Studio follow-up action taken", required=False)

    def cell(row, idx):
        return row[idx].strip() if idx is not None and idx < len(row) else ""

    by_studio_month = defaultdict(lambda: defaultdict(list))
    skipped = 0

    for row in rows[1:]:
        if not row or not cell(row, c_date):
            continue
        date = cell(row, c_date)
        if len(date) < 7 or date[4] != "-":
            skipped += 1
            continue
        month_key = date[:7]  # "YYYY-MM"
        studio = cell(row, c_studio)
        if not studio:
            skipped += 1
            continue

        escalated = cell(row, c_esc).lower() == "yes"
        penalized = cell(row, c_pen).lower() == "yes"
        impact = "Penalized" if penalized else ("Escalated" if escalated else "Flagged")

        by_studio_month[studio][month_key].append({
            "date": date,
            "type": cell(row, c_type) or "Other",
            "impact": impact,
            "feedback": cell(row, c_msg),
            "action": cell(row, c_action) or "—",
            "link": cell(row, c_link),
        })

    out = {studio: dict(months) for studio, months in by_studio_month.items()}
    total_rows = sum(len(v) for months in out.values() for v in months.values())

    collabs = read_collab_posts(gc.open_by_key(SHEET_ID))

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump({"studios": out, "collabs": collabs}, f, indent=2, ensure_ascii=False)

    print(f"Studios with offenses: {len(out)}")
    print(f"Total offense rows:    {total_rows}")
    if skipped:
        print(f"Skipped rows (bad date/missing studio): {skipped}")
    print(f"Written to {OUTPUT}")


def read_collab_posts(sheet):
    """{studio: {"YYYY-MM": [{date, link}]}} from the "Collab posts" tab, or {}
    if the tab doesn't exist yet."""
    try:
        rows = sheet.worksheet(COLLABS_WORKSHEET).get_all_values()
    except gspread.WorksheetNotFound:
        print(f"Note: no '{COLLABS_WORKSHEET}' tab yet — no manual collab posts.")
        return {}
    if not rows:
        return {}
    header = rows[0]
    c_date, c_studio, c_link = (_col(header, n) for n in ("Date", "Studio", "Link to post"))

    out = defaultdict(lambda: defaultdict(list))
    for row in rows[1:]:
        get = lambda i: row[i].strip() if i < len(row) else ""
        date, studio, link = get(c_date), get(c_studio), get(c_link)
        if len(date) < 7 or date[4] != "-" or not studio or not link:
            continue
        out[studio][date[:7]].append({"date": date, "link": link})
    total = sum(len(v) for m in out.values() for v in m.values())
    print(f"Manual collab posts:   {total}")
    return {studio: dict(months) for studio, months in out.items()}


if __name__ == "__main__":
    main()
