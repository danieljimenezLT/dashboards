"""
migrate_herriman_events_spend.py

One-time script: writes Herriman's grassroots spend + community events
into the NSO Config "Weekly Events & Spend" tab.

Requires the service account to have EDITOR access on the NSO Config sheet.
Run once after granting access:
    python scripts/migrate_herriman_events_spend.py
"""
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

ROOT = Path(__file__).parent.parent
NSO_CONFIG_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Ku0VSwOY6HVXuqucduWlsbKIiNzb0ojL21rozaPpHHU"

# Herriman data (from Running Promotional Spend + Presales Event Tracker sheets)
# Spend keyed by calendar week number
HERRIMAN_SPEND = {
    13: 347.89,
    17: 100.00,
}

# Events: calendar week -> count of events that week
HERRIMAN_EVENTS = {
    16: 1, 17: 8, 18: 7, 19: 10, 20: 10,
    21: 8, 22: 9, 23: 2, 24: 8,  25: 4,
    26: 5, 29: 3, 30: 2, 31: 1,  32: 1,
    33: 1, 34: 1, 38: 1, 39: 1,  41: 2,
    43: 1, 49: 1,
}

creds = Credentials.from_service_account_file(
    str(ROOT / "credentials" / "service_account.json"),
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)

sh = gc.open_by_url(NSO_CONFIG_SHEET_URL)
ws = sh.worksheet("Weekly Events & Spend")
all_rows = ws.get_all_values()

# Find Herriman rows (1-based for gspread)
herriman_events_row = herriman_spend_row = None
for i, row in enumerate(all_rows):
    name = str(row[0]).strip().lower() if row[0] else ""
    metric = str(row[1]).strip().lower() if len(row) > 1 else ""
    if name == "herriman":
        if "events" in metric or "#" in metric:
            herriman_events_row = i + 1
        elif "grassroots" in metric or "$" in metric:
            herriman_spend_row = i + 1

# Fallback: events=first herriman row, spend=events+1
if herriman_events_row and not herriman_spend_row:
    herriman_spend_row = herriman_events_row + 1

print(f"Herriman events row: {herriman_events_row}, spend row: {herriman_spend_row}")
assert herriman_events_row and herriman_spend_row, "Could not find Herriman rows"

# Column mapping: col 0=Name, col 1=Metric, col 2=Week 1 ... col N+1 = Week N
# gspread col is 1-based so Week N = column N+2
updates = []
for wk, cnt in HERRIMAN_EVENTS.items():
    if 1 <= wk <= 52:
        cell = gspread.utils.rowcol_to_a1(herriman_events_row, wk + 2)
        updates.append({"range": cell, "values": [[str(cnt)]]})

for wk, amt in HERRIMAN_SPEND.items():
    if 1 <= wk <= 52:
        cell = gspread.utils.rowcol_to_a1(herriman_spend_row, wk + 2)
        updates.append({"range": cell, "values": [["$" + f"{amt:.0f}"]]})

print(f"Writing {len(updates)} cells to NSO Config sheet...")
ws.batch_update(updates)
print("Done! Herriman data written to NSO Config Weekly Events & Spend tab.")
