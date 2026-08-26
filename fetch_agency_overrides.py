"""
fetch_agency_overrides.py
Reads the "Marketing Spend Adjustments" Google Sheet and writes
agency-spend-overrides.json — the source data behind the OS Dashboard's
Leadteam Discount / External Agency spend categories (see index-test.html).

Sheet: https://docs.google.com/spreadsheets/d/1oLvrjTfcGUmt-mi1bp2xnqmESgt4uu4QlLCfAOKoRd8
Columns (any tab, header row required): Studio | Start date | End date |
Spend Category | Monthly amount | Main SWEAT440 contact email | Notes
"Spend Category" must be "Leadteam Discount" or "External agency" (case-
insensitive) — other rows are skipped.

Uses the same service account as fetch_sheet_milestones.py. Run from the
repo root once credentials/service_account.json exists locally (or is
written by CI, same as refresh.yml does for nso-dashboard/):

    python fetch_agency_overrides.py

NOT wired into refresh.yml yet — run manually until this feature is
approved to go live, same as fetch_sheet_milestones.py today.
"""
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1oLvrjTfcGUmt-mi1bp2xnqmESgt4uu4QlLCfAOKoRd8"
TARGET_GID     = 1990620350  # "Leadteam Discount / External Agency" tab
CREDS_FILE     = "nso-dashboard/credentials/service_account.json"
STUDIOS_FILE   = "studios.json"
OUT_FILE       = "agency-spend-overrides.json"

CATEGORY_MAP = {
    "leadteam discount": "leadteam_discount",
    "external agency":   "external_agency",
}


def norm_date(s):
    """'8/15/2026' or '2026-08-15' -> '2026-08-15'."""
    s = (s or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def norm_amount(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return round(float(re.sub(r"[^0-9.]", "", s)), 2)
    except ValueError:
        return None


def resolve_studio(raw_name, known_studios):
    """Sheet studio names are sometimes short ('Uptown') vs. the full name
    used in data.json / studios.json ('Dallas - Uptown')."""
    name = raw_name.strip()
    if name in known_studios:
        return name
    lower = name.lower()
    for s in known_studios:
        sl = s.lower()
        if sl == lower or sl.endswith(lower) or sl.startswith(lower):
            return s
    return name  # fall back to the raw sheet name; flagged for review below


def find_tab_title(svc, spreadsheet_id, gid):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == gid:
            return props.get("title")
    raise RuntimeError(f"No tab with gid={gid} found in spreadsheet {spreadsheet_id}")


def run():
    repo_root = Path(__file__).resolve().parent
    studios_raw = json.loads((repo_root / STUDIOS_FILE).read_text(encoding="utf-8"))
    studios_list = studios_raw.get("studios", studios_raw) if isinstance(studios_raw, dict) else studios_raw
    known_studios = [s["name"] for s in studios_list if s.get("name")]

    creds = service_account.Credentials.from_service_account_file(
        repo_root / CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    svc = build("sheets", "v4", credentials=creds)

    tab_title = find_tab_title(svc, SPREADSHEET_ID, TARGET_GID)
    print(f"Reading tab '{tab_title}'...")
    res = svc.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=tab_title).execute()
    rows = res.get("values", [])
    if not rows:
        print("No rows found — nothing written.")
        return

    header = [h.strip().lower() for h in rows[0]]

    def col(name):
        return header.index(name) if name in header else -1

    c_studio = col("studio")
    c_start  = col("start date")
    c_end    = col("end date")
    c_cat    = col("spend category")
    c_amount = col("monthly amount")

    overrides = []
    unresolved = set()
    for row in rows[1:]:
        def cell(i):
            return row[i] if 0 <= i < len(row) else ""

        raw_studio = cell(c_studio).strip()
        raw_cat    = cell(c_cat).strip().lower()
        if not raw_studio or raw_cat not in CATEGORY_MAP:
            continue

        start = norm_date(cell(c_start))
        end   = norm_date(cell(c_end))
        amount = norm_amount(cell(c_amount))
        if not (start and end and amount is not None):
            continue

        studio = resolve_studio(raw_studio, known_studios)
        if studio not in known_studios:
            unresolved.add(raw_studio)

        overrides.append({
            "studio":         studio,
            "start_date":     start,
            "end_date":       end,
            "category":       CATEGORY_MAP[raw_cat],
            "monthly_amount": amount,
        })

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid={TARGET_GID}",
        "overrides": overrides,
    }
    (repo_root / OUT_FILE).write_text(json.dumps(output, indent=2), encoding="utf-8")

    print(f"OK Wrote {OUT_FILE} — {len(overrides)} override row(s)")
    if unresolved:
        print(f"   WARNING: could not match these sheet studio names to studios.json: {sorted(unresolved)}")


if __name__ == "__main__":
    run()
