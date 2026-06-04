"""
fetch_sheet_milestones.py
Reads scheduling dates AND tier pricing from the NSO Config Google Sheet,
then patches nso_scorecard_data.json with:
  - milestones: pre-launch date cards
  - pricing:    tier prices + week numbers when each tier starts

Run from the nso-dashboard/ folder:
    python scripts/fetch_sheet_milestones.py
"""
import json, re, math
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1Ku0VSwOY6HVXuqucduWlsbKIiNzb0ojL21rozaPpHHU"
CREDS_FILE     = "credentials/service_account.json"
SCORECARD_FILE = "nso_scorecard_data.json"

# Column indices (0-based). Row 0 = group headers, Row 1 = column headers, Row 2+ = data.
COL = {
    "name":                  0,
    "code":                  1,
    "tier1_price":           3,
    "tier2_price":           4,
    "tier3_price":           5,
    "early_lead_date":       6,
    "mbo_date":              7,
    "paid_lead_gen_date":    8,
    "week1_start":           9,
    "target_co_date":        10,
    "target_open_date":      11,
    "tier2_move_date":       12,
    "tier3_move_date":       13,
    "estimated_roms_target": 17,
    "tier1_members_target":  21,
    "tier2_members_target":  22,
    "tier3_members_target":  23,
}

MILESTONE_LABELS = {
    "early_lead_date":    "Early Lead Date",
    "mbo_date":           "MBO Date",
    "paid_lead_gen_date": "Paid Lead Gen Start",
}

MILESTONE_SUBTITLES = {
    "early_lead_date":    "Landing page & early interest list",
    "mbo_date":           "Lead form live on website",
    "paid_lead_gen_date": "Meta / Google paid ads begin",
}


def norm_date(s):
    """M/D/YYYY or YYYY-MM-DD → YYYY-MM-DD. Returns None if blank/n/a."""
    if not s or s.strip().lower() in ("", "n/a", "tbd", "-"):
        return None
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def norm_price(s):
    """'$99' or '99' → 99 (int). Returns None if blank/n/a."""
    if not s or s.strip().lower() in ("", "n/a", "-"):
        return None
    try:
        return int(re.sub(r"[^0-9]", "", s.strip()))
    except (ValueError, TypeError):
        return None


def cell(row, col):
    return row[col] if col < len(row) else ""


def date_to_week_num(move_date_str, week1_start_str):
    """
    'Week 1 Start' from the sheet is treated as Week 0 (the base).
    Week numbers count forward from there: ceil(days / 7).
    e.g. 22 days after Week 1 Start → ceil(22/7) = 4
    """
    if not move_date_str or not week1_start_str:
        return None
    try:
        base = datetime.strptime(week1_start_str, "%Y-%m-%d")
        move = datetime.strptime(move_date_str,   "%Y-%m-%d")
        days = (move - base).days
        if days <= 0:
            return None
        return math.ceil(days / 7)
    except ValueError:
        return None


# ── Read Google Sheet ────────────────────────────────────────────────────────
print("Reading NSO Config sheet...")
creds = service_account.Credentials.from_service_account_file(
    CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
)
svc  = build("sheets", "v4", credentials=creds)
res  = svc.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range="NSO Config"
).execute()
rows = res.get("values", [])
data_rows = rows[2:]  # skip group-header row and column-header row

# Build lookup: code -> {milestones, pricing_raw}
sheet_map = {}
for row in data_rows:
    code = cell(row, COL["code"]).strip()
    if not code:
        continue

    # Milestones
    milestones = []
    for key in ("early_lead_date", "mbo_date", "paid_lead_gen_date"):
        d = norm_date(cell(row, COL[key]))
        if d:
            milestones.append({
                "key":      key,
                "label":    MILESTONE_LABELS[key],
                "subtitle": MILESTONE_SUBTITLES[key],
                "date":     d,
            })
    milestones.sort(key=lambda m: m["date"])

    # Pricing (prices + move dates — week numbers resolved later against scorecard)
    week1_start = norm_date(cell(row, COL["week1_start"]))  # treated as Week 0 base
    t2_move     = norm_date(cell(row, COL["tier2_move_date"]))
    t3_move     = norm_date(cell(row, COL["tier3_move_date"]))

    sheet_map[code] = {
        "milestones":            milestones,
        "tier1_price":           norm_price(cell(row, COL["tier1_price"])),
        "tier2_price":           norm_price(cell(row, COL["tier2_price"])),
        "tier3_price":           norm_price(cell(row, COL["tier3_price"])),
        "week1_start":           week1_start,
        "tier2_start_week":      date_to_week_num(t2_move, week1_start),
        "tier3_start_week":      date_to_week_num(t3_move, week1_start),
        "tier2_start_date":      t2_move,
        "tier3_start_date":      t3_move,
        "tier1_members_target":  norm_price(cell(row, COL["tier1_members_target"])),
        "tier2_members_target":  norm_price(cell(row, COL["tier2_members_target"])),
        "tier3_members_target":  norm_price(cell(row, COL["tier3_members_target"])),
        "estimated_roms_target": (cell(row, COL["estimated_roms_target"]) or "").strip() or None,
    }

# ── Patch scorecard JSON ────────────────────────────────────────────────────
with open(SCORECARD_FILE) as f:
    sc = json.load(f)

updated = 0
for studio in sc.get("studios", []):
    code = studio.get("code", "")
    info = sheet_map.get(code)
    if not info:
        studio.setdefault("milestones", [])
        continue

    studio["milestones"] = info["milestones"]

    # Patch Week 1 date_start to match "Week 1 Start" from sheet
    w1_start = info.get("week1_start")
    if w1_start:
        for wk in studio.get("weeks", []):
            label = re.sub(r"[^0-9]", "", wk.get("week", ""))
            if label == "1":
                if wk.get("date_start") != w1_start:
                    print(f"    fixing W1 date_start: {wk.get('date_start')} -> {w1_start}")
                    wk["date_start"] = w1_start
                break

    pricing = {}
    for key in ("tier1_price", "tier2_price", "tier3_price",
                "tier2_start_week", "tier3_start_week",
                "tier2_start_date", "tier3_start_date",
                "tier1_members_target", "tier2_members_target", "tier3_members_target",
                "estimated_roms_target"):
        if info.get(key) is not None:
            pricing[key] = info[key]

    studio["pricing"] = pricing if pricing else None

    updated += 1
    print(f"  {code} ({studio['name']}): "
          f"pricing={pricing} | "
          f"{len(info['milestones'])} milestone(s)")

with open(SCORECARD_FILE, "w") as f:
    json.dump(sc, f, indent=2)

print(f"\nDone. {SCORECARD_FILE} updated — {updated} studio(s) patched")
