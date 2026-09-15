"""
fetch_other_costs.py
Reads the NSO Other Costs Google Sheet and outputs nso_other_costs.json.

Sheet structure:
  Tab 1 (ad-hoc entries, Google Form responses):
    Timestamp | Email | Studio | Start date of spend | End date of spend
    | Spend Category | Amount | Contact | Notes

  Tab 2 ("Monthly fees and discounts"):
    Studio | Start date | End date | Spend Category | Monthly amount | Contact | Notes

Run from nso-dashboard/:
    python scripts/fetch_other_costs.py
"""
import json, re
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1oLvrjTfcGUmt-mi1bp2xnqmESgt4uu4QlLCfAOKoRd8"
CREDS_FILE     = "credentials/service_account.json"
OUTPUT_FILE    = "nso_other_costs.json"

# Short name (lower) → full NSO studio name
STUDIO_MAP = {
    "herriman":                 "SWEAT440 Herriman",
    "naples - mercato":         "SWEAT440 Naples - Mercato",
    "naples":                   "SWEAT440 Naples - Mercato",
    "dallas - prestonwood":     "SWEAT440 Dallas - Prestonwood",
    "prestonwood":              "SWEAT440 Dallas - Prestonwood",
    "pinecrest":                "SWEAT440 Pinecrest - Palmetto Bay",
    "pinecrest - palmetto bay": "SWEAT440 Pinecrest - Palmetto Bay",
    "reston":                   "SWEAT440 Reston",
    "uptown":                   "SWEAT440 Dallas - Uptown",
    "dallas uptown":            "SWEAT440 Dallas - Uptown",
    "dallas - uptown":          "SWEAT440 Dallas - Uptown",
    "dr phillips":              "SWEAT440 Orlando - Dr Phillips",
    "orlando - dr phillips":    "SWEAT440 Orlando - Dr Phillips",
    "fort myers":               "SWEAT440 Fort Myers",
    "north miami":              "SWEAT440 North Miami",
    "aventura":                 "SWEAT440 Aventura",
    "dunwoody":                 "SWEAT440 Dunwoody",
    "old bridge":               "SWEAT440 Old Bridge",
    "middletown":               "SWEAT440 Middletown",
    "long beach":               "SWEAT440 Long Beach",
    "okc - rose creek":         "SWEAT440 OKC - Rose Creek",
    "charlotte - south end":    "SWEAT440 Charlotte - South End",
    "burlington":               "SWEAT440 Burlington",
}


def norm_date(s):
    if not s or str(s).strip().lower() in ("", "n/a", "tbd", "-"):
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def norm_studio(s):
    return STUDIO_MAP.get(str(s).strip().lower())


def norm_float(s):
    if not s or str(s).strip().lower() in ("", "n/a", "-"):
        return None
    try:
        return float(re.sub(r"[^0-9.]", "", str(s).strip()))
    except (ValueError, TypeError):
        return None


def cell(row, idx):
    return row[idx] if idx < len(row) else ""


def find_col(hdr, *keywords):
    for i, h in enumerate(hdr):
        h_lower = h.lower()
        if any(kw in h_lower for kw in keywords):
            return i
    return None


# ── Connect ──────────────────────────────────────────────────────────────────
print("Connecting to Google Sheets API...")
creds = service_account.Credentials.from_service_account_file(
    CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
)
svc = build("sheets", "v4", credentials=creds)

meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
sheet_titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
print(f"  Tabs found: {sheet_titles}")


# ── Ad-hoc tab (Form responses) ───────────────────────────────────────────────
adhoc_tab = next(
    (t for t in sheet_titles if any(kw in t.lower() for kw in ("form", "response", "spend", "ad-hoc", "adhoc"))),
    sheet_titles[0]
)
print(f"Reading ad-hoc tab: '{adhoc_tab}'")
res = svc.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=adhoc_tab).execute()
raw_rows = res.get("values", [])

# Find header row — first row that contains "studio"
hdr_idx = next((i for i, r in enumerate(raw_rows) if any("studio" in str(c).lower() for c in r)), 0)
hdr = [str(c).strip().lower() for c in raw_rows[hdr_idx]]

ci_studio = find_col(hdr, "studio")
ci_start  = find_col(hdr, "start date", "start")
ci_end    = find_col(hdr, "end date", "end_date")
ci_cat    = find_col(hdr, "category", "spend cat")
ci_amt    = find_col(hdr, "amount")

adhoc_entries = []
for row in raw_rows[hdr_idx + 1:]:
    if not any(c.strip() for c in row if c):
        continue
    studio_full = norm_studio(cell(row, ci_studio))
    if not studio_full:
        continue
    start = norm_date(cell(row, ci_start))
    end   = norm_date(cell(row, ci_end))
    cat   = str(cell(row, ci_cat)).strip()
    amt   = norm_float(cell(row, ci_amt))
    if not start or not end or not cat or amt is None:
        continue
    s_dt = datetime.strptime(start, "%Y-%m-%d")
    e_dt = datetime.strptime(end,   "%Y-%m-%d")
    span = max(1, (e_dt - s_dt).days + 1)
    adhoc_entries.append({
        "studio_key": studio_full,
        "start":      start,
        "end":        end,
        "category":   cat,
        "total":      round(amt, 2),
        "daily_rate": round(amt / span, 6),
    })

print(f"  {len(adhoc_entries)} ad-hoc entries loaded")


# ── Monthly fees tab ──────────────────────────────────────────────────────────
monthly_tab = next(
    (t for t in sheet_titles if any(kw in t.lower() for kw in ("monthly", "discount", "fee"))),
    sheet_titles[1] if len(sheet_titles) > 1 else None
)

monthly_entries = []
if monthly_tab:
    print(f"Reading monthly tab: '{monthly_tab}'")
    res2 = svc.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=monthly_tab).execute()
    raw_rows2 = res2.get("values", [])

    hdr2_idx = next((i for i, r in enumerate(raw_rows2) if any("studio" in str(c).lower() for c in r)), 0)
    hdr2 = [str(c).strip().lower() for c in raw_rows2[hdr2_idx]]

    ci2_studio  = find_col(hdr2, "studio")
    ci2_start   = find_col(hdr2, "start date", "start")
    ci2_end     = find_col(hdr2, "end date", "end_date")
    ci2_cat     = find_col(hdr2, "category", "spend")
    ci2_monthly = find_col(hdr2, "monthly")

    for row in raw_rows2[hdr2_idx + 1:]:
        if not any(c.strip() for c in row if c):
            continue
        studio_full = norm_studio(cell(row, ci2_studio))
        if not studio_full:
            continue
        start   = norm_date(cell(row, ci2_start))
        end     = norm_date(cell(row, ci2_end))
        cat     = str(cell(row, ci2_cat)).strip()
        monthly = norm_float(cell(row, ci2_monthly))
        if not start or not end or not cat or monthly is None:
            continue
        monthly_entries.append({
            "studio_key": studio_full,
            "start":      start,
            "end":        end,
            "category":   cat,
            "monthly":    round(monthly, 2),
        })

    print(f"  {len(monthly_entries)} monthly entries loaded")
else:
    print("  Monthly fees tab not found — skipping")


# ── Write output ──────────────────────────────────────────────────────────────
out = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "adhoc":        adhoc_entries,
    "monthly":      monthly_entries,
}
with open(OUTPUT_FILE, "w") as f:
    json.dump(out, f, indent=2)
print(f"Wrote {OUTPUT_FILE} — {len(adhoc_entries)} adhoc, {len(monthly_entries)} monthly entries")
