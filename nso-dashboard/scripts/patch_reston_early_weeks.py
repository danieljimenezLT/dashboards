"""
patch_reston_early_weeks.py
Reads Reston's Google Sheet (WEEKLY SCORECARD + TOTAL PRESALES SCORECARD) and:
  1. Relabels scorecard weeks to match the sheet's non-consecutive numbering
     (Reston has pause weeks every 5 active weeks: W13, W18, W23, W28, W33...)
  2. Patches all available KPIs from the sheet (leads, IG, events, spend, tiers, CPL/CPA, RMR)
  3. Fixes co_week / current_week to use the sheet's week numbers

Run from nso-dashboard/:
    python scripts/patch_reston_early_weeks.py
"""
import calendar, json, re
from datetime import date, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

LEADTEAM_MONTHLY = 1200.0


def calc_leadteam_fee(date_start, date_end):
    """$1,200/month prorated daily."""
    if date_start is None or date_end is None:
        return None
    total = 0.0
    d = date_start
    while d <= date_end:
        total += LEADTEAM_MONTHLY / calendar.monthrange(d.year, d.month)[1]
        d += timedelta(days=1)
    return round(total, 2)

SPREADSHEET_ID  = "1xx3C0ZL4N25kSX92Qp9ay3PmlrPcbaFP9ZuBKSNtK1E"
CREDS_FILE      = "credentials/service_account.json"
SCORECARD_FILE  = "nso_scorecard_data.json"
OVERRIDES_FILE  = "nso_spend_overrides.json"
RESTON_CODE     = "VA-001"
DEFAULT_YEAR    = 2026


def safe_float(v, default=None):
    if v is None:
        return default
    s = str(v).strip().replace("$", "").replace(",", "").replace("%", "")
    if s.lower() in ("", "n/a", "no data", "#div/0!"):
        return default
    try:
        return float(s)
    except ValueError:
        return default

def safe_int(v, default=0):
    f = safe_float(v)
    return int(round(f)) if f is not None else default

def cell(rows, row_idx, col_idx):
    if row_idx >= len(rows):
        return None
    row = rows[row_idx]
    return row[col_idx] if col_idx < len(row) else None

def parse_date_range(s):
    if not s:
        return None, None
    s_clean = str(s).strip().split("\n")[0].strip()
    m = re.match(r"[Pp]re[-\s]+(\d+)/(\d+)", s_clean)
    if m:
        mo, dy = int(m.group(1)), int(m.group(2))
        return None, date(2025 if mo >= 10 else DEFAULT_YEAR, mo, dy)
    m = re.match(r"(\d+)/(\d+)\s*[-]\s*(\d+)/(\d+)", s_clean)
    if m:
        sm, sd, em, ed = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
        sy = 2025 if sm >= 10 else DEFAULT_YEAR
        ey = 2025 if em >= 10 else DEFAULT_YEAR
        if em < sm and ey == sy:
            ey += 1
        return date(sy, sm, sd), date(ey, em, ed)
    return None, None


# ── Load Reston meta spend overrides ─────────────────────────────────────────
try:
    with open(OVERRIDES_FILE) as _f:
        _ov_all = json.load(_f)
    _reston_meta_ov = {str(k): float(v) for k, v in _ov_all.get(RESTON_CODE, {}).get("meta_spend", {}).items()}
except FileNotFoundError:
    _reston_meta_ov = {}

# ── Read sheets ───────────────────────────────────────────────────────────────
print("Reading Reston sheets...")
creds = service_account.Credentials.from_service_account_file(
    CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
)
svc = build("sheets", "v4", credentials=creds)

def read_tab(name):
    return svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=name
    ).execute().get("values", [])

weekly_rows = read_tab("WEEKLY SCORECARD")
total_rows  = read_tab("TOTAL PRESALES SCORECARD")

# Build sheet week map from TOTAL PRESALES SCORECARD headers
HEADER_DATES = total_rows[0] if total_rows else []
HEADER_WEEKS = total_rows[1] if len(total_rows) > 1 else []

sheet_cols = []   # list of {col, wn, ds, de, label}
sheet_by_start = {}
sheet_by_end   = {}

for ci in range(2, len(HEADER_DATES)):
    label_raw = str(HEADER_DATES[ci]).strip()
    wn_raw    = str(HEADER_WEEKS[ci]).strip() if ci < len(HEADER_WEEKS) else ""
    ds, de    = parse_date_range(label_raw)
    m         = re.search(r"(\d+)", wn_raw)
    wn        = int(m.group(1)) if m else -1
    if wn < 0:
        continue
    dr_label = label_raw.split("\n")[0].strip()
    entry = {"col": ci, "wn": wn, "ds": ds, "de": de, "label": dr_label}
    sheet_cols.append(entry)
    if ds:
        sheet_by_start[ds.isoformat()] = entry
    if de:
        sheet_by_end[de.isoformat()] = entry

# ── Load scorecard ────────────────────────────────────────────────────────────
with open(SCORECARD_FILE) as f:
    sc = json.load(f)

reston = next((s for s in sc["studios"] if s["code"] == RESTON_CODE), None)
if not reston:
    print("ERROR: Reston not found"); raise SystemExit(1)

weeks = reston["weeks"]

# ── Step 1: Relabel weeks to match sheet numbering ────────────────────────────
print("\nStep 1: Relabeling weeks...")
today_str = date.today().isoformat()
new_current = 0

for wk in weeks:
    ds = wk.get("date_start")
    de = wk.get("date_end")
    entry = None
    if ds and ds in sheet_by_start:
        entry = sheet_by_start[ds]
    elif de and de in sheet_by_end:
        entry = sheet_by_end[de]
    if not entry:
        continue

    old = wk.get("week", "")
    new_wn = entry["wn"]
    new_label = "Week 0" if new_wn == 0 else f"WEEK {new_wn}"
    if old != new_label:
        print(f"  {old:12s} -> {new_label}  ({ds} - {de})")
    wk["week"] = new_label

    # Update date_range label if sheet has a special name
    sh_label = entry["label"]
    if any(x in sh_label for x in ("Transition", "Training", "Target", "Ads")):
        wk["date_range"] = sh_label
    elif entry["ds"] and entry["de"]:
        wk["date_range"] = f"{entry['ds'].month}/{entry['ds'].day} - {entry['de'].month}/{entry['de'].day}"

    if ds and ds <= today_str:
        new_current = new_wn

reston["current_week"] = new_current
print(f"  current_week set to {new_current}")

# Fix co_week: find the week that contains the CO date (2026-02-02 for Reston)
for wk in weeks:
    if wk.get("date_start") == "2026-02-02":
        m = re.search(r"(\d+)", wk.get("week", ""))
        if m:
            reston["co_week"] = int(m.group(1))
            print(f"  co_week fixed to {reston['co_week']}")
        break

# ── Step 2: Weekly KPIs from WEEKLY SCORECARD ─────────────────────────────────
W_LEADS=2; W_EVENTS=9; W_GR_LEADS=10; W_GR_PRESALES=11; W_IG=12
print("\nStep 2: Applying weekly KPIs...")

for entry in sheet_cols:
    ci = entry["col"]
    ds_key = entry["ds"].isoformat() if entry["ds"] else None
    de_key = entry["de"].isoformat() if entry["de"] else None
    wk = next((w for w in weeks if
               (ds_key and w.get("date_start") == ds_key) or
               (de_key and w.get("date_end") == de_key)), None)
    if not wk:
        continue
    nl  = safe_int(cell(weekly_rows, W_LEADS, ci))
    ig  = safe_float(cell(weekly_rows, W_IG, ci))
    ev  = safe_int(cell(weekly_rows, W_EVENTS, ci))
    grl = safe_int(cell(weekly_rows, W_GR_LEADS, ci))
    grp = safe_int(cell(weekly_rows, W_GR_PRESALES, ci))
    if nl  > 0:           wk["new_leads"]           = nl
    if ig  is not None:   wk["ig_new_followers"]     = int(ig)
    if ev  > 0:           wk["comm_events"]          = ev
    if grl > 0:           wk["grassroots_leads"]     = grl
    if grp > 0:           wk["grassroots_presales"]  = grp

# ── Step 3: Cumulative KPIs from TOTAL PRESALES SCORECARD ────────────────────
T_LEADS=2; T_TIER0=10; T_TIER1=11; T_TIER2=12
T_RMR=13; T_DIG=15; T_GR=16; T_OTHER=17; T_TOT=19; T_CPL=20; T_CPA=21
print("\nStep 3: Applying cumulative KPIs + spend...")
prev = {"dig": 0.0, "gr": 0.0, "other": 0.0, "tot": 0.0}

for entry in sorted(sheet_cols, key=lambda e: e["ds"] or date(2099,1,1)):
    ci = entry["col"]
    ds_key = entry["ds"].isoformat() if entry["ds"] else None
    de_key = entry["de"].isoformat() if entry["de"] else None
    wk = next((w for w in weeks if
               (ds_key and w.get("date_start") == ds_key) or
               (de_key and w.get("date_end") == de_key)), None)
    if not wk:
        continue

    tl   = safe_float(cell(total_rows, T_LEADS, ci))
    rmr  = safe_float(cell(total_rows, T_RMR,   ci))
    cpl  = safe_float(cell(total_rows, T_CPL,   ci))
    cpa  = safe_float(cell(total_rows, T_CPA,   ci))
    t0   = safe_float(cell(total_rows, T_TIER0, ci), 0.0)
    t1   = safe_float(cell(total_rows, T_TIER1, ci), 0.0)
    t2   = safe_float(cell(total_rows, T_TIER2, ci), 0.0)

    dig_c = safe_float(cell(total_rows, T_DIG,   ci), prev["dig"])
    gr_c  = safe_float(cell(total_rows, T_GR,    ci), prev["gr"])
    oth_c = safe_float(cell(total_rows, T_OTHER, ci), prev["other"])
    tot_c = safe_float(cell(total_rows, T_TOT,   ci), prev["tot"])

    dig_w = max(0.0, dig_c - prev["dig"])
    gr_w  = max(0.0, gr_c  - prev["gr"])
    tot_w = max(0.0, tot_c - prev["tot"])
    prev  = {"dig": dig_c, "gr": gr_c, "other": oth_c, "tot": tot_c}

    # LeadTeam fee: calculated from date range, not read from sheet
    lt_w = calc_leadteam_fee(
        date.fromisoformat(wk["date_start"]) if wk.get("date_start") else None,
        date.fromisoformat(wk["date_end"])   if wk.get("date_end")   else None,
    ) or 0.0

    if tl   and tl   > 0: wk["total_leads"]        = tl
    # presales_count / cancellations_count: use Snowflake data (nso_sales_data.json)
    # — do NOT override from Google Sheet so both metrics share the same source
    if rmr  and rmr  > 0: wk["estimated_day1_rmr"]  = rmr
    if cpl  and cpl  > 0: wk["blended_cpl"]         = round(cpl, 2)
    if cpa  and cpa  > 0: wk["blended_cpa"]         = round(cpa, 2)
    if t1 > 0 or t0 > 0:
        wk["tier1_members"] = int(t1)
        wk["tier2_members"] = int(t2)
        wk["tier0_members"] = int(t0)
    wn = entry["wn"]
    if tot_w > 0:
        google_api = wk.get("google_spend") or 0.0
        # Use manual meta override if provided; otherwise derive from sheet total − Google API
        meta_ov = _reston_meta_ov.get(str(wn))
        meta_w  = float(meta_ov) if meta_ov is not None else max(0.0, dig_w - google_api)
        total_w = round(meta_w + google_api + gr_w + lt_w, 2)
        wk["meta_spend"]            = round(meta_w, 2)
        wk["google_spend"]          = round(google_api, 2)
        wk["grassroots_spend"]      = round(gr_w, 2)
        wk["leadteam_fee"]          = round(lt_w, 2)
        wk["total_marketing_spend"] = total_w

    print(f"  W{wn:2d}: tl={tl} rmr={rmr} spend={round(tot_w)} t1={int(t1) if t1 else 0}")

# ── Step 3b: Patch weeks absent from TOTAL PRESALES SCORECARD ────────────────
# W0, W1, W4, W5 have no column in the sheet; data sourced from
# "KPI - Running totals" / "KPI - Measured by Period" at GID 1518549438.
_GID_DATA = {
    # wn: (new_leads, total_leads_cum, tier1_cum, tier0_cum, tier2_cum)
    0: (12, 12,  3,  0, 0),
    1: (18, 30,  6,  0, 0),
    4: ( 2, 57, 11,  0, 0),
    5: ( 6, 63, 13,  0, 0),
}
print("\nStep 3b: Patching weeks absent from sheet (W0, W1, W4, W5)...")
for wk in weeks:
    m = re.search(r"\d+", wk.get("week", ""))
    wn = int(m.group(0)) if m else -1
    row = _GID_DATA.get(wn)
    if not row:
        continue
    nl_val, tl_val, t1_val, t0_val, t2_val = row
    if (wk.get("new_leads") or 0) == 0:
        wk["new_leads"] = nl_val
    wk["total_leads"] = float(tl_val)   # always override; sheet is authoritative
    if wk.get("tier1_members") is None:
        ps = wk.get("presales_count") or 0
        wk["tier1_members"] = int(ps) if ps else t1_val
    if wk.get("tier0_members") is None:
        wk["tier0_members"] = t0_val
    if wk.get("tier2_members") is None:
        wk["tier2_members"] = t2_val
    if wk.get("estimated_day1_rmr") is None:
        t1 = wk.get("tier1_members") or 0
        t0 = wk.get("tier0_members") or 0
        t2 = wk.get("tier2_members") or 0
        wk["estimated_day1_rmr"] = round(t1 * 129.0 + t0 * 99.0 + t2 * 149.0, 2) or None
    print(f"  W{wn}: tl={wk['total_leads']} ps={wk.get('presales_count')} t1={wk['tier1_members']} nl={wk['new_leads']}")

# ── Step 4: Recalculate cumulative total_leads from new_leads ─────────────────
# Always-overwrite so stale values from previous runs don't persist.
# Step 3b above ensures W0/W1/W4/W5 new_leads are correct before this runs.
print("\nStep 4: Recalculating cumulative total_leads...")
cum = 0.0
for wk in weeks:
    nl = wk.get("new_leads") or 0
    cum += nl
    if cum > 0:
        wk["total_leads"] = cum
print(f"  Final: {cum} leads")

# Ensure tier0_price (founders rate) is set for Reston so the dashboard
# shows Founders separately from Tier 1 in the RMR Breakdown.
if reston.get("pricing") is not None:
    reston["pricing"]["tier0_price"] = 99

# ── Step 5: IG gap → Week 0 ───────────────────────────────────────────────────
# The Meta Insights API only provides 30 days of daily follower_count history.
# Followers gained before that window are not captured per-week.
# Compute the gap (current_followers - sum of all weekly ig) and add to Week 0
# so the cumulative total equals the account's actual follower count.
print("\nStep 5: Calibrate Week 0 IG so cumulative total = current_followers...")
try:
    with open("social_insights.json") as _f:
        _social = json.load(_f)
    _cf = next(
        (ig.get("current_followers") for ig in _social.get("instagram", [])
         if ig.get("code") == "reston"),
        None
    )
    if _cf is not None:
        _cf = int(_cf)
        _w0 = next((w for w in weeks if w.get("week") == "Week 0"), None)
        _sum_rest = sum(
            (w.get("ig_new_followers") or 0) for w in weeks if w.get("week") != "Week 0"
        )
        # Week 0 = whatever is left so that total == current_followers
        _w0_val = max(0, _cf - _sum_rest)
        if _w0 is not None:
            _old = _w0.get("ig_new_followers")
            _w0["ig_new_followers"] = _w0_val if _w0_val > 0 else None
            print(f"  Week 0 ig: {_old} -> {_w0['ig_new_followers']}  "
                  f"(current={_cf}, rest={_sum_rest}, w0={_w0_val})")
        else:
            print("  Week 0 not found")
    else:
        print("  reston not found in social_insights.json, skipping")
except FileNotFoundError:
    print("  social_insights.json not found, skipping IG gap")

with open(SCORECARD_FILE, "w") as f:
    json.dump(sc, f, indent=2)

print("\nDone. Reston weeks relabeled + all KPIs patched from sheet.")
