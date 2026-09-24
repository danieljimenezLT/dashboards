#!/usr/bin/env python3
"""
build_all_scorecards.py

Shared library for the NSO scorecard (imported by build_scorecard_v2.py as
`_bac`): studio config from the NSO Config Google Sheet, week bounds, the
per-week data aggregation (sum_week_data), spend / leads / sales / IG loaders
and the LeadTeam fee helpers.

It no longer generates a scorecard file itself — the v1 file
(nso_scorecard_data.json) was retired; build_scorecard_v2.py writes the only
scorecard, nso_scorecard_data2.json.
"""

import calendar
import json
import math
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent        # nso-dashboard/
REPO_ROOT = ROOT.parent                    # dashboards/ (where data.json lives)
TODAY = date.today()

sys.path.insert(0, str(REPO_ROOT))
import studios as studios_registry

CREDS_PATH = ROOT / "credentials" / "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
LEADTEAM_MONTHLY = 1200.0  # $1,200/month, prorated daily
NSO_CONFIG_SHEET_URL = "https://docs.google.com/spreadsheets/d/1KhXfeofzltGyllmrfIm9UN2xqLUgBtjAxPhT-E0SQmk"

# Scorecard weeks: Week 1 starts on the configured week1_start date (any day), ends the
# following Saturday. All subsequent weeks run Sunday–Saturday. Charts independently use
# the same convention (Sunday start via getUTCDay() in _periodKey).
CAL_WEEK1_START = date(2025, 12, 28)  # Dec 28, 2025 = Sunday (reference only, not used in computation)

# Studios whose Week 1 still comes from the sheet's "Week 1 Start" column
# instead of Week 0 + 7 days (already opened — history must not shift).
LEGACY_WEEK1_CODES = ("FL-019", "VA-001")

# NSO Config sheet fixed column indices (0-based) — only for columns that never shift.
# Scheduling and targets columns use header-name lookup via _ch() in load_studio_config().
NSO_CFG_COL = {
    "name": 0,
    "code": 1,
}

# Maps studio display names (lowercase) in "Weekly Events & Spend" tab to studio codes
EVENTS_SPEND_STUDIO_MAP = {
    "naples": "FL-019",
    "naples - mercato": "FL-019",
    "reston": "VA-001",
    "herriman": "UT-001",
    "old bridge": "NJ-004",
    "dr phillips": "FL-020",
    "dr. phillips": "FL-020",
    "orlando - dr phillips": "FL-020",
    "aventura": "FL-018",
    "north miami": "FL-021",
    "uptown": "TX-004",
    "dallas uptown": "TX-004",
    "dallas - uptown": "TX-004",
    "dunwoody": "GA-001",
    "middletown": "NJ-005",
    "fort myers": "FL-022",
    "rose creek": "OK-001",
    "okc - rose creek": "OK-001",
    "south end": "NC-002",
    "charlotte - south end": "NC-002",
}


def calc_leadteam_fee(date_start, date_end):
    """$1,200/month prorated: sum of (1200 / days_in_month) for each day in the week."""
    if date_start is None or date_end is None:
        return None
    total = 0.0
    d = date_start
    while d <= date_end:
        total += LEADTEAM_MONTHLY / calendar.monthrange(d.year, d.month)[1]
        d += timedelta(days=1)
    return round(total, 2)


def date_to_week_num(d, week1_start):
    """Scorecard week number containing date d, using the same bounds as
    make_sun_sat_bounds(): Week 1 runs from week1_start to the first Saturday on
    or after it, then Sun–Sat weeks. None for dates before Week 1.
    (The old ceil(days/7) put Sundays — and Week 1's own first day — one week
    early, e.g. a Sunday C/O date landed on the week before its real week.)"""
    if not d or not week1_start or d < week1_start:
        return None
    week1_end = week1_start + timedelta(days=(5 - week1_start.weekday()) % 7)
    if d <= week1_end:
        return 1
    return 2 + (d - week1_end - timedelta(days=1)).days // 7


def load_lt_discounts():
    """{studio full name: [(start, end, monthly)]} — 'Leadteam Discount' rows
    from nso_other_costs.json (same source the dashboard's CPR tab uses)."""
    try:
        with open(ROOT / "nso_other_costs.json", encoding="utf-8") as f:
            monthly = json.load(f).get("monthly", [])
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    out = defaultdict(list)
    for e in monthly:
        if e.get("category") != "Leadteam Discount":
            continue
        try:
            out[e["studio_key"]].append(
                (date.fromisoformat(e["start"]), date.fromisoformat(e["end"]), float(e["monthly"])))
        except (KeyError, ValueError, TypeError):
            continue
    return out


def calc_leadteam_fee_net(date_start, date_end, lt_start, discounts):
    """LeadTeam fee with the same formula as the dashboard's _cprLtFee():
    $1,200 × days / 30.4375 from max(date_start, LT Fees Start Date), minus any
    'Leadteam Discount' prorated by the actual days in each calendar month."""
    if date_start is None or date_end is None:
        return None
    eff = max(date_start, lt_start) if lt_start else date_start
    if eff > date_end:
        return None
    base = LEADTEAM_MONTHLY * ((date_end - eff).days + 1) / 30.4375
    disc = 0.0
    for ds, de, monthly in discounts or []:
        d, stop = max(eff, ds), min(date_end, de)
        while d <= stop:
            disc += monthly / calendar.monthrange(d.year, d.month)[1]
            d += timedelta(days=1)
    return round(max(0.0, base - disc), 2)

# Studios where the data.json name differs from the display name.
# data.json uses STUDIO_NAME from Snowflake (strip_brand applied). Sourced
# from studios.json's canonical `name` (which IS the data.json spelling) —
# looked up by code at point of use via _canonical_name_by_code().
_STUDIOS_BY_CODE = studios_registry.by_code()


def _canonical_name_by_code(code: str, fallback: str) -> str:
    s = _STUDIOS_BY_CODE.get(code)
    return s["name"] if s else fallback

# Maps NSO studio code → social_insights.json 'code' field
IG_SOCIAL_CODE = studios_registry.ig_social_code_by_code()

# Google Sheets tab config per studio code
SHEET_CONFIG = {
    "FL-019": {
        "spend_tab": "*Running Marketing Spend",
        "spend_data_start": 4,      # row 3 = headers, data from row 4
        "spend_date_col": 0,        # DATE OF SPEND
        "spend_amount_col": 2,      # AMOUNT
        "events_tab": "*Events Tracker",
        "events_data_start": 11,    # row 10 = headers
        "events_date_col": 1,       # DATE OF EVENT
    },
    "UT-001": {
        "spend_tab": "Running Promotional Spend",
        "spend_data_start": 3,      # row 2 = headers, data from row 3
        "spend_date_col": 0,
        "spend_amount_col": 2,
        "events_tab": "Presales Event Tracker",
        "events_data_start": 10,    # row 9 = headers
        "events_date_col": 3,       # DATE OF EVENT (4th column: count, name, point person, date)
    },
    "VA-001": {
        # Reston — col layout: NAME | LOCATION | DATE OF EVENT | TYPE | ...
        "spend_tab": "*Running Promotional Spend",
        "spend_data_start": 3,
        "spend_date_col": 0,
        "spend_amount_col": 2,
        "events_tab": "*Events Tracker",
        "events_data_start": 10,
        "events_date_col": 2,  # DATE OF EVENT is col C (index 2)
    },
}

# Naples irregular week date bounds (weeks 1-10 are fixed; 11+ continue Sun-Sat from 4/19)
# Week 1 starts Feb 9 (Mon, "Ads Go Live") and ends Sat Feb 14.
# All subsequent weeks run Sunday–Saturday.
NAPLES_EXPLICIT_WEEKS = {
   -1:  (date(2026, 1, 26),  date(2026, 1, 31)),  # Mon Jan 26 – Sat Jan 31  (pre-presale week)
    0:  (None,               date(2026, 2,  8)),  # Pre Feb 9 (ends Sun Feb 8)
    1:  (date(2026, 2,  9),  date(2026, 2, 14)),  # Mon Feb 9  – Sat Feb 14
    2:  (date(2026, 2, 15),  date(2026, 2, 21)),  # Sun Feb 15 – Sat Feb 21
    3:  (date(2026, 2, 22),  date(2026, 2, 28)),  # Sun Feb 22 – Sat Feb 28
    4:  (date(2026, 3,  1),  date(2026, 3,  7)),  # Sun Mar 1  – Sat Mar 7
    5:  (date(2026, 3,  8),  date(2026, 3, 14)),  # Sun Mar 8  – Sat Mar 14
    6:  (date(2026, 3, 15),  date(2026, 3, 21)),  # Sun Mar 15 – Sat Mar 21
    7:  (date(2026, 3, 22),  date(2026, 3, 28)),  # Sun Mar 22 – Sat Mar 28
    8:  (date(2026, 3, 29),  date(2026, 4,  4)),  # Sun Mar 29 – Sat Apr 4
    9:  (date(2026, 4,  5),  date(2026, 4, 11)),  # Sun Apr 5  – Sat Apr 11
    10: (date(2026, 4, 12),  date(2026, 4, 18)),  # Sun Apr 12 – Sat Apr 18
}
NAPLES_SPECIAL_LABELS = {
    0:  "Pre 2/10",
    1:  "Ads Go Live 2/10 - 2/14",
    24: "Target C/O 7/19 - 7/25",
    27: "Target Grand Open 8/9 - 8/15",
}


# ---------------------------------------------------------------------------
# Date / amount parsing utilities
# ---------------------------------------------------------------------------

def _parse_sheet_date(s, default_year=2026):
    """Parse sheet date strings: 'M/D/YYYY', 'M/D/YY', 'M/D'."""
    s = str(s).strip()
    if not s:
        return None
    parts = s.split("/")
    try:
        if len(parts) == 2:
            return date(default_year, int(parts[0]), int(parts[1]))
        if len(parts) == 3:
            y = int(parts[2])
            if y < 100:
                y += 2000
            return date(y, int(parts[0]), int(parts[1]))
    except (ValueError, TypeError):
        pass
    return None


def _parse_amount(s):
    """Parse '$1,234.56' or '1234.56' → float."""
    cleaned = re.sub(r"[^\d.]", "", str(s))
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Config from nso_config.xlsx
# ---------------------------------------------------------------------------

def load_studio_config(gc):
    """Read per-studio config from the NSO Config Google Sheet."""

    def _parse_date(s):
        if not s or str(s).strip().lower() in ("", "n/a", "tbd", "-"):
            return None
        s = str(s).strip()
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    def _parse_float(s):
        if not s or str(s).strip().lower() in ("", "n/a", "-"):
            return None
        try:
            return float(re.sub(r"[^0-9.]", "", str(s).strip()))
        except ValueError:
            return None

    def _date_to_week(d, week1_start):
        return date_to_week_num(d, week1_start)

    sh = gc.open_by_url(NSO_CONFIG_SHEET_URL)
    ws = sh.worksheet("NSO Config")
    all_rows = ws.get_all_values()

    # Row 0 = group headers, Row 1 = column headers, Row 2+ = data
    header_row = all_rows[1] if len(all_rows) > 1 else []
    hdr_map = {h.strip().lower(): i for i, h in enumerate(header_row)}

    def _c(row, idx):
        return row[idx].strip() if 0 <= idx < len(row) else ""

    def _ch(row, *names):
        for name in names:
            i = hdr_map.get(name.lower(), -1)
            if 0 <= i < len(row) and row[i].strip():
                return row[i].strip()
        return ""

    studios = []
    for row in all_rows[2:]:
        name = _c(row, NSO_CFG_COL["name"])
        code = _c(row, NSO_CFG_COL["code"])
        if not name or not code:
            continue

        # Week 1 is always the week after Week 0 — the sheet's "Week 1 Start"
        # column is no longer maintained. Naples/Reston (already opened) keep
        # the column so their historical week numbering doesn't shift.
        week0_start  = _parse_date(_ch(row, "week 0 start"))
        if code in LEGACY_WEEK1_CODES:
            week1_start = _parse_date(_ch(row, "week 1 start"))
        else:
            week1_start = (week0_start + timedelta(days=7)) if week0_start else None
        co_date      = _parse_date(_ch(row, "target c/o date"))
        opening_date = _parse_date(_ch(row, "target opening date"))
        tier2_move   = _parse_date(_ch(row, "tier 2 start date"))
        lt_fee_start = _parse_date(_ch(row, "lt fees start date"))

        co_week = _date_to_week(co_date, week1_start)
        go_week = _date_to_week(opening_date, week1_start)

        total_leads  = _parse_float(_ch(row, "total leads"))
        presales_tgt = _parse_float(_ch(row, "presales"))
        rmr_tgt      = _parse_float(_ch(row, "day-1 rmr"))
        cpl_raw      = _ch(row, "cpl range") or None
        cpa_raw      = _ch(row, "cpa range") or None
        conv_rate    = _parse_float(_ch(row, "conversion rate"))
        mkt_budget      = _parse_float(_ch(row, "marketing budget"))
        goal_announce   = _parse_float(_ch(row, "goal to announce opening", "goal to announce")) or None

        # Milestone dates from Scheduling columns
        _MILESTONE_COLS = [
            ("early_lead_date",          "early lead date",                  "Early Lead Date",                    "Landing page & early interest list"),
            ("mbo_date",                 "full marketing build out complete", "Full Marketing Build Out Complete",  "Lead form live on website"),
            ("paid_presales_start_date", "paid presales start date",         "Paid Presales Start Date",           "Presales campaign begins"),
            ("paid_lead_gen_date",       "paid lead gen date",               "Paid Lead Gen Start",                "Meta / Google paid ads begin"),
            ("grassroots_start_date",    "grassroots start date",            "Grassroots Start Date",              "Community & grassroots outreach begins"),
        ]
        milestones = []
        for key, col_name, label, subtitle in _MILESTONE_COLS:
            d = _parse_date(_ch(row, col_name))
            if d:
                milestones.append({"key": key, "label": label, "subtitle": subtitle, "date": d.isoformat()})
        milestones.sort(key=lambda m: m["date"])
        # Reston fixed milestones (Preview Week + Grand Opening) — not in the Sheet
        if code == "VA-001":
            milestones += [
                {"key": "preview_week",  "label": "Preview Week",  "subtitle": "Aug 29 – Sep 4, 2026", "date": "2026-08-29"},
                {"key": "grand_opening", "label": "Grand Opening", "subtitle": "Doors open to the public",  "date": "2026-09-05"},
            ]

        # Platform IDs — found by header name (columns may vary)
        ig_id      = _ch(row, "instagram id") or None
        sheet_url  = _ch(row, "grassroots sheet url") or None
        gads_cid   = _ch(row, "google ads cid") or None
        gbp_loc_id = _ch(row, "gbp location id") or None
        fb_page_id = _ch(row, "facebook page id") or None

        sf_raw = _ch(row, "snowflake id")
        try:
            snowflake_id = str(int(float(sf_raw.replace(",", "")))) if sf_raw else None
        except (ValueError, TypeError):
            snowflake_id = sf_raw or None

        studios.append({
            "name":          name,
            "code":          code,
            "state":         "",
            "full_name":     f"SWEAT440 {name}",
            "data_name":     _canonical_name_by_code(code, name),
            "week0_date":    (week1_start - timedelta(days=1)) if week1_start else None,
            "week1_start":   week1_start,
            "co_date":       co_date,
            "co_week":       co_week,
            "opening_date":  opening_date,
            "go_week":       go_week,
            "tier_move":     tier2_move,
            "targets": {
                "total_leads":        total_leads,
                "presales_count":     presales_tgt,
                "estimated_day1_rmr": rmr_tgt,
                "blended_cpl":        f"${cpl_raw}" if cpl_raw else None,
                "blended_cpa":        f"${cpa_raw}" if cpa_raw else None,
                "conversion_rate":    conv_rate,
                "marketing_budget":   mkt_budget,
            },
            "fb_page_id":      fb_page_id,
            "ig_id":           ig_id,
            "sheet_url":       sheet_url,
            "snowflake_id":    snowflake_id,
            "gads_account_id":      gads_cid.replace("-", "") if gads_cid else None,
            "goal_announce_target": goal_announce,
            "milestones":           milestones,
            "lt_fee_start":         lt_fee_start,
        })

    return studios


# ---------------------------------------------------------------------------
# Week bounds builders
# ---------------------------------------------------------------------------

def make_naples_bounds(num_weeks):
    bounds = []
    explicit = NAPLES_EXPLICIT_WEEKS
    last_end = date(2026, 4, 18)   # Week 10 end (Saturday)
    next_sun = last_end + timedelta(days=1)  # 4/19 = Sunday

    # Include any pre-weeks (negative week numbers) from explicit dict
    for wn in sorted(k for k in explicit if k < 0):
        bounds.append((wn, explicit[wn][0], explicit[wn][1]))
    bounds.append((0, explicit[0][0], explicit[0][1]))
    for wn in range(1, num_weeks + 1):
        if wn in explicit:
            bounds.append((wn, explicit[wn][0], explicit[wn][1]))
        else:
            offset = wn - 11
            ws = next_sun + timedelta(weeks=offset)
            we = ws + timedelta(days=6)  # Saturday
            bounds.append((wn, ws, we))
    return bounds


def make_sun_sat_bounds(week1_start, num_weeks):
    """Build week bounds: Week 1 starts on week1_start (any day) and ends on the
    first Saturday on or after it. All subsequent weeks run Sunday–Saturday."""
    # Saturday = weekday 5 (Mon=0 … Sat=5, Sun=6)
    days_to_sat = (5 - week1_start.weekday()) % 7
    week1_end = week1_start + timedelta(days=days_to_sat)

    bounds = [(0, None, week1_start - timedelta(days=1))]
    bounds.append((1, week1_start, week1_end))

    next_sun = week1_end + timedelta(days=1)
    for i in range(2, num_weeks + 1):
        ws = next_sun + timedelta(weeks=i - 2)
        we = ws + timedelta(days=6)  # Saturday
        bounds.append((i, ws, we))
    return bounds


def fmt_dr(ws, we):
    return f"{ws.month}/{ws.day} - {we.month}/{we.day}"


def current_week_num(bounds):
    cw = 0
    for wn, ws, we in bounds:
        if ws is None:
            continue
        if ws <= TODAY:
            cw = wn
    return cw


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

_DIGITAL_SOURCES = {"meta ads", "google ads"}

def load_daily_leads(daily_rows):
    """Build {data_name: {date_str: {leads, grassroots_leads, digital_leads}}} from data.json."""
    by = defaultdict(lambda: defaultdict(lambda: {"leads": 0, "grassroots_leads": 0, "digital_leads": 0}))
    for r in daily_rows:
        studio = str(r.get("studio", ""))
        d = str(r.get("date", ""))[:10]
        if not studio or len(d) < 10:
            continue
        signups = int(r.get("signups") or 0)
        src = str(r.get("source", "")).lower()
        by[studio][d]["leads"] += signups
        if src == "grassroots":
            by[studio][d]["grassroots_leads"] += signups
        if src in _DIGITAL_SOURCES:
            by[studio][d]["digital_leads"] += signups
    return by


def load_sales_data(sales_raw):
    """Build {full_name: {date_str: {presales, cancellations, grassroots_presales, digital_presales, digital_cancellations}}}."""
    by = defaultdict(lambda: defaultdict(
        lambda: {"presales": 0, "cancellations": 0, "grassroots_presales": 0,
                 "digital_presales": 0, "digital_cancellations": 0}
    ))
    if not sales_raw:
        return by

    SNOWFLAKE_TO_FULL = {}
    for cfg in _STUDIO_CFG_CACHE:
        if cfg["snowflake_id"]:
            SNOWFLAKE_TO_FULL[cfg["snowflake_id"]] = cfg["full_name"]

    for sid, s in sales_raw.get("studios", {}).items():
        full = SNOWFLAKE_TO_FULL.get(str(sid))
        if not full:
            continue
        for row in s.get("daily", []):
            d = str(row.get("date", ""))[:10]
            src = str(row.get("source", "")).lower()
            by[full][d]["presales"]      += int(row.get("presales") or 0)
            by[full][d]["cancellations"] += int(row.get("cancellations") or 0)
            if src == "grassroots":
                by[full][d]["grassroots_presales"] += int(row.get("presales") or 0)
            if src in _DIGITAL_SOURCES:
                by[full][d]["digital_presales"]      += int(row.get("presales") or 0)
                by[full][d]["digital_cancellations"] += int(row.get("cancellations") or 0)
    return by


def load_ig_followers(social_raw):
    """Build ({nso_code: {date_str: daily_delta}}, {nso_code: current_followers}).

    For new/small accounts that lack daily follower_count history (Meta API only
    returns 30 days), falls back to current_followers on yesterday so the count
    shows up in the current week.  The caller uses current_followers to attribute
    any gap (followers gained before the API window) to Week 0."""
    from datetime import datetime, timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    _social_to_nso = {v: k for k, v in IG_SOCIAL_CODE.items()}
    by = defaultdict(dict)
    current = {}  # {nso_code: current_followers int}
    for ig in social_raw.get("instagram", []):
        social_code = ig.get("code", "")
        nso_code = _social_to_nso.get(social_code)
        if not nso_code:
            continue
        cf = ig.get("current_followers")
        if cf is not None:
            current[nso_code] = int(cf)
        has_daily = False
        for r in ig.get("daily", []):
            if "follower_count" in r:
                by[nso_code][r["date"]] = int(r["follower_count"] or 0)
                has_daily = True
        # New/small accounts: no daily history → use current_followers as this-week total
        if not has_daily and cf is not None:
            by[nso_code][yesterday] = int(cf)
    return by, current


def load_meta_spend(meta_rows):
    """Build {code: {date_str: spend}} from marketing_data.json meta_ads.

    Two attribution paths:
    1. Dedicated-account rows: have studio_code set explicitly (no campaign_name).
       Attributed directly by code — no name matching needed.
    2. Shared-account rows: campaign_name contains studio keyword.
       Matched via EVENTS_SPEND_STUDIO_MAP aliases (longest first).
    """
    all_codes = [cfg["code"] for cfg in _STUDIO_CFG_CACHE]

    # Build {code: [keywords sorted longest-first]} from the existing alias map
    code_keywords = defaultdict(list)
    for alias, code in EVENTS_SPEND_STUDIO_MAP.items():
        code_keywords[code].append(alias.lower())
    match_list = []
    for code in all_codes:
        keywords = sorted(code_keywords.get(code, []), key=len, reverse=True)
        keywords.append(code.lower())   # code itself as final fallback
        match_list.append((code, keywords))

    by = defaultdict(lambda: defaultdict(float))
    for r in meta_rows:
        d = str(r.get("date", ""))[:10]
        spend = float(r.get("spend") or 0)
        if spend <= 0 or len(d) < 10:
            continue
        # Path 1: dedicated account row tagged with studio_code
        studio_code = r.get("studio_code", "")
        if studio_code and studio_code in all_codes:
            by[studio_code][d] += spend
            continue
        # Path 2: shared account — match by campaign name
        cname = str(r.get("campaign_name", "")).lower()
        if not cname:
            continue
        for code, keywords in match_list:
            if any(kw in cname for kw in keywords):
                by[code][d] += spend
                break
    return by


def load_google_spend(gads_rows):
    """Build {code: {date_str: spend}} from nso_google_ads.json google_ads."""
    by = defaultdict(lambda: defaultdict(float))
    for cfg in _STUDIO_CFG_CACHE:
        acct = cfg.get("gads_account_id")
        if not acct:
            continue
        code = cfg["code"]
        for r in gads_rows:
            if str(r.get("account_id", "")) == acct:
                d = str(r.get("date", ""))[:10]
                spend = float(r.get("spend") or 0)
                if spend > 0 and len(d) >= 10:
                    by[code][d] += spend
    return by



def _parse_weekly_events_header(s: str, base_year: int):
    """'Jan 4 - Jan 10' → (date, date). Returns None if unparseable."""
    s = (s or '').strip()
    if ' - ' not in s:
        return None
    left, right = s.split(' - ', 1)
    def _p(x, yr):
        for fmt in ('%b %d', '%B %d'):
            try:
                return datetime.strptime(f"{x.strip()} {yr}", f'{fmt} %Y').date()
            except ValueError:
                pass
        return None
    start = _p(left, base_year)
    end   = _p(right, base_year)
    if not start or not end:
        return None
    if end < start:   # year rollover (e.g., Dec → Jan)
        end = _p(right, base_year + 1)
    return start, end


def load_events_spend_from_nso_config(gc, studio_cfgs=None):
    """Read 'Weekly Events' tab from NSO Config sheet.

    The sheet has calendar date-range column headers (e.g., 'Jan 4 - Jan 10'),
    with col 0 = studio name, col 1 = metric name, col 2+ = values.
    Returns {code: (spend_by_wk, events_by_wk, other_by_wk)} keyed by
    SCORECARD week number, resolved via date-to-week lookup.
    """
    result = {}
    if not gc:
        return result

    # Build date-string → scorecard week-number lookup per studio code.
    sc_date_to_wk: dict[str, dict[str, int]] = {}
    if studio_cfgs:
        for cfg in studio_cfgs:
            code = cfg["code"]
            w1   = cfg["week1_start"]
            if code == "FL-019":
                bounds = make_naples_bounds(70)
            elif w1:
                bounds = make_sun_sat_bounds(w1, 70)
            else:
                continue
            date_wk: dict[str, int] = {}
            for wn, ws_d, we_d in bounds:
                if ws_d is None:
                    continue
                d = ws_d
                while d <= we_d:
                    date_wk[d.isoformat()] = wn
                    d += timedelta(days=1)
            sc_date_to_wk[code] = date_wk

    try:
        sh = gc.open_by_url(NSO_CONFIG_SHEET_URL)
        ws = sh.worksheet("Weekly Events")
        all_rows = ws.get_all_values()
    except Exception as e:
        print(f"  Warning: could not read Weekly Events from NSO Config: {e}")
        return result

    if len(all_rows) < 3:
        return result

    # Row 0: title row — extract base year for date parsing (e.g., "2026")
    m_year = re.search(r'\b(20\d\d)\b', ' '.join(str(c) for c in all_rows[0]))
    base_year = int(m_year.group(1)) if m_year else date.today().year

    # Row 1: column headers — col 0 = studio label, col 1 = metric label, col 2+ = date ranges
    header_row = all_rows[1]
    col_to_dates: dict[int, tuple] = {}  # col_idx → (start_date, end_date)
    for ci, hdr in enumerate(header_row):
        if ci < 2:
            continue
        parsed = _parse_weekly_events_header(str(hdr), base_year)
        if parsed:
            col_to_dates[ci] = parsed

    if not col_to_dates:
        print("  Warning: no date-range columns found in Weekly Events tab header")
        return result

    # Data rows: col 0 = studio name, col 1 = metric, col 2+ = weekly values
    for row in all_rows[2:]:
        if not row:
            continue
        name_cell   = str(row[0]).strip().lower() if row[0] else ""
        metric_cell = str(row[1]).strip().lower() if len(row) > 1 else ""

        if not name_cell or not metric_cell:
            continue

        code = EVENTS_SPEND_STUDIO_MAP.get(name_cell)
        if not code:
            continue

        if code not in result:
            result[code] = (defaultdict(float), defaultdict(int), defaultdict(float))
        spend_d, events_d, other_d = result[code]

        is_events = "event" in metric_cell or "community" in metric_cell
        is_spend  = "grassroots" in metric_cell
        is_other  = "other" in metric_cell and "event" not in metric_cell

        if not (is_events or is_spend or is_other):
            continue

        date_wk = sc_date_to_wk.get(code, {})

        for ci, (wstart, wend) in col_to_dates.items():
            if ci >= len(row):
                continue
            val = str(row[ci]).strip()
            if not val or val == "-":
                continue

            # Map calendar week to scorecard week number via date lookup.
            sc_wn = date_wk.get(wstart.isoformat()) if date_wk else None
            if sc_wn is None and date_wk:
                d = wstart + timedelta(days=1)
                while d <= wend:
                    sc_wn = date_wk.get(d.isoformat())
                    if sc_wn is not None:
                        break
                    d += timedelta(days=1)
            if sc_wn is None:
                continue  # outside this studio's scorecard range

            if is_events:
                try:
                    cnt = int(re.sub(r"[^\d]", "", val))
                    if cnt > 0:
                        events_d[sc_wn] += cnt
                except (ValueError, TypeError):
                    pass
            elif is_spend:
                amt = _parse_amount(val)
                if amt > 0:
                    spend_d[sc_wn] += amt
            elif is_other:
                amt = _parse_amount(val)
                if amt > 0:
                    other_d[sc_wn] += amt

    for code, (sp, ev, ot) in result.items():
        print(f"  NSO Config Events [{code}]: {len(sp)} grassroots weeks, "
              f"{sum(ev.values())} total events, {len(ot)} other weeks")
    return result


def load_grassroots_data(gc, sheet_url, code):
    """Load spend and events dicts from a studio's Google Sheet."""
    empty = defaultdict(float), defaultdict(int)
    cfg = SHEET_CONFIG.get(code)
    if not cfg or not sheet_url:
        return empty

    try:
        sh = gc.open_by_url(sheet_url)
    except Exception as e:
        print(f"  Warning [{code}]: cannot open sheet ({type(e).__name__}): {e}")
        return empty

    spend_by_date = defaultdict(float)
    try:
        ws = sh.worksheet(cfg["spend_tab"])
        rows = ws.get_all_values()
        dc, ac = cfg["spend_date_col"], cfg["spend_amount_col"]
        for row in rows[cfg["spend_data_start"] - 1:]:
            d = _parse_sheet_date(row[dc] if len(row) > dc else "")
            amt = _parse_amount(row[ac] if len(row) > ac else "")
            if d and amt > 0:
                spend_by_date[d.isoformat()] += amt
        total = sum(spend_by_date.values())
        print(f"  [{code}] Spend sheet: {len(spend_by_date)} dates, ${total:,.2f} total")
    except Exception as e:
        print(f"  Warning [{code}]: spend tab error: {e}")

    events_by_date = defaultdict(int)
    try:
        ws = sh.worksheet(cfg["events_tab"])
        rows = ws.get_all_values()
        dc = cfg["events_date_col"]
        for row in rows[cfg["events_data_start"] - 1:]:
            if not any(str(v).strip() for v in row[:5]):
                continue
            d = _parse_sheet_date(row[dc] if len(row) > dc else "")
            if d:
                events_by_date[d.isoformat()] += 1
        print(f"  [{code}] Events sheet: {sum(events_by_date.values())} events")
    except Exception as e:
        print(f"  Warning [{code}]: events tab error: {e}")

    return spend_by_date, events_by_date


# ---------------------------------------------------------------------------
# Per-week aggregation
# ---------------------------------------------------------------------------

def sum_week_data(wn, ws_date, we_date, data_name, full_name,
                  leads_by_date, sales_by_date, ig_fc,
                  meta_spend_by_date, gads_spend_by_date,
                  gr_spend_by_week=None, events_by_week=None,
                  other_spend_by_week=None, w0_floor=None):
    """Sum all per-week metrics for a single week bound.
    Meta/Google spend are date-keyed (from APIs); grassroots/events/other are week-number-keyed
    (directly from 'Weekly Events & Spend' sheet where 'Week N' = studio Week N)."""
    if gr_spend_by_week is None:
        gr_spend_by_week = {}
    if events_by_week is None:
        events_by_week = {}
    if other_spend_by_week is None:
        other_spend_by_week = {}

    leads = gr_leads = digital_leads = 0
    presales = cancellations = gr_presales = digital_presales = digital_cancellations = 0
    ig_sum = 0; has_ig = False
    meta_spend = gads_spend = 0.0

    def _add_day(d_str):
        nonlocal leads, gr_leads, digital_leads, presales, cancellations, gr_presales, digital_presales, digital_cancellations
        nonlocal ig_sum, has_ig, meta_spend, gads_spend

        # Leads from data.json (keyed by short name)
        lv = leads_by_date.get(data_name, {}).get(d_str, {})
        leads         += lv.get("leads", 0)
        gr_leads      += lv.get("grassroots_leads", 0)
        digital_leads += lv.get("digital_leads", 0)

        # Sales from nso_sales_data.json (keyed by full name)
        sv = sales_by_date.get(full_name, {}).get(d_str, {})
        presales               += sv.get("presales", 0)
        cancellations          += sv.get("cancellations", 0)
        gr_presales            += sv.get("grassroots_presales", 0)
        digital_presales       += sv.get("digital_presales", 0)
        digital_cancellations  += sv.get("digital_cancellations", 0)

        # Instagram followers
        fc = ig_fc.get(d_str)
        if fc is not None:
            ig_sum += fc; has_ig = True

        # Ad spend (date-keyed, from APIs)
        meta_spend  += meta_spend_by_date.get(d_str, 0.0)
        gads_spend  += gads_spend_by_date.get(d_str, 0.0)

    if ws_date is None:
        # Week 0: all date-keyed data from w0_floor (day after the last negative
        # pre-week ends, if any) up to and including we_date. Without the floor,
        # this would re-sum everything from the beginning of time — double-counting
        # every negative pre-week's leads/presales/cancellations into Week 0.
        all_dates = set()
        for lookup in (leads_by_date.get(data_name, {}),
                       sales_by_date.get(full_name, {})):
            all_dates.update(lookup.keys())
        all_dates.update(ig_fc.keys())
        all_dates.update(meta_spend_by_date.keys())
        all_dates.update(gads_spend_by_date.keys())
        we_str = we_date.isoformat()
        w0_str = w0_floor.isoformat() if w0_floor else None
        for d_str in sorted(all_dates):
            if d_str <= we_str and (w0_str is None or d_str >= w0_str):
                _add_day(d_str)
    else:
        day = ws_date
        while day <= we_date:
            _add_day(day.isoformat())
            day += timedelta(days=1)

    # Grassroots spend, other spend, and events are week-number-keyed (not date-keyed)
    gr_spend    = round(gr_spend_by_week.get(wn, 0.0), 2)
    other_spend = round(other_spend_by_week.get(wn, 0.0), 2)
    comm_events = int(events_by_week.get(wn, 0))

    return {
        "leads":                  leads,
        "gr_leads":               gr_leads,
        "digital_leads":          digital_leads,
        "presales":               presales,
        "cancellations":          cancellations,
        "gr_presales":            gr_presales,
        "digital_presales":       digital_presales,
        "digital_cancellations":  digital_cancellations,
        "ig":                     ig_sum if has_ig else None,
        "meta_spend":             round(meta_spend, 2),
        "gads_spend":             round(gads_spend, 2),
        "gr_spend":               gr_spend,
        "other_spend":            other_spend,
        "comm_events":            comm_events,
    }


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

_STUDIO_CFG_CACHE = []   # populated by build_scorecard_v2.py so load_sales_data can use it
