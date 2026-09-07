#!/usr/bin/env python3
"""
build_all_scorecards.py

Generates nso_scorecard_data.json for all NSO studios.
Reads all studio config from the NSO Config Google Sheet (single source of truth).

Per-week fields calculated:
  new_leads, total_leads (cum), presales_week, presales_count (cum),
  cancellations_week, cancellations_count (cum),
  grassroots_leads, grassroots_presales, conversion_rate,
  comm_events, meta_spend, google_spend, grassroots_spend,
  leadteam_fee ($1,200/month prorated daily), total_marketing_spend,
  blended_cpl, blended_cpa, ig_new_followers, est_rmr (null)

Usage:
  python scripts/build_all_scorecards.py
  python scripts/build_all_scorecards.py --dry-run
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
        if not d or not week1_start:
            return None
        delta = (d - week1_start).days
        if delta <= 0:
            return None
        return math.ceil(delta / 7)

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

        week1_start  = _parse_date(_ch(row, "week 1 start"))
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
            ("early_lead_date",       "early lead date",                  "Early Lead Date",                    "Landing page & early interest list"),
            ("mbo_date",              "full marketing build out complete", "Full Marketing Build Out Complete",  "Lead form live on website"),
            ("paid_presales_start_date", "paid presales start date",       "Paid Presales Start Date",           "Presales campaign begins"),
            ("paid_lead_gen_date",    "paid lead gen date",               "Paid Lead Gen Start",                "Meta / Google paid ads begin"),
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



def load_events_spend_from_nso_config(gc, studio_cfgs=None):
    """Read 'Weekly Events & Spend' tab from NSO Config sheet.

    Returns {code: (spend_by_week, events_by_week, other_by_week)} keyed by
    SCORECARD week number.  The sheet stores values under its own 'Week 1 Start
    Date' (Mon–Sun), which may differ from the scorecard's Sun–Sat boundaries.
    We convert each sheet week column to a date (sheet_w1 + (N-1)*7 days) and
    look it up in the studio's scorecard bounds to get the real week number.
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
        ws = sh.worksheet("Weekly Events & Spend")
        all_rows = ws.get_all_values()
    except Exception as e:
        print(f"  Warning: could not read Weekly Events & Spend from NSO Config: {e}")
        return result

    if len(all_rows) < 3:
        return result

    # Row 1 = headers: "Name", "Week 1 Start Date", "Metric", "Week 1", "Week 2", ...
    # Use \s*$ so "Week 1 Start Date" is NOT matched (doesn't end with a digit).
    header_row = all_rows[1]
    col_to_sheet_wk: dict[int, int] = {}
    for col_i, hdr in enumerate(header_row):
        m = re.match(r"Week\s+(\d+)\s*$", str(hdr).strip(), re.IGNORECASE)
        if m:
            col_to_sheet_wk[col_i] = int(m.group(1))

    current_code:    str | None  = None
    current_sheet_w1             = None   # date of the sheet's own Week 1

    for row in all_rows[2:]:
        if not row:
            continue
        name_cell    = str(row[0]).strip().lower() if row[0] else ""
        w1_date_cell = str(row[1]).strip()          if len(row) > 1 else ""
        metric_cell  = str(row[2]).strip().lower()  if len(row) > 2 else ""

        if name_cell:
            current_code     = EVENTS_SPEND_STUDIO_MAP.get(name_cell)
            current_sheet_w1 = _parse_sheet_date(w1_date_cell) if w1_date_cell else None
            if current_code and current_code not in result:
                result[current_code] = (defaultdict(float), defaultdict(int), defaultdict(float))

        if not current_code or current_code not in result:
            continue

        spend_d, events_d, other_d = result[current_code]
        is_spend       = "grassroots" in metric_cell
        is_other_spend = "other" in metric_cell and "$" in metric_cell
        is_events      = "events" in metric_cell or "#" in metric_cell

        if not (is_spend or is_other_spend or is_events):
            continue

        date_wk = sc_date_to_wk.get(current_code, {})

        for col_i, sheet_wn in col_to_sheet_wk.items():
            if col_i >= len(row):
                continue
            val = str(row[col_i]).strip()
            if not val or val == "-":
                continue

            # Map sheet week number → scorecard week number via date.
            if current_sheet_w1 and date_wk:
                sheet_wk_start = current_sheet_w1 + timedelta(days=(sheet_wn - 1) * 7)
                sc_wn = date_wk.get(sheet_wk_start.isoformat())
                if sc_wn is None:
                    continue  # before W0 (pre-weeks) or beyond loaded range
            else:
                sc_wn = sheet_wn  # fallback: no bounds available

            if is_spend:
                amt = _parse_amount(val)
                if amt > 0:
                    spend_d[sc_wn] += amt
            elif is_other_spend:
                amt = _parse_amount(val)
                if amt > 0:
                    other_d[sc_wn] += amt
            elif is_events:
                try:
                    cnt = int(re.sub(r"[^\d]", "", val))
                    if cnt > 0:
                        events_d[sc_wn] += cnt
                except (ValueError, TypeError):
                    pass

    for code, (sp, ev, ot) in result.items():
        print(f"  NSO Config Events&Spend [{code}]: {len(sp)} grassroots weeks, "
              f"{sum(ev.values())} total events, {len(ot)} other spend weeks")
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
                  other_spend_by_week=None):
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
        # Week 0: all date-keyed data up to and including we_date
        all_dates = set()
        for lookup in (leads_by_date.get(data_name, {}),
                       sales_by_date.get(full_name, {})):
            all_dates.update(lookup.keys())
        all_dates.update(ig_fc.keys())
        all_dates.update(meta_spend_by_date.keys())
        all_dates.update(gads_spend_by_date.keys())
        we_str = we_date.isoformat()
        for d_str in sorted(all_dates):
            if d_str <= we_str:
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
# Main
# ---------------------------------------------------------------------------

_STUDIO_CFG_CACHE = []   # populated early so load_sales_data can use it


def main():
    dry_run = "--dry-run" in sys.argv

    # -- Init Google Sheets client (needed for studio config + events/spend)
    gc = None
    if CREDS_PATH.exists():
        try:
            from google.oauth2.service_account import Credentials
            import gspread
            creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
            gc = gspread.authorize(creds)
            print("Google Sheets auth OK")
        except Exception as e:
            print(f"  Warning: Google Sheets auth failed: {e}")

    if gc is None:
        print("ERROR: Google Sheets credentials required — cannot load studio config")
        sys.exit(1)

    # -- Load studio config from NSO Config Google Sheet
    print("\nLoading studio config from NSO Config sheet...")
    studio_cfgs = load_studio_config(gc)
    _STUDIO_CFG_CACHE.extend(studio_cfgs)
    for c in studio_cfgs:
        print(f"  {c['name']} ({c['code']})  GO week={c['go_week']}  "
              f"week1={c['week1_start']}  acct={c['gads_account_id']}")

    # -- Load data files
    print("\nLoading data.json ...")
    data_json_path = REPO_ROOT / "data.json"
    if not data_json_path.exists():
        print(f"  ERROR: {data_json_path} not found")
        sys.exit(1)
    with open(data_json_path, encoding="utf-8") as f:
        raw = json.load(f)
    daily_rows = raw.get("daily_detail", [])
    print(f"  {len(daily_rows)} rows")

    try:
        with open(ROOT / "nso_sales_data.json", encoding="utf-8") as f:
            sales_raw = json.load(f)
        print("Loaded nso_sales_data.json")
    except FileNotFoundError:
        sales_raw = {}
        print("  nso_sales_data.json not found — presales will be 0")

    try:
        with open(ROOT / "social_insights.json", encoding="utf-8") as f:
            social_raw = json.load(f)
        print("Loaded social_insights.json")
    except FileNotFoundError:
        social_raw = {}
        print("  social_insights.json not found — IG data will be null")

    try:
        with open(ROOT / "marketing_data.json", encoding="utf-8") as f:
            mkt_raw = json.load(f)
        meta_rows = mkt_raw.get("meta_ads", [])
        print(f"Loaded marketing_data.json ({len(meta_rows)} meta rows)")
    except FileNotFoundError:
        meta_rows = []
        print("  marketing_data.json not found — meta spend will be 0")

    try:
        with open(ROOT / "nso_spend_overrides.json", encoding="utf-8") as f:
            spend_overrides = json.load(f)
    except FileNotFoundError:
        spend_overrides = {}

    try:
        with open(ROOT / "nso_google_ads.json", encoding="utf-8") as f:
            gads_raw = json.load(f)
        gads_rows = gads_raw.get("google_ads", [])
        print(f"Loaded nso_google_ads.json ({len(gads_rows)} rows)")
    except FileNotFoundError:
        gads_rows = []
        print("  nso_google_ads.json not found — google spend will be 0")

    # -- Build global lookups
    leads_by_date = load_daily_leads(daily_rows)
    sales_by_date = load_sales_data(sales_raw)
    ig_by_code, ig_current_followers = load_ig_followers(social_raw)
    meta_by_code  = load_meta_spend(meta_rows)
    gads_by_code  = load_google_spend(gads_rows)

    # Load grassroots spend + community events + other spend from NSO Config "Weekly Events & Spend" tab.
    print("\nLoading grassroots/events/other spend data from NSO Config sheet...")
    nso_config_gr = load_events_spend_from_nso_config(gc, studio_cfgs)

    gr_spend_by_code    = {}
    events_by_code      = {}
    other_spend_by_code = {}
    for cfg in studio_cfgs:
        code = cfg["code"]
        if code in nso_config_gr:
            sp, ev, ot = nso_config_gr[code]
        else:
            sp, ev, ot = defaultdict(float), defaultdict(int), defaultdict(float)
        gr_spend_by_code[code]    = sp
        events_by_code[code]      = ev
        other_spend_by_code[code] = ot

    # Load date-range spend entries from nso_other_costs.json (Google Form responses).
    # Keyed by category: "grassroots" → gr_spend, "other" → other_spend.
    # Applied per-week using date-overlap in the studio loop below.
    print("\nLoading spend entries from nso_other_costs.json...")
    _oc_gr_by_name    = {}  # {full_name: [(start_date, end_date, amount)]}
    _oc_other_by_name = {}
    try:
        with open(ROOT / "nso_other_costs.json", encoding="utf-8") as _f:
            _oc = json.load(_f)
        for _e in _oc.get("adhoc", []):
            _cat = _e.get("category", "").lower()
            if _cat not in ("grassroots", "other"):
                continue
            try:
                _sd = date.fromisoformat(_e["start"])
                _ed = date.fromisoformat(_e["end"])
            except (KeyError, ValueError):
                continue
            _amt = float(_e.get("total") or 0)
            if _amt <= 0:
                continue
            _dest = _oc_gr_by_name if _cat == "grassroots" else _oc_other_by_name
            _dest.setdefault(_e.get("studio_key", ""), []).append((_sd, _ed, _amt))
        _oc_total = sum(len(v) for v in _oc_gr_by_name.values()) + sum(len(v) for v in _oc_other_by_name.values())
        print(f"  {_oc_total} entries across {len(_oc_gr_by_name)} grassroots + {len(_oc_other_by_name)} other studios")
    except FileNotFoundError:
        print("  nso_other_costs.json not found — skipping")

    # -- Build scorecard per studio
    output_studios = []

    for cfg in studio_cfgs:
        full  = cfg["full_name"]
        dname = cfg["data_name"]
        code  = cfg["code"]
        go_wk = cfg["go_week"] or 28
        co_wk = cfg["co_week"]
        num_weeks = max(go_wk + 3, 28)
        # Always extend to current week + 3-week buffer so data keeps flowing
        # after opening day. Naples has no week1_start in config — use its
        # hard-coded Week 1 date (Feb 9, 2026) as the base.
        _w1 = cfg["week1_start"] or (NAPLES_EXPLICIT_WEEKS[1][0] if cfg["code"] == "FL-019" else None)
        if _w1:
            weeks_elapsed = max(0, (TODAY - _w1).days // 7)
            num_weeks = max(num_weeks, weeks_elapsed + 3)

        print(f"\n{'='*60}\n  {full}  (data_name={dname!r})\n{'='*60}")

        # Build week bounds
        is_naples = cfg["code"] == "FL-019"
        if is_naples:
            bounds = make_naples_bounds(num_weeks)
        elif not cfg["week1_start"]:
            # No week1_start in sheet — auto-detect from first Snowflake data row
            all_dates = sorted(leads_by_date.get(dname, {}).keys())
            if not all_dates:
                print(f"  Skipping {full}: no week1_start and no Snowflake data found")
                continue
            auto_w1 = datetime.strptime(all_dates[0], "%Y-%m-%d").date()
            cfg["week1_start"] = auto_w1
            print(f"  Auto-detected week1_start={auto_w1} from first Snowflake row")
            bounds = make_sun_sat_bounds(auto_w1, num_weeks)
        else:
            bounds = make_sun_sat_bounds(cfg["week1_start"], num_weeks)

        ig_fc_studio    = ig_by_code.get(code, {})
        meta_spend_d    = meta_by_code.get(code, {})
        gads_spend_d    = gads_by_code.get(code, {})
        gr_spend_d      = defaultdict(float, gr_spend_by_code.get(code, {}))
        events_d        = events_by_code.get(code, defaultdict(int))
        other_spend_d   = other_spend_by_code.get(code, defaultdict(float))

        # Merge date-range spend from nso_other_costs.json into gr/other spend dicts
        other_spend_d = defaultdict(float, other_spend_d)
        for _oc_entries, _dest in (
            (_oc_gr_by_name.get(full, []),    gr_spend_d),
            (_oc_other_by_name.get(full, []), other_spend_d),
        ):
            for _wn, _ws, _we in bounds:
                if _ws is None or not _oc_entries:
                    continue
                for _sd, _ed, _amt in _oc_entries:
                    if _sd <= _we and _ed >= _ws:
                        _span    = max(1, (_ed - _sd).days + 1)
                        _overlap = (min(_ed, _we) - max(_sd, _ws)).days + 1
                        _dest[_wn] += round(_amt * _overlap / _span, 2)

        cw_num = current_week_num(bounds)
        print(f"  current_week: {cw_num}  (today={TODAY})")

        cum_leads = cum_presales = cum_canc = 0
        weeks_out = []

        for (wn, ws_date, we_date) in bounds:
            w = sum_week_data(
                wn, ws_date, we_date, dname, full,
                leads_by_date, sales_by_date, ig_fc_studio,
                meta_spend_d, gads_spend_d,
                gr_spend_by_week=gr_spend_d,
                events_by_week=events_d,
                other_spend_by_week=other_spend_d,
            )

            cum_leads    += w["leads"]
            cum_presales += w["presales"]
            cum_canc     += w["cancellations"]

            # Week label
            if wn == 0:
                wk_label = "Week 0"
                if is_naples:
                    dr = NAPLES_SPECIAL_LABELS.get(0, f"Pre {cfg['week1_start'].month}/{cfg['week1_start'].day}")
                else:
                    d0 = cfg["week1_start"]
                    dr = f"Pre {d0.month}/{d0.day}"
            else:
                wk_label = f"WEEK {wn}"
                if is_naples and wn in NAPLES_SPECIAL_LABELS:
                    dr = NAPLES_SPECIAL_LABELS[wn]
                else:
                    dr = fmt_dr(ws_date, we_date)

            # Cumulative nulls: show null if no data yet (not 0)
            tl = float(cum_leads)    if cum_leads    > 0 else (0.0 if wn == 0 else None)
            pc = float(cum_presales) if cum_presales > 0 else (0.0 if wn == 0 else None)
            cc = float(cum_canc)     if cum_canc     > 0 else (0.0 if wn == 0 else None)

            # Weekly fields: show 0 (or None) only for future weeks
            is_active = ws_date is not None and ws_date <= TODAY

            def _weekly_int(v, fallback=None):
                if v > 0:
                    return v
                return 0 if is_active else fallback

            def _weekly_float(v, fallback=None):
                if v > 0:
                    return round(v, 2)
                return 0.0 if is_active else fallback

            # Conversion rate: presales / leads (%)
            conv_rate = None
            if w["leads"] > 0 and is_active:
                conv_rate = round(w["presales"] / w["leads"] * 100, 1)

            # LeadTeam fee: $1,200/month prorated daily per week.
            # Week 0 gets the pre-presales lump sum from nso_spend_overrides.json
            # (covers LeadTeam work done before the presales period started).
            ov_pre = spend_overrides.get(code, {}).get("leadteam_pre")
            lt_start = cfg.get("lt_fee_start")
            if wn == 0 and ov_pre:
                lt_fee = float(ov_pre)
            elif is_active and wn >= 1:
                if lt_start and we_date < lt_start:
                    lt_fee = None          # week entirely before fee start
                elif lt_start and ws_date < lt_start:
                    lt_fee = calc_leadteam_fee(lt_start, we_date)  # prorate
                else:
                    lt_fee = calc_leadteam_fee(ws_date, we_date)
            else:
                lt_fee = None
            leadteam_fee = lt_fee

            # Total marketing spend
            meta_sp  = _weekly_float(w["meta_spend"])
            gads_sp  = _weekly_float(w["gads_spend"])
            gr_sp    = _weekly_float(w["gr_spend"])
            other_sp = _weekly_float(w["other_spend"])

            if lt_fee or (is_active and wn >= 1):
                total_spend = round(
                    (meta_sp or 0) + (gads_sp or 0) + (gr_sp or 0) + (other_sp or 0) + (lt_fee or 0), 2
                ) or None
            else:
                total_spend = None

            # CPL / CPA
            blended_cpl = blended_cpa = None
            if total_spend is not None and w["leads"] > 0:
                blended_cpl = round(total_spend / w["leads"], 2)
            if total_spend is not None and w["presales"] > 0:
                blended_cpa = round(total_spend / w["presales"], 2)

            entry = {
                "week":               wk_label,
                "date_range":         dr,
                "date_start":         ws_date.isoformat() if ws_date else None,
                "date_end":           we_date.isoformat(),
                "new_leads":          _weekly_int(w["leads"]),
                "total_leads":        tl,
                "digital_leads_week": _weekly_int(w["digital_leads"]),
                "presales_week":      _weekly_int(w["presales"]),
                "presales_count":     pc,
                "cancellations_week": _weekly_int(w["cancellations"]),
                "cancellations_count": cc,
                "grassroots_leads":   _weekly_int(w["gr_leads"]),
                "grassroots_presales": _weekly_int(w["gr_presales"]),
                "digital_presales_week":       _weekly_int(w["digital_presales"]),
                "digital_cancellations_week":  _weekly_int(w["digital_cancellations"]),
                "conversion_rate":    conv_rate,
                "comm_events":        _weekly_int(w["comm_events"]),
                "ig_new_followers":   w["ig"],
                "meta_spend":         meta_sp if (is_active and wn >= 1) else None,
                "google_spend":       gads_sp if (is_active and wn >= 1) else None,
                "grassroots_spend":   gr_sp if (is_active and wn >= 1) else None,
                "other_spend":        other_sp if (is_active and wn >= 1) else None,
                "leadteam_fee":       leadteam_fee,
                "total_marketing_spend": total_spend,
            }

            # Apply manual spend overrides (fixed values that won't be overwritten)
            ov = spend_overrides.get(code, {})
            wk_str = str(wn)
            for spend_key in ("meta_spend", "google_spend", "grassroots_spend", "other_spend"):
                if wk_str in ov.get(spend_key, {}):
                    entry[spend_key] = ov[spend_key][wk_str]
            if any(wk_str in ov.get(k, {}) for k in ("meta_spend", "google_spend", "grassroots_spend", "other_spend")):
                new_total = (
                    (entry.get("meta_spend") or 0) +
                    (entry.get("google_spend") or 0) +
                    (entry.get("grassroots_spend") or 0) +
                    (entry.get("other_spend") or 0) +
                    (entry.get("leadteam_fee") or 0)
                )
                entry["total_marketing_spend"] = new_total or None
                blended_cpl = round(new_total / w["leads"], 2) if w["leads"] > 0 else None
                blended_cpa = round(new_total / w["presales"], 2) if w["presales"] > 0 else None

            entry.update({
                "blended_cpl":        blended_cpl,
                "blended_cpa":        blended_cpa,
                "estimated_day1_rmr": None,
            })

            ig_str = str(w["ig"]) if w["ig"] is not None else "-"
            print(
                f"  W{wn:02d} {dr[:22]:<23} "
                f"+{w['leads']:4d}L +{w['presales']:3d}P -{w['cancellations']:2d}C "
                f"grL={w['gr_leads']:3d} grP={w['gr_presales']:2d} "
                f"ev={w['comm_events']:2d} "
                f"spend=${total_spend or 0:8.2f} "
                f"ig={ig_str}"
            )

            weeks_out.append(entry)

        # Attribute all IG followers not captured by the API's 30-day window to Week 0.
        # This ensures the cumulative always equals current_followers (total account followers).
        # VA-001 (Reston) handles its own calibration in patch_reston_early_weeks.py.
        cf_total = ig_current_followers.get(code)
        if cf_total is not None and weeks_out and code != "VA-001":
            accounted = sum(w.get("ig_new_followers") or 0 for w in weeks_out)
            gap = max(0, cf_total - accounted)
            if gap > 0:
                w0 = next((w for w in weeks_out if w["week"] == "Week 0"), None)
                if w0 is not None:
                    w0["ig_new_followers"] = (w0["ig_new_followers"] or 0) + gap
                    print(f"  IG gap {gap} added to Week 0 (total {cf_total}, accounted {accounted})")

        studio_entry = {
            "name":                 cfg["name"],
            "code":                 code,
            "full_name":            full,
            "data_name":            cfg["data_name"],
            "targets":              cfg["targets"],
            "co_week":              co_wk,
            "go_week":              go_wk,
            "opening_date":         cfg["opening_date"].isoformat() if cfg["opening_date"] else None,
            "goal_announce_target": cfg.get("goal_announce_target"),
            "milestones":           cfg.get("milestones", []),
            "current_week":         cw_num,
            "weeks":                weeks_out,
        }
        output_studios.append(studio_entry)

    if dry_run:
        print("\n[DRY RUN] Not writing.")
        return

    # Preserve tier_rmr_by_week from previous file — that field is written by
    # fetch_tier_rmr.py (requires Snowflake) and must not be wiped on each rebuild.
    out_path = ROOT / "nso_scorecard_data.json"
    if out_path.exists():
        try:
            with open(out_path, encoding="utf-8") as f:
                prev = json.load(f)
            prev_map = {s["code"]: s for s in prev.get("studios", [])}
            for s in output_studios:
                prev_s = prev_map.get(s["code"], {})
                if prev_s.get("tier_rmr_by_week") is not None:
                    s["tier_rmr_by_week"] = prev_s["tier_rmr_by_week"]
                if prev_s.get("pricing") is not None:
                    s["pricing"] = prev_s["pricing"]
        except Exception:
            pass

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"studios": output_studios}, f, indent=2, default=str)
    size_kb = out_path.stat().st_size // 1000
    print(f"\nOK  Written {out_path}  ({size_kb} KB)")


if __name__ == "__main__":
    main()
