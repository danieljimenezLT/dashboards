#!/usr/bin/env python3
"""
fetch_pre_weeks.py — Build pre-Week-0 weekly lead buckets for NSO studios.

Reads the NSO Manual Metrics Google Sheet for:
  - organic_lead_gen_begins (col G): when organic lead gen started
  - week_0_start            (col L): first day of Week 0 (VIP presales)
  - week_1_start            (col M): first day of Week 1

Reads data.json (Snowflake leads, committed to repo) for daily lead counts.

Generates WEEK -1, -2, ... buckets counting backwards from Week 0.
Output: nso_pre_weeks_data.json
"""

import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SHEET_ID   = "1KhXfeofzltGyllmrfIm9UN2xqLUgBtjAxPhT-E0SQmk"
CREDS_PATH = Path(__file__).parent.parent / "credentials" / "service_account.json"
DATA_JSON  = Path(__file__).parent.parent.parent / "data.json"
OUTPUT_DEFAULT = Path(__file__).parent.parent / "nso_pre_weeks_data.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Sheet name → data.json studio name (only entries that differ)
NAME_MAP = {
    "Dr Phillips": "Orlando - Dr Phillips",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_week_range_header(s: str, base_year: int):
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
    if end < start:          # Dec → Jan year rollover
        end = _p(right, base_year + 1)
    return start, end


def load_weekly_events(creds_path: Path, sheet_id: str) -> dict:
    """
    Read 'Weekly Events & Spend' tab.
    Returns {studio_name_lower: [(cal_start, cal_end, events_int), ...]}
    """
    creds = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)
    gc    = gspread.authorize(creds)
    ws    = gc.open_by_key(sheet_id).worksheet("Weekly Events & Spend")
    rows  = ws.get_all_values()
    if len(rows) < 2:
        return {}

    m = re.search(r'\b(20\d\d)\b', ' '.join(rows[0]))
    base_year = int(m.group(1)) if m else date.today().year

    hdr = rows[1]
    week_ranges = []   # (col_idx, start_date, end_date)
    for ci, h in enumerate(hdr):
        if ci < 2:
            continue
        parsed = _parse_week_range_header(h, base_year)
        if parsed:
            week_ranges.append((ci, parsed[0], parsed[1]))

    result: dict = {}
    for row in rows[2:]:
        name   = (row[0] if row else '').strip()
        metric = (row[1] if len(row) > 1 else '').strip().lower()
        if not name or ('community' not in metric and 'event' not in metric):
            continue
        entries = []
        for ci, wstart, wend in week_ranges:
            raw = row[ci].strip() if ci < len(row) else ''
            try:
                ev = int(float(raw.replace(',', ''))) if raw and raw != '-' else 0
            except (ValueError, TypeError):
                ev = 0
            if ev > 0:
                entries.append((wstart, wend, ev))
        result[name.lower()] = entries
    return result


def events_in_period(events_by_studio: dict, studio_name: str,
                     period_start: date, period_end: date):
    """Sum community events from calendar weeks overlapping [period_start, period_end]."""
    name = studio_name.strip().lower()
    if name.startswith("sweat440 "):
        name = name[9:]
    entries = None
    for k, v in events_by_studio.items():
        if k in name or name in k:
            entries = v
            break
    if not entries:
        return None
    total = sum(ev for wstart, wend, ev in entries
                if wstart <= period_end and wend >= period_start)
    return total if total > 0 else None


def _parse_date(val: str) -> date | None:
    if not val or val.strip().lower() in ("n/a", "", "tbd", "-"):
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            continue
    return None


def load_sheet_config() -> list[dict]:
    creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
    gc    = gspread.authorize(creds)
    ws    = gc.open_by_key(SHEET_ID).worksheet("NSO Config")
    rows  = ws.get_all_values()

    header = [h.strip().lower() for h in rows[1]]

    def _col(row, name):
        try:
            return row[header.index(name)].strip()
        except (ValueError, IndexError):
            return ""

    out = []
    for row in rows[2:]:
        name = _col(row, "name")
        code = _col(row, "code")
        if not name or not code:
            continue
        out.append({
            "name":         name,
            "code":         code,
            "week_0_start": _parse_date(_col(row, "week 0 start")),
            "week_1_start": _parse_date(_col(row, "week 1 start")),
            "lt_fee_start": _parse_date(_col(row, "lt fees start date")),
        })
    return out


def load_daily_leads() -> dict[str, dict[str, dict]]:
    """Returns {studio_name: {date_str: {total, grassroots}}}."""
    data = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    by_studio: dict[str, dict[str, dict]] = {}
    for row in data.get("daily_detail", []):
        studio   = row.get("studio", "")
        date_str = row.get("date", "")
        signups  = int(row.get("signups") or 0)
        src      = str(row.get("source", "")).lower()
        if not studio or not date_str:
            continue
        by_studio.setdefault(studio, {})
        by_studio[studio].setdefault(date_str, {"total": 0, "grassroots": 0})
        by_studio[studio][date_str]["total"] += signups
        if src == "grassroots":
            by_studio[studio][date_str]["grassroots"] += signups
    return by_studio


def build_pre_weeks(cfg: dict, daily_leads: dict,
                    events_by_studio: dict | None = None) -> list[dict]:
    """
    Build Week -N ... Week -1 entries using all lead data that exists
    before week_0_start. No organic start date needed — just go as far
    back as data.json has data for this studio.

    Week -1 = last 7 days before week_0_start
    Week -2 = 7 days before that
    etc.
    The earliest bucket may be shorter than 7 days.
    """
    week0_start = cfg["week_0_start"]
    if not week0_start:
        return []

    data_name     = NAME_MAP.get(cfg["name"], cfg["name"])
    leads_by_date = daily_leads.get(data_name, {})

    # Find earliest date with lead data before week_0_start.
    # Ignore isolated early outliers: skip any leading dates that are more than
    # 30 days before the next lead date (catches test/staff accounts far before
    # the real lead gen period started).
    pre_dates = sorted(d for d in leads_by_date if d < week0_start.isoformat())
    if not pre_dates:
        return []

    # Drop leading dates isolated by a 30+ day gap from the next date with leads
    while len(pre_dates) >= 2:
        d0 = date.fromisoformat(pre_dates[0])
        d1 = date.fromisoformat(pre_dates[1])
        if (d1 - d0).days > 30:
            pre_dates.pop(0)
        else:
            break

    if not pre_dates:
        return []

    earliest = date.fromisoformat(pre_dates[0])

    weeks    = []
    bucket_end = week0_start - timedelta(days=1)
    week_num   = -1

    while bucket_end >= earliest:
        bucket_start = max(bucket_end - timedelta(days=6), earliest)

        new_leads = grassroots_leads = 0
        d = bucket_start
        while d <= bucket_end:
            day_data = leads_by_date.get(d.isoformat(), {})
            new_leads       += day_data.get("total", 0)
            grassroots_leads += day_data.get("grassroots", 0)
            d += timedelta(days=1)

        weeks.append({
            "week_num":        week_num,
            "week":            f"WEEK {week_num}",
            "date_start":      bucket_start.isoformat(),
            "date_end":        bucket_end.isoformat(),
            "new_leads":       new_leads,
            "grassroots_leads": grassroots_leads if grassroots_leads > 0 else None,
            "comm_events":     events_in_period(events_by_studio or {}, cfg["name"],
                                               bucket_start, bucket_end),
            "total_leads":     None,  # filled below
        })

        bucket_end = bucket_start - timedelta(days=1)
        week_num  -= 1

    weeks.reverse()  # Week -N first, Week -1 last

    # Cumulative total_leads
    cumulative = 0
    for wk in weeks:
        cumulative        += wk["new_leads"]
        wk["total_leads"]  = cumulative

    return weeks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(OUTPUT_DEFAULT))
    args = parser.parse_args()
    global OUTPUT
    OUTPUT = Path(args.output)

    print("Reading NSO Config sheet...")
    studios = load_sheet_config()
    print(f"  {len(studios)} studio(s) found\n")

    print(f"Loading data.json from {DATA_JSON}...")
    if not DATA_JSON.exists():
        print(f"ERROR: data.json not found at {DATA_JSON}")
        sys.exit(1)
    daily_leads = load_daily_leads()
    print(f"  {len(daily_leads)} studio(s) in data.json\n")

    print("Loading Weekly Events & Spend...")
    try:
        events_by_studio = load_weekly_events(CREDS_PATH, SHEET_ID)
        print(f"  {len(events_by_studio)} studio(s) with community events data\n")
    except Exception as e:
        print(f"  WARNING: Could not load events: {e}\n")
        events_by_studio = {}

    result = {
        "generated_at": datetime.now().isoformat(),
        "source_sheet": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}",
        "studios":      {},
    }

    for cfg in studios:
        code    = cfg["code"]
        name    = cfg["name"]
        w0 = cfg["week_0_start"]
        w1 = cfg["week_1_start"]

        pre_weeks       = build_pre_weeks(cfg, daily_leads, events_by_studio)
        total_pre_leads = sum(w["new_leads"] for w in pre_weeks)

        print(f"  {name:<30}  w0={str(w0):<12}  w1={str(w1):<12}  "
              f"pre_weeks={len(pre_weeks):>3}  leads={total_pre_leads:>4}")

        lt = cfg["lt_fee_start"]
        result["studios"][code] = {
            "name":         name,
            "full_name":    f"SWEAT440 {name}",
            "week_0_start": w0.isoformat() if w0 else None,
            "week_1_start": w1.isoformat() if w1 else None,
            "lt_fee_start": lt.isoformat() if lt else None,
            "pre_weeks":    pre_weeks,
        }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    size_kb = OUTPUT.stat().st_size / 1000
    print(f"\nDone. Written to {OUTPUT} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
