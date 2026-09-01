"""
fetch_gbp.py — Pull Google Business Profile performance data for NSO studios.

Fetches daily performance metrics per location using the Business Profile
Performance API. Reviews endpoint is stubbed (no account IDs available yet).

Output JSON format:
{
    "generated_at": "2026-01-01T00:00:00",
    "date_range": {"start": "2025-01-01", "end": "2025-12-31"},
    "studios": [
        {
            "name": "Herriman",
            "code": "herriman",
            "location_id": "01689314637450990290",
            "daily": [
                {
                    "date": "2025-01-01",
                    "total_views": 150,
                    "desktop_maps": 20,
                    "desktop_search": 80,
                    "mobile_maps": 30,
                    "mobile_search": 20,
                    "calls": 5,
                    "website_clicks": 12,
                    "direction_requests": 8
                },
                ...
            ],
            "reviews": null
        },
        ...
    ]
}

Credentials (from environment / .env):
    GBP_CLIENT_ID
    GBP_CLIENT_SECRET
    GBP_REFRESH_TOKEN
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

NSO_CONFIG_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Ku0VSwOY6HVXuqucduWlsbKIiNzb0ojL21rozaPpHHU"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------

TOKEN_URL = "https://oauth2.googleapis.com/token"
PERFORMANCE_API = "https://businessprofileperformance.googleapis.com/v1"
MYBUSINESS_API  = "https://mybusiness.googleapis.com/v4"
ACCT_MGMT_API   = "https://mybusinessaccountmanagement.googleapis.com/v1"

METRICS = [
    "BUSINESS_IMPRESSIONS_DESKTOP_MAPS",
    "BUSINESS_IMPRESSIONS_DESKTOP_SEARCH",
    "BUSINESS_IMPRESSIONS_MOBILE_MAPS",
    "BUSINESS_IMPRESSIONS_MOBILE_SEARCH",
    "CALL_CLICKS",
    "WEBSITE_CLICKS",
    "BUSINESS_DIRECTION_REQUESTS",
]

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _get_access_token() -> str:
    """Get a fresh OAuth2 access token using the stored refresh token."""
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": os.environ["GBP_CLIENT_ID"],
            "client_secret": os.environ["GBP_CLIENT_SECRET"],
            "refresh_token": os.environ["GBP_REFRESH_TOKEN"],
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_gbp_account_name(token: str) -> str:
    """Discover the GBP account resource name via Account Management API."""
    try:
        resp = requests.get(
            f"{ACCT_MGMT_API}/accounts",
            headers=_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        accounts = resp.json().get("accounts", [])
        return accounts[0]["name"] if accounts else ""
    except Exception as exc:
        print(f"  Warning: could not discover GBP account: {exc}")
        return ""


# ---------------------------------------------------------------------------
# Extract GBP location ID from a URL or raw string
# Handles common formats:
#   https://business.google.com/u/0/n/4243744174605320602/profile
#   https://business.google.com/n/4243744174605320602/review
#   https://maps.google.com/?cid=4243744174605320602
#   4243744174605320602   (bare numeric ID)
# ---------------------------------------------------------------------------

def _extract_location_id(raw: str) -> str:
    """Return the numeric GBP location ID from a URL or bare string, or '' if none found."""
    if not raw:
        return ""
    raw = raw.strip()
    # business.google.com/n/{id}
    m = re.search(r"business\.google\.com/(?:u/\d+/)?n/(\d+)", raw)
    if m:
        return m.group(1)
    # maps.google.com/?cid={id}
    m = re.search(r"[?&]cid=(\d+)", raw)
    if m:
        return m.group(1)
    # Bare numeric string
    if re.fullmatch(r"\d{10,}", raw):
        return raw
    return ""



# ---------------------------------------------------------------------------
# Read NSO studio GBP location IDs from the NSO Config Google Sheet.
# Returns {full_name_lower: {"location_id": str, "code": str}} for any
# studio that has a non-empty "gbp location id" column in the sheet.
# ---------------------------------------------------------------------------

def load_nso_location_ids_from_sheet(credentials_path: str) -> dict:
    try:
        from google.oauth2.service_account import Credentials
        import gspread
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(NSO_CONFIG_SHEET_URL)
        ws = sh.worksheet("NSO Config")
        all_rows = ws.get_all_values()
    except Exception as e:
        print(f"  Warning: could not read NSO Config sheet: {e}")
        return {}

    if len(all_rows) < 2:
        return {}

    header_row = all_rows[1]
    hdr_map = {h.strip().lower(): i for i, h in enumerate(header_row)}

    def _col(row, *names):
        for name in names:
            i = hdr_map.get(name.lower(), -1)
            if 0 <= i < len(row) and row[i].strip():
                return row[i].strip()
        return ""

    result = {}
    for row in all_rows[2:]:
        name = row[0].strip() if row else ""
        code = row[1].strip() if len(row) > 1 else ""
        if not name or not code:
            continue
        raw_loc = _col(row, "gbp location id")
        if not raw_loc:
            continue
        loc_id = _extract_location_id(raw_loc)
        if loc_id:
            full_name = f"SWEAT440 {name}".lower()
            result[full_name] = {"location_id": loc_id, "code": code}
            print(f"  Sheet GBP: {name} ({code}) → location_id={loc_id}")

    return result


# ---------------------------------------------------------------------------
# Complete studio list (OFFICIAL_NAME_MBO), sourced from the canonical
# studios.json registry (see ../../studios.py). location_id is the GBP
# Performance API numeric ID — NOT a Google Maps CID. Studios with no
# confirmed location_id are skipped during the API fetch.
#
# NOTE: studios.json currently only tracks studios that are open, in the NSO
# pipeline, or otherwise referenced elsewhere in the repo. A handful of
# not-yet-launched locations that were in the old hardcoded list here
# (St. Petersburg, Charlotte #2, Roswell, OKC - Rose Creek, Tucson, Long
# Beach, Burlington, Jean Talon) have no registry entry and are intentionally
# NOT carried forward — add them to studios.json if/when they're confirmed.
# ---------------------------------------------------------------------------

ALL_STUDIOS = studios_registry.gbp_studio_rows()

# Backwards-compatible alias -- only studios with a confirmed location_id
NSO_STUDIOS = [s for s in ALL_STUDIOS if s["location_id"]]



# ---------------------------------------------------------------------------
# Core fetch — performance metrics
# ---------------------------------------------------------------------------


def fetch_performance_metrics(
    location_id: str,
    start_date: str,
    end_date: str,
    token: str,
) -> list:
    """
    Fetch daily performance metrics for a single GBP location.

    The Business Profile Performance API returns one metric at a time.
    Each response looks like:
        {
            "timeSeries": {
                "datedValues": [
                    {"date": {"year": 2026, "month": 1, "day": 1}, "value": "123"},
                    ...
                ]
            }
        }

    Returns a list of daily row dicts sorted by date.
    """
    loc = (
        location_id
        if location_id.startswith("locations/")
        else f"locations/{location_id}"
    )

    sd = datetime.strptime(start_date, "%Y-%m-%d")
    ed = datetime.strptime(end_date, "%Y-%m-%d")

    print(f"  Fetching GBP metrics for {loc} ({start_date} -> {end_date})...")

    # Accumulate per-date values keyed by metric name (lowercased)
    daily_data: dict = {}

    for metric in METRICS:
        url = f"{PERFORMANCE_API}/{loc}:getDailyMetricsTimeSeries"
        params = {
            "dailyMetric": metric,
            "dailyRange.startDate.year": sd.year,
            "dailyRange.startDate.month": sd.month,
            "dailyRange.startDate.day": sd.day,
            "dailyRange.endDate.year": ed.year,
            "dailyRange.endDate.month": ed.month,
            "dailyRange.endDate.day": ed.day,
        }

        try:
            resp = requests.get(
                url, headers=_headers(token), params=params, timeout=30
            )
            if resp.status_code == 429:
                print(f"    Rate limited on {metric}, waiting 5 seconds...")
                time.sleep(5)
                resp = requests.get(
                    url, headers=_headers(token), params=params, timeout=30
                )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"    Warning: error fetching {metric}: {exc}")
            continue

        # Fix: timeSeries is an OBJECT with a datedValues list, not a list itself.
        time_series = data.get("timeSeries", {})
        dated_values = time_series.get("datedValues", [])

        for point in dated_values:
            date_obj = point.get("date", {})
            year = date_obj.get("year")
            month = date_obj.get("month")
            day = date_obj.get("day")
            if not (year and month and day):
                continue
            date_str = f"{year}-{month:02d}-{day:02d}"
            if date_str not in daily_data:
                daily_data[date_str] = {"date": date_str}
            daily_data[date_str][metric.lower()] = int(point.get("value") or 0)

        time.sleep(1)

    # Build final output rows
    rows = []
    for date_str, m in sorted(daily_data.items()):
        total_views = (
            m.get("business_impressions_desktop_maps", 0)
            + m.get("business_impressions_desktop_search", 0)
            + m.get("business_impressions_mobile_maps", 0)
            + m.get("business_impressions_mobile_search", 0)
        )
        rows.append(
            {
                "date": date_str,
                "total_views": total_views,
                "desktop_maps": m.get("business_impressions_desktop_maps", 0),
                "desktop_search": m.get("business_impressions_desktop_search", 0),
                "mobile_maps": m.get("business_impressions_mobile_maps", 0),
                "mobile_search": m.get("business_impressions_mobile_search", 0),
                "calls": m.get("call_clicks", 0),
                "website_clicks": m.get("website_clicks", 0),
                "direction_requests": m.get("business_direction_requests", 0),
            }
        )

    print(f"  -> {len(rows)} daily GBP metric rows for {loc}.")
    return rows


# ---------------------------------------------------------------------------
# Reviews — stubbed until account IDs are available for all locations
# ---------------------------------------------------------------------------


def fetch_reviews(location_id: str, token: str, account_name: str = "") -> dict | None:
    """
    Fetch review summary for a GBP location.

    Paginates through all reviews to compute the 5-star count (the API only
    returns averageRating and totalReviewCount in the list response, not a
    breakdown by star rating).

    Returns:
        {
            "average_rating": 4.8,
            "total_count": 454,
            "five_star_count": 320
        }
    or None if account_name is empty or the request fails.
    """
    if not account_name:
        return None

    loc_id = location_id.removeprefix("locations/")
    url = f"{MYBUSINESS_API}/{account_name}/locations/{loc_id}/reviews"
    hdrs = _headers(token)

    total_count = 0
    avg_rating = None
    five_star_count = 0
    page_token = None

    print(f"  Fetching reviews for locations/{loc_id}...")

    while True:
        params = {"pageSize": 50}
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = requests.get(url, headers=hdrs, params=params, timeout=30)
            if resp.status_code == 429:
                print("    Rate limited on reviews, waiting 5 s...")
                time.sleep(5)
                resp = requests.get(url, headers=hdrs, params=params, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            print(f"    Warning: reviews error: {exc}")
            return None

        data = resp.json()

        if avg_rating is None:
            avg_rating = data.get("averageRating")
            total_count = data.get("totalReviewCount", 0)

        for rv in data.get("reviews", []):
            if rv.get("starRating") == "FIVE":
                five_star_count += 1

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        time.sleep(0.5)

    result = {
        "average_rating": round(float(avg_rating), 2) if avg_rating is not None else None,
        "total_count": total_count,
        "five_star_count": five_star_count,
    }
    print(f"  -> reviews: avg={result['average_rating']}, total={total_count}, 5-star={five_star_count}")
    return result


# ---------------------------------------------------------------------------
# Multi-studio runner
# ---------------------------------------------------------------------------


def fetch_all_studios(
    start_date: str,
    end_date: str,
    studios: list | None = None,
    sheet_ids: dict | None = None,
) -> dict:
    """
    Fetch GBP data for all studios in ALL_STUDIOS (or an explicit override list).

    Only studios with a non-empty location_id are fetched; the rest are skipped.
    sheet_ids: optional dict from load_nso_location_ids_from_sheet() — patches
    empty location_ids in ALL_STUDIOS and adds any new NSO studios not in the list.

    Args:
        start_date: ISO date string "YYYY-MM-DD"
        end_date:   ISO date string "YYYY-MM-DD"
        studios:    Optional override list (defaults to ALL_STUDIOS)
        sheet_ids:  Optional {full_name_lower: {location_id, code}} from sheet

    Returns:
        Output dict ready to be serialised to JSON.
    """
    token = _get_access_token()

    gbp_account_name = _get_gbp_account_name(token)
    if gbp_account_name:
        print(f"  GBP account: {gbp_account_name}")
    else:
        print("  Warning: could not discover GBP account — reviews will be skipped")

    if studios is None:
        studios = [dict(s) for s in ALL_STUDIOS]  # copy so we can mutate

    # Patch empty location_ids from sheet data and add any new NSO studios
    if sheet_ids:
        existing_names = {s["name"].lower() for s in studios}
        for full_lower, info in sheet_ids.items():
            matched = next((s for s in studios if s["name"].lower() == full_lower), None)
            if matched:
                if not matched.get("location_id"):
                    matched["location_id"] = info["location_id"]
                    matched["code"] = info["code"]
                    print(f"  Patched location_id from sheet: {matched['name']}")
            elif full_lower not in existing_names:
                # New studio not yet in the hardcoded list
                studios.append({
                    "name": full_lower.title().replace("Sweat440", "SWEAT440"),
                    "code": info["code"],
                    "location_id": info["location_id"],
                })
                print(f"  Added new studio from sheet: {full_lower}")

    studio_results = []
    errors = []

    for studio in studios:
        location_id = studio.get("location_id")
        if not location_id:
            print(f"  Skipping {studio['name']} — no location_id configured.")
            continue

        print(f"\n--- {studio['name']} ---")
        try:
            daily_rows = fetch_performance_metrics(
                location_id, start_date, end_date, token
            )
            is_nso = studio.get("status") == "nso"
            reviews = fetch_reviews(location_id, token, gbp_account_name) if not is_nso else None
        except Exception as exc:
            msg = f"{studio['name']}: {exc}"
            print(f"  Error fetching {studio['name']}: {exc}")
            errors.append(msg)
            daily_rows = []
            reviews = None

        studio_results.append(
            {
                "name": studio["name"],
                "code": studio["code"],
                "location_id": location_id,
                "daily": daily_rows,
                "reviews": reviews,
            }
        )

    result = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "date_range": {"start": start_date, "end": end_date},
        "studios": studio_results,
    }

    if errors:
        result["errors"] = errors

    return result


# ---------------------------------------------------------------------------
# Backwards-compatibility shim for run_all.py
# ---------------------------------------------------------------------------


def run(franchise_config: dict, start_date: str, end_date: str) -> dict:
    """
    Entry point called by run_all.py.

    Previously wrote to Google Sheets via sheets_writer; now returns the result
    dict and optionally writes it to gbp_data.json in the working directory.

    Args:
        franchise_config: Per-studio config dict (same shape as franchise_config.json).
                          The gbp.location_id field is used to build a single-studio list.
        start_date: ISO date string "YYYY-MM-DD"
        end_date:   ISO date string "YYYY-MM-DD"

    Returns:
        The result dict (same format as fetch_all_studios output).
    """
    gbp_cfg = franchise_config.get("gbp", {})
    location_id = gbp_cfg.get("location_id")
    studio_name = franchise_config.get("studio_name", "Unknown Studio")
    studio_code = franchise_config.get("studio_code", "unknown")

    studios_to_fetch = [
        {
            "name": studio_name,
            "code": studio_code,
            "location_id": location_id,
        }
    ]

    result = fetch_all_studios(start_date, end_date, studios=studios_to_fetch)

    # Write to file so run_all.py callers can access the output
    output_path = "gbp_data.json"
    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, ensure_ascii=False)
        print(f"  GBP data written to {output_path}")
    except OSError as exc:
        print(f"  Warning: could not write {output_path}: {exc}")

    print("  Google Business Profile done.")
    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="Fetch Google Business Profile performance data for NSO studios."
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Start date in YYYY-MM-DD format. Overrides --days when provided.",
    )
    parser.add_argument(
        "--end",
        default=yesterday,
        help=f"End date in YYYY-MM-DD format (default: yesterday = {yesterday}).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help=(
            "Number of days to look back from today when --start / --end are not "
            "both provided (default: 90)."
        ),
    )
    parser.add_argument(
        "--output",
        default="gbp_data.json",
        help="Path for the output JSON file (default: gbp_data.json).",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Path to Google service account JSON. When provided, GBP location IDs "
             "are read from the NSO Config sheet, overriding empty entries in the "
             "hardcoded list and auto-adding new studios.",
    )
    return parser.parse_args()


def _merge_with_existing(new_result: dict, output_path: str) -> dict:
    """
    Merge new fetch results into the existing gbp_data.json using upsert logic:
    - New daily rows win for overlapping dates (new data is more accurate)
    - Old daily rows for dates outside the fetch window are preserved
    - Reviews are always replaced with the latest fetch
    Returns the merged result dict.
    """
    out_path = Path(output_path)
    if not out_path.exists():
        return new_result

    try:
        with open(out_path, encoding="utf-8") as fh:
            existing = json.load(fh)
    except Exception as e:
        print(f"  WARNING: could not load existing {output_path} for merge: {e}")
        return new_result

    existing_studios = {s["name"]: s for s in existing.get("studios", [])}
    new_studios = {s["name"]: s for s in new_result.get("studios", [])}

    merged_studios = []
    all_names = list(existing_studios) + [n for n in new_studios if n not in existing_studios]

    for name in all_names:
        old = existing_studios.get(name, {})
        new = new_studios.get(name)

        if new is None:
            # Studio not in this fetch — keep existing as-is
            merged_studios.append(old)
            continue

        # Merge daily rows: upsert by date (new wins)
        old_daily = {r["date"]: r for r in old.get("daily", [])}
        new_daily = {r["date"]: r for r in new.get("daily", [])}
        old_daily.update(new_daily)
        merged_daily = sorted(old_daily.values(), key=lambda x: x["date"])

        merged_studios.append({
            **new,
            "daily": merged_daily,
            # Reviews: use new if fetched, else keep old
            "reviews": new.get("reviews") if new.get("reviews") is not None else old.get("reviews"),
        })

    old_start = existing.get("date_range", {}).get("start", new_result["date_range"]["start"])
    new_end   = new_result["date_range"]["end"]

    print(f"  Merged: historical start={old_start}, new end={new_end}, studios={len(merged_studios)}")

    return {
        "generated_at": new_result["generated_at"],
        "date_range": {"start": old_start, "end": new_end},
        "studios": merged_studios,
    }


def main() -> None:
    """CLI entry point — fetch GBP data for all NSO studios and write JSON."""
    args = _parse_args()
    end_date = args.end
    if args.start:
        start_date = args.start
    else:
        start_date = (datetime.utcnow() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    print(f"Fetching GBP data: {start_date} -> {end_date}")
    print(f"Output: {args.output}\n")

    sheet_ids = {}
    if args.credentials:
        print("Reading NSO studio GBP IDs from NSO Config sheet...")
        sheet_ids = load_nso_location_ids_from_sheet(args.credentials)
        print(f"  {len(sheet_ids)} studio(s) with GBP location ID in sheet\n")

    result = fetch_all_studios(start_date, end_date, sheet_ids=sheet_ids)

    # Merge with existing historical data so we don't re-fetch the full history
    result = _merge_with_existing(result, args.output)

    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, ensure_ascii=False)

    studios_fetched = len(result.get("studios", []))
    errors = result.get("errors", [])
    print(f"\nDone. {studios_fetched} studio(s) written to {args.output}.")
    if errors:
        print(f"Errors encountered ({len(errors)}):")
        for err in errors:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
