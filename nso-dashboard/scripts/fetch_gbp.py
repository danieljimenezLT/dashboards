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
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------

TOKEN_URL = "https://oauth2.googleapis.com/token"
PERFORMANCE_API = "https://businessprofileperformance.googleapis.com/v1"
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"

METRICS = [
    "BUSINESS_IMPRESSIONS_DESKTOP_MAPS",
    "BUSINESS_IMPRESSIONS_DESKTOP_SEARCH",
    "BUSINESS_IMPRESSIONS_MOBILE_MAPS",
    "BUSINESS_IMPRESSIONS_MOBILE_SEARCH",
    "CALL_CLICKS",
    "WEBSITE_CLICKS",
    "BUSINESS_DIRECTION_REQUESTS",
]

# Spreadsheet that holds studio config including GBP_LINK column
STUDIOS_SHEET_ID  = "1vnONe0LTxKsrJAbcH4CM1w5vzQ7PcADN7FncoPV3qL0"
STUDIOS_SHEET_TAB = "General"

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
# Load studio list from Google Sheets
# Falls back to empty list (caller must handle missing location IDs gracefully)
# ---------------------------------------------------------------------------

def load_studios_from_sheet(token: str) -> list:
    """
    Read the 'General' tab of the studios spreadsheet and return a list of
    {"name": ..., "code": ..., "location_id": ...} dicts for every row that
    has a non-empty GBP_LINK value.

    Expected columns (case-insensitive header match):
        STUDIO_NAME  or  Name  or  Studio   → studio display name
        STUDIO_CODE  or  Code               → short code (e.g. FL-001)
        GBP_LINK                            → GBP profile URL or numeric ID
    """
    url = f"{SHEETS_API}/{STUDIOS_SHEET_ID}/values/{STUDIOS_SHEET_TAB}"
    try:
        resp = requests.get(url, headers=_headers(token), timeout=30)
        if resp.status_code == 403:
            print("  WARNING: Sheets API returned 403 — token may lack spreadsheets scope.")
            print("           Re-authorize with scope: https://www.googleapis.com/auth/spreadsheets.readonly")
            return []
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"  WARNING: Could not read studios spreadsheet: {exc}")
        return []

    rows = data.get("values", [])
    if not rows:
        print("  WARNING: Studios spreadsheet is empty.")
        return []

    # Find column indices from the header row (case-insensitive)
    header = [h.strip().upper() for h in rows[0]]

    def _col(candidates):
        for c in candidates:
            try:
                return header.index(c.upper())
            except ValueError:
                pass
        return None

    name_col = _col(["STUDIO_NAME", "NAME", "STUDIO", "LOCATION"])
    code_col = _col(["STUDIO_CODE", "CODE", "ID"])
    gbp_col  = _col(["GBP_LINK", "GBP_URL", "GBP"])

    if gbp_col is None:
        print(f"  WARNING: No GBP_LINK column found in sheet. Headers: {rows[0]}")
        return []
    if name_col is None:
        print(f"  WARNING: No studio name column found in sheet. Headers: {rows[0]}")
        return []

    studios = []
    for row in rows[1:]:
        # Pad short rows
        while len(row) <= max(filter(None.__ne__, [name_col, code_col, gbp_col])):
            row.append("")
        name     = row[name_col].strip() if name_col is not None else ""
        code     = row[code_col].strip() if code_col is not None else ""
        gbp_raw  = row[gbp_col].strip()  if gbp_col  is not None else ""
        loc_id   = _extract_location_id(gbp_raw)

        if not name:
            continue  # skip blank rows

        # Normalise name: ensure it starts with "SWEAT440 "
        if name and not name.upper().startswith("SWEAT440"):
            name = "SWEAT440 " + name

        studios.append({"name": name, "code": code, "location_id": loc_id})

    found = sum(1 for s in studios if s["location_id"])
    print(f"  Loaded {len(studios)} studios from sheet, {found} have a GBP location ID.")
    return studios


# Backwards-compatible alias — populated at runtime by fetch_all_studios
NSO_STUDIOS = []
ALL_STUDIOS = []


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


def fetch_reviews(location_id: str, token: str) -> None:
    """
    Stub: returns None.

    The reviews endpoint requires an account ID:
        GET https://mybusiness.googleapis.com/v4/accounts/{account}/locations/{id}/reviews
    Account IDs are not yet available for all NSO studios. Once they are,
    implement this to return:
        {
            "average_rating": 4.8,
            "total_count": 120,
            "recent": [
                {"rating": 5, "comment": "...", "date": "2026-01-01"},
                ...
            ]
        }
    """
    return None


# ---------------------------------------------------------------------------
# Multi-studio runner
# ---------------------------------------------------------------------------


def fetch_all_studios(
    start_date: str,
    end_date: str,
    studios: list | None = None,
) -> dict:
    """
    Fetch GBP data for all studios.

    Studio list is loaded dynamically from the Google Sheets config spreadsheet
    (GBP_LINK column on the 'General' tab). Pass an explicit list via studios
    to skip the sheet lookup (used by run_all.py / single-studio mode).

    Args:
        start_date: ISO date string "YYYY-MM-DD"
        end_date:   ISO date string "YYYY-MM-DD"
        studios:    Optional override list

    Returns:
        Output dict ready to be serialised to JSON.
    """
    token = _get_access_token()

    if studios is None:
        print("\nLoading studio list from Google Sheets...")
        studios = load_studios_from_sheet(token)
        if not studios:
            print("  Sheet returned no studios — nothing to fetch.")
            return {
                "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                "date_range": {"start": start_date, "end": end_date},
                "studios": [],
                "errors": ["Could not load studio list from spreadsheet"],
            }

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
            reviews = fetch_reviews(location_id, token)
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
        default="2025-01-01",
        help="Start date in YYYY-MM-DD format (default: 2025-01-01).",
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
    return parser.parse_args()


def main() -> None:
    """CLI entry point — fetch GBP data for all NSO studios and write JSON."""
    args = _parse_args()
    start_date = args.start
    end_date = args.end

    print(f"Fetching GBP data: {start_date} -> {end_date}")
    print(f"Output: {args.output}\n")

    result = fetch_all_studios(start_date, end_date)

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
