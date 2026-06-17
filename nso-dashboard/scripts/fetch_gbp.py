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
# Complete studio list (OFFICIAL_NAME_MBO).
# location_id is the GBP Performance API numeric ID — NOT a Google Maps CID.
# Studios with only a short Maps URL cannot have their location_id resolved
# automatically; they are left empty and skipped during the API fetch until
# the ID is confirmed manually.
# ---------------------------------------------------------------------------

ALL_STUDIOS = [
    # -- Florida ---------------------------------------------------------------
    {"name": "SWEAT440 Aventura",                  "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Boca Raton",                "code": "S440-Boca",         "location_id": "7065200393670588135"},
    {"name": "SWEAT440 Coral Gables",              "code": "S440-Gables",       "location_id": "7463859448263219148"},
    {"name": "SWEAT440 Coral Springs",             "code": "S440-CoralSprings", "location_id": "664231527795610789"},
    {"name": "SWEAT440 Deerfield Beach",           "code": "S440-Deerfield",    "location_id": "10754559321713930787"},
    {"name": "SWEAT440 Doral",                     "code": "S440-Doral",        "location_id": "5492541826748651334"},
    {"name": "SWEAT440 Fort Lauderdale - Las Olas","code": "S440-LasOlas",      "location_id": "15716475017336552276"},
    {"name": "SWEAT440 Fort Myers",                "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Miami Beach",               "code": "S440-SOBE",         "location_id": "7903117717083019565"},
    {"name": "SWEAT440 Miami - Brickell",          "code": "S440-Brickell",     "location_id": "14204655201727465322"},
    {"name": "SWEAT440 Miami - Coconut Grove",     "code": "S440-Grove",        "location_id": "13239535872064474502"},
    {"name": "SWEAT440 Miami Lakes",               "code": "S440-Lakes",        "location_id": "17771088988285686232"},
    {"name": "SWEAT440 Miami - Midtown",           "code": "S440-Midtown",      "location_id": "7167540952370862788"},
    {"name": "SWEAT440 Miami - Upper East Side",   "code": "S440-UES",          "location_id": "10716690281924579206"},
    {"name": "SWEAT440 Miramar",                   "code": "S440-Miramar",      "location_id": "17466150247466580313"},
    {"name": "SWEAT440 Naples - Mercato",          "code": "S440-Naples",       "location_id": "9241286551304249574"},
    {"name": "SWEAT440 North Miami",               "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Orlando - Dr Phillips",     "code": "S440-Orlando",      "location_id": "11294882594993712026"},
    {"name": "SWEAT440 Pembroke Pines",            "code": "S440-Pines",        "location_id": "7667757151493569095"},
    {"name": "SWEAT440 Pinecrest - Palmetto Bay",  "code": "S440-Pinecrest",    "location_id": "13145255458617855723"},
    {"name": "SWEAT440 South Miami",               "code": "S440-SouthMiami",   "location_id": "6035103980871581769"},
    {"name": "SWEAT440 St. Petersburg",            "code": "",                  "location_id": ""},
    {"name": "SWEAT440 West Palm Beach",           "code": "S440-WPB",          "location_id": "17821267300280087598"},
    # -- New York --------------------------------------------------------------
    {"name": "SWEAT440 NYC - Chelsea",             "code": "S440-Chelsea",      "location_id": "17441960021947392889"},
    {"name": "SWEAT440 NYC - FiDi",                "code": "S440-FIDI",         "location_id": "2621266453224563061"},
    {"name": "SWEAT440 NYC - Park Slope",          "code": "S440-ParkSlope",    "location_id": "17704049939806391312"},
    {"name": "SWEAT440 Eastchester",               "code": "S440-Eastchester",  "location_id": "9100379360747055617"},
    # -- New Jersey ------------------------------------------------------------
    {"name": "SWEAT440 Middletown",                "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Ocean Township",            "code": "S440-OceanTownship","location_id": "14043102866859948554"},
    {"name": "SWEAT440 Old Bridge",                "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Toms River",                "code": "S440-TomsRiver",    "location_id": "11096005039049334458"},
    {"name": "SWEAT440 Wall Township",             "code": "S440-WallTownship", "location_id": "8536971399907740515"},
    # -- Texas -----------------------------------------------------------------
    {"name": "SWEAT440 Austin - Highland",         "code": "S440-Highland",     "location_id": "1169708896465579865"},
    {"name": "SWEAT440 Austin - Zilker",           "code": "S440-Zilker",       "location_id": "13711200876115787193"},
    {"name": "SWEAT440 Dallas - Prestonwood",      "code": "S440-Prestonwood",  "location_id": "11402535545027699120"},
    {"name": "SWEAT440 Dallas - Uptown",           "code": "",                  "location_id": ""},
    # -- Virginia --------------------------------------------------------------
    {"name": "SWEAT440 Reston",                    "code": "S440-Reston",       "location_id": "10767130387921211013"},
    # -- Utah ------------------------------------------------------------------
    {"name": "SWEAT440 Herriman",                  "code": "S440-Herriman",     "location_id": "4243744174605320602"},
    # -- North Carolina --------------------------------------------------------
    {"name": "SWEAT440 Charlotte - NoDa",          "code": "S440-NoDa",         "location_id": "13151717982539622083"},
    {"name": "SWEAT440 Charlotte #2",              "code": "",                  "location_id": ""},
    # -- Tennessee -------------------------------------------------------------
    {"name": "SWEAT440 Nashville - Capitol View",  "code": "S440-CapitolView",  "location_id": ""},
    {"name": "SWEAT440 Nashville - Music Row",     "code": "",                  "location_id": ""},
    # -- Georgia ---------------------------------------------------------------
    {"name": "SWEAT440 Dunwoody",                  "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Roswell",                   "code": "",                  "location_id": ""},
    # -- Oklahoma --------------------------------------------------------------
    {"name": "SWEAT440 OKC - Rose Creek",          "code": "",                  "location_id": ""},
    # -- Arizona ---------------------------------------------------------------
    {"name": "SWEAT440 Tucson",                    "code": "",                  "location_id": ""},
    # -- Alabama ---------------------------------------------------------------
    {"name": "SWEAT440 Huntsville",                "code": "",                  "location_id": ""},
    {"name": "SWEAT440 Madison",                   "code": "S440-Madison",      "location_id": "8346193268583733484"},
    # -- California ------------------------------------------------------------
    {"name": "SWEAT440 Long Beach",                "code": "",                  "location_id": ""},
    # -- Vermont ---------------------------------------------------------------
    {"name": "SWEAT440 Burlington",                "code": "",                  "location_id": ""},
    # -- Canada ----------------------------------------------------------------
    {"name": "SWEAT440 Jean Talon",                "code": "",                  "location_id": ""},
]

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
    Fetch GBP data for all studios in ALL_STUDIOS (or an explicit override list).

    Only studios with a non-empty location_id are fetched; the rest are skipped
    with a printed message.

    Args:
        start_date: ISO date string "YYYY-MM-DD"
        end_date:   ISO date string "YYYY-MM-DD"
        studios:    Optional override list (defaults to ALL_STUDIOS)

    Returns:
        Output dict ready to be serialised to JSON.
    """
    token = _get_access_token()

    if studios is None:
        studios = ALL_STUDIOS

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
