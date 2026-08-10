#!/usr/bin/env python3
"""
fetch_nso_sales.py — Pull presales and cancellations for all NSO studios
from MART_SALES_DETAILS in Snowflake. Pure transactional approach: every
buy (+1) and cancel (-1) transaction is counted on the day it occurs.

Key logic:
  - No dedup: all raw transactions counted, including LOC=98 rows, matching
              MindBody's raw transaction counts exactly.
  - Presales: QUANTITY=1, IS_RETURN=0, counted on their SALE_DATE.
  - Cancels : QUANTITY=-1 OR IS_RETURN=1, counted on their SALE_DATE (return
              date), matching how MindBody surfaces them in period views.
  - Source  : attributed via MART_LEADS_LOG / MART_CLIENTS per client email.
  - Revenue : GROSS_PAYMENTAMT_LOCAL (presale transactions only).

Refresh modes:
  Default (incremental): loads existing JSON, re-queries the last 14 days,
                         removes those rows from the existing data and appends
                         fresh results. Fast (~5 s in Snowflake).
  --full               : queries from DEFAULT_START to yesterday, overwrites
                         nso_sales_data.json entirely. Use after methodology
                         changes or to onboard a new studio.

Usage:
  python scripts/fetch_nso_sales.py              # incremental, 14-day lookback
  python scripts/fetch_nso_sales.py --full       # full rebuild from 2025-01-01
  python scripts/fetch_nso_sales.py --full --start 2026-01-01
  python scripts/fetch_nso_sales.py --output path/to/output.json
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

# ---------------------------------------------------------------------------
# Studio registry  {snowflake_studio_id: {name, code}} — sourced from studios.json
# ---------------------------------------------------------------------------
NSO_STUDIOS = studios_registry.nso_studios_by_snowflake_id()

ID_LIST       = ",".join(str(i) for i in NSO_STUDIOS)
DEFAULT_START = "2026-01-01"
LOOKBACK_DAYS = 14   # days re-queried on every incremental run

# ---------------------------------------------------------------------------
# Source attribution CASE — same mapping used across the dashboard
# ---------------------------------------------------------------------------
_SOURCE_CASE = """
    CASE
        -- Meta Ads (LEAD_SOURCE takes priority for paid social)
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'facebook lead ad'    THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'instagram'            THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%facebook%'
         AND LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%lead%'            THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%instagram%'       THEN 'Meta Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'meta ads'            THEN 'Meta Ads'
        -- Other paid channels
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'google ads'          THEN 'Google Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'tiktok ads'          THEN 'TikTok Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'local listings'      THEN 'Local Listings'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'internet / ai search' THEN
             IFF(CAST(c.SIGNEDUP_DATE AS DATE) < '2026-03-18'::DATE, 'Google Ads', 'Internet / AI Search')
        -- Offline / organic (REFERRED_BY — must come before owned-property LEAD_SOURCE checks)
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'local event'         THEN 'Grassroots'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'print ads / signs'   THEN 'Print Ads / Signs'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'social media'        THEN 'Social Media Organic'
        -- Owned properties (LEAD_SOURCE)
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'branded web app (bwa)'
                                                                 THEN 'Website (unattributed)'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('branded mobile app (bma)','consumer mode')
                                                                 THEN 'SWEAT440 App'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('business app','business mode')
                                                                 THEN 'Business Mode / App'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('mindbody app','mindbody web')
                                                                 THEN 'MindBody App'
        -- Word of mouth / catch-all (REFERRED_BY) before Public API fallback
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('another client','word of mouth')
                                                                 THEN 'Word of Mouth'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('drive by','flyer','frederick',
                                             'holly met outside of gym.','internet','n/a',
                                             'newspaper','other','radio','tv / streaming')
                                                                 THEN 'Other'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('public api')        THEN 'Public API'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('classpass','wellhub','wellness passport')
                                                                 THEN 'ClassPass / Platforms'
        ELSE 'N/A'
    END
"""


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


# ---------------------------------------------------------------------------
# Transactional query
# ---------------------------------------------------------------------------
def fetch_transactions(cur, start_date: str, end_date: str) -> list[dict]:
    """
    Returns one row per (studio_id, effective_date, source) with:
      presales      — count of QUANTITY=1 buy transactions
      cancellations — count of QUANTITY=-1 / IS_RETURN cancel transactions
      gross_revenue — sum of GROSS_PAYMENTAMT_LOCAL on buy transactions

    Methodology to match MindBody:
      - No dedup: all raw rows counted including LOC=98, matching MindBody exactly.
      - IS_RETURN cancellations are counted on their SALE_DATE (return date),
        consistent with how MindBody surfaces them in period reports.
    """
    sql = f"""
    WITH txn AS (
        -- All presale transactions, no dedup — matches MindBody raw counts.
        SELECT
            STUDIO_ID,
            EMAIL_ID,
            PRODUCT_DESCRIPTION,
            SALE_DATE::DATE                        AS effective_date,
            QUANTITY,
            IS_RETURN,
            COALESCE(GROSS_PAYMENTAMT_LOCAL, 0)    AS gross_revenue
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
        WHERE STUDIO_ID IN ({ID_LIST})
          AND ITEM_TYPE = 'Pricing Option'
          AND LOWER(PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
          AND (EMAIL_ID IS NULL OR (
              LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
          ))
    ),
    leads_src AS (
        -- Best lead-source record per client+studio (priority matches MARKETING_REPORTS.PUBLIC.LEADS)
        SELECT
            LOWER(TRIM(CLIENT_EMAIL)) AS email,
            STUDIO_ID,
            LEAD_SOURCE,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(CLIENT_EMAIL)), STUDIO_ID
                ORDER BY
                    CASE
                        WHEN LOWER(TRIM(LEAD_SOURCE)) = 'facebook lead ad'              THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) LIKE '%instagram%'                THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) LIKE '%facebook%'
                         AND LOWER(TRIM(LEAD_SOURCE)) LIKE '%lead%'                     THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) LIKE '%google%'                   THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) = 'public api'                    THEN 3
                        WHEN LEAD_SOURCE IS NULL                                         THEN 2
                        ELSE 1
                    END,
                    STAGE_START ASC
            ) AS rn
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_LEADS_LOG
        WHERE STUDIO_ID IN ({ID_LIST})
    ),
    clients_src AS (
        -- REFERRED_BY + SIGNEDUP_DATE per client+studio
        SELECT
            LOWER(TRIM(EMAIL_ID)) AS email,
            STUDIO_ID,
            REFERRED_BY,
            CAST(SIGNEDUP_DATE AS DATE) AS SIGNEDUP_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(EMAIL_ID)), STUDIO_ID
                ORDER BY SIGNEDUP_DATE ASC
            ) AS rn
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS
        WHERE STUDIO_ID IN ({ID_LIST})
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
    )
    SELECT
        t.STUDIO_ID,
        t.effective_date                                                     AS date,
        COALESCE({_SOURCE_CASE}, 'N/A')                                     AS source,
        COUNT(CASE WHEN t.QUANTITY = 1  AND t.IS_RETURN = 0 THEN 1 END)    AS presales,
        COUNT(CASE WHEN t.QUANTITY = -1 OR  t.IS_RETURN = 1 THEN 1 END)    AS cancellations,
        SUM(CASE WHEN t.QUANTITY = 1 AND t.IS_RETURN = 0
                 THEN t.gross_revenue ELSE 0 END)                           AS gross_revenue
    FROM txn t
    LEFT JOIN leads_src  l
           ON LOWER(TRIM(t.EMAIL_ID)) = l.email
          AND t.STUDIO_ID = l.STUDIO_ID AND l.rn = 1
    LEFT JOIN clients_src c
           ON LOWER(TRIM(t.EMAIL_ID)) = c.email
          AND t.STUDIO_ID = c.STUDIO_ID AND c.rn = 1
    WHERE t.effective_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    print(f"  {len(rows)} rows returned")
    return rows


# ---------------------------------------------------------------------------
# Existing JSON helpers
# ---------------------------------------------------------------------------
def load_existing(out_path: Path):
    """Load nso_sales_data.json and return (data_dict, max_date_str | None)."""
    if not out_path.exists():
        return None, None
    try:
        with open(out_path, encoding="utf-8") as f:
            data = json.load(f)
        max_date = None
        for s in data.get("studios", {}).values():
            for row in s.get("daily", []):
                d = row.get("date", "")
                if d and (max_date is None or d > max_date):
                    max_date = d
        return data, max_date
    except Exception as e:
        print(f"  Warning: could not read existing JSON ({e}) — will do full rebuild.")
        return None, None


# ---------------------------------------------------------------------------
# Build / merge output
# ---------------------------------------------------------------------------
def build_studios(existing_daily: dict[str, list], fresh_rows: list[dict],
                  cutoff: str) -> dict:
    """
    existing_daily : {sid_str: [rows with date < cutoff]}
    fresh_rows     : new rows from Snowflake (date >= cutoff)
    Returns complete studios dict with sorted daily arrays and totals.
    """
    studios = {}
    for sid, meta in NSO_STUDIOS.items():
        sid_str = str(sid)
        studios[sid_str] = {
            "name":   meta["name"],
            "code":   meta["code"],
            "daily":  list(existing_daily.get(sid_str, [])),
            "totals": {"presales": 0, "cancellations": 0,
                       "net_presales": 0, "gross_revenue": 0.0},
        }

    for r in fresh_rows:
        sid_str = str(int(r["studio_id"]))
        if sid_str not in studios:
            continue
        studios[sid_str]["daily"].append({
            "date":          str(r["date"])[:10],
            "source":        r.get("source") or "Unknown",
            "presales":      int(r.get("presales")      or 0),
            "cancellations": int(r.get("cancellations") or 0),
            "gross_revenue": round(float(r.get("gross_revenue") or 0), 2),
        })

    for s in studios.values():
        s["daily"].sort(key=lambda x: (x["date"], x["source"]))
        t = s["totals"]
        for row in s["daily"]:
            t["presales"]      += row["presales"]
            t["cancellations"] += row["cancellations"]
            t["gross_revenue"] += row["gross_revenue"]
        t["net_presales"]  = t["presales"] - t["cancellations"]
        t["gross_revenue"] = round(t["gross_revenue"], 2)

    return studios


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Fetch NSO presales/cancellations")
    parser.add_argument("--full",   action="store_true",
                        help="Full rebuild from DEFAULT_START (overwrites existing data)")
    parser.add_argument("--start",  default=None,
                        help="Override start date for --full mode (YYYY-MM-DD)")
    parser.add_argument("--end",    default=None,
                        help="Override end date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--output", default="nso_sales_data.json")
    args = parser.parse_args()

    out_path = Path(args.output)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_override = args.end or yesterday

    # ── Determine query window & existing data to keep ──────────────────────
    if args.full:
        start_date = args.start or DEFAULT_START
        end_date   = end_override
        existing_daily = {str(sid): [] for sid in NSO_STUDIOS}   # discard all
        cutoff         = start_date
        mode           = "FULL REBUILD"
    else:
        # Full rebuild ensures all cancels are correctly attributed regardless of
        # when the return/processing date falls relative to the lookback window.
        start_date = DEFAULT_START
        end_date   = end_override
        existing_daily = {str(sid): [] for sid in NSO_STUDIOS}
        cutoff         = start_date
        mode           = "FULL REBUILD"

    print("=" * 60)
    print(f"NSO Sales Fetch  [{mode}]")
    print(f"Query range : {start_date} to {end_date}")
    print(f"Studios     : {', '.join(m['name'].replace('SWEAT440 ','') for m in NSO_STUDIOS.values())}")
    print("=" * 60)

    try:
        conn = get_conn()
    except Exception as e:
        print(f"ERROR connecting to Snowflake: {e}")
        sys.exit(1)

    cur = conn.cursor()
    try:
        print("\nQuerying MART_SALES_DETAILS...")
        fresh_rows = fetch_transactions(cur, start_date, end_date)
    finally:
        cur.close()
        conn.close()

    studios = build_studios(existing_daily, fresh_rows, cutoff)

    print("\nSummary:")
    for s in studios.values():
        t = s["totals"]
        print(f"  {s['name'].replace('SWEAT440 ',''):<28}  "
              f"presales={t['presales']:>4}  canc={t['cancellations']:>3}  "
              f"net={t['net_presales']:>4}  revenue=${t['gross_revenue']:>10,.2f}")

    output = {
        "generated_at": datetime.now().isoformat(),
        "date_range":   {"start": DEFAULT_START, "end": end_date},
        "studios":      studios,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1000
    print(f"\nDone. Written to {out_path} ({size_kb:.1f} KB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
