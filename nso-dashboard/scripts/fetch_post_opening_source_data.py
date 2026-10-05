#!/usr/bin/env python3
"""
fetch_post_opening_source_data.py — Full daily leads + net presales history,
by source, for the NSO studios compared on post-opening.html.

Why this exists — two separate gaps in the pipelines this dashboard would
otherwise depend on:

1. Leads: data.json (built by fetch_data.py at the repo root) only pulls
   MARKETING_REPORTS.PUBLIC.LEADS from the start of the current calendar
   year forward. Fine for the Open Studio dashboard, but some studios here
   (e.g. Reston, Week 1 = 2025-11-09) started their presale campaign before
   that cutoff.

2. Presales/cancellations: nso_sales_data.json (fetch_nso_sales.py) has the
   same year-start cutoff (DEFAULT_START = 2026-01-01), AND deliberately
   stops presale tracking at opening_date - 1 for studios that have already
   opened (OPENED_STUDIOS) — so even a studio with full-year coverage has no
   row at all for its own opening day, freezing cumulative presales one day
   short right when this dashboard's chart needs it most.

This script re-runs both queries with no date restriction and no opening-date
cap, scoped to just the studios this dashboard cares about, and merges them
into one row per (date, studio, source). It does not touch data.json,
nso_sales_data.json, fetch_data.py, fetch_nso_sales.py, or any other
pipeline/dashboard's data — output goes to its own dedicated file.

It also feeds post-opening.html's "First 90 Days" tab, so each row carries the
Open Studio funnel too (first_visits / first_activations / first_sales, same
MARKETING_REPORTS.PUBLIC.LEADS view as data.json) plus first activations split
into members_presale (client holds a net presale purchase at that studio — they
activate around opening) and members_new (everyone else). Because post-opening
data keeps growing, nso_daily_refresh.yml runs this every day.

Run it manually when a new studio needs to be added to post-opening.html's
STUDIO_CFG:

Usage:
  python nso-dashboard/scripts/fetch_post_opening_source_data.py
  python nso-dashboard/scripts/fetch_post_opening_source_data.py --studios "Naples - Mercato" "Reston" "New Studio"

When adding a studio, pass the FULL existing list plus the new one (the
script overwrites post_opening_source_data.json each run, it doesn't merge
with the previous file), and make sure the new studio has a snowflake_id in
studios.json.
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

DEFAULT_STUDIOS = ["Naples - Mercato", "Reston", "Herriman"]
OUTPUT_PATH = Path(__file__).parent.parent / "post_opening_source_data.json"

# Same source-attribution CASE used by fetch_nso_sales.py, kept in sync so
# "source" means the same thing across the NSO dashboard family.
_SOURCE_CASE = """
    CASE
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'facebook lead ad'    THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'instagram'            THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%facebook%'
         AND LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%lead%'            THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%instagram%'       THEN 'Meta Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'meta ads'            THEN 'Meta Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'google ads'          THEN 'Google Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'tiktok ads'          THEN 'TikTok Ads'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'local listings'      THEN 'Local Listings'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'internet / ai search' THEN
             IFF(CAST(c.SIGNEDUP_DATE AS DATE) < '2026-03-18'::DATE, 'Google Ads', 'Internet / AI Search')
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'local event'         THEN 'Grassroots'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'print ads / signs'   THEN 'Print Ads / Signs'
        WHEN LOWER(TRIM(c.REFERRED_BY)) = 'social media'        THEN 'Social Media Organic'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'branded web app (bwa)'
                                                                 THEN 'Website (unattributed)'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('branded mobile app (bma)','consumer mode')
                                                                 THEN 'SWEAT440 App'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('business app','business mode')
                                                                 THEN 'Business Mode / App'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('mindbody app','mindbody web')
                                                                 THEN 'MindBody App'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'public api'
         AND LOWER(TRIM(c.REFERRED_BY)) IN ('drive by','flyer','frederick',
                                             'holly met outside of gym.','internet','n/a',
                                             'newspaper','other','radio','tv / streaming')
                                                                 THEN 'Website (unattributed)'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('another client','word of mouth')
                                                                 THEN 'Word of Mouth'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('drive by','flyer','frederick',
                                             'holly met outside of gym.','internet','n/a',
                                             'newspaper','other','radio','tv / streaming')
                                                                 THEN 'Other'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'public api'           THEN 'Public API'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('classpass','wellhub','wellness passport')
                                                                 THEN 'ClassPass / Platforms'
        ELSE 'N/A'
    END
"""


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def strip_brand(name):
    if not name:
        return name
    return name.replace("SWEAT440 - ", "").replace("SWEAT440 ", "")


def fetch_leads(cur, studio_names):
    like_clauses = " OR ".join(f"STUDIO_NAME ILIKE '%{s}%'" for s in studio_names)
    print(f"Fetching full daily leads/funnel history (MARKETING_REPORTS.PUBLIC.LEADS)...")
    cur.execute(f"""
        SELECT EVENT_DATE, STUDIO_NAME, SOURCE,
               SUM(SIGNUPS), SUM(FIRST_VISITS), SUM(FIRST_ACTIVATIONS), SUM(FIRST_SALES)
        FROM MARKETING_REPORTS.PUBLIC.LEADS
        WHERE ({like_clauses})
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    rows = cur.fetchall()
    print(f"  {len(rows):,} rows")
    return {
        (json_serial(r[0]), strip_brand(r[1]), r[2]): {
            "signups":           int(r[3] or 0),
            "first_visits":      int(r[4] or 0),
            "first_activations": int(r[5] or 0),
            "first_sales":       int(r[6] or 0),
        }
        for r in rows
    }


def fetch_presales(cur, studio_ids):
    id_list = ",".join(str(i) for i in studio_ids)
    print(f"Fetching full daily presales/cancellations history (MART_SALES_DETAILS)...")
    sql = f"""
    WITH txn AS (
        SELECT
            STUDIO_ID, EMAIL_ID, PRODUCT_DESCRIPTION,
            SALE_DATE::DATE AS effective_date,
            QUANTITY, IS_RETURN
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
        WHERE STUDIO_ID IN ({id_list})
          AND ITEM_TYPE = 'Pricing Option'
          AND LOWER(PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
          AND (EMAIL_ID IS NULL OR (
              LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
          ))
    ),
    leads_src AS (
        SELECT
            LOWER(TRIM(CLIENT_EMAIL)) AS email, STUDIO_ID, LEAD_SOURCE,
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
        WHERE STUDIO_ID IN ({id_list})
    ),
    clients_src AS (
        SELECT
            LOWER(TRIM(EMAIL_ID)) AS email, STUDIO_ID, REFERRED_BY,
            CAST(SIGNEDUP_DATE AS DATE) AS SIGNEDUP_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(EMAIL_ID)), STUDIO_ID
                ORDER BY SIGNEDUP_DATE ASC
            ) AS rn
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS
        WHERE STUDIO_ID IN ({id_list})
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
    )
    SELECT
        t.STUDIO_ID,
        t.effective_date AS date,
        COALESCE({_SOURCE_CASE}, 'N/A') AS source,
        COUNT(CASE WHEN t.QUANTITY = 1  AND t.IS_RETURN = 0 THEN 1 END) AS presales,
        COUNT(CASE WHEN t.QUANTITY = -1 OR  t.IS_RETURN = 1 THEN 1 END) AS cancellations
    FROM txn t
    LEFT JOIN leads_src  l ON LOWER(TRIM(t.EMAIL_ID)) = l.email AND t.STUDIO_ID = l.STUDIO_ID AND l.rn = 1
    LEFT JOIN clients_src c ON LOWER(TRIM(t.EMAIL_ID)) = c.email AND t.STUDIO_ID = c.STUDIO_ID AND c.rn = 1
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
    cur.execute(sql)
    rows = cur.fetchall()
    print(f"  {len(rows):,} rows")
    return rows


def fetch_member_split(cur, studio_ids):
    """First membership activations per (date, studio, source), split by
    whether the client holds a net presale purchase at that studio. Uses
    LEADS_LIST — the client-level version of the LEADS view — so source and
    activation date match first_activations above."""
    id_list = ",".join(str(i) for i in studio_ids)
    print("Fetching first activations split presale vs new (LEADS_LIST)...")
    cur.execute(f"""
        WITH presale AS (
            SELECT STUDIO_ID, LOWER(TRIM(EMAIL_ID)) AS email,
                   SUM(CASE WHEN QUANTITY = 1 AND IS_RETURN = 0 THEN 1 ELSE 0 END)
                 - SUM(CASE WHEN QUANTITY = -1 OR IS_RETURN = 1 THEN 1 ELSE 0 END) AS net
            FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
            WHERE STUDIO_ID IN ({id_list})
              AND ITEM_TYPE = 'Pricing Option'
              AND LOWER(PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
            GROUP BY 1, 2
        )
        SELECT l.FIRST_MEMBERSHIP_ACTIVATION_DATE::DATE, l.STUDIO_ID, l.SOURCE,
               COUNT_IF(p.net > 0)                   AS members_presale,
               COUNT_IF(p.net IS NULL OR p.net <= 0) AS members_new
        FROM MARKETING_REPORTS.PUBLIC.LEADS_LIST l
        LEFT JOIN presale p
               ON p.STUDIO_ID = l.STUDIO_ID AND p.email = LOWER(TRIM(l.EMAIL_ID))
        WHERE l.STUDIO_ID IN ({id_list})
          AND l.FIRST_MEMBERSHIP_ACTIVATION_DATE IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    rows = cur.fetchall()
    print(f"  {len(rows):,} rows")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--studios", nargs="+", default=DEFAULT_STUDIOS,
                         help="Studio names (canonical, no SWEAT440 prefix) to include")
    args = parser.parse_args()

    by_name = studios_registry.by_name()
    id_to_name = {}
    for name in args.studios:
        s = by_name.get(name)
        if not s or not s.get("snowflake_id"):
            print(f"  WARNING: no snowflake_id for {name!r} — skipping its presales/cancellations")
            continue
        id_to_name[s["snowflake_id"]] = name
    if not id_to_name:
        print("ERROR: none of the requested studios have a snowflake_id in studios.json")
        return

    print("Connecting to Snowflake...")
    conn = get_conn()
    cur = conn.cursor()

    leads_by_key = fetch_leads(cur, args.studios)
    presale_rows = fetch_presales(cur, id_to_name.keys())
    member_rows  = fetch_member_split(cur, id_to_name.keys())

    cur.close()
    conn.close()

    # Merge: one row per (date, studio, source) — funnel from the LEADS view,
    # presales/cancellations from MART_SALES_DETAILS, member split from LEADS_LIST.
    merged = defaultdict(lambda: {"signups": 0, "presales": 0, "cancellations": 0,
                                  "first_visits": 0, "first_activations": 0, "first_sales": 0,
                                  "members_presale": 0, "members_new": 0})
    for key, funnel in leads_by_key.items():
        for k, v in funnel.items():
            merged[key][k] += v
    for studio_id, d, source, presales, cancellations in presale_rows:
        name = id_to_name.get(studio_id)
        if not name:
            continue
        key = (json_serial(d), name, source)
        merged[key]["presales"] += int(presales or 0)
        merged[key]["cancellations"] += int(cancellations or 0)
    for d, studio_id, source, m_pre, m_new in member_rows:
        name = id_to_name.get(studio_id)
        if not name:
            continue
        key = (json_serial(d), name, source)
        merged[key]["members_presale"] += int(m_pre or 0)
        merged[key]["members_new"]     += int(m_new or 0)

    daily_detail = [
        {"date": k[0], "studio": k[1], "source": k[2], **v}
        for k, v in sorted(merged.items())
    ]
    print(f"Merged into {len(daily_detail):,} daily rows")

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "studios":      args.studios,
        "daily_detail": daily_detail,
    }
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
