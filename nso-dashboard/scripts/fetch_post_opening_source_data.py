#!/usr/bin/env python3
"""
fetch_post_opening_source_data.py — Full daily lead/sale history, by source,
for the NSO studios compared on post-opening.html.

Why this exists: data.json (built by fetch_data.py at the repo root) only
pulls MARKETING_REPORTS.PUBLIC.LEADS from the start of the current calendar
year forward (EVENT_DATE >= DATE_TRUNC('year', CURRENT_DATE())). That's fine
for the Open Studio dashboard, but post-opening.html compares NSO studios
from their true Week 0 — and some (e.g. Reston, Week 1 = 2025-11-09) started
their presale campaign before that cutoff, so their Meta/Google leads and
sales are missing for the earliest weeks.

This script queries the SAME table with no date restriction, filtered to
just the studios this dashboard cares about, so leads/sales-by-source
charts can start from each studio's actual Week 0. It does not touch
data.json, fetch_data.py, or any other pipeline/dashboard's data.

NOT part of any scheduled workflow — this data is meant to stay fixed once
a studio has opened (it's pre-opening history, it doesn't change). Run this
manually, on demand, only when a new studio needs to be added to
post-opening.html's STUDIO_CFG:

Usage:
  python nso-dashboard/scripts/fetch_post_opening_source_data.py
  python nso-dashboard/scripts/fetch_post_opening_source_data.py --studios "Naples - Mercato" "Reston" "New Studio"

When adding a studio, pass the FULL existing list plus the new one (the
script overwrites post_opening_source_data.json each run, it doesn't merge
with the previous file).
"""

import argparse
import json
import os
from datetime import datetime, date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

# ── Studios shown on post-opening.html — add more here as new studios open ──
DEFAULT_STUDIOS = ["Naples - Mercato", "Reston"]

OUTPUT_PATH = Path(__file__).parent.parent / "post_opening_source_data.json"


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
    """Same canonicalization as fetch_data.py, so studio names line up
    across data.json and this file."""
    if not name:
        return name
    return name.replace("SWEAT440 - ", "").replace("SWEAT440 ", "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--studios", nargs="+", default=DEFAULT_STUDIOS,
                         help="Studio names (canonical, no SWEAT440 prefix) to include")
    args = parser.parse_args()

    print("Connecting to Snowflake...")
    conn = get_conn()
    cur = conn.cursor()

    like_clauses = " OR ".join(f"STUDIO_NAME ILIKE '%{s}%'" for s in args.studios)
    print(f"Fetching full daily lead/sale history for: {', '.join(args.studios)}")
    cur.execute(f"""
        SELECT
            EVENT_DATE, STUDIO_NAME, SOURCE,
            SUM(SIGNUPS)     AS signups,
            SUM(FIRST_SALES) AS first_sales
        FROM MARKETING_REPORTS.PUBLIC.LEADS
        WHERE ({like_clauses})
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    daily_detail = [
        {
            "date":        json_serial(r[0]),
            "studio":      strip_brand(r[1]),
            "source":      r[2],
            "signups":     int(r[3] or 0),
            "first_sales": int(r[4] or 0),
        }
        for r in cur.fetchall()
    ]
    cur.close()
    conn.close()
    print(f"  {len(daily_detail):,} daily rows")

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
