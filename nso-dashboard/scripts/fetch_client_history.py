#!/usr/bin/env python3
"""
fetch_client_history.py — Pull individual membership sales and cancellations
for all NSO studios from Snowflake, for the Client History report.

Output JSON format:
{
    "generated_at": "2026-07-21T00:00:00",
    "studios": [
        {
            "name": "SWEAT440 Herriman",
            "code": "UT-001",
            "records": [
                {
                    "type": "sale",           -- "sale" | "cancel"
                    "client_name": "Jane Doe",
                    "email": "jane@example.com",
                    "phone": "8015551234",
                    "product": "3 Month Presale Membership",
                    "date": "2026-07-20",
                    "time": "14:32",          -- HH:MM local; "" if unknown
                    "platform": "Business Mode Web",
                    "sold_by": "Studio Staff",  -- derived from SOURCE_CHANNEL
                    "amount": 99.00
                },
                ...
            ]
        }
    ]
}

Usage:
  python scripts/fetch_client_history.py
  python scripts/fetch_client_history.py --output nso_client_history.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

NSO_STUDIOS = studios_registry.nso_studios_by_snowflake_id()
ID_LIST = ",".join(str(i) for i in NSO_STUDIOS)

# Studios that are open and sell real memberships (not just presales)
MEMBERSHIP_STUDIOS = {5751381}  # Naples - Mercato
MEM_ID_LIST = ",".join(str(i) for i in MEMBERSHIP_STUDIOS)

# ---------------------------------------------------------------------------
# Lead-source attribution CASE — mirrors fetch_nso_sales.py exactly
# ---------------------------------------------------------------------------
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
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('public api')        THEN 'Public API'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('mindbody app','mindbody web')
                                                                 THEN 'MindBody App'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('another client','word of mouth')
                                                                 THEN 'Word of Mouth'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('classpass','wellhub','wellness passport')
                                                                 THEN 'ClassPass / Platforms'
        WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('drive by','flyer','frederick',
                                             'holly met outside of gym.','internet','n/a',
                                             'newspaper','other','radio','tv / streaming')
                                                                 THEN 'Other'
        ELSE 'N/A'
    END
"""


# ---------------------------------------------------------------------------
# SOURCE_CHANNEL → human-readable "sold by" label
# ---------------------------------------------------------------------------
def _sold_by(source_channel: str | None, online_flag) -> str:
    ch = (source_channel or "").strip()
    if not ch:
        return "Unknown"
    if "business mode" in ch.lower():
        return "Studio Staff"
    if "branded mobile" in ch.lower():
        return "Client (App)"
    if "branded web" in ch.lower():
        return "Client (Web)"
    if "consumer mode" in ch.lower():
        return "Client (Consumer)"
    if "api" in ch.lower():
        return "API"
    if online_flag:
        return "Client (Online)"
    return "Studio Staff"


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "PLAYLIST_DATA_MART"),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


# ---------------------------------------------------------------------------
# Main query — individual presale + cancellation records with phone join
# ---------------------------------------------------------------------------
QUERY = f"""
WITH filtered AS (
    -- No LOC=98 dedup: show every raw transaction for audit purposes.
    -- This matches what MindBody surfaces in its own reports.
    SELECT
        s.STUDIO_ID,
        s.CLIENT_ID,
        s.CLIENT_FULL_NAME                          AS client_name,
        s.EMAIL_ID                                  AS email,
        s.PRODUCT_DESCRIPTION                       AS product,
        s.SALE_DATE::DATE                           AS sale_date,
        s.CREATED_DATE_TIME_UTC                     AS created_utc,
        s.ONLINE_FLAG,
        s.SOURCE_CHANNEL,
        s.QUANTITY,
        s.IS_RETURN,
        s.IS_AUTOPAY,
        COALESCE(s.PAYMENTAMT_LOCAL, 0)             AS paid_now,
        COALESCE(s.GROSS_PAYMENTAMT_LOCAL, 0)       AS amount
    FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS s
    WHERE s.STUDIO_ID IN ({ID_LIST})
      AND s.ITEM_TYPE = 'Pricing Option'
      AND LOWER(s.PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
      AND (s.EMAIL_ID IS NULL OR (
          LOWER(TRIM(s.EMAIL_ID)) NOT LIKE 'test%'
      AND LOWER(TRIM(s.EMAIL_ID)) NOT LIKE '%sweat440%'
      AND LOWER(TRIM(s.EMAIL_ID)) NOT LIKE '%leadteam%'
      ))
),
phones AS (
    SELECT
        CLIENT_ID,
        STUDIO_ID,
        COALESCE(NULLIF(TRIM(CELLPHONE),''), NULLIF(TRIM(HOMEPHONE),''), NULLIF(TRIM(WORKPHONE),'')) AS phone
    FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS
    WHERE STUDIO_ID IN ({ID_LIST})
),
leads_src AS (
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
                    WHEN LEAD_SOURCE = 'public api'                                  THEN 3
                    WHEN LEAD_SOURCE IS NULL                                          THEN 2
                    ELSE 1
                END,
                STAGE_START ASC
        ) AS rn
    FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_LEADS_LOG
    WHERE STUDIO_ID IN ({ID_LIST})
),
clients_src AS (
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
    f.STUDIO_ID,
    f.client_name,
    f.email,
    COALESCE(p.phone, '')                           AS phone,
    f.product,
    f.sale_date,
    f.created_utc,
    f.ONLINE_FLAG,
    COALESCE(f.SOURCE_CHANNEL, '')                  AS source_channel,
    CASE WHEN f.QUANTITY = 1 AND f.IS_RETURN = 0 THEN 'sale' ELSE 'cancel' END AS txn_type,
    f.amount,
    IFF(COALESCE(f.IS_AUTOPAY, 0) = 1 AND f.paid_now > 0, 1, 0) AS is_bill_charge,
    COALESCE({_SOURCE_CASE}, 'N/A')                 AS lead_source
FROM filtered f
LEFT JOIN phones     p ON p.CLIENT_ID             = f.CLIENT_ID
                      AND p.STUDIO_ID             = f.STUDIO_ID
LEFT JOIN leads_src  l ON LOWER(TRIM(f.email))   = l.email
                      AND f.STUDIO_ID             = l.STUDIO_ID AND l.rn = 1
LEFT JOIN clients_src c ON LOWER(TRIM(f.email))  = c.email
                      AND f.STUDIO_ID             = c.STUDIO_ID AND c.rn = 1
ORDER BY f.STUDIO_ID, f.sale_date DESC, f.created_utc DESC
"""


# ---------------------------------------------------------------------------
# Open-studio membership query — runs only for MEMBERSHIP_STUDIOS
# Pulls real membership buys/cancels (not presales) so they appear in
# Client History alongside the presale records for open studios.
# ---------------------------------------------------------------------------
MEMBERSHIP_QUERY = f"""
WITH filtered AS (
    SELECT
        s.STUDIO_ID,
        s.CLIENT_ID,
        s.CLIENT_FULL_NAME                          AS client_name,
        s.EMAIL_ID                                  AS email,
        s.PRODUCT_DESCRIPTION                       AS product,
        s.SALE_DATE::DATE                           AS sale_date,
        s.CREATED_DATE_TIME_UTC                     AS created_utc,
        s.ONLINE_FLAG,
        s.SOURCE_CHANNEL,
        s.QUANTITY,
        s.IS_RETURN,
        s.IS_AUTOPAY,
        COALESCE(s.PAYMENTAMT_LOCAL, 0)             AS paid_now,
        COALESCE(s.GROSS_PAYMENTAMT_LOCAL, 0)       AS amount
    FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS s
    WHERE s.STUDIO_ID IN ({MEM_ID_LIST})
      AND s.ITEM_TYPE = 'Pricing Option'
      AND s.REVENUE_CATEGORY = 'Memberships'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%pre%sale%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%employee%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%family%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%complimentary%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%guest%pass%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%aescape%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%massage%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%wellhub%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%wellness%passport%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%classpass%'
      AND LOWER(s.PRODUCT_DESCRIPTION) NOT LIKE '%class pass%'
      AND (s.EMAIL_ID IS NULL OR (
          LOWER(TRIM(s.EMAIL_ID)) NOT LIKE 'test%'
      AND LOWER(TRIM(s.EMAIL_ID)) NOT LIKE '%sweat440%'
      AND LOWER(TRIM(s.EMAIL_ID)) NOT LIKE '%leadteam%'
      ))
),
phones AS (
    SELECT
        CLIENT_ID,
        STUDIO_ID,
        COALESCE(NULLIF(TRIM(CELLPHONE),''), NULLIF(TRIM(HOMEPHONE),''), NULLIF(TRIM(WORKPHONE),'')) AS phone
    FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS
    WHERE STUDIO_ID IN ({MEM_ID_LIST})
),
leads_src AS (
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
                    WHEN LEAD_SOURCE = 'public api'                                  THEN 3
                    WHEN LEAD_SOURCE IS NULL                                          THEN 2
                    ELSE 1
                END,
                STAGE_START ASC
        ) AS rn
    FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_LEADS_LOG
    WHERE STUDIO_ID IN ({MEM_ID_LIST})
),
clients_src AS (
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
    WHERE STUDIO_ID IN ({MEM_ID_LIST})
      AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
      AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
      AND LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
)
SELECT
    f.STUDIO_ID,
    f.client_name,
    f.email,
    COALESCE(p.phone, '')                           AS phone,
    f.product,
    f.sale_date,
    f.created_utc,
    f.ONLINE_FLAG,
    COALESCE(f.SOURCE_CHANNEL, '')                  AS source_channel,
    CASE WHEN f.QUANTITY = 1 AND f.IS_RETURN = 0 THEN 'sale' ELSE 'cancel' END AS txn_type,
    f.amount,
    IFF(COALESCE(f.IS_AUTOPAY, 0) = 1 AND f.paid_now > 0, 1, 0) AS is_bill_charge,
    COALESCE({_SOURCE_CASE}, 'N/A')                 AS lead_source
FROM filtered f
LEFT JOIN phones     p ON p.CLIENT_ID             = f.CLIENT_ID
                      AND p.STUDIO_ID             = f.STUDIO_ID
LEFT JOIN leads_src  l ON LOWER(TRIM(f.email))   = l.email
                      AND f.STUDIO_ID             = l.STUDIO_ID AND l.rn = 1
LEFT JOIN clients_src c ON LOWER(TRIM(f.email))  = c.email
                      AND f.STUDIO_ID             = c.STUDIO_ID AND c.rn = 1
ORDER BY f.STUDIO_ID, f.sale_date DESC, f.created_utc DESC
"""


def _parse_rows(rows, cols) -> list[dict]:
    out = []
    for r in rows:
        row = dict(zip(cols, r))
        sid = row["STUDIO_ID"]
        time_str = ""
        if row["CREATED_UTC"]:
            try:
                dt = row["CREATED_UTC"]
                if hasattr(dt, "strftime"):
                    dt_et = dt.replace(tzinfo=timezone.utc).astimezone(ET)
                    time_str = dt_et.strftime("%H:%M")
            except Exception:
                pass
        out.append({
            "_sid":        sid,
            "type":        row["TXN_TYPE"],
            "client_name": row["CLIENT_NAME"] or "",
            "email":       row["EMAIL"] or "",
            "phone":       row["PHONE"] or "",
            "product":     row["PRODUCT"] or "",
            "date":        str(row["SALE_DATE"]) if row["SALE_DATE"] else "",
            "time":        time_str,
            "platform":    row["SOURCE_CHANNEL"] or "",
            "sold_by":     _sold_by(row["SOURCE_CHANNEL"], row["ONLINE_FLAG"]),
            "amount":      float(row["AMOUNT"] or 0),
            "source":      row["LEAD_SOURCE"] or "N/A",
            # Automatic charge on an existing contract (presale first bill / monthly
            # renewal), not a new purchase. Presale purchases are sold at $0 paid and
            # first charged on the bill date.
            **({"bill_charge": True} if row.get("IS_BILL_CHARGE") else {}),
        })
    return out


def _bill_dates(records: list[dict]) -> list[str]:
    """Days presale memberships were billed (every 4 weeks from the first bill):
    dates with 10+ automatic presale-product charges — single stray charges
    are ignored. Empty until the studio has billed its presales."""
    per_day: dict[str, int] = {}
    for r in records:
        if r.get("bill_charge") and r["type"] == "sale" and "pre" in r["product"].lower() and "sale" in r["product"].lower():
            per_day[r["date"]] = per_day.get(r["date"], 0) + 1
    return sorted(d for d, n in per_day.items() if n >= 10)


def fetch_all() -> dict:
    conn = get_conn()
    cur = conn.cursor()

    print("Running presale client history query...")
    cur.execute(QUERY)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    print(f"  {len(rows)} presale records fetched")
    all_records = _parse_rows(rows, cols)

    if MEMBERSHIP_STUDIOS:
        print("Running open-studio membership query...")
        cur.execute(MEMBERSHIP_QUERY)
        mem_rows = cur.fetchall()
        mem_cols = [d[0] for d in cur.description]
        print(f"  {len(mem_rows)} membership records fetched")
        all_records += _parse_rows(mem_rows, mem_cols)

    cur.close()
    conn.close()

    # Group by studio; sort combined records newest-first
    studio_records: dict[int, list] = {}
    for rec in sorted(all_records, key=lambda x: (x["date"], x["time"]), reverse=True):
        sid = rec.pop("_sid")
        if sid not in studio_records:
            studio_records[sid] = []
        studio_records[sid].append(rec)

    studios_out = []
    for sid, info in NSO_STUDIOS.items():
        records = studio_records.get(sid, [])
        studios_out.append({
            "name":    info["name"],
            "code":    info["code"],
            "bill_dates": _bill_dates(records),   # [0] = first bill date
            "records": records,
        })
        print(f"  {info['name']}: {len(records)} records")

    return {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "studios": sorted(studios_out, key=lambda s: s["name"]),
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch NSO client history from Snowflake.")
    parser.add_argument("--output", default="nso_client_history.json")
    args = parser.parse_args()

    result = fetch_all()

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    total = sum(len(s["records"]) for s in result["studios"])
    print(f"\nDone. {total} total records written to {args.output}")


if __name__ == "__main__":
    main()
