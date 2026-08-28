"""
fetch_2weeks_data.py
Fetch data for the "2 Weeks for $40" campaign dashboard.

Sources:
  - Snowflake: transactions with PRODUCT_DESCRIPTION = '2 Weeks for $40', by studio + lead source
  - Meta API:  campaign-level spend for '2026 - Open Studios - 2 Weeks for 40'

Output: campaign-dashboard/2weeks_data.json

Usage:
    python fetch_2weeks_data.py
    python fetch_2weeks_data.py --start 2026-08-26 --end 2026-08-28
"""

import json, os, sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv
    _here = Path(__file__).resolve().parent
    load_dotenv(_here / ".env") or load_dotenv(_here / "nso-dashboard" / ".env")
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

# ── Config ─────────────────────────────────────────────────────────────────
CAMPAIGN_START   = "2026-08-25"          # Monday the campaign launched
CAMPAIGN_NAME_KW = "2 Weeks for 40"      # keyword filter for Meta campaign name
META_ACCOUNT     = "act_1553887681409034"
OUT_PATH         = Path(__file__).parent / "campaign-dashboard" / "2weeks_data.json"

SALES    = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS"
LEADS    = "MARKETING_REPORTS.PUBLIC.LEADS_LIST"
CLIENTS  = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS"

STUDIOS = [
    "Boca Raton", "Brickell", "Chelsea", "Coconut Grove", "Coral Gables",
    "Coral Springs", "Deerfield", "Doral", "Eastchester", "FiDi", "Highland",
    "Las Olas", "Madison", "Miami Beach", "Miami Lakes", "Midtown", "Miramar",
    "NoDa", "Park Slope", "Pembroke", "Pinecrest", "Prestonwood", "Reston",
    "South Miami", "Upper East", "West Palm",
]

STUDIO_FILTER = " OR ".join(f"sd.STUDIO_NAME ILIKE '%%{s}%%'" for s in STUDIOS)


# ── Snowflake ───────────────────────────────────────────────────────────────
def fetch_snowflake(start: str, end: str) -> dict:
    import snowflake.connector
    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
    cur = con.cursor()

    # Transactions by studio + lead source
    cur.execute(f"""
        SELECT
            sd.STUDIO_NAME,
            CASE
                WHEN ll.SOURCE = 'Meta Ads'                          THEN 'Meta Ads'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'meta ads'         THEN 'Meta Ads'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'google ads'       THEN 'Google Ads'
                ELSE COALESCE(ll.SOURCE, 'Other')
            END AS lead_source,
            COUNT(*)                                                  AS transactions,
            COUNT(DISTINCT sd.CLIENT_ID)                              AS unique_clients,
            ROUND(SUM(sd.PAYMENTAMT_LOCAL), 2)                       AS revenue
        FROM {SALES} sd
        LEFT JOIN {LEADS} ll
            ON LOWER(TRIM(sd.EMAIL_ID)) = LOWER(TRIM(ll.EMAIL_ID))
           AND sd.STUDIO_ID = ll.STUDIO_ID
        LEFT JOIN {CLIENTS} c
            ON sd.CLIENT_ID = c.CLIENT_ID
           AND sd.STUDIO_ID = c.STUDIO_ID
        WHERE sd.PRODUCT_DESCRIPTION = '2 Weeks for $40'
          AND CAST(sd.SALE_DATE AS DATE) BETWEEN %s AND %s
          AND ({STUDIO_FILTER})
        GROUP BY 1, 2
        ORDER BY 1, 2
    """, (start, end))
    rows = cur.fetchall()

    # Daily totals for time series
    cur.execute(f"""
        SELECT
            CAST(sd.SALE_DATE AS DATE)::VARCHAR  AS sale_date,
            COUNT(*)                              AS transactions,
            COUNT(DISTINCT sd.CLIENT_ID)          AS unique_clients,
            ROUND(SUM(sd.PAYMENTAMT_LOCAL), 2)   AS revenue
        FROM {SALES} sd
        WHERE sd.PRODUCT_DESCRIPTION = '2 Weeks for $40'
          AND CAST(sd.SALE_DATE AS DATE) BETWEEN %s AND %s
          AND ({STUDIO_FILTER})
        GROUP BY 1
        ORDER BY 1
    """, (start, end))
    daily_rows = cur.fetchall()

    cur.close()
    con.close()

    # Build studio summary map
    studios: dict = {}
    for studio_name, lead_source, txns, clients, revenue in rows:
        if studio_name not in studios:
            studios[studio_name] = {
                "studio": studio_name,
                "transactions": 0,
                "unique_clients": 0,
                "revenue": 0.0,
                "meta_transactions": 0,
                "meta_clients": 0,
                "meta_revenue": 0.0,
                "by_source": [],
            }
        s = studios[studio_name]
        s["transactions"]   += int(txns)
        s["unique_clients"] += int(clients)
        s["revenue"]        += float(revenue)
        s["by_source"].append({
            "source": lead_source,
            "transactions": int(txns),
            "unique_clients": int(clients),
            "revenue": float(revenue),
        })
        if lead_source == "Meta Ads":
            s["meta_transactions"] += int(txns)
            s["meta_clients"]      += int(clients)
            s["meta_revenue"]      += float(revenue)

    studio_list = sorted(studios.values(), key=lambda x: -x["transactions"])

    daily = [
        {"date": str(d), "transactions": int(t), "unique_clients": int(u), "revenue": float(r)}
        for d, t, u, r in daily_rows
    ]

    totals = {
        "transactions":      sum(s["transactions"] for s in studio_list),
        "unique_clients":    sum(s["unique_clients"] for s in studio_list),
        "revenue":           round(sum(s["revenue"] for s in studio_list), 2),
        "meta_transactions": sum(s["meta_transactions"] for s in studio_list),
        "meta_revenue":      round(sum(s["meta_revenue"] for s in studio_list), 2),
    }

    return {"studios": studio_list, "daily": daily, "totals": totals}


# ── Snowflake: Meta Ads leads from LEADS_LIST ────────────────────────────────
def fetch_meta_leads(start: str, end: str) -> dict:
    import snowflake.connector
    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
    cur = con.cursor()

    cur.execute(f"""
        SELECT
            STUDIO_NAME,
            COUNT(*) AS leads
        FROM {LEADS}
        WHERE SOURCE = 'Meta Ads'
          AND SIGNUP_DATE BETWEEN %s AND %s
          AND ({STUDIO_FILTER.replace('sd.STUDIO_NAME', 'STUDIO_NAME')})
        GROUP BY 1
        ORDER BY 2 DESC
    """, (start, end))
    rows = cur.fetchall()

    cur.close()
    con.close()

    studios = [{"studio": str(name), "leads": int(n)} for name, n in rows]
    return {
        "total":   sum(s["leads"] for s in studios),
        "studios": studios,
    }


# Maps the studio label at the end of each adset name to the STUDIOS keyword
# used in Snowflake ILIKE filters, for matching back to sf.studios[].studio
ADSET_STUDIO_MAP = {
    "Boca Raton":      "Boca Raton",
    "Chelsea":         "Chelsea",
    "Coconut Grove":   "Coconut Grove",
    "Coral Gables":    "Coral Gables",
    "Coral Springs":   "Coral Springs",
    "Deerfield Beach": "Deerfield",
    "Doral":           "Doral",
    "Eastchester":     "Eastchester",
    "FiDi":            "FiDi",
    "Las Olas":        "Las Olas",
    "Miami Lakes":     "Miami Lakes",
    "Midtown Miami":   "Midtown",
    "Miramar":         "Miramar",
    "NODA":            "NoDa",
    "NYC Park Slope":  "Park Slope",
    "Pembroke Pines":  "Pembroke",
    "Pinecrest":       "Pinecrest",
    "South Miami":     "South Miami",
    "Upper East Side": "Upper East",
    "West Palm Beach": "West Palm",
}


def _adset_label(adset_name: str) -> str:
    """Extract the studio label from an adset name like '26-FL-003-02 - 2WeeksFor40 - Coral Gables'."""
    parts = adset_name.rsplit(" - ", 1)
    return parts[-1].strip() if len(parts) > 1 else adset_name


def _match_sf_studio(keyword: str, sf_studios: list) -> str:
    """Find the Snowflake studio name that contains `keyword` (case-insensitive)."""
    kw = keyword.lower()
    for s in sf_studios:
        if kw in s["studio"].lower():
            return s["studio"]
    return "SWEAT440 " + keyword   # fallback: prefix with brand name


# ── Meta API ────────────────────────────────────────────────────────────────
def fetch_meta_spend(start: str, end: str, sf_studios: list) -> dict:
    from meta_client import MetaClient
    client = MetaClient(os.environ.get("META_ACCESS_TOKEN") or os.environ.get("META_TOKEN"))

    print(f"  Querying Meta adset spend [{start} to {end}] ...")
    rows = client.get_insights(
        META_ACCOUNT,
        level="adset",
        date_start=start,
        date_end=end,
        time_increment=1,
        filtering=[{"field": "campaign.name", "operator": "CONTAIN", "value": CAMPAIGN_NAME_KW}],
    )

    campaigns: dict = {}
    daily: dict = {}
    studios: dict = {}

    for r in rows:
        cn    = r.get("campaign_name", "")
        date  = r.get("date_start", "")[:10]
        aname = r.get("adset_name", "")
        sp    = float(r.get("spend", 0) or 0)
        imp   = int(r.get("impressions", 0) or 0)
        cli   = int(r.get("clicks", 0) or 0)

        # Campaign-level aggregation
        if cn not in campaigns:
            campaigns[cn] = {"campaign_name": cn, "spend": 0.0, "impressions": 0, "clicks": 0}
        campaigns[cn]["spend"]       += sp
        campaigns[cn]["impressions"] += imp
        campaigns[cn]["clicks"]      += cli

        # Daily aggregation
        if date not in daily:
            daily[date] = {"date": date, "spend": 0.0, "impressions": 0, "clicks": 0}
        daily[date]["spend"]       += sp
        daily[date]["impressions"] += imp
        daily[date]["clicks"]      += cli

        # Studio-level aggregation via adset name
        label   = _adset_label(aname)
        keyword = ADSET_STUDIO_MAP.get(label, label)
        sf_name = _match_sf_studio(keyword, sf_studios)
        if sf_name not in studios:
            studios[sf_name] = {"studio": sf_name, "spend": 0.0, "impressions": 0, "clicks": 0}
        studios[sf_name]["spend"]       += sp
        studios[sf_name]["impressions"] += imp
        studios[sf_name]["clicks"]      += cli

    total_spend = round(sum(c["spend"] for c in campaigns.values()), 2)
    for c in campaigns.values():
        c["spend"] = round(c["spend"], 2)
    for d in daily.values():
        d["spend"] = round(d["spend"], 2)
    for s in studios.values():
        s["spend"] = round(s["spend"], 2)

    return {
        "total_spend": total_spend,
        "campaigns":   sorted(campaigns.values(), key=lambda x: -x["spend"]),
        "daily":       sorted(daily.values(), key=lambda x: x["date"]),
        "studios":     sorted(studios.values(), key=lambda x: -x["spend"]),
    }


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=CAMPAIGN_START)
    p.add_argument("--end",   default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
    args = p.parse_args()

    print(f"2 Weeks for $40 -- data fetch [{args.start} to {args.end}]")

    print("\n[1/3] Snowflake: transactions …")
    sf = fetch_snowflake(args.start, args.end)
    print(f"  Studios: {len(sf['studios'])}  |  Transactions: {sf['totals']['transactions']}")

    print("\n[2/3] Snowflake: Meta Ads leads …")
    try:
        meta_leads = fetch_meta_leads(args.start, args.end)
        print(f"  Total leads: {meta_leads['total']}  |  Studios: {len(meta_leads['studios'])}")
    except Exception as e:
        print(f"  WARNING: leads query failed — {e}")
        meta_leads = {"total": 0, "studios": []}

    print("\n[3/3] Meta API: adset spend by studio …")
    try:
        meta = fetch_meta_spend(args.start, args.end, sf["studios"])
        print(f"  Total spend: ${meta['total_spend']:,.2f}  |  Studios: {len(meta['studios'])}")
    except Exception as e:
        print(f"  WARNING: Meta API failed — {e}")
        meta = {"total_spend": 0.0, "campaigns": [], "daily": [], "studios": []}

    output = {
        "generated_at":  datetime.now().isoformat(),
        "date_range":    {"start": args.start, "end": args.end},
        "snowflake":     sf,
        "meta_leads":    meta_leads,
        "meta":          meta,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nWritten -> {OUT_PATH}")


if __name__ == "__main__":
    main()
