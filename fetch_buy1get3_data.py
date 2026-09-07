"""
fetch_buy1get3_data.py
Fetch Meta Ads data for the "Buy 1 Get 3 Free" campaign dashboard.

Sources:
  - Snowflake: Meta Ads leads from LEADS_LIST during the promo period
  - Meta API: adset-level spend for '2026 - Open Studios - Buy 1 Get 3'

Output: campaign-dashboard/buy1get3_data.json

Usage:
    python fetch_buy1get3_data.py
    python fetch_buy1get3_data.py --start 2026-05-12 --end 2026-05-26
"""

import json, os, sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    _here = Path(__file__).resolve().parent
    load_dotenv(_here / ".env") or load_dotenv(_here / "nso-dashboard" / ".env")
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

# ── Config ─────────────────────────────────────────────────────────────────
CAMPAIGN_START   = "2026-05-12"
CAMPAIGN_END     = "2026-05-26"
CAMPAIGN_NAME_KW = "Buy 1 Get 3"
META_ACCOUNT     = "act_1553887681409034"
OUT_PATH         = Path(__file__).parent / "campaign-dashboard" / "buy1get3_data.json"

LEADS = "MARKETING_REPORTS.PUBLIC.LEADS_LIST"

STUDIOS = [
    "Boca Raton", "Brickell", "Chelsea", "Coconut Grove", "Coral Gables",
    "Coral Springs", "Deerfield", "Doral", "Eastchester", "FiDi", "Highland",
    "Las Olas", "Madison", "Miami Beach", "Miami Lakes", "Midtown", "Miramar",
    "NoDa", "Park Slope", "Pembroke", "Pinecrest", "Prestonwood", "Reston",
    "South Miami", "Upper East", "West Palm", "Toms River", "Wall", "Ocean",
    "Zilker", "Herriman", "Fort Myers",
]

STUDIO_FILTER = " OR ".join(f"STUDIO_NAME ILIKE '%%{s}%%'" for s in STUDIOS)


# ── Snowflake: Meta leads ───────────────────────────────────────────────────
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
        SELECT STUDIO_NAME, COUNT(*) AS leads
        FROM {LEADS}
        WHERE SOURCE = 'Meta Ads'
          AND SIGNUP_DATE BETWEEN %s AND %s
          AND ({STUDIO_FILTER})
        GROUP BY 1
        ORDER BY 2 DESC
    """, (start, end))
    rows = cur.fetchall()
    cur.close()
    con.close()
    studios = [{"studio": str(name), "leads": int(n)} for name, n in rows]
    return {"total": sum(s["leads"] for s in studios), "studios": studios}


# ── Adset → studio keyword mapping ─────────────────────────────────────────
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
    "NoDa":            "NoDa",
    "NYC Park Slope":  "Park Slope",
    "Pembroke Pines":  "Pembroke",
    "Pinecrest":       "Pinecrest",
    "South Miami":     "South Miami",
    "Upper East Side": "Upper East",
    "West Palm Beach": "West Palm",
    "Austin Highland": "Highland",
    "Austin Zilker":   "Zilker",
    "Ocean Township":  "Ocean",
    "Toms River":      "Toms River",
    "Wall Township":   "Wall",
    "Madison":         "Madison",
    "Herriman":        "Herriman",
    "Fort Myers":      "Fort Myers",
    "Prestonwood":     "Prestonwood",
    "Reston":          "Reston",
}


def _adset_label(adset_name: str) -> str:
    """Extract studio label from adset name like '26-FL-003 - Buy1Get3 - Coral Gables'."""
    parts = adset_name.rsplit(" - ", 1)
    return parts[-1].strip() if len(parts) > 1 else adset_name


# ── Meta API ────────────────────────────────────────────────────────────────
def fetch_meta_spend(start: str, end: str) -> dict:
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

        if cn not in campaigns:
            campaigns[cn] = {"campaign_name": cn, "spend": 0.0, "impressions": 0, "clicks": 0}
        campaigns[cn]["spend"]       += sp
        campaigns[cn]["impressions"] += imp
        campaigns[cn]["clicks"]      += cli

        if date not in daily:
            daily[date] = {"date": date, "spend": 0.0, "impressions": 0, "clicks": 0}
        daily[date]["spend"]       += sp
        daily[date]["impressions"] += imp
        daily[date]["clicks"]      += cli

        label   = _adset_label(aname)
        keyword = ADSET_STUDIO_MAP.get(label, label)
        sf_name = "SWEAT440 " + keyword
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
    p.add_argument("--end",   default=CAMPAIGN_END)
    args = p.parse_args()

    print(f"Buy 1 Get 3 -- data fetch [{args.start} to {args.end}]")

    print("\n[1/2] Snowflake: Meta Ads leads …")
    try:
        meta_leads = fetch_meta_leads(args.start, args.end)
        print(f"  Total leads: {meta_leads['total']}  |  Studios: {len(meta_leads['studios'])}")
    except Exception as e:
        print(f"  WARNING: leads query failed — {e}")
        meta_leads = {"total": 0, "studios": []}

    print("\n[2/2] Meta API: adset spend by studio …")
    try:
        meta = fetch_meta_spend(args.start, args.end)
        print(f"  Total spend: ${meta['total_spend']:,.2f}  |  Studios: {len(meta['studios'])}")
    except Exception as e:
        print(f"  WARNING: Meta API failed — {e}")
        meta = {"total_spend": 0.0, "campaigns": [], "daily": [], "studios": []}

    output = {
        "generated_at": datetime.now().isoformat(),
        "date_range":   {"start": args.start, "end": args.end},
        "meta":         meta,
        "meta_leads":   meta_leads,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nWritten -> {OUT_PATH}")


if __name__ == "__main__":
    main()
