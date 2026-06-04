"""
fetch_tier_rmr.py
Queries Snowflake for actual presale prices per NSO studio and builds
cumulative tier counts (T1/T2/T3) week by week using REAL prices paid,
then patches nso_scorecard_data.json with 'tier_rmr_by_week' per studio.

Run from nso-dashboard/:
    python scripts/fetch_tier_rmr.py
"""
import json, os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector

SCORECARD_FILE = "nso_scorecard_data.json"

# Map Snowflake studio_id from scorecard JSON
SNOWFLAKE_IDS = {
    "FL-019": 5751381,  # Naples - Mercato
    "VA-001": 5750130,  # Reston
    "UT-001": 5752080,  # Herriman
    "FL-020": 5753281,  # Dr Phillips
    "FL-018": 5753604,  # Aventura
    "FL-021": 5753608,  # North Miami
    "TX-004": 5753491,  # Dallas - Uptown
    "NJ-004": 5753073,  # Old Bridge
}


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "PLAYLIST_DATA_MART"),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def price_to_tier(price, pricing):
    """Map an actual price paid to a tier key (t1/t2/t3)."""
    t1p = pricing.get("tier1_price") or 99
    t2p = pricing.get("tier2_price") or 129
    t3p = pricing.get("tier3_price")
    if t3p and abs(price - t3p) <= 5:
        return "t3"
    if abs(price - t2p) <= 5:
        return "t2"
    return "t1"


def fetch_daily_sales(cur, studio_id):
    """
    CLIENT-LEVEL approach: avoids same-day cancel+rebuy netting to zero.

    For each (client, product_description, price):
      - net = total_buys - total_cancels
      - net > 0 → client is ACTIVE: emit +1 on first_buy_date
      - net = 0 → bought then fully cancelled: emit +1 on first_buy, -1 on last_cancel
      - net < 0 → data anomaly: emit -1 on last_cancel

    This correctly counts cancel+rebuy clients as active (net=1 if they bought
    twice and cancelled once) instead of netting to zero at the daily level.
    """
    # ── Query 1: per-client, per-product, per-price buy summary ──────────
    cur.execute(f"""
        WITH dedup AS (
            SELECT CLIENT_ID, PRODUCT_DESCRIPTION,
                   GROSS_PAYMENTAMT_LOCAL AS price,
                   SALE_DATE::DATE AS sale_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY CLIENT_ID, PRODUCT_DESCRIPTION,
                                    SALE_DATE::DATE, QUANTITY
                       ORDER BY UNIQUE_SALE_ID) AS rn
            FROM MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
            WHERE STUDIO_ID = {studio_id}
              AND ITEM_TYPE = 'Pricing Option'
              AND GROSS_PAYMENTAMT_LOCAL > 0
              AND QUANTITY = 1 AND IS_RETURN = 0
        )
        SELECT CLIENT_ID, PRODUCT_DESCRIPTION, price,
               COUNT(*) AS total_buys,
               MIN(sale_date) AS first_buy
        FROM dedup WHERE rn = 1
        GROUP BY 1, 2, 3
    """)
    buy_rows = cur.fetchall()  # [(client_id, prod_desc, price, total_buys, first_buy), ...]

    # ── Query 2: per-client, per-product cancel summary ──────────────────
    cur.execute(f"""
        WITH dedup AS (
            SELECT CLIENT_ID, PRODUCT_DESCRIPTION,
                   SALE_DATE::DATE AS sale_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY CLIENT_ID, PRODUCT_DESCRIPTION,
                                    SALE_DATE::DATE, QUANTITY
                       ORDER BY UNIQUE_SALE_ID) AS rn
            FROM MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
            WHERE STUDIO_ID = {studio_id}
              AND ITEM_TYPE = 'Pricing Option'
              AND (QUANTITY = -1 OR IS_RETURN = 1)
        )
        SELECT CLIENT_ID, PRODUCT_DESCRIPTION,
               COUNT(*) AS total_cancels,
               MAX(sale_date) AS last_cancel
        FROM dedup WHERE rn = 1
        GROUP BY 1, 2
    """)
    # key: (client_id, prod_desc) → {total_cancels, last_cancel}
    cancel_map = {
        (str(r[0]), str(r[1])): {"n": int(r[2]), "date": str(r[3])}
        for r in cur.fetchall()
    }

    # ── Build time-series events in Python ────────────────────────────────
    from collections import defaultdict
    daily = defaultdict(float)  # (date_str, price) → net delta

    skipped = 0
    for client_id, prod_desc, price, total_buys, first_buy in buy_rows:
        # Only handle presale-like products
        if "pre" not in prod_desc.lower() or "sale" not in prod_desc.lower():
            skipped += 1
            continue

        key_cp = (str(client_id), str(prod_desc))
        c_info  = cancel_map.get(key_cp, {"n": 0, "date": None})
        total_cancels = c_info["n"]
        last_cancel   = c_info["date"]

        net = int(total_buys) - total_cancels
        p   = float(price)
        d0  = str(first_buy)

        if net > 0:
            # Active: client has net memberships at this price from first_buy
            daily[(d0, p)] += net
        elif net == 0 and last_cancel:
            # Bought and fully cancelled
            daily[(d0, p)]          += int(total_buys)
            daily[(last_cancel, p)] -= int(total_buys)
        elif net < 0 and last_cancel:
            # More cancels than buys (data anomaly): net cancel effect
            daily[(last_cancel, p)] += net  # negative delta

    if skipped:
        print(f"    ({skipped} non-presale products skipped)")

    return [(d, p, int(n)) for (d, p), n in sorted(daily.items()) if n != 0]


def build_tier_rmr(daily_sales, weeks, pricing):
    """
    For each week in `weeks`, compute cumulative net tier counts
    using actual prices paid.
    Returns list of {week, t1, t2, t3} dicts (only for weeks w > 0).
    """
    # Cumulative running totals (can go negative during the loop, floor at 0)
    cum = {"t1": 0, "t2": 0, "t3": 0}
    result = []

    past_weeks = sorted([w for w in weeks if w.get("week","").strip() not in ("Week 0","WEEK 0","")
                         and w.get("date_end")],
                        key=lambda w: w["date_end"])

    sale_idx = 0
    sorted_sales = sorted(daily_sales, key=lambda r: r[0])   # by date

    for wk in past_weeks:
        import re
        m = re.search(r"(\d+)", wk.get("week", ""))
        if not m:
            continue
        wnum = int(m.group(1))
        date_end = wk["date_end"]    # "YYYY-MM-DD"

        # Absorb all sales up to date_end
        while sale_idx < len(sorted_sales):
            sd, price, net = sorted_sales[sale_idx]
            sd_str = sd.isoformat() if hasattr(sd, "isoformat") else str(sd)
            if sd_str > date_end:
                break
            tier = price_to_tier(float(price), pricing)
            cum[tier] = max(0, cum[tier] + int(net))
            sale_idx += 1

        result.append({
            "week": wnum,
            "t1":   cum["t1"],
            "t2":   cum["t2"],
            "t3":   cum["t3"],
        })

    return result


# ── Main ─────────────────────────────────────────────────────────────────────
with open(SCORECARD_FILE) as f:
    sc = json.load(f)

print("Connecting to Snowflake...")
conn = connect()
cur = conn.cursor()

for studio in sc.get("studios", []):
    code = studio.get("code", "")
    sid = SNOWFLAKE_IDS.get(code)
    if not sid:
        print(f"  {code}: no Snowflake ID configured, skipping")
        continue

    pricing = studio.get("pricing") or {}
    weeks   = studio.get("weeks", [])

    print(f"  Fetching {studio['name']} (id={sid})...")
    daily = fetch_daily_sales(cur, sid)

    tier_rmr = build_tier_rmr(daily, weeks, pricing)
    studio["tier_rmr_by_week"] = tier_rmr

    # Summary
    last = tier_rmr[-1] if tier_rmr else {}
    t1p = pricing.get("tier1_price", 99)
    t2p = pricing.get("tier2_price", 129)
    t3p = pricing.get("tier3_price", 0) or 0
    rmr_est = last.get("t1",0)*t1p + last.get("t2",0)*t2p + last.get("t3",0)*t3p
    print(f"    W{last.get('week','?')}: T1={last.get('t1')} T2={last.get('t2')} "
          f"T3={last.get('t3')}  Est.RMR=${rmr_est:,.0f}")

conn.close()

with open(SCORECARD_FILE, "w") as f:
    json.dump(sc, f, indent=2)

print(f"\nDone. {SCORECARD_FILE} updated with tier_rmr_by_week for all studios.")
