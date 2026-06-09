"""
fetch_tier_rmr.py
Queries Snowflake for actual presale prices per NSO studio and builds
cumulative tier counts (T1/T2/T3) week by week using REAL prices paid,
then patches nso_scorecard_data.json with 'tier_rmr_by_week' per studio.

Run from nso-dashboard/:
    python scripts/fetch_tier_rmr.py
"""
import json, os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector

SCORECARD_FILE = "nso_scorecard_data.json"
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# Per-studio pricing config (source: NSO Config Google Sheet).
# tier0_price is a special founders tier below the normal T1 (Reston only).
# These values are the source of truth — do NOT read pricing from the JSON.
STUDIO_PRICING = {
    "FL-019": {"tier1_price": 99,  "tier2_price": 129, "tier3_price": 149},  # Naples
    "VA-001": {"tier0_price": 99,  "tier1_price": 129, "tier2_price": 149},  # Reston: founders=$99
    "UT-001": {"tier1_price": 99,  "tier2_price": 129, "tier3_price": 149},  # Herriman
    "FL-020": {"tier1_price": 99,  "tier2_price": 129, "tier3_price": 149},  # Dr Phillips
    "FL-018": {"tier1_price": 129, "tier2_price": 149},                       # Aventura
    "FL-021": {"tier1_price": 129, "tier2_price": 149},                       # North Miami
    "TX-004": {"tier1_price": 129, "tier2_price": 149},                       # Dallas Uptown
    "NJ-004": {"tier1_price": 99,  "tier2_price": 129},                       # Old Bridge
}

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
    """Map an actual price paid to a tier key (t0/t1/t2/t3).
    t0 is a founders/lower price below tier1_price (e.g., Reston founders at $99)."""
    t0p = pricing.get("tier0_price")
    t1p = pricing.get("tier1_price") or 99
    t2p = pricing.get("tier2_price") or 129
    t3p = pricing.get("tier3_price")
    if t3p and abs(price - t3p) <= 5:
        return "t3"
    if abs(price - t2p) <= 5:
        return "t2"
    if abs(price - t1p) <= 5:
        return "t1"
    if t0p and abs(price - t0p) <= 5:
        return "t0"
    return "t1"


def fetch_daily_sales(cur, studio_id, end_date):
    """
    CLIENT-LEVEL approach matching fetch_nso_sales.py methodology exactly:
    group by (client, product) — no price split — so the same client is never
    counted in multiple tier buckets due to loc1/loc98 price rounding differences
    or tier upgrades.  Includes $0 presales (comps/staff) to match the scorecard.

    end_date: only include records with SALE_DATE <= end_date (same cutoff as
    fetch_nso_sales.py so both scripts always cover the same date range).

    For each (client, product_description):
      - net = total_buys - total_cancels
      - net > 0 → client ACTIVE: emit +1 on first_buy_date
      - net = 0 → bought then fully cancelled: emit +1 on first_buy, -1 on last_cancel
      - net < 0 → data anomaly: skip

    price = AVG of positive-amount buy records (used only for tier assignment).
    """
    # ── Query 1: per-client, per-product buy summary (no price split) ─────
    cur.execute(f"""
        SELECT CLIENT_ID, EMAIL_ID, PRODUCT_DESCRIPTION,
               COALESCE(ROUND(AVG(CASE WHEN GROSS_PAYMENTAMT_LOCAL > 0
                                       THEN GROSS_PAYMENTAMT_LOCAL END), 0), 0) AS price,
               COUNT(*)               AS total_buys,
               MIN(SALE_DATE::DATE)   AS first_buy
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
        WHERE STUDIO_ID = {studio_id}
          AND ITEM_TYPE = 'Pricing Option'
          AND LOWER(PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
          AND QUANTITY = 1 AND IS_RETURN = 0
          AND SALE_DATE::DATE <= '{end_date}'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%test%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
          AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
        GROUP BY 1, 2, 3
    """)
    buy_rows_raw = cur.fetchall()  # [(client_id, email, prod_desc, price, total_buys, first_buy), ...]

    # Deduplicate by email: same email = same person, keep earliest purchase per email+product
    _seen_email_prod = {}
    buy_rows = []
    for row in sorted(buy_rows_raw, key=lambda r: r[5]):  # sort by first_buy
        client_id, email_id, prod_desc = str(row[0]), (row[1] or '').lower().strip(), str(row[2])
        key = (email_id, prod_desc)
        if email_id and key in _seen_email_prod:
            continue
        if email_id:
            _seen_email_prod[key] = True
        buy_rows.append((client_id, prod_desc, row[3], row[4], row[5]))  # drop email_id

    # ── Query 2: per-client, per-product cancel — raw count, first cancel date
    cur.execute(f"""
        SELECT CLIENT_ID, PRODUCT_DESCRIPTION,
               COUNT(*)             AS total_cancels,
               MIN(SALE_DATE::DATE) AS first_cancel
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
        WHERE STUDIO_ID = {studio_id}
          AND ITEM_TYPE = 'Pricing Option'
          AND LOWER(PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
          AND (QUANTITY = -1 OR IS_RETURN = 1)
          AND SALE_DATE::DATE <= '{end_date}'
        GROUP BY 1, 2
    """)
    cancel_map = {
        (str(r[0]), str(r[1])): {"n": int(r[2]), "date": str(r[3])}
        for r in cur.fetchall()
    }

    # ── Build time-series events in Python ────────────────────────────────
    from collections import defaultdict
    daily = defaultdict(float)  # (date_str, price) → net delta

    for client_id, prod_desc, price, total_buys, first_buy in buy_rows:
        key_cp = (str(client_id), str(prod_desc))
        c_info  = cancel_map.get(key_cp, {"n": 0, "date": None})
        total_cancels = c_info["n"]
        last_cancel   = c_info["date"]

        net = int(total_buys) - total_cancels
        p   = float(price)
        d0  = str(first_buy)

        if net > 0:
            daily[(d0, p)] += 1
        elif net == 0 and last_cancel:
            daily[(d0, p)]          += 1
            daily[(last_cancel, p)] -= 1
        # net < 0 is a data anomaly — skip

    return [(d, p, int(n)) for (d, p), n in sorted(daily.items()) if n != 0]


def build_tier_rmr(daily_sales, weeks, pricing):
    """
    For each week in `weeks`, compute cumulative net tier counts
    using actual prices paid.
    Returns list of {week, t0, t1, t2, t3} dicts (only for weeks w > 0).
    """
    # Cumulative running totals (can go negative during the loop, floor at 0)
    cum = {"t0": 0, "t1": 0, "t2": 0, "t3": 0}
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
            "t0":   cum["t0"],
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

    pricing = STUDIO_PRICING.get(code) or {}
    weeks   = studio.get("weeks", [])

    print(f"  Fetching {studio['name']} (id={sid}) up to {YESTERDAY}...")
    daily = fetch_daily_sales(cur, sid, YESTERDAY)

    tier_rmr = build_tier_rmr(daily, weeks, pricing)
    studio["tier_rmr_by_week"] = tier_rmr

    # Summary
    last = tier_rmr[-1] if tier_rmr else {}
    t0p = pricing.get("tier0_price", 0) or 0
    t1p = pricing.get("tier1_price", 99)
    t2p = pricing.get("tier2_price", 129)
    t3p = pricing.get("tier3_price", 0) or 0
    rmr_est = last.get("t0",0)*t0p + last.get("t1",0)*t1p + last.get("t2",0)*t2p + last.get("t3",0)*t3p
    print(f"    W{last.get('week','?')}: T0={last.get('t0')} T1={last.get('t1')} T2={last.get('t2')} "
          f"T3={last.get('t3')}  Est.RMR=${rmr_est:,.0f}")

conn.close()

with open(SCORECARD_FILE, "w") as f:
    json.dump(sc, f, indent=2)

print(f"\nDone. {SCORECARD_FILE} updated with tier_rmr_by_week for all studios.")
