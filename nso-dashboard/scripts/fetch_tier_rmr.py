"""
fetch_tier_rmr.py
Queries Snowflake for actual presale prices per NSO studio and builds
cumulative tier counts (T1/T2/T3) week by week using REAL prices paid,
then patches nso_scorecard_data.json with 'tier_rmr_by_week' per studio.

Run from nso-dashboard/:
    python scripts/fetch_tier_rmr.py
"""
import json, os, sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

SCORECARD_FILE = "nso_scorecard_data.json"
YESTERDAY = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

# Per-studio pricing config, sourced from studios.json's tier_pricing (originally
# from the NSO Config Google Sheet). tier0_price is a special founders tier below
# the normal T1 (Reston only). Used only for RMR estimation — tier assignment
# uses SKU_TIER_MAP below.
STUDIO_PRICING = studios_registry.tier_pricing_by_code()

_STANDARD_SKU_TIERS = {
    "pre sale membership":  "t1",
    "pre sales membership": "t2",
    "pre-sales membership": "t3",
}


def build_sku_tier_map(sku_map, pricing):
    """Compute {sku_name_lower: tier_key} from the sheet's sku_map + studio pricing.

    sku_map keys are price_99 / price_129 / price_149 (from fetch_sheet_milestones).
    pricing maps tier0_price / tier1_price / tier2_price / tier3_price → int.
    SKU at price_X is assigned the tier whose tier?_price == X.
    Falls back to _STANDARD_SKU_TIERS if sku_map is empty.
    """
    if not sku_map:
        return dict(_STANDARD_SKU_TIERS)

    price_to_tier = {}
    for tier_key in ("t0", "t1", "t2", "t3"):
        p = pricing.get(f"tier{tier_key[1:]}_price")
        if p:
            price_to_tier[int(p)] = tier_key

    result = {}
    for col_key, sku_name in sku_map.items():
        if not sku_name:
            continue
        try:
            price_num = int(col_key.replace("price_", ""))
        except ValueError:
            continue
        tier = price_to_tier.get(price_num)
        if tier:
            result[sku_name.strip().lower()] = tier

    return result or dict(_STANDARD_SKU_TIERS)


def sku_to_tier(prod_desc, sku_tier_map):
    """Assign tier by SKU name using a pre-computed map. Defaults to 't1'."""
    return sku_tier_map.get(prod_desc.strip().lower(), "t1")


# Map Snowflake studio_id from scorecard JSON, sourced from studios.json.
SNOWFLAKE_IDS = studios_registry.snowflake_id_by_code()


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
    TRANSACTION-LEVEL approach — no email dedup, matches fetch_nso_sales.py exactly.
    Groups by (CLIENT_ID, PRODUCT_DESCRIPTION) so every raw MindBody transaction
    is counted independently.

    For each (CLIENT_ID, PRODUCT_DESCRIPTION):
      - net = total_buys - total_cancels
      - net > 0 → active: emit +net on first_buy_date
      - net = 0 → bought then cancelled: emit +buys on first_buy, -cancels on last_cancel
      - net < 0 → data anomaly: skip
    """
    # ── Query 1: buys per (CLIENT_ID, PRODUCT_DESCRIPTION) ───────────────
    cur.execute(f"""
        SELECT CLIENT_ID, PRODUCT_DESCRIPTION,
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
          AND (EMAIL_ID IS NULL OR (
              LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
              AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
              AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
          ))
        GROUP BY 1, 2
    """)
    buy_rows = cur.fetchall()  # [(client_id, prod_desc, price, total_buys, first_buy), ...]

    # ── Query 2: cancels per (CLIENT_ID, PRODUCT_DESCRIPTION) ────────────
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
    daily = defaultdict(float)  # (date_str, prod_desc) → net delta

    for client_id, prod_desc, price, total_buys, first_buy in buy_rows:
        key_cp = (str(client_id), str(prod_desc))
        c_info  = cancel_map.get(key_cp, {"n": 0, "date": None})
        total_cancels = c_info["n"]
        last_cancel   = c_info["date"]

        net = int(total_buys) - total_cancels
        d0  = str(first_buy)

        if net > 0:
            daily[(d0, prod_desc)] += net          # count all active instances, not just 1
        elif net == 0 and last_cancel:
            daily[(d0, prod_desc)]          += int(total_buys)
            daily[(last_cancel, prod_desc)] -= total_cancels
        # net < 0 is a data anomaly — skip

    return [(d, pd, int(n)) for (d, pd), n in sorted(daily.items()) if n != 0]


def build_tier_rmr(daily_sales, weeks, pricing, sku_map=None, code=""):
    """
    For each week in `weeks`, compute cumulative net tier counts
    using SKU name (not price) so discounted members stay in correct tier.
    sku_map comes from the scorecard JSON (written by fetch_sheet_milestones.py).
    Returns list of {week, t0, t1, t2, t3} dicts (only for weeks w > 0).
    """
    sku_tier_map = build_sku_tier_map(sku_map or {}, pricing)
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
            sd, prod_desc, net = sorted_sales[sale_idx]
            sd_str = sd.isoformat() if hasattr(sd, "isoformat") else str(sd)
            if sd_str > date_end:
                break
            tier = sku_to_tier(prod_desc, sku_tier_map)
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
    sku_map = studio.get("sku_map") or {}
    weeks   = studio.get("weeks", [])

    print(f"  Fetching {studio['name']} (id={sid}) up to {YESTERDAY}...")
    daily = fetch_daily_sales(cur, sid, YESTERDAY)

    tier_rmr = build_tier_rmr(daily, weeks, pricing, sku_map, code)

    studio["tier_rmr_by_week"] = tier_rmr

    # Daily tier sales: aggregate by date for precise frontend date filtering.
    # Same email-dedup as tier_rmr_by_week but at day granularity.
    from collections import defaultdict as _dd
    _by_date = _dd(int)
    for d, _pd, n in daily:
        _by_date[str(d)] += n
    studio["tier_daily_sales"] = [
        {"date": d, "count": n}
        for d, n in sorted(_by_date.items())
        if n != 0
    ]

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

print(f"\nDone. {SCORECARD_FILE} updated with tier_rmr_by_week + tier_daily_sales for all studios.")
