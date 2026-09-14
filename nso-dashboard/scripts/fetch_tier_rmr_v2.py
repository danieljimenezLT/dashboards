#!/usr/bin/env python3
"""
fetch_tier_rmr_v2.py

Same logic as fetch_tier_rmr.py but patches nso_scorecard_data2.json
instead of nso_scorecard_data.json.

Since build_tier_rmr() uses date_end from each week object (not week number),
running this against nso_scorecard_data2.json (which has corrected week dates)
produces tier_rmr_by_week values correctly aligned to the new week boundaries.

Run from nso-dashboard/:
    python scripts/fetch_tier_rmr_v2.py
"""

import json, os, sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

SCORECARD_FILE = Path(__file__).resolve().parent.parent / "nso_scorecard_data2.json"
YESTERDAY = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

STUDIO_PRICING = studios_registry.tier_pricing_by_code()
SNOWFLAKE_IDS  = studios_registry.snowflake_id_by_code()

_STANDARD_SKU_TIERS = {
    "pre sale membership":  "t1",
    "pre sales membership": "t2",
    "pre-sales membership": "t3",
}


def build_sku_tier_map(sku_map, pricing):
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
    return sku_tier_map.get(prod_desc.strip().lower(), "t1")


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "PLAYLIST_DATA_MART"),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def fetch_daily_sales(cur, studio_id, end_date, opening_date=None):
    # For opened studios: cap buys strictly before opening_date so post-opening
    # members don't inflate the presale tier counts.
    # For opened studios: cap both buys and cancels before opening_date to match
    # the pre-opening net presales view (consistent with nso_sales_data.json / Summary tab).
    buy_op    = "< " + f"'{opening_date}'" if opening_date else "<= " + f"'{end_date}'"
    cancel_op = "< " + f"'{opening_date}'" if opening_date else "<= " + f"'{end_date}'"

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
          AND SALE_DATE::DATE {buy_op}
          AND (EMAIL_ID IS NULL OR (
              LOWER(TRIM(EMAIL_ID)) NOT LIKE 'test%'
              AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
              AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
          ))
        GROUP BY 1, 2
    """)
    buy_rows = cur.fetchall()

    cur.execute(f"""
        SELECT CLIENT_ID, PRODUCT_DESCRIPTION,
               COUNT(*)             AS total_cancels,
               MIN(SALE_DATE::DATE) AS first_cancel
        FROM PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
        WHERE STUDIO_ID = {studio_id}
          AND ITEM_TYPE = 'Pricing Option'
          AND LOWER(PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
          AND (QUANTITY = -1 OR IS_RETURN = 1)
          AND SALE_DATE::DATE {cancel_op}
        GROUP BY 1, 2
    """)
    cancel_map = {
        (str(r[0]), str(r[1])): {"n": int(r[2]), "date": str(r[3])}
        for r in cur.fetchall()
    }

    from collections import defaultdict
    daily = defaultdict(float)

    if opening_date:
        # Opened studio: count pre-opening buys minus ALL cancellations (including post-opening).
        # Net = buys_before_open - all_cancels. Only emit if net > 0.
        for client_id, prod_desc, price, total_buys, first_buy in buy_rows:
            key_cp        = (str(client_id), str(prod_desc))
            total_cancels = cancel_map.get(key_cp, {"n": 0})["n"]
            net = int(total_buys) - total_cancels
            if net > 0:
                daily[(str(first_buy), prod_desc)] += net
    else:
        for client_id, prod_desc, price, total_buys, first_buy in buy_rows:
            key_cp        = (str(client_id), str(prod_desc))
            c_info        = cancel_map.get(key_cp, {"n": 0, "date": None})
            total_cancels = c_info["n"]
            last_cancel   = c_info["date"]
            net = int(total_buys) - total_cancels
            d0  = str(first_buy)
            if net > 0:
                daily[(d0, prod_desc)] += net
            elif net == 0 and last_cancel:
                daily[(d0, prod_desc)]          += int(total_buys)
                daily[(last_cancel, prod_desc)] -= total_cancels

    return [(d, pd, int(n)) for (d, pd), n in sorted(daily.items()) if n != 0]


def build_tier_rmr(daily_sales, weeks, pricing, sku_map=None, code="", opening_date=None):
    import re
    sku_tier_map = build_sku_tier_map(sku_map or {}, pricing)
    cum = {"t0": 0, "t1": 0, "t2": 0, "t3": 0}
    result = []

    past_weeks = sorted(
        [w for w in weeks
         if w.get("week", "").strip() not in ("Week 0", "WEEK 0", "")
         and w.get("date_end")
         and (not opening_date or w["date_end"] <= opening_date)],
        key=lambda w: w["date_end"]
    )

    sale_idx     = 0
    sorted_sales = sorted(daily_sales, key=lambda r: r[0])

    for wk in past_weeks:
        m = re.search(r"(\d+)", wk.get("week", ""))
        if not m:
            continue
        wnum     = int(m.group(1))
        date_end = wk["date_end"]

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if not SCORECARD_FILE.exists():
    print(f"ERROR: {SCORECARD_FILE} not found — run build_scorecard_v2.py first")
    sys.exit(1)

with open(SCORECARD_FILE, encoding="utf-8") as f:
    sc = json.load(f)

print(f"Connecting to Snowflake...")
conn = connect()
cur  = conn.cursor()

for studio in sc.get("studios", []):
    code = studio.get("code", "")
    sid  = SNOWFLAKE_IDS.get(code)
    if not sid:
        print(f"  {code}: no Snowflake ID configured, skipping")
        continue

    # Prefer pricing from the scorecard JSON (kept up-to-date by fetch_sheet_milestones.py)
    # so tier1/2/3 prices are always correct; fall back to the studio registry.
    pricing = studio.get("pricing") or STUDIO_PRICING.get(code) or {}
    sku_map = studio.get("sku_map") or {}
    weeks   = studio.get("weeks", [])

    # For studios that have already opened, cap buys before opening_date and
    # count all cancellations so tier counts reflect true pre-opening net presales.
    op_date = studio.get("opening_date")
    opening_date = op_date if op_date and op_date <= YESTERDAY else None

    print(f"  Fetching {studio['name']} (id={sid}) up to {YESTERDAY}"
          + (f" [opened {opening_date}]" if opening_date else "") + "...")
    daily    = fetch_daily_sales(cur, sid, YESTERDAY, opening_date)
    tier_rmr = build_tier_rmr(daily, weeks, pricing, sku_map, code, opening_date)

    studio["tier_rmr_by_week"] = tier_rmr

    from collections import defaultdict as _dd
    _by_date = _dd(int)
    for d, _pd, n in daily:
        _by_date[str(d)] += n
    studio["tier_daily_sales"] = [
        {"date": d, "count": n}
        for d, n in sorted(_by_date.items())
        if n != 0
    ]

    last   = tier_rmr[-1] if tier_rmr else {}
    t0p    = pricing.get("tier0_price", 0) or 0
    t1p    = pricing.get("tier1_price", 99)
    t2p    = pricing.get("tier2_price", 129)
    t3p    = pricing.get("tier3_price", 0) or 0
    rmr_est = (last.get("t0", 0)*t0p + last.get("t1", 0)*t1p +
               last.get("t2", 0)*t2p + last.get("t3", 0)*t3p)
    print(f"    W{last.get('week','?')}: T0={last.get('t0')} T1={last.get('t1')} "
          f"T2={last.get('t2')} T3={last.get('t3')}  Est.RMR=${rmr_est:,.0f}")

conn.close()

with open(SCORECARD_FILE, "w", encoding="utf-8") as f:
    json.dump(sc, f, indent=2)

print(f"\nDone. {SCORECARD_FILE.name} updated with tier_rmr_by_week + tier_daily_sales.")
