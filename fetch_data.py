import snowflake.connector
import json
import os
import requests
from datetime import datetime, date, timedelta
from cryptography.hazmat.primitives import serialization

# ── Credentials ───────────────────────────────────────────────────────────────
SF_ACCOUNT   = os.getenv("SF_ACCOUNT",   "MINDBODYORG-PLAYLIST_DATA_MART_SWEAT440")
SF_USER      = os.getenv("SF_USER",      "SWEAT440")
SF_ROLE      = os.getenv("SF_ROLE",      "SYSADMIN")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.getenv("SF_DATABASE",  "MARKETING_REPORTS")
SF_SCHEMA    = os.getenv("SF_SCHEMA",    "PUBLIC")
SF_TOKEN     = os.getenv("SF_TOKEN")

META_TOKEN   = os.getenv("META_TOKEN")
META_ACT     = os.getenv("META_ACT", "act_1553887681409034")  # Corporate Studios
META_API     = "https://graph.facebook.com/v19.0"

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def get_action(actions, *types):
    """Extract value for any of the given action types from Meta actions array."""
    for a in (actions or []):
        if a.get("action_type") in types:
            return int(float(a.get("value", 0)))
    return 0

# ════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE
# ════════════════════════════════════════════════════════════════════════════
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=SF_ACCOUNT, user=SF_USER, token=SF_TOKEN,
    authenticator="programmatic_access_token",
    role=SF_ROLE, warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA
)
cur = conn.cursor()

# ── Daily: previous quarter start → today ─────────────────────────────────
cur.execute("""
    SELECT
        EVENT_DATE, STUDIO_NAME, SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE >= DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
      AND EVENT_DATE <= CURRENT_DATE()
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
daily_detail = [
    {"date": json_serial(r[0]), "studio": r[1], "source": r[2],
     "signups": int(r[3] or 0), "first_visits": int(r[4] or 0),
     "first_activations": int(r[5] or 0), "first_sales": int(r[6] or 0)}
    for r in cur.fetchall()
]

# ── Monthly: older history, 3yr cap ───────────────────────────────────────
cur.execute("""
    SELECT
        DATE_TRUNC('month', EVENT_DATE) AS month,
        STUDIO_NAME, SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE <  DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
      AND EVENT_DATE >= DATEADD('year', -3, CURRENT_DATE())
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
monthly_detail = [
    {"month": json_serial(r[0]), "studio": r[1], "source": r[2],
     "signups": int(r[3] or 0), "first_visits": int(r[4] or 0),
     "first_activations": int(r[5] or 0), "first_sales": int(r[6] or 0)}
    for r in cur.fetchall()
]

# ── Studio + source lists ──────────────────────────────────────────────────
cur.execute("SELECT DISTINCT STUDIO_NAME FROM MARKETING_REPORTS.PUBLIC.LEADS WHERE STUDIO_NAME IS NOT NULL ORDER BY 1")
studios = [r[0] for r in cur.fetchall()]

cur.execute("SELECT DISTINCT SOURCE FROM MARKETING_REPORTS.PUBLIC.LEADS WHERE SOURCE IS NOT NULL ORDER BY 1")
sources = [r[0] for r in cur.fetchall()]

conn.close()
print(f"  Snowflake: {len(daily_detail):,} daily rows, {len(monthly_detail):,} monthly rows")

# ════════════════════════════════════════════════════════════════════════════
# META ADS — Corporate Studios account only (all studios since Apr 1 2026)
# ════════════════════════════════════════════════════════════════════════════
meta_daily   = []
meta_monthly = []

if META_TOKEN:
    print("Fetching Meta Ads data...")

    def meta_get(url, params):
        params["access_token"] = META_TOKEN
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # ── Daily: last 90 days ────────────────────────────────────────────────
    today     = date.today()
    since     = (today - timedelta(days=90)).isoformat()
    until     = today.isoformat()

    params = {
        "fields":         "spend,impressions,clicks,actions,cost_per_action_type",
        "time_range":     json.dumps({"since": since, "until": until}),
        "time_increment": "1",
        "limit":          "90",
        "level":          "account",
    }

    data = meta_get(f"{META_API}/{META_ACT}/insights", params)
    rows = data.get("data", [])

    # paginate if needed
    while "paging" in data and "next" in data.get("paging", {}).get("cursors", {}):
        data = meta_get(data["paging"]["next"], {})
        rows += data.get("data", [])

    for row in rows:
        actions = row.get("actions", [])
        cpa     = row.get("cost_per_action_type", [])

        leads      = get_action(actions, "lead", "onsite_conversion.lead_grouped")
        calls      = get_action(actions, "phone_call", "click_to_call")
        directions = get_action(actions, "get_directions")

        spend      = float(row.get("spend", 0))
        impressions= int(row.get("impressions", 0))
        clicks     = int(row.get("clicks", 0))
        opportunities = leads + calls + directions

        # Cost per lead from cost_per_action_type
        cpl = next((float(a["value"]) for a in cpa if a["action_type"] == "lead"), 0)
        cpo = round(spend / opportunities, 2) if opportunities else 0

        meta_daily.append({
            "date":          row["date_start"],
            "spend":         round(spend, 2),
            "impressions":   impressions,
            "clicks":        clicks,
            "leads":         leads,
            "calls":         calls,
            "directions":    directions,
            "opportunities": opportunities,
            "cpl":           round(cpl, 2),
            "cpo":           cpo,
        })

    # ── Monthly: roll up daily into monthly ───────────────────────────────
    monthly_map = {}
    for row in meta_daily:
        m = row["date"][:7] + "-01"
        if m not in monthly_map:
            monthly_map[m] = {"month": m, "spend": 0, "impressions": 0,
                              "clicks": 0, "leads": 0, "calls": 0,
                              "directions": 0, "opportunities": 0}
        for k in ["spend","impressions","clicks","leads","calls","directions","opportunities"]:
            monthly_map[m][k] += row[k]

    for m in monthly_map.values():
        m["spend"]  = round(m["spend"], 2)
        m["cpl"]    = round(m["spend"] / m["leads"], 2)    if m["leads"]        else 0
        m["cpo"]    = round(m["spend"] / m["opportunities"], 2) if m["opportunities"] else 0
    meta_monthly = sorted(monthly_map.values(), key=lambda x: x["month"])

    print(f"  Meta: {len(meta_daily)} daily rows, {len(meta_monthly)} monthly rows")
    print(f"  Meta: total leads={sum(r['leads'] for r in meta_daily)}, "
          f"spend=${sum(r['spend'] for r in meta_daily):,.2f}")
else:
    print("  Meta: no token, skipping")

# ════════════════════════════════════════════════════════════════════════════
# WRITE OUTPUT
# ════════════════════════════════════════════════════════════════════════════
output = {
    "generated_at":   datetime.utcnow().isoformat() + "Z",
    # Snowflake leads data
    "studios":        studios,
    "sources":        sources,
    "daily_detail":   daily_detail,
    "monthly_detail": monthly_detail,
    # Meta Ads data
    "meta": {
        "account":       "SWEAT440 - Corporate Studios",
        "account_id":    META_ACT,
        "daily":         meta_daily,
        "monthly":       meta_monthly,
    }
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2, default=json_serial)

size_kb = os.path.getsize("data.json") / 1024
print(f"\n✅  data.json written — {size_kb:.1f} KB")
print(f"    Snowflake: {len(daily_detail):,} daily + {len(monthly_detail):,} monthly rows")
print(f"    Meta:      {len(meta_daily)} daily + {len(meta_monthly)} monthly rows")
