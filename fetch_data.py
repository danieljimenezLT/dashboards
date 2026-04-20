import snowflake.connector
import json
import os
from datetime import datetime, date
from cryptography.hazmat.primitives import serialization

# ── Credentials ───────────────────────────────────────────────────────────────
ACCOUNT     = os.getenv("SF_ACCOUNT",   "MINDBODYORG-PLAYLIST_DATA_MART_SWEAT440")
USER        = os.getenv("SF_USER",      "SWEAT440")
ROLE        = os.getenv("SF_ROLE",      "SYSADMIN")
WAREHOUSE   = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
DATABASE    = os.getenv("SF_DATABASE",  "MARKETING_REPORTS")
SCHEMA      = os.getenv("SF_SCHEMA",    "PUBLIC")
TOKEN       = os.getenv("SF_TOKEN")

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, token=TOKEN,
    authenticator="programmatic_access_token",
    role=ROLE, warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA
)
cur = conn.cursor()

# ── 1. Monthly detail: every studio+source+month combo ────────────────────────
# This is the main dataset — all filters are applied client-side from this
cur.execute("""
    SELECT
        DATE_TRUNC('month', EVENT_DATE)  AS month,
        STUDIO_NAME,
        SOURCE,
        SUM(SIGNUPS)                     AS signups,
        SUM(FIRST_VISITS)                AS first_visits,
        SUM(FIRST_ACTIVATIONS)           AS first_activations,
        SUM(FIRST_SALES)                 AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
monthly_detail = [
    {
        "month":             json_serial(r[0]),
        "studio":            r[1],
        "source":            r[2],
        "signups":           int(r[3] or 0),
        "first_visits":      int(r[4] or 0),
        "first_activations": int(r[5] or 0),
        "first_sales":       int(r[6] or 0),
    }
    for r in cur.fetchall()
]

# ── 2. Studio list ─────────────────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT STUDIO_NAME
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE STUDIO_NAME IS NOT NULL
    ORDER BY 1
""")
studios = [r[0] for r in cur.fetchall()]

# ── 3. Source list ─────────────────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT SOURCE
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE SOURCE IS NOT NULL
    ORDER BY 1
""")
sources = [r[0] for r in cur.fetchall()]

# ── Write output ───────────────────────────────────────────────────────────────
output = {
    "generated_at":   datetime.utcnow().isoformat() + "Z",
    "studios":        studios,
    "sources":        sources,
    "monthly_detail": monthly_detail,
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2, default=json_serial)

conn.close()
print(f"✅  data.json written — {len(monthly_detail)} rows, {len(studios)} studios, {len(sources)} sources")
