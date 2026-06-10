"""
fetch_campaign_data.py
Queries Snowflake and writes campaign_data.json.
Reads credentials from .env in the same directory.
Run: python fetch_campaign_data.py
"""
import json, os
from datetime import datetime, date

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # rely on env vars being set externally

import snowflake.connector

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
TOKEN     = os.environ["SNOWFLAKE_PASSWORD"]   # programmatic access token (JWT)
WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
ROLE      = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")

# Fully-qualified table references
SALES    = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS"
LEADS    = "MARKETING_REPORTS.PUBLIC.LEADS_LIST"
CLIENTS  = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS"

# ── Campaign definitions ────────────────────────────────────────────────────
# Add / remove campaigns here. Each campaign maps to a PRODUCT_DESCRIPTION.
CAMPAIGNS = [
    {
        "id":        "buy-1-get-3",
        "name":      "Buy 1 Get 3 Free",
        "product":   "Buy 1 Get 3 Free",
        "date_from": "2026-05-01",
        "date_to":   "2026-12-31",
    },
]


def serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(type(obj))


def strip_brand(s):
    return (s or "").replace("SWEAT440 ", "").strip()


def q1(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()


def qall(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def build_campaign(cur, camp):
    p, frm, to = camp["product"], camp["date_from"], camp["date_to"]

    # ── 1. Basic KPIs ───────────────────────────────────────────────────────
    r = q1(cur, f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT CLIENT_ID),
            COALESCE(SUM(PAYMENTAMT_LOCAL), 0),
            COALESCE(AVG(PAYMENTAMT_LOCAL), 0)
        FROM (
            SELECT CLIENT_ID, PAYMENTAMT_LOCAL
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s
              AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        )
    """, (p, frm, to))
    total_txns, uniq, total_rev, avg_t = int(r[0]), int(r[1]), float(r[2]), float(r[3])

    # ── 1b. Client segmentation (4 groups) ──────────────────────────────────
    # New           = SIGNEDUP_DATE >= campaign start
    # Platform      = old account, no direct paid purchase, came via ClassPass/Gympass/etc.
    # Free Class    = old account, no direct paid purchase, no platform — only free intro visits
    # Existing      = had at least one direct paid purchase before the promo
    r1b = q1(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, SALE_DATE
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped
            GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf
            JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        )
        SELECT
            COUNT(DISTINCT CASE WHEN sg.earliest_signup >= %s                                                          THEN pf.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NOT NULL    THEN pf.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NULL        THEN pf.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN hp.CLIENT_ID IS NOT NULL                                                          THEN pf.CLIENT_ID END)
        FROM promo_first pf
        LEFT JOIN signup sg         ON pf.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON pf.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON pf.CLIENT_ID = pl.CLIENT_ID
    """, (p, frm, to, frm, frm, frm))
    new_clients        = int(r1b[0])
    platform_clients   = int(r1b[1])
    free_class_clients = int(r1b[2])
    existing_clients   = int(r1b[3])

    # ── 2. Post-promo conversion totals ────────────────────────────────────
    r2 = q1(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID,
                MIN(SALE_DATE) AS PD
            FROM deduped
            GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        pp AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID, SUM(sd.PAYMENTAMT_LOCAL) AS rev
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD
               AND sd.PRODUCT_DESCRIPTION != %s
               AND sd.PAYMENTAMT_LOCAL > 0
            GROUP BY pu.CLIENT_ID, pu.STUDIO_ID
        )
        SELECT
            COUNT(DISTINCT pp.CLIENT_ID),
            COALESCE(SUM(pp.rev), 0)
        FROM pu
        LEFT JOIN pp ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
    """, (p, frm, to, p))
    conv, post_rev = int(r2[0]), float(r2[1])

    kpis = {
        "total_purchases":         total_txns,
        "unique_clients":          uniq,
        "new_clients":             new_clients,
        "platform_clients":        platform_clients,
        "free_class_clients":      free_class_clients,
        "existing_clients":        existing_clients,
        "total_revenue":           round(total_rev, 2),
        "avg_ticket":              round(avg_t, 2),
        "converted_to_paid":       conv,
        "conversion_rate":         round(conv * 100 / max(uniq, 1), 1),
        "post_promo_revenue":      round(post_rev, 2),
        "revenue_per_participant": round(post_rev / max(uniq, 1), 2),
    }

    # ── 3. Lead source attribution ─────────────────────────────────────────
    rows = qall(cur, f"""
        SELECT
            COALESCE(ll.SOURCE, 'N/A') AS src,
            COUNT(DISTINCT sd.CLIENT_ID),
            COUNT(*),
            COALESCE(SUM(sd.PAYMENTAMT_LOCAL), 0),
            COALESCE(AVG(sd.PAYMENTAMT_LOCAL), 0)
        FROM (
            SELECT * FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ) sd
        LEFT JOIN {LEADS} ll
            ON LOWER(TRIM(sd.EMAIL_ID)) = LOWER(TRIM(ll.EMAIL_ID))
           AND sd.STUDIO_ID = ll.STUDIO_ID
        GROUP BY 1
        ORDER BY 2 DESC
    """, (p, frm, to))
    lead_sources = [
        {
            "source":       r[0],
            "clients":      int(r[1]),
            "transactions": int(r[2]),
            "revenue":      round(float(r[3]), 2),
            "avg_ticket":   round(float(r[4]), 2),
        }
        for r in rows
    ]

    # ── 3b. Segment breakdown by lead source (New / Platform / Free Class / Existing) ──
    nt_rows = qall(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, SALE_DATE
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped
            GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf
            JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        )
        SELECT
            COALESCE(ll.SOURCE, 'N/A') AS src,
            COUNT(DISTINCT CASE WHEN sg.earliest_signup >= %s                                                          THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NOT NULL    THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NULL        THEN sd.CLIENT_ID END),
            COUNT(DISTINCT hp.CLIENT_ID)
        FROM deduped sd
        LEFT JOIN {LEADS} ll
            ON LOWER(TRIM(sd.EMAIL_ID)) = LOWER(TRIM(ll.EMAIL_ID))
           AND sd.STUDIO_ID = ll.STUDIO_ID
        LEFT JOIN signup sg         ON sd.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON sd.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON sd.CLIENT_ID = pl.CLIENT_ID
        GROUP BY 1
    """, (p, frm, to, frm, frm, frm))
    nt_src_map = {r[0]: (int(r[1]), int(r[2]), int(r[3]), int(r[4])) for r in nt_rows}
    for src in lead_sources:
        nc, pc, fc, ec = nt_src_map.get(src["source"], (0, 0, 0, 0))
        src["new_clients"]        = nc
        src["platform_clients"]   = pc
        src["free_class_clients"] = fc
        src["existing_clients"]   = ec

    # ── 4. Studio breakdown (studio × source) ──────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped
            GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf
            JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        )
        SELECT
            COALESCE(ll.STUDIO_NAME, sd.STUDIO_NAME, 'Unknown') AS studio,
            COALESCE(ll.SOURCE, 'N/A') AS src,
            COUNT(DISTINCT sd.CLIENT_ID),
            COALESCE(SUM(sd.PAYMENTAMT_LOCAL), 0),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup >= %s                                                          THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NOT NULL    THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NULL        THEN sd.CLIENT_ID END),
            COUNT(DISTINCT hp.CLIENT_ID)
        FROM deduped sd
        LEFT JOIN {LEADS} ll
            ON LOWER(TRIM(sd.EMAIL_ID)) = LOWER(TRIM(ll.EMAIL_ID))
           AND sd.STUDIO_ID = ll.STUDIO_ID
        LEFT JOIN signup sg         ON sd.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON sd.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON sd.CLIENT_ID = pl.CLIENT_ID
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """, (p, frm, to, frm, frm, frm))
    studios = [
        {
            "studio":             strip_brand(r[0]),
            "source":             r[1],
            "clients":            int(r[2]),
            "revenue":            round(float(r[3]), 2),
            "new_clients":        int(r[4]),
            "platform_clients":   int(r[5]),
            "free_class_clients": int(r[6]),
            "existing_clients":   int(r[7]),
        }
        for r in rows
    ]

    # ── 5. Purchase channel ────────────────────────────────────────────────
    rows = qall(cur, f"""
        SELECT
            COALESCE(SOURCE_CHANNEL, 'Unknown'),
            COUNT(*),
            COUNT(DISTINCT CLIENT_ID)
        FROM (
            SELECT SOURCE_CHANNEL, CLIENT_ID
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        )
        GROUP BY 1
        ORDER BY 2 DESC
    """, (p, frm, to))
    channels = [
        {"channel": r[0], "transactions": int(r[1]), "clients": int(r[2])}
        for r in rows
    ]

    # ── 6. Post-promo funnel by source ─────────────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID,
                MIN(SALE_DATE) AS PD
            FROM deduped
            GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        pp AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID, SUM(sd.PAYMENTAMT_LOCAL) AS rev
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD
               AND sd.PRODUCT_DESCRIPTION != %s
               AND sd.PAYMENTAMT_LOCAL > 0
            GROUP BY pu.CLIENT_ID, pu.STUDIO_ID
        )
        SELECT
            COALESCE(ll.SOURCE, 'N/A'),
            COUNT(DISTINCT pu.CLIENT_ID),
            COUNT(DISTINCT pp.CLIENT_ID),
            ROUND(COUNT(DISTINCT pp.CLIENT_ID) * 100.0
                / NULLIF(COUNT(DISTINCT pu.CLIENT_ID), 0), 1),
            COALESCE(SUM(pp.rev), 0)
        FROM pu
        LEFT JOIN pp ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        LEFT JOIN {LEADS} ll
            ON LOWER(TRIM(pu.EMAIL_ID)) = LOWER(TRIM(ll.EMAIL_ID))
           AND pu.STUDIO_ID = ll.STUDIO_ID
        GROUP BY 1
        ORDER BY 2 DESC
    """, (p, frm, to, p))
    funnel = [
        {
            "source":            r[0],
            "received_promo":    int(r[1]),
            "converted_to_paid": int(r[2]),
            "conversion_pct":    float(r[3] or 0),
            "post_promo_revenue": round(float(r[4]), 2),
        }
        for r in rows
    ]

    # ── 7. Post-promo product breakdown ───────────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, MIN(SALE_DATE) AS PD
            FROM deduped
            GROUP BY CLIENT_ID, STUDIO_ID
        )
        SELECT
            sd.PRODUCT_DESCRIPTION,
            sd.ITEM_TYPE,
            sd.REVENUE_CATEGORY,
            COUNT(DISTINCT sd.CLIENT_ID) AS clients,
            COUNT(*) AS transactions,
            ROUND(SUM(sd.PAYMENTAMT_LOCAL), 2) AS revenue
        FROM pu
        JOIN {SALES} sd
            ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
           AND sd.SALE_DATE > pu.PD
           AND sd.PRODUCT_DESCRIPTION != %s
           AND sd.PAYMENTAMT_LOCAL > 0
           AND sd.REVENUE_CATEGORY IN ('Memberships', 'Class/Class Packs')
        GROUP BY 1, 2, 3
        ORDER BY 4 DESC
    """, (p, frm, to, p))
    post_promo_products = [
        {
            "product":      r[0],
            "item_type":    r[1],
            "category":     r[2],
            "clients":      int(r[3]),
            "transactions": int(r[4]),
            "revenue":      round(float(r[5]), 2),
        }
        for r in rows
    ]

    # ── 8. Time series (purchases by day) ──────────────────────────────────
    rows = qall(cur, f"""
        SELECT SALE_DATE, COUNT(*), COALESCE(SUM(PAYMENTAMT_LOCAL), 0)
        FROM (
            SELECT SALE_DATE, PAYMENTAMT_LOCAL
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        )
        GROUP BY 1
        ORDER BY 1
    """, (p, frm, to))
    time_series = [
        {"date": r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[:10],
         "purchases": int(r[1]), "revenue": round(float(r[2]), 2)}
        for r in rows
    ]

    # ── 9. Segment conversion breakdown ────────────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, SALE_DATE
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped
            GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(promo_date) AS first_promo_date
            FROM pu
            GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf
            JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.first_promo_date
              AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID
            FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.first_promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        ),
        pp AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID, SUM(sd.PAYMENTAMT_LOCAL) AS rev
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID
               AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.promo_date
               AND sd.PRODUCT_DESCRIPTION != %s
               AND sd.PAYMENTAMT_LOCAL > 0
            GROUP BY pu.CLIENT_ID, pu.STUDIO_ID
        )
        SELECT
            CASE
                WHEN sg.earliest_signup >= %s THEN 'New'
                WHEN hp.CLIENT_ID IS NOT NULL THEN 'Existing'
                WHEN pl.CLIENT_ID IS NOT NULL THEN 'Platform'
                ELSE 'Free Class'
            END AS segment,
            COUNT(DISTINCT pu.CLIENT_ID)       AS total_clients,
            COUNT(DISTINCT pp.CLIENT_ID)       AS converted,
            ROUND(COUNT(DISTINCT pp.CLIENT_ID) * 100.0
                / NULLIF(COUNT(DISTINCT pu.CLIENT_ID), 0), 1) AS conv_pct,
            COALESCE(SUM(pp.rev), 0)           AS post_rev,
            COALESCE(SUM(pp.rev) / NULLIF(COUNT(DISTINCT pp.CLIENT_ID), 0), 0) AS avg_per_converter
        FROM pu
        LEFT JOIN signup sg         ON pu.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON pu.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON pu.CLIENT_ID = pl.CLIENT_ID
        LEFT JOIN pp                ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        GROUP BY 1
        ORDER BY total_clients DESC
    """, (p, frm, to, p, frm))
    segment_breakdown = [
        {
            "segment":           r[0],
            "total_clients":     int(r[1]),
            "converted":         int(r[2]),
            "conversion_pct":    float(r[3] or 0),
            "post_rev":          round(float(r[4]), 2),
            "avg_per_converter": round(float(r[5]), 2),
        }
        for r in rows
    ]

    # ── 10. Multi-location clients ─────────────────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, EMAIL_ID, FIRST_NAME, LAST_NAME, STUDIO_ID, STUDIO_NAME, SALE_DATE
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_ID) = 1
        )
        SELECT
            MIN(d.EMAIL_ID)                                     AS email,
            TRIM(COALESCE(MIN(d.FIRST_NAME),'') || ' ' || COALESCE(MIN(d.LAST_NAME),'')) AS full_name,
            COUNT(DISTINCT d.STUDIO_ID)                         AS studio_count,
            COUNT(*)                                            AS purchases,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT
                REPLACE(d.STUDIO_NAME, 'SWEAT440 ', '')), ', ') AS studios
        FROM deduped d
        GROUP BY d.CLIENT_ID
        HAVING COUNT(DISTINCT d.STUDIO_ID) > 1
        ORDER BY studio_count DESC, purchases DESC
    """, (p, frm, to))
    multi_location = [
        {
            "email":        r[0] or '',
            "name":         r[1].strip() if r[1] else '',
            "studio_count": int(r[2]),
            "purchases":    int(r[3]),
            "studios":      r[4] or '',
        }
        for r in rows
    ]

    return {
        "id":          camp["id"],
        "name":        camp["name"],
        "product":     p,
        "date_from":   frm,
        "date_to":     to,
        "notes":       camp.get("notes", ""),
        "kpis":               kpis,
        "lead_sources":       lead_sources,
        "studios":            studios,
        "channels":           channels,
        "funnel":             funnel,
        "post_promo_products": post_promo_products,
        "time_series":        time_series,
        "segment_breakdown":  segment_breakdown,
        "multi_location":     multi_location,
    }


# ── Connect and fetch ───────────────────────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    token=TOKEN,
    authenticator="programmatic_access_token",
    role=ROLE,
    warehouse=WAREHOUSE,
)
cur = conn.cursor()

results = []
for camp in CAMPAIGNS:
    print(f"  Fetching: {camp['name']} ({camp['product']})…")
    try:
        data = build_campaign(cur, camp)
        results.append(data)
        k = data["kpis"]
        print(f"    OK  {k['total_purchases']} purchases | {k['unique_clients']} clients | "
              f"{k['conversion_rate']}% conv | ${k['post_promo_revenue']:,.2f} post-rev")
    except Exception as e:
        print(f"    ERROR: {e}")
        raise

conn.close()

out = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "campaigns":    results,
}

with open("campaign_data.json", "w") as f:
    json.dump(out, f, indent=2, default=serial)

kb = os.path.getsize("campaign_data.json") / 1024
print(f"\nDONE  campaign_data.json written ({kb:.1f} KB)")
