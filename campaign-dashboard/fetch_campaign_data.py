"""
fetch_campaign_data.py
Queries Snowflake and writes campaign_data.json.
Source attribution uses the same _SOURCE_CASE logic as the NSO dashboard
(MART_LEADS_LOG + MART_CLIENTS), not the pre-computed LEADS_LIST view.
Run: python fetch_campaign_data.py
"""
import json, os
from datetime import datetime, date

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import snowflake.connector

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
TOKEN     = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
ROLE      = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")

SALES     = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS"
LEADS_LOG = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_LEADS_LOG"
CLIENTS   = "PLAYLIST_DATA_MART.MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS"
# Client-level version of MARKETING_REPORTS.PUBLIC.LEADS (the view behind the
# Open Studio dashboard's data.json) — same SOURCE attribution, one row per
# client+studio.
LEADS_LIST = "MARKETING_REPORTS.PUBLIC.LEADS_LIST"

# ── Campaign definitions ────────────────────────────────────────────────────
CAMPAIGNS = [
    {
        "id":            "buy-1-get-3",
        "name":          "Buy 1 Get 3 Free",
        "product":       "Buy 1 Get 3 Free",
        "date_from":     "2026-05-12",
        "date_to":       "2026-05-26",
        "post_promo_to": "2026-06-30",
    },
    {
        "id":            "summer-pass",
        "name":          "Summer Pass",
        "product":       "3 Month Summer Pass",
        "date_from":     "2026-05-15",
        "date_to":       "2026-06-15",
        "post_promo_to": "2026-10-15",
    },
    {
        "id":            "33-percent-off",
        "name":          "33% Off",
        "product":       "Summer 33% off 10 and 20 pack",
        "filter_col":    "PROMOTION_NAME",   # filter by promotion, not product
        "filter_value":  "Summer 33% off 10 and 20 pack",
        "date_from":     "2026-07-01",
        "date_to":       "2026-07-31",
        "post_promo_to": "2026-09-26",
    },
    # Buy 3 Get 5 Free — two variants sharing one campaign button (the
    # dashboard groups entries by "name" and shows a toggle for "variant").
    # SKU is "Buy 3 get 5 free" (8 classes, $96). "Buy 3 Get 2 Free" is an
    # unrelated 5-class pack. The product stayed on sale in MindBody after the
    # promo ended, so the second variant tracks those out-of-window sales.
    {
        "id":            "buy-3-get-5",
        "name":          "Buy 3 Get 5 Free",
        "variant":       "Official Campaign",
        "product":       "Buy 3 get 5 free",   # exact PRODUCT_DESCRIPTION in Snowflake
        "date_from":     "2026-08-31",
        "date_to":       "2026-09-07",
        "post_promo_to": "2026-09-27",
    },
    {
        "id":            "buy-3-get-5-outside",
        "name":          "Buy 3 Get 5 Free",
        "variant":       "Outside Official Dates",
        "product":       "Buy 3 get 5 free",
        "date_from":     "2026-08-25",
        "date_to":       "2026-09-27",
        # Sales inside the official window belong to the other variant.
        "exclude_from":  "2026-08-31",
        "exclude_to":    "2026-09-07",
        "post_promo_to": "2026-09-27",
    },
    {
        "id":            "two-weeks-for-40",
        "name":          "2 Weeks for $40",
        "product":       "2 Weeks for $40",
        "date_from":     "2026-08-25",
        "date_to":       "2026-09-07",
        "post_promo_to": "2026-10-07",
        # Same ~25-studio allowlist fetch_2weeks_data.py used — this promo
        # ran at open studios only, not NSO/presale locations (e.g. Naples -
        # Mercato sold the same-named product during its own presale, but
        # that's unrelated activity, not this campaign).
        "studio_filter": [
            "Boca Raton", "Brickell", "Chelsea", "Coconut Grove", "Coral Gables",
            "Coral Springs", "Deerfield", "Doral", "Eastchester", "FiDi", "Highland",
            "Las Olas", "Madison", "Miami Beach", "Miami Lakes", "Midtown", "Miramar",
            "NoDa", "Park Slope", "Pembroke", "Pinecrest", "Prestonwood", "Reston",
            "South Miami", "Upper East", "West Palm",
        ],
        # Meta-only post-promo for the Ads Report tab (see build_meta_post_promo).
        # Excludes studios where this Meta campaign had no spend: their "Meta
        # Ads" leads come from other campaigns (NSO / presale).
        "meta_post_promo": {
            "exclude_studios": ["Prestonwood", "Highland", "Reston"],
        },
    },
]

# ── Source attribution (mirrors MARKETING_REPORTS.PUBLIC.LEADS view) ────────
_SOURCE_CASE = """
    CASE
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'facebook lead ad'    THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'instagram'            THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%%facebook%%'
         AND LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%%lead%%'          THEN 'Meta Ads'
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%%instagram%%'     THEN 'Meta Ads'
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
        WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('business app','business mode','public api')
                                                                 THEN 'Business Mode'
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

# Shared CTEs for source attribution
_LEADS_SRC_CTE = f"""
    leads_src AS (
        SELECT
            LOWER(TRIM(CLIENT_EMAIL))   AS email,
            LEAD_SOURCE,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(CLIENT_EMAIL))
                ORDER BY
                    CASE
                        WHEN LOWER(TRIM(LEAD_SOURCE)) = 'facebook lead ad'          THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) LIKE '%%instagram%%'          THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) LIKE '%%facebook%%'
                         AND LOWER(TRIM(LEAD_SOURCE)) LIKE '%%lead%%'               THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) LIKE '%%google%%'             THEN 0
                        WHEN LOWER(TRIM(LEAD_SOURCE)) = 'public api'                THEN 3
                        WHEN LEAD_SOURCE IS NULL                                     THEN 2
                        ELSE 1
                    END,
                    STAGE_START ASC
            ) AS rn
        FROM {LEADS_LOG}
    )"""

_CLIENTS_SRC_CTE = f"""
    clients_src AS (
        SELECT
            LOWER(TRIM(EMAIL_ID))       AS email,
            REFERRED_BY,
            CAST(SIGNEDUP_DATE AS DATE) AS SIGNEDUP_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(EMAIL_ID))
                ORDER BY SIGNEDUP_DATE ASC
            ) AS rn
        FROM {CLIENTS}
    )"""

def quality_filter(alias='sd'):
    """Quality post-promo product filter: all memberships + 10/20 packs."""
    return f"""(
        {alias}.REVENUE_CATEGORY = 'Memberships'
     OR LOWER(TRIM({alias}.PRODUCT_DESCRIPTION)) IN ('10 pack','20 pack')
)"""

_QUALITY_FILTER    = quality_filter('sd')  # used in JOIN conditions (queries 6, 7, 9)
_QUALITY_FILTER_PP = quality_filter('pp')  # used in outer SELECT from pp CTE (queries 2, 4b)


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
    frm, to = camp["date_from"], camp["date_to"]
    ppt = camp.get("post_promo_to", "2099-12-31")
    fc  = camp.get("filter_col", "PRODUCT_DESCRIPTION")     # column to filter on
    p   = camp.get("filter_value", camp["product"])          # value to match
    filt     = f"{fc} = %s"                                  # positive filter
    neg_filt = f"({fc} != %s OR {fc} IS NULL)" if fc != "PRODUCT_DESCRIPTION" else f"{fc} != %s"

    # Net sales only: a refunded sale (IS_RETURNED=1) and its refund row
    # (IS_RETURN=1, negative amount) cancel out, so neither counts as a campaign
    # purchase, and a refunded post-promo purchase isn't a conversion. filt is
    # only used on bare {SALES} scans; neg_filt only in joins aliased "sd".
    filt     = f"{filt} AND IS_RETURN = 0 AND IS_RETURNED = 0"
    neg_filt = f"{neg_filt} AND sd.IS_RETURNED = 0"

    # Optional studio allowlist — restricts which studios' purchases count as
    # this campaign (e.g. "2 Weeks for $40" only ran at ~25 open studios, not
    # NSO/presale locations that happened to sell the same-named product).
    # Applied only to filt/neg_filt (which define "who bought the campaign
    # product, at which studio"), not to the unrelated prior-history JOINs
    # used for client segmentation (those look at a client's full history
    # regardless of studio).
    studio_names = camp.get("studio_filter")
    if studio_names:
        # %% (not %) — this is embedded directly into the SQL string, not
        # passed as a bind param, so literal % wildcards must be escaped for
        # the connector's pyformat parameter substitution.
        studio_clause    = "(" + " OR ".join(f"STUDIO_NAME ILIKE '%%{s}%%'"    for s in studio_names) + ")"
        studio_clause_sd = "(" + " OR ".join(f"sd.STUDIO_NAME ILIKE '%%{s}%%'" for s in studio_names) + ")"
        filt     = f"{filt} AND {studio_clause}"
        neg_filt = f"{neg_filt} AND {studio_clause_sd}"

    # Optional excluded sub-range inside date_from..date_to (e.g. the "outside
    # official dates" variant skips the official promo window). Only filt gets
    # it: it defines which purchases count as the campaign; post-promo
    # purchases (neg_filt) are unaffected. Dates come from the config above,
    # so embedding them as literals is safe.
    ex_from, ex_to = camp.get("exclude_from"), camp.get("exclude_to")
    if ex_from and ex_to:
        filt = f"{filt} AND NOT (SALE_DATE BETWEEN '{ex_from}' AND '{ex_to}')"

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
            WHERE {filt}
              AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        )
    """, (p, frm, to))
    total_txns, uniq, total_rev, avg_t = int(r[0]), int(r[1]), float(r[2]), float(r[3])

    # ── 1b. Client segmentation ─────────────────────────────────────────────
    r1b = q1(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, SALE_DATE
            FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped GROUP BY CLIENT_ID
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
            WHERE s.SALE_DATE < pf.promo_date AND s.PAYMENTAMT_LOCAL > 0
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
            COUNT(DISTINCT CASE WHEN sg.earliest_signup >= %s                                                       THEN pf.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NOT NULL THEN pf.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NULL     THEN pf.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN hp.CLIENT_ID IS NOT NULL                                                       THEN pf.CLIENT_ID END)
        FROM promo_first pf
        LEFT JOIN signup sg         ON pf.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON pf.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON pf.CLIENT_ID = pl.CLIENT_ID
    """, (p, frm, to, frm, frm, frm))
    new_clients        = int(r1b[0])
    platform_clients   = int(r1b[1])
    free_class_clients = int(r1b[2])
    existing_clients   = int(r1b[3])

    # ── 2. Post-promo conversions — any paid + quality products split ────────
    r2 = q1(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, MIN(SALE_DATE) AS PD
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        pp_raw AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID,
                sd.REVENUE_CATEGORY, sd.PRODUCT_DESCRIPTION, sd.PAYMENTAMT_LOCAL,
                ROW_NUMBER() OVER (PARTITION BY pu.CLIENT_ID, pu.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
        ),
        pp AS (
            SELECT CLIENT_ID, STUDIO_ID, REVENUE_CATEGORY, PRODUCT_DESCRIPTION,
                SUM(CASE WHEN REVENUE_CATEGORY = 'Memberships' AND mem_rn > 1 THEN 0 ELSE PAYMENTAMT_LOCAL END) AS rev
            FROM pp_raw
            GROUP BY CLIENT_ID, STUDIO_ID, REVENUE_CATEGORY, PRODUCT_DESCRIPTION
        )
        SELECT
            COUNT(DISTINCT pp.CLIENT_ID)                                                   AS conv_any,
            COALESCE(SUM(pp.rev), 0)                                                       AS rev_any,
            COUNT(DISTINCT CASE WHEN pp.REVENUE_CATEGORY = 'Memberships'       THEN pp.CLIENT_ID END) AS conv_mem,
            COALESCE(SUM(CASE WHEN pp.REVENUE_CATEGORY = 'Memberships'         THEN pp.rev END), 0)   AS rev_mem,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(pp.PRODUCT_DESCRIPTION)) IN ('10 pack','20 pack') THEN pp.CLIENT_ID END) AS conv_pack,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(pp.PRODUCT_DESCRIPTION)) IN ('10 pack','20 pack')   THEN pp.rev END), 0)   AS rev_pack,
            COUNT(DISTINCT CASE WHEN {_QUALITY_FILTER_PP}                        THEN pp.CLIENT_ID END) AS conv_quality,
            COALESCE(SUM(CASE WHEN {_QUALITY_FILTER_PP}                          THEN pp.rev END), 0)   AS rev_quality
        FROM pu
        LEFT JOIN pp ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
    """, (p, frm, to, ppt, p))
    conv        = int(r2[0]);  post_rev    = float(r2[1])
    conv_mem    = int(r2[2]);  rev_mem     = float(r2[3])
    conv_pack   = int(r2[4]);  rev_pack    = float(r2[5])
    conv_qual   = int(r2[6]);  rev_qual    = float(r2[7])

    kpis = {
        "total_purchases":           total_txns,
        "unique_clients":            uniq,
        "new_clients":               new_clients,
        "platform_clients":          platform_clients,
        "free_class_clients":        free_class_clients,
        "existing_clients":          existing_clients,
        "total_revenue":             round(total_rev, 2),
        "avg_ticket":                round(avg_t, 2),
        "quality_converted":         conv_qual,
        "quality_conv_rate":         round(conv_qual * 100 / max(uniq, 1), 1),
        "quality_revenue":           round(rev_qual, 2),
        "membership_conversions":    conv_mem,
        "membership_conv_rate":      round(conv_mem * 100 / max(uniq, 1), 1),
        "membership_revenue":        round(rev_mem, 2),
        "pack_conversions":          conv_pack,
        "pack_conv_rate":            round(conv_pack * 100 / max(uniq, 1), 1),
        "pack_revenue":              round(rev_pack, 2),
        "converted_to_paid":         conv,
        "conversion_rate":           round(conv * 100 / max(uniq, 1), 1),
        "post_promo_revenue":        round(post_rev, 2),
        "revenue_per_participant":   round(post_rev / max(uniq, 1), 2),
    }

    # ── 3. Lead source attribution ─────────────────────────────────────────
    rows = qall(cur, f"""
        WITH {_LEADS_SRC_CTE},
        {_CLIENTS_SRC_CTE},
        deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        )
        SELECT
            COALESCE({_SOURCE_CASE}, 'N/A') AS src,
            COUNT(DISTINCT sd.CLIENT_ID),
            COUNT(*),
            COALESCE(SUM(sd.PAYMENTAMT_LOCAL), 0),
            COALESCE(AVG(sd.PAYMENTAMT_LOCAL), 0)
        FROM deduped sd
        LEFT JOIN leads_src  l ON LOWER(TRIM(sd.EMAIL_ID)) = l.email  AND l.rn = 1
        LEFT JOIN clients_src c ON LOWER(TRIM(sd.EMAIL_ID)) = c.email AND c.rn = 1
        GROUP BY 1
        ORDER BY 2 DESC
    """, (p, frm, to))
    lead_sources = [
        {"source": r[0], "clients": int(r[1]), "transactions": int(r[2]),
         "revenue": round(float(r[3]), 2), "avg_ticket": round(float(r[4]), 2)}
        for r in rows
    ]

    # ── 3b. Segment breakdown by lead source ────────────────────────────────
    nt_rows = qall(cur, f"""
        WITH {_LEADS_SRC_CTE},
        {_CLIENTS_SRC_CTE},
        deduped AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, SALE_DATE
            FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(SALE_DATE) AS promo_date FROM deduped GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        )
        SELECT
            COALESCE({_SOURCE_CASE}, 'N/A') AS src,
            COUNT(DISTINCT CASE WHEN sg.earliest_signup >= %s                                                       THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NOT NULL THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NULL     THEN sd.CLIENT_ID END),
            COUNT(DISTINCT hp.CLIENT_ID)
        FROM deduped sd
        LEFT JOIN leads_src  l ON LOWER(TRIM(sd.EMAIL_ID)) = l.email  AND l.rn = 1
        LEFT JOIN clients_src c ON LOWER(TRIM(sd.EMAIL_ID)) = c.email AND c.rn = 1
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
        WITH {_LEADS_SRC_CTE},
        {_CLIENTS_SRC_CTE},
        deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(SALE_DATE) AS promo_date FROM deduped GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        )
        SELECT
            REPLACE(sd.STUDIO_NAME, 'SWEAT440 ', '') AS studio,
            COALESCE({_SOURCE_CASE}, 'N/A')           AS src,
            COUNT(DISTINCT sd.CLIENT_ID),
            COUNT(*)                                   AS purchases,
            COALESCE(SUM(sd.PAYMENTAMT_LOCAL), 0),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup >= %s                                                       THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NOT NULL THEN sd.CLIENT_ID END),
            COUNT(DISTINCT CASE WHEN sg.earliest_signup <  %s AND hp.CLIENT_ID IS NULL AND pl.CLIENT_ID IS NULL     THEN sd.CLIENT_ID END),
            COUNT(DISTINCT hp.CLIENT_ID)
        FROM deduped sd
        LEFT JOIN leads_src  l ON LOWER(TRIM(sd.EMAIL_ID)) = l.email  AND l.rn = 1
        LEFT JOIN clients_src c ON LOWER(TRIM(sd.EMAIL_ID)) = c.email AND c.rn = 1
        LEFT JOIN signup sg         ON sd.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON sd.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON sd.CLIENT_ID = pl.CLIENT_ID
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """, (p, frm, to, frm, frm, frm))
    studios = [
        {"studio": r[0], "source": r[1], "clients": int(r[2]), "purchases": int(r[3]),
         "revenue": round(float(r[4]), 2), "new_clients": int(r[5]),
         "platform_clients": int(r[6]), "free_class_clients": int(r[7]),
         "existing_clients": int(r[8])}
        for r in rows
    ]

    # ── 4b. Per-studio membership + pack conversions ───────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, STUDIO_NAME, MIN(SALE_DATE) AS PD
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID, STUDIO_NAME
        ),
        pp_raw AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID,
                sd.REVENUE_CATEGORY, sd.PRODUCT_DESCRIPTION, sd.PAYMENTAMT_LOCAL,
                ROW_NUMBER() OVER (PARTITION BY pu.CLIENT_ID, pu.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
        ),
        pp AS (
            SELECT CLIENT_ID, STUDIO_ID, REVENUE_CATEGORY, PRODUCT_DESCRIPTION,
                SUM(CASE WHEN REVENUE_CATEGORY = 'Memberships' AND mem_rn > 1 THEN 0 ELSE PAYMENTAMT_LOCAL END) AS rev
            FROM pp_raw
            GROUP BY CLIENT_ID, STUDIO_ID, REVENUE_CATEGORY, PRODUCT_DESCRIPTION
        )
        SELECT
            REPLACE(pu.STUDIO_NAME, 'SWEAT440 ', '')                                              AS studio,
            COUNT(DISTINCT pu.CLIENT_ID)                                                           AS total,
            COUNT(DISTINCT CASE WHEN pp.REVENUE_CATEGORY = 'Memberships'       THEN pu.CLIENT_ID END) AS mem_conv,
            COALESCE(SUM(CASE WHEN pp.REVENUE_CATEGORY = 'Memberships'         THEN pp.rev END), 0)   AS mem_rev,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(pp.PRODUCT_DESCRIPTION)) IN ('10 pack','20 pack') THEN pu.CLIENT_ID END) AS pack_conv,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(pp.PRODUCT_DESCRIPTION)) IN ('10 pack','20 pack')   THEN pp.rev END), 0)   AS pack_rev,
            COUNT(DISTINCT CASE WHEN {_QUALITY_FILTER_PP}                        THEN pu.CLIENT_ID END) AS quality_conv,
            COALESCE(SUM(CASE WHEN {_QUALITY_FILTER_PP}                          THEN pp.rev END), 0)   AS quality_rev,
            COALESCE(SUM(pp.rev), 0)                                                               AS total_post_rev
        FROM pu
        LEFT JOIN pp ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        GROUP BY 1 ORDER BY 1
    """, (p, frm, to, ppt, p))
    studio_conversions = [
        {"studio":         r[0],
         "promo_clients":  int(r[1]),
         "mem_conv":       int(r[2]),
         "mem_conv_pct":   round(int(r[2]) * 100 / max(int(r[1]), 1), 1),
         "mem_revenue":    round(float(r[3]), 2),
         "pack_conv":      int(r[4]),
         "pack_revenue":   round(float(r[5]), 2),
         "quality_conv":   int(r[6]),
         "quality_conv_pct": round(int(r[6]) * 100 / max(int(r[1]), 1), 1),
         "quality_revenue":  round(float(r[7]), 2),
         "total_post_rev": round(float(r[8]), 2)}
        for r in rows
    ]

    # ── 5. Purchase channel ────────────────────────────────────────────────
    rows = qall(cur, f"""
        SELECT COALESCE(SOURCE_CHANNEL, 'Unknown'), COUNT(*), COUNT(DISTINCT CLIENT_ID)
        FROM (
            SELECT SOURCE_CHANNEL, CLIENT_ID FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        )
        GROUP BY 1 ORDER BY 2 DESC
    """, (p, frm, to))
    channels = [{"channel": r[0], "transactions": int(r[1]), "clients": int(r[2])} for r in rows]

    # ── 6. Post-promo funnel by source (quality products only) ──────────────
    rows = qall(cur, f"""
        WITH {_LEADS_SRC_CTE},
        {_CLIENTS_SRC_CTE},
        deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, MIN(STUDIO_NAME) AS STUDIO_NAME, EMAIL_ID, MIN(SALE_DATE) AS PD
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        pp_raw AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID,
                sd.REVENUE_CATEGORY, sd.PRODUCT_DESCRIPTION, sd.PAYMENTAMT_LOCAL,
                ROW_NUMBER() OVER (PARTITION BY pu.CLIENT_ID, pu.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
               AND {_QUALITY_FILTER}
        ),
        pp AS (
            SELECT CLIENT_ID, STUDIO_ID,
                SUM(CASE WHEN REVENUE_CATEGORY = 'Memberships' AND mem_rn > 1 THEN 0 ELSE PAYMENTAMT_LOCAL END) AS rev
            FROM pp_raw
            GROUP BY CLIENT_ID, STUDIO_ID
        )
        SELECT
            COALESCE({_SOURCE_CASE}, 'N/A'),
            COUNT(DISTINCT pu.CLIENT_ID),
            COUNT(DISTINCT pp.CLIENT_ID),
            ROUND(COUNT(DISTINCT pp.CLIENT_ID) * 100.0 / NULLIF(COUNT(DISTINCT pu.CLIENT_ID), 0), 1),
            COALESCE(SUM(pp.rev), 0)
        FROM pu
        LEFT JOIN pp ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        LEFT JOIN leads_src  l ON LOWER(TRIM(pu.EMAIL_ID)) = l.email  AND l.rn = 1
        LEFT JOIN clients_src c ON LOWER(TRIM(pu.EMAIL_ID)) = c.email AND c.rn = 1
        GROUP BY 1 ORDER BY 2 DESC
    """, (p, frm, to, ppt, p))
    funnel = [
        {"source": r[0], "received_promo": int(r[1]), "converted_to_paid": int(r[2]),
         "conversion_pct": float(r[3] or 0), "post_promo_revenue": round(float(r[4]), 2)}
        for r in rows
    ]

    # ── 6b. Post-promo funnel by source × studio ───────────────────────────
    rows = qall(cur, f"""
        WITH {_LEADS_SRC_CTE},
        {_CLIENTS_SRC_CTE},
        deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, MIN(STUDIO_NAME) AS STUDIO_NAME, EMAIL_ID, MIN(SALE_DATE) AS PD
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        pp_raw AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID,
                sd.REVENUE_CATEGORY, sd.PRODUCT_DESCRIPTION, sd.PAYMENTAMT_LOCAL,
                ROW_NUMBER() OVER (PARTITION BY pu.CLIENT_ID, pu.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
               AND {_QUALITY_FILTER}
        ),
        pp AS (
            SELECT CLIENT_ID, STUDIO_ID,
                SUM(CASE WHEN REVENUE_CATEGORY = 'Memberships' AND mem_rn > 1 THEN 0 ELSE PAYMENTAMT_LOCAL END) AS rev
            FROM pp_raw
            GROUP BY CLIENT_ID, STUDIO_ID
        )
        SELECT
            REPLACE(pu.STUDIO_NAME, 'SWEAT440 ', '') AS studio,
            COALESCE({_SOURCE_CASE}, 'N/A')           AS src,
            COUNT(DISTINCT pu.CLIENT_ID),
            COUNT(DISTINCT pp.CLIENT_ID),
            ROUND(COUNT(DISTINCT pp.CLIENT_ID) * 100.0 / NULLIF(COUNT(DISTINCT pu.CLIENT_ID), 0), 1),
            COALESCE(SUM(pp.rev), 0)
        FROM pu
        LEFT JOIN pp ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        LEFT JOIN leads_src  l ON LOWER(TRIM(pu.EMAIL_ID)) = l.email  AND l.rn = 1
        LEFT JOIN clients_src c ON LOWER(TRIM(pu.EMAIL_ID)) = c.email AND c.rn = 1
        GROUP BY 1, 2
        ORDER BY studio, 3 DESC
    """, (p, frm, to, ppt, p))
    funnel_by_studio = [
        {"studio": r[0], "source": r[1], "received_promo": int(r[2]),
         "converted_to_paid": int(r[3]), "conversion_pct": float(r[4] or 0),
         "post_promo_revenue": round(float(r[5]), 2)}
        for r in rows
    ]

    # ── 7. Post-promo product breakdown (quality products only) ────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, MIN(SALE_DATE) AS PD
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID
        ),
        sd_all AS (
            SELECT sd.CLIENT_ID, sd.STUDIO_ID, sd.PRODUCT_DESCRIPTION, sd.ITEM_TYPE,
                   sd.REVENUE_CATEGORY, sd.PAYMENTAMT_LOCAL,
                   ROW_NUMBER() OVER (PARTITION BY sd.CLIENT_ID, sd.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
               AND sd.IS_RETURNED = 0
               AND {_QUALITY_FILTER}
        )
        SELECT
            TRIM(PRODUCT_DESCRIPTION) AS product,
            ITEM_TYPE,
            REVENUE_CATEGORY,
            COUNT(DISTINCT CLIENT_ID) AS clients,
            COUNT(*) AS transactions,
            ROUND(SUM(PAYMENTAMT_LOCAL), 2) AS revenue
        FROM sd_all
        WHERE REVENUE_CATEGORY != 'Memberships' OR mem_rn = 1
        GROUP BY 1, 2, 3
        ORDER BY transactions DESC
    """, (p, frm, to, ppt, p))
    post_promo_products = [
        {"product": r[0], "item_type": r[1], "category": r[2],
         "clients": int(r[3]), "transactions": int(r[4]), "revenue": round(float(r[5]), 2)}
        for r in rows
    ]

    # ── 7b. Post-promo products × studio ──────────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT * FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, MIN(STUDIO_NAME) AS STUDIO_NAME, MIN(SALE_DATE) AS PD
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID
        ),
        sd_all AS (
            SELECT pu.STUDIO_NAME, sd.CLIENT_ID, sd.STUDIO_ID, sd.PRODUCT_DESCRIPTION, sd.ITEM_TYPE,
                   sd.REVENUE_CATEGORY, sd.PAYMENTAMT_LOCAL,
                   ROW_NUMBER() OVER (PARTITION BY sd.CLIENT_ID, sd.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.PD AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
               AND sd.IS_RETURNED = 0
               AND {_QUALITY_FILTER}
        )
        SELECT
            REPLACE(STUDIO_NAME, 'SWEAT440 ', '') AS studio,
            TRIM(PRODUCT_DESCRIPTION) AS product,
            ITEM_TYPE,
            REVENUE_CATEGORY,
            COUNT(DISTINCT CLIENT_ID) AS clients,
            COUNT(*) AS transactions,
            ROUND(SUM(PAYMENTAMT_LOCAL), 2) AS revenue
        FROM sd_all
        WHERE REVENUE_CATEGORY != 'Memberships' OR mem_rn = 1
        GROUP BY 1, 2, 3, 4
        ORDER BY studio, transactions DESC
    """, (p, frm, to, ppt, p))
    products_by_studio = [
        {"studio": r[0], "product": r[1], "item_type": r[2], "category": r[3],
         "clients": int(r[4]), "transactions": int(r[5]), "revenue": round(float(r[6]), 2)}
        for r in rows
    ]

    # Sync membership/pack conversions with transaction counts from post_promo_products
    # Memberships: count by unique clients (already in kpis from conv_mem)
    # Packs: count by transactions (more accurate for one-time purchases)
    pack_txn = sum(p['transactions'] for p in post_promo_products if p['category'] != 'Memberships')
    kpis['pack_conversions'] = pack_txn
    kpis['pack_conv_rate']   = round(pack_txn * 100 / max(uniq, 1), 1)

    # ── 8. Time series (purchases by day) ──────────────────────────────────
    rows = qall(cur, f"""
        SELECT SALE_DATE, COUNT(*), COALESCE(SUM(PAYMENTAMT_LOCAL), 0)
        FROM (
            SELECT SALE_DATE, PAYMENTAMT_LOCAL FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        )
        GROUP BY 1 ORDER BY 1
    """, (p, frm, to))
    time_series = [
        {"date": r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[:10],
         "purchases": int(r[1]), "revenue": round(float(r[2]), 2)}
        for r in rows
    ]

    # ── 8b. Time series × studio ───────────────────────────────────────────
    rows = qall(cur, f"""
        SELECT REPLACE(STUDIO_NAME, 'SWEAT440 ', ''), SALE_DATE, COUNT(*), COALESCE(SUM(PAYMENTAMT_LOCAL), 0)
        FROM (
            SELECT SALE_DATE, STUDIO_NAME, PAYMENTAMT_LOCAL FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        )
        GROUP BY 1, 2
        ORDER BY 2, 1
    """, (p, frm, to))
    time_series_by_studio = [
        {"studio": r[0],
         "date": r[1].strftime('%Y-%m-%d') if hasattr(r[1], 'strftime') else str(r[1])[:10],
         "purchases": int(r[2]), "revenue": round(float(r[3]), 2)}
        for r in rows
    ]

    # ── 9. Segment conversion breakdown (quality products only) ────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, SALE_DATE
            FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, EMAIL_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(promo_date) AS first_promo_date FROM pu GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.first_promo_date AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.first_promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        ),
        pp_raw AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID,
                sd.REVENUE_CATEGORY, sd.PRODUCT_DESCRIPTION, sd.PAYMENTAMT_LOCAL,
                ROW_NUMBER() OVER (PARTITION BY pu.CLIENT_ID, pu.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.promo_date AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
               AND {_QUALITY_FILTER}
        ),
        pp AS (
            SELECT CLIENT_ID, STUDIO_ID,
                SUM(CASE WHEN REVENUE_CATEGORY = 'Memberships' AND mem_rn > 1 THEN 0 ELSE PAYMENTAMT_LOCAL END) AS rev
            FROM pp_raw
            GROUP BY CLIENT_ID, STUDIO_ID
        )
        SELECT
            CASE WHEN sg.earliest_signup >= %s THEN 'New'
                 WHEN hp.CLIENT_ID IS NOT NULL THEN 'Existing'
                 WHEN pl.CLIENT_ID IS NOT NULL THEN 'Platform'
                 ELSE 'Free Class'
            END AS segment,
            COUNT(DISTINCT pu.CLIENT_ID)       AS total_clients,
            COUNT(DISTINCT pp.CLIENT_ID)       AS converted,
            ROUND(COUNT(DISTINCT pp.CLIENT_ID) * 100.0 / NULLIF(COUNT(DISTINCT pu.CLIENT_ID), 0), 1) AS conv_pct,
            COALESCE(SUM(pp.rev), 0)           AS post_rev,
            COALESCE(SUM(pp.rev) / NULLIF(COUNT(DISTINCT pp.CLIENT_ID), 0), 0) AS avg_per_converter
        FROM pu
        LEFT JOIN signup sg         ON pu.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON pu.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON pu.CLIENT_ID = pl.CLIENT_ID
        LEFT JOIN pp                ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        GROUP BY 1
        ORDER BY total_clients DESC
    """, (p, frm, to, ppt, p, frm))
    segment_breakdown = [
        {"segment": r[0], "total_clients": int(r[1]), "converted": int(r[2]),
         "conversion_pct": float(r[3] or 0), "post_rev": round(float(r[4]), 2),
         "avg_per_converter": round(float(r[5]), 2)}
        for r in rows
    ]

    # ── 9b. Segment conversion breakdown × studio ──────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, STUDIO_ID, STUDIO_NAME, EMAIL_ID, SALE_DATE
            FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        ),
        pu AS (
            SELECT CLIENT_ID, STUDIO_ID, MIN(STUDIO_NAME) AS STUDIO_NAME, EMAIL_ID, MIN(SALE_DATE) AS promo_date
            FROM deduped GROUP BY CLIENT_ID, STUDIO_ID, EMAIL_ID
        ),
        promo_first AS (
            SELECT CLIENT_ID, MIN(promo_date) AS first_promo_date FROM pu GROUP BY CLIENT_ID
        ),
        signup AS (
            SELECT mc.CLIENT_ID, MIN(mc.SIGNEDUP_DATE) AS earliest_signup
            FROM promo_first pf JOIN {CLIENTS} mc ON mc.CLIENT_ID = pf.CLIENT_ID
            GROUP BY mc.CLIENT_ID
        ),
        had_prior_paid AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.first_promo_date AND s.PAYMENTAMT_LOCAL > 0
        ),
        had_platform AS (
            SELECT DISTINCT pf.CLIENT_ID FROM promo_first pf
            JOIN {SALES} s ON pf.CLIENT_ID = s.CLIENT_ID
            WHERE s.SALE_DATE < pf.first_promo_date
              AND (s.REVENUE_CATEGORY = 'ClassPass'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%classpass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%gympass%%'
                   OR LOWER(s.PRODUCT_DESCRIPTION) LIKE '%%jackedrabbit%%')
        ),
        pp_raw AS (
            SELECT pu.CLIENT_ID, pu.STUDIO_ID,
                sd.REVENUE_CATEGORY, sd.PRODUCT_DESCRIPTION, sd.PAYMENTAMT_LOCAL,
                ROW_NUMBER() OVER (PARTITION BY pu.CLIENT_ID, pu.STUDIO_ID ORDER BY sd.SALE_DATE) AS mem_rn
            FROM pu
            JOIN {SALES} sd
                ON pu.CLIENT_ID = sd.CLIENT_ID AND pu.STUDIO_ID = sd.STUDIO_ID
               AND sd.SALE_DATE > pu.promo_date AND sd.SALE_DATE <= %s
               AND {neg_filt} AND sd.PAYMENTAMT_LOCAL > 0
               AND {_QUALITY_FILTER}
        ),
        pp AS (
            SELECT CLIENT_ID, STUDIO_ID,
                SUM(CASE WHEN REVENUE_CATEGORY = 'Memberships' AND mem_rn > 1 THEN 0 ELSE PAYMENTAMT_LOCAL END) AS rev
            FROM pp_raw
            GROUP BY CLIENT_ID, STUDIO_ID
        )
        SELECT
            REPLACE(pu.STUDIO_NAME, 'SWEAT440 ', '') AS studio,
            CASE WHEN sg.earliest_signup >= %s THEN 'New'
                 WHEN hp.CLIENT_ID IS NOT NULL  THEN 'Existing'
                 WHEN pl.CLIENT_ID IS NOT NULL  THEN 'Platform'
                 ELSE 'Free Class'
            END AS segment,
            COUNT(DISTINCT pu.CLIENT_ID)       AS total_clients,
            COUNT(DISTINCT pp.CLIENT_ID)       AS converted,
            ROUND(COUNT(DISTINCT pp.CLIENT_ID) * 100.0 / NULLIF(COUNT(DISTINCT pu.CLIENT_ID), 0), 1) AS conv_pct,
            COALESCE(SUM(pp.rev), 0)           AS post_rev,
            COALESCE(SUM(pp.rev) / NULLIF(COUNT(DISTINCT pp.CLIENT_ID), 0), 0) AS avg_per_converter
        FROM pu
        LEFT JOIN signup sg         ON pu.CLIENT_ID = sg.CLIENT_ID
        LEFT JOIN had_prior_paid hp ON pu.CLIENT_ID = hp.CLIENT_ID
        LEFT JOIN had_platform pl   ON pu.CLIENT_ID = pl.CLIENT_ID
        LEFT JOIN pp                ON pu.CLIENT_ID = pp.CLIENT_ID AND pu.STUDIO_ID = pp.STUDIO_ID
        GROUP BY 1, 2
        ORDER BY studio, total_clients DESC
    """, (p, frm, to, ppt, p, frm))
    segment_by_studio = [
        {"studio": r[0], "segment": r[1], "total_clients": int(r[2]), "converted": int(r[3]),
         "conversion_pct": float(r[4] or 0), "post_rev": round(float(r[5]), 2),
         "avg_per_converter": round(float(r[6]), 2)}
        for r in rows
    ]

    # ── 10. Multi-location clients ─────────────────────────────────────────
    rows = qall(cur, f"""
        WITH deduped AS (
            SELECT CLIENT_ID, EMAIL_ID, FIRST_NAME, LAST_NAME, STUDIO_ID, STUDIO_NAME, SALE_DATE
            FROM {SALES}
            WHERE {filt} AND SALE_DATE BETWEEN %s AND %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID, STUDIO_ID ORDER BY SALE_ID) = 1
        )
        SELECT
            MIN(d.EMAIL_ID)                                                               AS email,
            TRIM(COALESCE(MIN(d.FIRST_NAME),'') || ' ' || COALESCE(MIN(d.LAST_NAME),'')) AS full_name,
            COUNT(DISTINCT d.STUDIO_ID)                                                   AS studio_count,
            COUNT(*)                                                                       AS purchases,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT REPLACE(d.STUDIO_NAME,'SWEAT440 ','')),', ') AS studios
        FROM deduped d
        GROUP BY d.CLIENT_ID
        HAVING COUNT(DISTINCT d.STUDIO_ID) > 1
        ORDER BY studio_count DESC, purchases DESC
    """, (p, frm, to))
    multi_location = [
        {"email": r[0] or '', "name": r[1].strip() if r[1] else '',
         "studio_count": int(r[2]), "purchases": int(r[3]), "studios": r[4] or ''}
        for r in rows
    ]

    return {
        "id":                camp["id"],
        "name":              camp["name"],
        "product":           p,
        "date_from":         frm,
        "date_to":           to,
        "post_promo_to":     ppt,
        "variant":           camp.get("variant"),
        "exclude_from":      ex_from,
        "exclude_to":        ex_to,
        "notes":             camp.get("notes", ""),
        "kpis":              kpis,
        "lead_sources":      lead_sources,
        "studios":           studios,
        "studio_conversions": studio_conversions,
        "channels":          channels,
        "funnel":            funnel,
        "funnel_by_studio":  funnel_by_studio,
        "post_promo_products": post_promo_products,
        "products_by_studio": products_by_studio,
        "time_series":       time_series,
        "time_series_by_studio": time_series_by_studio,
        "segment_breakdown": segment_breakdown,
        "segment_by_studio": segment_by_studio,
        "multi_location":    multi_location,
    }


# ── Connect and fetch ───────────────────────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, token=TOKEN,
    authenticator="programmatic_access_token",
    role=ROLE, warehouse=WAREHOUSE,
)
cur = conn.cursor()

def build_meta_post_promo(cur, camp):
    """Post-promo for the campaign's Meta audience, split into two groups that
    never overlap:
      - buyers: net purchasers of the campaign product whose source is Meta Ads
        (same rule as fetch_2weeks_data.py: LEADS_LIST source or REFERRED_BY);
      - leads:  Meta Ads sign-ups in the promo window (same definition as
        data.json) who did NOT buy the campaign product at that studio.
    Post-promo = net paid purchases of any other product at the same studio,
    after the promo purchase (buyers) / from the sign-up date (leads), through
    post_promo_to. Membership revenue follows the main query's rule (only the
    client's first post-promo purchase counts), so autopay renewals don't
    inflate it."""
    cfg = camp["meta_post_promo"]
    frm, to, ppt, prod = camp["date_from"], camp["date_to"], camp["post_promo_to"], camp["product"]

    def studios_ok(col):
        parts = []
        inc = camp.get("studio_filter") or []
        if inc:
            parts.append("(" + " OR ".join(f"{col} ILIKE '%%{s}%%'" for s in inc) + ")")
        parts += [f"{col} NOT ILIKE '%%{s}%%'" for s in cfg.get("exclude_studios") or []]
        return " AND ".join(parts) or "TRUE"

    cte = f"""
        WITH meta_leads AS (
            SELECT LOWER(TRIM(EMAIL_ID)) AS email, STUDIO_ID,
                   MIN(STUDIO_NAME) AS studio_name, MIN(SIGNUP_DATE) AS start_date
            FROM {LEADS_LIST}
            WHERE SOURCE = 'Meta Ads' AND EMAIL_ID IS NOT NULL
              AND SIGNUP_DATE BETWEEN %s AND %s AND {studios_ok('STUDIO_NAME')}
            GROUP BY 1, 2
        ),
        buys AS (
            SELECT LOWER(TRIM(EMAIL_ID)) AS email, CLIENT_ID, STUDIO_ID,
                   MIN(STUDIO_NAME) AS studio_name, MIN(SALE_DATE)::DATE AS buy_date
            FROM {SALES}
            WHERE PRODUCT_DESCRIPTION = %s AND IS_RETURN = 0 AND IS_RETURNED = 0
              AND SALE_DATE BETWEEN %s AND %s AND {studios_ok('STUDIO_NAME')}
            GROUP BY 1, 2, 3
        ),
        meta_buyers AS (
            SELECT DISTINCT b.email, b.STUDIO_ID, b.studio_name, b.buy_date AS start_date
            FROM buys b
            LEFT JOIN {LEADS_LIST} ll
                   ON LOWER(TRIM(ll.EMAIL_ID)) = b.email AND ll.STUDIO_ID = b.STUDIO_ID
            LEFT JOIN {CLIENTS} c
                   ON c.CLIENT_ID = b.CLIENT_ID AND c.STUDIO_ID = b.STUDIO_ID
            WHERE ll.SOURCE = 'Meta Ads' OR LOWER(TRIM(c.REFERRED_BY)) = 'meta ads'
        ),
        grp AS (
            SELECT 'buyers' AS grp, email, STUDIO_ID, studio_name, start_date FROM meta_buyers
            UNION ALL
            SELECT 'leads', m.email, m.STUDIO_ID, m.studio_name, m.start_date
            FROM meta_leads m
            WHERE NOT EXISTS (SELECT 1 FROM buys b WHERE b.email = m.email AND b.STUDIO_ID = m.STUDIO_ID)
        ),
        pp_raw AS (
            SELECT g.grp, g.email, g.STUDIO_ID, sd.PRODUCT_DESCRIPTION, sd.REVENUE_CATEGORY,
                   sd.PAYMENTAMT_LOCAL, IFF({_QUALITY_FILTER}, 1, 0) AS is_quality,
                   ROW_NUMBER() OVER (PARTITION BY g.grp, g.email, g.STUDIO_ID ORDER BY sd.SALE_DATE) AS rn
            FROM grp g
            JOIN {SALES} sd
              ON LOWER(TRIM(sd.EMAIL_ID)) = g.email AND sd.STUDIO_ID = g.STUDIO_ID
             AND (sd.SALE_DATE::DATE > g.start_date
                  OR (g.grp = 'leads' AND sd.SALE_DATE::DATE = g.start_date))
             AND sd.SALE_DATE <= %s AND sd.PRODUCT_DESCRIPTION != %s
             AND sd.PAYMENTAMT_LOCAL > 0 AND sd.IS_RETURN = 0 AND sd.IS_RETURNED = 0
        ),
        pp AS (
            SELECT *, CASE WHEN REVENUE_CATEGORY = 'Memberships' AND rn > 1 THEN 0
                           ELSE PAYMENTAMT_LOCAL END AS rev
            FROM pp_raw
        )"""
    params = (frm, to, prod, frm, to, ppt, prod)

    # Group sizes + conversions per studio (studio rows are disjoint, so the
    # dashboard can sum them for any studio filter).
    studio_rows = qall(cur, cte + """
        , sizes AS (
            SELECT g.STUDIO_ID, REPLACE(MIN(g.studio_name), 'SWEAT440 ', '') AS studio,
                   COUNT_IF(g.grp = 'buyers') AS buyers,
                   COUNT_IF(g.grp = 'leads')  AS lead_only
            FROM grp g GROUP BY g.STUDIO_ID
        ),
        lead_counts AS (
            SELECT STUDIO_ID, COUNT(*) AS leads FROM meta_leads GROUP BY 1
        ),
        conv AS (
            SELECT grp, STUDIO_ID,
                   COUNT(DISTINCT email) AS converted,
                   COUNT(DISTINCT IFF(is_quality = 1, email, NULL)) AS quality,
                   SUM(rev) AS revenue
            FROM pp GROUP BY 1, 2
        )
        SELECT s.studio, COALESCE(lc.leads, 0), s.buyers, s.lead_only,
               COALESCE(cb.converted, 0), COALESCE(cb.quality, 0), COALESCE(cb.revenue, 0),
               COALESCE(cl.converted, 0), COALESCE(cl.quality, 0), COALESCE(cl.revenue, 0)
        FROM sizes s
        LEFT JOIN lead_counts lc ON lc.STUDIO_ID = s.STUDIO_ID
        LEFT JOIN conv cb ON cb.grp = 'buyers' AND cb.STUDIO_ID = s.STUDIO_ID
        LEFT JOIN conv cl ON cl.grp = 'leads'  AND cl.STUDIO_ID = s.STUDIO_ID
        ORDER BY 2 DESC, 1
    """, params)
    product_rows = qall(cur, cte + """
        SELECT pp.grp, REPLACE(st.studio_name, 'SWEAT440 ', ''), pp.PRODUCT_DESCRIPTION, pp.REVENUE_CATEGORY,
               MAX(pp.is_quality), COUNT(DISTINCT pp.email), SUM(pp.rev)
        FROM pp
        JOIN (SELECT STUDIO_ID, MIN(studio_name) AS studio_name FROM grp GROUP BY 1) st
          ON st.STUDIO_ID = pp.STUDIO_ID
        GROUP BY 1, 2, 3, 4 ORDER BY 6 DESC, 7 DESC
    """, params)

    return {
        "lead_date_from":   frm,
        "lead_date_to":     to,
        "post_promo_to":    ppt,
        "excluded_studios": cfg.get("exclude_studios") or [],
        "studios": [{
            "studio": r[0], "leads": int(r[1]), "buyers": int(r[2]), "lead_only": int(r[3]),
            "buyers_converted": int(r[4]), "buyers_quality": int(r[5]), "buyers_revenue": round(float(r[6]), 2),
            "leads_converted":  int(r[7]), "leads_quality":  int(r[8]), "leads_revenue":  round(float(r[9]), 2),
        } for r in studio_rows],
        "products": [{
            "group": r[0], "studio": r[1], "product": r[2], "category": r[3],
            "quality": bool(r[4]), "clients": int(r[5]), "revenue": round(float(r[6] or 0), 2),
        } for r in product_rows],
    }


results = []
for camp in CAMPAIGNS:
    if camp.get("hidden"):
        print(f"  Skipping (hidden): {camp['name']}")
        continue
    print(f"  Fetching: {camp['name']} ({camp['product']})…")
    try:
        data = build_campaign(cur, camp)
        if camp.get("meta_post_promo"):
            data["meta_post_promo"] = build_meta_post_promo(cur, camp)
            m = data["meta_post_promo"]["studios"]
            print(f"    Meta post-promo: {sum(r['leads'] for r in m)} leads | "
                  f"{sum(r['buyers'] for r in m)} buyers | {sum(r['lead_only'] for r in m)} lead-only")
        results.append(data)
        k = data["kpis"]
        print(f"    OK  {k['total_purchases']} purchases | {k['unique_clients']} clients | "
              f"quality {k['quality_conv_rate']}% conv | ${k['post_promo_revenue']:,.2f} post-rev")
    except Exception as e:
        print(f"    ERROR: {e}")
        raise

conn.close()

out = {"generated_at": datetime.utcnow().isoformat() + "Z", "campaigns": results}
with open("campaign_data.json", "w") as f:
    json.dump(out, f, indent=2, default=serial)

kb = os.path.getsize("campaign_data.json") / 1024
print(f"\nDONE  campaign_data.json written ({kb:.1f} KB)")
