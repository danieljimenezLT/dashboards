"""
analyze_ads.py  —  Paid Ads Rule-Based Analysis
Reads daniel-paid-ads-data.json, applies performance rules, writes paid-ads-analysis.json.

Usage:
  python analyze_ads.py
"""
import json, pathlib, datetime
from collections import defaultdict

ROOT      = pathlib.Path(__file__).parent
DATA_FILE = ROOT / "daniel-paid-ads-data.json"
OUT_FILE  = ROOT / "paid-ads-analysis.json"

# ── CPL thresholds (absolute $) ───────────────────────────────────────────────
CPL_GOOD            = 50.0   # below $50  → good
CPL_WARN            = 80.0   # $50–$80    → regular / medium warning
#                              above $80   → bad / high alert
MIN_LEADS_CPL       = 2      # minimum leads before evaluating CPL

# ── Wasted spend ──────────────────────────────────────────────────────────────
MIN_SPEND_WASTED    = 80.0   # $ spent in window with 0 leads → high alert

# ── Low impressions in 7-day window ──────────────────────────────────────────
MIN_HIST_SPEND      = 30.0   # $ all-time spend to consider ad "should be running"
MIN_IMP_WINDOW      = 200    # impressions in window — below this = alert

# ── Scale opportunity (good CPL, few impressions) ─────────────────────────────
MIN_LEADS_SCALE     = 2      # minimum leads in window
SCALE_MAX_IMP       = 3_000  # impressions ceiling — still considered "limited reach"

# ── Low CTR ───────────────────────────────────────────────────────────────────
LOW_CTR_THRESHOLD   = 0.5    # %
MIN_IMP_FOR_CTR     = 2_000  # minimum impressions before evaluating CTR

# ── Paused ads ────────────────────────────────────────────────────────────────
MIN_SPEND_PAUSED    = 100.0  # $ all-time spend to flag a paused ad
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(v):
    try:   return float(v or 0)
    except: return 0.0

def safe_int(v):
    try:   return int(v or 0)
    except: return 0

def cpl(spend, leads):
    return round(spend / leads, 2) if leads else 0.0

def ctr(clicks, impressions):
    return round(clicks / impressions * 100, 3) if impressions else 0.0

# ── Analysis window: last 7 days (rolling) ───────────────────────────────────
_today        = datetime.date.today()
_window_start = _today - datetime.timedelta(days=7)
_window_end   = _today - datetime.timedelta(days=1)
WINDOW_START  = _window_start.isoformat()
WINDOW_END    = _window_end.isoformat()
WINDOW_LABEL  = f"{_window_start.strftime('%b %d')} – {_window_end.strftime('%b %d, %Y')}"
# ─────────────────────────────────────────────────────────────────────────────

# ── Load data ─────────────────────────────────────────────────────────────────
data        = json.loads(DATA_FILE.read_text(encoding="utf-8"))
campaigns   = data.get("campaigns", {})
all_das_raw = data.get("daily_ad_studio", [])

# Window rows only (for spend / CPL / CTR rules)
all_das = [r for r in all_das_raw if WINDOW_START <= r.get("date", "") <= WINDOW_END]

print(f"Loaded {len(campaigns)} campaigns, {len(all_das_raw)} total rows")
print(f"Analysis window: {WINDOW_LABEL}  ({len(all_das)} rows in range)")

# ad_id → window rows (last 7 days)
das_by_ad: dict[str, list] = defaultdict(list)
for row in all_das:
    das_by_ad[row["ad_id"]].append(row)

# ad_id → all historical rows (used for silent / paused checks)
das_by_ad_all: dict[str, list] = defaultdict(list)
for row in all_das_raw:
    das_by_ad_all[row["ad_id"]].append(row)

# ─────────────────────────────────────────────────────────────────────────────

def analyze_campaign(camp_key: str, camp: dict) -> dict:
    ads       = camp.get("ads", [])
    totals    = camp.get("totals", {})
    disp_name = camp.get("display_name", camp_key)

    ad_meta = {a["ad_id"]: a for a in ads}
    ad_ids  = set(ad_meta.keys())

    camp_dates  = sorted(r["date"] for r in all_das if r["ad_id"] in ad_ids and r.get("date"))
    most_recent = camp_dates[-1] if camp_dates else None

    # Aggregate 7-day window totals per ad
    ad_totals: dict[str, dict] = {}
    for ad_id in ad_ids:
        rows = das_by_ad.get(ad_id, [])
        sp = sum(safe_float(r.get("spend"))     for r in rows)
        im = sum(safe_int(r.get("impressions")) for r in rows)
        cl = sum(safe_int(r.get("clicks"))      for r in rows)
        le = sum(safe_int(r.get("leads"))       for r in rows)
        ad_totals[ad_id] = {
            "spend": round(sp, 2), "impressions": im,
            "clicks": cl, "leads": le,
            "ctr": ctr(cl, im), "cpl": cpl(sp, le),
        }

    findings = []

    for ad_id, meta in ad_meta.items():
        t    = ad_totals.get(ad_id, {})
        sp   = t.get("spend", 0)
        le   = t.get("leads", 0)
        im   = t.get("impressions", 0)
        c_tr = t.get("ctr", 0)
        c_pl = t.get("cpl", 0)
        name   = meta.get("name", ad_id)
        studio = meta.get("studio_code", "")
        status = meta.get("status", "")

        # All-time totals (for silent / paused checks)
        hist_rows  = das_by_ad_all.get(ad_id, [])
        hist_spend = sum(safe_float(r.get("spend"))   for r in hist_rows)
        hist_le    = sum(safe_int(r.get("leads"))     for r in hist_rows)
        hist_cpl   = cpl(hist_spend, hist_le)

        # Skip completely inactive ads (no history, no window activity)
        if hist_spend == 0 and im == 0:
            continue

        # ── Rule 1: Wasted spend ──────────────────────────────────────────────
        # $80+ in last 7 days with zero leads → high alert
        if sp >= MIN_SPEND_WASTED and le == 0:
            findings.append({
                "type": "wasted_spend", "severity": "high",
                "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                "spend": sp, "leads": le, "impressions": im,
                "headline": f"${sp:,.0f} spent — zero leads",
                "detail": f"This ad spent ${sp:,.2f} in the last 7 days without generating a single lead.",
                "recommendation": "Pause immediately and audit the landing page or creative.",
            })

        # ── Rule 2: Low CTR ───────────────────────────────────────────────────
        if im >= MIN_IMP_FOR_CTR and c_tr < LOW_CTR_THRESHOLD:
            findings.append({
                "type": "low_ctr", "severity": "medium",
                "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                "spend": sp, "ctr": c_tr, "impressions": im,
                "headline": f"Low CTR — {c_tr:.2f}% on {im:,} impressions",
                "detail": f"Ad is showing but people are not clicking (CTR {c_tr:.2f}% is below the {LOW_CTR_THRESHOLD}% benchmark).",
                "recommendation": "Test a new hook, headline, or visual — the creative is not resonating.",
            })

        # ── Rule 3: CPL evaluation (absolute thresholds) ─────────────────────
        # Requires at least MIN_LEADS_CPL leads to evaluate
        if le >= MIN_LEADS_CPL and c_pl > 0:
            if c_pl > CPL_WARN:
                # Bad CPL — above $80
                findings.append({
                    "type": "cpl_bad", "severity": "high",
                    "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                    "spend": sp, "leads": le, "cpl": c_pl,
                    "headline": f"Bad CPL — ${c_pl:,.0f} (limit is ${CPL_WARN:.0f})",
                    "detail": f"CPL of ${c_pl:.2f} exceeds the ${CPL_WARN:.0f} limit. Target: below ${CPL_GOOD:.0f}.",
                    "recommendation": "Pause or significantly reduce budget — cost per lead is unsustainable.",
                })
            elif c_pl > CPL_GOOD:
                # Regular CPL — between $50 and $80
                findings.append({
                    "type": "cpl_regular", "severity": "medium",
                    "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                    "spend": sp, "leads": le, "cpl": c_pl,
                    "headline": f"Regular CPL — ${c_pl:,.0f} (target: below ${CPL_GOOD:.0f})",
                    "detail": f"CPL of ${c_pl:.2f} is in the acceptable-but-improvable range (${CPL_GOOD:.0f}–${CPL_WARN:.0f}).",
                    "recommendation": f"Monitor and test creative variations to bring CPL below ${CPL_GOOD:.0f}.",
                })

        # ── Rule 4: Good CPL with limited impressions → scale opportunity ─────
        if le >= MIN_LEADS_SCALE and c_pl > 0 and c_pl < CPL_GOOD and im < SCALE_MAX_IMP:
            findings.append({
                "type": "scale_opportunity", "severity": "opportunity",
                "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                "spend": sp, "leads": le, "cpl": c_pl, "impressions": im,
                "headline": f"Good CPL ${c_pl:,.0f} but only {im:,} impressions — scale this",
                "detail": f"CPL of ${c_pl:.2f} is below the ${CPL_GOOD:.0f} target but the ad is getting limited exposure ({im:,} impressions).",
                "recommendation": "Increase daily budget or widen audience targeting to reach more people.",
            })

        # ── Rule 5: Low impressions in the 7-day window ───────────────────────
        # Had historical spend but barely showed in the last 7 days
        if hist_spend >= MIN_HIST_SPEND and im < MIN_IMP_WINDOW:
            findings.append({
                "type": "low_impressions", "severity": "medium",
                "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                "spend": sp, "impressions": im, "leads": le,
                "headline": f"Only {im:,} impressions in the last 7 days",
                "detail": f"Ad has ${hist_spend:,.0f} in historical spend but barely showed this week ({im:,} impressions).",
                "recommendation": "Check budget exhaustion, bid strategy, audience size, or ad approval status.",
            })

        # ── Rule 6: Paused ad with significant historical spend ───────────────
        if status == "PAUSED" and hist_spend >= MIN_SPEND_PAUSED:
            findings.append({
                "type": "paused_high_spend", "severity": "info",
                "ad_id": ad_id, "ad_name": name, "studio_code": studio,
                "spend": hist_spend, "leads": hist_le, "cpl": hist_cpl,
                "headline": f"Paused — ${hist_spend:,.0f} historical spend",
                "detail": f"Ad is currently paused with ${hist_spend:,.2f} in total historical spend ({hist_le} leads, CPL ${hist_cpl:.2f}).",
                "recommendation": "Review performance before reactivating — know why it was paused.",
            })

    sev_order = {"high": 0, "medium": 1, "opportunity": 2, "info": 3}
    findings.sort(key=lambda f: (sev_order.get(f["severity"], 9), -f.get("spend", 0)))

    high_count          = sum(1 for f in findings if f["severity"] == "high")
    medium_count        = sum(1 for f in findings if f["severity"] == "medium")
    opportunities_count = sum(1 for f in findings if f["severity"] == "opportunity")

    return {
        "display_name":        disp_name,
        "period_label":        camp.get("period_label", ""),
        "totals":              totals,
        "most_recent_date":    most_recent,
        "high_count":          high_count,
        "medium_count":        medium_count,
        "alerts_count":        high_count + medium_count,
        "opportunities_count": opportunities_count,
        "findings_count":      len(findings),
        "findings":            findings,
    }


# ── Run analysis ──────────────────────────────────────────────────────────────
results = {}
for key, camp in campaigns.items():
    print(f"  Analyzing {key} ...")
    results[key] = analyze_campaign(key, camp)
    f = results[key]
    print(f"    -> {f['alerts_count']} alerts, {f['opportunities_count']} opportunities, {f['findings_count']} total findings")

# ── Write output ──────────────────────────────────────────────────────────────
output = {
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "window_start": WINDOW_START,
    "window_end":   WINDOW_END,
    "window_label": WINDOW_LABEL,
    "campaigns":    results,
}
OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"\nWrote {OUT_FILE}  ({OUT_FILE.stat().st_size:,} bytes)")
