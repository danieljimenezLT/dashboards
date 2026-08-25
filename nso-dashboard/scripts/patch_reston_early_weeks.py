"""
patch_reston_early_weeks.py

For Reston (VA-001), selectively overlays static data from reston_static_weeks.json
into the scorecard produced by build_all_scorecards.py:

  - new_leads, grassroots_leads: filled from static for weeks where data.json has no
    coverage (date_end < DATA_START). data.json for Reston starts 2026-01-03 (W7).
  - ig_new_followers: overlaid from static for ALL weeks, then W0 calibrated so
    the cumulative equals current_followers from social_insights.json.
  - meta_spend, google_spend, presales, etc.: kept from live API data (already accurate).
  - Weeks W31+ (date_start > 2026-06-13): kept as-is from build_all_scorecards.py.

This avoids date-boundary conflicts between the static file's Mon–Sun weeks and the
live scorecard's Sun–Sat weeks.
"""
import json
from datetime import date
from pathlib import Path

ROOT           = Path(__file__).parent.parent
SCORECARD_FILE = ROOT / "nso_scorecard_data.json"
STATIC_FILE    = ROOT / "reston_static_weeks.json"
RESTON_CODE    = "VA-001"
DATA_START     = "2026-01-03"   # first date data.json has Reston rows


def _wk_num(label):
    """Extract integer week number from 'WEEK 5' or 'Week 0'."""
    try:
        return int(label.strip().split()[-1])
    except (ValueError, IndexError):
        return -1


# ── Load static weeks ──────────────────────────────────────────────────────────
print("Loading reston_static_weeks.json ...")
with open(STATIC_FILE) as f:
    static_weeks = json.load(f)

# Build lookup: static week number → {new_leads, grassroots_leads, ig_new_followers}
static_by_num = {}
for sw in static_weeks:
    n = _wk_num(sw.get("week", ""))
    if n >= 0:
        static_by_num[n] = sw
print(f"  {len(static_weeks)} static weeks loaded (W0–W{max(static_by_num)})")

# ── Load current scorecard ─────────────────────────────────────────────────────
with open(SCORECARD_FILE) as f:
    sc = json.load(f)

reston = next((s for s in sc["studios"] if s["code"] == RESTON_CODE), None)
if not reston:
    print("ERROR: Reston not found in scorecard")
    raise SystemExit(1)

# ── Overlay static data into live weeks ───────────────────────────────────────
today_str = date.today().isoformat()
patched = 0

for wk in reston["weeks"]:
    n = _wk_num(wk.get("week", ""))
    sw = static_by_num.get(n)
    if sw is None:
        continue

    date_end = wk.get("date_end", "")
    date_start = wk.get("date_start") or ""

    # 1. Leads: fill from static only for weeks before data.json coverage
    if date_end < DATA_START or (not date_start and date_end < DATA_START):
        if not wk.get("new_leads"):
            wk["new_leads"] = sw.get("new_leads") or 0
        if not wk.get("grassroots_leads"):
            wk["grassroots_leads"] = sw.get("grassroots_leads") or 0

    # 2. IG followers: always overlay from static (API only covers 30 days)
    static_ig = sw.get("ig_new_followers")
    if static_ig is not None and not wk.get("ig_new_followers"):
        wk["ig_new_followers"] = static_ig
        patched += 1

print(f"\nOverlaid static data: {patched} IG updates")

# ── Recalculate cumulative total_leads ────────────────────────────────────────
print("Recalculating cumulative total_leads...")
cum = 0.0
for wk in reston["weeks"]:
    nl = wk.get("new_leads") or 0
    cum += nl
    wk["total_leads"] = cum if cum > 0 else (0.0 if wk.get("week") == "Week 0" else None)
print(f"  Final cumulative leads: {cum}")

# ── Calibrate IG Week 0 so cumulative = current_followers ─────────────────────
print("\nCalibrating IG Week 0 so cumulative = current_followers...")
try:
    with open(ROOT / "social_insights.json") as f:
        social = json.load(f)
    cf = next(
        (ig.get("current_followers") for ig in social.get("instagram", [])
         if ig.get("code") == "reston"),
        None,
    )
    if cf is not None:
        cf = int(cf)
        w0 = next((w for w in reston["weeks"] if w.get("week") == "Week 0"), None)
        sum_rest = sum(
            (w.get("ig_new_followers") or 0)
            for w in reston["weeks"] if w.get("week") != "Week 0"
        )
        w0_val = max(0, cf - sum_rest)
        if w0 is not None:
            old_ig = w0.get("ig_new_followers")
            w0["ig_new_followers"] = w0_val if w0_val > 0 else None
            print(f"  Week 0 ig: {old_ig} -> {w0['ig_new_followers']}  "
                  f"(current={cf}, rest={sum_rest})")
    else:
        print("  reston not found in social_insights.json, skipping")
except FileNotFoundError:
    print("  social_insights.json not found, skipping IG calibration")

# ── Write back ────────────────────────────────────────────────────────────────
with open(SCORECARD_FILE, "w") as f:
    json.dump(sc, f, indent=2)

print("\nDone. Reston early weeks overlaid from static file.")
