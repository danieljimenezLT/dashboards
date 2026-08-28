"""
build_spend_data.py — generate spend-data.json

Combines Meta Ads and Google Ads studio-level spend into a single file:

  spend-data.json
  ├── daily   — one row per (date, studio_code) for ALL of 2026
  │             Jan–Mar 2026: monthly totals distributed evenly across days,
  │             unless a baked real-daily source covers that month (see below)
  │             Apr 2026+:    actual daily API data (live ad_daily/daily, or
  │                           a platform's *-ads-baked.json studio_daily for
  │                           a range manually backfilled via
  │                           backfill_meta_month.py / backfill_google_range.py)
  │             a month with neither live nor baked daily data falls back to
  │             its studio_monthly total, distributed evenly across days
  └── monthly — one row per (month, studio_code) for everything before 2026

`daily` is a SELF-ACCUMULATING LEDGER, not a stateless recompute: each run
loads its own prior output as a base and only overwrites the (date,
studio_code) cells its current sources actually supply a value for. A cell
that ages out of every live/baked source this run — because a platform's
source file failed to load, or a date simply isn't covered by anything
currently available — is left exactly as it was, never reset to zero. This
is what makes "keep daily spend until new order" hold even as the underlying
90-day rolling windows (ad_daily, google daily) roll forward. `monthly`
(pre-2026) has no such requirement — its own inputs (meta-ads-baked.json's
studio_monthly, google-ads-data.json's monthly) are already permanent and
append-only, so it's recomputed fresh from scratch every run.

Run after fetch_meta_ads.py and fetch_google_ads.py have written their files.
"""

import json
import calendar
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT        = Path(__file__).resolve().parent
META_BAKED_PATH   = REPO_ROOT / "meta-ads-baked.json"
META_LIVE_PATH    = REPO_ROOT / "meta-ads-data.json"
GOOGLE_PATH       = REPO_ROOT / "google-ads-data.json"
GOOGLE_BAKED_PATH = REPO_ROOT / "google-ads-baked.json"
STUDIOS_PATH      = REPO_ROOT / "studios.json"
OUT_PATH          = REPO_ROOT / "spend-data.json"

DAILY_START_YEAR = 2026   # all of this year is represented as daily rows


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  Warning: could not read {path.name}: {e}")
        return {}


def days_in_month(year, month):
    return calendar.monthrange(year, month)[1]


def distribute_monthly_to_days(month_str, spend, studio_code):
    """Yield one daily row per day in the month with spend / n_days."""
    year, mo = int(month_str[:4]), int(month_str[5:7])
    n = days_in_month(year, mo)
    daily = round(spend / n, 4) if n else 0
    for day in range(1, n + 1):
        yield {
            "date":        f"{year:04d}-{mo:02d}-{day:02d}",
            "studio_code": studio_code,
            "daily":       daily,
        }


def run():
    print("Loading source files...")
    studios_raw  = json.loads(STUDIOS_PATH.read_text(encoding="utf-8"))
    meta_baked   = load_json(META_BAKED_PATH)
    meta_live    = load_json(META_LIVE_PATH)
    google_raw   = load_json(GOOGLE_PATH)
    google_baked = load_json(GOOGLE_BAKED_PATH)  # optional — may not exist yet

    # ── Build nametocode lookup from studios.json ─────────────────────
    studios_list = studios_raw.get("studios", studios_raw) if isinstance(studios_raw, dict) else studios_raw
    name_to_code = {}
    for s in studios_list:
        if s.get("name") and s.get("code"):
            name_to_code[s["name"]] = s["code"]

    # ── 1. Collect Meta monthly rows (baked Jan 2025–Mar 2026) ───────
    # Key: (month[:7], studio_code) to spend
    meta_monthly: dict[tuple, float] = {}
    for r in meta_baked.get("studio_monthly", []):
        k = (r["month"][:7], r["studio_code"])
        meta_monthly[k] = meta_monthly.get(k, 0) + (r.get("spend") or 0)

    # ── 2. Collect Meta daily rows (Apr 2026+) ───────────────────────
    meta_daily: dict[tuple, float] = {}
    for r in meta_live.get("ad_daily", []):
        d = r.get("date", "")
        if d[:4] == str(DAILY_START_YEAR) and d >= f"{DAILY_START_YEAR}-04-01":
            k = (d, r["studio_code"])
            meta_daily[k] = meta_daily.get(k, 0) + (r.get("spend") or 0)

    # Baked real daily rows — e.g. a one-time backfill_meta_month.py run, or a
    # user-supplied import for a range that aged out of ad_daily's 90-day
    # window (or predates it, e.g. Jan–Mar). Preserves true daily granularity
    # instead of falling back to a flattened monthly total for that range.
    for r in meta_baked.get("studio_daily", []):
        d = r.get("date", "")
        if d[:4] == str(DAILY_START_YEAR):
            k = (d, r["studio_code"])
            meta_daily[k] = meta_daily.get(k, 0) + (r.get("spend") or 0)

    # ── 3. Collect Google monthly rows (pre-2026) ────────────────────
    google_monthly: dict[tuple, float] = {}
    for r in google_raw.get("monthly", []):
        mo = r.get("month", "")[:7]
        if mo < str(DAILY_START_YEAR):
            code = name_to_code.get(r.get("studio", ""))
            if not code:
                continue
            k = (mo, code)
            google_monthly[k] = google_monthly.get(k, 0) + (r.get("spend") or 0)

    # ── 4. Collect Google daily rows (Apr 2026+) ─────────────────────
    google_daily: dict[tuple, float] = {}
    for r in google_raw.get("daily", []):
        d = r.get("date", "")
        if d[:4] == str(DAILY_START_YEAR) and d >= f"{DAILY_START_YEAR}-04-01":
            code = name_to_code.get(r.get("studio", ""))
            if not code:
                continue
            k = (d, code)
            google_daily[k] = google_daily.get(k, 0) + (r.get("spend") or 0)

    # Baked real daily rows for Google — e.g. a one-time
    # backfill_google_range.py run for a gap or a Jan–Mar real-daily upgrade.
    for r in google_baked.get("studio_daily", []):
        d = r.get("date", "")
        if d[:4] == str(DAILY_START_YEAR):
            code = name_to_code.get(r.get("studio", ""))
            if not code:
                continue
            k = (d, code)
            google_daily[k] = google_daily.get(k, 0) + (r.get("spend") or 0)

    # ── 5. Self-accumulating daily ledger ────────────────────────────
    # Seed from spend-data.json's OWN prior output, not from scratch. Only
    # cells this run's fresh sources (above) actually touch get overwritten;
    # everything else — including a platform whose source file failed to
    # load this run, which naturally contributes nothing above — is carried
    # forward untouched. This is what "keep until new order" means in practice.
    existing_out = load_json(OUT_PATH)
    daily_out: dict[tuple, dict] = {
        (r["date"], r["studio_code"]): {
            "date":         r["date"],
            "studio_code":  r["studio_code"],
            "meta_spend":   r.get("meta_spend", 0.0),
            "google_spend": r.get("google_spend", 0.0),
        }
        for r in existing_out.get("daily", [])
    }

    def row(date_str, code):
        k = (date_str, code)
        if k not in daily_out:
            daily_out[k] = {"date": date_str, "studio_code": code, "meta_spend": 0.0, "google_spend": 0.0}
        return daily_out[k]

    # Jan–Mar 2026 from Meta monthly, distributed across days (only for
    # months not already covered by real daily data above)
    for (mo, code), spend in meta_monthly.items():
        if mo[:4] == str(DAILY_START_YEAR):
            for r in distribute_monthly_to_days(mo, spend, code):
                row(r["date"], code)["meta_spend"] = round(r["daily"], 4)

    # 2026 months from Google monthly not already covered by google_daily —
    # normally just Jan-Mar (pre-daily-window), but also picks up any later
    # 2026 month whose daily rows have aged out of google-ads-data.json's
    # 90-day window and been folded into its "monthly" bucket instead.
    google_daily_months = {d[:7] for (d, _code) in google_daily}
    for r in google_raw.get("monthly", []):
        mo = r.get("month", "")[:7]
        if mo[:4] == str(DAILY_START_YEAR) and mo not in google_daily_months:
            code = name_to_code.get(r.get("studio", ""))
            if not code:
                continue
            for dr in distribute_monthly_to_days(mo, r.get("spend") or 0, code):
                row(dr["date"], code)["google_spend"] = round(dr["daily"], 4)

    # Apr 2026+ Meta daily (live ad_daily + baked real-daily, combined above)
    for (d, code), spend in meta_daily.items():
        row(d, code)["meta_spend"] = round(spend, 4)

    # Apr 2026+ Google daily (live daily + baked real-daily, combined above)
    for (d, code), spend in google_daily.items():
        row(d, code)["google_spend"] = round(spend, 4)

    # ── 6. Build monthly output rows (pre-2026) — stateless, recomputed ──
    # fresh every run since its own inputs are already permanent/append-only.
    all_keys = set(meta_monthly.keys()) | set(google_monthly.keys())
    monthly_out = []
    for (mo, code) in sorted(all_keys):
        if mo[:4] >= str(DAILY_START_YEAR):
            continue
        monthly_out.append({
            "month":        mo,
            "studio_code":  code,
            "meta_spend":   round(meta_monthly.get((mo, code), 0), 2),
            "google_spend": round(google_monthly.get((mo, code), 0), 2),
        })

    daily_list = sorted(daily_out.values(), key=lambda r: (r["date"], r["studio_code"]))

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "daily":        daily_list,
        "monthly":      monthly_out,
    }

    OUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        f"OK Wrote {OUT_PATH.name} — "
        f"{len(daily_list)} daily rows ({len(existing_out.get('daily', []))} carried in from prior run), "
        f"{len(monthly_out)} monthly rows"
    )
    daily_months = sorted(set(r["date"][:7] for r in daily_list))
    if daily_months:
        print(f"   Daily range: {daily_months[0]} to {daily_months[-1]}")
    monthly_months = sorted(set(r["month"] for r in monthly_out))
    if monthly_months:
        print(f"   Monthly range: {monthly_months[0]} to {monthly_months[-1]}")


if __name__ == "__main__":
    run()
