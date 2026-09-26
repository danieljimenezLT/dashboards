"""
fetch_meta_corporate_spend_monthly.py
--------------------------------------------------------------
Corporate-account Meta Ads spend by studio, one row per (month, studio_code).
Powers admin-reports/meta-spend-corporate.html.

Replaces the old daily/ad-level pipeline that report used to read from
(spend-data.json, via fetch_meta_ads.py). That pipeline tracked spend at
(date, ad_id) granularity with a rolling re-fetch window — which is exactly
what let a misattributed ad go undetected for months once it aged past the
window (see the Reston/Prestonwood incident, PRs #272/#279). It also mixed
in dedicated-account spend, requiring a separate meta_spend_dedicated field
to net back out to a corporate-only number.

This fetches ONCE PER MONTH, at the ad-set level (not per-ad), for the whole
prior month in a single request:
  - No rolling window, no per-ad tracking, no attribution drift to chase —
    each month is fetched exactly once, right after it closes, when Meta's
    reporting has settled. There's no daily re-fetch to silently regress a
    value days or weeks later.
  - Corporate-account only, by construction: queries only the shared
    corporate ad account (studios_registry.defaults()["meta_ad_account_id"]).
    Nothing to net out — a dedicated-account studio's spend simply isn't in
    this account at all, so it's never fetched here.

Output: meta-corporate-spend-monthly.json
  {
    "generated_at": "...",
    "monthly": [{"month": "2026-04", "studio_code": "FL-001", "spend": 907.6}, ...]
  }
Each run replaces only the target month's rows; every other month passes
through untouched (permanent once written — there's no rolling window for
old months to fall out of).

Usage:
    python fetch_meta_corporate_spend_monthly.py                # last full calendar month
    python fetch_meta_corporate_spend_monthly.py --month 2026-04 # a specific month (backfill)
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from meta_client import MetaClient

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
import studios as studios_registry

OUT_PATH = REPO_ROOT / "meta-corporate-spend-monthly.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("meta-corporate-monthly")


def last_full_month(today: date) -> str:
    last_day_of_prior_month = today.replace(day=1) - timedelta(days=1)
    return last_day_of_prior_month.strftime("%Y-%m")


def month_bounds(month_str: str) -> tuple[str, str]:
    year, mo = int(month_str[:4]), int(month_str[5:7])
    n_days = calendar.monthrange(year, mo)[1]
    return f"{year:04d}-{mo:02d}-01", f"{year:04d}-{mo:02d}-{n_days:02d}"


def run(month: str | None = None):
    target_month = month or last_full_month(date.today())
    date_start, date_end = month_bounds(target_month)
    log.info(f"Fetching corporate-account adset spend for {target_month} ({date_start} to {date_end})")

    ad_account = studios_registry.defaults()["meta_ad_account_id"]
    # Full registry, dedicated-account studios included: we're only ever
    # querying the corporate account here, so there's nothing to double-fetch
    # or exclude — if an adset name in this account happens to embed a
    # dedicated-account studio's code, it should still resolve to that
    # studio like anywhere else match_studios() is used.
    studios_cfg = studios_registry.meta_studio_rows()

    meta = MetaClient()
    # level="adset", no time_increment -> one row per adset for the whole month.
    rows = meta.get_insights(ad_account, level="adset", date_start=date_start, date_end=date_end)
    log.info(f"  {len(rows)} adset rows")

    by_studio: dict[str, float] = {}
    matched, skipped = 0, 0
    for row in rows:
        spend = float(row.get("spend") or 0)
        if spend <= 0:
            continue
        studios = studios_registry.match_studios(row.get("adset_name", ""), studios_cfg)
        if not studios:
            skipped += 1
            continue
        matched += 1
        # Shared adsets (several codes in the name) split spend evenly
        for studio, share in zip(studios, studios_registry.split_evenly(spend, len(studios))):
            code = studio["code"]
            by_studio[code] = by_studio.get(code, 0) + share

    log.info(f"  {matched} adsets matched to a studio, {skipped} skipped (no studio match)")

    existing: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Replace this month's rows only; every other month passes through untouched.
    monthly = [r for r in existing.get("monthly", []) if r.get("month") != target_month]
    for code, spend in by_studio.items():
        monthly.append({"month": target_month, "studio_code": code, "spend": round(spend, 2)})
    monthly.sort(key=lambda r: (r["month"], r["studio_code"]))

    output = {"generated_at": datetime.now(timezone.utc).isoformat(), "monthly": monthly}
    OUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {OUT_PATH} — {len(monthly)} total (month, studio) rows across {len(set(r['month'] for r in monthly))} months")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default=None,
                        help="Specific month to (re)fetch, YYYY-MM. Default: last full calendar month.")
    args = parser.parse_args()
    try:
        run(month=args.month)
    except Exception as e:
        log.exception(f"❌ ETL failed: {e}")
        sys.exit(1)
