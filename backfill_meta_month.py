"""
backfill_meta_month.py — one-time recovery for a Meta Ads month lost to the
90-day ad_daily rolling window (see fetch_meta_ads.py).

Fetches real day-level ad insights for one month directly from the Meta API
and bakes the studio-level daily totals into meta-ads-baked.json's
studio_daily (permanent, never subject to the 90-day trim) — so the month
keeps full daily granularity instead of being flattened into a single
monthly total. A single month of daily rows is far lighter than the 365-day
daily backfill that failed previously.

Usage:
    python3 backfill_meta_month.py 2026-04

After running, regenerate spend-data.json:
    python3 build_spend_data.py

Requires META_TOKEN in the environment, same as fetch_meta_ads.py.
"""
from __future__ import annotations

import calendar
import json
import sys
from pathlib import Path

from meta_client import MetaClient
from fetch_meta_ads import match_studio, safe_float

REPO_ROOT  = Path(__file__).resolve().parent
BAKED_PATH = REPO_ROOT / "meta-ads-baked.json"


def run(month_str: str) -> None:
    import studios as studios_registry

    year, month = int(month_str[:4]), int(month_str[5:7])
    date_start = f"{year:04d}-{month:02d}-01"
    date_end   = f"{year:04d}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"

    ad_account  = studios_registry.defaults()["meta_ad_account_id"]
    studios_cfg = studios_registry.meta_studio_rows()

    meta = MetaClient()
    print(f"Fetching Meta ad insights for {date_start} -> {date_end} (daily) ...")
    rows = meta.get_insights(
        ad_account,
        level="ad",
        date_start=date_start,
        date_end=date_end,
        time_increment=1,
    )
    print(f"  {len(rows)} ad x day rows")

    spend_by_day: dict[tuple, float] = {}
    skipped = 0
    for row in rows:
        adset_name = row.get("adset_name", "")
        d = row.get("date_start")
        if not d:
            skipped += 1
            continue
        studio = match_studio(adset_name, studios_cfg)
        if not studio:
            skipped += 1
            continue
        k = (d, studio["code"])
        spend_by_day[k] = round(spend_by_day.get(k, 0.0) + safe_float(row.get("spend")), 2)

    print(f"  matched {len(rows) - skipped} rows, skipped {skipped} (no studio match)")
    total = round(sum(spend_by_day.values()), 2)
    print(f"  {len(spend_by_day)} (date, studio) rows, total spend ${total:,.2f}")

    if not spend_by_day:
        print("Nothing to bake — exiting without touching meta-ads-baked.json.")
        return

    existing = json.loads(BAKED_PATH.read_text(encoding="utf-8")) if BAKED_PATH.exists() else {}
    # Drop any prior rows for this month before re-adding (idempotent re-run)
    studio_daily = [r for r in existing.get("studio_daily", []) if r.get("date", "")[:7] != month_str]
    for (d, sc), spend in sorted(spend_by_day.items()):
        studio_daily.append({"date": d, "studio_code": sc, "spend": spend})
    studio_daily.sort(key=lambda r: (r["date"], r["studio_code"]))
    existing["studio_daily"] = studio_daily

    BAKED_PATH.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Baked {len(spend_by_day)} (date, studio) rows for {month_str} into {BAKED_PATH}")
    print("Now run: python3 build_spend_data.py")


if __name__ == "__main__":
    if len(sys.argv) != 2 or len(sys.argv[1]) != 7 or sys.argv[1][4] != "-":
        print("Usage: python3 backfill_meta_month.py YYYY-MM")
        sys.exit(1)
    run(sys.argv[1])
