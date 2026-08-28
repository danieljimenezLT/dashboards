"""
backfill_google_range.py — one-time recovery for a Google Ads spend gap.

fetch_google_ads.py's "daily" output is a quarter-relative window (current Q
+ previous Q, trimmed to the last 90 days) — a self-imposed restriction, not
a Google Ads API limitation. This script bypasses that window entirely by
calling _fetch_daily_rows() directly for an arbitrary date range, so it can
recover:
  - the 2026-04-01 to 2026-05-05 gap left by the 90-day rolling trim, or
  - real daily granularity for 2026-01-01 to 2026-03-31, if the API still
    has it (verify by checking this script's own row-count output).

Output: google-ads-baked.json, a new static file (never auto-overwritten by
fetch_google_ads.py) with studio_daily: [{date, studio, spend}, ...] —
spend-only, matching backfill_meta_month.py's scope. Google's engagement
metrics beyond 90 days already have a permanent home in google-ads-data.json's
monthly fold (see fetch_google_ads.py) — this script doesn't touch those.

Usage:
    python3 backfill_google_range.py 2026-04-01 2026-05-05

After running, regenerate spend-data.json:
    python3 build_spend_data.py

Requires the same env vars as fetch_google_ads.py (GOOGLE_ADS_DEVELOPER_TOKEN,
GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN,
GOOGLE_ADS_LOGIN_CUSTOMER_ID).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

from fetch_google_ads import build_client, _list_accessible_customers, _fetch_daily_rows, log

REPO_ROOT  = Path(__file__).resolve().parent
BAKED_PATH = REPO_ROOT / "google-ads-baked.json"


def run(date_from_str: str, date_to_str: str) -> None:
    date_from = date.fromisoformat(date_from_str)
    date_to   = date.fromisoformat(date_to_str)

    client = build_client()
    mcc_id = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"].replace("-", "")

    customer_ids = _list_accessible_customers(client, mcc_id)
    if not customer_ids:
        log.warning("No accessible customer accounts found under MCC. Exiting.")
        return

    log.info(f"Fetching Google Ads performance for {date_from_str} -> {date_to_str} ...")
    spend_by_day: dict[tuple, float] = {}
    for cid in customer_ids:
        log.info(f"  account {cid} ...")
        perf, _leads, _dirs = _fetch_daily_rows(client, cid, date_from, date_to)
        for (studio, date_str), v in perf.items():
            k = (date_str, studio)
            spend_by_day[k] = round(spend_by_day.get(k, 0.0) + v.get("spend", 0.0), 2)

    total = round(sum(spend_by_day.values()), 2)
    log.info(f"  {len(spend_by_day)} (date, studio) rows, total spend ${total:,.2f}")

    if not spend_by_day:
        log.info("Nothing to bake — exiting without touching google-ads-baked.json.")
        return

    existing = json.loads(BAKED_PATH.read_text(encoding="utf-8")) if BAKED_PATH.exists() else {}
    # Drop any prior rows inside this range before re-adding (idempotent re-run)
    studio_daily = [
        r for r in existing.get("studio_daily", [])
        if not (date_from_str <= r.get("date", "") <= date_to_str)
    ]
    for (d, studio), spend in sorted(spend_by_day.items()):
        studio_daily.append({"date": d, "studio": studio, "spend": spend})
    studio_daily.sort(key=lambda r: (r["date"], r["studio"]))
    existing["studio_daily"] = studio_daily

    BAKED_PATH.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Baked {len(spend_by_day)} (date, studio) rows for {date_from_str}..{date_to_str} into {BAKED_PATH}")
    log.info("Now run: python3 build_spend_data.py")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 backfill_google_range.py YYYY-MM-DD YYYY-MM-DD")
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])
