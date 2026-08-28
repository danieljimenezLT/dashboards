#!/usr/bin/env python3
"""
backfill_ig_monthly.py — Backfill historical monthly Instagram data from Looker Studio exports.

Export these tables from Looker Studio before running:
  - reach.csv           : Date (Year) | Date (Month) | Page ID | Reach Period
  - new_posts.csv       : Date (Year) | Date (Month) | Page ID | New Posts
  - median_like_count.csv : Date (Year) | Date (Month) | Page ID | Like Count   (optional)
  - median_reach.csv    : Date (Year) | Date (Month) | Page ID | Reach Period  (optional)

Usage:
    python scripts/backfill_ig_monthly.py reach.csv new_posts.csv
    python scripts/backfill_ig_monthly.py reach.csv new_posts.csv \\
        --median-likes median_like_count.csv --median-reach median_reach.csv
    python scripts/backfill_ig_monthly.py reach.csv new_posts.csv --dry-run
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
NSO_DIR = SCRIPT_DIR.parent
REPO_ROOT = NSO_DIR.parent
STUDIOS_JSON = REPO_ROOT / "studios.json"


def load_page_id_map() -> dict:
    """Build {page_id_str: studio_name} from studios.json."""
    with open(STUDIOS_JSON, encoding="utf-8") as f:
        data = json.load(f)
    page_map = {}
    for s in data.get("studios", []):
        pid = (s.get("meta") or {}).get("page_id")
        if pid:
            page_map[str(pid)] = s["name"]
    page_map["2077978269155137"] = "Corporate"
    return page_map


def read_csv_data(path: str, value_col: str) -> dict:
    """Read a Looker Studio CSV. Returns {(page_id, year, month): value}."""
    data = {}
    p = Path(path)
    if not p.exists():
        print(f"ERROR: File not found: {p}", file=sys.stderr)
        sys.exit(1)
    with open(p, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                year = int(row["Date (Year)"])
                month = int(row["Date (Month)"])
                page_id = str(row["Page ID"]).strip()
                value = int(float(row[value_col] or 0))
                key = (page_id, year, month)
                data[key] = data.get(key, 0) + value
            except (ValueError, KeyError):
                continue
    return data


def rolling_window_start() -> str:
    """YYYY-MM-01 of the month at the 90-day window boundary."""
    start = datetime.now() - timedelta(days=90)
    return f"{start.year}-{start.month:02d}-01"


def main():
    parser = argparse.ArgumentParser(description="Backfill historical Instagram monthly data.")
    parser.add_argument("reach_csv", help="reach.csv: Date (Year) | Date (Month) | Page ID | Reach Period")
    parser.add_argument("posts_csv", help="new_posts.csv: Date (Year) | Date (Month) | Page ID | New Posts")
    parser.add_argument("--median-likes", metavar="CSV",
                        help="median_like_count.csv: Date (Year) | Date (Month) | Page ID | Like Count")
    parser.add_argument("--median-reach", metavar="CSV",
                        help="median_reach.csv: Date (Year) | Date (Month) | Page ID | Reach Period")
    parser.add_argument("--followers", metavar="CSV",
                        help="followers.csv: Date (Year) | Date (Month) | Page ID | Followers")
    parser.add_argument("--output", default=str(NSO_DIR / "social_insights.json"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    out_path = Path(args.output)
    if not out_path.exists():
        print(f"ERROR: Output file not found: {out_path}", file=sys.stderr)
        sys.exit(1)

    page_map = load_page_id_map()
    print(f"Page ID map: {len(page_map)} studios")

    reach_data  = read_csv_data(args.reach_csv, "Reach Period")
    posts_data  = read_csv_data(args.posts_csv, "New Posts")
    likes_data  = read_csv_data(args.median_likes, "Like Count") if args.median_likes else {}
    mreach_data = read_csv_data(args.median_reach, "Reach Period") if args.median_reach else {}
    foll_data   = read_csv_data(args.followers, "Followers") if args.followers else {}
    print(f"Reach:         {len(reach_data)} rows")
    print(f"Posts:         {len(posts_data)} rows")
    if likes_data:  print(f"Median likes:  {len(likes_data)} rows")
    if mreach_data: print(f"Median reach:  {len(mreach_data)} rows")
    if foll_data:   print(f"Followers:     {len(foll_data)} rows")

    # Build per-page monthly entries
    all_keys = set(reach_data) | set(posts_data) | set(likes_data) | set(mreach_data) | set(foll_data)
    by_page: dict[str, dict] = {}
    unmapped: set = set()

    for (page_id, year, month) in all_keys:
        reach  = reach_data.get((page_id, year, month), 0)
        posts  = posts_data.get((page_id, year, month), 0)
        mlikes = likes_data.get((page_id, year, month), None)
        mreach = mreach_data.get((page_id, year, month), None)
        foll   = foll_data.get((page_id, year, month), None)
        if reach == 0 and posts == 0 and mlikes is None and mreach is None and foll is None:
            continue
        if page_id not in page_map:
            unmapped.add(page_id)
            continue
        month_key = f"{year}-{month:02d}-01"
        if page_id not in by_page:
            by_page[page_id] = {}
        by_page[page_id][month_key] = {
            "month": month_key,
            "reach": reach,
            "accounts_engaged": 0,
            "total_interactions": 0,
            "days_with_data": 0,
            "post_count": posts,
            "median_likes": mlikes,
            "median_reach_per_post": mreach,
            "followers": foll,
        }

    if unmapped:
        print(f"\nWARNING: {len(unmapped)} unmapped page ID(s) in CSV:")
        for pid in sorted(unmapped):
            print(f"  {pid} — add to meta.page_id in studios.json")

    window_month_key = rolling_window_start()
    print(f"\nRolling window boundary: {window_month_key} — skipping this month and later")

    studio_to_page = {name.lower(): pid for pid, name in page_map.items()}

    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)

    added_total = updated_medians = skipped_window = updated_studios = 0

    for studio in data.get("instagram", []):
        studio_name = studio.get("studio", "")
        page_id = studio_to_page.get(studio_name.lower())
        if not page_id or page_id not in by_page:
            continue

        csv_monthly = by_page[page_id]
        existing_monthly = {m["month"]: m for m in studio.get("monthly", [])}
        added = updated = 0

        for month_key, entry in csv_monthly.items():
            if month_key >= window_month_key:
                skipped_window += 1
                continue

            if month_key in existing_monthly:
                changed = False
                for field in ("median_likes", "median_reach_per_post"):
                    if entry.get(field) is not None and existing_monthly[month_key].get(field) is None:
                        existing_monthly[month_key][field] = entry[field]
                        changed = True
                # Always overwrite followers — previous backfills used SUM aggregation (wrong).
                if entry.get("followers") is not None:
                    existing_monthly[month_key]["followers"] = entry["followers"]
                    changed = True
                if changed:
                    updated += 1
            else:
                existing_monthly[month_key] = entry
                added += 1

        if added or updated:
            studio["monthly"] = sorted(existing_monthly.values(), key=lambda x: x["month"])
            updated_studios += 1
            added_total += added
            updated_medians += updated
            parts = []
            if added:   parts.append(f"+{added} new months")
            if updated: parts.append(f"{updated} medians updated")
            print(f"  {studio_name}: {', '.join(parts)}")

    print(f"\nSummary:")
    print(f"  Studios touched:    {updated_studios}")
    print(f"  Months added:       {added_total}")
    print(f"  Medians updated:    {updated_medians}")
    print(f"  Skipped (window):   {skipped_window}")

    if args.dry_run:
        print("\n[DRY RUN] No file written.")
        return

    data["generated_at"] = datetime.now().isoformat()
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nWritten to {out_path}")


if __name__ == "__main__":
    main()
