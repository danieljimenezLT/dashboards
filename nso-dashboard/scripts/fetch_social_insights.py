#!/usr/bin/env python3
"""
fetch_social_insights.py - Fetch Instagram organic insights for all SWEAT440 studios.
Instagram Business Account IDs are auto-discovered from Facebook Page IDs at runtime.

Outputs: social_insights.json  (instagram section only)

Usage:
    python scripts/fetch_social_insights.py
    python scripts/fetch_social_insights.py --days 30
    python scripts/fetch_social_insights.py --start 2026-04-01 --end 2026-05-24
"""

import argparse
import json
import os
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import studios as studios_registry

BASE = "https://graph.facebook.com/v21.0"

# All SWEAT440 studios — ig_id is discovered at runtime via the Meta API.
# code = short slug used as the thumbnails sub-directory name. Sourced from
# studios.json (canonical registry); "Corporate" is a house account, not a
# studio, so it's appended separately rather than living in the registry.
STUDIOS = studios_registry.social_insights_rows() + [
    {"name": "Corporate", "code": "corporate", "page_id": "2077978269155137", "ig_id": None},
]


def discover_ig_ids(studios, user_token):
    """
    Query Meta API to resolve Instagram Business Account IDs from Page IDs.
    Populates studio['ig_id'] in-place for any page that has a linked IG account.
    """
    print("\nDiscovering Instagram Business Account IDs from Pages...")
    found = 0
    for studio in studios:
        if studio.get("ig_id"):
            print(f"  ✓ {studio['name']}: {studio['ig_id']} (pre-configured)")
            found += 1
            continue
        if not studio.get("page_id"):
            print(f"  – {studio['name']}: no page_id configured, skipping")
            continue
        try:
            r = requests.get(f"{BASE}/{studio['page_id']}", params={
                "fields": "instagram_business_account",
                "access_token": user_token,
            })
            data = r.json()
            if "error" not in data and "instagram_business_account" in data:
                studio["ig_id"] = data["instagram_business_account"]["id"]
                print(f"  ✓ {studio['name']}: {studio['ig_id']}")
                found += 1
            else:
                print(f"  – {studio['name']}: no IG account linked")
            time.sleep(0.15)  # stay within rate limits
        except Exception as e:
            print(f"  ! {studio['name']}: {e}")
    print(f"  {found}/{len(studios)} studios have a linked Instagram account\n")


def _month_key(date_str):
    """Return 'YYYY-MM-01' for a given date string, or '' on error."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{d.year}-{d.month:02d}-01"
    except (ValueError, TypeError):
        return ""


def build_monthly_aggregates(daily_rows, posts, before_date_str):
    """
    Aggregate daily rows into monthly summaries for complete calendar months
    that fall entirely before before_date_str (the rolling window start).
    Only months where the last day < before_date_str are included, ensuring
    partial boundary months are never archived prematurely.
    Returns {month_key: aggregate_dict}.
    """
    import statistics
    from calendar import monthrange
    cutoff = datetime.strptime(before_date_str, "%Y-%m-%d")
    monthly = {}
    # reach by date — needed to estimate reach per post
    reach_by_date = {}

    for row in daily_rows:
        try:
            d = datetime.strptime(row["date"], "%Y-%m-%d")
        except (ValueError, KeyError):
            continue
        last_day = datetime(d.year, d.month, monthrange(d.year, d.month)[1])
        if last_day >= cutoff:
            continue  # month not yet fully outside the rolling window
        mk = f"{d.year}-{d.month:02d}-01"
        if mk not in monthly:
            monthly[mk] = {"month": mk, "reach": 0, "accounts_engaged": 0,
                           "total_interactions": 0, "days_with_data": 0, "post_count": 0,
                           "median_likes": None, "median_reach_per_post": None,
                           "_likes": [], "_reach_per_post": []}
        m = monthly[mk]
        m["reach"] += row.get("reach") or 0
        m["accounts_engaged"] += row.get("accounts_engaged") or 0
        m["total_interactions"] += row.get("total_interactions") or 0
        m["days_with_data"] += 1
        reach_by_date[row["date"]] = row.get("reach") or 0
        # Track last known follower_count in the month
        fc = row.get("follower_count")
        if fc is not None:
            if m.get("followers") is None or row["date"] > m.get("_last_fc_date", ""):
                m["followers"] = fc
                m["_last_fc_date"] = row["date"]

    # Count posts per date to estimate per-post reach
    posts_per_date = {}
    valid_posts = []
    for post in posts:
        try:
            d = datetime.strptime(post.get("date", ""), "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        last_day = datetime(d.year, d.month, monthrange(d.year, d.month)[1])
        if last_day >= cutoff:
            continue
        posts_per_date[post["date"]] = posts_per_date.get(post["date"], 0) + 1
        valid_posts.append(post)

    for post in valid_posts:
        mk = f"{post['date'][:7]}-01"
        if mk not in monthly:
            continue
        m = monthly[mk]
        m["post_count"] += 1
        likes = post.get("likes")
        if likes is not None:
            m["_likes"].append(int(likes))
        n = posts_per_date.get(post["date"], 1)
        reach = reach_by_date.get(post["date"], 0)
        if n > 0 and reach > 0:
            m["_reach_per_post"].append(reach // n)

    # Compute medians and remove temp fields
    for m in monthly.values():
        if m["_likes"]:
            m["median_likes"] = int(statistics.median(m["_likes"]))
        if m["_reach_per_post"]:
            m["median_reach_per_post"] = int(statistics.median(m["_reach_per_post"]))
        del m["_likes"]
        del m["_reach_per_post"]
        m.pop("_last_fc_date", None)

    return monthly


def date_chunks(start_date, end_date, chunk_days=28):
    """Split a date range into chunks of max chunk_days days."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)
    return chunks


def fetch_instagram_insights(ig_id, studio_name, start_date, end_date, user_token):
    print(f"  [{studio_name}] instagram {ig_id}...")
    daily_map = {}

    # ── 0. Current follower count (point-in-time) ──
    current_followers = None
    try:
        r = requests.get(f"{BASE}/{ig_id}", params={
            "fields": "id,username,followers_count,media_count",
            "access_token": user_token,
        })
        data = r.json()
        if "error" not in data:
            current_followers = data.get("followers_count")
            print(f"    Current followers: {current_followers}, media: {data.get('media_count')}")
        else:
            print(f"    WARNING IG account fields: {data['error'].get('message','')[:80]}")
    except Exception as e:
        print(f"    WARNING IG account fields: {e}")

    chunks = date_chunks(start_date, end_date, chunk_days=28)
    print(f"    Fetching in {len(chunks)} chunk(s)...")

    # follower_count daily only works for last 30 days
    today = datetime.now().date()
    fc_since = (today - timedelta(days=29)).strftime("%Y-%m-%d")
    fc_until = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    for chunk_start, chunk_end in chunks:
        # Reach (period=day, max 30 days)
        try:
            r = requests.get(f"{BASE}/{ig_id}/insights", params={
                "metric": "reach",
                "period": "day",
                "since": chunk_start,
                "until": chunk_end,
                "access_token": user_token,
            })
            data = r.json()
            if "error" in data:
                err_msg = data["error"].get("message", "")
                if "does not exist" in err_msg or "missing permissions" in err_msg:
                    print(f"    WARNING IG not accessible: {err_msg[:80]}")
                    break
                print(f"    WARNING IG reach: {err_msg[:80]}")
            else:
                for metric_obj in data.get("data", []):
                    for val in metric_obj.get("values", []):
                        day = val["end_time"][:10]
                        if start_date <= day <= end_date:
                            if day not in daily_map:
                                daily_map[day] = {"date": day}
                            daily_map[day]["reach"] = val["value"]
        except Exception as e:
            print(f"    WARNING IG reach chunk: {e}")


    # ── follower_count daily (last 30 days only) ──
    try:
        r = requests.get(f"{BASE}/{ig_id}/insights", params={
            "metric": "follower_count",
            "period": "day",
            "since": fc_since,
            "until": fc_until,
            "access_token": user_token,
        })
        data = r.json()
        if "error" in data:
            print(f"    WARNING IG follower_count: {data['error'].get('message','')[:80]}")
        else:
            fc_count = 0
            for metric_obj in data.get("data", []):
                for val in metric_obj.get("values", []):
                    day = val["end_time"][:10]
                    if day not in daily_map:
                        daily_map[day] = {"date": day}
                    daily_map[day]["follower_count"] = val["value"]
                    fc_count += 1
            print(f"    follower_count: {fc_count} daily values (last 30 days)")
    except Exception as e:
        print(f"    WARNING IG follower_count: {e}")

    print(f"    {len(daily_map)} daily rows total")

    # Media posts (no date restriction)
    posts = []
    try:
        r = requests.get(f"{BASE}/{ig_id}/media", params={
            "fields": "id,caption,timestamp,like_count,comments_count,media_type,permalink,thumbnail_url,media_url",
            "limit": 100,
            "access_token": user_token,
        })
        data = r.json()
        if "error" in data:
            print(f"    WARNING IG media: {data['error'].get('message', '')[:80]}")
        else:
            for media in data.get("data", []):
                ts = (media.get("timestamp") or "")[:10]
                if not ts or ts < start_date or ts > end_date:
                    continue
                image_url = media.get("thumbnail_url") or media.get("media_url") or ""
                likes = media.get("like_count") or 0
                comments = media.get("comments_count") or 0
                posts.append({
                    "date": ts,
                    "caption": (media.get("caption") or "")[:150],
                    "likes": likes,
                    "comments": comments,
                    "media_type": media.get("media_type") or "",
                    "permalink": media.get("permalink") or "",
                    "image_url": image_url,
                    "engagement": likes + comments,
                })
            posts.sort(key=lambda x: x["engagement"], reverse=True)
            print(f"    {len(posts)} posts in range")
    except Exception as e:
        print(f"    WARNING IG media: {e}")

    return {
        "current_followers": current_followers,
        "daily": sorted(daily_map.values(), key=lambda x: x["date"]),
        "posts": posts[:20],
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch Instagram insights for all SWEAT440 studios")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--output", default="social_insights.json")
    args = parser.parse_args()

    if args.start and args.end:
        start_date, end_date = args.start, args.end
    else:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    user_token = os.environ.get("META_ACCESS_TOKEN")
    if not user_token:
        print("ERROR: META_ACCESS_TOKEN not set in .env")
        sys.exit(1)

    print("=" * 60)
    print("SWEAT440 - Instagram Insights Fetch")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Studios: {len(STUDIOS)}")
    print("=" * 60)

    # Resolve Instagram Business Account IDs from Page IDs at runtime.
    discover_ig_ids(STUDIOS, user_token)

    output = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {"start": start_date, "end": end_date},
        "instagram": [],
    }

    def fetch_studio(studio):
        """Fetch Instagram data for one studio."""
        if not studio.get("ig_id"):
            return studio, None
        print(f"\n>> Instagram: {studio['name']}")
        try:
            ig_data = fetch_instagram_insights(
                studio["ig_id"], studio["name"], start_date, end_date, user_token
            )
            return studio, {"studio": studio["name"], "code": studio["code"],
                            "ig_id": studio["ig_id"], **ig_data}
        except Exception as e:
            print(f"  ERROR {studio['name']}: {e}")
            return studio, {"studio": studio["name"], "code": studio["code"],
                            "ig_id": studio["ig_id"],
                            "current_followers": None, "daily": [], "posts": [], "error": str(e)}

    # ── Load existing data for append-only merge ──────────────────────────
    out_path = Path(args.output)
    existing_studios = {}
    if out_path.exists():
        try:
            with open(out_path, encoding="utf-8") as f:
                existing = json.load(f)
            for s in existing.get("instagram", []):
                existing_studios[s["studio"]] = s
            print(f"\nLoaded existing data: {len(existing_studios)} studios")
        except Exception as e:
            print(f"\nWARNING: could not load existing {out_path}: {e}")

    # Fetch all studios in parallel — 5 workers balances speed vs Meta rate limits.
    ig_map = {}
    print(f"\nFetching {len(STUDIOS)} studios with 5 parallel workers...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_studio, s): s for s in STUDIOS}
        for future in as_completed(futures):
            studio, ig_result = future.result()
            if ig_result:
                ig_map[studio["name"]] = ig_result

    # ── Merge new fetch with existing historical data ──────────────────────
    for studio_name, new_data in ig_map.items():
        old = existing_studios.get(studio_name, {})

        # Daily rows: new data wins for overlapping dates; old data preserved for the rest
        old_daily = {r["date"]: r for r in old.get("daily", [])}
        new_daily = {r["date"]: r for r in new_data.get("daily", [])}
        old_daily.update(new_daily)
        all_daily = sorted(old_daily.values(), key=lambda x: x["date"])

        # Posts: dedup by permalink, new API data wins for engagement counts
        old_posts = {p["permalink"]: p for p in old.get("posts", []) if p.get("permalink")}
        new_posts = {p["permalink"]: p for p in new_data.get("posts", []) if p.get("permalink")}
        old_posts.update(new_posts)
        all_posts = sorted(old_posts.values(), key=lambda x: x.get("date", ""), reverse=True)

        # Monthly aggregates: compute from full accumulated data for complete months
        # entirely before the 90-day window. Existing stored months are preserved;
        # recomputed months (still in accumulated daily data) overwrite the stored value.
        new_monthly = build_monthly_aggregates(all_daily, all_posts, start_date)
        old_monthly = {m["month"]: m for m in old.get("monthly", [])}
        old_monthly.update(new_monthly)
        new_data["monthly"] = sorted(old_monthly.values(), key=lambda x: x["month"])

        # Prune daily rows and posts from months that are now fully archived.
        # Boundary months (partially in the 90-day window) are kept intact so
        # they can be correctly aggregated when they eventually fall out.
        archived_keys = set(new_monthly.keys())
        new_data["daily"] = [r for r in all_daily
                             if _month_key(r["date"]) not in archived_keys]
        new_data["posts"] = [p for p in all_posts
                             if _month_key(p.get("date", "")) not in archived_keys]

        ig_map[studio_name] = new_data

    # Build final list: STUDIOS order first, then any studios in existing file not in STUDIOS
    seen = set()
    final_ig = []
    for s in STUDIOS:
        if s["name"] in ig_map:
            final_ig.append(ig_map[s["name"]])
            seen.add(s["name"])
    for studio_name, data in existing_studios.items():
        if studio_name not in seen and studio_name not in ig_map:
            final_ig.append(data)
    output["instagram"] = final_ig

    # ── Download thumbnails and replace image_url with local paths ────────
    # Instagram/Facebook CDN URLs expire in ~24h. We download each post's
    # thumbnail as a static file so GitHub Pages serves them with no expiry.
    # Saved to: nso-dashboard/thumbnails/{code}/{post_slug}.jpg
    # JSON stores the repo-relative path so the dashboard can load them directly.
    thumbs_root = out_path.parent / "thumbnails"
    print(f"\n{'=' * 60}")
    print("Downloading thumbnails...")
    total_ok = total_fail = total_skip = 0

    for studio in output["instagram"]:
        code = studio.get("code", "unknown")
        studio_dir = thumbs_root / code
        studio_dir.mkdir(parents=True, exist_ok=True)
        for post in studio.get("posts", []):
            url = post.get("image_url", "")
            if not url:
                continue
            # Already saved from a previous run — skip re-download
            if url.startswith("nso-dashboard/thumbnails/"):
                total_skip += 1
                continue
            slug = ""
            permalink = post.get("permalink", "")
            if permalink:
                slug = permalink.rstrip("/").split("/")[-1]
            if not slug:
                slug = post.get("date", "unknown").replace("-", "") + "_" + str(total_ok + total_fail)
            local_path = studio_dir / f"{slug}.jpg"
            try:
                r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    f.write(r.content)
                post["image_url"] = f"nso-dashboard/thumbnails/{code}/{slug}.jpg"
                total_ok += 1
            except Exception as e:
                print(f"  WARNING thumbnail {slug}: {e}")
                post["image_url"] = ""
                total_fail += 1

    print(f"  Downloaded: {total_ok}  Failed/cleared: {total_fail}  Skipped (existing): {total_skip}")

    # Summary
    print("\n" + "=" * 60)
    ig_ok = [s["studio"] for s in output["instagram"] if s.get("daily") or s.get("posts")]
    print(f"  Instagram: {len(output['instagram'])} studios, posts for: {len(ig_ok)}")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1000
    print(f"\nDone. Written to {out_path} ({size_kb:.1f} KB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
