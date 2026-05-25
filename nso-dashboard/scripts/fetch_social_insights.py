#!/usr/bin/env python3
"""
fetch_social_insights.py - Fetch Facebook Page + Instagram organic insights
for all SWEAT440 studios.

Outputs: social_insights.json

Usage:
    python scripts/fetch_social_insights.py --days 90
    python scripts/fetch_social_insights.py --start 2026-01-27 --end 2026-04-26
"""

import argparse
import json
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

BASE = "https://graph.facebook.com/v21.0"

# All SWEAT440 studios — ig_id is discovered at runtime via the Meta API.
# code = short slug used as the thumbnails sub-directory name.
STUDIOS = [
    {"name": "Coral Gables",          "code": "gables",        "page_id": "110268611810389",    "ig_id": None},
    {"name": "Dallas - Prestonwood",  "code": "prestonwood",   "page_id": "845182982009071",    "ig_id": None},
    {"name": "Toms River",            "code": "tomsriver",     "page_id": "108238962107956",    "ig_id": None},
    {"name": "South Beach",           "code": "sobe",          "page_id": "105547208952141",    "ig_id": None},
    {"name": "North Miami",           "code": "northmiami",    "page_id": "1145229115329264",   "ig_id": None},
    {"name": "Austin - Highland",     "code": "highland",      "page_id": "351629338025804",    "ig_id": None},
    {"name": "Miami Lakes",           "code": "miamilakes",    "page_id": "101752329601099",    "ig_id": None},
    {"name": "Aventura",              "code": "aventura",      "page_id": "1047698415096383",   "ig_id": None},
    {"name": "Eastchester",           "code": "eastchester",   "page_id": "664055870131827",    "ig_id": None},
    {"name": "Boca Raton",            "code": "boca",          "page_id": "637367179456983",    "ig_id": None},
    {"name": "Midtown Miami",         "code": "midtown",       "page_id": "103476569380316",    "ig_id": None},
    {"name": "Orlando - Dr. Phillips","code": "drphillips",    "page_id": "986634234541338",    "ig_id": None},
    {"name": "Dallas - Uptown",       "code": "uptown",        "page_id": "1013504171849728",   "ig_id": None},
    {"name": "Herriman",              "code": "herriman",      "page_id": "1016504601542354",   "ig_id": None},
    {"name": "Pinecrest",             "code": "pinecrest",     "page_id": "848877064975048",    "ig_id": None},
    {"name": "Austin - Zilker",       "code": "zilker",        "page_id": "119087484495208",    "ig_id": None},
    {"name": "Coral Springs",         "code": "coralsprings",  "page_id": "106485985557697",    "ig_id": None},
    {"name": "South Miami",           "code": "southmiami",    "page_id": "116631111441913",    "ig_id": None},
    {"name": "Deerfield Beach",       "code": "deerfield",     "page_id": "106597632212836",    "ig_id": None},
    {"name": "Doral",                 "code": "doral",         "page_id": "103789732469470",    "ig_id": None},
    {"name": "Brooklyn - Park Slope", "code": "parkslope",     "page_id": "681591261709097",    "ig_id": None},
    {"name": "West Palm Beach",       "code": "westpalm",      "page_id": "703238786198886",    "ig_id": None},
    {"name": "Coconut Grove",         "code": "coconutgrove",  "page_id": "196520916880971",    "ig_id": None},
    {"name": "Charlotte - NoDa",      "code": "noda",          "page_id": "104198619263881",    "ig_id": None},
    {"name": "Corporate",             "code": "corporate",     "page_id": "2077978269155137",   "ig_id": None},
    {"name": "Ocean Township",        "code": "oceantownship", "page_id": "184923338027883",    "ig_id": None},
    {"name": "Upper East Side",       "code": "uppereastside", "page_id": "108861828669519",    "ig_id": None},
    {"name": "Brickell",              "code": "brickell",      "page_id": "107873258720021",    "ig_id": None},
    {"name": "Reston",                "code": "reston",        "page_id": "875200972337017",    "ig_id": None},
    {"name": "Wall NJ",               "code": "wallnj",        "page_id": "700746796454324",    "ig_id": None},
    {"name": "Chelsea",               "code": "chelsea",       "page_id": "105456357683242",    "ig_id": None},
    {"name": "Las Olas",              "code": "lasolas",       "page_id": "300173986520471",    "ig_id": None},
    {"name": "Miramar",               "code": "miramar",       "page_id": "203760659484865",    "ig_id": None},
    {"name": "Pembroke Pines",        "code": "pembrokepines", "page_id": "328512683684059",    "ig_id": None},
    {"name": "FiDi",                  "code": "fidi",          "page_id": "149250091597748",    "ig_id": None},
    {"name": "Madison",               "code": "madison",       "page_id": "111726744769276",    "ig_id": None},
]


def discover_ig_ids(studios, user_token):
    """
    Query Meta API to resolve Instagram Business Account IDs from Page IDs.
    Populates studio['ig_id'] in-place for any page that has a linked IG account.
    """
    print("\nDiscovering Instagram Business Account IDs from Pages...")
    found = 0
    for studio in studios:
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


def get_page_token(page_id, user_token):
    """Exchange user access token for a page access token."""
    r = requests.get(f"{BASE}/{page_id}", params={
        "fields": "access_token,name",
        "access_token": user_token,
    })
    data = r.json()
    if "error" in data:
        print(f"    WARNING get_page_token: {data['error'].get('message')}")
        return user_token  # fall back to user token
    page_name = data.get("name", "")
    page_token = data.get("access_token", user_token)
    if page_token != user_token:
        print(f"    Page token obtained for: {page_name}")
    else:
        print(f"    Using user token for: {page_name} (page token not returned)")
    return page_token


def fetch_page_insights(page_id, studio_name, start_date, end_date, user_token):
    print(f"  [{studio_name}] page {page_id}...")

    page_token = get_page_token(page_id, user_token)
    daily_map = {}

    # ── 1. Follower / fan count via page fields ──
    total_fans = None
    talking_about = None
    try:
        r = requests.get(f"{BASE}/{page_id}", params={
            "fields": "followers_count,fan_count,talking_about_count,name",
            "access_token": page_token,
        })
        data = r.json()
        if "error" not in data:
            total_fans = data.get("followers_count") or data.get("fan_count")
            talking_about = data.get("talking_about_count")
            print(f"    Followers: {total_fans}, Talking about: {talking_about}")
        else:
            print(f"    WARNING page fields: {data['error'].get('message')}")
    except Exception as e:
        print(f"    WARNING page fields: {e}")

    # ── 2. Page-level daily insights (deprecated for New Pages Experience, try anyway) ──
    METRIC_CANDIDATES = [
        "page_impressions",
        "page_impressions_unique",
        "page_engaged_users",
        "page_views_total",
        "page_fans_add",
    ]
    for metric in METRIC_CANDIDATES:
        try:
            r = requests.get(f"{BASE}/{page_id}/insights", params={
                "metric": metric,
                "period": "day",
                "since": start_date,
                "until": end_date,
                "access_token": page_token,
            })
            data = r.json()
            if "error" not in data:
                for metric_obj in data.get("data", []):
                    for val in metric_obj.get("values", []):
                        day = val["end_time"][:10]
                        if start_date <= day <= end_date:
                            if day not in daily_map:
                                daily_map[day] = {"date": day}
                            daily_map[day][metric] = val["value"]
        except Exception:
            pass

    # ── 3. Posts with reactions, comments, shares ──
    posts = []
    try:
        params = {
            "fields": "id,message,created_time,full_picture,permalink_url,reactions.summary(true),comments.summary(true),shares",
            "limit": 100,
            "access_token": page_token,
        }
        r = requests.get(f"{BASE}/{page_id}/posts", params=params)
        data = r.json()
        if "error" in data:
            params["fields"] = "id,message,created_time,full_picture,permalink_url,shares"
            r = requests.get(f"{BASE}/{page_id}/posts", params=params)
            data = r.json()

        if "error" in data:
            print(f"    WARNING page posts: {data['error'].get('message')}")
        else:
            for post in data.get("data", []):
                ts = (post.get("created_time") or "")[:10]
                if not ts or ts < start_date or ts > end_date:
                    continue
                likes = 0
                comments = 0
                shares = 0
                try:
                    likes = post["reactions"]["summary"]["total_count"]
                except Exception:
                    pass
                try:
                    comments = post["comments"]["summary"]["total_count"]
                except Exception:
                    pass
                try:
                    shares = post["shares"]["count"]
                except Exception:
                    pass
                posts.append({
                    "_id": post.get("id", ""),
                    "date": ts,
                    "message": (post.get("message") or "")[:150],
                    "image_url": post.get("full_picture") or "",
                    "permalink": post.get("permalink_url") or "",
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "engagement": likes + comments + shares,
                })
            posts.sort(key=lambda x: x["engagement"], reverse=True)
            print(f"    {len(posts)} posts in range")
    except Exception as e:
        print(f"    WARNING page posts: {e}")

    # ── 4. If no page-level daily data, build daily aggregates from per-post insights ──
    if not daily_map and posts:
        # Aggregate engagement from all posts by date into daily_map
        for p in posts:
            day = p["date"]
            if day not in daily_map:
                daily_map[day] = {"date": day, "likes": 0, "comments": 0, "shares": 0, "engagement": 0}
            daily_map[day]["likes"]      += p["likes"]
            daily_map[day]["comments"]   += p["comments"]
            daily_map[day]["shares"]     += p["shares"]
            daily_map[day]["engagement"] += p["engagement"]

        # Fetch per-post reach/impressions for the 20 most recent posts
        recent_posts = sorted(posts, key=lambda x: x["date"], reverse=True)[:20]
        print(f"    Page insights deprecated — fetching per-post reach for {len(recent_posts)} recent posts...")
        for p in recent_posts:
            pid = p.get("_id")
            if not pid:
                continue
            try:
                r = requests.get(f"{BASE}/{pid}/insights", params={
                    "metric": "post_impressions,post_impressions_unique,post_engaged_users",
                    "access_token": page_token,
                })
                data = r.json()
                if "error" not in data:
                    day = p["date"]
                    for metric_obj in data.get("data", []):
                        mname = metric_obj.get("name", "")
                        vals = metric_obj.get("values", [])
                        if vals:
                            v = vals[0].get("value", 0)
                            daily_map[day][mname] = daily_map[day].get(mname, 0) + (v or 0)
                time.sleep(0.3)
            except Exception:
                pass

        if daily_map:
            print(f"    Built {len(daily_map)} daily rows from post data")
        else:
            print(f"    No daily data available")
    else:
        # Merge post engagement into existing daily_map where dates match
        for p in posts:
            day = p["date"]
            if day not in daily_map:
                daily_map[day] = {"date": day}
            daily_map[day]["likes"]      = daily_map[day].get("likes", 0)      + p["likes"]
            daily_map[day]["comments"]   = daily_map[day].get("comments", 0)   + p["comments"]
            daily_map[day]["shares"]     = daily_map[day].get("shares", 0)     + p["shares"]
            daily_map[day]["engagement"] = daily_map[day].get("engagement", 0) + p["engagement"]

        if daily_map:
            print(f"    {len(daily_map)} daily rows")

    # Remove internal post IDs before returning
    for p in posts:
        p.pop("_id", None)

    return {
        "total_fans": total_fans,
        "talking_about": talking_about,
        "daily": sorted(daily_map.values(), key=lambda x: x["date"]),
        "posts": posts[:20],
    }


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

        # Interactions (metric_type=total_value)
        for metric_name in ("accounts_engaged", "total_interactions"):
            try:
                r = requests.get(f"{BASE}/{ig_id}/insights", params={
                    "metric": metric_name,
                    "period": "day",
                    "metric_type": "total_value",
                    "since": chunk_start,
                    "until": chunk_end,
                    "access_token": user_token,
                })
                data = r.json()
                if "error" not in data:
                    for metric_obj in data.get("data", []):
                        for val in metric_obj.get("values", []):
                            day = val["end_time"][:10]
                            if start_date <= day <= end_date:
                                if day not in daily_map:
                                    daily_map[day] = {"date": day}
                                daily_map[day][metric_name] = val["value"]
            except Exception as e:
                print(f"    WARNING IG {metric_name} chunk: {e}")

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
    parser = argparse.ArgumentParser(description="Fetch Facebook Page + Instagram insights for all NSO studios")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--source", choices=["facebook", "instagram", "all"], default="all")
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
    print("SWEAT440 - Social Insights Fetch")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Source: {args.source}")
    print(f"Studios: {len(STUDIOS)}")
    print("=" * 60)

    # Resolve Instagram Business Account IDs from Page IDs at runtime.
    # This avoids hardcoding IDs that can become stale.
    if args.source in ("instagram", "all"):
        discover_ig_ids(STUDIOS, user_token)

    output = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {"start": start_date, "end": end_date},
        "facebook": [],
        "instagram": [],
    }

    for studio in STUDIOS:
        # Facebook Page
        if args.source in ("facebook", "all") and studio["page_id"]:
            print(f"\n>> Facebook: {studio['name']}")
            try:
                fb_data = fetch_page_insights(
                    studio["page_id"], studio["name"], start_date, end_date, user_token
                )
                output["facebook"].append({
                    "studio": studio["name"],
                    "code": studio["code"],
                    "page_id": studio["page_id"],
                    **fb_data,
                })
            except Exception as e:
                print(f"  ERROR: {e}")
                output["facebook"].append({
                    "studio": studio["name"],
                    "code": studio["code"],
                    "page_id": studio["page_id"],
                    "total_fans": None, "daily": [], "posts": [],
                    "error": str(e),
                })

        # Instagram
        if args.source in ("instagram", "all") and studio["ig_id"]:
            print(f"\n>> Instagram: {studio['name']}")
            try:
                ig_data = fetch_instagram_insights(
                    studio["ig_id"], studio["name"], start_date, end_date, user_token
                )
                output["instagram"].append({
                    "studio": studio["name"],
                    "code": studio["code"],
                    "ig_id": studio["ig_id"],
                    **ig_data,
                })
            except Exception as e:
                print(f"  ERROR: {e}")
                output["instagram"].append({
                    "studio": studio["name"],
                    "code": studio["code"],
                    "ig_id": studio["ig_id"],
                    "daily": [], "posts": [],
                    "error": str(e),
                })

    # ── Download thumbnails and replace image_url with local paths ────────
    # Instagram/Facebook CDN URLs expire in ~24h. We download each post's
    # thumbnail as a static file so GitHub Pages serves them with no expiry.
    # Saved to: nso-dashboard/thumbnails/{code}/{post_slug}.jpg
    # JSON stores the repo-relative path so the dashboard can load them directly.
    out_path = Path(args.output)
    thumbs_root = out_path.parent / "thumbnails"
    print(f"\n{'=' * 60}")
    print("Downloading thumbnails...")
    total_ok = total_fail = 0

    for section in ("facebook", "instagram"):
        for studio in output[section]:
            code = studio.get("code", "unknown")
            studio_dir = thumbs_root / code
            studio_dir.mkdir(parents=True, exist_ok=True)
            for post in studio.get("posts", []):
                url = post.get("image_url", "")
                if not url:
                    continue
                # Derive a stable filename from the permalink slug or date+index
                slug = ""
                permalink = post.get("permalink", "")
                if permalink:
                    slug = permalink.rstrip("/").split("/")[-1]
                if not slug:
                    slug = post.get("date", "unknown").replace("-", "") + "_" + str(total_ok + total_fail)
                local_path = studio_dir / f"{slug}.jpg"
                # Always re-download to keep images fresh
                try:
                    r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                    r.raise_for_status()
                    with open(local_path, "wb") as f:
                        f.write(r.content)
                    # Store repo-relative path for the dashboard
                    post["image_url"] = f"nso-dashboard/thumbnails/{code}/{slug}.jpg"
                    total_ok += 1
                except Exception as e:
                    print(f"  WARNING thumbnail {slug}: {e}")
                    post["image_url"] = ""  # clear expired URL; dashboard shows placeholder
                    total_fail += 1

    print(f"  Downloaded: {total_ok}  Failed/cleared: {total_fail}")

    # Summary
    print("\n" + "=" * 60)
    fb_ok = [s["studio"] for s in output["facebook"] if s.get("daily") or s.get("total_fans")]
    ig_ok = [s["studio"] for s in output["instagram"] if s.get("daily") or s.get("posts")]
    print(f"  Facebook: {len(output['facebook'])} studios, data for: {', '.join(fb_ok) or 'none'}")
    print(f"  Instagram: {len(output['instagram'])} studios, posts for: {', '.join(ig_ok) or 'none'}")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1000
    print(f"\nDone. Written to {out_path} ({size_kb:.1f} KB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
