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
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

# Google Drive upload (optional — only active when service_account.json is present)
DRIVE_ROOT_FOLDER  = "1XZPoMSawhnnF8YIJHkeGywe5VeNNCROd"
DRIVE_BASE         = "https://www.googleapis.com/drive/v3"
DRIVE_UPLOAD       = "https://www.googleapis.com/upload/drive/v3"

class _DriveClient:
    def __init__(self, sa_file: Path):
        from google.auth.transport.requests import Request as _GReq
        from google.oauth2 import service_account as _SA
        self._creds = _SA.Credentials.from_service_account_file(
            str(sa_file), scopes=["https://www.googleapis.com/auth/drive"]
        )
        self._GReq = _GReq

    def _token(self):
        if not self._creds.valid:
            self._creds.refresh(self._GReq())
        return self._creds.token

    def _h(self):
        return {"Authorization": f"Bearer {self._token()}"}

    def get_or_create_folder(self, name: str, parent_id: str) -> str:
        q = (f"name='{name}' and mimeType='application/vnd.google-apps.folder'"
             f" and '{parent_id}' in parents and trashed=false")
        r = requests.get(f"{DRIVE_BASE}/files",
            params={"q": q, "fields": "files(id)", "pageSize": 1,
                    "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"},
            headers=self._h(), timeout=30)
        r.raise_for_status()
        files = r.json().get("files", [])
        if files:
            return files[0]["id"]
        r = requests.post(f"{DRIVE_BASE}/files",
            params={"supportsAllDrives": "true"},
            headers={**self._h(), "Content-Type": "application/json"},
            json={"name": name, "mimeType": "application/vnd.google-apps.folder",
                  "parents": [parent_id]},
            timeout=30)
        r.raise_for_status()
        return r.json()["id"]

    def upload(self, data: bytes, filename: str, parent_id: str) -> str:
        meta_bytes = json.dumps({"name": filename, "parents": [parent_id]}).encode()
        boundary = b"xbnd"
        body = (b"--" + boundary + b"\r\nContent-Type: application/json\r\n\r\n"
                + meta_bytes
                + b"\r\n--" + boundary + b"\r\nContent-Type: image/jpeg\r\n\r\n"
                + data + b"\r\n--" + boundary + b"--")
        for attempt in range(4):
            r = requests.post(f"{DRIVE_UPLOAD}/files",
                params={"uploadType": "multipart", "fields": "id", "supportsAllDrives": "true"},
                headers={**self._h(), "Content-Type": "multipart/related; boundary=xbnd"},
                data=body, timeout=300)
            if r.status_code in (429, 503) or (r.status_code == 403 and "userRateLimit" in r.text):
                time.sleep(2 ** attempt * 5)
                continue
            r.raise_for_status()
            break
        fid = r.json()["id"]
        requests.post(f"{DRIVE_BASE}/files/{fid}/permissions",
            headers={**self._h(), "Content-Type": "application/json"},
            json={"type": "anyone", "role": "reader"}, timeout=30)
        return fid

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


def build_monthly_aggregates(daily_rows, posts, before_date_str, current_followers=None):
    """
    Aggregate daily rows into monthly summaries for complete calendar months
    that fall entirely before before_date_str (the rolling window start).
    Only months where the last day < before_date_str are included, ensuring
    partial boundary months are never archived prematurely.
    Returns {month_key: aggregate_dict}.

    `current_followers` (optional) is the absolute follower total as of "now".
    It's used to reconstruct each month's absolute follower count, because
    Meta's `follower_count` daily insight is a net-new-followers DELTA for
    that day, not an absolute total — see the reconstruction below.
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
                           "stories_count": 0,
                           "median_likes": None, "median_reach_per_post": None,
                           "median_engagement_rate": None, "followers": None,
                           "_likes": [], "_reach_per_post": [], "_engagement_rates": []}
        m = monthly[mk]
        m["reach"] += row.get("reach") or 0
        m["accounts_engaged"] += row.get("accounts_engaged") or 0
        m["total_interactions"] += row.get("total_interactions") or 0
        m["days_with_data"] += 1
        # `stories` is a same-day snapshot of currently-active Stories (see
        # fetch_instagram_insights below) — summing daily snapshots approximates
        # monthly Stories volume since each Story is live for ~24h, close to the
        # ~24h gap between daily fetch runs.
        m["stories_count"] += row.get("stories") or 0
        reach_by_date[row["date"]] = row.get("reach") or 0

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
            post_reach_estimate = reach // n
            m["_reach_per_post"].append(post_reach_estimate)
            # Engagement rate = (likes + comments) / reach, using the same
            # per-post reach estimate as median_reach_per_post above. Shares
            # aren't included — Meta's shares metric needs one extra API call
            # per post, which isn't worth the added load on this daily job for
            # every studio's every post; likes+comments is the available proxy.
            if post_reach_estimate > 0:
                comments = post.get("comments") or 0
                engagement = (likes or 0) + comments
                m["_engagement_rates"].append(engagement / post_reach_estimate)

    # Compute medians and remove temp fields
    for m in monthly.values():
        if m["_likes"]:
            m["median_likes"] = int(statistics.median(m["_likes"]))
        if m["_reach_per_post"]:
            m["median_reach_per_post"] = int(statistics.median(m["_reach_per_post"]))
        if m["_engagement_rates"]:
            m["median_engagement_rate"] = round(statistics.median(m["_engagement_rates"]), 4)
        del m["_likes"]
        del m["_reach_per_post"]
        del m["_engagement_rates"]

    # Reconstruct each month's absolute follower count. Meta's `follower_count`
    # daily insight is a net-new-followers DELTA for that day, not an absolute
    # total, so the absolute count as of a given date = current_followers minus
    # the sum of every delta that occurred after that date. Mirrors the
    # client-side reconstruction already used in nso-studios.html.
    if current_followers is not None:
        fc_rows = sorted(
            (r for r in daily_rows if r.get("follower_count") is not None),
            key=lambda r: r["date"],
        )
        for mk, m in monthly.items():
            d = datetime.strptime(mk, "%Y-%m-%d")
            last_day_str = datetime(d.year, d.month, monthrange(d.year, d.month)[1]).strftime("%Y-%m-%d")
            deltas_after = sum(r["follower_count"] for r in fc_rows if r["date"] > last_day_str)
            m["followers"] = current_followers - deltas_after

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

    # ── Currently-active Stories (snapshot, ~24h lifetime) ──
    # There is no historical Stories endpoint — Stories expire after 24h — so
    # this daily job records a same-day count of whatever is active right now.
    # build_monthly_aggregates sums these daily snapshots into stories_count,
    # which approximates monthly volume since the fetch cadence (~daily) is
    # close to a Story's ~24h lifetime.
    try:
        r = requests.get(f"{BASE}/{ig_id}/stories", params={
            "fields": "id,timestamp",
            "access_token": user_token,
        })
        data = r.json()
        if "error" in data:
            print(f"    WARNING IG stories: {data['error'].get('message','')[:80]}")
        else:
            today_str = datetime.now().strftime("%Y-%m-%d")
            story_count = len(data.get("data", []))
            if today_str not in daily_map:
                daily_map[today_str] = {"date": today_str}
            daily_map[today_str]["stories"] = story_count
            print(f"    stories (active now): {story_count}")
    except Exception as e:
        print(f"    WARNING IG stories: {e}")

    print(f"    {len(daily_map)} daily rows total")

    # Media posts (no date restriction)
    posts = []
    try:
        r = requests.get(f"{BASE}/{ig_id}/media", params={
            "fields": "id,caption,timestamp,like_count,comments_count,media_type,permalink,thumbnail_url,media_url",
            "limit": 25,
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
                    "id": media.get("id") or "",
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
    parser.add_argument("--days", type=int, default=5)
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

    # Fetch all studios in parallel — 10 workers balances speed vs Meta rate limits.
    ig_map = {}
    print(f"\nFetching {len(STUDIOS)} studios with 10 parallel workers...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_studio, s): s for s in STUDIOS}
        for future in as_completed(futures):
            studio, ig_result = future.result()
            if ig_result:
                ig_map[studio["name"]] = ig_result

    # ── Merge new fetch with existing historical data ──────────────────────
    # The archive cutoff is a fixed 90-day rolling window, independent of --days
    # (which only controls how much fresh data this run pulls from the Meta API).
    # Tying the cutoff to --days meant a small daily --days value (e.g. 5, used by
    # the scheduled refresh) archived and pruned raw daily rows for any month older
    # than 5 days instead of 90, leaving the dashboard's "daily" reach window only
    # a few days deep even though posts (kept for a year) still showed further back.
    rolling_window_cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    for studio_name, new_data in ig_map.items():
        old = existing_studios.get(studio_name, {})

        # Daily rows: merge per field, not per row — a partial fetch (e.g. follower_count's
        # fixed 30-day window reaching further back than a shorter --days reach window) must
        # not blow away fields it didn't fetch, like a previously-captured reach value.
        old_daily = {r["date"]: r for r in old.get("daily", [])}
        new_daily = {r["date"]: r for r in new_data.get("daily", [])}
        merged_daily = dict(old_daily)
        for date, row in new_daily.items():
            merged_daily[date] = {**merged_daily.get(date, {}), **row}
        all_daily = sorted(merged_daily.values(), key=lambda x: x["date"])

        # Posts: dedup by permalink, new API data wins for engagement counts.
        # Preserve Drive image URLs — API always returns expiring CDN URLs.
        old_posts = {p["permalink"]: p for p in old.get("posts", []) if p.get("permalink")}
        new_posts = {p["permalink"]: p for p in new_data.get("posts", []) if p.get("permalink")}
        for permalink, new_p in new_posts.items():
            old_p = old_posts.get(permalink, {})
            merged = {**old_p, **new_p}
            old_img = old_p.get("image_url", "")
            if old_img.startswith("https://drive.google.com/thumbnail"):
                merged["image_url"] = old_img
            old_posts[permalink] = merged
        all_posts = sorted(old_posts.values(), key=lambda x: x.get("date", ""), reverse=True)

        # Monthly aggregates: compute once, from full accumulated data, for complete months
        # entirely before the 90-day window — then archive write-once. A month's raw daily
        # rows get pruned right after it's archived (below), but follower_count's fixed
        # 30-day window keeps re-requesting dates regardless of archival status and can
        # resurrect a handful of reach-less rows for an already-archived month; recomputing
        # from those stragglers would silently overwrite a correct archive with a near-empty
        # one. So a month already in old_monthly is never recomputed — with one exception
        # (below): a month the old --days-based cutoff bug archived too early, before it
        # was genuinely outside the 90-day window.
        old_monthly = {m["month"]: m for m in old.get("monthly", [])}

        # Repair pass for months archived by that bug: if an already-archived month's
        # last day is still inside today's real 90-day window, it never should have been
        # archived. Un-archive it — but only once a fetch (typically a manual --days 90
        # backfill) has actually returned near-complete day-level reach for it, so a
        # handful of follower_count-only stragglers (see above) or a normal small --days
        # run can't accidentally trigger this before a real backfill has run.
        cutoff_dt = datetime.strptime(rolling_window_cutoff, "%Y-%m-%d")
        for mk in list(old_monthly.keys()):
            y, mo = int(mk[:4]), int(mk[5:7])
            days_in_month = monthrange(y, mo)[1]
            month_last_day = datetime(y, mo, days_in_month)
            if month_last_day < cutoff_dt:
                continue  # genuinely outside the window — stays archived
            fresh_days_with_reach = sum(
                1 for r in all_daily
                if _month_key(r["date"]) == mk and r.get("reach") is not None
            )
            if fresh_days_with_reach >= days_in_month * 0.9:
                del old_monthly[mk]

        new_monthly = build_monthly_aggregates(all_daily, all_posts, rolling_window_cutoff,
                                                new_data.get("current_followers"))
        for mk, agg in new_monthly.items():
            if mk not in old_monthly:
                old_monthly[mk] = agg
        new_data["monthly"] = sorted(old_monthly.values(), key=lambda x: x["month"])

        # Prune daily rows from any archived month (this run's or a prior run's), keeping
        # the JSON lean and stopping stray resurrected rows from accumulating indefinitely.
        # Boundary months (partially in the 90-day window) are kept intact so they can be
        # correctly aggregated when they eventually fall out.
        archived_keys = set(old_monthly.keys())
        new_data["daily"] = [r for r in all_daily
                             if _month_key(r["date"]) not in archived_keys]

        # Posts are kept for a full rolling year so the Instagram tab shows
        # 12 months of content. Older posts are already summarised in monthly[].
        year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        new_data["posts"] = [p for p in all_posts
                             if p.get("date", "") >= year_ago]

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

    # ── Upload thumbnails to Google Drive ─────────────────────────────────
    # Instagram/Facebook CDN URLs expire in ~24h. We upload each post's
    # thumbnail to Google Drive so the dashboard can load them with no expiry.
    # Requires: nso-dashboard/credentials/service_account.json
    sa_file = out_path.parent / "credentials" / "service_account.json"
    print(f"\n{'=' * 60}")
    if not sa_file.exists():
        print("WARNING: service_account.json not found — skipping thumbnail upload.")
        print("  Thumbnails will remain as CDN URLs (expire in ~24h).")
    else:
        print("Uploading thumbnails to Google Drive...")
        drive = _DriveClient(sa_file)
        ig_folder = drive.get_or_create_folder("instagram", DRIVE_ROOT_FOLDER)
        studio_folders: dict = {}
        total_ok = total_fail = total_skip = 0

        for studio in output["instagram"]:
            code = studio.get("code", "unknown")
            for post in studio.get("posts", []):
                url = post.get("image_url", "")
                if not url:
                    continue
                # Already on Drive — skip
                if url.startswith("https://drive.google.com/thumbnail"):
                    total_skip += 1
                    continue
                permalink = post.get("permalink", "")
                slug = permalink.rstrip("/").split("/")[-1] if permalink else \
                       post.get("date", "unknown").replace("-", "") + "_" + str(total_ok + total_fail)
                if code not in studio_folders:
                    studio_folders[code] = drive.get_or_create_folder(code, ig_folder)
                try:
                    r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                    r.raise_for_status()
                    fid = drive.upload(r.content, f"{slug}.jpg", studio_folders[code])
                    post["image_url"] = f"https://drive.google.com/thumbnail?id={fid}&sz=w400"
                    total_ok += 1
                except Exception as e:
                    print(f"  WARNING thumbnail {code}/{slug}: {e}")
                    post["image_url"] = ""
                    total_fail += 1

        print(f"  Uploaded: {total_ok}  Failed/cleared: {total_fail}  Skipped (existing): {total_skip}")

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
