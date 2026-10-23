#!/usr/bin/env python3
"""
fetch_organic_social.py - Organic Instagram data for the HQ @sweat440 account.

Feeds the Organic Social Media dashboard (organic-social/index.html). Unlike
nso-dashboard/scripts/fetch_social_insights.py, which estimates reach per post
by splitting a day's account reach across that day's posts, this pulls each
post's own insights from Instagram: reach, views, likes, comments, shares,
saves and, for Reels, average watch time. Rates and medians are computed in
the dashboard so they follow its filters.

Outputs: organic-social/organic_social.json
  account          username, followers, media count (as of this run)
  posts            one row per feed post (Reels, carousels, statics), own
                   posts plus collabs; Stories are not posts here
  monthly          account totals per calendar month: views (followers vs
                   non-followers), profile visits, follows, unfollows
  followers_history  follower count snapshot per run date
  stories_by_month Stories posted per month (from the studio fetch, see
                   stories_by_month below)
  demographics     follower gender/age split, one snapshot per month

Run once a month (manual workflow: organic-social-refresh.yml).

Usage:
    python organic-social/scripts/fetch_organic_social.py
    python organic-social/scripts/fetch_organic_social.py --since 2025-01-01 --refresh-days 365
"""

import argparse
import json
import os
import struct
import sys
import time
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "nso-dashboard" / ".env")
load_dotenv(ROOT / ".env")

# _DriveClient / DRIVE_ROOT_FOLDER are reused so HQ thumbnails land in the same
# Drive folder (instagram/corporate) the studio Instagram fetch already uses.
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "nso-dashboard" / "scripts"))
from fetch_social_insights import _DriveClient, DRIVE_ROOT_FOLDER  # noqa: E402

BASE = "https://graph.facebook.com/v21.0"

HQ_IG_ID = "17841406944098426"
HQ_USERNAME = "sweat440"

# First month of history kept. Account totals and posts are backfilled to here
# on a run that doesn't have them yet.
HISTORY_START = "2025-01-01"

# Reach and views keep adding up for days after a post goes out, so recent
# posts get their insights re-pulled every run. Older posts keep their stored
# values (one call per post saved per run).
REFRESH_DAYS = 60

# Insights each media type supports. Asking for one a type doesn't support fails
# the whole call, so these are split by type (see fetch_media_insights for the
# per-metric fallback when Meta changes support).
REEL_METRICS = ["reach", "views", "likes", "comments", "shares", "saved",
                "total_interactions", "ig_reels_avg_watch_time"]
FEED_METRICS = ["reach", "views", "likes", "comments", "shares", "saved",
                "total_interactions", "profile_visits", "follows"]

MEDIA_FIELDS = ("id,caption,timestamp,media_type,media_product_type,permalink,"
                "thumbnail_url,media_url,like_count,comments_count,username,"
                "collaborators{username}")


def graph_get(path, params, token, retries=4):
    """GET a Graph API path, retrying on rate limits. Returns the JSON body;
    errors come back as {"error": ...} for the caller to handle."""
    url = path if path.startswith("http") else f"{BASE}/{path}"
    params = dict(params or {}, access_token=token) if not path.startswith("http") else None
    for attempt in range(retries):
        try:
            data = requests.get(url, params=params, timeout=60).json()
        except Exception as e:
            data = {"error": {"message": str(e), "code": -1}}
        code = (data.get("error") or {}).get("code")
        if code in (4, 17, 32, 613, -1) and attempt < retries - 1:
            time.sleep(2 ** attempt * 10)
            continue
        return data
    return data


def err_msg(data):
    return (data.get("error") or {}).get("message", "")[:120]


# ── Posts ────────────────────────────────────────────────────────────────────

def post_format(media):
    if media.get("media_product_type") == "REELS":
        return "Reel"
    return {"CAROUSEL_ALBUM": "Carousel", "IMAGE": "Static"}.get(media.get("media_type"), "Video")


def fetch_hq_media(token, since):
    """Every HQ feed post since `since`, newest first. Returns (items, complete);
    complete is False if paging stopped on an error, so callers never prune
    stored posts against a partial list."""
    items, url, params = [], f"{HQ_IG_ID}/media", {"fields": MEDIA_FIELDS, "limit": 100}
    while url:
        data = graph_get(url, params, token)
        if "error" in data:
            print(f"  WARNING HQ media: {err_msg(data)}")
            return items, False
        page = data.get("data", [])
        items.extend(page)
        oldest = min(((m.get("timestamp") or "")[:10] for m in page), default="")
        url, params = data.get("paging", {}).get("next"), None
        if not page or (oldest and oldest < since):
            break
    return [m for m in items if (m.get("timestamp") or "")[:10] >= since], True


def studio_started_collabs(social_insights_path):
    """Media ids of studio-owned posts with @sweat440 as a collaborator.

    Instagram's /media edge only lists posts HQ owns, so a collab a studio
    started and HQ accepted never shows up there. The daily studio fetch records
    each studio post's collaborators (see fetch_social_insights.py), which is
    how these are found. Only covers what that file has kept (about a year of
    posts; collaborators recorded since mid-2026)."""
    try:
        with open(social_insights_path, encoding="utf-8") as f:
            studios = json.load(f).get("instagram", [])
    except Exception as e:
        print(f"  WARNING could not read {social_insights_path}: {e}")
        return {}
    found = {}
    for s in studios:
        if s.get("studio") == "Corporate":
            continue
        for p in s.get("posts", []):
            if HQ_USERNAME in (p.get("collaborators") or []) and p.get("id"):
                found[p["id"]] = s.get("username") or s.get("studio")
    return found


def fetch_media_insights(media_id, metrics, token):
    """{metric: value} for one post. If the batch call fails (a metric Meta no
    longer supports for this post), retries metric by metric and keeps what
    works."""
    def parse(data):
        return {m["name"]: (m.get("values") or [{}])[0].get("value")
                for m in data.get("data", [])}

    data = graph_get(f"{media_id}/insights", {"metric": ",".join(metrics)}, token)
    if "error" not in data:
        return parse(data)
    out = {}
    for metric in metrics:
        one = graph_get(f"{media_id}/insights", {"metric": metric}, token)
        if "error" not in one:
            out.update(parse(one))
    if not out:
        print(f"    WARNING insights {media_id}: {err_msg(data)}")
    return out


def mp4_duration_s(data):
    """Duration in seconds from an MP4's mvhd box, or None. The Graph API has
    no video length field, and reel retention needs it."""
    i = data.find(b"mvhd")
    if i < 4 or len(data) < i + 36:
        return None
    if data[i + 4] == 1:
        timescale, duration = struct.unpack(">IQ", data[i + 24:i + 36])
    else:
        timescale, duration = struct.unpack(">II", data[i + 16:i + 24])
    if not timescale:
        return None
    secs = duration / timescale
    return round(secs, 2) if 0 < secs < 3600 else None


def reel_length_s(video_url):
    """Reel length from its video file. Instagram's MP4s keep the header (moov)
    up front, so the first 512 KB is normally enough; falls back to the whole
    file if not."""
    if not video_url:
        return None
    try:
        r = requests.get(video_url, headers={"Range": "bytes=0-524287"}, timeout=60)
        secs = mp4_duration_s(r.content)
        if secs is None and r.status_code == 206:
            secs = mp4_duration_s(requests.get(video_url, timeout=300).content)
        return secs
    except Exception as e:
        print(f"    WARNING reel length: {e}")
        return None


def build_post(media, insights, collab_source, owner):
    collaborators = [c["username"] for c in (media.get("collaborators") or {}).get("data", [])
                     if c.get("username")]
    if collab_source is None and collaborators:
        collab_source = "hq"
    fmt = post_format(media)
    return {
        "id": media.get("id"),
        "permalink": media.get("permalink") or "",
        "date": (media.get("timestamp") or "")[:10],
        "timestamp": media.get("timestamp") or "",
        "caption": media.get("caption") or "",
        "format": fmt,
        "owner": owner,
        "collab": collab_source is not None,
        "collab_source": collab_source,   # "hq" = HQ started it, "studio" = a studio did
        "collaborators": collaborators,
        "likes": insights.get("likes", media.get("like_count")),
        "comments": insights.get("comments", media.get("comments_count")),
        "shares": insights.get("shares"),
        "saves": insights.get("saved"),
        "reach": insights.get("reach"),
        "views": insights.get("views"),
        "total_interactions": insights.get("total_interactions"),
        "profile_visits": insights.get("profile_visits"),
        "follows": insights.get("follows"),
        "avg_watch_time_ms": insights.get("ig_reels_avg_watch_time") if fmt == "Reel" else None,
        "image_url": media.get("thumbnail_url") or media.get("media_url") or "",
        "insights_at": date.today().isoformat(),
    }


def refresh_posts(token, since, refresh_days, old_posts, social_insights_path):
    """Merge stored posts with this run's fetch. Returns (posts, live_ids or None)."""
    print("\nPosts")
    hq_media, complete = fetch_hq_media(token, since)
    print(f"  {len(hq_media)} HQ posts since {since}")

    media_list = [(m, None, HQ_USERNAME) for m in hq_media]
    collab_ids = studio_started_collabs(social_insights_path)
    hq_ids = {m["id"] for m in hq_media}
    for media_id, owner in collab_ids.items():
        if media_id in hq_ids:
            continue
        m = graph_get(media_id, {"fields": MEDIA_FIELDS}, token)
        if "error" in m:
            print(f"  WARNING collab {media_id}: {err_msg(m)}")
            continue
        if (m.get("timestamp") or "")[:10] >= since:
            media_list.append((m, "studio", m.get("username") or owner))
    print(f"  {len(media_list) - len(hq_media)} studio-started collabs")

    refresh_from = (date.today() - timedelta(days=refresh_days)).isoformat()
    posts, pulled = dict(old_posts), 0
    for media, collab_source, owner in media_list:
        if media.get("media_product_type") in ("STORY", "AD"):
            continue
        old = old_posts.get(media["id"])
        ts = (media.get("timestamp") or "")[:10]
        if old and old.get("reach") is not None and ts < refresh_from:
            continue  # settled post — keep stored insights
        metrics = REEL_METRICS if post_format(media) == "Reel" else FEED_METRICS
        post = build_post(media, fetch_media_insights(media["id"], metrics, token),
                          collab_source, owner)
        pulled += 1
        if old:
            # Keep the permanent Drive thumbnail and the reel length already found.
            if old.get("image_url", "").startswith("https://drive.google.com/"):
                post["image_url"] = old["image_url"]
            post["reel_length_s"] = old.get("reel_length_s")
        if post["format"] == "Reel" and not post.get("reel_length_s"):
            post["reel_length_s"] = reel_length_s(media.get("media_url"))
        posts[media["id"]] = post
    print(f"  insights pulled for {pulled} posts")

    # Drop HQ posts deleted/archived on Instagram since they were stored. Only
    # when the media list came back complete; studio collabs are left alone.
    if complete:
        live = {m["id"] for m in hq_media}
        gone = [pid for pid, p in posts.items()
                if p.get("collab_source") != "studio" and p.get("date", "") >= since
                and pid not in live]
        for pid in gone:
            del posts[pid]
        if gone:
            print(f"  removed {len(gone)} post(s) no longer on Instagram")

    return sorted(posts.values(), key=lambda p: p.get("timestamp", ""), reverse=True)


def sync_thumbnails(posts, sa_file, corporate_posts):
    """Swap expiring Instagram CDN thumbnails for permanent Drive links."""
    # The studio fetch already uploaded thumbnails for HQ posts since mid-2026.
    known = {p["permalink"]: p["image_url"] for p in corporate_posts
             if p.get("permalink") and p.get("image_url", "").startswith("https://drive.google.com/")}
    todo = []
    for p in posts:
        if p.get("image_url", "").startswith("https://drive.google.com/"):
            continue
        if p["permalink"] in known:
            p["image_url"] = known[p["permalink"]]
        elif p.get("image_url"):
            todo.append(p)
    if not todo:
        return
    if not sa_file.exists():
        print(f"\nWARNING {sa_file} not found — {len(todo)} thumbnails stay as CDN URLs (expire in ~24h).")
        return
    print(f"\nUploading {len(todo)} thumbnails to Google Drive...")
    drive = _DriveClient(sa_file)
    folder = drive.get_or_create_folder(
        "corporate", drive.get_or_create_folder("instagram", DRIVE_ROOT_FOLDER))
    for p in todo:
        slug = p["permalink"].rstrip("/").split("/")[-1] or p["id"]
        try:
            r = requests.get(p["image_url"], timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            fid = drive.upload(r.content, f"{slug}.jpg", folder)
            p["image_url"] = f"https://drive.google.com/thumbnail?id={fid}&sz=w400"
        except Exception as e:
            print(f"  WARNING thumbnail {slug}: {e}")
            p["image_url"] = ""


# ── Account totals ───────────────────────────────────────────────────────────

def total_value(metric, since, until, token, breakdown=None):
    """One account metric summed over [since, until). Returns (total,
    {breakdown value: n}) or (None, {}) if Instagram has no data for it."""
    params = {"metric": metric, "period": "day", "metric_type": "total_value",
              "since": since, "until": until}
    if breakdown:
        params["breakdown"] = breakdown
    data = graph_get(f"{HQ_IG_ID}/insights", params, token)
    if "error" in data:
        return None, {}
    for m in data.get("data", []):
        tv = m.get("total_value") or {}
        split = {}
        for b in tv.get("breakdowns", []):
            for res in b.get("results", []):
                split[(res.get("dimension_values") or ["?"])[0]] = res.get("value", 0)
        return tv.get("value"), split
    return None, {}


def month_chunks(month_start):
    """Two [since, until) halves of a calendar month. Instagram caps a
    total_value range at 30 days, so a 31-day month can't go in one call;
    every metric below is a plain count, so the halves add up."""
    y, m = month_start.year, month_start.month
    mid = date(y, m, 16)
    end = date(y, m, monthrange(y, m)[1]) + timedelta(days=1)
    end = min(end, date.today())
    chunks = [(month_start, min(mid, end))]
    if end > mid:
        chunks.append((mid, end))
    return [(a.isoformat(), b.isoformat()) for a, b in chunks if a < b]


def fetch_month(month_start, token):
    row = {"month": month_start.isoformat(), "views": None, "views_followers": None,
           "views_non_followers": None, "profile_views": None,
           "follows": None, "unfollows": None, "complete": True}

    def add(key, val):
        if val is not None:
            row[key] = (row[key] or 0) + val

    for since, until in month_chunks(month_start):
        views, split = total_value("views", since, until, token, breakdown="follow_type")
        if views is None:  # older API naming
            views, split = total_value("views", since, until, token, breakdown="follower_type")
        if views is None:
            views, split = total_value("views", since, until, token)
        add("views", views)
        add("views_followers", split.get("FOLLOWER"))
        add("views_non_followers", split.get("NON_FOLLOWER"))
        add("profile_views", total_value("profile_views", since, until, token)[0])
        _, fu = total_value("follows_and_unfollows", since, until, token, breakdown="follow_type")
        # follow_type FOLLOWER = accounts that followed, NON_FOLLOWER = accounts that unfollowed
        add("follows", fu.get("FOLLOWER"))
        add("unfollows", fu.get("NON_FOLLOWER"))
    last_day = date(month_start.year, month_start.month, monthrange(month_start.year, month_start.month)[1])
    row["complete"] = last_day < date.today()
    return row


def refresh_monthly(token, since, old_monthly):
    """Account totals per month. Closed months already stored aren't re-pulled,
    except last month (Instagram finalises counts a day or two late)."""
    print("\nAccount totals by month")
    today = date.today()
    cur = date(today.year, today.month, 1)
    prev = (cur - timedelta(days=1)).replace(day=1)
    months, m = [], date.fromisoformat(since).replace(day=1)
    while m <= cur:
        months.append(m)
        m = (m + timedelta(days=32)).replace(day=1)
    out = {r["month"]: r for r in old_monthly}
    for m in months:
        old = out.get(m.isoformat())
        if old and old.get("complete") and old.get("views") is not None and m < prev:
            continue
        row = fetch_month(m, token)
        out[m.isoformat()] = row
        print(f"  {m:%Y-%m}: views {row['views']}, profile visits {row['profile_views']}, "
              f"follows {row['follows']}, unfollows {row['unfollows']}")
    return sorted(out.values(), key=lambda r: r["month"])


def fetch_demographics(token):
    out = {}
    for breakdown in ("gender", "age"):
        data = graph_get(f"{HQ_IG_ID}/insights", {
            "metric": "follower_demographics", "period": "lifetime",
            "metric_type": "total_value", "timeframe": "this_month",
            "breakdown": breakdown}, token)
        if "error" in data:
            print(f"  WARNING demographics {breakdown}: {err_msg(data)}")
            continue
        for m in data.get("data", []):
            for b in (m.get("total_value") or {}).get("breakdowns", []):
                out[breakdown] = {(r.get("dimension_values") or ["?"])[0]: r.get("value", 0)
                                  for r in b.get("results", [])}
    return out


def stories_by_month(corporate, old_counts):
    """{"YYYY-MM": {"count": Stories posted, "days": days with a snapshot}} for HQ.

    Stories live 24h and Instagram has no history endpoint, so a monthly run
    can't count them. The daily studio fetch (fetch_social_insights.py) records
    how many HQ Stories are live each day, on daily rows while they're kept and
    as stories_count once a month is archived; this reads both. Months already
    counted are kept even after that file drops them. `days` lets the dashboard
    flag a month the snapshot didn't cover in full (it started in late
    September 2026, so that month reads low)."""
    counts = dict(old_counts)
    for m in corporate.get("monthly", []):
        if m.get("stories_count"):
            counts[m["month"][:7]] = {"count": m["stories_count"], "days": m.get("days_with_data")}
    daily = {}
    for r in corporate.get("daily", []):
        if r.get("stories") is not None:
            row = daily.setdefault(r["date"][:7], {"count": 0, "days": 0})
            row["count"] += r["stories"]
            row["days"] += 1
    counts.update(daily)
    return dict(sorted(counts.items()))


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch organic Instagram data for HQ @sweat440")
    parser.add_argument("--output", default=str(ROOT / "organic-social" / "organic_social.json"))
    parser.add_argument("--since", default=HISTORY_START, help="first date kept, YYYY-MM-DD")
    parser.add_argument("--refresh-days", type=int, default=REFRESH_DAYS,
                        help="re-pull insights for posts this many days old or newer")
    parser.add_argument("--social-insights", default=str(ROOT / "nso-dashboard" / "social_insights.json"))
    parser.add_argument("--credentials", default=str(ROOT / "nso-dashboard" / "credentials" / "service_account.json"))
    args = parser.parse_args()

    token = os.environ.get("META_ACCESS_TOKEN")
    if not token:
        print("ERROR: META_ACCESS_TOKEN not set")
        sys.exit(1)

    out_path = Path(args.output)
    old = {}
    if out_path.exists():
        with open(out_path, encoding="utf-8") as f:
            old = json.load(f)

    print("=" * 60)
    print(f"SWEAT440 HQ organic Instagram — @{HQ_USERNAME}")
    print("=" * 60)

    acct = graph_get(HQ_IG_ID, {"fields": "username,followers_count,media_count"}, token)
    if "error" in acct:
        print(f"ERROR account: {err_msg(acct)}")
        sys.exit(1)
    today = date.today().isoformat()
    print(f"  followers {acct.get('followers_count')}, media {acct.get('media_count')}")

    followers_history = {r["date"]: r for r in old.get("followers_history", [])}
    followers_history[today] = {"date": today, "followers": acct.get("followers_count")}

    old_posts = {p["id"]: p for p in old.get("posts", [])}
    posts = refresh_posts(token, args.since, args.refresh_days, old_posts, args.social_insights)

    corporate = {}
    try:
        with open(args.social_insights, encoding="utf-8") as f:
            corporate = next((s for s in json.load(f).get("instagram", [])
                              if s.get("studio") == "Corporate"), {})
    except Exception:
        pass
    sync_thumbnails(posts, Path(args.credentials), corporate.get("posts", []))

    monthly = refresh_monthly(token, args.since, old.get("monthly", []))

    stories = stories_by_month(corporate, old.get("stories_by_month", {}))
    print(f"\nStories by month: {stories}")

    demographics = dict(old.get("demographics", {}))
    demo = fetch_demographics(token)
    if demo:
        demographics[today[:7]] = {"as_of": today, **demo}

    output = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "account": {"ig_id": HQ_IG_ID, "username": acct.get("username"),
                    "followers": acct.get("followers_count"),
                    "media_count": acct.get("media_count")},
        "posts": posts,
        "monthly": monthly,
        "followers_history": sorted(followers_history.values(), key=lambda r: r["date"]),
        "stories_by_month": stories,
        "demographics": demographics,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nDone. {len(posts)} posts, {len(monthly)} months → {out_path}")


if __name__ == "__main__":
    main()
