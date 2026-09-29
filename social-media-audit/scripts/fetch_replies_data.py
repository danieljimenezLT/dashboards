#!/usr/bin/env python3
"""
fetch_replies_data.py

Monthly-only fetch of Instagram DM and comment reply times, for the
"Replies to DMs" (within 4 business hours) and "Replies to comments"
(within 1 business day) rubric items. Outputs data/replies.json.

- Comments: read from each post's own comment thread via the Graph API.
  Comment threads persist indefinitely, so there's no retention concern
  running this once a month for the prior month.
- DMs: read via the Conversations API — customer-initiated threads only,
  which is also the only kind visible to a Page/IG account in the first
  place. One real constraint: Instagram drops an unanswered thread from
  the "Requests" folder — and therefore from the API — after 30 days of
  inactivity. Since this runs once a month for the month that just
  ended, the first few days of that month sit close to that 30-day cliff
  by the time it runs; a day-1 message that was never answered has a
  real chance of aging out before this ever sees it. Not fixable without
  fetching more often than once a month — flagged, not hidden.

Business hours/days are evaluated in each studio's own local timezone
(see business_time.py), Mon-Fri 9am-6pm, no holiday calendar.

Reuses ig_id and each post's media id (both resolved daily by
fetch_social_insights.py) and page_id (studios.json) — makes no calls
that duplicate that daily job.

Usage:
    python scripts/fetch_replies_data.py
    python scripts/fetch_replies_data.py --month 2026-08
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
AUDIT_DIR = SCRIPT_DIR.parent
REPO_ROOT = AUDIT_DIR.parent

sys.path.insert(0, str(SCRIPT_DIR))
import business_time as bt  # noqa: E402

sys.path.insert(0, str(REPO_ROOT))
import studios as studios_registry  # noqa: E402

load_dotenv(REPO_ROOT / "nso-dashboard" / ".env")

BASE = "https://graph.facebook.com/v21.0"
SOCIAL_INSIGHTS_PATH = REPO_ROOT / "nso-dashboard" / "social_insights.json"
OUTPUT_PATH = AUDIT_DIR / "data" / "replies.json"

DM_THRESHOLD_HOURS = 4
DM_ONTIME_PASS = "Yes"
DM_ONTIME_PARTIAL = "Sometimes"
DM_ONTIME_FAIL = "No"


def previous_month_key(today) -> str:
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month.fromordinal(first_of_this_month.toordinal() - 1)
    return f"{last_of_prev_month.year}-{last_of_prev_month.month:02d}"


def _parse_time(iso_str: str, tz):
    """Graph API timestamps (media/comments: 'YYYY-MM-DDTHH:MM:SS+0000';
    messages: same ISO8601-with-offset shape) -> aware datetime in tz."""
    dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S%z")
    return dt.astimezone(tz)


# ── Pure evaluation functions (no network) — unit-tested separately ────────

def evaluate_comments(comments, ig_id: str, tz, month_key: str):
    """comments: list of {id, timestamp, from:{id}, replies:{data:[...]}}
    as returned by /{media-id}/comments?fields=id,timestamp,from,replies{...}.
    Returns (total_incoming, on_time_count) for comments posted in month_key."""
    total = 0
    on_time = 0
    for c in comments:
        from_id = (c.get("from") or {}).get("id")
        if from_id == ig_id:
            continue  # the studio's own top-level comment, not incoming
        ts = c.get("timestamp")
        if not ts:
            continue
        comment_time = _parse_time(ts, tz)
        if comment_time.strftime("%Y-%m") != month_key:
            continue
        total += 1

        deadline = bt.next_business_day_end(comment_time)
        replies = (c.get("replies") or {}).get("data", [])
        studio_replies = [
            _parse_time(r["timestamp"], tz) for r in replies
            if (r.get("from") or {}).get("id") == ig_id and r.get("timestamp")
        ]
        if studio_replies and min(studio_replies) <= deadline:
            on_time += 1
    return total, on_time


def evaluate_conversation_messages(messages, ig_id: str, tz, month_key: str):
    """messages: chronologically ASCENDING list of {id, created_time, from:{id}}.
    Returns (total_incoming, on_time_count) for customer messages sent in
    month_key, matched to the next studio message after each, if any."""
    total = 0
    on_time = 0
    n = len(messages)
    for i, m in enumerate(messages):
        from_id = (m.get("from") or {}).get("id")
        if from_id == ig_id:
            continue  # the studio's own message
        ts = m.get("created_time")
        if not ts:
            continue
        msg_time = _parse_time(ts, tz)
        if msg_time.strftime("%Y-%m") != month_key:
            continue
        total += 1

        reply_time = None
        for later in messages[i + 1:]:
            if (later.get("from") or {}).get("id") == ig_id and later.get("created_time"):
                reply_time = _parse_time(later["created_time"], tz)
                break
        if reply_time and bt.business_hours_between(msg_time, reply_time) <= DM_THRESHOLD_HOURS:
            on_time += 1
    return total, on_time


def classify_dms(total: int, on_time: int) -> str:
    if total == 0 or on_time == total:
        return DM_ONTIME_PASS
    if on_time == 0:
        return DM_ONTIME_FAIL
    return DM_ONTIME_PARTIAL


def classify_comments(total: int, on_time: int) -> str:
    """Mirrors classify_dms(): full credit if every comment was replied to in
    time (or there were none), no credit if none were, partial credit
    ("Sometimes") in between — instead of collapsing any single late reply
    down to the same 0 credit as never replying at all."""
    if total == 0 or on_time == total:
        return DM_ONTIME_PASS
    if on_time == 0:
        return DM_ONTIME_FAIL
    return DM_ONTIME_PARTIAL


# ── Network calls ───────────────────────────────────────────────────────────

def _get(url, params, token):
    params = {**params, "access_token": token}
    r = requests.get(url, params=params, timeout=30)
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", str(data["error"])))
    return data


def _paginate(url, params, token, limit=10):
    """Yields items across up to `limit` pages of a Graph API edge."""
    page = 0
    while url and page < limit:
        data = _get(url, params, token) if page == 0 else requests.get(url, timeout=30).json()
        if "error" in data:
            raise RuntimeError(data["error"].get("message", str(data["error"])))
        yield from data.get("data", [])
        url = (data.get("paging") or {}).get("next")
        params = {}
        page += 1


def fetch_comment_stats(ig_id: str, posts_in_month: list, tz, month_key: str, token: str):
    total = on_time = 0
    for post in posts_in_month:
        media_id = post.get("id")
        if not media_id:
            continue
        try:
            comments = list(_paginate(
                f"{BASE}/{media_id}/comments",
                {"fields": "id,timestamp,from,replies{id,timestamp,from}", "limit": 50},
                token,
            ))
        except Exception as e:
            print(f"    WARNING comments for media {media_id}: {e}")
            continue
        t, o = evaluate_comments(comments, ig_id, tz, month_key)
        total += t
        on_time += o
    return total, on_time


def fetch_dm_stats(page_id: str, ig_id: str, tz, month_key: str, token: str):
    total = on_time = 0
    try:
        conversations = list(_paginate(
            f"{BASE}/{page_id}/conversations",
            {"platform": "instagram", "fields": "id,updated_time", "limit": 50},
            token,
        ))
    except Exception as e:
        print(f"    WARNING conversations: {e}")
        return total, on_time

    for convo in conversations:
        convo_id = convo["id"]
        try:
            data = _get(
                f"{BASE}/{convo_id}",
                {"fields": "messages.limit(100){id,created_time,from}"},
                token,
            )
        except Exception as e:
            print(f"    WARNING messages for conversation {convo_id}: {e}")
            continue
        messages = (data.get("messages") or {}).get("data", [])
        messages.reverse()  # Graph API returns newest-first; we need ascending
        t, o = evaluate_conversation_messages(messages, ig_id, tz, month_key)
        total += t
        on_time += o
        time.sleep(0.1)  # stay well within rate limits across many conversations
    return total, on_time


def main():
    parser = argparse.ArgumentParser(description="Fetch Instagram DM/comment reply-time stats")
    parser.add_argument("--month", help="YYYY-MM (default: most recently completed calendar month)")
    args = parser.parse_args()

    target_month = args.month or previous_month_key(datetime.now().date())
    print(f"Target month: {target_month}")

    token = os.environ.get("META_ACCESS_TOKEN")
    if not token:
        print("ERROR: META_ACCESS_TOKEN not set")
        sys.exit(1)

    if not SOCIAL_INSIGHTS_PATH.exists():
        print(f"ERROR: {SOCIAL_INSIGHTS_PATH} not found — run fetch_social_insights.py first.")
        sys.exit(1)
    with open(SOCIAL_INSIGHTS_PATH, encoding="utf-8") as f:
        social_insights = json.load(f)
    ig_by_name = {s["studio"]: s for s in social_insights.get("instagram", [])}

    all_studios = studios_registry.load_studios()

    out = {}
    for s in all_studios:
        if s.get("status") not in ("open", "nso"):
            continue
        name = s["name"]
        ig_entry = ig_by_name.get(name)
        if not ig_entry or not ig_entry.get("ig_id"):
            continue
        page_id = (s.get("meta") or {}).get("page_id")
        if not page_id:
            print(f"  – {name}: no page_id, skipping DMs")

        tz = bt.studio_timezone(s.get("state"))
        posts_in_month = [
            p for p in ig_entry.get("posts", [])
            if p.get("date", "").startswith(target_month) and p.get("id")
        ]

        print(f"\n>> {name} ({len(posts_in_month)} posts this month)")
        c_total, c_on_time = fetch_comment_stats(ig_entry["ig_id"], posts_in_month, tz, target_month, token)
        d_total = d_on_time = 0
        if page_id:
            d_total, d_on_time = fetch_dm_stats(page_id, ig_entry["ig_id"], tz, target_month, token)

        print(f"    comments: {c_on_time}/{c_total} on time  |  DMs: {d_on_time}/{d_total} on time")

        out[name] = {
            "repliesComments": classify_comments(c_total, c_on_time),
            "repliesDMs": classify_dms(d_total, d_on_time),
            "commentStats": {"total": c_total, "onTime": c_on_time},
            "dmStats": {"total": d_total, "onTime": d_on_time},
        }

    existing = {}
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            existing = json.load(f).get("studios", {})
    for name, month_data in out.items():
        existing.setdefault(name, {})[target_month] = month_data

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "studios": existing,
        }, f, indent=2, ensure_ascii=False)

    print(f"\nStudios written: {len(out)}")
    print(f"Written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
