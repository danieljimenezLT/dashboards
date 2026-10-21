#!/usr/bin/env python3
"""
fetch_replies_data.py

Monthly-only fetch of Instagram DM and comment reply times, for the
"Replies to DMs" (within 4 business hours) and "Replies to comments"
(share of comments with >=1 like or a studio reply) rubric items.
Outputs data/replies.json.

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

Who counts as "the studio": the Graph API's `from.id` on a comment is an
Instagram-scoped ID (IGSID), NOT the business account's ig_id — so an
id-only match never recognizes the studio's own replies (this scored every
studio 0 on-time for 2026-09). Authorship is matched on username first,
with ig_id kept as a fallback.

Permissions: without instagram_manage_comments on META_ACCESS_TOKEN the API
still returns comments and replies but strips `from`/`username` from all of
them, so no reply can be attributed to the studio — those posts are skipped
rather than scored as "never replied". DMs need instagram_manage_messages
and a Page access token (the Conversations API rejects a user token with
error #190), so one is exchanged per studio from META_ACCESS_TOKEN. The
granted permissions are printed at the start of each run.

A studio whose posts/conversations couldn't be read at all is left out of
that month's result for that item (build_audit_data.py then treats it as
not-yet-sourced) instead of being scored as if it had zero activity.

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

def _is_studio(author: dict, ig_id: str, username: str | None) -> bool:
    """author: a comment/reply/message dict (has `from`, and for comments a
    top-level `username`). Username is the reliable key — see the module
    docstring on IGSID vs ig_id."""
    frm = author.get("from") or {}
    if username:
        names = {(author.get("username") or "").lower(), (frm.get("username") or "").lower()}
        if username.lower() in names:
            return True
    return bool(ig_id) and frm.get("id") == ig_id


def evaluate_comments(comments, ig_id: str, username: str | None, tz, month_key: str):
    """comments: list of {id, timestamp, username, from:{id,username},
    like_count, replies:{data:[...]}} as returned by
    /{media-id}/comments?fields=id,timestamp,username,from,like_count,replies{...}.
    Returns (total_incoming, on_time_count, replied_count, engaged_count)
    for comments posted in month_key — replied_count ignores the deadline;
    engaged_count is comments with >=1 like (from anyone — the API only
    exposes a count, not who liked it) or a studio reply, at any time."""
    total = 0
    on_time = 0
    replied = 0
    engaged = 0
    for c in comments:
        if _is_studio(c, ig_id, username):
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
            if _is_studio(r, ig_id, username) and r.get("timestamp")
        ]
        if studio_replies:
            replied += 1
            if min(studio_replies) <= deadline:
                on_time += 1
        if studio_replies or (c.get("like_count") or 0) >= 1:
            engaged += 1
    return total, on_time, replied, engaged


def evaluate_conversation_messages(messages, ig_id: str, username: str | None, tz, month_key: str):
    """messages: chronologically ASCENDING list of {id, created_time, from:{id,username}}.
    Returns (total_incoming, on_time_count) for customer messages sent in
    month_key, matched to the next studio message after each, if any."""
    total = 0
    on_time = 0
    n = len(messages)
    for i, m in enumerate(messages):
        if _is_studio(m, ig_id, username):
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
            if _is_studio(later, ig_id, username) and later.get("created_time"):
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


COMMENTS_PASS_RATE = 0.9
COMMENTS_PARTIAL_RATE = 0.5


def classify_comments(total: int, engaged: int) -> str:
    """Share of comments with >=1 like or a studio reply: >=90% (or no
    comments) = Yes, 50-90% = Sometimes, <50% = No."""
    if total == 0:
        return DM_ONTIME_PASS
    rate = engaged / total
    if rate >= COMMENTS_PASS_RATE:
        return DM_ONTIME_PASS
    if rate >= COMMENTS_PARTIAL_RATE:
        return DM_ONTIME_PARTIAL
    return DM_ONTIME_FAIL


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


def fetch_studio_username(ig_id: str, token: str) -> str | None:
    try:
        return _get(f"{BASE}/{ig_id}", {"fields": "username"}, token).get("username")
    except Exception as e:
        print(f"    WARNING username for {ig_id}: {e}")
        return None


def fetch_page_token(page_id: str, token: str) -> str | None:
    """The Conversations API needs a Page access token, not a user token."""
    try:
        return _get(f"{BASE}/{page_id}", {"fields": "access_token"}, token).get("access_token")
    except Exception as e:
        print(f"    WARNING page token for {page_id}: {e}")
        return None


def log_token_permissions(token: str):
    """Reply detection needs instagram_manage_comments (comment authors) and
    instagram_manage_messages (DMs) — print what the token actually has."""
    try:
        perms = _get(f"{BASE}/me/permissions", {}, token).get("data", [])
    except Exception as e:
        print(f"WARNING could not read token permissions: {e}")
        return
    granted = {p["permission"] for p in perms if p.get("status") == "granted"}
    for needed in ("instagram_manage_comments", "instagram_manage_messages"):
        print(f"Token permission {needed}: {'granted' if needed in granted else 'MISSING'}")


def fetch_comment_stats(ig_id: str, username: str | None, posts_in_month: list, tz, month_key: str, token: str):
    """Returns (total, on_time, replied, engaged, posts_read, authorless) — posts_read
    lets the caller tell "no comments" apart from "couldn't read any post";
    authorless means the API hid comment authors, so replies can't be attributed."""
    total = on_time = replied = engaged = posts_read = 0
    authorless = False
    for post in posts_in_month:
        media_id = post.get("id")
        if not media_id:
            continue
        try:
            comments = list(_paginate(
                f"{BASE}/{media_id}/comments",
                {"fields": "id,timestamp,username,from,like_count,replies{id,timestamp,username,from}", "limit": 50},
                token,
            ))
        except Exception as e:
            print(f"    WARNING comments for media {media_id}: {e}")
            continue
        if any(not (c.get("from") or {}).get("id") and not c.get("username") for c in comments):
            # The API hides comment authors when the token lacks
            # instagram_manage_comments — every reply would look like it
            # came from a customer, so the item can't be scored at all.
            authorless = True
            continue
        posts_read += 1
        t, o, r, e = evaluate_comments(comments, ig_id, username, tz, month_key)
        total += t
        on_time += o
        replied += r
        engaged += e
    return total, on_time, replied, engaged, posts_read, authorless


def fetch_dm_stats(page_id: str, ig_id: str, username: str | None, tz, month_key: str, token: str):
    """token must be a Page access token. Returns (total, on_time), or None
    if the conversation list couldn't be read at all."""
    total = on_time = 0
    # Small pages (limit 50 trips "Please reduce the amount of data"). The
    # list comes newest-updated first, so stop at the first conversation
    # last touched before the target month — it can't hold any of its messages.
    # The list comes newest-updated first, so stop at the first conversation
    # last touched before the target month — it can't hold any of its
    # messages. Known limitation: for Pages with a large inbox Meta's
    # endpoint times out ("Please reduce the amount of data" / "Timeout")
    # even at tiny page sizes; those studios' DMs stay unscored. The robust
    # fix is a messaging webhook, not polling.
    month_start = datetime.strptime(month_key + "-01", "%Y-%m-%d").replace(tzinfo=tz)
    conversations = []
    try:
        for convo in _paginate(
            f"{BASE}/{page_id}/conversations",
            {"platform": "instagram", "fields": "id,updated_time", "limit": 10},
            token,
            limit=50,
        ):
            ts = convo.get("updated_time")
            if ts and _parse_time(ts, tz) < month_start:
                break
            conversations.append(convo)
    except Exception as e:
        print(f"    WARNING conversations: {e}")
        if not conversations:
            return None

    for convo in conversations:
        convo_id = convo["id"]
        try:
            data = _get(
                f"{BASE}/{convo_id}",
                {"fields": "messages.limit(50){id,created_time,from}"},
                token,
            )
        except Exception as e:
            print(f"    WARNING messages for conversation {convo_id}: {e}")
            continue
        messages = (data.get("messages") or {}).get("data", [])
        messages.reverse()  # Graph API returns newest-first; we need ascending
        t, o = evaluate_conversation_messages(messages, ig_id, username, tz, month_key)
        total += t
        on_time += o
        time.sleep(0.1)  # stay well within rate limits across many conversations
    return total, on_time


def main():
    parser = argparse.ArgumentParser(description="Fetch Instagram DM/comment reply-time stats")
    parser.add_argument("--month", help="YYYY-MM (default: most recently completed calendar month)")
    parser.add_argument("--studio", help="Only fetch this studio (exact name) — for quick token/permission checks")
    args = parser.parse_args()

    target_month = args.month or previous_month_key(datetime.now().date())
    print(f"Target month: {target_month}")

    token = os.environ.get("META_ACCESS_TOKEN")
    if not token:
        print("ERROR: META_ACCESS_TOKEN not set")
        sys.exit(1)
    log_token_permissions(token)

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
        if args.studio and name != args.studio:
            continue
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

        ig_id = ig_entry["ig_id"]
        username = ig_entry.get("username") or fetch_studio_username(ig_id, token)
        print(f"\n>> {name} (@{username or '?'}, {len(posts_in_month)} posts this month)")
        if not username:
            print("    WARNING no username — falling back to id match, studio replies may be missed")

        c_total, c_on_time, c_replied, c_engaged, c_read, c_authorless = fetch_comment_stats(
            ig_id, username, posts_in_month, tz, target_month, token)
        dm_stats = None
        if page_id:
            page_token = fetch_page_token(page_id, token)
            if page_token:
                dm_stats = fetch_dm_stats(page_id, ig_id, username, tz, target_month, page_token)

        # Items that couldn't be read at all are left out, so they fall back
        # to build_audit_data.py's not-yet-sourced default instead of scoring.
        month_data = {}
        if c_authorless:
            print("    comments: API returned comments without authors — not scored")
        elif posts_in_month and c_read == 0:
            print(f"    comments: none of {len(posts_in_month)} posts readable — not scored")
        else:
            print(f"    comments: {c_engaged}/{c_total} liked or replied, {c_replied}/{c_total} replied, "
                  f"{c_on_time}/{c_total} replied on time ({c_read}/{len(posts_in_month)} posts read)")
            month_data["repliesComments"] = classify_comments(c_total, c_engaged)
            month_data["commentStats"] = {"total": c_total, "engaged": c_engaged, "onTime": c_on_time,
                                          "replied": c_replied, "postsRead": c_read,
                                          "postsInMonth": len(posts_in_month)}
        if dm_stats is None:
            print("    DMs: not readable — not scored")
        else:
            d_total, d_on_time = dm_stats
            print(f"    DMs: {d_on_time}/{d_total} on time")
            month_data["repliesDMs"] = classify_dms(d_total, d_on_time)
            month_data["dmStats"] = {"total": d_total, "onTime": d_on_time}
        out[name] = month_data

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
