#!/usr/bin/env python3
"""
build_audit_data.py

Builds social-media-audit/data/latest.json — the file index.html reads.

Combines three sources for one target month (default: the most recently
completed calendar month):
  - studios.json                          — studio identity, NSO/Open Studio status
  - nso-dashboard/social_insights.json    — Instagram: posts, reach, stories,
                                             engagement rate, followers (fetched
                                             daily by fetch_social_insights.py;
                                             not re-fetched here)
  - data/brand_offenses.json              — off-brand posts, and per-category
                                             penalized counts (image/video,
                                             caption, audio, stories) used to
                                             compute % on-brand for each
  - data/replies.json                     — DM/comment reply-time results
                                             (fetch_replies_data.py)

Every rubric item is sourced now. A studio missing from replies.json (e.g.
that fetch failed for it, or hasn't run yet) falls back to a neutral
"passing" placeholder — see MOCK_DEFAULTS below — so a gap in one source
doesn't drag its score down before that source catches up.

Run once a month (see .github/workflows/social-media-audit-refresh.yml),
after fetch_brand_offenses.py and fetch_replies_data.py. Does NOT touch
Instagram's API itself — that's the existing daily job in
fetch_social_insights.py, plus the monthly fetch_replies_data.py.

Usage:
    python scripts/build_audit_data.py
    python scripts/build_audit_data.py --month 2026-08
"""
import argparse
import json
import statistics
import sys
from datetime import date
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
AUDIT_DIR  = SCRIPT_DIR.parent
REPO_ROOT  = AUDIT_DIR.parent

sys.path.insert(0, str(REPO_ROOT))
import studios as studios_registry  # noqa: E402

sys.path.insert(0, str(REPO_ROOT / "nso-dashboard" / "scripts"))
from fetch_social_insights import build_monthly_aggregates  # noqa: E402

SOCIAL_INSIGHTS_PATH = REPO_ROOT / "nso-dashboard" / "social_insights.json"
BRAND_OFFENSES_PATH  = AUDIT_DIR / "data" / "brand_offenses.json"
REPLIES_PATH          = AUDIT_DIR / "data" / "replies.json"
OUTPUT_PATH           = AUDIT_DIR / "data" / "latest.json"

MONTHS_KEPT = 12  # trailing months retained per studio in the output file

# Studios exempt from the follower-growth requirement (see the Sept 2026
# rubric update doc: Dallas-Uptown inherited an old 19k-follower account and
# isn't penalized for growth or follower count).
EXEMPT_GROWTH_STUDIOS = {"Dallas - Uptown"}

# Placeholder values for rubric items with no real data source yet. All
# "passing" by design so an unsourced item doesn't drag scores down before
# it's actually wired up — see MOCK_FIELDS in index.html for what's real vs
# not (kept in sync manually; there's no shared source of truth for this).
MOCK_DEFAULTS = {
    "repliesDMs": "Yes",
    "repliesComments": True,
}

# Brand Offenses Tracker "Type of offense" values -> the rubric's on-brand
# categories. Matched loosely (substring, case-insensitive) since the sheet's
# exact wording ("Image/ Video" — note the space) isn't guaranteed to stay
# byte-for-byte stable.
OFFENSE_TYPE_TO_CATEGORY = [
    (("image", "video"), "imgVideo"),
    (("caption",), "caption"),
    (("audio",), "audio"),
    (("story", "stories"), "stories"),
]


def normalize_offense_category(raw_type: str) -> str | None:
    t = (raw_type or "").strip().lower()
    for keywords, category in OFFENSE_TYPE_TO_CATEGORY:
        if any(kw in t for kw in keywords):
            return category
    return None


def on_brand_pct(penalized_count: int, total: int) -> float:
    """1 - penalized/total, matching the original workbook's on-brand formula.
    No content that month (total==0) reads as full credit — nothing to
    penalize, not the same as failing."""
    if not total:
        return 1.0
    return max(0.0, round(1 - penalized_count / total, 4))


def previous_month_key(today: date) -> str:
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month.fromordinal(first_of_this_month.toordinal() - 1)
    return f"{last_of_prev_month.year}-{last_of_prev_month.month:02d}"


def month_after(month_key: str) -> str:
    y, m = int(month_key[:4]), int(month_key[5:7])
    return f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"


def match_studio_name(sheet_name: str, canonical_names: list[str]) -> str | None:
    """Best-effort match of a Brand Offenses sheet studio name (often
    shortened, e.g. "Capitol View") to studios.json's canonical name (e.g.
    "Nashville - Capitol View")."""
    low = sheet_name.strip().lower()
    for name in canonical_names:
        if name.lower() == low:
            return name
    for name in canonical_names:
        if name.split(" - ")[-1].strip().lower() == low:
            return name
    for name in canonical_names:
        nlow = name.lower()
        if low in nlow or nlow in low:
            return name
    return None


def get_month_aggregate(ig_entry: dict, month_key: str) -> dict:
    """Returns {post_count, reach, stories_count, median_engagement_rate,
    followers} for month_key, from social_insights.json's archived monthly[]
    if present, else recomputed on the fly from daily[]/posts[] (covers the
    common case: this script runs right after month-end, before that month
    has aged out of the 90-day rolling window and been archived)."""
    for m in ig_entry.get("monthly", []):
        if m.get("month", "").startswith(month_key):
            return m

    before = month_after(month_key)
    recomputed = build_monthly_aggregates(
        ig_entry.get("daily", []), ig_entry.get("posts", []), before,
    )
    return recomputed.get(f"{month_key}-01", {})


def main():
    parser = argparse.ArgumentParser(description="Build social-media-audit/data/latest.json")
    parser.add_argument("--month", help="YYYY-MM (default: most recently completed calendar month)")
    args = parser.parse_args()

    target_month = args.month or previous_month_key(date.today())
    print(f"Target month: {target_month}")

    if not SOCIAL_INSIGHTS_PATH.exists():
        print(f"ERROR: {SOCIAL_INSIGHTS_PATH} not found — run fetch_social_insights.py first (daily job).")
        sys.exit(1)
    with open(SOCIAL_INSIGHTS_PATH, encoding="utf-8") as f:
        social_insights = json.load(f)
    ig_by_name = {s["studio"]: s for s in social_insights.get("instagram", [])}

    brand_offenses = {}
    if BRAND_OFFENSES_PATH.exists():
        with open(BRAND_OFFENSES_PATH, encoding="utf-8") as f:
            brand_offenses = json.load(f).get("studios", {})
    else:
        print(f"WARNING: {BRAND_OFFENSES_PATH} not found — run fetch_brand_offenses.py first. Off-brand posts will be empty.")

    replies_by_name = {}
    if REPLIES_PATH.exists():
        with open(REPLIES_PATH, encoding="utf-8") as f:
            replies_by_name = json.load(f).get("studios", {})
    else:
        print(f"WARNING: {REPLIES_PATH} not found — run fetch_replies_data.py first. DM/comment replies will default to passing.")

    existing = {"studios": []}
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            existing = json.load(f)
    existing_by_code = {s["code"]: s for s in existing.get("studios", [])}

    all_studios = studios_registry.load_studios()
    canonical_names = [s["name"] for s in all_studios]

    offense_lookup = {}
    unmatched_offense_studios = set()
    for sheet_name in brand_offenses:
        matched = match_studio_name(sheet_name, canonical_names)
        if matched:
            offense_lookup[matched] = brand_offenses[sheet_name]
        else:
            unmatched_offense_studios.add(sheet_name)
    if unmatched_offense_studios:
        print(f"WARNING: {len(unmatched_offense_studios)} Brand Offenses studio name(s) didn't match studios.json:")
        for n in sorted(unmatched_offense_studios):
            print(f"  '{n}'")

    out_studios = []
    skipped_no_ig = []

    for s in all_studios:
        if s.get("status") not in ("open", "nso"):
            continue
        if not s.get("social_slug"):
            continue

        name = s["name"]
        ig_entry = ig_by_name.get(name)
        if not ig_entry:
            skipped_no_ig.append(name)
            continue

        agg = get_month_aggregate(ig_entry, target_month)
        posts = agg.get("post_count") or 0
        reach = agg.get("reach") or 0
        stories_count = agg.get("stories_count") or 0
        engagement_pct = agg.get("median_engagement_rate") or 0
        followers_count = agg.get("followers")
        if followers_count is None:
            followers_count = ig_entry.get("current_followers") or 0

        existing_studio = existing_by_code.get(s.get("code") or s["social_slug"], {})
        existing_months = existing_studio.get("months", {})
        prior_month_keys = sorted(k for k in existing_months if k < target_month)
        prior_followers = existing_months[prior_month_keys[-1]]["followersCount"] if prior_month_keys else None

        if prior_followers:
            follower_growth_pct = round((followers_count - prior_followers) / prior_followers, 4)
            net_new_followers = followers_count - prior_followers
        else:
            follower_growth_pct = 0.0
            net_new_followers = None

        offenses_this_month = [
            o for o in offense_lookup.get(name, {}).get(target_month, [])
        ]

        replies_this_month = replies_by_name.get(name, {}).get(target_month, {})
        replies_dms = replies_this_month.get("repliesDMs", MOCK_DEFAULTS["repliesDMs"])
        replies_comments = replies_this_month.get("repliesComments", MOCK_DEFAULTS["repliesComments"])

        # % on-brand per category = 1 - (penalized offenses in that category
        # / total posts), same formula the current workbook uses — just
        # sourced from the Brand Offenses Tracker instead of manual review.
        penalized_by_category = {"imgVideo": 0, "caption": 0, "audio": 0, "stories": 0}
        for o in offenses_this_month:
            if o.get("impact") != "Penalized":
                continue
            category = normalize_offense_category(o.get("type"))
            if category:
                penalized_by_category[category] += 1

        month_data = {
            "posts": posts,
            "imgVideoPct": on_brand_pct(penalized_by_category["imgVideo"], posts),
            "captionPct": on_brand_pct(penalized_by_category["caption"], posts),
            "audioPct": on_brand_pct(penalized_by_category["audio"], posts),
            "engagementPct": engagement_pct,
            "monthlyReach": reach,
            "storiesCount": stories_count,
            "storiesOnBrandPct": on_brand_pct(penalized_by_category["stories"], stories_count),
            "repliesDMs": replies_dms,
            "repliesComments": replies_comments,
            "followerGrowthPct": follower_growth_pct,
            "followersCount": followers_count,
            "netNewFollowers": net_new_followers,
            "offBrandPosts": offenses_this_month,
        }

        months = dict(existing_months)
        months[target_month] = month_data
        kept_keys = sorted(months.keys())[-MONTHS_KEPT:]
        months = {k: months[k] for k in kept_keys}

        entry = {
            "code": s.get("code") or s["social_slug"],
            "name": name,
            "type": "nso" if s["status"] == "nso" else "open",
            "months": months,
        }
        if name in EXEMPT_GROWTH_STUDIOS:
            entry["exemptGrowth"] = True
        out_studios.append(entry)

    if skipped_no_ig:
        print(f"WARNING: {len(skipped_no_ig)} studio(s) have no Instagram data in social_insights.json yet:")
        for n in skipped_no_ig:
            print(f"  {n}")

    out = {
        "generated_at": __import__("datetime").datetime.now().isoformat(),
        "target_month": target_month,
        "studios": out_studios,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"\nStudios written: {len(out_studios)}")
    print(f"Written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
