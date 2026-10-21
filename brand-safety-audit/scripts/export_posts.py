"""
SWEAT440 Brand Safety Audit — Buffer export for manual analysis (Level 1.5)

Use this while ANTHROPIC_API_KEY billing isn't set up yet. Unlike
run_audit.py, this script only needs BUFFER_API_TOKEN — it pulls the same
posts from Buffer and writes a clean export file, but does NOT call Claude.

Weekly manual flow:
  1. Run:  python export_posts.py
  2. Open Claude.ai (any normal account, no API key needed). Save
     eval_prompt.py's EVAL_PROMPT + MANUAL_BATCH_INSTRUCTIONS as a Project's
     instructions once, so you don't have to paste it every week.
  3. Upload the generated exports/<date>-posts.json file to that chat/Project
     and ask Claude to run the audit.
  4. Save Claude's JSON response to a file.
  5. Open the dashboard (index.html) and use "Publish new cycle" to upload
     that file — see apps-script/Code.gs for how that's stored.

Once ANTHROPIC_API_KEY billing is approved, switch to run_audit.py — it reads
the exact same Buffer data (via buffer_client.py) and automates steps 2-4.
"""

import json
import os
import datetime
import pathlib

from dotenv import load_dotenv

from buffer_client import CHANNELS, DISPLAY_NAMES, pull_posts, post_type_and_asset
from eval_prompt import EVAL_PROMPT, MANUAL_BATCH_INSTRUCTIONS

load_dotenv(pathlib.Path(__file__).parent.parent / ".env")

BUFFER_TOKEN = os.environ["BUFFER_API_TOKEN"]

EXPORTS_DIR = pathlib.Path(__file__).parent.parent / "exports"


def build_export(posts, start_date, end_date):
    """Group posts by account into the export shape Claude needs — everything
    it needs to judge each post, and everything the dashboard needs per flag,
    so Claude's output can be pasted straight into data/latest.json without
    any post-processing merge step.

    `coverage` records the range actually queried (see MANUAL_BATCH_INSTRUCTIONS
    — Claude is asked to copy cycle/period/generated/coverage through
    unchanged), so the dashboard can show "reviewed through X" and pre-fill
    the next download's start date, avoiding re-analysis of the same posts."""
    by_channel = {}
    for post in posts:
        by_channel.setdefault(post["channelId"], []).append(post)

    accounts = []
    skipped = 0
    for channel_id, handle in CHANNELS.items():
        channel_posts = by_channel.get(channel_id, [])
        posts_for_review = []
        for post in channel_posts:
            post_type, asset = post_type_and_asset(post)
            if asset is None:
                skipped += 1
                continue  # Reel/Story/video/no-image post — Phase 2
            posts_for_review.append(
                {
                    "id": f"{post['id']}",
                    "date": post.get("sentAt") or post.get("dueAt", ""),
                    "postType": post_type,
                    "caption": post.get("text", ""),
                    "imageUrl": asset["source"],
                    "link": post.get("externalLink", ""),
                }
            )
        accounts.append(
            {
                "name": DISPLAY_NAMES.get(handle, handle),
                "handle": handle,
                "posts": posts_for_review,
            }
        )

    coverage_end = datetime.date.fromisoformat(end_date)
    return {
        "cycle": coverage_end.strftime("%Y-%m"),
        "period": coverage_end.strftime("%B %Y"),
        "generated": datetime.date.today().isoformat(),
        "coverage": {"from": start_date, "to": end_date},
        "note": f"{skipped} Reel(s)/Story post(s) excluded — video and 24h-ephemeral content aren't in scope yet.",
        "accounts": accounts,
    }


def write_export(export):
    EXPORTS_DIR.mkdir(exist_ok=True)
    out_path = EXPORTS_DIR / f"{export['coverage']['from']}_to_{export['coverage']['to']}-posts.json"
    out_path.write_text(json.dumps(export, indent=2))

    prompt_path = EXPORTS_DIR / "eval-prompt-for-claude.txt"
    prompt_path.write_text(EVAL_PROMPT + "\n\n" + MANUAL_BATCH_INSTRUCTIONS)

    total_posts = sum(len(a["posts"]) for a in export["accounts"])
    print(f"Wrote {out_path} ({total_posts} posts across {len(export['accounts'])} accounts)")
    print(f"Wrote {prompt_path} — paste this into a Claude.ai Project's instructions")
    print()
    print("Next: upload", out_path.name, "to that Claude.ai chat/Project and ask it")
    print("to run the audit. Save its JSON response, then publish it via the")
    print('dashboard\'s "Publish new cycle" panel.')


if __name__ == "__main__":
    import sys

    # Optional: python export_posts.py 2026-08-01 2026-08-10 — defaults to
    # the last 7 days if omitted.
    cli_start = sys.argv[1] if len(sys.argv) > 1 else None
    cli_end = sys.argv[2] if len(sys.argv) > 2 else None

    posts, start_date, end_date = pull_posts(BUFFER_TOKEN, cli_start, cli_end)
    export = build_export(posts, start_date, end_date)
    write_export(export)
