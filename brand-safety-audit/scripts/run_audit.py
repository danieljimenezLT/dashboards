"""
SWEAT440 Brand Safety Audit — automated runner (Level 2, needs Anthropic API billing)

Called by .github/workflows/audit-refresh.yml every Monday.

Pipeline:
 1. Pull last 7 days of sent posts from Buffer (GraphQL API) — see buffer_client.py
 2. For each Static/Carousel post, send the cover image + caption to Claude
    with the Brand Safety Audit 6-category framework
 3. Write brand-safety-audit/data/latest.json (+ dated archive copy)

STATUS: not yet run against real data — get a real BUFFER_API_TOKEN and
ANTHROPIC_API_KEY (see .env.example) and run once manually before enabling
the scheduled workflow.

The 6 categories in eval_prompt.py are taken verbatim from the "Evaluation
Criteria" tab already built into index.html — that tab is the source of
truth for this framework, not a reconstruction. If that tab's copy changes,
update eval_prompt.py to match.

Reels (video + audio) are explicitly out of scope here — see "Current scope"
in README.md. This script only evaluates Static and Carousel-cover posts;
Instagram Reels are detected via `metadata.type == "reel"` and skipped. Note
category 2 below (explicit lyrics in audio) requires an audio track this
pipeline never has access to — see the note in EVAL_PROMPT.

Until ANTHROPIC_API_KEY billing is set up, use export_posts.py instead: it
pulls the same Buffer data with no Claude API dependency, for manual analysis
in a normal Claude.ai account, then publish the result via the dashboard's
"Publish new cycle" panel (see index.html + apps-script/Code.gs).
"""

import json
import os
import datetime
import pathlib

from dotenv import load_dotenv
from anthropic import Anthropic

from buffer_client import CHANNELS, DISPLAY_NAMES, pull_posts, post_type_and_asset
from eval_prompt import EVAL_PROMPT, FLAG_OUTPUT_SCHEMA

# Local dev: reads brand-safety-audit/.env (see .env.example). In GitHub
# Actions, BUFFER_API_TOKEN / ANTHROPIC_API_KEY come from repo secrets instead
# and this is a no-op (no .env file present there).
load_dotenv(pathlib.Path(__file__).parent.parent / ".env")

BUFFER_TOKEN = os.environ["BUFFER_API_TOKEN"]

# Anthropic() reads ANTHROPIC_API_KEY from the environment.
client = Anthropic()
MODEL = "claude-opus-4-8"

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"


def evaluate_post(post, asset):
    """Send one post's cover image + caption to Claude for brand-safety evaluation."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        thinking={"type": "adaptive"},
        output_config={
            "effort": "medium",
            "format": {"type": "json_schema", "schema": FLAG_OUTPUT_SCHEMA},
        },
        system=EVAL_PROMPT,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {"type": "url", "url": asset["source"]},
                    },
                    {
                        "type": "text",
                        "text": f"Caption: {post.get('text', '')}",
                    },
                ],
            }
        ],
    )
    text_block = next(b for b in response.content if b.type == "text")
    return json.loads(text_block.text)


def analyze(posts, start_date, end_date):
    """Group posts by account, evaluate each, and assemble the audit JSON.

    `start_date`/`end_date` ("YYYY-MM-DD") are the range actually queried —
    same coverage/cycle-naming convention as handleExportBrandAuditPosts in
    refresh-proxy/worker.js: cycle/period key off the END of the range, not
    "today", and `coverage` records the range so the dashboard can show
    "reviewed through X" and avoid re-pulling already-covered dates.
    """
    by_channel = {}
    for post in posts:
        by_channel.setdefault(post["channelId"], []).append(post)

    accounts = []
    total_reviewed = 0
    for channel_id, handle in CHANNELS.items():
        channel_posts = by_channel.get(channel_id, [])
        flags = []
        post_log = []
        for post in channel_posts:
            post_type, asset = post_type_and_asset(post)
            if asset is None:
                continue  # skipped Reel/video/no-image post — Phase 2
            total_reviewed += 1
            verdict = evaluate_post(post, asset)
            post_date = post.get("sentAt") or post.get("dueAt", "")
            caption = post.get("text", "")
            link = post.get("externalLink", "")
            if verdict["flagged"]:
                flags.append(
                    {
                        "id": f"{post['id']}",
                        "level": verdict["level"],
                        "category": verdict["category"],
                        "confidence": verdict["confidence"],
                        "date": post_date,
                        "postType": post_type,
                        "visualReviewed": True,
                        "caption": caption,
                        "why": verdict["why"],
                        "action": verdict["action"],
                        "link": link,
                    }
                )
            # postLog covers EVERY post (flagged or not) — unlike "flags", it's
            # what lets the dashboard filter by the post's own publish date
            # across clean posts too, not just flagged ones.
            post_log.append(
                {
                    "id": f"{post['id']}",
                    "date": post_date,
                    "postType": post_type,
                    "caption": caption,
                    "link": link,
                    "flagged": verdict["flagged"],
                    "level": verdict["level"] if verdict["flagged"] else None,
                    "category": verdict["category"] if verdict["flagged"] else None,
                }
            )
        accounts.append(
            {
                "name": DISPLAY_NAMES.get(handle, handle),
                "handle": handle,
                "posts": len(channel_posts),
                "status": "flagged" if flags else "clean",
                "flags": flags,
                "postLog": post_log,
            }
        )

    coverage_end = datetime.date.fromisoformat(end_date)
    return {
        "cycle": coverage_end.strftime("%Y-%m"),
        "period": coverage_end.strftime("%B %Y"),
        "generated": datetime.date.today().isoformat(),
        "coverage": {"from": start_date, "to": end_date},
        "source": "Buffer (automated GraphQL pull)",
        "postsReviewed": total_reviewed,
        "accounts": accounts,
    }


def write_output(audit):
    DATA_DIR.mkdir(exist_ok=True)
    latest = DATA_DIR / "latest.json"
    latest.write_text(json.dumps(audit, indent=2))
    archive = DATA_DIR / f"{audit['cycle']}.json"
    archive.write_text(json.dumps(audit, indent=2))
    print(f"Wrote {latest} and {archive}")


if __name__ == "__main__":
    import sys

    # Optional: python run_audit.py 2026-08-01 2026-08-10 — defaults to the
    # last 7 days if omitted.
    cli_start = sys.argv[1] if len(sys.argv) > 1 else None
    cli_end = sys.argv[2] if len(sys.argv) > 2 else None

    posts, start_date, end_date = pull_posts(BUFFER_TOKEN, cli_start, cli_end)
    audit = analyze(posts, start_date, end_date)
    write_output(audit)
