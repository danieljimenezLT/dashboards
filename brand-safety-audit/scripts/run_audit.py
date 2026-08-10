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


def analyze(posts):
    """Group posts by account, evaluate each, and assemble the audit JSON."""
    by_channel = {}
    for post in posts:
        by_channel.setdefault(post["channelId"], []).append(post)

    accounts = []
    total_reviewed = 0
    for channel_id, handle in CHANNELS.items():
        channel_posts = by_channel.get(channel_id, [])
        flags = []
        for post in channel_posts:
            post_type, asset = post_type_and_asset(post)
            if asset is None:
                continue  # skipped Reel/video/no-image post — Phase 2
            total_reviewed += 1
            verdict = evaluate_post(post, asset)
            if verdict["flagged"]:
                flags.append(
                    {
                        "id": f"{post['id']}",
                        "level": verdict["level"],
                        "category": verdict["category"],
                        "confidence": verdict["confidence"],
                        "date": post.get("sentAt") or post.get("dueAt", ""),
                        "postType": post_type,
                        "visualReviewed": True,
                        "caption": post.get("text", ""),
                        "why": verdict["why"],
                        "action": verdict["action"],
                        "link": post.get("externalLink", ""),
                    }
                )
        accounts.append(
            {
                "name": DISPLAY_NAMES.get(handle, handle),
                "handle": handle,
                "posts": len(channel_posts),
                "status": "flagged" if flags else "clean",
                "flags": flags,
            }
        )

    today = datetime.date.today()
    return {
        "cycle": today.strftime("%Y-%m"),
        "period": today.strftime("%B %Y"),
        "generated": today.isoformat(),
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
    posts = pull_posts(BUFFER_TOKEN)
    audit = analyze(posts)
    write_output(audit)
