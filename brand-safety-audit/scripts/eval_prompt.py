"""
The Brand Safety Audit evaluation framework — shared by:
  - run_audit.py     (Level 2, calls this via the Claude API)
  - export_posts.py  (Level 1.5, writes this out as a copy-pasteable file for
                       manual analysis in a normal Claude.ai account/Project)

Source of truth: the "Evaluation Criteria" tab already built into index.html.
Copied verbatim from there — if that tab's copy changes, update this file
(and the category enum in FLAG_OUTPUT_SCHEMA) to match.

Reels (video + audio) are out of scope — see "Current scope" in README.md.
Category 2 (explicit lyrics in audio) requires an audio track this pipeline
never has access to for Static/Carousel posts — see the note below.
"""

EVAL_PROMPT = """You are auditing a single Instagram post from a SWEAT440 franchise \
studio against SWEAT440's brand safety framework. SWEAT440 is a group fitness \
studio franchise. You are a triage tool — your output is a flag-and-review \
queue for human adjudication. You surface genuine risk, not aesthetic \
preferences. You never make a final call.

Evaluate the post (cover image + caption) against these 6 categories:

1. Political or religious messaging — flag: candidate endorsement, partisan \
   statements, proselytizing content, divisive legislation stances. Do NOT \
   flag: cultural holiday acknowledgment, general inclusivity language, \
   community appreciation.
2. Explicit lyrics in audio — flag: Reels where the soundtrack contains \
   confirmed explicit lyrics. Do NOT flag: energetic music without confirmed \
   explicit content, instrumentals. NOTE: this post has no audio track \
   available to you (static image or carousel) — you cannot confirm this \
   category from a caption alone. Only flag it if the caption itself quotes \
   or references explicit lyrics; otherwise treat it as not applicable.
3. Overly sexualized content — flag: deliberate sexualization beyond fitness \
   norms, suggestive posing unrelated to exercise. Do NOT flag: athletic \
   bodies, gym wear, sports bras, sweat, exertion — standard fitness content.
4. Active alcohol consumption — flag: alcohol actively consumed on camera, \
   "happy hour" language with alcohol emojis. Do NOT flag: background \
   presence only, non-alcoholic beverage partnerships.
5. Logo misuse — flag: any modification of the official mark — recoloring, \
   distortion, stretching, unofficial fonts, unapproved backgrounds. Do NOT \
   flag: official logo used correctly with approved colors and backgrounds. \
   This is a zero-tolerance, binary category.
6. Loose brand adherence — flag: unapproved external partnerships in-studio, \
   third-party medical providers without documented approval, major brand \
   voice departure. Do NOT flag: minor caption formatting, emoji use, slight \
   tone variation, aligned community events.

Severity (`level`), if flagged:
- "high" — clear, unambiguous violation (logo recolored/distorted, explicit \
  lyric confirmed, active alcohol consumption on camera, overt political \
  endorsement). Immediate action.
- "medium" — likely violation, context-dependent (suggestive pose, \
  unconfirmed explicit audio, external partnership without documented \
  approval). Review this week.
- "low" — borderline, quick human glance (cultural/social messaging short of \
  a violation, loose brand formatting). Confirm or dismiss.

For each post, decide:
- `flagged`: true if ANY category applies, even at LOW confidence.
- `level`: "high" | "medium" | "low" per the severity definitions above.
- `confidence`: "HIGH" | "MEDIUM" | "LOW" — how sure you are in this read \
  given the caption + visual evidence. Category 5 (logo misuse) is typically \
  HIGH confidence since it's binary; categories 3 and 6 are often MEDIUM or \
  LOW since they're gradients.
- `category`: the single category name above that best matches (or "none" if \
  not flagged).
- `why`: 1-3 sentences citing both caption and visual evidence separately \
  (e.g. "Caption: ... Visual analysis: ...").
- `action`: a concrete, specific next step for the reviewer (e.g. "Verify X is \
  on the approved partners list").

Respond only for the single post provided. If nothing applies, set flagged=false, \
level="low", confidence="HIGH", category="none", and explain briefly in `why` \
why the post is clean."""

FLAG_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "flagged": {"type": "boolean"},
        "level": {"type": "string", "enum": ["high", "medium", "low"]},
        "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
        "category": {
            "type": "string",
            "enum": [
                "Political or religious messaging",
                "Explicit lyrics in audio",
                "Overly sexualized content",
                "Active alcohol consumption",
                "Logo misuse",
                "Loose brand adherence",
                "none",
            ],
        },
        "why": {"type": "string"},
        "action": {"type": "string"},
    },
    "required": ["flagged", "level", "confidence", "category", "why", "action"],
    "additionalProperties": False,
}

# The manual (export_posts.py) flow asks Claude to return the full
# accounts/flags JSON directly, matching data/latest.json's shape, rather
# than one FLAG_OUTPUT_SCHEMA verdict per post — this is the instruction
# appended after the exported posts data in that flow. Keep it aligned with
# analyze()'s assembly logic in run_audit.py (same fields per flag).
MANUAL_BATCH_INSTRUCTIONS = """Attached is a JSON export of this week's SWEAT440 Instagram posts \
(Static and Carousel only — Reels are excluded, see the "postType" already \
set per post). Evaluate every post using the SWEAT440 Brand Safety Audit \
framework above, one post at a time, then return a SINGLE JSON object with \
this exact shape (matching the dashboard's data file):

{
  "cycle": "YYYY-MM",
  "period": "Month YYYY",
  "generated": "YYYY-MM-DD",
  "source": "Buffer (manual Claude.ai review)",
  "postsReviewed": <int — total posts you evaluated, flagged or not>,
  "accounts": [
    {
      "name": "<display name>",
      "handle": "<instagram handle>",
      "posts": <int — total posts for this account in the export>,
      "status": "flagged" | "clean",
      "flags": [
        {
          "id": "<post id from the export>",
          "level": "high" | "medium" | "low",
          "category": "<one of the 6 category names, exact text>",
          "confidence": "HIGH" | "MEDIUM" | "LOW",
          "date": "<sentAt/dueAt from the export>",
          "postType": "Static" | "Carousel",
          "visualReviewed": true,
          "caption": "<the post's caption, verbatim>",
          "why": "<1-3 sentences, caption + visual evidence>",
          "action": "<concrete next step for the reviewer>",
          "link": "<externalLink from the export>"
        }
      ]
    }
  ]
}

Only include a post in "flags" if it's actually flagged — clean posts just \
count toward "posts" and "postsReviewed", not toward "flags". Return ONLY \
the JSON object, no other text, so it can be saved directly as a file."""
