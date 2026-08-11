"""
Shared Buffer GraphQL client for the SWEAT440 Brand Safety Audit.

Used by both:
  - export_posts.py  — Buffer-only export for manual analysis in Claude.ai
  - run_audit.py      — full Level 2 automation (Buffer + Claude API)

Schema confirmed against developers.buffer.com's published examples and
reference (Get Posts For Channels, Get Posts With Assets, Get Posts With
Metrics, the Post type, PostsFiltersInput). Not yet run against this org's
real data — run `pull_posts()` alone first with a real BUFFER_API_TOKEN to
confirm field availability before relying on it in either script above.
"""

import datetime

import requests

ORG_ID = "651eb8e1f9dfd3e786474267"  # LeadTeam

# Pilot accounts (HQ excluded from audits going forward)
CHANNELS = {
    "65392ba23b0f09002f9e6a89": "sweat440coralsprings",
    "65392c0c3b0f09002fa24a51": "sweat440tomsriver",
    "65392d203b0f09002fad128b": "sweat440doral",
    "65411a93f8d09f1c5e0a8cb3": "sweat440gables",
    "654a51b47a6cf4d23fe20515": "sweat440midtownmiami",
    "65411a93f8d09f1c5e0a8c27": "sweat440southmiami",
    "65411a93f8d09f1c5e0a8c34": "sweat440miamilakes",
    "65411a93f8d09f1c5e0a8c7e": "sweat440deerfield",
    "65411a93f8d09f1c5e0a8c88": "sweat440noda",
}

# handle -> display name shown in the dashboard
DISPLAY_NAMES = {
    "sweat440coralsprings": "Coral Springs",
    "sweat440tomsriver": "Toms River",
    "sweat440doral": "Doral",
    "sweat440gables": "Coral Gables",
    "sweat440midtownmiami": "Midtown Miami",
    "sweat440southmiami": "South Miami",
    "sweat440miamilakes": "Miami Lakes",
    "sweat440deerfield": "Deerfield Beach",
    "sweat440noda": "NoDa Charlotte",
}

BUFFER_API_URL = "https://api.buffer.com"

# `metadata` is a per-network union; only the Instagram variant is queried
# here since this org's channels are all Instagram.
POSTS_QUERY = """
query GetSentPosts(
  $organizationId: OrganizationId!
  $channelIds: [ChannelId!]
  $startDate: DateTime!
  $endDate: DateTime!
  $first: Int!
  $after: String
) {
  posts(
    first: $first
    after: $after
    input: {
      organizationId: $organizationId
      filter: { status: [sent], channelIds: $channelIds, startDate: $startDate, endDate: $endDate }
      sort: [{ field: dueAt, direction: desc }]
    }
  ) {
    edges {
      node {
        id
        text
        channelId
        dueAt
        sentAt
        externalLink
        metadata {
          ... on InstagramPostMetadata {
            type
          }
        }
        assets {
          id
          type
          mimeType
          source
          thumbnail
        }
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"""


def pull_posts(buffer_token, start_date=None, end_date=None, days=7):
    """Pull sent posts from Buffer's GraphQL API for a date range.

    `start_date`/`end_date` take plain "YYYY-MM-DD" strings. If omitted,
    defaults to the last `days` days (today inclusive). Kept in sync with
    the same date-range behavior in refresh-proxy/worker.js — that Worker is
    what the dashboard's "Download posts" button actually calls; this
    function is the Level 1.5/2 (manual/API) equivalent.

    Get an API key from Buffer under Settings → API (scopes needed: at least
    `posts:read`).

    Both `startDate` AND `endDate` must be passed together — confirmed by
    testing against this org's real data: `startDate` alone (with no
    `endDate`) silently returns zero posts, even with a start date years in
    the past. Not documented; just how Buffer's filter behaves.
    """
    today = datetime.date.today()
    start_date = start_date or (today - datetime.timedelta(days=days)).isoformat()
    end_date = end_date or today.isoformat()
    start_datetime = f"{start_date}T00:00:00Z"
    end_datetime = f"{end_date}T23:59:59Z"

    posts = []
    after = None
    while True:
        response = requests.post(
            BUFFER_API_URL,
            json={
                "query": POSTS_QUERY,
                "variables": {
                    "organizationId": ORG_ID,
                    "channelIds": list(CHANNELS.keys()),
                    "startDate": start_datetime,
                    "endDate": end_datetime,
                    "first": 50,
                    "after": after,
                },
            },
            headers={
                "Authorization": f"Bearer {buffer_token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if "errors" in payload:
            raise RuntimeError(f"Buffer API errors: {payload['errors']}")

        page = payload["data"]["posts"]
        posts.extend(edge["node"] for edge in page["edges"])

        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]

    return posts, start_date, end_date


def post_type_and_asset(post):
    """Classify a post and pick its cover image, or (None, None) to skip.

    Allow-list, not block-list: only `metadata.type == "post"` (a permanent
    Instagram feed post) is evaluated. Confirmed against real org data that
    Buffer's `metadata.type` also returns "story" for Instagram Stories —
    these slip through a Reel-only exclusion because they have an image
    asset and no caption, but they're out of scope here just like Reels:
    they expire in 24h (any permalink is dead by the time a weekly review
    happens) and README's "Current scope" only covers Static + Carousel feed
    posts. Anything other than "post" (reel, story, or an unrecognized
    future type) is skipped rather than guessed at.
    """
    metadata = post.get("metadata") or {}
    if metadata.get("type") != "post":
        return None, None

    image_assets = [
        a for a in (post.get("assets") or []) if (a.get("mimeType") or "").startswith("image/")
    ]
    if not image_assets:
        return None, None

    post_type = "Carousel" if len(image_assets) > 1 else "Static"
    return post_type, image_assets[0]
