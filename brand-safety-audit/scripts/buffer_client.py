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

# Every Instagram channel connected to the LeadTeam Buffer org (43, pulled
# live via Buffer's list_channels — NOT the 9-studio pilot list this used to
# be). Facebook/LinkedIn/YouTube/TikTok/Start Page channels in the same org
# are excluded; this audit is Instagram-content-only. Re-pull list_channels
# and diff by hand if a studio's Instagram gets connected/disconnected later
# — there's no live sync, this is a point-in-time snapshot.
CHANNELS = {
    "69efb8695c4c051afae67ea7": "sweat440aventurafl",
    "68005f2c2799bb0a2395d839": "sweat440boca",
    "65392b403b0f09002f9a7e25": "sweat440brickell",
    "6a43e0fa5ab6d2f1068b8577": "sweat440burlington",
    "6a7c9930b2d9d57743641383": "sweat440charlottesouthend",
    "65411a93f8d09f1c5e0a8ce0": "sweat440chelsea",
    "6596d444c3b752215af03cdb": "sweat440coconutgrove",
    "65411a93f8d09f1c5e0a8cb3": "sweat440gables",
    "65392ba23b0f09002f9e6a89": "sweat440coralsprings",
    "69d57dc9031bfa423cdd39e5": "sweat440dallasuptown",
    "65411a93f8d09f1c5e0a8c7e": "sweat440deerfield",
    "65392d203b0f09002fad128b": "sweat440doral",
    "6a33fe6938b5579345ac3157": "sweat440dunwoody",
    "6865a743acfb098c6929f5cb": "sweat440eastchester",
    "654a51b47a6cf4d23fe20502": "sweat440fidi",
    "6a2c455b38b55793458ba0bc": "sweat440fortmyers",
    "69a1b99d4be271803d73fefe": "sweat440herriman",
    "66684e487839e9e879c69b53": "sweat440lasolas",
    "6a3d7c8d5ab6d2f106707848": "sweat440longbeachny",
    "65411a93f8d09f1c5e0a8c34": "sweat440miamilakes",
    "6a1de417c687a22dd44d053f": "sweat440middletown",
    "654a51b47a6cf4d23fe20515": "sweat440midtownmiami",
    "6596d383c3b752215ae84719": "sweat440miramar",
    "69824b1531b76c40ca27198e": "sweat440naplesmercato",
    "65411a93f8d09f1c5e0a8c88": "sweat440noda",
    "6a01f7b7090476fb990aa1a4": "sweat440north.miami",
    "655d0c147a6cf4d23ffdc750": "sweat440oceantownship",
    "6a4ecc374048344628859259": "sweat440okcrosecreek",
    "6a3d7cec5ab6d2f106707b58": "sweat440oldbridge",
    "69d57d59031bfa423cdd38be": "sweat440orlandodrphillips",
    "68757183459a7d1cfb795515": "sweat440parkslope",
    "666b25557839e9e87956e554": "sweat440pembrokepines",
    "68fa9c0b669affb4c9830806": "sweat440pinecrest",
    "68fa9b30669affb4c9830651": "sweat440prestonwood",
    "68fa9c73669affb4c983092d": "sweat440reston",
    "65411a93f8d09f1c5e0a8cc4": "sweat440sobe",
    "65411a93f8d09f1c5e0a8c27": "sweat440southmiami",
    "65392bc93b0f09002f9fe03a": "sweat440",
    "65392c0c3b0f09002fa24a51": "sweat440tomsriver",
    "65411a93f8d09f1c5e0a8c93": "sweat440uppereastside",
    "686c2b33111211c55767c82a": "sweat440wallnj",
    "6834d4ce9c617476ae72466a": "sweat440westpalmbeach",
    "664be217b9b04b20032686ba": "sweat440highland",
}

# handle -> display name shown in the dashboard
DISPLAY_NAMES = {
    "sweat440aventurafl": "Aventura",
    "sweat440boca": "Boca Raton",
    "sweat440brickell": "Brickell",
    "sweat440burlington": "Burlington",
    "sweat440charlottesouthend": "Charlotte - South End",
    "sweat440chelsea": "Chelsea",
    "sweat440coconutgrove": "Coconut Grove",
    "sweat440gables": "Coral Gables",
    "sweat440coralsprings": "Coral Springs",
    "sweat440dallasuptown": "Dallas - Uptown",
    "sweat440deerfield": "Deerfield Beach",
    "sweat440doral": "Doral",
    "sweat440dunwoody": "Dunwoody",
    "sweat440eastchester": "Eastchester",
    "sweat440fidi": "FiDi",
    "sweat440fortmyers": "Fort Myers",
    "sweat440herriman": "Herriman",
    "sweat440lasolas": "Las Olas",
    "sweat440longbeachny": "Long Beach NY",
    "sweat440miamilakes": "Miami Lakes",
    "sweat440middletown": "Middletown",
    "sweat440midtownmiami": "Midtown Miami",
    "sweat440miramar": "Miramar",
    "sweat440naplesmercato": "Naples - Mercato",
    "sweat440noda": "NoDa Charlotte",
    "sweat440north.miami": "North Miami",
    "sweat440oceantownship": "Ocean Township",
    "sweat440okcrosecreek": "OKC - Rose Creek",
    "sweat440oldbridge": "Old Bridge",
    "sweat440orlandodrphillips": "Orlando - Dr Phillips",
    "sweat440parkslope": "Park Slope",
    "sweat440pembrokepines": "Pembroke Pines",
    "sweat440pinecrest": "Pinecrest",
    "sweat440prestonwood": "Prestonwood",
    "sweat440reston": "Reston",
    "sweat440sobe": "South Beach",
    "sweat440southmiami": "South Miami",
    "sweat440": "SWEAT440 (Corporate)",
    "sweat440tomsriver": "Toms River",
    "sweat440uppereastside": "Upper East Side",
    "sweat440wallnj": "Wall Township",
    "sweat440westpalmbeach": "West Palm Beach",
    "sweat440highland": "Austin - Highland",
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


CHANNELS_QUERY = """
query GetChannels($organizationId: OrganizationId!) {
  channels(input: { organizationId: $organizationId }) {
    id
    service
    isDisconnected
  }
}
"""

PAGE_SIZE = 100
MAX_PAGES = 200  # no Worker subrequest limit here; just a runaway guard


def _query(buffer_token, query, variables):
    """POST one GraphQL query. Buffer intermittently returns UNEXPECTED_ERROR
    ("connection ... timed out") on large pages — one retry clears it. Rate
    limit is 100 requests / 15 min per token, shared with the Worker's
    dashboard Download (same token)."""
    for attempt in range(2):
        response = requests.post(
            BUFFER_API_URL,
            json={"query": query, "variables": variables},
            headers={
                "Authorization": f"Bearer {buffer_token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        if response.status_code == 429:
            mins = -(-int(response.headers.get("Retry-After") or 900) // 60)
            raise RuntimeError(f"Buffer rate limit reached (100 requests per 15 min) — try again in ~{mins} min.")
        response.raise_for_status()
        payload = response.json()
        if "errors" not in payload:
            return payload["data"]
        transient = all(
            (e.get("extensions") or {}).get("code") == "UNEXPECTED_ERROR" for e in payload["errors"]
        )
        if not transient or attempt == 1:
            raise RuntimeError(f"Buffer API errors: {payload['errors']}")


def resolve_channels(buffer_token):
    """Split CHANNELS into (available channel ids, unavailable handles).

    Buffer rejects the WHOLE posts query with FORBIDDEN ("Actor can not
    access the specified channels") if even one requested channel id no
    longer exists in the org — which is what broke exports when Zilker's
    Instagram was removed. Same logic as resolveBrandAuditChannels in
    refresh-proxy/worker.js.
    """
    data = _query(buffer_token, CHANNELS_QUERY, {"organizationId": ORG_ID})
    live = {
        c["id"] for c in data["channels"] if c["service"] == "instagram" and not c.get("isDisconnected")
    }
    available = [cid for cid in CHANNELS if cid in live]
    unavailable = [handle for cid, handle in CHANNELS.items() if cid not in live]
    return available, unavailable


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

    Channels no longer in Buffer are skipped (see resolve_channels) and
    reported on stdout.
    """
    today = datetime.date.today()
    start_date = start_date or (today - datetime.timedelta(days=days)).isoformat()
    end_date = end_date or today.isoformat()
    start_datetime = f"{start_date}T00:00:00Z"
    end_datetime = f"{end_date}T23:59:59Z"

    channel_ids, unavailable = resolve_channels(buffer_token)
    if unavailable:
        print(f"Not in Buffer anymore (skipped): {', '.join(unavailable)}")

    posts = []
    after = None
    for _ in range(MAX_PAGES):
        page = _query(
            buffer_token,
            POSTS_QUERY,
            {
                "organizationId": ORG_ID,
                "channelIds": channel_ids,
                "startDate": start_datetime,
                "endDate": end_datetime,
                "first": PAGE_SIZE,
                "after": after,
            },
        )["posts"]
        posts.extend(edge["node"] for edge in page["edges"])

        if not page["pageInfo"]["hasNextPage"]:
            return posts, start_date, end_date
        after = page["pageInfo"]["endCursor"]

    raise RuntimeError(f"More than {MAX_PAGES * PAGE_SIZE} posts in range — pick a shorter date range.")


def backfill_url(post):
    """Buffer's permanent S3 copy of a Story (or a video's cover frame). It
    outlives the Story itself, whose instagram.com/stories/... link is dead
    after 24h. Assets carry it as `thumbnail`; rebuilt here only as a
    fallback. Same as bufferBackfillUrl in refresh-proxy/worker.js."""
    return (
        "https://buffer-updates-media-backfill-bucket.s3.amazonaws.com/"
        f"{ORG_ID}/{post['channelId']}/{post['id']}_thumbnail.jpg"
    )


def post_type_and_asset(post):
    """Classify a post and pick the image to review, or (None, None) to skip.

    Returns (post_type, {"source": image url, "link": url for "View"}).
    Allow-list, not block-list — same rules as postTypeAndAsset in
    refresh-proxy/worker.js:
      - feed post ("post") → "Static" / "Carousel", cover image, Instagram link
      - Story ("story")    → "Story" (full image) or "Video story (cover)"
        (cover frame only; the video file expires in 1-2 days). Image AND
        link are Buffer's permanent S3 copy.
      - Reel, or an unrecognized type → skipped (video review isn't built yet)
    """
    kind = (post.get("metadata") or {}).get("type")
    assets = post.get("assets") or []

    if kind == "post":
        image_assets = [a for a in assets if (a.get("mimeType") or "").startswith("image/")]
        if not image_assets:
            return None, None
        post_type = "Carousel" if len(image_assets) > 1 else "Static"
        return post_type, {"source": image_assets[0]["source"], "link": post.get("externalLink", "")}

    if kind == "story" and assets:
        asset = assets[0]
        url = asset.get("thumbnail") or backfill_url(post)
        is_video = asset.get("type") == "video" or (asset.get("mimeType") or "").startswith("video/")
        return ("Video story (cover)" if is_video else "Story"), {"source": url, "link": url}

    return None, None
