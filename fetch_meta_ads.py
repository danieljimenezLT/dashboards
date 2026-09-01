"""
fetch_meta_ads.py
─────────────────────────────────────────────────────────────
ETL Meta Ads (account-level) → meta-ads-data.json

Output shape
────────────
{
  "generated_at":   "2026-05-14T...",

  "ad_daily":       [{date, studio_code, ad_id, ad_name,
                       spend, impressions, clicks, leads, trials}, ...]
                    // rolling 90-day window; upsert on (date, ad_id) — see the
                    // key-shape comment at ad_daily_idx's definition below

  "studio_daily":   [{date, studio_code, impressions, clicks, leads, trials}, ...]
                    // studio-level, NO spend (spend lives only in the spend
                    // pipeline — see build_spend_data.py). From 2026-04-01
                    // forward, never trimmed — grows indefinitely. Recomputed
                    // from ad_daily each run for the current 90-day window;
                    // older rows pass through untouched from the existing file.

  "ad_meta":        {ad_id: {name, status, media_type, studio_code,
                              thumbnail_url, library_url}, ...}
                    // current snapshot of all known ads; merged each run
}

No campaign IDs in config. Queries the ad account directly.
"""
from __future__ import annotations

import json
import logging
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from meta_client import MetaClient, leads_of, offsite_leads_of, purchases_of, trials_of

# ── paths ────────────────────────────────────────────────────────────
REPO_ROOT      = Path(__file__).resolve().parent
OUT_PATH       = REPO_ROOT / "meta-ads-data.json"
DQ_ALERTS_PATH = REPO_ROOT / "data-quality-alerts.json"
DQ_ALERTS_MAX_AGE_DAYS = 90  # prune alerts older than this so the file doesn't grow forever
# Studio-level spend lives in meta-ads-baked.json + spend-data.json, not here —
# see build_spend_data.py, backfill_meta_month.py, import_meta_daily_csv.py.

# How many days back to re-fetch on each daily run.
# Historical data outside this window is preserved via upsert from the existing file.
DAILY_LOOKBACK_DAYS = 21

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("meta-ads-etl")


# ── classification helpers (unchanged) ──────────────────────────────

def match_studio(name: str, studios: list[dict]) -> dict | None:
    """Match an ad/adset name to a studio. Every ad follows a "{date}-{CODE}-{seq}
    - ..." naming convention (e.g. "26-FL-018-01 Open"), so the studio's own code
    is checked first — it's embedded reliably even when the studio name/keyword
    isn't (many NSO adsets say just "Open", never the studio name). Falls back to
    the keyword `match` substring for any legacy naming that doesn't embed a code.
    """
    n = name or ""
    for s in studios:
        if s.get("code") and s["code"] in n:
            return s
    n_lower = n.lower()
    for s in studios:
        if s.get("match") and s["match"].lower() in n_lower:
            return s
    return None



_VIDEO_KEYWORDS  = {"VIDEO", "REEL", "REELS", "GIF", "STORY", "STORIES"}
_STATIC_KEYWORDS = {"STATIC", "IMAGE", "PHOTO", "CAROUSEL"}


def _media_type_from_creative(creative: dict) -> str | None:
    if not creative:
        return None
    ot = (creative.get("object_type") or "").upper()
    if ot == "VIDEO": return "Video"
    if ot == "PHOTO": return "Static"
    if creative.get("video_id"):  return "Video"
    if creative.get("image_hash"): return "Static"
    oss = creative.get("object_story_spec") or {}
    if isinstance(oss, dict):
        vd = oss.get("video_data") or {}
        if isinstance(vd, dict) and (vd.get("video_id") or vd.get("image_url")):
            return "Video"
        ld = oss.get("link_data") or {}
        if isinstance(ld, dict):
            if ld.get("video_id"):                          return "Video"
            if ld.get("image_hash") or ld.get("picture"):  return "Static"
        pd = oss.get("photo_data") or {}
        if isinstance(pd, dict) and pd.get("image_hash"):  return "Static"
    afs = creative.get("asset_feed_spec") or {}
    if isinstance(afs, dict):
        if afs.get("videos"): return "Video"
        if afs.get("images"): return "Static"
    return None


def _extract_image_hash(creative: dict) -> str:
    """Return the best image_hash from a creative, trying all known locations."""
    if not creative:
        return ""
    if creative.get("image_hash"):
        return creative["image_hash"]
    oss = creative.get("object_story_spec") or {}
    if isinstance(oss, dict):
        ld = oss.get("link_data") or {}
        if isinstance(ld, dict) and ld.get("image_hash"):
            return ld["image_hash"]
        pd = oss.get("photo_data") or {}
        if isinstance(pd, dict) and pd.get("image_hash"):
            return pd["image_hash"]
    afs = creative.get("asset_feed_spec") or {}
    if isinstance(afs, dict):
        images = afs.get("images") or []
        if images and isinstance(images[0], dict) and images[0].get("hash"):
            return images[0]["hash"]
    return ""


def _extract_inner_fb_ads_url(url: str) -> str:
    """If url is a Meta emg1/external- proxy wrapping a facebook.com/ads/image/ URL,
    extract and return the inner URL — it's publicly accessible and permanent.
    Returns empty string if this URL is not that pattern."""
    if not url or ("external-" not in url and "/emg1/" not in url):
        return ""
    from urllib.parse import unquote as _unquote
    m = re.search(r'[?&]url=([^&]+)', url)
    if not m:
        return ""
    inner = _unquote(m.group(1))
    return inner if "facebook.com/ads/image/" in inner else ""


def _is_small_thumb(url: str) -> bool:
    """Return True if the URL is a tiny Meta CDN thumbnail (p64x64 or similar < 300px)."""
    if not url:
        return True
    if "external-" in url or "/emg1/" in url:
        # emg1/external- wrapping facebook.com/ads/image/ is publicly accessible
        return not bool(_extract_inner_fb_ads_url(url))
    m = re.search(r'[_/]p(\d+)x\d+', url)
    return bool(m and int(m.group(1)) < 300)


def _extract_thumb(creative: dict) -> str:
    """
    Cascade through known Meta creative shapes to find a thumbnail URL.

    Most ads in this account are dynamic creative / Advantage+, so the
    top-level thumbnail_url / image_url come back empty. The actual image
    URL is nested inside object_story_spec or asset_feed_spec.

    thumbnail_url from the batch /ids endpoint often comes back as a tiny
    64×64 CDN URL (stp=...p64x64...). We skip those and prefer
    asset_feed_spec.images[0].url which is the full-resolution image.
    Fall back to the small thumbnail only as a last resort so something
    always shows.
    """
    if not creative:
        return ""

    top_thumb = creative.get("thumbnail_url") or ""
    top_img   = creative.get("image_url") or ""

    # 1) Top-level full-size (large thumbnail_url or image_url)
    if top_thumb and not _is_small_thumb(top_thumb):
        return _extract_inner_fb_ads_url(top_thumb) or top_thumb
    if top_img and not _is_small_thumb(top_img):
        return _extract_inner_fb_ads_url(top_img) or top_img

    # 2) object_story_spec — classic page-post ads
    oss = creative.get("object_story_spec") or {}
    if isinstance(oss, dict):
        vd = oss.get("video_data") or {}
        if isinstance(vd, dict) and vd.get("image_url"):
            return vd["image_url"]
        ld = oss.get("link_data") or {}
        if isinstance(ld, dict) and ld.get("picture"):
            return ld["picture"]

    # 3) asset_feed_spec — dynamic creative / Advantage+ (full-res image)
    afs = creative.get("asset_feed_spec") or {}
    if isinstance(afs, dict):
        images = afs.get("images") or []
        if isinstance(images, list) and images:
            first = images[0] if isinstance(images[0], dict) else {}
            if first.get("url"):           return first["url"]
            if first.get("permalink_url"): return first["permalink_url"]
        videos = afs.get("videos") or []
        if isinstance(videos, list) and videos:
            first = videos[0] if isinstance(videos[0], dict) else {}
            if first.get("thumbnail_url"): return first["thumbnail_url"]
            if first.get("url"):           return first["url"]

    # 4) Fallback: return the small thumbnail — better than nothing
    return top_thumb or top_img


def _media_type_from_name(ad_name: str) -> str:
    if not ad_name: return "Other"
    words = {w.upper() for w in re.findall(r"\w+", ad_name)}
    if words & _VIDEO_KEYWORDS:  return "Video"
    if words & _STATIC_KEYWORDS: return "Static"
    return "Other"


def safe_float(x, default=0.0):
    try:    return float(x)
    except: return default


# ── quarter helpers ───────────────────────────────────────────────────

def current_quarter_bounds(today: date) -> tuple[str, str]:
    """Return (start, end) ISO strings for the current quarter."""
    q = (today.month - 1) // 3
    q_start = date(today.year, q * 3 + 1, 1)
    q_end_month = q * 3 + 3
    q_end = date(today.year, q_end_month,
                 [31,28,31,30,31,30,31,31,30,31,30,31][q_end_month - 1])
    # leap year adjustment
    if q_end_month == 2 and (today.year % 4 == 0 and (today.year % 100 != 0 or today.year % 400 == 0)):
        q_end = date(today.year, 2, 29)
    return q_start.isoformat(), q_end.isoformat()


def previous_quarter_bounds(today: date) -> tuple[str, str]:
    q = (today.month - 1) // 3
    if q == 0:
        pq_year = today.year - 1; pq = 3
    else:
        pq_year = today.year;     pq = q - 1
    pq_start = date(pq_year, pq * 3 + 1, 1)
    pq_end_month = pq * 3 + 3
    pq_end = date(pq_year, pq_end_month,
                  [31,28,31,30,31,30,31,31,30,31,30,31][pq_end_month - 1])
    if pq_end_month == 2 and (pq_year % 4 == 0 and (pq_year % 100 != 0 or pq_year % 400 == 0)):
        pq_end = date(pq_year, 2, 29)
    return pq_start.isoformat(), min(pq_end.isoformat(), date.today().isoformat())


# ── main ETL ─────────────────────────────────────────────────────────

def run(start_override: str | None = None, end_override: str | None = None):
    import studios as studios_registry

    ad_account  = studios_registry.defaults()["meta_ad_account_id"]  # e.g. "act_1553887681409034"
    dedicated_accounts = studios_registry.meta_dedicated_accounts()
    dedicated_codes    = {d["code"] for d in dedicated_accounts}
    # Studios with their own ad account are excluded from shared-account matching —
    # their spend now lives entirely in their dedicated account (see step 3b below).
    studios_cfg = [s for s in studios_registry.meta_studio_rows() if s.get("code") not in dedicated_codes]

    today     = date.today()
    today_iso = today.isoformat()

    # ── date windows ─────────────────────────────────────────────────
    # Fetch only the last DAILY_LOOKBACK_DAYS days each run.
    # Older data is preserved via upsert from the existing file.
    # end_override lets a caller re-verify a bounded historical window (e.g. a
    # monthly attribution audit chunking through the last 90 days) without
    # re-fetching all the way to today every time — see monthly-attribution-
    # audit.yml. Only meaningful together with start_override.
    daily_start = start_override or (today - timedelta(days=DAILY_LOOKBACK_DAYS)).isoformat()
    daily_end   = end_override or today_iso

    # ── load existing output (for upsert + baked monthly) ────────────
    existing: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    existing_ad_daily = existing.get("ad_daily", [])
    existing_ad_meta  = existing.get("ad_meta",  {})

    existing_dq_alerts: list[dict] = []
    if DQ_ALERTS_PATH.exists():
        try:
            existing_dq_alerts = json.loads(DQ_ALERTS_PATH.read_text(encoding="utf-8")).get("alerts", [])
        except Exception:
            pass

    meta = MetaClient()

    # ── 1. Fetch daily ad-level insights (account level) ─────────────
    # Returns one row per (ad_id, date) with adset_name for studio matching.
    log.info(f"Fetching daily ad insights [{daily_start} → {daily_end}] from {ad_account} ...")
    try:
        raw_daily_ad = meta.get_insights(
            ad_account,
            level="ad",
            date_start=daily_start,
            date_end=daily_end,
            time_increment=1,
        )
        log.info(f"  {len(raw_daily_ad)} ad×day rows")
    except Exception as e:
        log.exception(f"❌ Daily ad insights failed: {e}")
        sys.exit(1)

    # ── 2. Fetch ad metadata (status, creative) ───────────────────────
    # Get all active/paused ads on the account for thumbnails + status.
    log.info("Fetching ad list from account ...")
    try:
        all_ads = meta.list_ads_for_account(ad_account)
        log.info(f"  {len(all_ads)} ads")
    except Exception as e:
        log.warning(f"  list_ads_for_account failed: {e} — using existing ad_meta")
        all_ads = []

    # Batch-fetch creatives
    cids_per_ad: dict[str, str] = {}
    for ad in all_ads:
        ad_id = ad.get("id")
        if not ad_id: continue
        cid = (ad.get("creative") or {}).get("id")
        if cid: cids_per_ad[ad_id] = cid

    unique_cids = list({c for c in cids_per_ad.values()})
    if unique_cids:
        log.info(f"Fetching {len(unique_cids)} creatives in batch ...")
        try:
            cdetails = meta.get_creatives_by_ids(unique_cids)
        except Exception as e:
            log.warning(f"  creatives batch failed: {e}")
            cdetails = {}
    else:
        cdetails = {}

    status_by_ad = {ad["id"]: ad.get("status", "UNKNOWN") for ad in all_ads if ad.get("id")}

    # Collect preview_shareable_link and Ads Library URL per ad.
    # preview_shareable_link works for all ad types (including Lead Gen forms).
    # library_url is kept as a fallback using effective_object_story_id.
    preview_link_by_ad: dict[str, str] = {}
    library_id_by_ad: dict[str, str] = {}
    post_url_by_ad: dict[str, str] = {}
    for ad in all_ads:
        ad_id = ad.get("id")
        if not ad_id: continue
        psl = ad.get("preview_shareable_link") or ""
        if psl:
            preview_link_by_ad[ad_id] = psl
        eosi = ad.get("effective_object_story_id") or ""
        if "_" in eosi:
            page_id, post_id = eosi.split("_", 1)
            library_id_by_ad[ad_id] = post_id
            post_url_by_ad[ad_id] = f"https://www.facebook.com/{page_id}/posts/{post_id}/"
        elif eosi:
            library_id_by_ad[ad_id] = eosi

    # ── 3. Process daily ad rows → ad_daily ──────────────────────────
    # Index bucket keyed by (date, ad_id) — deliberately NOT studio_code. An ad
    # belongs to exactly one studio on a given day; if a re-fetch resolves it to
    # a different studio than before (adset renamed/reassigned, or a one-off bad
    # match from Meta's API), this key shape means the new attribution REPLACES
    # the old one instead of sitting alongside it. Keying by (date, studio_code,
    # ad_id) previously let both linger forever once written, double-counting
    # that ad's spend across two studios for as long as both entries existed.
    ad_daily_idx: dict[tuple, dict] = {}

    # Seed from existing data (will be overwritten for dates we're refetching)
    for r in existing_ad_daily:
        k = (r["date"], r["ad_id"])
        ad_daily_idx[k] = r

    # Track ad name per ad_id (for ad_meta, in case list_ads_for_account failed)
    ad_name_seen: dict[str, str] = {}
    ad_studio_seen: dict[str, str] = {}

    rows_written = 0
    rows_skipped = 0
    rows_reattributed = 0
    dq_alerts_new: list[dict] = []  # new reattributions found THIS run; merged into DQ_ALERTS_PATH below

    for row in raw_daily_ad:
        ad_id     = row.get("ad_id")
        adset_name = row.get("adset_name", "")
        d          = row.get("date_start")
        if not ad_id or not d:
            rows_skipped += 1
            continue

        studio = match_studio(adset_name, studios_cfg)
        if not studio:
            rows_skipped += 1
            continue

        sc       = studio["code"]
        ad_name  = row.get("ad_name", "")
        spend    = round(safe_float(row.get("spend")), 2)
        impr     = int(safe_float(row.get("impressions")))
        clicks   = int(safe_float(row.get("clicks")))
        leads          = leads_of(row)
        offsite_leads  = offsite_leads_of(row)
        trials         = trials_of(row)

        ad_name_seen[ad_id]  = ad_name
        ad_studio_seen[ad_id] = sc

        # ad_daily upsert — (date, ad_id) only, see key-shape comment above.
        ak = (d, ad_id)
        prev = ad_daily_idx.get(ak)
        if prev and prev.get("studio_code") and prev["studio_code"] != sc:
            log.warning(
                f"  ⚠ ad {ad_id} ({ad_name!r}) on {d} matched to {sc}, was "
                f"{prev['studio_code']} — adset renamed/reassigned, or a stale/"
                f"inconsistent match. Verify against the ad account if unexpected."
            )
            rows_reattributed += 1
            dq_alerts_new.append({
                "id":           f"{d}:{ad_id}:{prev['studio_code']}:{sc}",
                "type":         "meta_ad_reattribution",
                "date":         d,
                "ad_id":        ad_id,
                "ad_name":      ad_name,
                "from_studio":  prev["studio_code"],
                "to_studio":    sc,
                "detected_at":  datetime.now(timezone.utc).isoformat(),
            })
        ad_daily_idx[ak] = {
            "date":          d,
            "studio_code":   sc,
            "ad_id":         ad_id,
            "ad_name":       ad_name,
            "spend":         spend,
            "impressions":   impr,
            "clicks":        clicks,
            "leads":         leads,
            "offsite_leads": offsite_leads,
            "trials":        trials,
        }

        rows_written += 1

    log.info(f"  processed: {rows_written} matched, {rows_skipped} skipped (no studio match)")
    if rows_reattributed:
        log.warning(f"  ⚠ {rows_reattributed} (date, ad_id) rows changed studio attribution this run — see warnings above")
    log.info(f"  ad_daily:  {len(ad_daily_idx)} total (date×ad) rows")

    # ── 3b. Dedicated per-studio accounts (account-level spend only) ──
    # Studios that migrated off the shared account onto their own — 100% of an
    # account's spend belongs to that studio, so no name-matching is needed.
    # Account-level only for now (no ad list/creatives), one synthetic row per
    # day keyed by a per-studio ad_id ("ACCOUNT_TOTAL:<code>", not the shared
    # literal "ACCOUNT_TOTAL" — ad_daily_idx is now keyed by (date, ad_id) only,
    # so a shared literal would let one dedicated studio's row overwrite
    # another's for the same day) so build_spend_data.py's per-studio_code sum
    # picks it up like any other ad_daily row.
    for studio in dedicated_accounts:
        code, name, account_id = studio["code"], studio["name"], studio["account_id"]
        log.info(f"Fetching account-level daily insights [{daily_start} → {daily_end}] "
                 f"from {account_id} ({name}) ...")
        try:
            raw_account_daily = meta.get_insights(
                account_id,
                level="account",
                date_start=daily_start,
                date_end=daily_end,
                time_increment=1,
            )
            log.info(f"  {len(raw_account_daily)} day rows")
        except Exception as e:
            log.warning(f"  ❌ account-level insights failed for {name} ({account_id}): {e}")
            continue

        for row in raw_account_daily:
            d = row.get("date_start")
            if not d:
                continue
            account_ad_id = f"ACCOUNT_TOTAL:{code}"
            ad_daily_idx[(d, account_ad_id)] = {
                "date":          d,
                "studio_code":   code,
                "ad_id":         account_ad_id,
                "ad_name":       "(account-level total)",
                "spend":         round(safe_float(row.get("spend")), 2),
                "impressions":   int(safe_float(row.get("impressions"))),
                "clicks":        int(safe_float(row.get("clicks"))),
                "leads":         leads_of(row),
                "offsite_leads": offsite_leads_of(row),
                "trials":        trials_of(row),
            }

    # ── 4. Build ad_meta snapshot ─────────────────────────────────────
    # Start from existing, then update with fresh data
    ad_meta: dict[str, dict] = dict(existing_ad_meta)

    thumb_hits = 0
    thumb_missing = 0
    for ad_id in set(list(ad_name_seen.keys()) + list(status_by_ad.keys())):
        creative   = cdetails.get(cids_per_ad.get(ad_id, ""), {})
        media_type = _media_type_from_creative(creative) or _media_type_from_name(ad_name_seen.get(ad_id, ""))
        image_url  = _extract_thumb(creative)   # best full-res URL available
        native_thumb = (creative.get("thumbnail_url") or creative.get("image_url") or "")

        if image_url:
            thumb_hits += 1
        elif creative:
            thumb_missing += 1

        ad_meta[ad_id] = {
            "name":          ad_name_seen.get(ad_id) or (ad_meta.get(ad_id) or {}).get("name", ""),
            "status":        status_by_ad.get(ad_id, (ad_meta.get(ad_id) or {}).get("status", "UNKNOWN")),
            "media_type":    media_type,
            "studio_code":   ad_studio_seen.get(ad_id) or (ad_meta.get(ad_id) or {}).get("studio_code"),
            "thumbnail_url": (_extract_inner_fb_ads_url(native_thumb)
                              or _extract_inner_fb_ads_url(image_url)
                              or (image_url if "facebook.com/ads/image/" in image_url else "")
                              or native_thumb or image_url
                              or (ad_meta.get(ad_id) or {}).get("thumbnail_url", "")),
            "image_url":     (_extract_inner_fb_ads_url(image_url)
                              or _extract_inner_fb_ads_url(native_thumb)
                              or (image_url if "facebook.com/ads/image/" in image_url else "")
                              or image_url or native_thumb
                              or (ad_meta.get(ad_id) or {}).get("image_url", "")),
            "preview_link":  preview_link_by_ad.get(ad_id) or (ad_meta.get(ad_id) or {}).get("preview_link", ""),
            "post_url":      post_url_by_ad.get(ad_id) or (ad_meta.get(ad_id) or {}).get("post_url", ""),
            "library_url":   f"https://www.facebook.com/ads/library/?id={library_id_by_ad.get(ad_id, ad_id)}&country=US",
            "video_id":      creative.get("video_id", "") if creative else "",
            "image_hash":    _extract_image_hash(creative) if creative else "",
            "fetched_at":    daily_end,
        }

    log.info(f"  ad_meta: {len(ad_meta)} ads (thumb hits {thumb_hits}, creatives w/ no thumb {thumb_missing})")

    # ── 4b. Build studio_daily: non-spend engagement, since 2026-04-01,
    # never trimmed. Recomputed fresh from ad_daily_idx (this run's full
    # 90-day window) each time; existing rows outside that window are
    # carried forward untouched, so the table grows forever instead of
    # rolling off with ad_daily. Deliberately excludes spend — spend has
    # its own lineage (meta-ads-baked.json → spend-data.json).
    STUDIO_DAILY_FLOOR = "2026-04-01"
    studio_daily_idx: dict[tuple, dict] = {
        (r["date"], r["studio_code"]): r
        for r in existing.get("studio_daily", [])
        if r.get("date", "") >= STUDIO_DAILY_FLOOR
    }

    studio_daily_fresh: dict[tuple, dict] = {}
    for r in ad_daily_idx.values():
        if r["date"] < STUDIO_DAILY_FLOOR:
            continue
        k = (r["date"], r["studio_code"])
        if k not in studio_daily_fresh:
            studio_daily_fresh[k] = {
                "date": r["date"], "studio_code": r["studio_code"],
                "impressions": 0, "clicks": 0, "leads": 0, "offsite_leads": 0, "trials": 0,
            }
        b = studio_daily_fresh[k]
        b["impressions"]   += r["impressions"]
        b["clicks"]        += r["clicks"]
        b["leads"]         += r["leads"]
        b["offsite_leads"] += r.get("offsite_leads", 0)
        b["trials"]        += r["trials"]

    studio_daily_idx.update(studio_daily_fresh)  # this run's window wins; older rows pass through
    studio_daily_out = sorted(studio_daily_idx.values(), key=lambda r: (r["date"], r["studio_code"]))
    log.info(f"  studio_daily: {len(studio_daily_out)} total (date×studio) rows, floor {STUDIO_DAILY_FLOOR}")

    # ── 5. Sort, trim to 90-day rolling window, and write ────────────
    ad_daily_out = sorted(
        ad_daily_idx.values(),
        key=lambda r: (r["date"], r["studio_code"], r["ad_id"])
    )

    # Rolling 90-day window — ad-level detail moved to spend-data.json's daily
    # rows once trimmed (studio-level spend lives in meta-ads-baked.json /
    # build_spend_data.py instead, not here).
    cutoff_90 = (today - timedelta(days=90)).isoformat()
    ad_daily_out = [r for r in ad_daily_out if r["date"] >= cutoff_90]

    output = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "ad_daily":       ad_daily_out,
        "studio_daily":   studio_daily_out,
        "ad_meta":        ad_meta,
    }

    OUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(
        f"Wrote {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes) — "
        f"{len(ad_daily_out)} ad_daily, {len(studio_daily_out)} studio_daily, {len(ad_meta)} ad_meta"
    )

    # Data quality alerts — accumulated across runs (keyed by id, so a re-run
    # never adds the same reattribution twice), pruned past DQ_ALERTS_MAX_AGE_DAYS
    # so the file doesn't grow forever once something's had plenty of time to be
    # seen. home.html reads this and shows a banner for admin users; dismissal
    # is tracked client-side (localStorage), not here — this file only tracks
    # what's been *found*, not what's been *seen*.
    dq_alerts_by_id = {a["id"]: a for a in existing_dq_alerts if a.get("id")}
    for a in dq_alerts_new:
        dq_alerts_by_id[a["id"]] = a
    dq_cutoff = (datetime.now(timezone.utc) - timedelta(days=DQ_ALERTS_MAX_AGE_DAYS)).isoformat()
    dq_alerts_out = sorted(
        (a for a in dq_alerts_by_id.values() if a.get("detected_at", "") >= dq_cutoff),
        key=lambda a: a["detected_at"],
        reverse=True,
    )
    DQ_ALERTS_PATH.write_text(
        json.dumps({"generated_at": output["generated_at"], "alerts": dq_alerts_out}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    if dq_alerts_new:
        log.warning(f"  ⚠ {len(dq_alerts_new)} new alert(s) this run, {len(dq_alerts_out)} total in {DQ_ALERTS_PATH.name}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=None,
                        help="Override fetch start date (YYYY-MM-DD). Default: last DAILY_LOOKBACK_DAYS days.")
    parser.add_argument("--end", default=None,
                        help="Override fetch end date (YYYY-MM-DD). Default: today. Only meaningful with --start "
                             "(e.g. a bounded historical chunk for a backfill/audit run).")
    args = parser.parse_args()
    try:
        run(start_override=args.start, end_override=args.end)
    except Exception as e:
        log.exception(f"❌ ETL failed: {e}")
        sys.exit(1)
