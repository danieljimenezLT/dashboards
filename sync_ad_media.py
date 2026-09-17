#!/usr/bin/env python3
"""
sync_ad_media.py — Download Meta ad thumbnails/videos → Google Drive.

Run immediately after fetch_meta_ads.py (OS) or fetch_all_marketing.py (NSO)
while Meta HMAC tokens are still fresh (same GitHub Actions run).

Reads:
  meta-ads-data.json            (OS — ad_meta keyed by ad_id)
  nso-dashboard/nso_ad_meta.json (NSO — ad_meta keyed by ad name)

Writes:
  Both JSON files updated with drive_thumb_url / drive_video_url fields.
  os_media_cache.json   — persists Drive file IDs for OS across runs
  nso-dashboard/nso_media_cache.json — same for NSO

Usage:
    python sync_ad_media.py --dashboard os    # OS only
    python sync_ad_media.py --dashboard nso   # NSO only
    python sync_ad_media.py                   # both

    --max-new-videos N   cap new video uploads per run (default 50)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest

load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).parent / "nso-dashboard" / ".env", override=False)

log = logging.getLogger("sync-ad-media")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

REPO_ROOT = Path(__file__).resolve().parent

try:
    sys.path.insert(0, str(REPO_ROOT))
    import studios as _studios_mod
    _STUDIO_CODE_TO_ACCOUNT: dict[str, str] = {
        d["code"]: d["account_id"]
        for d in _studios_mod.meta_dedicated_accounts()
    }
except Exception:
    _STUDIO_CODE_TO_ACCOUNT = {}

OS_JSON       = REPO_ROOT / "meta-ads-data.json"
NSO_JSON      = REPO_ROOT / "nso-dashboard" / "nso_ad_meta.json"
OS_CACHE      = REPO_ROOT / "os_media_cache.json"
NSO_CACHE     = REPO_ROOT / "nso-dashboard" / "nso_media_cache.json"

OS_RETENTION_DAYS = 90
# Shared Drive folder — shared with the service account at this ID.
# Subfolders "os/" and "nso/" are created automatically inside it.
DRIVE_ROOT_FOLDER_ID = "1XZPoMSawhnnF8YIJHkeGywe5VeNNCROd"

DRIVE_BASE   = "https://www.googleapis.com/drive/v3"
DRIVE_UPLOAD = "https://www.googleapis.com/upload/drive/v3"


# ── Google Drive client (raw REST, no google-api-python-client needed) ────────

class DriveClient:
    def __init__(self, sa_file: str):
        self._creds = service_account.Credentials.from_service_account_file(
            sa_file, scopes=["https://www.googleapis.com/auth/drive"]
        )

    def _token(self) -> str:
        if not self._creds.valid:
            self._creds.refresh(GoogleAuthRequest())
        return self._creds.token

    def _h(self) -> dict:
        return {"Authorization": f"Bearer {self._token()}"}

    def get_or_create_folder(self, name: str, parent_id: str | None = None) -> str:
        q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            q += f" and '{parent_id}' in parents"
        r = requests.get(
            f"{DRIVE_BASE}/files",
            params={"q": q, "fields": "files(id)", "pageSize": 1,
                    "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"},
            headers=self._h(),
        )
        if not r.ok:
            log.error(f"Drive list error {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
        files = r.json().get("files", [])
        if files:
            return files[0]["id"]
        body: dict = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            body["parents"] = [parent_id]
        r = requests.post(
            f"{DRIVE_BASE}/files",
            params={"supportsAllDrives": "true"},
            headers={**self._h(), "Content-Type": "application/json"},
            json=body,
        )
        if not r.ok:
            log.error(f"Drive create folder error {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
        return r.json()["id"]

    def upload(self, data: bytes, filename: str, mime: str, parent_id: str) -> str:
        meta_bytes = json.dumps({"name": filename, "parents": [parent_id]}).encode()
        boundary   = b"xbnd"
        body = (
            b"--" + boundary + b"\r\nContent-Type: application/json\r\n\r\n"
            + meta_bytes
            + b"\r\n--" + boundary + b"\r\nContent-Type: " + mime.encode() + b"\r\n\r\n"
            + data
            + b"\r\n--" + boundary + b"--"
        )
        # Retry with exponential backoff on rate-limit (429) or transient errors
        for attempt in range(4):
            r = requests.post(
                f"{DRIVE_UPLOAD}/files",
                params={"uploadType": "multipart", "fields": "id", "supportsAllDrives": "true"},
                headers={**self._h(), "Content-Type": "multipart/related; boundary=xbnd"},
                data=body,
                timeout=300,
            )
            if r.status_code == 429 or (r.status_code == 403 and "userRateLimit" in r.text):
                wait = 2 ** attempt * 5  # 5s, 10s, 20s, 40s
                log.warning(f"  Drive rate limit — waiting {wait}s (attempt {attempt+1}/4)")
                time.sleep(wait)
                continue
            if not r.ok:
                log.error(f"Drive upload error {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
            break
        file_id = r.json()["id"]
        # Make publicly readable (anyone with the link)
        requests.post(
            f"{DRIVE_BASE}/files/{file_id}/permissions",
            headers={**self._h(), "Content-Type": "application/json"},
            json={"type": "anyone", "role": "reader"},
            timeout=30,
        )
        return file_id

    def delete(self, file_id: str):
        try:
            requests.delete(f"{DRIVE_BASE}/files/{file_id}", headers=self._h(), timeout=30)
        except Exception as e:
            log.warning(f"Delete {file_id} failed: {e}")


# ── helpers ───────────────────────────────────────────────────────────────────

_PREFIX_RE = re.compile(r"^\d{2}-[A-Z]{2}-\d{3}-\d{2}(?:\s*-\s*|\s+)", re.IGNORECASE)

def _creative_name(ad_name: str) -> str:
    """Strip studio/campaign prefix (e.g. '26-FL-013-02 - ') to get the shared creative name."""
    m = _PREFIX_RE.match(ad_name or "")
    return ad_name[m.end():].strip() if m else ""


def _ck(key: str) -> str:
    """Short stable cache key: use ad_id as-is (numeric), MD5-prefix for names."""
    return key if key.isdigit() else hashlib.md5(key.encode()).hexdigest()[:16]


def _thumb_url(fid: str) -> str:
    return f"https://drive.google.com/thumbnail?id={fid}&sz=w500"


def _video_embed_url(fid: str) -> str:
    return f"https://drive.google.com/file/d/{fid}/preview"


_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.facebook.com/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

def _download(url: str, timeout: int = 60) -> bytes | None:
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True, headers=_BROWSER_HEADERS)
        if r.status_code == 200:
            ct = r.headers.get("Content-Type", "")
            if ct and "image" not in ct:
                log.warning(f"  Non-image response ({ct[:60]}) from {url[:70]}")
                return None
            return r.content
        log.warning(f"  HTTP {r.status_code} downloading {url[:80]}")
        return None
    except Exception as e:
        log.warning(f"  Download error: {e}")
        return None


META_AD_ACCOUNT_ID = "act_1553887681409034"


class RateLimitError(Exception):
    pass


def _video_embed_src(video_id: str, token: str) -> str:
    """Return the permanent Facebook embed iframe src URL for a Meta ad video.

    Uses GET /{video_id}?fields=embed_html which works with an ad-account token
    and returns a stable facebook.com/plugins/video.php URL (never expires).
    Raises RateLimitError on Meta error code 4 so the caller can stop early.
    """
    import re as _re
    try:
        r = requests.get(
            f"https://graph.facebook.com/v20.0/{video_id}",
            params={"fields": "embed_html", "access_token": token},
            timeout=30,
        )
        resp = r.json()
        err_code = (resp.get("error") or {}).get("code")
        if err_code == 4:
            raise RateLimitError("Meta API rate limit (#4) reached")
        embed_html = resp.get("embed_html", "")
        m = _re.search(r'src="([^"]+)"', embed_html)
        return m.group(1) if m else ""
    except RateLimitError:
        raise
    except Exception as e:
        log.warning(f"  video embed failed ({video_id}): {e}")
        return ""


def _best_image_url(entry: dict) -> str:
    """Return the best full-size image URL for a static ad entry.
    Prefers facebook.com/ads/image/ (permanent, full-res) over scontent HMAC."""
    import urllib.parse as _up
    for key in ("image_url", "thumbnail_url"):
        url = entry.get(key, "")
        if url and "facebook.com/ads/image/" in url:
            return url
    # emg1/external- proxy URLs wrap the real facebook.com/ads/image/ URL in ?url=
    for key in ("image_url", "thumbnail_url"):
        url = entry.get(key, "")
        if url and ("external-" in url or "/emg1/" in url):
            inner = _up.parse_qs(_up.urlparse(url).query).get("url", [""])[0]
            if inner and "facebook.com/ads/image/" in inner:
                return inner
    return entry.get("image_url", "") or entry.get("thumbnail_url", "")


def _fullres_via_hash(image_hash: str, token: str, account_id: str = META_AD_ACCOUNT_ID) -> str:
    """Call adimages API with image_hash to get the permanent full-res CDN URL."""
    try:
        r = requests.get(
            f"https://graph.facebook.com/v20.0/{account_id}/adimages",
            params={"hashes[]": image_hash, "fields": "url", "access_token": token},
            timeout=30,
        )
        data = r.json().get("data", [])
        return data[0].get("url", "") if data else ""
    except Exception as e:
        log.warning(f"  adimages hash lookup failed ({image_hash[:8]}...): {e}")
        return ""



# ── core processing ───────────────────────────────────────────────────────────

def process(
    drive: DriveClient,
    ads: dict,
    folder_id: str,
    cache: dict,
    token: str,
    today: date,
    max_new_videos: int,
    limit: int = 0,
    max_new_images: int = 0,
    skip_images: bool = False,
    reprocess_dedicated: bool = False,
) -> tuple[int, int]:
    """Process ads dict, upload missing media to Drive.  Returns (thumbs, videos)."""
    thumbs_uploaded = 0
    videos_uploaded = 0
    total_uploaded  = 0

    # Dedup maps: video_id → embed URL (no Drive upload for videos);
    # image_hash → Drive file ID (images still go to Drive).
    vid_to_embed: dict[str, str] = {}
    hash_to_fid:  dict[str, str] = {}
    for ce in cache.values():
        if ce.get("video_id") and ce.get("video_embed_url"):
            vid_to_embed[ce["video_id"]] = ce["video_embed_url"]
        if ce.get("image_hash") and ce.get("drive_image_id"):
            hash_to_fid[ce["image_hash"]] = ce["drive_image_id"]

    for key_raw, entry in ads.items():
        if limit and total_uploaded >= limit:
            break
        ck            = _ck(key_raw)
        video_id      = entry.get("video_id", "")
        ad_name       = entry.get("name", key_raw)
        creative_name = _creative_name(ad_name)
        # SHARE-type video ad: cross-posted video post with no video_id in the creative.
        # Detected by object_type==SHARE + no image_hash (video shares have no image_hash;
        # photo shares do). post_url is NOT required — Meta's API often omits
        # effective_object_story_id for cross-posted video ads, so post_url may be empty.
        # Limited to dedicated-account studios so we don't change behavior for the
        # 490+ corporate SHARE ads that already have cached thumbnails and work fine.
        # An entry is truly from a dedicated account only when meta_account_id is
        # set (NSO entries) OR when its studio_code maps to a known dedicated
        # account in studios.json (OS entries). Plain studio_code alone is not
        # enough for OS: the corporate account tags all its ads with studio_code
        # via campaign-name matching, so using studio_code alone would flag
        # corporate ads as dedicated and run the wrong hash-API account lookup.
        _is_ded_entry = bool(
            entry.get("meta_account_id")
            or _STUDIO_CODE_TO_ACCOUNT.get(entry.get("studio_code", ""))
        )
        is_share_video = (
            _is_ded_entry
            and entry.get("object_type") == "SHARE"
            and not entry.get("image_hash")
        )
        is_video = (
            entry.get("media_type") == "Video"
            or entry.get("object_type") == "VIDEO"
            or bool(video_id)
            or is_share_video
        )

        cached = cache.setdefault(ck, {})
        cached["last_seen"] = today.isoformat()

        # Dedicated-account ad: if reprocess_dedicated is set and we have a
        # previously-uploaded (likely small) image, delete the old Drive file so
        # the next block re-uploads the full-res version.  Drop the not is_video
        # guard so SHARE-type video ads (which were previously uploaded as images)
        # also get their stale Drive files removed.
        is_dedicated = _is_ded_entry
        if reprocess_dedicated and is_dedicated and cached.get("drive_image_id"):
            log.info(f"  reprocess dedicated — deleting old Drive file for {key_raw[:60]}")
            old_fid = cached["drive_image_id"]
            drive.delete(old_fid)
            # Remove stale hash → fid mapping so the dedup path can't reuse the deleted file
            ih_stale = cached.get("image_hash", "")
            if ih_stale and hash_to_fid.get(ih_stale) == old_fid:
                del hash_to_fid[ih_stale]
            del cached["drive_image_id"]
            if "drive_image_url" in entry:
                del entry["drive_image_url"]

        # SHARE-type video: store the Facebook post URL as the embed (no Drive upload).
        if is_share_video and not cached.get("video_embed_url"):
            post_url = entry.get("post_url", "")
            if post_url:
                cached["video_embed_url"] = post_url
                entry["drive_video_url"]  = post_url
                log.info(f"  video  share→post_url {key_raw[:55]}")

        # Restore URLs from cache into the entry (so JSON has them even when we skip upload)
        if cached.get("drive_image_id"):
            entry["drive_image_url"] = _thumb_url(cached["drive_image_id"])
        if cached.get("video_embed_url"):
            entry["drive_video_url"] = cached["video_embed_url"]

        # Static ads only: upload full-res image to Drive for click-to-view.
        # Video thumbnail URLs are kept fresh by re-fetching from Meta API each run.
        need_image = (
            not skip_images
            and not is_video
            and not cached.get("drive_image_id")
            and (max_new_images == 0 or thumbs_uploaded < max_new_images)
        )
        need_video = (
            is_video
            and not cached.get("video_embed_url")
            and bool(video_id)
            and bool(token)
            and videos_uploaded < max_new_videos
        )

        if not need_image and not need_video:
            continue

        # Upload full-res image to Drive (static ads only)
        if need_image:
            ih = entry.get("image_hash", "")
            fid = None
            if ih and ih in hash_to_fid:
                fid = hash_to_fid[ih]
                log.info(f"  image  dedup(hash) {key_raw[:50]}")
            else:
                img_url = _best_image_url(entry)
                if ih and token:
                    # For dedicated-account studios: ALWAYS use the adimages hash API to
                    # get the full-res CDN URL.  Their creative thumbnail_url is a SHARE-type
                    # thumbnail (scontent t45.1600-4/...) which is a small frame capture,
                    # not the original ad image.  The hash API returns the full-res source.
                    # For corporate ads: only fall back when thumbnail_url is empty or is a
                    # facebook.com/ads/image/ URL (requires a browser session to download).
                    if is_dedicated or not img_url or "facebook.com/ads/image/" in img_url:
                        acct = (
                            entry.get("meta_account_id")
                            or _STUDIO_CODE_TO_ACCOUNT.get(entry.get("studio_code", ""))
                            or META_AD_ACCOUNT_ID
                        )
                        full = _fullres_via_hash(ih, token, acct)
                        if full:
                            img_url = full
                if img_url:
                    log.info(f"  image  <- {key_raw[:60]}")
                    img_data = _download(img_url)
                    if img_data:
                        fid = drive.upload(img_data, f"{ck}_image.jpg", "image/jpeg", folder_id)
                        if ih:
                            hash_to_fid[ih] = fid
                        thumbs_uploaded += 1
                        total_uploaded  += 1
            if fid:
                cached["drive_image_id"] = fid
                if ih:
                    cached["image_hash"] = ih
                entry["drive_image_url"] = _thumb_url(fid)

        # Store Facebook embed URL for video ads (no Drive upload needed)
        if need_video:
            embed_src = None
            if video_id in vid_to_embed:
                embed_src = vid_to_embed[video_id]
                log.info(f"  video  dedup(id) {key_raw[:50]}")
            else:
                try:
                    embed_src = _video_embed_src(video_id, token)
                except RateLimitError:
                    log.warning("  Meta rate limit hit — stopping video processing for this run")
                    break
            if embed_src:
                cached["video_embed_url"] = embed_src
                cached["video_id"]        = video_id
                vid_to_embed[video_id]    = embed_src
                entry["drive_video_url"]  = embed_src
                videos_uploaded += 1
                total_uploaded  += 1
                log.info(f"  video  embed ({videos_uploaded}/{max_new_videos}) {key_raw[:50]}")

        time.sleep(0.5)

    return thumbs_uploaded, videos_uploaded


def prune_old(drive: DriveClient, cache: dict, cutoff_iso: str) -> int:
    """Delete Drive files for entries older than cutoff_iso. Returns count deleted."""
    to_remove = []
    deleted   = 0
    for ck, entry in cache.items():
        if (entry.get("last_seen", "9999") >= cutoff_iso):
            continue
        for fkey in ("drive_thumb_id", "drive_video_id"):
            fid = entry.get(fkey)
            if fid:
                log.info(f"  prune  Drive:{fid}  (last_seen={entry.get('last_seen')})")
                drive.delete(fid)
                deleted += 1
        to_remove.append(ck)
    for ck in to_remove:
        del cache[ck]
    return deleted


# ── per-dashboard runners ─────────────────────────────────────────────────────

def run_os(drive: DriveClient, folder_id: str, token: str, today: date, max_new_videos: int, limit: int = 0, max_new_images: int = 0, skip_images: bool = False, reprocess_dedicated: bool = False):
    if not OS_JSON.exists():
        log.warning(f"Not found: {OS_JSON}")
        return
    cache = json.loads(OS_CACHE.read_text(encoding="utf-8")) if OS_CACHE.exists() else {}
    data  = json.loads(OS_JSON.read_text(encoding="utf-8"))
    ads   = data.get("ad_meta", {})
    cutoff = (today - timedelta(days=OS_RETENTION_DAYS)).isoformat()
    ads_recent = {k: v for k, v in ads.items() if v.get("fetched_at", "") >= cutoff}
    if not ads_recent:
        ads_recent = ads
    log.info(f"OS: {len(ads)} ads total, {len(ads_recent)} within 90-day window")

    nt, nv = process(drive, ads_recent, folder_id, cache, token, today, max_new_videos, limit, max_new_images, skip_images, reprocess_dedicated)
    for k, v in ads_recent.items():
        if k in ads:
            ads[k].update({fk: fv for fk, fv in v.items() if fk.startswith("drive_")})
    pruned = prune_old(drive, cache, cutoff)

    data["ad_meta"] = ads
    OS_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    OS_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"OS done: {nt} thumbs, {nv} videos stored, {pruned} Drive files pruned")


def run_nso(drive: DriveClient, folder_id: str, token: str, today: date, max_new_videos: int, limit: int = 0, max_new_images: int = 0, skip_images: bool = False, reprocess_dedicated: bool = False):
    if not NSO_JSON.exists():
        log.warning(f"Not found: {NSO_JSON}")
        return
    cache = json.loads(NSO_CACHE.read_text(encoding="utf-8")) if NSO_CACHE.exists() else {}
    data  = json.loads(NSO_JSON.read_text(encoding="utf-8"))
    ads   = data.get("ad_meta", {})
    log.info(f"NSO: {len(ads)} ads")

    nt, nv = process(drive, ads, folder_id, cache, token, today, max_new_videos, limit, max_new_images, skip_images, reprocess_dedicated)

    data["ad_meta"] = ads
    NSO_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    NSO_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"NSO done: {nt} thumbs, {nv} videos stored")


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Meta ad media to Google Drive")
    parser.add_argument("--dashboard", choices=["os", "nso", "both"], default="both")
    parser.add_argument("--max-new-videos", type=int, default=50,
                        help="Max new video uploads per run (default 50).")
    parser.add_argument("--max-new-images", type=int, default=300,
                        help="Max new image uploads per run (default 300). "
                             "Limits burst on first run. 0 = unlimited.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Cap total uploads (thumbs + videos) for testing. 0 = unlimited.")
    parser.add_argument("--skip-images", action="store_true",
                        help="Skip all image processing (useful when only syncing videos).")
    parser.add_argument("--reprocess-dedicated", action="store_true",
                        help="Delete and re-upload images for dedicated-account studios "
                             "(use once after fixing the account-ID lookup to replace "
                             "previously-downloaded small thumbnails with full-res images).")
    args = parser.parse_args()

    token = (
        os.environ.get("META_ACCESS_TOKEN")
        or os.environ.get("META_TOKEN")
        or ""
    )
    if not token:
        log.warning("META_ACCESS_TOKEN not set — video downloads will be skipped")

    sa_file = (
        os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY_PATH")
        or "nso-dashboard/credentials/service_account.json"
    )
    if not Path(sa_file).exists():
        log.error(f"Service account file not found: {sa_file}")
        sys.exit(1)

    today = date.today()
    drive = DriveClient(sa_file)
    root  = DRIVE_ROOT_FOLDER_ID

    reprocess_dedicated = args.reprocess_dedicated

    if args.dashboard in ("os", "both"):
        folder = drive.get_or_create_folder("os", root)
        run_os(drive, folder, token, today, args.max_new_videos, args.limit, args.max_new_images, args.skip_images, reprocess_dedicated)

    if args.dashboard in ("nso", "both"):
        folder = drive.get_or_create_folder("nso", root)
        run_nso(drive, folder, token, today, args.max_new_videos, args.limit, args.max_new_images, args.skip_images, reprocess_dedicated)


if __name__ == "__main__":
    main()
