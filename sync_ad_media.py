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
    return f"https://drive.google.com/file/d/{fid}/view"


def _video_embed_url(fid: str) -> str:
    return f"https://drive.google.com/file/d/{fid}/preview"


def _download(url: str, timeout: int = 60) -> bytes | None:
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.content
        log.warning(f"  HTTP {r.status_code} downloading {url[:80]}")
        return None
    except Exception as e:
        log.warning(f"  Download error: {e}")
        return None


META_AD_ACCOUNT_ID = "act_1553887681409034"


def _video_source_url(video_id: str, token: str) -> str:
    """Get the direct MP4 URL for a Meta video via the advideos edge."""
    try:
        r = requests.get(
            f"https://graph.facebook.com/v20.0/{META_AD_ACCOUNT_ID}/advideos",
            params={"video_ids": video_id, "fields": "source", "access_token": token},
            timeout=30,
        )
        data = r.json().get("data", [])
        return data[0].get("source", "") if data else ""
    except Exception as e:
        log.warning(f"  video source failed ({video_id}): {e}")
        return ""


def _best_image_url(entry: dict) -> str:
    """Return the best full-size image URL for a static ad entry.
    Prefers facebook.com/ads/image/ (permanent, full-res) over scontent HMAC."""
    for key in ("image_url", "thumbnail_url"):
        url = entry.get(key, "")
        if url and "facebook.com/ads/image/" in url:
            return url
    # Fall back to whatever is available
    return entry.get("image_url", "") or entry.get("thumbnail_url", "")


def _fullres_via_hash(image_hash: str, token: str) -> str:
    """Call adimages API with image_hash to get the permanent full-res URL."""
    try:
        r = requests.get(
            f"https://graph.facebook.com/v20.0/{META_AD_ACCOUNT_ID}/adimages",
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
    drive: "DriveClient | None",
    ads: dict,
    folder_id: str,
    cache: dict,
    token: str,
    today: date,
    max_new_videos: int,
    limit: int = 0,
    max_new_images: int = 0,
    local_save: "Path | None" = None,
) -> tuple[int, int]:
    """Process ads dict, upload missing media to Drive.  Returns (thumbs, videos).

    If local_save is set, files are saved to that directory instead of Drive.
    Cache and JSON files are NOT modified in local_save mode — use the manifest
    (local_save/manifest.json) to review what was downloaded and what was deduped.
    """
    thumbs_uploaded = 0
    videos_uploaded = 0
    total_uploaded  = 0
    manifest: list[dict] = []          # populated only when local_save is set

    if local_save:
        local_save.mkdir(parents=True, exist_ok=True)

    # Dedup by exact match only — creative-name dedup is intentionally excluded for videos
    # because different studios upload separate files under the same campaign name.
    #   vid_to_fid    — same Meta video_id → definitely same file
    #   vid_hash_fid  — same MP4 content hash → same bytes, safe to reuse Drive file
    #   hash_to_fid   — same image_hash → same pixels
    vid_to_fid:   dict[str, str] = {}
    vid_hash_fid: dict[str, str] = {}  # content_hash → drive_id OR local_filename
    hash_to_fid:  dict[str, str] = {}
    for ce in cache.values():
        if ce.get("video_id") and ce.get("drive_video_id"):
            vid_to_fid[ce["video_id"]] = ce["drive_video_id"]
        if ce.get("video_content_hash") and ce.get("drive_video_id"):
            vid_hash_fid[ce["video_content_hash"]] = ce["drive_video_id"]
        if ce.get("image_hash") and ce.get("drive_image_id"):
            hash_to_fid[ce["image_hash"]] = ce["drive_image_id"]

    for key_raw, entry in ads.items():
        if limit and total_uploaded >= limit:
            break
        ck            = _ck(key_raw)
        video_id      = entry.get("video_id", "")
        ad_name       = entry.get("name", key_raw)
        creative_name = _creative_name(ad_name)
        is_video = (
            entry.get("media_type") == "Video"
            or entry.get("object_type") == "VIDEO"
            or bool(video_id)
        )

        cached = cache.setdefault(ck, {})
        if not local_save:
            cached["last_seen"] = today.isoformat()

        # Restore Drive URLs from cache into the entry (so JSON has them even when we skip upload)
        if not local_save:
            if cached.get("drive_image_id"):
                entry["drive_image_url"] = _thumb_url(cached["drive_image_id"])
            if cached.get("drive_video_id"):
                entry["drive_video_url"] = _video_embed_url(cached["drive_video_id"])

        # Videos: upload only the MP4 (grid thumbnail stays as-is from Facebook URL)
        # Static ads: upload full-res image to Drive for click-to-view
        need_image = (
            not is_video
            and not cached.get("drive_image_id")
            and (max_new_images == 0 or thumbs_uploaded < max_new_images)
        )
        need_video = (
            is_video
            and not cached.get("drive_video_id")
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
                if local_save:
                    manifest.append({"type": "image", "ad": ad_name, "ck": ck,
                                     "dedup_from_hash": ih, "local_file": None})
            else:
                img_url = _best_image_url(entry)
                if img_url and "facebook.com/ads/image/" not in img_url and token and ih:
                    full = _fullres_via_hash(ih, token)
                    if full:
                        img_url = full
                if img_url:
                    log.info(f"  image  <- {key_raw[:60]}")
                    img_data = _download(img_url)
                    if img_data:
                        local_name = f"{ck}_image.jpg"
                        if local_save:
                            (local_save / local_name).write_bytes(img_data)
                            manifest.append({"type": "image", "ad": ad_name, "ck": ck,
                                             "image_hash": ih, "local_file": local_name,
                                             "size_kb": len(img_data) // 1024})
                            if ih:
                                hash_to_fid[ih] = local_name  # track for dedup within run
                            thumbs_uploaded += 1
                            total_uploaded  += 1
                        else:
                            fid = drive.upload(img_data, local_name, "image/jpeg", folder_id)
                            if ih:
                                hash_to_fid[ih] = fid
                            thumbs_uploaded += 1
                            total_uploaded  += 1
            if fid and not local_save:
                cached["drive_image_id"] = fid
                if ih:
                    cached["image_hash"] = ih
                entry["drive_image_url"] = _thumb_url(fid)

        # Upload full video MP4
        if need_video:
            fid = None
            if video_id in vid_to_fid:
                fid = vid_to_fid[video_id]
                log.info(f"  video  dedup(vid_id) {key_raw[:50]}")
                if local_save:
                    manifest.append({"type": "video", "ad": ad_name, "ck": ck,
                                     "video_id": video_id, "dedup_from_vid_id": video_id,
                                     "local_file": None})
            else:
                src = _video_source_url(video_id, token)
                if src:
                    vid_data = _download(src, timeout=300)
                    if vid_data:
                        vh = hashlib.md5(vid_data).hexdigest()
                        local_name = f"{ck}_video.mp4"
                        if vh in vid_hash_fid:
                            existing = vid_hash_fid[vh]
                            log.info(f"  video  dedup(content) {key_raw[:50]} <- {str(existing)[:40]}")
                            if local_save:
                                manifest.append({"type": "video", "ad": ad_name, "ck": ck,
                                                 "video_id": video_id, "content_hash": vh,
                                                 "dedup_from": str(existing), "local_file": None})
                            else:
                                fid = existing
                        else:
                            size_kb = len(vid_data) // 1024
                            if local_save:
                                (local_save / local_name).write_bytes(vid_data)
                                vid_hash_fid[vh] = local_name
                                manifest.append({"type": "video", "ad": ad_name, "ck": ck,
                                                 "video_id": video_id, "content_hash": vh,
                                                 "local_file": local_name, "size_kb": size_kb})
                                videos_uploaded += 1
                                total_uploaded  += 1
                                log.info(f"    {size_kb} KB -> {local_name} ({videos_uploaded}/{max_new_videos})")
                            else:
                                fid = drive.upload(vid_data, local_name, "video/mp4", folder_id)
                                vid_hash_fid[vh] = fid
                                videos_uploaded += 1
                                total_uploaded  += 1
                                log.info(f"    {size_kb} KB -> Drive ({videos_uploaded}/{max_new_videos})")
                        if not local_save:
                            vid_to_fid[video_id] = fid
                            cached["video_content_hash"] = vh
            if fid and not local_save:
                cached["drive_video_id"] = fid
                cached["video_id"]       = video_id
                entry["drive_video_url"] = _video_embed_url(fid)

        time.sleep(0.2 if local_save else 0.5)

    if local_save and manifest:
        (local_save / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        deduped = sum(1 for m in manifest if m.get("dedup_from") or m.get("dedup_from_hash") or m.get("dedup_from_vid_id"))
        log.info(f"  local-save: {thumbs_uploaded} images + {videos_uploaded} videos downloaded, "
                 f"{deduped} deduped. Manifest: {local_save / 'manifest.json'}")

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

def run_os(drive: "DriveClient | None", folder_id: str, token: str, today: date,
           max_new_videos: int, limit: int = 0, max_new_images: int = 0,
           local_save: "Path | None" = None):
    if not OS_JSON.exists():
        log.warning(f"Not found: {OS_JSON}")
        return
    cache = json.loads(OS_CACHE.read_text(encoding="utf-8")) if OS_CACHE.exists() else {}
    data  = json.loads(OS_JSON.read_text(encoding="utf-8"))
    ads   = data.get("ad_meta", {})
    cutoff = (today - timedelta(days=OS_RETENTION_DAYS)).isoformat()
    # Only sync media for ads seen in the last 90 days.
    # fetched_at is stamped by fetch_meta_ads.py; fall back to all ads if field not yet present.
    ads_recent = {k: v for k, v in ads.items() if v.get("fetched_at", "") >= cutoff}
    if not ads_recent:
        ads_recent = ads  # first run before fetch_meta_ads.py stamps fetched_at
    log.info(f"OS: {len(ads)} ads total, {len(ads_recent)} within 90-day window")

    nt, nv = process(drive, ads_recent, folder_id, cache, token, today,
                     max_new_videos, limit, max_new_images, local_save)
    if not local_save:
        # Write Drive URLs back to the full ads dict
        for k, v in ads_recent.items():
            if k in ads:
                ads[k].update({fk: fv for fk, fv in v.items() if fk.startswith("drive_")})
        pruned = prune_old(drive, cache, cutoff)
        data["ad_meta"] = ads
        OS_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        OS_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"OS done: {nt} thumbs, {nv} videos uploaded, {pruned} Drive files pruned")
    else:
        log.info(f"OS local-save done: {nt} images + {nv} videos saved to {local_save}")


def run_nso(drive: "DriveClient | None", folder_id: str, token: str, today: date,
            max_new_videos: int, limit: int = 0, max_new_images: int = 0,
            local_save: "Path | None" = None):
    if not NSO_JSON.exists():
        log.warning(f"Not found: {NSO_JSON}")
        return
    cache = json.loads(NSO_CACHE.read_text(encoding="utf-8")) if NSO_CACHE.exists() else {}
    data  = json.loads(NSO_JSON.read_text(encoding="utf-8"))
    ads   = data.get("ad_meta", {})
    log.info(f"NSO: {len(ads)} ads")

    nt, nv = process(drive, ads, folder_id, cache, token, today,
                     max_new_videos, limit, max_new_images, local_save)
    if not local_save:
        # NSO keeps media for the full year — no pruning
        data["ad_meta"] = ads
        NSO_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        NSO_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"NSO done: {nt} thumbs, {nv} videos uploaded")
    else:
        log.info(f"NSO local-save done: {nt} images + {nv} videos saved to {local_save}")


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
    parser.add_argument("--local-save", metavar="DIR",
                        help="Download files to DIR instead of Drive. "
                             "Cache/JSON not modified — review manifest.json then run normally.")
    args = parser.parse_args()

    token = (
        os.environ.get("META_ACCESS_TOKEN")
        or os.environ.get("META_TOKEN")
        or ""
    )
    if not token:
        log.warning("META_ACCESS_TOKEN not set — video downloads will be skipped")

    local_save = Path(args.local_save) if args.local_save else None

    sa_file = (
        os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY_PATH")
        or "nso-dashboard/credentials/service_account.json"
    )
    drive = None
    folder_os = folder_nso = ""
    if not local_save:
        if not Path(sa_file).exists():
            log.error(f"Service account file not found: {sa_file}")
            sys.exit(1)
        drive = DriveClient(sa_file)
        root  = DRIVE_ROOT_FOLDER_ID
        if args.dashboard in ("os", "both"):
            folder_os = drive.get_or_create_folder("os", root)
        if args.dashboard in ("nso", "both"):
            folder_nso = drive.get_or_create_folder("nso", root)

    today = date.today()

    if args.dashboard in ("os", "both"):
        ls = (local_save / "os") if local_save else None
        run_os(drive, folder_os, token, today, args.max_new_videos, args.limit,
               args.max_new_images, ls)

    if args.dashboard in ("nso", "both"):
        ls = (local_save / "nso") if local_save else None
        run_nso(drive, folder_nso, token, today, args.max_new_videos, args.limit,
                args.max_new_images, ls)


if __name__ == "__main__":
    main()
