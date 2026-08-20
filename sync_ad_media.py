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
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest

load_dotenv(Path(__file__).parent / ".env")

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
DRIVE_ROOT_FOLDER = "SWEAT440 Ad Media"

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
            params={"q": q, "fields": "files(id)", "pageSize": 1},
            headers=self._h(),
        )
        files = r.json().get("files", [])
        if files:
            return files[0]["id"]
        body: dict = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            body["parents"] = [parent_id]
        r = requests.post(
            f"{DRIVE_BASE}/files",
            headers={**self._h(), "Content-Type": "application/json"},
            json=body,
        )
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
        r = requests.post(
            f"{DRIVE_UPLOAD}/files",
            params={"uploadType": "multipart", "fields": "id"},
            headers={**self._h(), "Content-Type": "multipart/related; boundary=xbnd"},
            data=body,
            timeout=300,
        )
        r.raise_for_status()
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

def _ck(key: str) -> str:
    """Short stable cache key: use ad_id as-is (numeric), MD5-prefix for names."""
    return key if key.isdigit() else hashlib.md5(key.encode()).hexdigest()[:16]


def _needs_upload(url: str) -> bool:
    if not url:
        return True
    if "facebook.com/ads/image/" in url:
        return False  # permanent public URL — no upload needed
    if "lh3.googleusercontent.com" in url or "drive.google.com" in url:
        return False  # already in Drive
    return True  # scontent HMAC — expires, needs upload


def _thumb_url(fid: str) -> str:
    return f"https://lh3.googleusercontent.com/d/{fid}"


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


def _video_source_url(video_id: str, token: str) -> str:
    """Get the direct MP4 URL for a Meta video via Graph API."""
    try:
        r = requests.get(
            f"https://graph.facebook.com/v20.0/{video_id}",
            params={"fields": "source", "access_token": token},
            timeout=30,
        )
        return r.json().get("source", "")
    except Exception as e:
        log.warning(f"  video source failed ({video_id}): {e}")
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
) -> tuple[int, int]:
    """Process ads dict, upload missing media to Drive.  Returns (thumbs, videos)."""
    thumbs_uploaded = 0
    videos_uploaded = 0

    for key_raw, entry in ads.items():
        ck       = _ck(key_raw)
        thumb    = entry.get("thumbnail_url", "")
        video_id = entry.get("video_id", "")
        is_video = (
            entry.get("media_type") == "Video"
            or entry.get("object_type") == "VIDEO"
            or bool(video_id)
        )

        cached = cache.setdefault(ck, {})
        cached["last_seen"] = today.isoformat()

        # Restore Drive URLs from cache into the entry (so JSON has them even when we skip upload)
        if cached.get("drive_thumb_id"):
            entry["drive_thumb_url"] = _thumb_url(cached["drive_thumb_id"])
        if cached.get("drive_video_id"):
            entry["drive_video_url"] = _video_embed_url(cached["drive_video_id"])

        # Decide what still needs uploading
        need_thumb  = not cached.get("drive_thumb_id") and _needs_upload(thumb) and bool(thumb)
        need_video  = (
            is_video
            and not cached.get("drive_video_id")
            and bool(video_id)
            and bool(token)
            and videos_uploaded < max_new_videos
        )

        if not need_thumb and not need_video:
            continue

        # Upload thumbnail image
        if need_thumb:
            log.info(f"  thumb  ← {key_raw[:70]}")
            data = _download(thumb)
            if data:
                fid = drive.upload(data, f"{ck}_thumb.jpg", "image/jpeg", folder_id)
                cached["drive_thumb_id"] = fid
                entry["drive_thumb_url"] = _thumb_url(fid)
                thumbs_uploaded += 1

        # Upload full video
        if need_video:
            log.info(f"  video  ← video_id={video_id}")
            src = _video_source_url(video_id, token)
            if src:
                data = _download(src, timeout=300)
                if data:
                    size_kb = len(data) // 1024
                    fid = drive.upload(data, f"{ck}_video.mp4", "video/mp4", folder_id)
                    cached["drive_video_id"] = fid
                    entry["drive_video_url"] = _video_embed_url(fid)
                    videos_uploaded += 1
                    log.info(f"    {size_kb} KB → Drive ({videos_uploaded}/{max_new_videos})")

        time.sleep(0.1)

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

def run_os(drive: DriveClient, folder_id: str, token: str, today: date, max_new_videos: int):
    if not OS_JSON.exists():
        log.warning(f"Not found: {OS_JSON}")
        return
    cache = json.loads(OS_CACHE.read_text(encoding="utf-8")) if OS_CACHE.exists() else {}
    data  = json.loads(OS_JSON.read_text(encoding="utf-8"))
    ads   = data.get("ad_meta", {})
    log.info(f"OS: {len(ads)} ads")

    nt, nv = process(drive, ads, folder_id, cache, token, today, max_new_videos)

    cutoff = (today - timedelta(days=OS_RETENTION_DAYS)).isoformat()
    pruned = prune_old(drive, cache, cutoff)

    data["ad_meta"] = ads
    OS_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    OS_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"OS done: {nt} thumbs, {nv} videos uploaded, {pruned} Drive files pruned")


def run_nso(drive: DriveClient, folder_id: str, token: str, today: date, max_new_videos: int):
    if not NSO_JSON.exists():
        log.warning(f"Not found: {NSO_JSON}")
        return
    cache = json.loads(NSO_CACHE.read_text(encoding="utf-8")) if NSO_CACHE.exists() else {}
    data  = json.loads(NSO_JSON.read_text(encoding="utf-8"))
    ads   = data.get("ad_meta", {})
    log.info(f"NSO: {len(ads)} ads")

    nt, nv = process(drive, ads, folder_id, cache, token, today, max_new_videos)

    # NSO keeps media for the full year — no pruning
    data["ad_meta"] = ads
    NSO_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    NSO_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"NSO done: {nt} thumbs, {nv} videos uploaded")


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Meta ad media to Google Drive")
    parser.add_argument("--dashboard", choices=["os", "nso", "both"], default="both")
    parser.add_argument("--max-new-videos", type=int, default=50,
                        help="Max new video uploads per run (default 50). "
                             "Thumbs are unlimited. After the first week all "
                             "existing ads are cached and only new ads upload.")
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
    root  = drive.get_or_create_folder(DRIVE_ROOT_FOLDER)

    if args.dashboard in ("os", "both"):
        folder = drive.get_or_create_folder("os", root)
        run_os(drive, folder, token, today, args.max_new_videos)

    if args.dashboard in ("nso", "both"):
        folder = drive.get_or_create_folder("nso", root)
        run_nso(drive, folder, token, today, args.max_new_videos)


if __name__ == "__main__":
    main()
