"""
migrate_ig_thumbs_to_drive.py
One-time migration: upload existing local Instagram thumbnails to Google Drive
and update social_insights.json with lh3.googleusercontent.com URLs.

Usage:
    python nso-dashboard/scripts/migrate_ig_thumbs_to_drive.py

Requires:
    nso-dashboard/credentials/service_account.json
    nso-dashboard/social_insights.json
    nso-dashboard/thumbnails/  (existing local files)

After running:
    - social_insights.json has Drive URLs instead of local paths
    - Commit social_insights.json
    - Delete nso-dashboard/thumbnails/ from git (git rm -r --cached nso-dashboard/thumbnails/)
    - Add nso-dashboard/thumbnails/ to .gitignore
"""

import json
import time
import logging
from pathlib import Path

import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
REPO_ROOT          = Path(__file__).resolve().parents[2]
SA_FILE            = REPO_ROOT / "nso-dashboard" / "credentials" / "service_account.json"
SOCIAL_JSON        = REPO_ROOT / "nso-dashboard" / "social_insights.json"
THUMBS_ROOT        = REPO_ROOT / "nso-dashboard" / "thumbnails"
DRIVE_ROOT_FOLDER  = "1XZPoMSawhnnF8YIJHkeGywe5VeNNCROd"
DRIVE_BASE         = "https://www.googleapis.com/drive/v3"
DRIVE_UPLOAD       = "https://www.googleapis.com/upload/drive/v3"


# ── Drive client (same pattern as sync_ad_media.py) ──────────────────────────
class DriveClient:
    def __init__(self, sa_file: Path):
        self._creds = service_account.Credentials.from_service_account_file(
            str(sa_file), scopes=["https://www.googleapis.com/auth/drive"]
        )

    def _token(self) -> str:
        if not self._creds.valid:
            self._creds.refresh(GoogleAuthRequest())
        return self._creds.token

    def _h(self) -> dict:
        return {"Authorization": f"Bearer {self._token()}"}

    def get_or_create_folder(self, name: str, parent_id: str) -> str:
        q = (f"name='{name}' and mimeType='application/vnd.google-apps.folder'"
             f" and '{parent_id}' in parents and trashed=false")
        r = requests.get(
            f"{DRIVE_BASE}/files",
            params={"q": q, "fields": "files(id)", "pageSize": 1,
                    "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"},
            headers=self._h(), timeout=30,
        )
        r.raise_for_status()
        files = r.json().get("files", [])
        if files:
            return files[0]["id"]
        r = requests.post(
            f"{DRIVE_BASE}/files",
            params={"supportsAllDrives": "true"},
            headers={**self._h(), "Content-Type": "application/json"},
            json={"name": name, "mimeType": "application/vnd.google-apps.folder",
                  "parents": [parent_id]},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["id"]

    def upload(self, data: bytes, filename: str, parent_id: str) -> str:
        meta_bytes = json.dumps({"name": filename, "parents": [parent_id]}).encode()
        boundary = b"xbnd"
        body = (
            b"--" + boundary + b"\r\nContent-Type: application/json\r\n\r\n"
            + meta_bytes
            + b"\r\n--" + boundary + b"\r\nContent-Type: image/jpeg\r\n\r\n"
            + data
            + b"\r\n--" + boundary + b"--"
        )
        for attempt in range(4):
            r = requests.post(
                f"{DRIVE_UPLOAD}/files",
                params={"uploadType": "multipart", "fields": "id",
                        "supportsAllDrives": "true"},
                headers={**self._h(), "Content-Type": "multipart/related; boundary=xbnd"},
                data=body, timeout=300,
            )
            if r.status_code in (429, 503) or (r.status_code == 403 and "userRateLimit" in r.text):
                wait = 2 ** attempt * 5
                log.warning(f"  Rate limit — waiting {wait}s (attempt {attempt+1}/4)")
                time.sleep(wait)
                continue
            r.raise_for_status()
            break
        file_id = r.json()["id"]
        # Make publicly readable
        requests.post(
            f"{DRIVE_BASE}/files/{file_id}/permissions",
            headers={**self._h(), "Content-Type": "application/json"},
            json={"type": "anyone", "role": "reader"},
            timeout=30,
        )
        return file_id


def drive_thumb_url(fid: str) -> str:
    return f"https://drive.google.com/thumbnail?id={fid}&sz=w400"


def main():
    if not SA_FILE.exists():
        raise FileNotFoundError(f"Service account not found: {SA_FILE}")
    if not SOCIAL_JSON.exists():
        raise FileNotFoundError(f"social_insights.json not found: {SOCIAL_JSON}")

    drive = DriveClient(SA_FILE)

    # Get/create instagram/ folder inside root
    log.info("Getting Drive folder...")
    ig_folder = drive.get_or_create_folder("instagram", DRIVE_ROOT_FOLDER)
    log.info(f"  instagram/ folder: {ig_folder}")

    # Build map: local_path_str → drive_url  (from any already-migrated entries)
    # Also load social_insights.json
    with open(SOCIAL_JSON, encoding="utf-8-sig") as f:
        insights = json.load(f)

    # Count what we need to upload
    to_upload = []   # (studio, post, code, source) where source is Path or CDN URL string
    already_drive = 0

    for studio in insights.get("instagram", []):
        code = studio.get("code", "unknown")
        for post in studio.get("posts", []):
            url = post.get("image_url", "")
            if not url:
                continue
            if url.startswith("https://drive.google.com/thumbnail"):
                already_drive += 1
                continue
            if url.startswith("nso-dashboard/thumbnails/"):
                # Local path
                local = REPO_ROOT / url
                if local.exists():
                    to_upload.append((studio, post, code, local))
                else:
                    log.warning(f"  Missing local file: {url}")
            else:
                # CDN URL (Instagram/Facebook) — download and upload to Drive
                permalink = post.get("permalink", "")
                slug = permalink.rstrip("/").split("/")[-1] if permalink else post.get("date", "unknown")
                to_upload.append((studio, post, code, (url, slug)))

    log.info(f"\nSummary:")
    log.info(f"  Already on Drive: {already_drive}")
    log.info(f"  To upload:        {len(to_upload)}")

    if not to_upload:
        log.info("Nothing to upload.")
    else:
        log.info(f"\nUploading {len(to_upload)} thumbnails to Drive...")
        studio_folders: dict[str, str] = {}
        ok = fail = 0

        for i, (studio, post, code, source) in enumerate(to_upload, 1):
            if code not in studio_folders:
                studio_folders[code] = drive.get_or_create_folder(code, ig_folder)

            try:
                if isinstance(source, Path):
                    data = source.read_bytes()
                    filename = source.name
                else:
                    # CDN URL tuple: (url, slug)
                    cdn_url, slug = source
                    r = requests.get(cdn_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
                    r.raise_for_status()
                    data = r.content
                    filename = f"{slug}.jpg"

                fid = drive.upload(data, filename, studio_folders[code])
                post["image_url"] = drive_thumb_url(fid)
                ok += 1
                if i % 25 == 0 or i == len(to_upload):
                    log.info(f"  [{i}/{len(to_upload)}] {code}/{filename} → {fid}")
            except Exception as e:
                log.warning(f"  FAILED {code}/{local.name}: {e}")
                fail += 1

        log.info(f"\nDone — uploaded: {ok}  failed: {fail}")

    # Save updated social_insights.json
    with open(SOCIAL_JSON, "w", encoding="utf-8") as f:
        json.dump(insights, f, ensure_ascii=False, indent=2)
    log.info(f"\nUpdated {SOCIAL_JSON.name}")

    log.info("""
Next steps:
  1. Verify thumbnails load in the browser (open nso-studios.html → Instagram tab)
  2. git add nso-dashboard/social_insights.json
  3. git rm -r --cached nso-dashboard/thumbnails/
  4. Add to .gitignore:  nso-dashboard/thumbnails/
  5. git commit -m "chore: migrate Instagram thumbnails to Google Drive"
  6. git push
""")


if __name__ == "__main__":
    main()
