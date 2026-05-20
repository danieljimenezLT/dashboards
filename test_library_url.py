#!/usr/bin/env python3
"""
test_library_url.py
-------------------
Diagnostic: tries multiple paths to find the ad_archive_id used by
the public Ads Library URL.

Step 1 - probe ad & creative for any 'archive' field.
Step 2 - query the /ads_archive endpoint by page_id (and optionally post_id)
         to find the ad_archive_id Meta uses in the public library.
"""
import os
import sys
import requests

API_VERSION = "v20.0"
BASE_URL    = f"https://graph.facebook.com/{API_VERSION}"


def graph(path: str, params: dict, token: str) -> dict:
    params = {**params, "access_token": token}
    r = requests.get(f"{BASE_URL}/{path}", params=params, timeout=60)
    return r.json()


def probe_ad(ad_id: str, token: str) -> None:
    print(f"\n===== ad_id: {ad_id} =====")

    # 1) Pull ad with creative, try several "archive"-ish fields
    ad = graph(ad_id, {
        "fields": "id,name,status,creative{id,effective_object_story_id,object_story_id,instagram_permalink_url,effective_instagram_media_id}"
    }, token)
    if "error" in ad:
        print("  ad endpoint error:", ad["error"].get("message"))
        return
    print(f"  name           : {ad.get('name','')}")
    creative = ad.get("creative") or {}
    eosi = creative.get("effective_object_story_id") or ""
    page_id, post_id = ("", "")
    if "_" in eosi:
        page_id, post_id = eosi.split("_", 1)
    print(f"  page_id        : {page_id}")
    print(f"  post_id        : {post_id}")
    print(f"  ig_media_id    : {creative.get('effective_instagram_media_id','')}")

    if not page_id:
        print("  no page_id - cannot search Ads Library")
        return

    # 2) Query the public Ad Library API for this page, filter to active ads
    print(f"\n  --- /ads_archive search for page_id={page_id} ---")
    arch = graph("ads_archive", {
        "search_page_ids":      f"[{page_id}]",
        "ad_active_status":     "ALL",
        "ad_reached_countries": "['US']",
        "fields":               "ad_archive_id,ad_creative_body,ad_creative_link_caption,page_id,ad_snapshot_url",
        "limit":                25,
    }, token)
    if "error" in arch:
        print("  ads_archive error:", arch["error"].get("message"))
        print("  (the Ad Library API may need a separate permission/token scope)")
        return
    data = arch.get("data") or []
    print(f"  returned {len(data)} ads")
    # Show first few, see if any include 1942589402912793 etc.
    for row in data[:5]:
        aaid = row.get("ad_archive_id")
        body_preview = (row.get("ad_creative_body") or "")[:60].replace("\n", " ")
        print(f"    ad_archive_id={aaid}  body='{body_preview}...'")
    # Look for snapshot URL pattern
    if data:
        print(f"  sample snapshot_url: {data[0].get('ad_snapshot_url','')}")


def main(ad_ids: list[str]) -> int:
    token = os.environ.get("META_TOKEN")
    if not token:
        print("ERROR: META_TOKEN not set", file=sys.stderr)
        return 2
    for ad_id in ad_ids:
        probe_ad(ad_id, token)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["120248075197290249"]))
