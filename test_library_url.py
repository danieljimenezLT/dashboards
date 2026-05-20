#!/usr/bin/env python3
"""
test_library_url.py
-------------------
Probes the Meta API for one or more ad_ids and shows what fields are
actually available for constructing an Ads Library deep link.
"""
import os
import sys
import json
import requests

API_VERSION = "v20.0"
BASE_URL    = f"https://graph.facebook.com/{API_VERSION}"


def graph(path: str, params: dict, token: str) -> dict:
    params = {**params, "access_token": token}
    r = requests.get(f"{BASE_URL}/{path}", params=params, timeout=30)
    return r.json()


def probe_ad(ad_id: str, token: str) -> None:
    print(f"\n===== ad_id: {ad_id} =====")
    # Pull ad with creative.effective_object_story_id via field expansion
    ad = graph(ad_id, {"fields": "id,name,status,creative{id,effective_object_story_id,object_story_id,object_type}"}, token)
    if "error" in ad:
        print("  ad endpoint error:", ad["error"].get("message"))
        return
    print(f"  name           : {ad.get('name','')}")
    print(f"  status         : {ad.get('status','')}")
    creative = ad.get("creative") or {}
    creative_id = creative.get("id", "")
    print(f"  creative.id    : {creative_id}")
    print(f"  creative.object_type            : {creative.get('object_type','')}")
    print(f"  creative.effective_object_story_id : {creative.get('effective_object_story_id','')}")
    print(f"  creative.object_story_id           : {creative.get('object_story_id','')}")

    # Pick whichever post-ID-bearing field is present
    eosi = creative.get("effective_object_story_id") or creative.get("object_story_id") or ""
    post_id = eosi.split("_", 1)[1] if "_" in eosi else (eosi or ad_id)
    url = f"https://www.facebook.com/ads/library/?id={post_id}&country=US"
    print(f"  -> library_url : {url}")

    # Also probe the creative directly with the kitchen sink of plausible fields,
    # in case one route returns when another doesn't.
    if creative_id:
        cdetail = graph(creative_id, {
            "fields": "id,name,object_type,effective_object_story_id,object_story_id,instagram_permalink_url,effective_instagram_media_id"
        }, token)
        if "error" not in cdetail:
            print(f"\n  raw creative dump ({creative_id}):")
            for k, v in cdetail.items():
                if k != "id":
                    print(f"    {k}: {v}")
        else:
            print(f"\n  creative endpoint error: {cdetail['error'].get('message')}")


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
