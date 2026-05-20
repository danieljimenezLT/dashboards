#!/usr/bin/env python3
"""
test_library_url.py
-------------------
Validates the library URL construction logic against the live Meta API
without running the full fetcher. Call with one or more ad_ids:

    python test_library_url.py 120248075197290249 120245634501850249

Output for each ad:
    ad_id          : 120248075197290249
    name           : 26-FL-XXX - VC - PYC - Video - StillWondering
    eosi           : 123456789_1942589402912793
    post_id        : 1942589402912793
    library_url    : https://www.facebook.com/ads/library/?id=1942589402912793&country=US
"""
import os
import sys
import requests

API_VERSION = "v20.0"
BASE_URL    = f"https://graph.facebook.com/{API_VERSION}"

def fetch_ad(ad_id: str, token: str) -> dict:
    r = requests.get(
        f"{BASE_URL}/{ad_id}",
        params={
            "access_token": token,
            "fields": "id,name,status,effective_object_story_id",
        },
        timeout=30,
    )
    return r.json()

def build_library_url(eosi: str, ad_id: str) -> str:
    post_id = eosi.split("_", 1)[1] if eosi and "_" in eosi else (eosi or ad_id)
    return f"https://www.facebook.com/ads/library/?id={post_id}&country=US"

def main(ad_ids: list[str]) -> int:
    token = os.environ.get("META_TOKEN")
    if not token:
        print("ERROR: META_TOKEN not set", file=sys.stderr)
        return 2

    for ad_id in ad_ids:
        data = fetch_ad(ad_id, token)
        if "error" in data:
            print(f"ad_id        : {ad_id}")
            print(f"  ERROR      : {data['error'].get('message')}")
            print()
            continue
        eosi = data.get("effective_object_story_id", "") or ""
        post_id = eosi.split("_", 1)[1] if "_" in eosi else (eosi or "(none)")
        url = build_library_url(eosi, ad_id)
        print(f"ad_id        : {ad_id}")
        print(f"name         : {data.get('name','')}")
        print(f"status       : {data.get('status','')}")
        print(f"eosi         : {eosi or '(none)'}")
        print(f"post_id      : {post_id}")
        print(f"library_url  : {url}")
        print()
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["120248075197290249"]))
