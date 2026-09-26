"""
studios.py
Shared loader for studios.json — the canonical per-studio registry. Every
fetch_*.py script that needs studio identity/config reads through here instead
of hardcoding its own list.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
STUDIOS_PATH = REPO_ROOT / "studios.json"


def load_registry() -> dict:
    with open(STUDIOS_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_studios() -> list[dict]:
    return load_registry()["studios"]


def defaults() -> dict:
    return load_registry()["defaults"]


def by_code() -> dict[str, dict]:
    return {s["code"]: s for s in load_studios() if s.get("code")}


def by_name() -> dict[str, dict]:
    return {s["name"]: s for s in load_studios()}


def meta_studio_rows() -> list[dict]:
    """Shape expected by match_studios(): {code, name, state, match}.

    Includes any studio with a `code` OR a `meta.match` — not just ones with a
    match keyword — because match_studios() checks the code embedded in the ad/
    adset name first (e.g. "26-FL-018-01 Open" -> FL-018) and only falls back to
    the keyword. A studio with a code but no match keyword is still matchable.
    """
    rows = []
    for s in load_studios():
        code = s.get("code")
        match = s.get("meta", {}).get("match")
        if not code and not match:
            continue
        rows.append({"code": code, "name": s["name"], "state": s.get("state"), "match": match})
    return rows


def match_studios(name: str, studios: list[dict]) -> list[dict]:
    """Match an ad/adset name to one or MORE studios, for ad sets shared between
    studios (e.g. "26-FL-012+FL-014-01 - Miramar/Pembroke"). Every studio code
    embedded in the name is returned, in the order it appears in the name, and
    callers split spend/metrics evenly across them (see split_evenly). With no
    code in the name, falls back to the first keyword `match` substring hit.
    """
    n = name or ""
    hits = [(n.find(s["code"]), s) for s in studios if s.get("code") and s["code"] in n]
    if hits:
        out, seen = [], set()
        for _, s in sorted(hits, key=lambda h: h[0]):
            if s["code"] not in seen:
                seen.add(s["code"])
                out.append(s)
        return out
    n_lower = n.lower()
    for s in studios:
        if s.get("match") and s["match"].lower() in n_lower:
            return [s]
    return []


def split_evenly(value, n: int, ndigits: int = 2) -> list:
    """Split `value` into n even shares whose sum is exactly `value` (any rounding
    remainder goes to the first share). n == 1 returns [value] unchanged, so
    single-studio ads keep their original int/float types."""
    if n <= 1:
        return [value]
    shares = [round(value / n, ndigits)] * n
    shares[0] = round(value - sum(shares[1:]), ndigits)
    return shares


def meta_dedicated_accounts() -> list[dict]:
    """Studios with their own Meta ad account (migrated off the shared corporate
    account). All spend/leads in that account belong 100% to the studio — no
    name-matching needed. Shape: {code, name, account_id}.

    `meta.ad_account_id` may be a single account ID or a list of them (e.g. a
    studio that moved to a new ad account mid-campaign) — one row per account
    is returned, all tagged with the same code, so fetch_all_marketing.py pulls
    each account separately and nso-studios.html's metaFilter() sums them."""
    rows = []
    for s in load_studios():
        acct = s.get("meta", {}).get("ad_account_id")
        if not acct:
            continue
        accts = acct if isinstance(acct, list) else [acct]
        for a in accts:
            rows.append({"code": s.get("code"), "name": s["name"], "account_id": a})
    return rows


def google_ads_campaign_map() -> list[tuple[re.Pattern, str]]:
    """Shape expected by fetch_google_ads.py's studio_from_campaign(): [(compiled_regex, canonical_name)].

    A studio's `google_ads.pattern` is used as a true regex if present, otherwise
    its `google_ads.match` is used as a plain (regex-escaped) substring.
    """
    entries = []
    for s in load_studios():
        ga = s.get("google_ads", {})
        pattern = ga.get("pattern")
        match = ga.get("match")
        if pattern:
            entries.append((re.compile(pattern, re.IGNORECASE), s["name"]))
        elif match:
            entries.append((re.compile(re.escape(match), re.IGNORECASE), s["name"]))
    return entries


def gbp_studio_rows() -> list[dict]:
    """Shape expected by fetch_gbp.py's ALL_STUDIOS: {name, code, location_id},
    with the 'SWEAT440 ' prefix restored (that script's convention) and using
    gbp.code (its own code scheme) if set, else falling back to the main code."""
    rows = []
    for s in load_studios():
        gbp = s.get("gbp", {})
        rows.append({
            "name": f"SWEAT440 {s['name']}",
            "code": gbp.get("code") or s.get("code") or "",
            "location_id": gbp.get("location_id") or "",
            "status": s.get("status", "open"),
        })
    return rows


def ga4_page_slugs() -> list[str]:
    """Flat list of every studio's ga4.studio_page_path, for fetch_ga4.py's
    STUDIO_PAGE_SLUGS. Studios with no path set are omitted."""
    return [s["ga4"]["studio_page_path"] for s in load_studios()
            if s.get("ga4", {}).get("studio_page_path")]


def social_insights_rows() -> list[dict]:
    """Shape expected by fetch_social_insights.py's STUDIOS: {name, code, page_id,
    ig_id}. `code` here is social_slug (thumbnail-dir slug), not the main code.
    Only studios with a social_slug set are included."""
    rows = []
    for s in load_studios():
        if not s.get("social_slug"):
            continue
        meta = s.get("meta", {})
        rows.append({
            "name": s["name"],
            "code": s["social_slug"],
            "page_id": meta.get("page_id"),
            "ig_id": meta.get("instagram_account_id"),
        })
    return rows


def tier_pricing_by_code() -> dict[str, dict]:
    """{code: tier_pricing} for studios with tier_pricing set — for fetch_tier_rmr_v2.py's
    STUDIO_PRICING."""
    return {s["code"]: s["tier_pricing"] for s in load_studios()
            if s.get("code") and s.get("tier_pricing")}


def snowflake_id_by_code() -> dict[str, int]:
    """{code: snowflake_id} for studios with snowflake_id set — for fetch_tier_rmr_v2.py's
    SNOWFLAKE_IDS."""
    return {s["code"]: s["snowflake_id"] for s in load_studios()
            if s.get("code") and s.get("snowflake_id")}


def nso_studios_by_snowflake_id() -> dict[int, dict]:
    """{snowflake_id: {name: "SWEAT440 X", code}} for fetch_nso_sales.py's NSO_STUDIOS.
    Includes status=='nso' studios plus any open studio with nso_track==true (studios
    that graduated from presale but still need tracking in the NSO dashboard)."""
    return {
        s["snowflake_id"]: {"name": f"SWEAT440 {s['name']}", "code": s["code"]}
        for s in load_studios()
        if (s.get("status") == "nso" or s.get("nso_track"))
        and s.get("snowflake_id") and s.get("code")
    }


def ig_social_code_by_code() -> dict[str, str]:
    """{code: social_slug} for studios that have both set — used by
    build_all_scorecards.py's IG_SOCIAL_CODE to join against social_insights.json."""
    return {s["code"]: s["social_slug"] for s in load_studios()
            if s.get("code") and s.get("social_slug")}


def next_code(state: str) -> str:
    """Suggest the next sequential {STATE}-{NNN} code for a given state, for the
    add-studio widget. Purely advisory — the caller may override it."""
    existing = [s["code"] for s in load_studios() if (s.get("code") or "").startswith(f"{state}-")]
    nums = [int(c.split("-")[1]) for c in existing if c.split("-")[1].isdigit()]
    return f"{state}-{(max(nums) + 1):03d}" if nums else f"{state}-001"
