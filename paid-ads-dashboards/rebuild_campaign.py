"""
rebuild_campaign.py
────────────────────────────────────────────────────────────────────
Rebuilds daily_series for one campaign from existing daily_ad_studio
rows WITHOUT calling the Meta API. Use this when concept detection
has been fixed but the data doesn't need to be re-fetched.

Usage:
  python rebuild_campaign.py win_a_free_month_apr2026
  python rebuild_campaign.py free_class_open_studios_2026
"""
import json, sys, logging
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]  %(message)s")
log = logging.getLogger(__name__)

ROOT      = Path(__file__).parent
DATA_FILE = ROOT / "daniel-paid-ads-data.json"

import yaml
from fetch_paid_ads import match_audience, match_pillar, detect_concept

CAMPAIGN_KEY = sys.argv[1] if len(sys.argv) > 1 else "win_a_free_month_apr2026"
CONFIG_FILE  = ROOT / "config.yaml"

def safe_float(v):
    try:    return float(v or 0)
    except: return 0.0

def _zero():
    return {"spend": 0.0, "impressions": 0, "clicks": 0, "leads": 0, "trials": 0, "purchases": 0}

def _finish(b):
    b["cpl"] = round(b["spend"] / b["leads"],     2) if b["leads"]     else 0
    b["cpt"] = round(b["spend"] / b["trials"],    2) if b["trials"]    else 0
    b["cpp"] = round(b["spend"] / b["purchases"], 2) if b["purchases"] else 0
    b["ctr"] = round(b["clicks"] / b["impressions"] * 100,  2) if b["impressions"] else 0
    b["cpm"] = round(b["spend"]  / b["impressions"] * 1000, 2) if b["impressions"] else 0
    return b

log.info(f"Loading {DATA_FILE.name} ...")
data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
camp = data["campaigns"][CAMPAIGN_KEY]

cfg     = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))
c_cfg   = cfg["campaigns"][CAMPAIGN_KEY]
aud_tokens    = c_cfg.get("audience_tokens", {})
pillar_tokens = c_cfg.get("pillar_tokens",   {})
aud_flat      = {t for toks in aud_tokens.values() for t in toks}
pillar_flat   = {t for toks in pillar_tokens.values() for t in toks}
studios       = c_cfg.get("studios", [])
studio_by_match = {s["match"].upper(): s for s in studios}

def find_studio(ad_name):
    upper = ad_name.upper()
    for match_str, s in studio_by_match.items():
        if match_str in upper:
            return s
    return None

# Rebuild ad_dims with fixed detect_concept
ad_dims = {}
for ad in camp.get("ads", []):
    ad_id = ad.get("ad_id")
    if not ad_id:
        continue
    name   = ad.get("name", "")
    studio = find_studio(name)
    sc_    = ad.get("studio_code") or (studio["code"] if studio else None)
    aud_   = match_audience(name, aud_tokens)
    pil_   = match_pillar(name, pillar_tokens)
    con_   = detect_concept(
        name,
        studio_match=studio["match"] if studio else None,
        audience_tokens_flat=aud_flat,
        pillar_tokens_flat=pillar_flat,
        state_code=studio["state"] if studio else None,
    )
    ad_dims[ad_id] = {
        "studio_code": sc_,
        "audience":    aud_,
        "pillar":      pil_,
        "concept":     con_,
        "media_type":  ad.get("media_type"),
    }

log.info(f"Ad dims rebuilt: {len(ad_dims)} ads")
concepts = sorted(set(d["concept"] for d in ad_dims.values()))
log.info(f"Concepts: {concepts}")

# Filter daily_ad_studio to this campaign
camp_ad_ids = set(ad_dims.keys())
all_das     = data.get("daily_ad_studio", [])
camp_das    = [r for r in all_das if r.get("ad_id") in camp_ad_ids]
log.info(f"daily_ad_studio rows for this campaign: {len(camp_das)}")

# Rebuild daily_series
ds_studio   = defaultdict(lambda: {**_zero(), "reach": 0})
ds_audience = defaultdict(_zero)
ds_pillar   = defaultdict(_zero)
ds_concept  = defaultdict(_zero)
ds_media    = defaultdict(_zero)
ds_stu_aud  = defaultdict(_zero)
ds_stu_pil  = defaultdict(_zero)
ds_stu_con  = defaultdict(_zero)
ds_stu_med  = defaultdict(_zero)

for row in camp_das:
    ad_id = row.get("ad_id")
    dims  = ad_dims.get(ad_id)
    if not dims:
        continue
    d_   = row.get("date")
    sc_  = dims["studio_code"]
    aud_ = dims["audience"]
    pil_ = dims["pillar"]
    con_ = dims["concept"]
    mt_  = dims["media_type"] or "unknown"

    sp = safe_float(row.get("spend"))
    im = int(safe_float(row.get("impressions")))
    cl = int(safe_float(row.get("clicks")))
    rc = int(safe_float(row.get("reach", 0)))
    le = int(row.get("leads", 0))
    tr = int(row.get("trials", 0))
    pu = int(row.get("purchases", 0))

    def _add(b, has_reach=False):
        b["spend"]       += sp
        b["impressions"] += im
        b["clicks"]      += cl
        b["leads"]       += le
        b["trials"]      += tr
        b["purchases"]   += pu
        if has_reach:
            b["reach"]   += rc

    _add(ds_studio[(d_, sc_)], has_reach=True)
    if aud_: _add(ds_audience[(d_, aud_)])
    if pil_: _add(ds_pillar[(d_, pil_)])
    if con_ and con_ != "(other)": _add(ds_concept[(d_, con_)])
    _add(ds_media[(d_, mt_)])
    if aud_: _add(ds_stu_aud[(d_, sc_, aud_)])
    if pil_: _add(ds_stu_pil[(d_, sc_, pil_)])
    if con_ and con_ != "(other)": _add(ds_stu_con[(d_, sc_, con_)])
    _add(ds_stu_med[(d_, sc_, mt_)])

win_start = camp.get("date_start", "")
win_end   = camp.get("date_end", "")
win_days  = len({r["date"] for r in camp_das})

daily_series = {
    "window_start": win_start,
    "window_end":   win_end,
    "window_days":  win_days,
    "campaign":     camp["display_name"],
    "by_studio": [
        {"studio_code": sc_, "date": d_, **_finish(dict(b))}
        for (d_, sc_), b in sorted(ds_studio.items())
    ],
    "by_audience": [
        {"audience": aud_, "date": d_, **_finish(dict(b))}
        for (d_, aud_), b in sorted(ds_audience.items())
    ],
    "by_pillar": [
        {"pillar": pil_, "date": d_, **_finish(dict(b))}
        for (d_, pil_), b in sorted(ds_pillar.items())
    ],
    "by_concept": [
        {"concept": con_, "date": d_, **_finish(dict(b))}
        for (d_, con_), b in sorted(ds_concept.items())
    ],
    "by_media_type": [
        {"media_type": mt_, "date": d_, **_finish(dict(b))}
        for (d_, mt_), b in sorted(ds_media.items())
    ],
    "by_studio_audience": [
        {"studio_code": sc_, "audience": aud_, "date": d_, **_finish(dict(b))}
        for (d_, sc_, aud_), b in sorted(ds_stu_aud.items())
    ],
    "by_studio_pillar": [
        {"studio_code": sc_, "pillar": pil_, "date": d_, **_finish(dict(b))}
        for (d_, sc_, pil_), b in sorted(ds_stu_pil.items())
    ],
    "by_studio_concept": [
        {"studio_code": sc_, "concept": con_, "date": d_, **_finish(dict(b))}
        for (d_, sc_, con_), b in sorted(ds_stu_con.items())
    ],
    "by_studio_media_type": [
        {"studio_code": sc_, "media_type": mt_, "date": d_, **_finish(dict(b))}
        for (d_, sc_, mt_), b in sorted(ds_stu_med.items())
    ],
}

n_con = len(daily_series["by_concept"])
log.info(f"daily_series rebuilt: {win_days} days | {n_con} concept-day rows")

# Rebuild campaign daily totals
daily_map = {}
for r in daily_series["by_studio"]:
    d_ = r["date"]
    if d_ not in daily_map:
        daily_map[d_] = {"date": d_, "spend": 0.0, "impressions": 0, "clicks": 0, "reach": 0, "leads": 0, "trials": 0, "purchases": 0}
    row = daily_map[d_]
    for k in ["spend", "impressions", "clicks", "reach", "leads", "trials", "purchases"]:
        row[k] += r.get(k, 0)
daily_out = sorted(daily_map.values(), key=lambda x: x["date"])

# Recalculate totals
totals = {k: 0 for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]}
totals["spend"] = 0.0
for row in camp_das:
    sc_ = ad_dims.get(row.get("ad_id"), {}).get("studio_code")
    if not sc_:
        continue
    totals["spend"]       += safe_float(row.get("spend"))
    totals["impressions"] += int(safe_float(row.get("impressions")))
    totals["clicks"]      += int(safe_float(row.get("clicks")))
    totals["reach"]       += int(safe_float(row.get("reach", 0)))
    totals["leads"]       += int(row.get("leads", 0))
    totals["trials"]      += int(row.get("trials", 0))
    totals["purchases"]   += int(row.get("purchases", 0))
totals["spend"] = round(totals["spend"], 2)
totals["ctr"]   = round(totals["clicks"] / totals["impressions"] * 100,  2) if totals["impressions"] else 0
totals["cpm"]   = round(totals["spend"]  / totals["impressions"] * 1000, 2) if totals["impressions"] else 0
totals["cpl"]   = round(totals["spend"]  / totals["leads"],              2) if totals["leads"] else 0
log.info(f"  totals: spend=${totals['spend']:,.2f}  leads={totals['leads']}  CPL=${totals['cpl']:.2f}")

# Write back
camp["daily"]        = daily_out
camp["daily_series"] = daily_series
camp["totals"]       = totals

import datetime as _dt
data["generated_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
DATA_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
log.info(f"Done. {DATA_FILE.name} updated ({DATA_FILE.stat().st_size:,} bytes)")
