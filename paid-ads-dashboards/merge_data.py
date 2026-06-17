import json, pathlib, yaml

ROOT      = pathlib.Path(__file__).parent
new_file  = ROOT / "paid-ads-data.json"        # freshly fetched campaign(s)
base_file = ROOT / "daniel-paid-ads-data.json" # existing merged file
cfg_file  = ROOT / "config.yaml"

new_data  = json.loads(new_file.read_text(encoding="utf-8"))
base_data = json.loads(base_file.read_text(encoding="utf-8"))
cfg       = yaml.safe_load(cfg_file.read_text(encoding="utf-8"))

# Only overwrite campaigns that were actually re-fetched from the API;
# cached campaigns (not in campaigns_to_refresh) keep their existing data in
# daniel-paid-ads-data.json — this preserves manually-rebuilt concept maps.
refresh_set = set(cfg.get("campaigns_to_refresh", []))

# campaigns is a dict keyed by campaign name
base_camps = base_data.get("campaigns", {})
new_camps  = new_data.get("campaigns", {})

# Overwrite with fresh data only for refreshed campaigns
for key, camp in new_camps.items():
    if key in refresh_set:
        base_camps[key] = camp
        print(f"  Updated: {key}  ({len(camp.get('ads', []))} ads, {camp['totals']['leads']} leads)")
    else:
        print(f"  Kept cached: {key}  (not in campaigns_to_refresh)")

# daily_ad_studio is a top-level array used by the ETL internally;
# the dashboard reads from campaign.daily_series instead, so we just
# keep whatever the fresh fetch produced (avoids duplicates growing).
merged_das = new_data.get("daily_ad_studio", [])
print(f"  daily_ad_studio: {len(merged_das)} rows (fresh)")

# Rebuild campaigns_index from actual campaign data (fixes stale spend/leads)
# Most-recently-started campaign becomes the default
all_keys = list(base_camps.keys())
# Sort by date_start descending to find most recent
sorted_keys = sorted(all_keys,
    key=lambda k: base_camps[k].get("date_start", ""), reverse=True)
default_key = sorted_keys[0]  # most recent campaign

# Prefer the active_campaign set by the ETL/config; fall back to most-recent by date
active_key = new_data.get("active_campaign", default_key)
if active_key not in base_camps:
    active_key = default_key
print(f"  active campaign: {active_key}")

campaigns_index = []
for key in all_keys:
    c = base_camps[key]
    campaigns_index.append({
        "key":          key,
        "display_name": c["display_name"],
        "period_label": c["period_label"],
        "date_start":   c["date_start"],
        "date_end":     c["date_end"],
        "leads":        c["totals"]["leads"],
        "spend":        c["totals"]["spend"],
        "is_default":   key == active_key,
    })

result = {
    **base_data,
    "active_campaign":  active_key,
    "campaigns_index":  campaigns_index,
    "campaigns":        base_camps,
    "daily_ad_studio":  merged_das,
}

out = ROOT / "daniel-paid-ads-data.json"
out.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
print(f"Wrote {out}  ({out.stat().st_size:,} bytes, {len(base_camps)} campaigns)")
