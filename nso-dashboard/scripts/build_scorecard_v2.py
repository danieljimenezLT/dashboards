#!/usr/bin/env python3
"""
build_scorecard_v2.py

Generates nso_scorecard_data2.json using week1_start dates from the
PRE_WEEKS sheet (same source as fetch_pre_weeks.py / nso_pre_weeks_data.json)
so that week boundaries in the Test dashboard match the pre-weeks data.

All leads, presales, and spend are re-aggregated from daily rows using the
corrected week dates.  Special cases:
  - Naples  (FL-019): hardcoded NAPLES_EXPLICIT_WEEKS, unchanged
  - Reston  (VA-001): copied as-is from nso_scorecard_data.json
                      (preserves patch_reston_early_weeks.py data)
  - pricing: copied from nso_scorecard_data.json (static, date-independent)
  - tier_rmr_by_week: NOT copied — run fetch_tier_rmr_v2.py afterwards

Usage (from nso-dashboard/):
  python scripts/build_scorecard_v2.py
  python scripts/build_scorecard_v2.py --dry-run
"""

import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import build_all_scorecards as _bac

ROOT      = _bac.ROOT
REPO_ROOT = _bac.REPO_ROOT
TODAY     = date.today()

PRE_WEEKS_SHEET_ID = "1KhXfeofzltGyllmrfIm9UN2xqLUgBtjAxPhT-E0SQmk"


# ---------------------------------------------------------------------------
# Load week1_start overrides from PRE_WEEKS sheet
# ---------------------------------------------------------------------------

def _load_pw_dates(gc):
    """Return {code: {week_0_start, week_1_start}} from PRE_WEEKS sheet."""
    ws   = gc.open_by_key(PRE_WEEKS_SHEET_ID).worksheet("NSO Config")
    rows = ws.get_all_values()
    hdr  = [h.strip().lower() for h in rows[1]]

    def _col(row, name):
        try:   return row[hdr.index(name)].strip()
        except (ValueError, IndexError): return ""

    def _d(s):
        if not s or s.lower() in ("", "n/a", "tbd", "-"):
            return None
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
            try:   return datetime.strptime(s.strip(), fmt).date()
            except ValueError: continue
        return None

    out = {}
    for row in rows[2:]:
        code = _col(row, "code")
        if not code:
            continue
        out[code] = {
            "week_0_start": _d(_col(row, "week 0 start")),
            "week_1_start": _d(_col(row, "week 1 start")),
            "lt_fee_start": _d(_col(row, "lt fees start date")),
        }
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    dry_run = "--dry-run" in sys.argv

    # Google Sheets auth
    gc = None
    if _bac.CREDS_PATH.exists():
        try:
            from google.oauth2.service_account import Credentials
            import gspread
            creds = Credentials.from_service_account_file(str(_bac.CREDS_PATH), scopes=_bac.SCOPES)
            gc    = gspread.authorize(creds)
            print("Google Sheets auth OK")
        except Exception as e:
            print(f"Warning: {e}")
    if not gc:
        print("ERROR: Google Sheets credentials required")
        sys.exit(1)

    # Load studio configs (targets, IDs, milestones) from NSO Config sheet
    print("\nLoading studio config from NSO Config sheet...")
    studio_cfgs = _bac.load_studio_config(gc)
    _bac._STUDIO_CFG_CACHE.clear()
    _bac._STUDIO_CFG_CACHE.extend(studio_cfgs)
    print(f"  {len(studio_cfgs)} studios")

    # Override week1_start with PRE_WEEKS sheet dates
    print(f"\nLoading week1_start overrides from PRE_WEEKS sheet...")
    pw_dates = _load_pw_dates(gc)
    for cfg in studio_cfgs:
        code  = cfg["code"]
        if code in ("FL-019", "VA-001"):
            print(f"  {code} {cfg['name']:<25} skipped (special handling)")
            continue
        pw     = pw_dates.get(code, {})
        new_w1 = pw.get("week_1_start")
        old_w1 = cfg["week1_start"]
        if new_w1:
            cfg["week1_start"] = new_w1
            delta = (new_w1 - old_w1).days if old_w1 else None
            tag   = f"shift {delta:+d}d" if delta else "no change"
            print(f"  {code} {cfg['name']:<25} {str(old_w1):<12} → {new_w1}  ({tag})")
        else:
            print(f"  {code} {cfg['name']:<25} no PRE_WEEKS date, keeping {old_w1}")
        cfg["lt_fee_start"] = pw.get("lt_fee_start")

    # Load data files
    print("\nLoading data files...")
    with open(REPO_ROOT / "data.json", encoding="utf-8") as f:
        raw = json.load(f)
    daily_rows = raw.get("daily_detail", [])
    print(f"  data.json: {len(daily_rows)} rows")

    def _load(rel_path, label):
        try:
            with open(ROOT / rel_path, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"  {label}: not found — skipping")
            return {}

    sales_raw        = _load("nso_sales_data.json",     "nso_sales_data.json")
    social_raw       = _load("social_insights.json",    "social_insights.json")
    mkt_raw          = _load("marketing_data.json",     "marketing_data.json")
    gads_raw         = _load("nso_google_ads.json",     "nso_google_ads.json")
    spend_overrides  = _load("nso_spend_overrides.json","nso_spend_overrides.json")

    meta_rows = mkt_raw.get("meta_ads",   [])
    gads_rows = gads_raw.get("google_ads", [])

    # Build global lookups
    leads_by_date = _bac.load_daily_leads(daily_rows)
    sales_by_date = _bac.load_sales_data(sales_raw)
    ig_by_code, ig_current = _bac.load_ig_followers(social_raw)
    meta_by_code  = _bac.load_meta_spend(meta_rows)
    gads_by_code  = _bac.load_google_spend(gads_rows)

    print("\nLoading grassroots/events/other spend from NSO Config sheet...")
    nso_gr = _bac.load_events_spend_from_nso_config(gc, studio_cfgs)
    gr_spend_by_code = {}; events_by_code = {}; other_spend_by_code = {}
    for cfg in studio_cfgs:
        code = cfg["code"]
        if code in nso_gr:
            sp, ev, ot = nso_gr[code]
        else:
            sp = defaultdict(float); ev = defaultdict(int); ot = defaultdict(float)
        gr_spend_by_code[code]    = sp
        events_by_code[code]      = ev
        other_spend_by_code[code] = ot

    # Load date-range spend entries from nso_other_costs.json (Google Form responses)
    print("\nLoading spend entries from nso_other_costs.json...")
    _oc_gr_by_name    = {}  # {full_name: [(start_date, end_date, amount)]}
    _oc_other_by_name = {}
    try:
        with open(ROOT / "nso_other_costs.json", encoding="utf-8") as _f:
            _oc = json.load(_f)
        for _e in _oc.get("adhoc", []):
            _cat = _e.get("category", "").lower()
            if _cat not in ("grassroots", "other"):
                continue
            try:
                _sd = date.fromisoformat(_e["start"])
                _ed = date.fromisoformat(_e["end"])
            except (KeyError, ValueError):
                continue
            _amt = float(_e.get("total") or 0)
            if _amt <= 0:
                continue
            _dest = _oc_gr_by_name if _cat == "grassroots" else _oc_other_by_name
            _dest.setdefault(_e.get("studio_key", ""), []).append((_sd, _ed, _amt))
        _oc_total = sum(len(v) for v in _oc_gr_by_name.values()) + sum(len(v) for v in _oc_other_by_name.values())
        print(f"  {_oc_total} entries across {len(_oc_gr_by_name)} grassroots + {len(_oc_other_by_name)} other studios")
        # Monthly recurring entries
        _oc_lt_disc_by_name       = {}
        _oc_monthly_other_by_name = {}
        for _e in _oc.get("monthly", []):
            try:
                _sd = date.fromisoformat(_e["start"])
                _ed = date.fromisoformat(_e["end"])
            except (KeyError, ValueError):
                continue
            _monthly = float(_e.get("monthly") or 0)
            if _monthly <= 0:
                continue
            _cat = _e.get("category", "").lower()
            _key = _e.get("studio_key", "")
            if "leadteam discount" in _cat:
                _oc_lt_disc_by_name.setdefault(_key, []).append((_sd, _ed, _monthly))
            else:
                _oc_monthly_other_by_name.setdefault(_key, []).append((_sd, _ed, _monthly))
        _mo_total = sum(len(v) for v in _oc_lt_disc_by_name.values()) + sum(len(v) for v in _oc_monthly_other_by_name.values())
        print(f"  {_mo_total} monthly entries ({len(_oc_lt_disc_by_name)} lt-discount + {len(_oc_monthly_other_by_name)} other studios)")
    except FileNotFoundError:
        print("  nso_other_costs.json not found — skipping")
        _oc_lt_disc_by_name       = {}
        _oc_monthly_other_by_name = {}

    # Load pre-launch week date ranges for spend allocation to negative weeks.
    try:
        with open(ROOT / "nso_pre_weeks_data.json", encoding="utf-8") as _f:
            _pw_studios = json.load(_f).get("studios", {})
        print(f"  Loaded nso_pre_weeks_data.json ({len(_pw_studios)} studios with pre-launch data)")
    except FileNotFoundError:
        _pw_studios = {}
        print("  nso_pre_weeks_data.json not found — pre-launch spend will go to Week 0")

    # Load existing v1 file — for Reston copy, static pricing, and comm_events
    prev_map = {}
    v1_path  = ROOT / "nso_scorecard_data.json"
    if v1_path.exists():
        with open(v1_path, encoding="utf-8") as f:
            prev_map = {s["code"]: s for s in json.load(f).get("studios", [])}
        print(f"\nLoaded nso_scorecard_data.json ({len(prev_map)} studios)")

    # Build comm_events lookup from v1 by date range so v2 can inherit them.
    # v1 is already patched by fetch_sheet_milestones.py; v2 uses different week
    # boundaries so we match by date overlap instead of week number.
    def _v1_comm_events(code, ws_date, we_date):
        """Return comm_events from v1 for the week overlapping [ws_date, we_date].
        Week 0 has ws_date=None; match v1 Week 0 by date_end."""
        s1 = prev_map.get(code)
        if not s1:
            return None
        best = None
        for w1 in s1.get("weeks", []):
            d0 = w1.get("date_start"); d1 = w1.get("date_end")
            if not d1:
                continue
            try:
                w1e = date.fromisoformat(d1)
            except ValueError:
                continue
            if ws_date is None:
                # Week 0: match v1 Week 0 (date_start=None) by date_end
                if d0 is None and we_date and w1e == we_date:
                    ev = w1.get("comm_events")
                    if ev is not None:
                        best = ev
                        break
                continue
            if not d0:
                continue
            try:
                w1s = date.fromisoformat(d0)
            except ValueError:
                continue
            if w1s <= we_date and w1e >= ws_date:
                ev = w1.get("comm_events")
                if ev is not None:
                    best = ev
                    break
        return best

    # ---------------------------------------------------------------------------
    # Build scorecards
    # ---------------------------------------------------------------------------
    output_studios = []

    for cfg in studio_cfgs:
        full  = cfg["full_name"]
        dname = cfg["data_name"]
        code  = cfg["code"]

        # Reston: copy as-is (preserves patch_reston_early_weeks data)
        if code == "VA-001":
            if code in prev_map:
                output_studios.append(prev_map[code])
                print(f"\n  VA-001 Reston: copied from nso_scorecard_data.json")
            else:
                print(f"\n  VA-001 Reston: not found in v1, skipping")
            continue

        go_wk     = cfg["go_week"] or 28
        co_wk     = cfg["co_week"]
        num_weeks = max(go_wk + 3, 28)
        _w1 = cfg["week1_start"] or (_bac.NAPLES_EXPLICIT_WEEKS[1][0] if code == "FL-019" else None)
        if _w1:
            num_weeks = max(num_weeks, max(0, (TODAY - _w1).days // 7) + 3)

        print(f"\n{'='*60}\n  {full}  (data_name={dname!r})\n{'='*60}")

        is_naples = code == "FL-019"
        if is_naples:
            bounds = _bac.make_naples_bounds(num_weeks)
        elif not cfg["week1_start"]:
            # No week1_start — auto-detect from first Snowflake data row.
            # Skip leading dates isolated by a 30+ day gap (outlier leads far before
            # real activity, same logic as fetch_pre_weeks.py).
            all_dates = sorted(leads_by_date.get(dname, {}).keys())
            if not all_dates:
                print(f"  Skipping {full}: no week1_start and no Snowflake data found")
                continue
            while len(all_dates) >= 2:
                d0 = date.fromisoformat(all_dates[0])
                d1 = date.fromisoformat(all_dates[1])
                if (d1 - d0).days > 30:
                    all_dates.pop(0)
                else:
                    break
            auto_w1 = date.fromisoformat(all_dates[0])
            cfg["week1_start"] = auto_w1
            num_weeks = max(num_weeks, max(0, (TODAY - auto_w1).days // 7) + 3)
            print(f"  Auto-detected week1_start={auto_w1} from first Snowflake row")
            bounds = _bac.make_sun_sat_bounds(auto_w1, num_weeks)
        else:
            bounds = _bac.make_sun_sat_bounds(cfg["week1_start"], num_weeks)

        # Prepend pre-launch (negative) week bounds so spend entries before Week 1
        # are allocated to the correct calendar week rather than discarded.
        _pw_list = _pw_studios.get(code, {}).get("pre_weeks", [])
        if _pw_list:
            _pre_bounds = [
                (pw["week_num"], date.fromisoformat(pw["date_start"]), date.fromisoformat(pw["date_end"]))
                for pw in sorted(_pw_list, key=lambda x: x["week_num"])
            ]
            bounds = _pre_bounds + bounds

        ig_fc_s       = ig_by_code.get(code, {})
        meta_spend_d  = meta_by_code.get(code, {})
        gads_spend_d  = gads_by_code.get(code, {})
        gr_spend_d    = defaultdict(float, gr_spend_by_code.get(code, {}))
        events_d      = events_by_code.get(code, defaultdict(int))
        other_spend_d = other_spend_by_code.get(code, defaultdict(float))

        # Merge date-range spend from nso_other_costs.json into gr/other spend dicts.
        # Week 0 (ws=None) gets a synthetic start = day after the last negative week
        # ends (or date.min if no negative weeks), so spend already allocated to a
        # negative week is not double-counted into Week 0.
        other_spend_d = defaultdict(float, other_spend_d)
        _pre_ends = [_we for _wn, _ws, _we in bounds if _wn < 0]
        _w0_start_eff = (max(_pre_ends) + timedelta(days=1)) if _pre_ends else date.min
        for _oc_entries, _dest in (
            (_oc_gr_by_name.get(full, []),    gr_spend_d),
            (_oc_other_by_name.get(full, []), other_spend_d),
        ):
            if not _oc_entries:
                continue
            for _wn, _ws, _we in bounds:
                _ws_eff = _ws if _ws is not None else _w0_start_eff
                for _sd, _ed, _amt in _oc_entries:
                    if _sd <= _we and _ed >= _ws_eff:
                        _span    = max(1, (_ed - _sd).days + 1)
                        _overlap = (min(_ed, _we) - max(_sd, _ws_eff)).days + 1
                        _dest[_wn] += round(_amt * _overlap / _span, 2)

        # Monthly other spend (external agency, etc.) — prorated at monthly/30.44 per day
        _mo_other = _oc_monthly_other_by_name.get(full, [])
        if _mo_other:
            for _wn, _ws, _we in bounds:
                _ws_eff = _ws if _ws is not None else _w0_start_eff
                for _sd, _ed, _monthly in _mo_other:
                    if _sd <= _we and _ed >= _ws_eff:
                        _overlap = (min(_ed, _we) - max(_sd, _ws_eff)).days + 1
                        other_spend_d[_wn] += round(_monthly * _overlap / 30.44, 2)

        cw_num    = _bac.current_week_num(bounds)
        cum_leads = cum_presales = cum_canc = 0
        weeks_out = []

        for wn, ws_date, we_date in bounds:
            w = _bac.sum_week_data(
                wn, ws_date, we_date, dname, full,
                leads_by_date, sales_by_date, ig_fc_s,
                meta_spend_d, gads_spend_d,
                gr_spend_by_week=gr_spend_d,
                events_by_week=events_d,
                other_spend_by_week=other_spend_d,
            )
            cum_leads    += w["leads"]
            cum_presales += w["presales"]
            cum_canc     += w["cancellations"]

            if wn == 0:
                wk_label = "Week 0"
                dr = (f"Pre {cfg['week1_start'].month}/{cfg['week1_start'].day}"
                      if cfg["week1_start"] else "Pre")
            else:
                wk_label = f"WEEK {wn}"
                dr = (_bac.NAPLES_SPECIAL_LABELS[wn]
                      if is_naples and wn in _bac.NAPLES_SPECIAL_LABELS
                      else _bac.fmt_dr(ws_date, we_date))

            tl = float(cum_leads)    if cum_leads    > 0 else (0.0 if wn == 0 else None)
            pc = float(cum_presales) if cum_presales > 0 else (0.0 if wn == 0 else None)
            cc = float(cum_canc)     if cum_canc     > 0 else (0.0 if wn == 0 else None)
            is_active = ws_date is not None and ws_date <= TODAY
            is_past = we_date is not None and we_date <= TODAY

            # Default-arg trick captures is_active at definition time (avoids late-binding)
            def _wi(v, fb=None, _a=is_active): return v if v > 0 else (0 if _a else fb)
            def _wf(v, fb=None, _a=is_active): return round(v, 2) if v > 0 else (0.0 if _a else fb)

            conv_rate = (round(w["presales"] / w["leads"] * 100, 1)
                         if w["leads"] > 0 and is_active else None)

            ov_pre = spend_overrides.get(code, {}).get("leadteam_pre")
            lt_start = cfg.get("lt_fee_start")
            if wn == 0 and ov_pre:
                lt_fee = float(ov_pre)
            elif is_active and wn >= 1:
                if lt_start and we_date < lt_start:
                    lt_fee = None          # week entirely before fee start
                elif lt_start and ws_date < lt_start:
                    lt_fee = _bac.calc_leadteam_fee(lt_start, we_date)  # prorate
                else:
                    lt_fee = _bac.calc_leadteam_fee(ws_date, we_date)
            else:
                lt_fee = None

            # Apply monthly Leadteam Discount — reduces lt_fee (floor 0), prorated daily
            if lt_fee is not None and ws_date and we_date:
                _lt_disc_entries = _oc_lt_disc_by_name.get(full, [])
                for _sd, _ed, _monthly in _lt_disc_entries:
                    if _sd <= we_date and _ed >= ws_date:
                        _overlap = (min(_ed, we_date) - max(_sd, ws_date)).days + 1
                        lt_fee = max(0.0, round(lt_fee - _monthly * _overlap / 30.44, 2))

            meta_sp  = _wf(w["meta_spend"])
            gads_sp  = _wf(w["gads_spend"])
            gr_sp    = _wf(w["gr_spend"])
            other_sp = _wf(w["other_spend"])

            if lt_fee or (is_active and wn >= 1):
                total_spend = round(
                    (meta_sp or 0)+(gads_sp or 0)+(gr_sp or 0)+(other_sp or 0)+(lt_fee or 0), 2
                ) or None
            elif is_past and wn <= 0 and (gr_sp or other_sp):
                # Pre-launch / Week-0: only grassroots + other spend (no digital)
                total_spend = round((gr_sp or 0) + (other_sp or 0), 2) or None
            else:
                total_spend = None

            blended_cpl = (round(total_spend / w["leads"],    2)
                           if total_spend and w["leads"]    > 0 else None)
            blended_cpa = (round(total_spend / w["presales"], 2)
                           if total_spend and w["presales"] > 0 else None)

            entry = {
                "week":                       wk_label,
                "date_range":                 dr,
                "date_start":                 ws_date.isoformat() if ws_date else None,
                "date_end":                   we_date.isoformat(),
                "new_leads":                  _wi(w["leads"]),
                "total_leads":                tl,
                "digital_leads_week":         _wi(w["digital_leads"]),
                "presales_week":              _wi(w["presales"]),
                "presales_count":             pc,
                "cancellations_week":         _wi(w["cancellations"]),
                "cancellations_count":        cc,
                "grassroots_leads":           _wi(w["gr_leads"]),
                "grassroots_presales":        _wi(w["gr_presales"]),
                "digital_presales_week":      _wi(w["digital_presales"]),
                "digital_cancellations_week": _wi(w["digital_cancellations"]),
                "conversion_rate":            conv_rate,
                "comm_events":                _v1_comm_events(code, ws_date, we_date),
                "ig_new_followers":           w["ig"],
                "meta_spend":                 meta_sp  if (is_active and wn >= 1) else None,
                "google_spend":               gads_sp  if (is_active and wn >= 1) else None,
                "grassroots_spend":           gr_sp    if ((is_active and wn >= 1) or (is_past and wn <= 0)) else None,
                "other_spend":                other_sp if ((is_active and wn >= 1) or (is_past and wn <= 0)) else None,
                "leadteam_fee":               lt_fee,
                "total_marketing_spend":      total_spend,
                "blended_cpl":                blended_cpl,
                "blended_cpa":                blended_cpa,
                "estimated_day1_rmr":         None,
            }

            # Apply manual spend overrides
            ov     = spend_overrides.get(code, {})
            wk_str = str(wn)
            for sk in ("meta_spend", "google_spend", "grassroots_spend", "other_spend"):
                if wk_str in ov.get(sk, {}):
                    entry[sk] = ov[sk][wk_str]
            if any(wk_str in ov.get(k, {}) for k in ("meta_spend","google_spend","grassroots_spend","other_spend")):
                new_total = sum(entry.get(k) or 0
                                for k in ("meta_spend","google_spend","grassroots_spend","other_spend","leadteam_fee"))
                entry["total_marketing_spend"] = new_total or None
                entry["blended_cpl"] = round(new_total/w["leads"],    2) if w["leads"]    > 0 else None
                entry["blended_cpa"] = round(new_total/w["presales"], 2) if w["presales"] > 0 else None

            weeks_out.append(entry)
            print(f"  W{wn:02d} {dr[:22]:<23} +{w['leads']:4d}L +{w['presales']:3d}P "
                  f"-{w['cancellations']:2d}C  spend=${total_spend or 0:8.2f}")

        # Attribute IG followers not in API window to Week 0
        cf_total = ig_current.get(code)
        if cf_total and weeks_out and code != "VA-001":
            accounted = sum(w.get("ig_new_followers") or 0 for w in weeks_out)
            gap = max(0, cf_total - accounted)
            if gap:
                w0 = next((w for w in weeks_out if w["week"] == "Week 0"), None)
                if w0:
                    w0["ig_new_followers"] = (w0["ig_new_followers"] or 0) + gap
                    print(f"  IG gap {gap} added to Week 0")

        s_entry = {
            "name":                 cfg["name"],
            "code":                 code,
            "full_name":            full,
            "data_name":            cfg["data_name"],
            "targets":              cfg["targets"],
            "co_week":              co_wk,
            "go_week":              go_wk,
            "opening_date":         cfg["opening_date"].isoformat() if cfg["opening_date"] else None,
            "goal_announce_target": cfg.get("goal_announce_target"),
            "milestones":           cfg.get("milestones", []),
            "current_week":         cw_num,
            "weeks":                weeks_out,
        }

        # Preserve static pricing from v1 (date-independent config data)
        prev_s = prev_map.get(code, {})
        if prev_s.get("pricing") is not None:
            s_entry["pricing"] = prev_s["pricing"]
        if prev_s.get("sku_map") is not None:
            s_entry["sku_map"] = prev_s["sku_map"]

        output_studios.append(s_entry)

    if dry_run:
        print("\n[DRY RUN] Not writing.")
        return

    out_path = ROOT / "nso_scorecard_data2.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"studios": output_studios}, f, indent=2, default=str)
    size_kb = out_path.stat().st_size // 1000
    print(f"\nOK  Written {out_path}  ({size_kb} KB)")
    print("Next step: python scripts/fetch_tier_rmr_v2.py")


if __name__ == "__main__":
    main()
