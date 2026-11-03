/**
 * change-dots.js — Reusable "change markers" overlay for Chart.js 4 dashboards.
 *
 * Draws orange dots below chart x-axes on dates where operational changes occurred.
 * Uses a canvas overlay to avoid Chart.js 4 reactive-proxy stack overflow that
 * happens when pushing to chart.data.datasets directly.
 *
 * ── USAGE ────────────────────────────────────────────────────────────────────
 *
 * 1. Include this file in your HTML (after Chart.js):
 *      <script src="../shared/change-dots.js"></script>
 *
 * 2. Initialize once, passing callbacks that read your dashboard's current state:
 *      ChangeDots.init({
 *        getGran:     () => GRAN,                       // 'daily' | 'weekly' | 'monthly'
 *        getFrom:     () => document.getElementById('fFrom')?.value || null,
 *        getTo:       () => document.getElementById('fTo')?.value   || null,
 *        getStudios:  () => _effectiveSt(),             // array of studio name strings, or null for "all"
 *        getSource:   () => _getSel('sourceMenu'),      // array of selected source strings
 *        maxStudios:  5,                                // optional, default 5
 *        metaOnly:    true,                             // optional, default true — dots only when Meta is sole source
 *      });
 *
 * 3. After loading your changes data (CSV or JSON array), call:
 *      ChangeDots.setChanges(array);   // array of { date, studio, category, change, made_by }
 *      // OR from CSV text:
 *      ChangeDots.setChangesFromCsv(csvText);
 *
 * 4. Inside your render / applyFilters function, for each Chart.js chart:
 *      ChangeDots.addDots(myChart, rowKeys);  // rowKeys = array of date strings used as x-labels
 *
 *    Then at the END of your render function (after all addDots calls):
 *      requestAnimationFrame(ChangeDots.draw);
 *
 * 5. At the TOP of your applyFilters / render function, call:
 *      ChangeDots.clearAll();
 *    This removes stale dots from previous renders before rebuilding charts.
 *
 * ── DATA FORMAT ──────────────────────────────────────────────────────────────
 *
 * Each change record:
 *   {
 *     date:     "YYYY-MM-DD",   // required
 *     studio:   "Aventura",     // optional; "All" or "" means all studios
 *     category: "Meta Ads",     // e.g. "Meta Ads", "Budget", "Creative", "Targeting"
 *     change:   "Increased budget by $500",
 *     made_by:  "John"          // optional
 *   }
 *
 * ── INTEGRATION EXAMPLE ──────────────────────────────────────────────────────
 *
 *   function applyFilters() {
 *     if (!RAW) return;
 *     ChangeDots.clearAll();            // ← clear stale state at top
 *     // ... your render code ...
 *     buildMyChart(data);
 *     ChangeDots.addDots(myChart, dateKeys);
 *     requestAnimationFrame(ChangeDots.draw);
 *   }
 */

(function (global) {
  'use strict';

  // ── State ─────────────────────────────────────────────────────────────────
  let _changes = [];
  let _config = {
    getGran:    () => 'daily',
    getFrom:    () => null,
    getTo:      () => null,
    getStudios: () => null,
    getSource:  () => [],
    maxStudios: 5,
    metaOnly:   true,
    keyMapper:  null,  // optional (date, gran, rowKeys) => bucketKey | null
  };

  const _dotsMap    = new WeakMap(); // chart → { rowKeys, byKey }
  const _dotsCharts = [];            // registered chart instances

  // ── Public API ────────────────────────────────────────────────────────────
  const ChangeDots = {};

  /**
   * Initialize with callbacks that read your dashboard's filter state.
   * Call once before using any other method.
   */
  ChangeDots.init = function (opts) {
    Object.assign(_config, opts || {});
  };

  /**
   * Load changes from a pre-parsed array of objects.
   */
  ChangeDots.setChanges = function (arr) {
    _changes = Array.isArray(arr) ? arr : [];
  };

  /**
   * Load changes from a CSV string.
   * Expected columns (any order): date, studio, category, change, made_by
   */
  ChangeDots.setChangesFromCsv = function (csv) {
    _changes = _parseCsv(csv);
  };

  /**
   * Register a Chart.js chart instance to receive dots.
   * Call this after building/updating the chart, before requestAnimationFrame(ChangeDots.draw).
   *
   * @param {Chart}    chart    — Chart.js instance
   * @param {string[]} rowKeys  — array of date strings matching chart x-labels (same granularity)
   */
  ChangeDots.addDots = function (chart, rowKeys) {
    // Always deregister first (handles chart recreation)
    const idx = _dotsCharts.indexOf(chart);
    if (idx >= 0) _dotsCharts.splice(idx, 1);
    _dotsMap.delete(chart);

    // Also clear any stale overlay canvas left in the DOM
    if (chart && chart.canvas) {
      const _ov = chart._nsoOverlay ||
                  chart.canvas.parentElement?.querySelector('[data-nso-ov]');
      if (_ov) {
        const _oc = _ov.getContext('2d');
        if (_oc) _oc.clearRect(0, 0, _ov.width, _ov.height);
      }
    }

    if (!chart || !rowKeys || !rowKeys.length || !_changes.length) return;

    const from    = _config.getFrom();
    const to      = _config.getTo();
    const studios = _config.getStudios();

    // Studio count guard
    if (!studios || studios.length > (_config.maxStudios || 5)) return;

    // Meta-only source guard
    if (_config.metaOnly) {
      const sr = _config.getSource();
      if (!(sr && sr.length === 1 && /meta|facebook/i.test(sr[0]))) return;
    }

    const inPeriod = _changesInPeriod(from, to, studios);
    if (!inPeriod.length) return;

    const gran = _config.getGran();
    const byKey = {};
    inPeriod.forEach(c => {
      const mapped = _config.keyMapper ? _config.keyMapper(c.date, gran, rowKeys) : null;
      const k = mapped != null ? mapped : _toKey(c.date, gran);
      if (!k) return;
      if (!byKey[k]) byKey[k] = [];
      byKey[k].push(c);
    });

    if (!rowKeys.some(k => byKey[k])) return;

    _dotsMap.set(chart, { rowKeys, byKey, gran });
    _dotsCharts.push(chart);
  };

  /**
   * Draw dots on all registered charts.
   * Pass to requestAnimationFrame: requestAnimationFrame(ChangeDots.draw)
   */
  ChangeDots.draw = function () {
    _dotsCharts.forEach(_drawOne);
  };

  /**
   * Clear all dot state and wipe every overlay canvas.
   * Call at the TOP of your applyFilters / render function.
   */
  ChangeDots.clearAll = function () {
    _dotsCharts.length = 0;
    document.querySelectorAll('[data-nso-ov]').forEach(ov => {
      const ctx = ov.getContext('2d');
      if (ctx) ctx.clearRect(0, 0, ov.width, ov.height);
    });
  };

  // ── Internal helpers ──────────────────────────────────────────────────────

  function _toKey(date, gran) {
    if (gran === 'monthly') return date.substring(0, 7) + '-01';
    if (gran === 'weekly') {
      const dt = new Date(date + 'T00:00:00Z');
      const ws = new Date(dt);
      ws.setUTCDate(dt.getUTCDate() - dt.getUTCDay());
      return ws.toISOString().slice(0, 10);
    }
    return date; // daily
  }

  function _changesInPeriod(from, to, studios) {
    return _changes.filter(c => {
      if (from && c.date < from) return false;
      if (to   && c.date > to)   return false;
      if (c.studio && c.studio !== 'All' && c.studio !== 'all') {
        const sel = studios || [];
        if (sel.length && !sel.some(s =>
          s.replace(/^SWEAT440\s+/i, '').toLowerCase() === c.studio.toLowerCase()
        )) return false;
      }
      return true;
    });
  }

  function _parseCsv(csv) {
    const rows = csv.trim().split('\n');
    if (rows.length < 2) return [];
    const headers = rows[0].split(',').map(h => h.replace(/^"|"$/g, '').trim());
    return rows.slice(1).map(row => {
      const vals = []; let cur = '', inQ = false;
      for (let i = 0; i < row.length; i++) {
        const ch = row[i];
        if (ch === '"' && !inQ)             { inQ = true; }
        else if (ch === '"' && inQ)         { if (row[i+1] === '"') { cur += '"'; i++; } else { inQ = false; } }
        else if (ch === ',' && !inQ)        { vals.push(cur); cur = ''; }
        else                                { cur += ch; }
      }
      vals.push(cur);
      const obj = {};
      headers.forEach((h, i) => { obj[h] = (vals[i] || '').replace(/^"|"$/g, ''); });
      return obj;
    }).filter(r => r.date);
  }

  function _drawOne(chart) {
    if (!chart || !chart.canvas) return;
    const mc = chart.canvas;
    const cont = mc.parentElement;
    if (!cont) return;

    // Shared tooltip
    let tip = document.getElementById('_cdTip');
    if (!tip) {
      tip = document.createElement('div');
      tip.id = '_cdTip';
      tip.style.cssText = [
        'position:fixed',
        'background:#1a1a2e',
        'color:#e0f0fa',
        'padding:8px 12px',
        'border-radius:6px',
        'font-size:12px',
        'line-height:1.6',
        'pointer-events:none',
        'z-index:9999',
        'display:none',
        'max-width:280px',
        'box-shadow:0 2px 10px rgba(0,0,0,.4)',
        'border:1px solid #f4a021',
      ].join(';');
      document.body.appendChild(tip);
    }

    // Overlay canvas (created once per chart instance)
    if (!chart._nsoOverlay) {
      const ov = document.createElement('canvas');
      ov.style.cssText = 'position:absolute;top:0;left:0;pointer-events:none;z-index:5;';
      ov.setAttribute('data-nso-ov', '1');
      cont.style.position = 'relative';
      cont.appendChild(ov);
      chart._nsoOverlay = ov;

      mc.addEventListener('mousemove', function (e) {
        const pos = chart._cdDotPos;
        if (!pos || !pos.length) { tip.style.display = 'none'; return; }
        const r  = mc.getBoundingClientRect();
        const sx = mc.width  / r.width;
        const sy = mc.height / r.height;
        const mx = (e.clientX - r.left) * sx;
        const my = (e.clientY - r.top)  * sy;
        let found = null;
        for (const p of pos) {
          const dx = mx - p.x, dy = my - p.y;
          if (dx * dx + dy * dy <= 100) { found = p.ch; break; }
        }
        if (found) {
          tip.innerHTML =
            '<div style="font-weight:600;color:#f4a021;margin-bottom:4px">Changes</div>' +
            found.map(c =>
              `<div>• <b>${c.category}</b>: ${c.change}` +
              (c.made_by ? `<span style="color:#8aaabb"> (${c.made_by})</span>` : '') +
              '</div>'
            ).join('');
          tip.style.display = 'block';
          tip.style.left    = (e.clientX + 14) + 'px';
          tip.style.top     = (e.clientY - 30)  + 'px';
        } else {
          tip.style.display = 'none';
        }
      });
      mc.addEventListener('mouseleave', () => { tip.style.display = 'none'; });
    }

    const ov = chart._nsoOverlay;
    ov.width  = mc.width;
    ov.height = mc.height;
    ov.style.width  = (mc.offsetWidth  || mc.width)  + 'px';
    ov.style.height = (mc.offsetHeight || mc.height) + 'px';

    const octx = ov.getContext('2d');
    octx.clearRect(0, 0, ov.width, ov.height);
    chart._cdDotPos = [];

    const info = _dotsMap.get(chart);
    if (!info) return;
    const { rowKeys, byKey } = info;

    const meta0 = chart.getDatasetMeta(0);
    if (!meta0 || !meta0.data) return;

    const ca  = chart.chartArea;
    // Chart.js positions are in CSS px; overlay is in physical px — scale by DPR.
    const dpr  = mc.width / (mc.offsetWidth || mc.width) || 1;
    const dotY = ca ? (ca.bottom + 14) * dpr : ov.height;

    rowKeys.forEach((k, i) => {
      if (!byKey[k]) return;
      const pt = meta0.data[i];
      if (!pt) return;
      const x = pt.x * dpr;
      octx.save();
      octx.beginPath();
      octx.arc(x, dotY, 7 * dpr, 0, Math.PI * 2);
      octx.fillStyle   = '#f4a021';
      octx.strokeStyle = '#fff';
      octx.lineWidth   = 2 * dpr;
      octx.fill();
      octx.stroke();
      octx.restore();
      chart._cdDotPos.push({ x, y: dotY, ch: byKey[k] });
    });
  }

  // Expose globally
  global.ChangeDots = ChangeDots;

}(typeof window !== 'undefined' ? window : this));
