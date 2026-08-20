// SWEAT440 Dashboard — Shared Utilities
// Import this file in every dashboard HTML
// Each dashboard must define applyFilters() in its own <script> block

function _applyFilters() {
  if (typeof applyFilters === 'function') applyFilters();
}


// -- Sortable table utility -------------------------------------------------
const _sortState = {};
function sortableTable(tableId, data, renderRow, totalsRow) {
  const table = document.getElementById(tableId);
  if (!table) return;
  const ths = table.querySelectorAll('thead th[data-sort]');
  const tbody = table.querySelector('tbody');
  const tfoot = table.querySelector('tfoot') || (() => {
    const tf = document.createElement('tfoot'); table.appendChild(tf); return tf;
  })();
  if (!_sortState[tableId]) _sortState[tableId] = {col: null, asc: true};
  const state = _sortState[tableId];
  function render() {
    const sorted = state.col === null ? [...data] : [...data].sort((a, b) => {
      const va = a[state.col], vb = b[state.col];
      if (va == null) return 1; if (vb == null) return -1;
      const cmp = typeof va === 'string' ? va.localeCompare(vb) : va - vb;
      return state.asc ? cmp : -cmp;
    });
    tbody.innerHTML = sorted.map((row, i) => renderRow(row, i + 1)).join('');
    ths.forEach(th => {
      th.classList.remove('sort-asc', 'sort-desc');
      if (th.dataset.sort === state.col) th.classList.add(state.asc ? 'sort-asc' : 'sort-desc');
    });
    if (totalsRow) tfoot.innerHTML = totalsRow(sorted);
  }
  state._render = render;
  ths.forEach(th => {
    th.classList.add('sortable');
    if (!th._sortableTableBound) {
      th._sortableTableBound = true;
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (state.col === col) { state.asc = !state.asc; } else { state.col = col; state.asc = true; }
        state._render();
      });
    }
  });
  render();
}

// -- Chart instance registry ------------------------------------------------
const _areaCharts = {};

// -- Source color map -------------------------------------------------------
const SRC_COLORS = ['#00A3E0','#00C9A7','#9aab00','#f4a021','#e05a5a','#7c5cbf',
  '#00bcd4','#8bc34a','#ff7043','#5c6bc0','#26a69a','#ef5350','#ab47bc','#78909c'];

const SRC_COLOR_MAP = {
  'Meta Ads':                '#1877F2',  // Facebook/Meta blue
  'Google Ads':              '#34A853',  // Google green
  'Local Listings':          '#FBBC05',  // Google yellow (maps/local)
  'ClassPass / Platforms':   '#7B68EE',  // ClassPass medium-purple
  'Grassroots':              '#8BC34A',  // organic lime-green
  'Social Media Organic':    '#E1306C',  // Instagram pink
  'Word of Mouth':           '#27AE60',  // referral green
  'Website (unattributed)':  '#2ECC71',  // web green
  'SWEAT440 App':            '#F39C12',  // app amber
  'MindBody App':            '#8E44AD',  // MindBody purple
  'Business Mode':           '#E74C3C',  // red (legacy — kept for backwards compat)
  'Business Mode / App':     '#E74C3C',  // red
  'Public API':              '#C0392B',  // dark red
  'Print Ads / Signs':       '#2980B9',  // print blue
  'N/A':                     '#7F8C8D',  // grey
  'Other':                   '#BDC3C7',  // light grey
};

// -- Studio color map — consistent across all tabs and both dashboards -------
// Keys are short names WITHOUT the "SWEAT440 " prefix.
// Organized by franchisee so colors within each portfolio are maximally distinct.
const STUDIO_COLOR_MAP = {
  // SWEAT440 flagship / corporate
  'Miami Beach':                '#00A3E0',  // brand blue
  'Miami - Brickell':           '#0071BC',  // deep brand blue

  // Marc Gralnick (5 studios) — warm family
  'Coral Springs':              '#C62828',  // dark red
  'Deerfield Beach':            '#EF6C00',  // deep orange
  'Fort Lauderdale - Las Olas': '#C2185B',  // dark pink-red
  'Miami - Coconut Grove':      '#6D4C41',  // warm brown
  'Miami - Upper East Side':    '#FF7043',  // coral-orange

  // Carlos de Varona (3) — teals/greens
  'Miami Lakes':                '#00897B',  // teal
  'Miramar':                    '#558B2F',  // olive
  'Pembroke Pines':             '#00ACC1',  // cyan-blue

  // Mark Cacciaguida (2) — deep warm
  'Doral':                      '#E64A19',  // burnt orange
  'Miami - Midtown':            '#9E9D24',  // dark lime/olive

  // Erika Sanchez (2) — magentas
  'Coral Gables':               '#880E4F',  // dark magenta
  'Naples - Mercato':           '#F06292',  // medium pink

  // Max Gelrud (1)
  'South Miami':                '#4E342E',  // dark brown

  // Alex Avila (7) — blues/purples/teals (widest spread)
  'Aventura':                   '#1565C0',  // deep blue (NSO)
  'North Miami':                '#9C27B0',  // purple (NSO)
  'Boca Raton':                 '#5E35B1',  // deep purple
  'West Palm Beach':            '#0288D1',  // medium blue
  'NYC - Chelsea':              '#00695C',  // dark teal
  'NYC - Park Slope':           '#AD1457',  // dark pink
  'NYC - FiDi':                 '#37474F',  // blue-grey

  // Kristen Albert (1)
  'Eastchester':                '#546E7A',  // steel blue-grey

  // Nick Marco (5) — varied to maximise intra-group distinction
  'Toms River':                 '#2E7D32',  // dark green
  'Ocean Township':             '#283593',  // dark indigo
  'Wall Township':              '#6A1B9A',  // dark purple
  'Middletown':                 '#BF360C',  // dark burnt-orange
  'Old Bridge':                 '#455A64',  // dark slate (NSO)

  // Paul Marcus (2) — Austin
  'Austin - Zilker':            '#FF8F00',  // amber
  'Austin - Highland':          '#795548',  // medium brown

  // Single-studio franchisees / NSO
  'Dallas - Prestonwood':       '#78909C',  // blue-grey
  'Dallas - Uptown':            '#00BFA5',  // bright teal (NSO)
  'Pinecrest - Palmetto Bay':   '#304FFE',  // electric indigo
  'Pinecrest':                  '#304FFE',  // short-name alias
  'Orlando - Dr Phillips':      '#1B5E20',  // very dark green (NSO)
  'Charlotte - NoDa':           '#33691E',  // dark olive-green
  'Nashville - Capitol View':   '#263238',  // very dark blue-grey
  'Nashville - Music Row':      '#263238',
  'Music Row':                  '#263238',
  'Herriman':                   '#B71C1C',  // dark red (NSO)
  'Reston':                     '#311B92',  // very dark purple (NSO)
  'Madison':                    '#F9A825',  // amber-yellow
};

function studioColor(name){
  const short = (name||'').replace(/^SWEAT440\s+/,'').trim();
  if(STUDIO_COLOR_MAP[short]) return STUDIO_COLOR_MAP[short];
  // Fallback: deterministic hash so the same studio always gets the same color
  let h=0; for(let i=0;i<short.length;i++) h=(h*31+short.charCodeAt(i))&0xFFFFFF;
  const hue=h%360, sat=55+h%25, lit=38+h%18;
  return `hsl(${hue},${sat}%,${lit}%)`;
}

// -- Default exclusions (can be overridden per dashboard) -------------------
// Populated from studios.json by loadStudiosRegistry() — call it once in each
// dashboard's loadData(), before buildMultiSelect(). Kept as `const` arrays
// (mutated in place via .push, never reassigned) so every file that already
// references these exact array objects keeps working unchanged.
const DEFAULT_EXCL_SOURCES = ['ClassPass / Platforms','Grassroots'];
const DEFAULT_EXCL_STUDIOS = [];
const NSO_STUDIOS = [];
const CLOSED_STUDIOS = [];

// google-ads-data.json rows written before the studios.json migration used
// shorter/older studio names. This is a historical-compat shim, not studio
// config — new fetches already write canonical names, so this map should
// never need new entries.
const GOOGLE_STUDIO_ALIAS = {
  'Pinecrest':               'Pinecrest - Palmetto Bay',
  'Naples Mercato':          'Naples - Mercato',
  'Dr Phillips':             'Orlando - Dr Phillips',
  'Capitol View':            'Nashville - Capitol View',
  'NYC - Financial District':'NYC - FiDi',
};
function googleStudioName(raw) { return GOOGLE_STUDIO_ALIAS[raw] || raw; }

// Canonical studio code -> name (no "SWEAT440 " prefix). Populated from
// studios.json by loadStudiosRegistry().
const META_CODE_TO_STUDIO = {};

// studio code -> Facebook Page ID. Populated from studios.json by loadStudiosRegistry().
const META_CODE_TO_PAGE_ID = {};

// Fetches studios.json and populates DEFAULT_EXCL_STUDIOS / NSO_STUDIOS /
// CLOSED_STUDIOS / META_CODE_TO_STUDIO / META_CODE_TO_PAGE_ID in place.
// Call once per page load, before buildMultiSelect() or any lookup.
async function loadStudiosRegistry(basePath = '') {
  let registry = { studios: [] };
  try {
    const res = await fetch(basePath + 'studios.json?_=' + Date.now());
    registry = await res.json();
  } catch (e) {
    console.warn('studios.json unavailable:', e);
  }
  DEFAULT_EXCL_STUDIOS.length = 0;
  NSO_STUDIOS.length = 0;
  CLOSED_STUDIOS.length = 0;
  for (const key of Object.keys(META_CODE_TO_STUDIO)) delete META_CODE_TO_STUDIO[key];
  for (const key of Object.keys(META_CODE_TO_PAGE_ID)) delete META_CODE_TO_PAGE_ID[key];
  for (const s of (registry.studios || [])) {
    if (s.excluded_default) { DEFAULT_EXCL_STUDIOS.push(s.name); DEFAULT_EXCL_STUDIOS.push('- ' + s.name); }
    if (s.status === 'nso')    { NSO_STUDIOS.push(s.name);     NSO_STUDIOS.push('- ' + s.name); }
    if (s.status === 'closed') { CLOSED_STUDIOS.push(s.name);  CLOSED_STUDIOS.push('- ' + s.name); }
    if (s.code) META_CODE_TO_STUDIO[s.code] = s.name;
    if (s.code && s.meta && s.meta.page_id) META_CODE_TO_PAGE_ID[s.code] = s.meta.page_id;
  }
  return registry;
}

function localDateStr(d) { return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'); }

// Multi-select

function toggleDropdown(id) {
  document.querySelectorAll('.ms-menu').forEach(m => { if (m.id !== id+'Menu') m.classList.remove('open'); });
  document.getElementById(id+'Menu').classList.toggle('open');
}
document.addEventListener('click', e => {
  if (!e.target.closest('.multi-select')) document.querySelectorAll('.ms-menu').forEach(m => m.classList.remove('open'));
});

function buildSourceSelect(menuId, labelId, items, defaultExcluded) {
  const NON_MARKETING = ['ClassPass / Platforms', 'Grassroots'];
  const menu = document.getElementById(menuId);
  if (!menu) return;
  menu.innerHTML = '';

  // Select All
  const allDiv = document.createElement('div');
  allDiv.className = 'ms-item ms-select-all';
  allDiv.innerHTML = `<input type="checkbox" id="${menuId}_all"> <label for="${menuId}_all" style="cursor:pointer">Select all</label>`;
  menu.appendChild(allDiv);
  const div0 = document.createElement('div'); div0.className = 'ms-divider'; menu.appendChild(div0);

  const marketingItems    = items.filter(i => !NON_MARKETING.includes(i));
  const nonMarketingItems = items.filter(i =>  NON_MARKETING.includes(i));

  function addItems(arr) {
    arr.forEach(item => {
      const div = document.createElement('div'); div.className = 'ms-item';
      const checked = !defaultExcluded.includes(item);
      const safeId = `ms_${menuId}_${item.replace(/[^a-z0-9]/gi,'_')}`;
      div.innerHTML = `<input type="checkbox" id="${safeId}" value="${item}" ${checked?'checked':''}> <label for="${safeId}" style="cursor:pointer">${item}</label>`;
      div.querySelector('input').addEventListener('change', () => { syncSelectAll(menuId); updateLabel(menuId, labelId, items); _applyFilters(); });
      menu.appendChild(div);
    });
  }

  // Non-Marketing Sources first (unchecked by default)
  const catNon = document.createElement('div');
  catNon.className = 'ms-category'; catNon.textContent = 'Non-Marketing Sources';
  menu.appendChild(catNon);
  addItems(nonMarketingItems);

  // Marketing Sources
  const catMarketing = document.createElement('div');
  catMarketing.className = 'ms-category'; catMarketing.textContent = 'Marketing Sources';
  menu.appendChild(catMarketing);
  addItems(marketingItems);

  const allChk = document.getElementById(menuId+'_all');
  syncSelectAll(menuId);
  allChk.addEventListener('change', () => {
    menu.querySelectorAll('input[value]').forEach(c => c.checked = allChk.checked);
    updateLabel(menuId, labelId, items); _applyFilters();
  });
  updateLabel(menuId, labelId, items);
}

function buildMultiSelect(menuId, labelId, items, defaultExcluded) {
  const menu = document.getElementById(menuId);
  menu.innerHTML = '';
  const allDiv = document.createElement('div');
  allDiv.className = 'ms-item ms-select-all';
  allDiv.innerHTML = `<input type="checkbox" id="${menuId}_all"> <label for="${menuId}_all" style="cursor:pointer">Select all</label>`;
  menu.appendChild(allDiv);
  const div0 = document.createElement('div'); div0.className = 'ms-divider'; menu.appendChild(div0);

  const itemSafeId = item => `ms_${menuId}_${item.replace(/[^a-z0-9]/gi,'_')}`;

  // Open Studios first, NSO second, Closed third — alphabetical within each category
  const categories = [
    { key: 'os',     label: 'Open Studios', items: items.filter(i => !NSO_STUDIOS.includes(i) && !CLOSED_STUDIOS.includes(i)).slice().sort() },
    { key: 'nso',    label: 'NSO',          items: items.filter(i =>  NSO_STUDIOS.includes(i)).slice().sort() },
    { key: 'closed', label: 'Closed',       items: items.filter(i =>  CLOSED_STUDIOS.includes(i)).slice().sort() },
  ];

  function syncCategory(cat) {
    const catChk = document.getElementById(`${menuId}_cat_${cat.key}`);
    if (!catChk) return;
    const boxes = cat.items.map(item => document.getElementById(itemSafeId(item))).filter(Boolean);
    const n = boxes.filter(c => c.checked).length;
    catChk.checked = boxes.length > 0 && n === boxes.length;
    catChk.indeterminate = n > 0 && n < boxes.length;
  }

  categories.forEach(cat => {
    if (!cat.items.length) return;
    const catId = `${menuId}_cat_${cat.key}`;
    const catDiv = document.createElement('div');
    catDiv.className = 'ms-cat-header';
    catDiv.innerHTML = `<input type="checkbox" id="${catId}"> <label for="${catId}" style="cursor:pointer">${cat.label}</label>`;
    menu.appendChild(catDiv);

    cat.items.forEach(item => {
      const div = document.createElement('div'); div.className = 'ms-item';
      const checked = !defaultExcluded.includes(item);
      const safeId = itemSafeId(item);
      const displayName = item.replace(/^-\s+/, '');
      div.innerHTML = `<input type="checkbox" id="${safeId}" value="${item}" ${checked?'checked':''}> <label for="${safeId}" style="cursor:pointer">${displayName}</label>`;
      div.querySelector('input').addEventListener('change', () => {
        syncCategory(cat); syncSelectAll(menuId); updateLabel(menuId, labelId, items); _applyFilters();
      });
      menu.appendChild(div);
    });

    syncCategory(cat);
    document.getElementById(catId).addEventListener('change', () => {
      const catChk = document.getElementById(catId);
      cat.items.forEach(item => {
        const inp = document.getElementById(itemSafeId(item));
        if (inp) inp.checked = catChk.checked;
      });
      syncSelectAll(menuId); updateLabel(menuId, labelId, items); _applyFilters();
    });
  });

  const allChk = document.getElementById(menuId+'_all');
  syncSelectAll(menuId);
  allChk.addEventListener('change', () => {
    menu.querySelectorAll('input[value]').forEach(c => c.checked = allChk.checked);
    categories.forEach(syncCategory);
    updateLabel(menuId, labelId, items); _applyFilters();
  });
  updateLabel(menuId, labelId, items);
}

function syncSelectAll(menuId) {
  const menu = document.getElementById(menuId);
  const all  = [...menu.querySelectorAll('input[value]')];
  const allChk = document.getElementById(menuId+'_all');
  const n = all.filter(c=>c.checked).length;
  allChk.checked = n === all.length;
  allChk.indeterminate = n > 0 && n < all.length;
}

function updateLabel(menuId, labelId, items) {
  const checked = [...document.getElementById(menuId).querySelectorAll('input[value]:checked')].map(c=>c.value);
  const label = document.getElementById(labelId);
  if      (checked.length === 0)            label.textContent = 'None selected';
  else if (checked.length === items.length) label.textContent = 'All';
  else if (checked.length <= 2)             label.textContent = checked.join(', ');
  else                                      label.textContent = checked.length + ' selected';
}

function getSelected(menuId) {
  const menu = document.getElementById(menuId);
  if (!menu) return null;
  return [...menu.querySelectorAll('input[value]:checked')].map(c=>c.value);
}

// Granularity

function getQuarterBounds() {
  const now = new Date();
  const yearStart = new Date(Date.UTC(now.getUTCFullYear(), 0, 1));
  return { dailyFrom: yearStart, dailyTo: now };
}

function setGran(gran) {
  const btn = document.getElementById('gran'+gran.charAt(0).toUpperCase()+gran.slice(1));
  if (btn && btn.disabled) return;
  GRAN = gran;
  ['daily','weekly','monthly'].forEach(g => {
    const b = document.getElementById('gran'+g.charAt(0).toUpperCase()+g.slice(1));
    if (b) b.classList.toggle('active', g === gran);
  });
  _applyFilters();
}

function updateGranButtons(from, to) {
  // Daily data covers: start of current year → today
  // We check if the requested range overlaps with our daily data window at all
  const { dailyFrom, dailyTo } = getQuarterBounds();
  // Daily and Weekly require the full range to start within the daily data window
  const hasDaily  = from >= dailyFrom;
  const hasWeekly = from >= dailyFrom;
  const daysDiff  = (to - from) / 86400000;
  const btnD = document.getElementById('granDaily');
  const btnW = document.getElementById('granWeekly');
  if (btnD) btnD.disabled = !hasDaily;
  if (btnW) btnW.disabled = !hasWeekly || daysDiff > 366;
  if (GRAN === 'daily'  && btnD && btnD.disabled) { GRAN = 'weekly';  setGran('weekly'); }
  if (GRAN === 'weekly' && btnW && btnW.disabled) { GRAN = 'monthly'; setGran('monthly'); }
}

function getRowDate(r) { return new Date((r.date||r.month)+'T00:00:00Z'); }

function filterRows(arr, fromDate, toDate, studios, sources) {
  return arr.filter(r => {
    const d = getRowDate(r);
    if (fromDate && d < fromDate) return false;
    if (toDate   && d > toDate)   return false;
    if (studios  && !studios.includes(r.studio)) return false;
    if (sources  && !sources.includes(r.source)) return false;
    return true;
  });
}

function sumRows(rows) {
  return rows.reduce((acc,r) => {
    acc.leads += r.signups||0; acc.ft += r.first_visits||0; acc.mem += r.first_activations||0;
    return acc;
  }, {leads:0,ft:0,mem:0});
}

function computeWindows(from, to) {
  const ms = to - from;
  const ppTo   = new Date(from.getTime()-86400000);
  const ppFrom = new Date(ppTo.getTime()-ms);
  const pyFrom = new Date(from); pyFrom.setFullYear(pyFrom.getFullYear()-1);
  const pyTo   = new Date(to);   pyTo.setFullYear(pyTo.getFullYear()-1);
  return {ppFrom,ppTo,pyFrom,pyTo};
}

function fmtDate(d) { return d.toLocaleString('en-US',{month:'short',year:'numeric'}); }

function fmtMonthLabel(iso) { return new Date(iso+'T00:00:00Z').toLocaleString('en-US',{month:'short',year:'2-digit',timeZone:'UTC'}); }

function fmtDayLabel(iso)   { return new Date(iso+'T00:00:00Z').toLocaleString('en-US',{month:'short',day:'numeric',timeZone:'UTC'}); }

function fmtFullDate(d) { return d.toLocaleString('en-US',{month:'short',day:'numeric',year:'numeric',timeZone:'UTC'}); }

// Data freshness note ------------------------------------------------------
// Every data file stamps its own generated_at when a refresh succeeds; if a
// refresh fails, generated_at stays put and the computed "through" date falls
// behind on its own — no separate failure tracking needed. Snowflake-backed
// files lag 2 days (fetch_data.py/fetch_nso_sales.py cap at today-2); ad
// platform / GBP / social files lag 1 day (data is current through yesterday).
function dataThroughDate(raw, lagDays) {
  if (!raw || !raw.generated_at) return null;
  const gen = new Date(raw.generated_at);
  if (isNaN(gen)) return null;
  return new Date(Date.UTC(gen.getUTCFullYear(), gen.getUTCMonth(), gen.getUTCDate() - lagDays));
}

function renderFreshnessNote(elId, sources) {
  const el = document.getElementById(elId);
  if (!el) return;
  let oldest = null;
  sources.forEach(({raw, lagDays}) => {
    const d = dataThroughDate(raw, lagDays);
    if (d && (!oldest || d < oldest)) oldest = d;
  });
  el.textContent = oldest ? ('Data current through ' + fmtFullDate(oldest)) : '';
}

function toTimeSeries(dailyRows, monthlyRows) {
  if (GRAN === 'daily') {
    const map = {};
    dailyRows.forEach(r => {
      if (!map[r.date]) map[r.date]={key:r.date,label:fmtDayLabel(r.date),signups:0,first_visits:0,first_activations:0};
      map[r.date].signups+=r.signups||0; map[r.date].first_visits+=r.first_visits||0; map[r.date].first_activations+=r.first_activations||0;
    });
    return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
  }
  if (GRAN === 'weekly') {
    const map = {};
    [...dailyRows,...monthlyRows].forEach(r => {
      const d = new Date((r.date||r.month)+'T00:00:00Z');
      const ws = new Date(d); ws.setUTCDate(d.getUTCDate()-d.getUTCDay());
      const key = ws.toISOString().slice(0,10);
      if (!map[key]) map[key]={key,label:fmtDayLabel(key),signups:0,first_visits:0,first_activations:0};
      map[key].signups+=r.signups||0; map[key].first_visits+=r.first_visits||0; map[key].first_activations+=r.first_activations||0;
    });
    return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
  }
  // monthly
  const map = {};
  [...dailyRows,...monthlyRows].forEach(r => {
    const key=(r.date||r.month).slice(0,7)+'-01';
    if (!map[key]) map[key]={key,label:fmtMonthLabel(key),signups:0,first_visits:0,first_activations:0};
    map[key].signups+=r.signups||0; map[key].first_visits+=r.first_visits||0; map[key].first_activations+=r.first_activations||0;
  });
  return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
}

function calcDelta(curr,prev) { if(!prev||prev===0) return null; return (curr-prev)/prev*100; }

function srcColor(src,i){return SRC_COLOR_MAP[src]||SRC_COLORS[i%SRC_COLORS.length];}

// Chart instance registry — see top of file

function buildAreaChart(canvasId, togglesId, series, srcList, valueKey){
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  // Destroy existing chart instance
  if (_areaCharts[canvasId]) { _areaCharts[canvasId].destroy(); _areaCharts[canvasId] = null; }

  const timeKeys = [...new Set(series.map(r=>r.key))].sort();

  // Sort biggest first = bottom of stack; exclude sources with no data in this period
  const srcTotals = srcList.map(src => series.filter(r=>r.source===src).reduce((s,r)=>s+(r[valueKey]||0),0));
  const sortedSrcs = [...srcList.keys()]
    .filter(i => srcTotals[i] > 0)
    .sort((a,b)=>srcTotals[b]-srcTotals[a])
    .map(i=>srcList[i]);

  const datasets = sortedSrcs.map(src => {
    const c = srcColor(src, srcList.indexOf(src));
    return { label:src, data:timeKeys.map(tk=>series.filter(r=>r.key===tk&&r.source===src).reduce((s,r)=>s+(r[valueKey]||0),0)),
      borderColor:c, backgroundColor:c+'cc', borderWidth:1, pointRadius:0, pointHoverRadius:5, tension:.35, fill:true };
  });

  const isMob = window.innerWidth<=768;
  const labels = timeKeys.map(k => k.length===10 && k.slice(8)!=='01'
    ? new Date(k+'T00:00:00Z').toLocaleString('en-US',{month:'short',day:'numeric',timeZone:'UTC'})
    : new Date(k.slice(0,7)+'-01T00:00:00Z').toLocaleString('en-US',{month:'short',year:'2-digit',timeZone:'UTC'}));

  const chart = new Chart(ctx, {
    type:'line', data:{labels, datasets},
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{display:false}, tooltip:{mode:'index',intersect:false,
        callbacks:{
          label: item => ' '+item.dataset.label+': '+item.parsed.y.toLocaleString(),
          afterBody: items => { const t=items.filter(i=>!i.dataset.hidden).reduce((s,i)=>s+i.parsed.y,0); return ['','Total: '+t.toLocaleString()]; }
        }
      }},
      scales:{
        x:{grid:{color:'rgba(0,0,0,.05)'},ticks:{font:{size:10},color:'#5a7a8a',autoSkip:true,maxTicksLimit:isMob?5:12,maxRotation:0}},
        y:{grid:{color:'rgba(0,0,0,.05)'},ticks:{font:{size:11},color:'#5a7a8a'},stacked:true,min:0}
      }
    }
  });
  _areaCharts[canvasId] = chart;

  // Build toggle buttons  reference chart via registry, not closure
  const tg = document.getElementById(togglesId);
  if (tg) {
    tg.innerHTML = '';
    sortedSrcs.forEach((src, btnI) => {
      const c = srcColor(src, srcList.indexOf(src));
      const btn = document.createElement('button');
      btn.className = 'tog-btn on';
      btn.style.cssText = 'border-color:'+c+';background:'+c+';color:#fff;font-size:11px;padding:4px 10px';
      btn.innerHTML = '<span class="dot" style="background:#fff"></span>'+src;
      btn.dataset.canvasId = canvasId;
      btn.dataset.dsIdx = btnI;
      btn.onclick = function() {
        const ch = _areaCharts[this.dataset.canvasId];
        if (!ch) return;
        const ds = ch.data.datasets[parseInt(this.dataset.dsIdx)];
        ds.hidden = !ds.hidden;
        this.classList.toggle('on');
        if (this.classList.contains('on')) { this.style.background=c; this.style.borderColor=c; this.style.color='#fff'; }
        else { this.style.background='#f4f9fd'; this.style.borderColor='#c0d4df'; this.style.color='#5a7a8a'; }
        ch.update();
      };
      tg.appendChild(btn);
    });
  }
  return chart;
}

function buildRingChart(canvasId,legendId,labels,values){
  const ctx=document.getElementById(canvasId);if(!ctx)return;
  if(_areaCharts[canvasId]){_areaCharts[canvasId].destroy();_areaCharts[canvasId]=null;}
  const total=values.reduce((s,v)=>s+v,0);
  const colors=labels.map((l,i)=>srcColor(l,i));
  _areaCharts[canvasId]=new Chart(ctx,{type:'doughnut',data:{labels,datasets:[{data:values,backgroundColor:colors,borderColor:'#fff',borderWidth:2,hoverOffset:6}]},options:{responsive:true,maintainAspectRatio:true,plugins:{legend:{display:false}},cutout:'65%'}});
  const leg=document.getElementById(legendId);
  if(leg)leg.innerHTML=labels.map((l,i)=>{
    const pct=total?(values[i]/total*100).toFixed(1):0;
    const vol=values[i].toLocaleString();
    return '<div class="ring-legend-item"><span class="ring-legend-dot" style="background:'+colors[i]+'"></span><span class="ring-legend-label">'+l+'</span><span class="ring-legend-vol">'+vol+'</span><span class="ring-legend-pct">'+pct+'%</span></div>';
  }).join('');
}

function buildStudioRankTable(tableId,rows,valueKey){
  const tbody=document.querySelector('#'+tableId+' tbody');if(!tbody)return;
  const byS={};rows.forEach(r=>{byS[r.studio]=(byS[r.studio]||0)+(r[valueKey]||0);});
  const sorted=Object.entries(byS).sort((a,b)=>b[1]-a[1]);
  const total=sorted.reduce((s,[,v])=>s+v,0);
  tbody.innerHTML=sorted.map(([studio,val])=>{const pct=total?(val/total*100).toFixed(1):0;const bw=total?Math.round(val/total*80):0;return '<tr><td><span class="bar-mini" style="width:'+bw+'px"></span>'+studio.replace('SWEAT440 ','')+'</td><td class="num">'+val.toLocaleString()+'</td><td class="pct">'+pct+'%</td></tr>';}).join('');
}

// Partial period warning state — set by applyFilters, applied to ALL cards
let _partialPeriod = {pp: false, py: false};

function _isFullMonths(from, to) {
  if (!from || !to) return false;
  const f = typeof from === 'string' ? new Date(from+'T00:00:00Z') : from;
  const t = typeof to   === 'string' ? new Date(to  +'T00:00:00Z') : to;
  const firstOfMonth = f.getUTCDate() === 1;
  const lastDay = new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth()+1, 0)).getUTCDate();
  const lastOfMonth = t.getUTCDate() === lastDay;
  return firstOfMonth && lastOfMonth;
}

// Partial-period warning line. `kind` is 'period' or 'year' — controls the
// label text and the popup copy. `tag`/`cls` let callers reuse this for
// either kpi-card divs or the funnel bars' <span class="delta"> markup.
function _partialPeriodInfoBtn(kind) {
  const label = kind === 'year' ? 'prev year' : 'prev period';
  const desc  = 'This range doesn\'t cover complete calendar months, so the '+label+' comparison can\'t be shown reliably. Select a date range from the 1st through the last day of the month(s) to see it.';
  return {
    label,
    btn: '<button class="kpi-info-btn kpi-info-inline" onclick="showKpiInfo(this,event)" data-title="Partial Period" data-desc="'+_esc(desc)+'" data-date="">i</button>'
  };
}
function _partialPeriodWarn(kind) {
  const {label, btn} = _partialPeriodInfoBtn(kind);
  return '<div class="kpi-card-delta neu">&#9888; '+label+' - partial period'+btn+'</div>';
}
function _partialPeriodWarnSpan(kind) {
  const {label, btn} = _partialPeriodInfoBtn(kind);
  return '<span class="delta neu">&#9888; '+label+' - partial period'+btn+'</span>';
}

function kpiCard(label,val,pp,py,fmt,info,extraHtml){
  const f=fmt||(v=>Number.isFinite(v)?v.toLocaleString():v);
  const dpp=pp!=null&&pp!==0?(val-pp)/pp*100:null;
  const dpy=py!=null&&py!==0?(val-py)/py*100:null;
  const ppH = _partialPeriod.pp
    ? (dpp!=null ? _partialPeriodWarn('period') : '')
    : (dpp!=null?'<div class="kpi-card-delta '+(dpp>=0?'pos':'neg')+'">'+(dpp>=0?'&#9650;':'&#9660;')+' '+Math.abs(dpp).toFixed(1)+'% prev period</div>':'');
  const pyH = _partialPeriod.py
    ? (dpy!=null ? _partialPeriodWarn('year') : '')
    : (dpy!=null?'<div class="kpi-card-delta '+(dpy>=0?'pos':'neg')+'">'+(dpy>=0?'&#9650;':'&#9660;')+' '+Math.abs(dpy).toFixed(1)+'% prev year</div>':'');
  const infoBtn = info ? '<button class="kpi-info-btn" onclick="showKpiInfo(this,event)" data-title="'+_esc(info.title||label)+'" data-desc="'+_esc(info.desc)+'" data-date="'+_esc(info.date||'')+'">i</button>' : '';
  return '<div class="kpi-card">'+infoBtn+'<div class="kpi-card-label">'+label+'</div><div class="kpi-card-val">'+f(val)+'</div>'+ppH+pyH+(extraHtml||'')+'</div>';
}
function _esc(s){return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function _csvField(v) {
  const s = v == null ? '' : String(v);
  return /[",\n]/.test(s) ? '"'+s.replace(/"/g,'""')+'"' : s;
}

// Builds a CSV from a header row + array of row arrays, triggers a browser
// download via a Blob + synthetic <a download>, and revokes the object URL.
function downloadCSV(filename, header, rows) {
  const csv = [header, ...rows].map(r => r.map(_csvField).join(',')).join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function toSourceTimeSeries(dailyRows,monthlyRows,gran){
  const g=gran||GRAN;
  const map={};
  const allRows=g==='daily'?dailyRows:[...dailyRows,...monthlyRows];
  allRows.forEach(r=>{
    let key;
    if(g==='daily')key=r.date;
    else if(g==='weekly'){const d=new Date((r.date||r.month)+'T00:00:00Z');const ws=new Date(d);ws.setUTCDate(d.getUTCDate()-d.getUTCDay());key=ws.toISOString().slice(0,10);}
    else key=(r.date||r.month).slice(0,7)+'-01';
    const mk=key+'|'+r.source;
    if(!map[mk])map[mk]={key,source:r.source,signups:0,first_visits:0,first_activations:0};
    map[mk].signups+=r.signups||0;map[mk].first_visits+=r.first_visits||0;map[mk].first_activations+=r.first_activations||0;
  });
  return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
}

function cprKpiCard(label,val,pp,py,fmt,info,extraHtml){
  const f=fmt||(v=>Number.isFinite(v)?v.toLocaleString():v);
  const dpp=pp!=null&&pp!==0?(val-pp)/pp*100:null;
  const dpy=py!=null&&py!==0?(val-py)/py*100:null;
  const ppH=_partialPeriod.pp
    ? (dpp!=null ? _partialPeriodWarn('period') : '')
    : (dpp!=null?'<div class="kpi-card-delta '+(dpp<=0?'pos':'neg')+'">'+(dpp<=0?'&#9660;':'&#9650;')+' '+Math.abs(dpp).toFixed(1)+'% prev period</div>':'');
  const pyH=_partialPeriod.py
    ? (dpy!=null ? _partialPeriodWarn('year') : '')
    : (dpy!=null?'<div class="kpi-card-delta '+(dpy<=0?'pos':'neg')+'">'+(dpy<=0?'&#9660;':'&#9650;')+' '+Math.abs(dpy).toFixed(1)+'% prev year</div>':'');
  const infoBtn = info ? '<button class="kpi-info-btn" onclick="showKpiInfo(this,event)" data-title="'+_esc(info.title||label)+'" data-desc="'+_esc(info.desc)+'" data-date="'+_esc(info.date||'')+'">i</button>' : '';
  return '<div class="kpi-card">'+infoBtn+'<div class="kpi-card-label">'+label+'</div><div class="kpi-card-val">'+f(val)+'</div>'+ppH+pyH+(extraHtml||'')+'</div>';
}

// -- Network average comparison -----------------------------------------
// Renders a single "vs network average" delta line appended to a kpi-card,
// with an (i) button explaining how the average was calculated. The network
// average itself is computed by the caller (per-studio maps live in each
// dashboard's data, not in utils.js) and passed in as avgPerStudio.
//   actual       — the card's current value for the selected studios
//   avgPerStudio — average value per network studio (population and
//                  zero-handling are the caller's call — see index.html)
//   netCount     — how many studios avgPerStudio was averaged across
//   selCount     — how many studios are currently selected
//   fmt          — value formatter (same as kpiCard's fmt)
//   invert       — true when lower is better (CPL/CPFT/CPA), matching
//                  cprKpiCard's color logic
//   scale        — true to project avgPerStudio * selCount (additive
//                  metrics like Leads/Spend); false to use avgPerStudio
//                  as-is (blended ratio metrics like CPL, which don't
//                  scale with studio count)
//   info         — {label, costBased, ratioDesc} describing the metric for
//                  the (i) popup copy:
//                    label      — metric name, e.g. 'leads', 'Meta Ads Spend', 'CPL'
//                    costBased  — true if the population is "studios with
//                                 real digital ads spend during this period"
//                                 (all cost cards) rather than every open
//                                 network studio (volume cards)
//                    ratioDesc  — for scale=false only, e.g. 'total spend ÷ total leads'
function _netAvgCore(actual, avgPerStudio, netCount, selCount, scale, invert) {
  if (!netCount || selCount <= 0) return null;
  const projected = scale ? avgPerStudio * selCount : avgPerStudio;
  if (!(projected > 0)) return null;
  const delta = (actual - projected) / projected * 100;
  const cls   = invert ? (delta <= 0 ? 'pos' : 'neg') : (delta >= 0 ? 'pos' : 'neg');
  const arrow = invert ? (delta <= 0 ? '&#9660;' : '&#9650;') : (delta >= 0 ? '&#9650;' : '&#9660;');
  return {projected, delta, cls, arrow};
}

function _netAvgInfoBtn(avgPerStudio, netCount, selCount, projected, scale, fmt, info) {
  const f = fmt || (v => Number.isFinite(v) ? v.toLocaleString() : v);
  const label       = (info && info.label) || 'value';
  const studioWord  = netCount === 1 ? 'studio' : 'studios';
  const spendPhrase = ' with real digital ads spend during this period';

  const population = (info && info.costBased) ? (netCount+' '+studioWord+spendPhrase) : (netCount+' '+studioWord);
  let desc;
  if (scale) {
    desc = 'Network average '+label+' is calculated across '+population+': '+f(avgPerStudio)+' per studio.';
    if (selCount > 1) {
      desc += '\nFor your '+selCount+' selected studios, the expected value is '+f(projected)+'.';
    }
  } else {
    const ratioDesc = (info && info.ratioDesc) || 'total ÷ total';
    desc = 'Network average '+label+' is calculated as a blended ratio ('+ratioDesc+') across '+population+': '+f(avgPerStudio)+'.';
  }
  return '<button class="kpi-info-btn kpi-info-inline" onclick="showKpiInfo(this,event)" data-title="Network Average" data-desc="'+_esc(desc)+'" data-date="">i</button>';
}

function netAvgHtml(actual, avgPerStudio, netCount, selCount, fmt, invert, scale, info) {
  const core = _netAvgCore(actual, avgPerStudio, netCount, selCount, scale, invert);
  if (!core) return '';
  const infoBtn = _netAvgInfoBtn(avgPerStudio, netCount, selCount, core.projected, scale, fmt, info);
  return '<div class="kpi-card-delta '+core.cls+'">'+core.arrow+' '+Math.abs(core.delta).toFixed(1)+'% vs network avg'+infoBtn+'</div>';
}

// Same as netAvgHtml, but rendered as <span class="delta"> for the funnel
// bars, which use a flex-column of <span> deltas instead of kpi-card divs.
function netAvgSpan(actual, avgPerStudio, netCount, selCount, fmt, invert, scale, info) {
  const core = _netAvgCore(actual, avgPerStudio, netCount, selCount, scale, invert);
  if (!core) return '';
  const infoBtn = _netAvgInfoBtn(avgPerStudio, netCount, selCount, core.projected, scale, fmt, info);
  return '<span class="delta '+core.cls+'">'+core.arrow+' '+Math.abs(core.delta).toFixed(1)+'% vs network avg'+infoBtn+'</span>';
}

function getMock() {
  const studios = [
    'SWEAT440 Miami Beach','SWEAT440 Brickell','SWEAT440 Wynwood',
    'SWEAT440 Coral Gables','SWEAT440 Doral','SWEAT440 Miami Lakes'
  ];
  const sources = [
    'Website (unattributed)','Business Mode / App','Public API','Meta Ads','Google Ads',
    'SWEAT440 App','MindBody App','Social Media Organic','Local Listings',
    'Word of Mouth','Print Ads / Signs','N/A','Other'
  ];
  // Deterministic seed — no Math.random()
  function seed(studio, source, month) {
    const si = studios.indexOf(studio), ri = sources.indexOf(source);
    const mi = parseInt(month.slice(5,7));
    return (si+1)*7 + (ri+1)*3 + mi;
  }
  const monthly = [];
  for (let y=2023; y<=2025; y++) {
    for (let mo=1; mo<=12; mo++) {
      const month = y+'-'+String(mo).padStart(2,'0')+'-01';
      studios.forEach(st => {
        sources.forEach(sr => {
          const b = seed(st, sr, month);
          monthly.push({ month, studio:st, source:sr,
            signups:           b,
            first_visits:      Math.floor(b*0.7),
            first_activations: Math.floor(b*0.15),
            first_sales:       Math.floor(b*0.08),
          });
        });
      });
    }
  }
  // Daily: last 90 days, also deterministic
  const daily = [];
  const baseMs = new Date('2026-01-20T00:00:00Z').getTime(); // JS will compute this correctly at runtime
  for (let i=89; i>=0; i--) {
    const d = new Date(baseMs - i*86400000);
    const ds = d.getUTCFullYear()+'-'+String(d.getUTCMonth()+1).padStart(2,'0')+'-'+String(d.getUTCDate()).padStart(2,'0');
    studios.forEach((st,si) => {
      sources.forEach((sr,ri) => {
        const b = (si+1)*2 + (ri+1) + (i%7);
        daily.push({ date:ds, studio:st, source:sr,
          signups:           b,
          first_visits:      Math.floor(b*0.7),
          first_activations: Math.floor(b*0.15),
          first_sales:       Math.floor(b*0.08),
        });
      });
    });
  }
  return { studios, sources, monthly_detail:monthly, daily_detail:daily };
}