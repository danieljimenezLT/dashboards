// Cloudflare Worker — proxies GitHub Actions workflow_dispatch calls, the
// Add Studio admin widget, the Brand Safety Audit's Buffer export, and the
// on-demand Snowflake lead-detail export, so none of those tokens ship to
// the browser. Secrets (GITHUB_TOKEN, APP_SECRET, BUFFER_API_TOKEN, SF_TOKEN)
// are configured via `wrangler secret put`, never committed here.

const REPO = 'dev-leadteam/dashboards';
// Production origin, plus local dev servers matching this repo's own
// documented test setup (CLAUDE.md's `python3 -m http.server 8080`) and
// VS Code's Live Server default port, so features can be tested before merge.
const ALLOWED_ORIGINS = [
  'https://reports.sweat440.com',
  'https://dev-leadteam.github.io',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
  'http://localhost:5500',
  'http://127.0.0.1:5500',
];

const WORKFLOW_SETS = {
  all:       ['nso_daily_refresh.yml', 'refresh.yml', 'refresh-paid-ads.yml'],
  'no-ads':  ['refresh-no-ads.yml'],
  snowflake: ['refresh-snowflake.yml'],
};

function corsHeaders(origin) {
  return {
    'Access-Control-Allow-Origin':  ALLOWED_ORIGINS.includes(origin) ? origin : 'null',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-App-Secret',
  };
}

function jsonResponse(obj, status, headers) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...headers, 'Content-Type': 'application/json' },
  });
}

function b64EncodeUnicode(str) {
  return btoa(unescape(encodeURIComponent(str)));
}

function b64DecodeUnicode(b64) {
  return decodeURIComponent(escape(atob(b64)));
}

function slugify(name) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

// Normalize a studio name for duplicate comparison: case-insensitive, and
// dashes/extra whitespace collapsed — so "Dallas - Uptown" and "Dallas Uptown"
// (or "dallas  uptown") are recognized as the same studio.
function normalizeName(name) {
  return name.toLowerCase().replace(/[-–—]/g, ' ').replace(/\s+/g, ' ').trim();
}

function ghHeaders(env) {
  return {
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    'Content-Type': 'application/json',
    'User-Agent': 'sweat440-refresh-proxy',
    Accept: 'application/vnd.github+json',
  };
}

async function dispatchWorkflows(body, env, headers) {
  const workflows = WORKFLOW_SETS[body.set];
  if (!workflows) return jsonResponse({ ok: false, error: 'Unknown workflow set' }, 400, headers);

  const results = await Promise.all(
    workflows.map(wf =>
      fetch(`https://api.github.com/repos/${REPO}/actions/workflows/${wf}/dispatches`, {
        method: 'POST',
        headers: ghHeaders(env),
        body: JSON.stringify({ ref: 'main' }),
      })
    )
  );

  const statuses = results.map(r => r.status);
  const allOk = statuses.every(s => s === 204);
  return jsonResponse({ ok: allOk, workflows, statuses }, allOk ? 200 : 502, headers);
}

async function handleTriggerSocialAuditRefresh(body, env, headers) {
  const month = typeof body.month === 'string' ? body.month.trim() : '';
  if (month && !/^\d{4}-\d{2}$/.test(month)) {
    return jsonResponse({ ok: false, error: 'month must be YYYY-MM.' }, 400, headers);
  }

  const dispatchBody = { ref: 'main' };
  if (month) dispatchBody.inputs = { month };

  const res = await fetch(`https://api.github.com/repos/${REPO}/actions/workflows/social-media-audit-refresh.yml/dispatches`, {
    method: 'POST',
    headers: ghHeaders(env),
    body: JSON.stringify(dispatchBody),
  });
  if (res.status !== 204) {
    return jsonResponse({ ok: false, error: `GitHub returned ${res.status}` }, 502, headers);
  }
  return jsonResponse({ ok: true }, 200, headers);
}

// Build a normalized studios.json entry from whatever fields the widget submitted.
// Every field defaults to null rather than being omitted, so every entry in the
// registry has the same shape.
function normalizeStudio(studio) {
  const str = v => (typeof v === 'string' && v.trim()) ? v.trim() : null;
  const tp = studio.tier_pricing || {};
  const tierKeys = ['tier0_price','tier1_price','tier2_price','tier3_price'];
  const tierObj = {};
  for (const k of tierKeys) if (tp[k] != null && tp[k] !== '') tierObj[k] = Number(tp[k]);
  return {
    name: str(studio.name),
    code: str(studio.code),
    state: str(studio.state),
    status: str(studio.status) || 'nso',
    excluded_default: !!studio.excluded_default,
    nso_track: studio.nso_track ? true : undefined,
    meta: {
      match: str(studio.meta && studio.meta.match),
      ad_account_id: str(studio.meta && studio.meta.ad_account_id),
      page_id: str(studio.meta && studio.meta.page_id),
      instagram_account_id: str(studio.meta && studio.meta.instagram_account_id),
    },
    google_ads: {
      match: str(studio.google_ads && studio.google_ads.match),
      pattern: str(studio.google_ads && studio.google_ads.pattern),
      customer_id: str(studio.google_ads && studio.google_ads.customer_id),
    },
    ga4: { studio_page_path: str(studio.ga4 && studio.ga4.studio_page_path) },
    gbp: {
      location_id: str(studio.gbp && studio.gbp.location_id),
      code: str(studio.gbp && studio.gbp.code),
    },
    snowflake_id: studio.snowflake_id ? Number(studio.snowflake_id) : null,
    social_slug: str(studio.social_slug),
    tier_pricing: Object.keys(tierObj).length ? tierObj : null,
  };
}

// Merge a submitted patch into an existing studios.json entry. For most
// optional fields, a blank/null value in the patch means "leave unchanged"
// (so filling in just one missing field doesn't wipe out the rest of the
// profile). name/state/status/excluded_default always take the submitted
// value as-is — the form always shows a definite current value for those,
// there's no "blank" state to distinguish from "unchanged".
function mergeStudio(existing, patch) {
  const keep = (existingVal, patchVal) => (patchVal === null || patchVal === undefined) ? existingVal : patchVal;
  return {
    name: patch.name || existing.name,
    code: keep(existing.code, patch.code),
    state: patch.state || existing.state,
    status: patch.status || existing.status,
    excluded_default: !!patch.excluded_default,
    meta: {
      match: keep(existing.meta.match, patch.meta.match),
      ad_account_id: keep(existing.meta.ad_account_id, patch.meta.ad_account_id),
      page_id: keep(existing.meta.page_id, patch.meta.page_id),
      instagram_account_id: keep(existing.meta.instagram_account_id, patch.meta.instagram_account_id),
    },
    nso_track: patch.nso_track || existing.nso_track || undefined,
    google_ads: {
      match: keep(existing.google_ads.match, patch.google_ads.match),
      pattern: keep(existing.google_ads.pattern, patch.google_ads.pattern),
      customer_id: keep(existing.google_ads.customer_id, patch.google_ads.customer_id),
    },
    ga4: { studio_page_path: keep(existing.ga4.studio_page_path, patch.ga4.studio_page_path) },
    gbp: {
      location_id: keep(existing.gbp.location_id, patch.gbp.location_id),
      code: keep(existing.gbp.code, patch.gbp.code),
    },
    snowflake_id: keep(existing.snowflake_id, patch.snowflake_id),
    social_slug: keep(existing.social_slug, patch.social_slug),
    // tier_pricing merges key-by-key so e.g. adding tier3 doesn't drop tier1/tier2.
    tier_pricing: patch.tier_pricing
      ? { ...(existing.tier_pricing || {}), ...patch.tier_pricing }
      : existing.tier_pricing,
  };
}

function fieldRows(studio) {
  return [
    ['Code', studio.code],
    ['State', studio.state],
    ['Status', studio.status],
    ['Excluded from dashboards by default', studio.excluded_default],
    ['Meta match', studio.meta.match],
    ['Meta ad account', studio.meta.ad_account_id],
    ['Meta page ID', studio.meta.page_id],
    ['Meta IG ID', studio.meta.instagram_account_id],
    ['Google Ads match', studio.google_ads.match],
    ['Google Ads customer ID', studio.google_ads.customer_id],
    ['Google Ads regex pattern', studio.google_ads.pattern],
    ['GA4 page path', studio.ga4.studio_page_path],
    ['GBP location ID', studio.gbp.location_id],
    ['GBP code', studio.gbp.code],
    ['Snowflake ID', studio.snowflake_id],
    ['Social slug', studio.social_slug],
    ['Tier pricing', studio.tier_pricing ? JSON.stringify(studio.tier_pricing) : null],
  ];
}

function prBodyForAdd(studio) {
  const lines = [
    `Adds **${studio.name}** to the canonical studio registry (\`studios.json\`), submitted via the Add Studio admin widget.`,
    '',
    '| Field | Value |',
    '|---|---|',
    ...fieldRows(studio).map(([label, val]) => `| ${label} | ${val ?? '_(none)_'} |`),
    '',
    'Review the fields above against real ad-account/campaign naming before merging — this only wires up config, it does not verify the studio actually has live campaigns yet.',
  ];
  return lines.join('\n');
}

function prBodyForUpdate(existing, merged) {
  const before = new Map();
  for (const [label, val] of fieldRows(existing)) before.set(label, val);
  const after = fieldRows(merged);
  const changed = after.filter(([label, val]) => JSON.stringify(before.get(label)) !== JSON.stringify(val));

  const lines = [
    `Updates **${existing.name}**'s entry in the canonical studio registry (\`studios.json\`), submitted via the Add Studio admin widget's Edit mode.`,
    '',
  ];
  if (existing.name !== merged.name) {
    lines.push(`Renamed to **${merged.name}**.`, '');
  }
  if (changed.length) {
    lines.push('| Field | Before | After |', '|---|---|---|');
    for (const [label, val] of changed) {
      lines.push(`| ${label} | ${before.get(label) ?? '_(none)_'} | ${val ?? '_(none)_'} |`);
    }
  } else {
    lines.push('_No fields changed._');
  }
  lines.push('', 'Review before merging.');
  return lines.join('\n');
}

// Shared GitHub plumbing for both add and update: read studios.json, let the
// caller mutate the parsed registry (returning an {error, status} to abort),
// then branch + commit + open a PR with the result.
async function openStudiosPr(env, { branchPrefix, commitMessage, prTitle, mutate }) {
  const gh = ghHeaders(env);

  const contentRes = await fetch(`https://api.github.com/repos/${REPO}/contents/studios.json?ref=main`, { headers: gh });
  if (!contentRes.ok) {
    return { error: `Failed to read studios.json (${contentRes.status})`, status: 502 };
  }
  const contentJson = await contentRes.json();
  const registry = JSON.parse(b64DecodeUnicode(contentJson.content));

  const mutation = mutate(registry) || {};
  if (mutation.error) return mutation;
  const prBody = mutation.prBody;

  registry.studios.sort((a, b) => a.name.localeCompare(b.name));
  const newContent = JSON.stringify(registry, null, 2) + '\n';

  const refRes = await fetch(`https://api.github.com/repos/${REPO}/git/ref/heads/main`, { headers: gh });
  if (!refRes.ok) {
    return { error: `Failed to read main ref (${refRes.status})`, status: 502 };
  }
  const mainSha = (await refRes.json()).object.sha;

  const branch = `${branchPrefix}-${Date.now()}`;
  const branchRes = await fetch(`https://api.github.com/repos/${REPO}/git/refs`, {
    method: 'POST',
    headers: gh,
    body: JSON.stringify({ ref: `refs/heads/${branch}`, sha: mainSha }),
  });
  if (!branchRes.ok) {
    return { error: `Failed to create branch (${branchRes.status})`, status: 502 };
  }

  const commitRes = await fetch(`https://api.github.com/repos/${REPO}/contents/studios.json`, {
    method: 'PUT',
    headers: gh,
    body: JSON.stringify({
      message: commitMessage,
      content: b64EncodeUnicode(newContent),
      sha: contentJson.sha,
      branch,
    }),
  });
  if (!commitRes.ok) {
    return { error: `Failed to commit studios.json (${commitRes.status})`, status: 502 };
  }

  const prRes = await fetch(`https://api.github.com/repos/${REPO}/pulls`, {
    method: 'POST',
    headers: gh,
    body: JSON.stringify({ title: prTitle, head: branch, base: 'main', body: prBody }),
  });
  if (!prRes.ok) {
    return { error: `Failed to open pull request (${prRes.status})`, status: 502 };
  }
  const pr = await prRes.json();

  return { prUrl: pr.html_url };
}

// ─────────────────────────────────────────────────────────────────────────────
// User management (users.js)
// ─────────────────────────────────────────────────────────────────────────────

function serializeUserEntry(email, user) {
  let studiosStr;
  if (user.studios === null) {
    studiosStr = 'null';
  } else {
    studiosStr = '[\n' + user.studios.map(s => `      '${s.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`).join(',\n') + ',\n    ]';
  }
  return [
    `  '${email}': {`,
    `    passwordHash: '${user.passwordHash}',`,
    `    name: '${user.name.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}',`,
    `    studios: ${studiosStr},`,
    `    role: '${user.role}'`,
    `  }`,
  ].join('\n');
}

function addUserToFile(content, email, user) {
  const entry = serializeUserEntry(email, user) + ',';
  const closeIdx = content.lastIndexOf('\n};');
  if (closeIdx === -1) throw new Error('Could not find end of USERS object in users.js');
  return content.slice(0, closeIdx) + '\n' + entry + '\n' + content.slice(closeIdx);
}

function updateUserInFile(content, email, user) {
  const esc = email.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const keyRe = new RegExp(`'${esc}'\\s*:\\s*\\{`);
  const keyMatch = keyRe.exec(content);
  if (!keyMatch) return null;

  // Walk forward from the opening brace to find the matching closing brace
  const braceStart = content.indexOf('{', keyMatch.index + keyMatch[0].length - 1);
  let depth = 0, end = -1;
  for (let i = braceStart; i < content.length; i++) {
    if (content[i] === '{') depth++;
    else if (content[i] === '}') { depth--; if (depth === 0) { end = i; break; } }
  }
  if (end === -1) return null;

  let afterEnd = end + 1;
  if (content[afterEnd] === ',') afterEnd++;

  return content.slice(0, keyMatch.index) + serializeUserEntry(email, user) + ',' + content.slice(afterEnd);
}

function getUserCurrentHash(content, email) {
  const esc = email.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const m = content.match(new RegExp(`'${esc}'[\\s\\S]*?passwordHash:\\s*'([^']+)'`));
  return m ? m[1] : null;
}

async function openUsersPr(env, { branchPrefix, commitMessage, prTitle, mutate }) {
  const gh = ghHeaders(env);
  const contentRes = await fetch(`https://api.github.com/repos/${REPO}/contents/users.js?ref=main`, { headers: gh });
  if (!contentRes.ok) return { error: `Failed to read users.js (${contentRes.status})`, status: 502 };
  const contentJson = await contentRes.json();
  const fileContent = b64DecodeUnicode(contentJson.content);

  const mutation = mutate(fileContent) || {};
  if (mutation.error) return mutation;
  const { newContent, prBody } = mutation;

  const refRes = await fetch(`https://api.github.com/repos/${REPO}/git/ref/heads/main`, { headers: gh });
  if (!refRes.ok) return { error: `Failed to read main ref (${refRes.status})`, status: 502 };
  const mainSha = (await refRes.json()).object.sha;

  const branch = `${branchPrefix}-${Date.now()}`;
  const branchRes = await fetch(`https://api.github.com/repos/${REPO}/git/refs`, {
    method: 'POST', headers: gh,
    body: JSON.stringify({ ref: `refs/heads/${branch}`, sha: mainSha }),
  });
  if (!branchRes.ok) return { error: `Failed to create branch (${branchRes.status})`, status: 502 };

  const commitRes = await fetch(`https://api.github.com/repos/${REPO}/contents/users.js`, {
    method: 'PUT', headers: gh,
    body: JSON.stringify({ message: commitMessage, content: b64EncodeUnicode(newContent), sha: contentJson.sha, branch }),
  });
  if (!commitRes.ok) return { error: `Failed to commit users.js (${commitRes.status})`, status: 502 };

  const prRes = await fetch(`https://api.github.com/repos/${REPO}/pulls`, {
    method: 'POST', headers: gh,
    body: JSON.stringify({ title: prTitle, head: branch, base: 'main', body: prBody }),
  });
  if (!prRes.ok) return { error: `Failed to open pull request (${prRes.status})`, status: 502 };
  return { prUrl: (await prRes.json()).html_url };
}

async function handleAddUser(body, env, headers) {
  const { email, name, passwordHash, role, studios } = body.user || {};
  if (!email || !name || !passwordHash || !role) {
    return jsonResponse({ ok: false, error: 'email, name, passwordHash, and role are required.' }, 400, headers);
  }
  const validRoles = ['admin', 'corporate', 'franchisee', 'manager'];
  if (!validRoles.includes(role)) {
    return jsonResponse({ ok: false, error: `Invalid role: ${role}` }, 400, headers);
  }

  const user = { passwordHash, name, studios: studios || null, role };

  const result = await openUsersPr(env, {
    branchPrefix: `add-user-${slugify(email.split('@')[0])}`,
    commitMessage: `Add user: ${email}`,
    prTitle: `Add user: ${email}`,
    mutate(content) {
      const esc = email.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      if (new RegExp(`'${esc}'\\s*:`).test(content)) {
        return { error: `User ${email} already exists in users.js.`, status: 409 };
      }
      const newContent = addUserToFile(content, email, user);
      const prBody = [
        `Adds **${email}** to the dashboard user registry (\`users.js\`), submitted via the User Management admin widget.`,
        '',
        '| Field | Value |',
        '|---|---|',
        `| Name | ${name} |`,
        `| Role | ${role} |`,
        `| Studios | ${studios && studios.length ? studios.join(', ') : '_All studios_'} |`,
        '',
        '⚠️ The password hash is included in the commit. Review the diff before merging.',
      ].join('\n');
      return { newContent, prBody };
    },
  });

  if (result.error) return jsonResponse({ ok: false, error: result.error }, result.status || 500, headers);
  return jsonResponse({ ok: true, prUrl: result.prUrl }, 200, headers);
}

async function handleUpdateUser(body, env, headers) {
  const originalEmail = (typeof body.originalEmail === 'string' ? body.originalEmail.trim().toLowerCase() : '');
  const { email, name, passwordHash, role, studios } = body.user || {};
  if (!originalEmail) return jsonResponse({ ok: false, error: 'Missing originalEmail.' }, 400, headers);
  if (!email || !name || !role) return jsonResponse({ ok: false, error: 'email, name, and role are required.' }, 400, headers);
  const validRoles = ['admin', 'corporate', 'franchisee', 'manager'];
  if (!validRoles.includes(role)) return jsonResponse({ ok: false, error: `Invalid role: ${role}` }, 400, headers);

  const result = await openUsersPr(env, {
    branchPrefix: `update-user-${slugify(email.split('@')[0])}`,
    commitMessage: `Update user: ${originalEmail}`,
    prTitle: `Update user: ${originalEmail}`,
    mutate(content) {
      const finalHash = passwordHash || getUserCurrentHash(content, originalEmail);
      if (!finalHash) return { error: `Could not find existing password hash for ${originalEmail}.`, status: 404 };
      const user = { passwordHash: finalHash, name, studios: studios || null, role };
      const newContent = updateUserInFile(content, originalEmail, user);
      if (!newContent) return { error: `User ${originalEmail} not found in users.js.`, status: 404 };
      const prBody = [
        `Updates **${originalEmail}** in the dashboard user registry (\`users.js\`), submitted via the User Management admin widget.`,
        '',
        '| Field | Value |',
        '|---|---|',
        `| Name | ${name} |`,
        `| Role | ${role} |`,
        `| Studios | ${studios && studios.length ? studios.join(', ') : '_All studios_'} |`,
        `| Password | ${passwordHash ? '_Changed_' : '_Unchanged_'} |`,
      ].join('\n');
      return { newContent, prBody };
    },
  });

  if (result.error) return jsonResponse({ ok: false, error: result.error }, result.status || 500, headers);
  return jsonResponse({ ok: true, prUrl: result.prUrl }, 200, headers);
}

async function handleAddStudio(body, env, headers) {
  const studio = normalizeStudio(body.studio || {});
  if (!studio.name || !studio.state) {
    return jsonResponse({ ok: false, error: 'Studio name and state are required.' }, 400, headers);
  }

  const result = await openStudiosPr(env, {
    branchPrefix: `add-studio-${slugify(studio.name)}`,
    commitMessage: `Add studio: ${studio.name}`,
    prTitle: `Add studio: ${studio.name}`,
    mutate(registry) {
      if (studio.code && registry.studios.some(s => s.code === studio.code)) {
        return { error: `Code ${studio.code} already exists in studios.json.`, status: 409 };
      }
      const normalizedNew = normalizeName(studio.name);
      const nameMatch = registry.studios.find(s => normalizeName(s.name) === normalizedNew);
      if (nameMatch) {
        return {
          error: `"${studio.name}" looks like a duplicate of the existing studio "${nameMatch.name}"` +
                 (nameMatch.code ? ` (${nameMatch.code})` : '') + '.',
          status: 409,
        };
      }
      registry.studios.push(studio);
      return { prBody: prBodyForAdd(studio) };
    },
  });

  if (result.error) return jsonResponse({ ok: false, error: result.error }, result.status || 502, headers);
  return jsonResponse({ ok: true, prUrl: result.prUrl }, 200, headers);
}

async function handleUpdateStudio(body, env, headers) {
  const originalName = typeof body.originalName === 'string' ? body.originalName.trim() : '';
  const patch = normalizeStudio(body.studio || {});
  if (!originalName) {
    return jsonResponse({ ok: false, error: 'Missing originalName — which studio to update.' }, 400, headers);
  }
  if (!patch.name || !patch.state) {
    return jsonResponse({ ok: false, error: 'Studio name and state are required.' }, 400, headers);
  }

  const result = await openStudiosPr(env, {
    branchPrefix: `update-studio-${slugify(patch.name)}`,
    commitMessage: `Update studio: ${patch.name}`,
    prTitle: `Update studio: ${patch.name}`,
    mutate(registry) {
      const idx = registry.studios.findIndex(s => s.name === originalName);
      if (idx === -1) {
        return {
          error: `Couldn't find a studio named "${originalName}" in studios.json — it may have been renamed or removed. Refresh and try again.`,
          status: 404,
        };
      }
      const existing = registry.studios[idx];
      const merged = mergeStudio(existing, patch);

      const others = registry.studios.filter((_, i) => i !== idx);
      if (merged.code && others.some(s => s.code === merged.code)) {
        return { error: `Code ${merged.code} already exists on another studio in studios.json.`, status: 409 };
      }
      const normalizedNew = normalizeName(merged.name);
      const nameMatch = others.find(s => normalizeName(s.name) === normalizedNew);
      if (nameMatch) {
        return {
          error: `"${merged.name}" looks like a duplicate of the existing studio "${nameMatch.name}"` +
                 (nameMatch.code ? ` (${nameMatch.code})` : '') + '.',
          status: 409,
        };
      }

      registry.studios[idx] = merged;
      return { prBody: prBodyForUpdate(existing, merged) };
    },
  });

  if (result.error) return jsonResponse({ ok: false, error: result.error }, result.status || 502, headers);
  return jsonResponse({ ok: true, prUrl: result.prUrl }, 200, headers);
}

// ─── SWEAT440 Brand Safety Audit — Buffer export + cycle publish ───────────
// Two actions, both scoped to brand-safety-audit/data/*.json in this repo:
//   - export_brand_audit_posts   → live-pulls Buffer (BUFFER_API_TOKEN secret,
//     never sent to the browser) and returns a clean per-post export.
//   - publish_brand_audit_cycle  → receives Claude's analysis JSON from the
//     browser and commits it straight to data/latest.json + a dated archive
//     copy (direct to main — this is routine data, not code, same as the
//     scheduled refresh workflows).

const BUFFER_ORG_ID = '651eb8e1f9dfd3e786474267'; // LeadTeam

// Every Instagram channel connected to the LeadTeam Buffer org (43, pulled
// live via Buffer's list_channels — NOT the 9-studio pilot list this used to
// be). Facebook/LinkedIn/YouTube/TikTok/Start Page channels in the same org
// are excluded; this audit is Instagram-content-only. Keep in sync with
// scripts/buffer_client.py's CHANNELS/DISPLAY_NAMES — same source snapshot,
// duplicated here since the Worker can't import that Python module.
const BRAND_AUDIT_CHANNELS = {
  '69efb8695c4c051afae67ea7': 'sweat440aventurafl',
  '68005f2c2799bb0a2395d839': 'sweat440boca',
  '65392b403b0f09002f9a7e25': 'sweat440brickell',
  '6a43e0fa5ab6d2f1068b8577': 'sweat440burlington',
  '6a7c9930b2d9d57743641383': 'sweat440charlottesouthend',
  '65411a93f8d09f1c5e0a8ce0': 'sweat440chelsea',
  '6596d444c3b752215af03cdb': 'sweat440coconutgrove',
  '65411a93f8d09f1c5e0a8cb3': 'sweat440gables',
  '65392ba23b0f09002f9e6a89': 'sweat440coralsprings',
  '69d57dc9031bfa423cdd39e5': 'sweat440dallasuptown',
  '65411a93f8d09f1c5e0a8c7e': 'sweat440deerfield',
  '65392d203b0f09002fad128b': 'sweat440doral',
  '6a33fe6938b5579345ac3157': 'sweat440dunwoody',
  '6865a743acfb098c6929f5cb': 'sweat440eastchester',
  '654a51b47a6cf4d23fe20502': 'sweat440fidi',
  '6a2c455b38b55793458ba0bc': 'sweat440fortmyers',
  '69a1b99d4be271803d73fefe': 'sweat440herriman',
  '66684e487839e9e879c69b53': 'sweat440lasolas',
  '6a3d7c8d5ab6d2f106707848': 'sweat440longbeachny',
  '65411a93f8d09f1c5e0a8c34': 'sweat440miamilakes',
  '6a1de417c687a22dd44d053f': 'sweat440middletown',
  '654a51b47a6cf4d23fe20515': 'sweat440midtownmiami',
  '6596d383c3b752215ae84719': 'sweat440miramar',
  '69824b1531b76c40ca27198e': 'sweat440naplesmercato',
  '65411a93f8d09f1c5e0a8c88': 'sweat440noda',
  '6a01f7b7090476fb990aa1a4': 'sweat440north.miami',
  '655d0c147a6cf4d23ffdc750': 'sweat440oceantownship',
  '6a4ecc374048344628859259': 'sweat440okcrosecreek',
  '6a3d7cec5ab6d2f106707b58': 'sweat440oldbridge',
  '69d57d59031bfa423cdd38be': 'sweat440orlandodrphillips',
  '68757183459a7d1cfb795515': 'sweat440parkslope',
  '666b25557839e9e87956e554': 'sweat440pembrokepines',
  '68fa9c0b669affb4c9830806': 'sweat440pinecrest',
  '68fa9b30669affb4c9830651': 'sweat440prestonwood',
  '68fa9c73669affb4c983092d': 'sweat440reston',
  '65411a93f8d09f1c5e0a8cc4': 'sweat440sobe',
  '65411a93f8d09f1c5e0a8c27': 'sweat440southmiami',
  '65392bc93b0f09002f9fe03a': 'sweat440',
  '65392c0c3b0f09002fa24a51': 'sweat440tomsriver',
  '65411a93f8d09f1c5e0a8c93': 'sweat440uppereastside',
  '686c2b33111211c55767c82a': 'sweat440wallnj',
  '6834d4ce9c617476ae72466a': 'sweat440westpalmbeach',
  '664be217b9b04b20032686ba': 'sweat440highland',
};

const BRAND_AUDIT_DISPLAY_NAMES = {
  'sweat440aventurafl': 'Aventura',
  'sweat440boca': 'Boca Raton',
  'sweat440brickell': 'Brickell',
  'sweat440burlington': 'Burlington',
  'sweat440charlottesouthend': 'Charlotte - South End',
  'sweat440chelsea': 'Chelsea',
  'sweat440coconutgrove': 'Coconut Grove',
  'sweat440gables': 'Coral Gables',
  'sweat440coralsprings': 'Coral Springs',
  'sweat440dallasuptown': 'Dallas - Uptown',
  'sweat440deerfield': 'Deerfield Beach',
  'sweat440doral': 'Doral',
  'sweat440dunwoody': 'Dunwoody',
  'sweat440eastchester': 'Eastchester',
  'sweat440fidi': 'FiDi',
  'sweat440fortmyers': 'Fort Myers',
  'sweat440herriman': 'Herriman',
  'sweat440lasolas': 'Las Olas',
  'sweat440longbeachny': 'Long Beach NY',
  'sweat440miamilakes': 'Miami Lakes',
  'sweat440middletown': 'Middletown',
  'sweat440midtownmiami': 'Midtown Miami',
  'sweat440miramar': 'Miramar',
  'sweat440naplesmercato': 'Naples - Mercato',
  'sweat440noda': 'NoDa Charlotte',
  'sweat440north.miami': 'North Miami',
  'sweat440oceantownship': 'Ocean Township',
  'sweat440okcrosecreek': 'OKC - Rose Creek',
  'sweat440oldbridge': 'Old Bridge',
  'sweat440orlandodrphillips': 'Orlando - Dr Phillips',
  'sweat440parkslope': 'Park Slope',
  'sweat440pembrokepines': 'Pembroke Pines',
  'sweat440pinecrest': 'Pinecrest',
  'sweat440prestonwood': 'Prestonwood',
  'sweat440reston': 'Reston',
  'sweat440sobe': 'South Beach',
  'sweat440southmiami': 'South Miami',
  'sweat440': 'SWEAT440 (Corporate)',
  'sweat440tomsriver': 'Toms River',
  'sweat440uppereastside': 'Upper East Side',
  'sweat440wallnj': 'Wall Township',
  'sweat440westpalmbeach': 'West Palm Beach',
  'sweat440highland': 'Austin - Highland',
};

// Repo-root-relative — brand-safety-audit/ sits alongside nso-dashboard/,
// admin-reports/, etc. in dev-leadteam/dashboards, not nested under pkg/
// (that's only a local staging path in the working copy this was built in).
const BRAND_AUDIT_DATA_DIR = 'brand-safety-audit/data';

// Field/type names and the startDate+endDate requirement confirmed against
// developers.buffer.com and this org's real data — see
// brand-safety-audit/scripts/buffer_client.py for the Python equivalent and
// full notes (this Worker and that script must be kept in sync manually).
const BUFFER_POSTS_QUERY = `
query GetSentPosts(
  $organizationId: OrganizationId!
  $channelIds: [ChannelId!]
  $startDate: DateTime!
  $endDate: DateTime!
  $first: Int!
  $after: String
) {
  posts(
    first: $first
    after: $after
    input: {
      organizationId: $organizationId
      filter: { status: [sent], channelIds: $channelIds, startDate: $startDate, endDate: $endDate }
      sort: [{ field: dueAt, direction: desc }]
    }
  ) {
    edges {
      node {
        id
        text
        channelId
        dueAt
        sentAt
        externalLink
        metadata {
          ... on InstagramPostMetadata {
            type
          }
        }
        assets {
          id
          type
          mimeType
          source
          thumbnail
        }
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
`;

const BUFFER_CHANNELS_QUERY = `
query GetChannels($organizationId: OrganizationId!) {
  channels(input: { organizationId: $organizationId }) {
    id
    service
    isDisconnected
  }
}
`;

// ~1,000 posts/week across the org (mostly Stories, which are filtered out
// later — Buffer has no post-type filter), so 100/page. MAX_PAGES keeps one
// call inside the Worker's subrequest limit; the dashboard splits long
// ranges into 7-day chunks so a single call never gets close to it.
const BUFFER_PAGE_SIZE = 100;
const BUFFER_MAX_PAGES = 40;

// Buffer intermittently returns UNEXPECTED_ERROR ("connection ... timed
// out") on large pages — one retry clears it in practice. Its rate limit is
// 100 requests / 15 min per token (shared with scripts/buffer_client.py
// runs using the same token), surfaced as a wait-N-minutes message.
async function bufferQuery(env, query, variables) {
  for (let attempt = 0; ; attempt++) {
    const res = await fetch('https://api.buffer.com', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.BUFFER_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query, variables }),
    });
    if (res.status === 429) {
      const mins = Math.ceil((parseInt(res.headers.get('Retry-After'), 10) || 900) / 60);
      throw new Error(`Buffer rate limit reached (100 requests per 15 min) — try again in ~${mins} min.`);
    }
    const data = await res.json();
    if (!data.errors) return data.data;
    const transient = data.errors.every((e) => e.extensions && e.extensions.code === 'UNEXPECTED_ERROR');
    if (!transient || attempt >= 1) throw new Error('Buffer API errors: ' + JSON.stringify(data.errors));
  }
}

// Buffer rejects the WHOLE posts query with FORBIDDEN ("Actor can not access
// the specified channels") if even one requested channel id no longer
// exists in the org — which is what broke Download when Zilker's Instagram
// was removed. So intersect BRAND_AUDIT_CHANNELS with the org's live,
// connected Instagram channels first, and report the rest instead of failing.
async function resolveBrandAuditChannels(env) {
  const data = await bufferQuery(env, BUFFER_CHANNELS_QUERY, { organizationId: BUFFER_ORG_ID });
  const live = new Set(
    data.channels.filter((c) => c.service === 'instagram' && !c.isDisconnected).map((c) => c.id)
  );
  const available = [];
  const unavailable = [];
  for (const [id, handle] of Object.entries(BRAND_AUDIT_CHANNELS)) {
    if (live.has(id)) available.push(id);
    else unavailable.push(handle);
  }
  return { available, unavailable };
}

// Both date args are full ISO datetimes (see resolveDateRange). Buffer's
// filter silently returns zero posts if startDate is sent without endDate —
// see scripts/buffer_client.py for the confirmed-against-real-data note.
async function pullBufferPosts(env, channelIds, startDate, endDate) {
  const posts = [];
  let after = null;
  for (let page = 0; page < BUFFER_MAX_PAGES; page++) {
    const data = await bufferQuery(env, BUFFER_POSTS_QUERY, {
      organizationId: BUFFER_ORG_ID,
      channelIds,
      startDate,
      endDate,
      first: BUFFER_PAGE_SIZE,
      after,
    });
    const page_ = data.posts;
    posts.push(...page_.edges.map((e) => e.node));
    if (!page_.pageInfo.hasNextPage) return posts;
    after = page_.pageInfo.endCursor;
  }
  // Results are sorted newest-first, so stopping here would silently drop
  // the OLDEST posts of the range — fail loudly instead of exporting a
  // partial range under a coverage that claims it's complete.
  throw new Error(
    `More than ${BUFFER_MAX_PAGES * BUFFER_PAGE_SIZE} posts in this range — pick a shorter date range.`
  );
}

// Buffer keeps a permanent copy of every synced Story (and every video's
// cover frame) at this fixed S3 path — it outlives the Story itself, whose
// instagram.com/stories/... link is dead after 24h. Assets carry it as
// `thumbnail`; the path is rebuilt here only as a fallback.
function bufferBackfillUrl(post) {
  return `https://buffer-updates-media-backfill-bucket.s3.amazonaws.com/${BUFFER_ORG_ID}/${post.channelId}/${post.id}_thumbnail.jpg`;
}

// Allow-list, not block-list. Returns { postType, imageUrl, link } for what
// the audit reviews, or null to skip:
//   - feed post  → "Static" / "Carousel" (cover image), link = Instagram
//   - Story      → "Story" (full image) or "Video story (cover)" (cover frame
//                  only — the video file expires in 1-2 days), image AND
//                  link = Buffer's permanent S3 copy
//   - Reel / anything else → skipped (video review isn't built yet)
function postTypeAndAsset(post) {
  const type = (post.metadata || {}).type;
  const assets = post.assets || [];

  if (type === 'post') {
    const imageAssets = assets.filter((a) => (a.mimeType || '').startsWith('image/'));
    if (!imageAssets.length) return null;
    return {
      postType: imageAssets.length > 1 ? 'Carousel' : 'Static',
      imageUrl: imageAssets[0].source,
      link: post.externalLink || '',
    };
  }

  if (type === 'story' && assets.length) {
    const asset = assets[0];
    const url = asset.thumbnail || bufferBackfillUrl(post);
    const isVideo = asset.type === 'video' || (asset.mimeType || '').startsWith('video/');
    return { postType: isVideo ? 'Video story (cover)' : 'Story', imageUrl: url, link: url };
  }

  return null;
}

// `startDate`/`endDate` are the plain "YYYY-MM-DD" strings the request was
// resolved to (see resolveDateRange) — not the full ISO datetimes sent to
// Buffer. `cycle`/`period` key off the END of the range, not "today": a
// review of an older date range should file under the month it covers, not
// the month it happened to be run in.
function buildBrandAuditExport(posts, startDate, endDate, unavailable) {
  const byChannel = {};
  for (const post of posts) {
    (byChannel[post.channelId] = byChannel[post.channelId] || []).push(post);
  }

  const accounts = [];
  let skipped = 0;
  for (const [channelId, handle] of Object.entries(BRAND_AUDIT_CHANNELS)) {
    if (unavailable.includes(handle)) continue;
    const channelPosts = byChannel[channelId] || [];
    const postsForReview = [];
    for (const post of channelPosts) {
      const item = postTypeAndAsset(post);
      if (!item) { skipped++; continue; }
      postsForReview.push({
        id: String(post.id),
        date: post.sentAt || post.dueAt || '',
        postType: item.postType,
        caption: post.text || '',
        imageUrl: item.imageUrl,
        link: item.link,
      });
    }
    accounts.push({
      name: BRAND_AUDIT_DISPLAY_NAMES[handle] || handle,
      handle,
      posts: postsForReview,
    });
  }

  const coverageEnd = new Date(endDate + 'T00:00:00Z');
  return {
    cycle: coverageEnd.toISOString().slice(0, 7),
    period: coverageEnd.toLocaleString('en-US', { month: 'long', year: 'numeric', timeZone: 'UTC' }),
    generated: new Date().toISOString().slice(0, 10),
    coverage: { from: startDate, to: endDate },
    note: `${skipped} Reel(s) excluded — video review isn't built yet.`
      + (unavailable.length ? ` Not in Buffer anymore (skipped): ${unavailable.join(', ')}.` : ''),
    skipped,
    unavailableChannels: unavailable,
    accounts,
  };
}

// Defaults to the last 7 days when the request doesn't specify a range.
// Accepts plain "YYYY-MM-DD" dates (what a browser <input type="date"> sends)
// and returns both the plain dates (for display/cycle-naming) and the full
// ISO datetimes Buffer's API requires (start of day / end of day, UTC).
function resolveDateRange(body) {
  const today = new Date();
  const todayStr = today.toISOString().slice(0, 10);
  const weekAgoStr = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const startDate = (body && body.startDate) || weekAgoStr;
  const endDate = (body && body.endDate) || todayStr;

  if (new Date(startDate) > new Date(endDate)) {
    throw new Error('startDate must not be after endDate');
  }

  return {
    startDate,
    endDate,
    startDateTime: `${startDate}T00:00:00Z`,
    endDateTime: `${endDate}T23:59:59Z`,
  };
}

async function handleExportBrandAuditPosts(body, env, headers) {
  let range;
  try {
    range = resolveDateRange(body);
  } catch (e) {
    return jsonResponse({ ok: false, error: e.message }, 400, headers);
  }
  const { available, unavailable } = await resolveBrandAuditChannels(env);
  const posts = await pullBufferPosts(env, available, range.startDateTime, range.endDateTime);
  const exportData = buildBrandAuditExport(posts, range.startDate, range.endDate, unavailable);
  return jsonResponse({ ok: true, export: exportData }, 200, headers);
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

function validateBrandAuditPayload(payload) {
  if (!payload || !Array.isArray(payload.accounts)) return 'Missing "accounts" array.';
  // "coverage" is REQUIRED, not just recommended: the publish handler picks
  // which cycle becomes latest.json by comparing coverage.to across every
  // published cycle, and a coverage-less payload is deliberately never
  // allowed to win that comparison (an earlier bug let one silently
  // overwrite a later cycle). Without this check, a coverage-less payload
  // instead gets silently stuck behind whatever's already latest — twice
  // now the manual review flow has shipped a file missing this field, so
  // reject it up front with an actionable error instead of accepting a
  // publish that the dashboard then appears to ignore.
  if (
    !payload.coverage ||
    !ISO_DATE.test(payload.coverage.from || '') ||
    !ISO_DATE.test(payload.coverage.to || '')
  ) {
    return 'Missing or invalid "coverage" — needs {"from":"YYYY-MM-DD","to":"YYYY-MM-DD"} for the actual date range reviewed, not the date this was generated. Without it the dashboard can silently fail to show this cycle as the latest.';
  }
  for (const a of payload.accounts) {
    if (typeof a.handle !== 'string' || !Array.isArray(a.flags)) {
      return `Account "${a.name || '?'}" is missing "handle" or "flags".`;
    }
    // postLog is optional (older cycles won't have it) — but if present it
    // must be an array, since it's what drives the dashboard's post-date filter.
    if (a.postLog !== undefined && !Array.isArray(a.postLog)) {
      return `Account "${a.name || '?'}" has a "postLog" that isn't an array.`;
    }
  }
  return null;
}

// Commits multiple files in ONE atomic commit via the Git Data API (blobs →
// tree → commit → move the branch ref) — NOT one `contents` PUT per file.
// Publishing writes both data/latest.json and data/<cycle>.json; two separate
// PUTs are two separate commits, and two commits landing seconds apart each
// trigger their own `deploy.yml` run — those two runs then race to push the
// gh-pages branch and the loser fails with "rejected (fetch first)".
// Confirmed in production (run 31644338645). One commit → one deploy trigger,
// and it's the semantically correct shape for "one logical change" anyway.
async function putRepoFiles(env, files, message) {
  const gh = ghHeaders(env);

  const refRes = await fetch(`https://api.github.com/repos/${REPO}/git/ref/heads/main`, { headers: gh });
  if (!refRes.ok) throw new Error(`Failed to read main ref (${refRes.status})`);
  const parentSha = (await refRes.json()).object.sha;

  const parentCommitRes = await fetch(`https://api.github.com/repos/${REPO}/git/commits/${parentSha}`, { headers: gh });
  if (!parentCommitRes.ok) throw new Error(`Failed to read parent commit (${parentCommitRes.status})`);
  const baseTreeSha = (await parentCommitRes.json()).tree.sha;

  const treeItems = [];
  for (const file of files) {
    const blobRes = await fetch(`https://api.github.com/repos/${REPO}/git/blobs`, {
      method: 'POST',
      headers: gh,
      body: JSON.stringify({ content: file.content, encoding: 'utf-8' }),
    });
    if (!blobRes.ok) throw new Error(`Failed to create blob for ${file.path} (${blobRes.status})`);
    const blobSha = (await blobRes.json()).sha;
    treeItems.push({ path: file.path, mode: '100644', type: 'blob', sha: blobSha });
  }

  const treeRes = await fetch(`https://api.github.com/repos/${REPO}/git/trees`, {
    method: 'POST',
    headers: gh,
    body: JSON.stringify({ base_tree: baseTreeSha, tree: treeItems }),
  });
  if (!treeRes.ok) throw new Error(`Failed to create tree (${treeRes.status})`);
  const newTreeSha = (await treeRes.json()).sha;

  const commitRes = await fetch(`https://api.github.com/repos/${REPO}/git/commits`, {
    method: 'POST',
    headers: gh,
    body: JSON.stringify({ message, tree: newTreeSha, parents: [parentSha] }),
  });
  if (!commitRes.ok) throw new Error(`Failed to create commit (${commitRes.status})`);
  const newCommitSha = (await commitRes.json()).sha;

  const updateRefRes = await fetch(`https://api.github.com/repos/${REPO}/git/refs/heads/main`, {
    method: 'PATCH',
    headers: gh,
    body: JSON.stringify({ sha: newCommitSha }),
  });
  if (!updateRefRes.ok) {
    const errText = await updateRefRes.text().catch(() => '');
    throw new Error(`Failed to update main ref (${updateRefRes.status}): ${errText}`);
  }
}

// Cycle files used to be keyed by calendar month ("2026-08.json"), which
// meant a second run landing in the same month — e.g. backfilling Aug 1-4
// after Aug 5-12 was already published — silently overwrote the earlier
// one. Key by the actual date range reviewed instead, so every publish gets
// its own file and history only ever accumulates. Falls back to the
// generation date for older manual exports that omit "coverage".
function cycleFilenameBase(payload) {
  const coverage = payload.coverage;
  if (coverage && coverage.from && coverage.to) return `${coverage.from}_to_${coverage.to}`;
  return payload.generated || payload.cycle || new Date().toISOString().slice(0, 10);
}

async function handlePublishBrandAuditCycle(body, env, headers) {
  const payload = body.payload;
  const problem = validateBrandAuditPayload(payload);
  if (problem) return jsonResponse({ ok: false, error: problem }, 400, headers);

  const listRes = await fetch(`https://api.github.com/repos/${REPO}/contents/${BRAND_AUDIT_DATA_DIR}`, {
    headers: ghHeaders(env),
  });
  const existingFiles = listRes.ok ? await listRes.json() : [];

  const base = cycleFilenameBase(payload);
  const existingNames = new Set(existingFiles.map(f => f.name.replace(/\.json$/, '')));
  let filename = base;
  for (let n = 2; existingNames.has(filename); n++) filename = `${base}-${n}`;

  // latest.json tracks whichever cycle covers the most RECENT dates, not
  // simply whichever was published most recently — otherwise backfilling an
  // earlier gap would wrongly rewind "reviewed through" and cause the next
  // Download to re-pull dates that are already covered. A payload missing
  // "coverage" only wins by default (nothing to compare its date against).
  let latestPayload = payload;
  let latestTo = payload.coverage && payload.coverage.to;
  for (const f of existingFiles) {
    if (f.name === 'latest.json' || !f.name.endsWith('.json')) continue;
    try {
      const fileRes = await fetch(f.download_url, { headers: ghHeaders(env) });
      const data = await fileRes.json();
      const to = data.coverage && data.coverage.to;
      if (to && (!latestTo || to > latestTo)) {
        latestTo = to;
        latestPayload = data;
      }
    } catch (e) {
      // one unreadable cycle file shouldn't block the publish
    }
  }

  const content = JSON.stringify(payload, null, 2) + '\n';
  const latestContent = JSON.stringify(latestPayload, null, 2) + '\n';

  await putRepoFiles(
    env,
    [
      { path: `${BRAND_AUDIT_DATA_DIR}/${filename}.json`, content },
      { path: `${BRAND_AUDIT_DATA_DIR}/latest.json`, content: latestContent },
    ],
    `Publish brand safety audit cycle ${filename}`
  );

  return jsonResponse({ ok: true }, 200, headers);
}

// The dashboard's post-date filter needs the list of published cycle files
// (brand-safety-audit/data/2026-06.json, brand-safety-audit/data/2026-08-05_to_2026-08-12.json,
// etc.) to search across. The repo is private, so the browser can't hit the
// GitHub Contents API directly — it 404s with no auth even though the path
// exists — this proxies that one read through the Worker's own GITHUB_TOKEN.
async function handleListBrandAuditCycles(env, headers) {
  const res = await fetch(`https://api.github.com/repos/${REPO}/contents/${BRAND_AUDIT_DATA_DIR}`, {
    headers: ghHeaders(env),
  });
  if (!res.ok) return jsonResponse({ ok: false, error: `GitHub returned ${res.status}` }, 502, headers);
  const files = await res.json();
  const cycles = files
    .filter(f => f.name !== 'latest.json' && f.name !== 'statuses.json' && f.name.endsWith('.json'))
    .map(f => f.name.replace(/\.json$/, ''))
    .sort()
    .reverse();
  return jsonResponse({ ok: true, cycles }, 200, headers);
}

// Flag review statuses (Pending review / Reported to Ops / Cleared), keyed
// by flag id, committed as one shared file — this is what makes status
// changes visible to every reviewer/device instead of stuck in one
// browser's localStorage. The dashboard sends the FULL statuses object on
// every save (not a diff), so this is a plain overwrite.
async function handleSaveBrandAuditStatuses(body, env, headers) {
  const statuses = body.statuses;
  if (!statuses || typeof statuses !== 'object' || Array.isArray(statuses)) {
    return jsonResponse({ ok: false, error: 'Missing or invalid "statuses" object.' }, 400, headers);
  }
  const content = JSON.stringify(statuses, null, 2) + '\n';
  await putRepoFiles(env, [{ path: `${BRAND_AUDIT_DATA_DIR}/statuses.json`, content }], 'Update brand safety audit flag statuses');
  return jsonResponse({ ok: true }, 200, headers);
}

// ─── Lead detail on-demand export (LEADS_LIST) ─────────────────────────────
// Live-queries Snowflake's SQL REST API using the same Programmatic Access
// Token (PAT) fetch_data.py uses — never written to git, unlike data.json's
// scheduled aggregate refresh. The PAT is a Wrangler secret (env.SF_TOKEN);
// account/role/warehouse/db/schema mirror fetch_data.py's non-secret defaults.
//
// LEADS_LIST is one row per lead (wide format — CELLPHONE, EMAIL_ID,
// SIGNUP_DATE, FIRST_VISIT_DATE, FIRST_MEMBERSHIP_ACTIVATION_DATE,
// FIRST_SALE_DATE, SOURCE, STUDIO_ID, STUDIO_NAME — confirmed against the
// real view, no EVENT_DATE like the aggregate LEADS view has). The date
// filter uses SIGNUP_DATE, matching "Leads (New Accounts Created)" — the
// same definition the Funnel tab's lead count uses.
//
// Columns are named explicitly (not SELECT *) so the four DATE columns can
// be formatted as YYYY-MM-DD strings — Snowflake's SQL API otherwise returns
// DATE columns as raw internal day-count integers, not human-readable dates.

// Hyphens, not underscores: Cloudflare's resolver rejects underscore
// hostnames (error 1016). Snowflake serves both forms for the same account.
const SF_ACCOUNT   = 'MINDBODYORG-PLAYLIST-DATA-MART-SWEAT440';
const SF_ROLE      = 'SYSADMIN';
const SF_WAREHOUSE = 'COMPUTE_WH';
const SF_DATABASE  = 'MARKETING_REPORTS';
const SF_SCHEMA    = 'PUBLIC';

const LEADS_EXPORT_DEFAULT_CAP = 50000;
const LEADS_EXPORT_MAX_CAP = 100000;

function snowflakeHeaders(env, extra) {
  return {
    Authorization: `Bearer ${env.SF_TOKEN}`,
    'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
    'User-Agent': 'sweat440-refresh-proxy',
    Accept: 'application/json',
    ...extra,
  };
}

// Defaults to the last 30 days when the request doesn't specify a range.
// studios/sources are optional IN-list filters; cap bounds the row count so
// a wide-open request can't hang the Worker or return an unbounded file.
function resolveLeadsExportParams(body) {
  const today = new Date();
  const todayStr = today.toISOString().slice(0, 10);
  const monthAgoStr = new Date(today.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const dateFrom = (body && body.dateFrom) || monthAgoStr;
  const dateTo   = (body && body.dateTo)   || todayStr;
  if (new Date(dateFrom) > new Date(dateTo)) {
    throw new Error('dateFrom must not be after dateTo');
  }

  const cleanList = (arr) => Array.isArray(arr) ? arr.filter(s => typeof s === 'string' && s.trim()) : [];
  const studios = cleanList(body && body.studios);
  const sources = cleanList(body && body.sources);

  const requestedCap = Number(body && body.cap);
  const cap = Number.isFinite(requestedCap) && requestedCap > 0
    ? Math.min(requestedCap, LEADS_EXPORT_MAX_CAP)
    : LEADS_EXPORT_DEFAULT_CAP;

  return { dateFrom, dateTo, studios, sources, cap };
}

// Studio names arrive stripped of the "SWEAT440 " brand prefix (same
// canonical short form used across data.json/paid-ads-data.json — see
// fetch_data.py's strip_brand) — re-add it to match STUDIO_NAME in Snowflake.
function fullStudioName(name) {
  return name.startsWith('SWEAT440 ') ? name : `SWEAT440 ${name}`;
}

function buildLeadsQuery(params) {
  const conditions = ['SIGNUP_DATE >= ?', 'SIGNUP_DATE <= ?'];
  const bindings = {
    '1': { type: 'TEXT', value: params.dateFrom },
    '2': { type: 'TEXT', value: params.dateTo },
  };
  let n = 3;
  if (params.studios.length) {
    conditions.push(`STUDIO_NAME IN (${params.studios.map(() => '?').join(', ')})`);
    for (const s of params.studios) bindings[String(n++)] = { type: 'TEXT', value: fullStudioName(s) };
  }
  if (params.sources.length) {
    conditions.push(`SOURCE IN (${params.sources.map(() => '?').join(', ')})`);
    for (const s of params.sources) bindings[String(n++)] = { type: 'TEXT', value: s };
  }

  const statement =
    `SELECT STUDIO_ID, STUDIO_NAME, EMAIL_ID, CELLPHONE, ` +
    `TO_VARCHAR(SIGNUP_DATE, 'YYYY-MM-DD') AS SIGNUP_DATE, ` +
    `TO_VARCHAR(FIRST_VISIT_DATE, 'YYYY-MM-DD') AS FIRST_VISIT_DATE, ` +
    `TO_VARCHAR(FIRST_MEMBERSHIP_ACTIVATION_DATE, 'YYYY-MM-DD') AS FIRST_MEMBERSHIP_ACTIVATION_DATE, ` +
    `TO_VARCHAR(FIRST_SALE_DATE, 'YYYY-MM-DD') AS FIRST_SALE_DATE, ` +
    `SOURCE ` +
    `FROM ${SF_DATABASE}.${SF_SCHEMA}.LEADS_LIST ` +
    `WHERE ${conditions.join(' AND ')} ` +
    `ORDER BY SIGNUP_DATE LIMIT ${params.cap + 1}`;

  return { statement, bindings };
}

// Fetches the query result, following any additional result partitions the
// Snowflake SQL API reports (large results are split — the first partition
// comes back inline with the statement response, the rest need follow-up GETs).
async function pullLeadsDetail(env, params) {
  const { statement, bindings } = buildLeadsQuery(params);
  const res = await fetch(`https://${SF_ACCOUNT}.snowflakecomputing.com/api/v2/statements`, {
    method: 'POST',
    headers: snowflakeHeaders(env, { 'Content-Type': 'application/json' }),
    body: JSON.stringify({
      statement,
      bindings,
      warehouse: SF_WAREHOUSE,
      role: SF_ROLE,
      database: SF_DATABASE,
      schema: SF_SCHEMA,
      timeout: 60,
    }),
  });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(`Snowflake query failed (${res.status}): ${data.message || JSON.stringify(data)}`);
  }

  const columns = (data.resultSetMetaData && data.resultSetMetaData.rowType || []).map(c => c.name);
  let rows = data.data || [];

  const totalPartitions = (data.resultSetMetaData && data.resultSetMetaData.partitionInfo || []).length || 1;
  for (let p = 1; p < totalPartitions && rows.length <= params.cap; p++) {
    const partRes = await fetch(
      `https://${SF_ACCOUNT}.snowflakecomputing.com/api/v2/statements/${data.statementHandle}?partition=${p}`,
      { headers: snowflakeHeaders(env) }
    );
    const partData = await partRes.json();
    if (!partRes.ok) throw new Error(`Snowflake partition fetch failed (${partRes.status})`);
    rows = rows.concat(partData.data || []);
  }

  return { columns, rows };
}

function buildLeadsExport(result, cap) {
  const truncated = result.rows.length > cap;
  const rows = truncated ? result.rows.slice(0, cap) : result.rows;
  return { columns: result.columns, rows, rowCount: rows.length, truncated };
}

async function handleExportLeadsDetail(body, env, headers) {
  let params;
  try {
    params = resolveLeadsExportParams(body);
  } catch (e) {
    return jsonResponse({ ok: false, error: e.message }, 400, headers);
  }
  const result = await pullLeadsDetail(env, params);
  const exportData = buildLeadsExport(result, params.cap);
  return jsonResponse({ ok: true, export: exportData }, 200, headers);
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const headers = corsHeaders(origin);

    if (request.method === 'OPTIONS') return new Response(null, { headers });
    if (!ALLOWED_ORIGINS.includes(origin)) return new Response('Forbidden origin', { status: 403, headers });
    if (request.method !== 'POST') return new Response('Method not allowed', { status: 405, headers });
    if (request.headers.get('X-App-Secret') !== env.APP_SECRET) {
      return new Response('Unauthorized', { status: 401, headers });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response('Bad request', { status: 400, headers });
    }

    if (body.action === 'add_studio' || body.action === 'update_studio') {
      try {
        return await (body.action === 'add_studio' ? handleAddStudio(body, env, headers) : handleUpdateStudio(body, env, headers));
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'add_user' || body.action === 'update_user') {
      try {
        return await (body.action === 'add_user' ? handleAddUser(body, env, headers) : handleUpdateUser(body, env, headers));
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'export_brand_audit_posts') {
      try {
        return await handleExportBrandAuditPosts(body, env, headers);
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'publish_brand_audit_cycle') {
      try {
        return await handlePublishBrandAuditCycle(body, env, headers);
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'list_brand_audit_cycles') {
      try {
        return await handleListBrandAuditCycles(env, headers);
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'save_brand_audit_statuses') {
      try {
        return await handleSaveBrandAuditStatuses(body, env, headers);
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'trigger_social_audit_refresh') {
      try {
        return await handleTriggerSocialAuditRefresh(body, env, headers);
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    if (body.action === 'export_leads_detail') {
      try {
        return await handleExportLeadsDetail(body, env, headers);
      } catch (e) {
        return jsonResponse({ ok: false, error: `Unexpected error: ${e.message}` }, 500, headers);
      }
    }

    return dispatchWorkflows(body, env, headers);
  },
};
