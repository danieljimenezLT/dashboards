// Cloudflare Worker — proxies GitHub Actions workflow_dispatch calls and the
// Add Studio admin widget so the GitHub token never ships to the browser.
// Secrets (GITHUB_TOKEN, APP_SECRET) are configured via `wrangler secret put`,
// never committed here.

const REPO = 'dev-leadteam/dashboards';
const ALLOWED_ORIGIN = 'https://reports.sweat440.com';

const WORKFLOW_SETS = {
  all:       ['nso_daily_refresh.yml', 'refresh.yml', 'refresh-paid-ads.yml'],
  'no-ads':  ['refresh-no-ads.yml'],
  snowflake: ['refresh-snowflake.yml'],
};

function corsHeaders(origin) {
  return {
    'Access-Control-Allow-Origin':  origin === ALLOWED_ORIGIN ? origin : 'null',
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

// Build a normalized studios.json entry from whatever fields the widget submitted.
// Every field defaults to null rather than being omitted, so every entry in the
// registry has the same shape.
function normalizeStudio(studio) {
  const str = v => (typeof v === 'string' && v.trim()) ? v.trim() : null;
  return {
    name: str(studio.name),
    code: str(studio.code),
    state: str(studio.state),
    status: str(studio.status) || 'nso',
    excluded_default: !!studio.excluded_default,
    meta: {
      match: str(studio.meta && studio.meta.match),
      ad_account_id: str(studio.meta && studio.meta.ad_account_id),
      page_id: str(studio.meta && studio.meta.page_id),
      instagram_account_id: str(studio.meta && studio.meta.instagram_account_id),
    },
    google_ads: {
      match: str(studio.google_ads && studio.google_ads.match),
      customer_id: str(studio.google_ads && studio.google_ads.customer_id),
    },
    ga4: { studio_page_path: str(studio.ga4 && studio.ga4.studio_page_path) },
    gbp: { location_id: str(studio.gbp && studio.gbp.location_id) },
    snowflake_id: studio.snowflake_id ? Number(studio.snowflake_id) : null,
    social_slug: str(studio.social_slug),
    tier_pricing: (studio.tier_pricing && Object.keys(studio.tier_pricing).length) ? studio.tier_pricing : null,
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
    google_ads: {
      match: keep(existing.google_ads.match, patch.google_ads.match),
      customer_id: keep(existing.google_ads.customer_id, patch.google_ads.customer_id),
    },
    ga4: { studio_page_path: keep(existing.ga4.studio_page_path, patch.ga4.studio_page_path) },
    gbp: { location_id: keep(existing.gbp.location_id, patch.gbp.location_id) },
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
    ['GA4 page path', studio.ga4.studio_page_path],
    ['GBP location ID', studio.gbp.location_id],
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
  const before = new Map(fieldRows(existing));
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

const BRAND_AUDIT_CHANNELS = {
  '65392ba23b0f09002f9e6a89': 'sweat440coralsprings',
  '65392c0c3b0f09002fa24a51': 'sweat440tomsriver',
  '65392d203b0f09002fad128b': 'sweat440doral',
  '65411a93f8d09f1c5e0a8cb3': 'sweat440gables',
  '654a51b47a6cf4d23fe20515': 'sweat440midtownmiami',
  '65411a93f8d09f1c5e0a8c27': 'sweat440southmiami',
  '65411a93f8d09f1c5e0a8c34': 'sweat440miamilakes',
  '65411a93f8d09f1c5e0a8c7e': 'sweat440deerfield',
  '65411a93f8d09f1c5e0a8c88': 'sweat440noda',
};

const BRAND_AUDIT_DISPLAY_NAMES = {
  sweat440coralsprings: 'Coral Springs',
  sweat440tomsriver: 'Toms River',
  sweat440doral: 'Doral',
  sweat440gables: 'Coral Gables',
  sweat440midtownmiami: 'Midtown Miami',
  sweat440southmiami: 'South Miami',
  sweat440miamilakes: 'Miami Lakes',
  sweat440deerfield: 'Deerfield Beach',
  sweat440noda: 'NoDa Charlotte',
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

async function pullBufferPosts(env, days = 7) {
  const now = new Date();
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  const fmt = (d) => d.toISOString().replace(/\.\d{3}Z$/, 'Z');

  const posts = [];
  let after = null;
  for (let page = 0; page < 10; page++) { // safety cap — 9 channels/7 days never needs this many pages
    const res = await fetch('https://api.buffer.com', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.BUFFER_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: BUFFER_POSTS_QUERY,
        variables: {
          organizationId: BUFFER_ORG_ID,
          channelIds: Object.keys(BRAND_AUDIT_CHANNELS),
          startDate: fmt(start),
          endDate: fmt(now),
          first: 50,
          after,
        },
      }),
    });
    const data = await res.json();
    if (data.errors) throw new Error('Buffer API errors: ' + JSON.stringify(data.errors));

    const page_ = data.data.posts;
    posts.push(...page_.edges.map((e) => e.node));
    if (!page_.pageInfo.hasNextPage) break;
    after = page_.pageInfo.endCursor;
  }
  return posts;
}

// Allow-list, not block-list: only metadata.type === "post" (a permanent
// Instagram feed post) is evaluated. Reels AND Stories both slip through a
// Reel-only exclusion — Stories have an image asset and no caption but are
// gone in 24h, so any permalink is dead by the time a weekly review happens.
function postTypeAndAsset(post) {
  const metadata = post.metadata || {};
  if (metadata.type !== 'post') return [null, null];

  const imageAssets = (post.assets || []).filter((a) => (a.mimeType || '').startsWith('image/'));
  if (!imageAssets.length) return [null, null];

  const postType = imageAssets.length > 1 ? 'Carousel' : 'Static';
  return [postType, imageAssets[0]];
}

function buildBrandAuditExport(posts) {
  const byChannel = {};
  for (const post of posts) {
    (byChannel[post.channelId] = byChannel[post.channelId] || []).push(post);
  }

  const accounts = [];
  let skipped = 0;
  for (const [channelId, handle] of Object.entries(BRAND_AUDIT_CHANNELS)) {
    const channelPosts = byChannel[channelId] || [];
    const postsForReview = [];
    for (const post of channelPosts) {
      const [postType, asset] = postTypeAndAsset(post);
      if (!asset) { skipped++; continue; }
      postsForReview.push({
        id: String(post.id),
        date: post.sentAt || post.dueAt || '',
        postType,
        caption: post.text || '',
        imageUrl: asset.source,
        link: post.externalLink || '',
      });
    }
    accounts.push({
      name: BRAND_AUDIT_DISPLAY_NAMES[handle] || handle,
      handle,
      posts: postsForReview,
    });
  }

  const today = new Date();
  return {
    cycle: today.toISOString().slice(0, 7),
    period: today.toLocaleString('en-US', { month: 'long', year: 'numeric' }),
    generated: today.toISOString().slice(0, 10),
    note: `${skipped} Reel(s)/Story post(s) excluded — video and 24h-ephemeral content aren't in scope yet.`,
    accounts,
  };
}

async function handleExportBrandAuditPosts(env, headers) {
  const posts = await pullBufferPosts(env);
  const exportData = buildBrandAuditExport(posts);
  return jsonResponse({ ok: true, export: exportData }, 200, headers);
}

function validateBrandAuditPayload(payload) {
  if (!payload || !Array.isArray(payload.accounts)) return 'Missing "accounts" array.';
  for (const a of payload.accounts) {
    if (typeof a.handle !== 'string' || !Array.isArray(a.flags)) {
      return `Account "${a.name || '?'}" is missing "handle" or "flags".`;
    }
  }
  return null;
}

async function getRepoFileSha(env, path) {
  const res = await fetch(`https://api.github.com/repos/${REPO}/contents/${path}?ref=main`, {
    headers: ghHeaders(env),
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Failed to read ${path} (${res.status})`);
  const json = await res.json();
  return json.sha;
}

async function putRepoFile(env, path, content, message) {
  const sha = await getRepoFileSha(env, path);
  const body = { message, content: b64EncodeUnicode(content), branch: 'main' };
  if (sha) body.sha = sha;
  const res = await fetch(`https://api.github.com/repos/${REPO}/contents/${path}`, {
    method: 'PUT',
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text().catch(() => '');
    throw new Error(`Failed to write ${path} (${res.status}): ${errText}`);
  }
}

async function handlePublishBrandAuditCycle(body, env, headers) {
  const payload = body.payload;
  const problem = validateBrandAuditPayload(payload);
  if (problem) return jsonResponse({ ok: false, error: problem }, 400, headers);

  const cycle = payload.cycle || new Date().toISOString().slice(0, 7);
  const content = JSON.stringify(payload, null, 2) + '\n';

  await putRepoFile(env, `${BRAND_AUDIT_DATA_DIR}/latest.json`, content, `Publish brand safety audit cycle ${cycle}`);
  await putRepoFile(env, `${BRAND_AUDIT_DATA_DIR}/${cycle}.json`, content, `Archive brand safety audit cycle ${cycle}`);

  return jsonResponse({ ok: true }, 200, headers);
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const headers = corsHeaders(origin);

    if (request.method === 'OPTIONS') return new Response(null, { headers });
    if (origin !== ALLOWED_ORIGIN) return new Response('Forbidden origin', { status: 403, headers });
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

    if (body.action === 'export_brand_audit_posts') {
      try {
        return await handleExportBrandAuditPosts(env, headers);
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

    return dispatchWorkflows(body, env, headers);
  },
};
