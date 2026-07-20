// natpc-cache — Cloudflare edge cache in front of Supabase catalog reads.
//
// The storefront polls two identical `cards` queries every 30s from every open
// browser; at drop time that's ~20 req/s of the same public data hitting
// Supabase. This worker serves those reads from the Cloudflare edge, so
// Supabase sees roughly one request per query shape per region.
//
// Uses the Cache API keyed on our own hostname: Supabase is itself behind
// Cloudflare, so `cf: {cacheTtl}` fetch options are ignored for its zone.
//
// Scope is deliberately narrow: fully-public catalog data only. Per-buyer data
// (buyers, reservations, giveaway_entries) and all mutating RPCs must stay
// direct-to-Supabase and uncached.
//
// ── 2026-07-19 (drop-readiness pass) ────────────────────────────────────────
// Three changes, all driven by load-testing ahead of a high-traffic drop:
//
//   1. STALE-WHILE-REVALIDATE. Previously entries simply expired after 30s, so
//      at every TTL boundary *all* concurrent visitors missed at once and
//      stampeded Supabase together (measured: 25 concurrent cold requests =>
//      ~4.4s p50, 0% hit rate). Now an entry stays servable for STALE_TTL;
//      once past FRESH_TTL we return it immediately and refresh in the
//      background, so the origin sees one refresh instead of a herd.
//
//   2. RPC CACHING. get_vault_cards / get_member_leaderboard are POSTs, so the
//      old GET-only worker passed them through — every homepage load hit
//      Postgres twice. Both return identical public data for every visitor.
//      POST requests can't be used as Cache API keys, so we synthesise a GET
//      key from the function name + normalised body.
//
//   3. PARAMETER ALLOWLIST. Any unrecognised query string previously minted a
//      fresh cache key, so iterating `?offset=1,2,3...` bypassed the cache
//      entirely and let one client drive Supabase directly. Only the query
//      shapes the storefront actually uses are accepted now; everything else
//      is rejected at the edge without touching the origin.

const FRESH_TTL = 30;   // seconds an entry is served without revalidating
const STALE_TTL = 600;  // seconds an entry remains servable while refreshing
const LOCK_TTL = 10;    // best-effort single-flight window for revalidation

// Per-table allowlist of PostgREST parameters. Derived from every cachedFetch()
// call site in natpc-shop/index.html — keep in sync when adding a query.
const ALLOWED_PARAMS = {
  cards:     new Set(['select', 'order', 'limit', 'visibility', 'status', 'is_permanent', 'is_featured', 'id']),
  settings:  new Set(['select', 'key']),
  reviews:   new Set(['select', 'order', 'limit', 'visible']),
  giveaways: new Set(['select', 'limit', 'is_active']),
};

// Read-only, non-personalised RPCs that are safe to serve from a shared cache.
// Anything token-gated or mutating must never be listed here.
const ALLOWED_RPCS = new Set(['get_vault_cards', 'get_member_leaderboard']);

const MAX_LIMIT = 200;
const MAX_BODY = 256;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'apikey, authorization, content-type',
};

function clientResponse(body, status, contentType, edgeStatus, age) {
  const headers = new Headers(CORS);
  headers.set('Content-Type', contentType);
  // Browsers must not cache — every poll should reach the edge for freshness
  headers.set('Cache-Control', 'no-store');
  headers.set('X-Edge-Cache', edgeStatus);
  if (age !== undefined) headers.set('X-Edge-Age', String(age));
  return new Response(body, { status, headers });
}

const deny = (reason) => new Response(JSON.stringify({ error: 'not allowed', reason }), {
  status: 403,
  headers: { 'Content-Type': 'application/json', ...CORS },
});

// Reject anything outside the storefront's known query vocabulary. This is what
// stops unbounded cache-key generation, so it must run before we touch the cache.
function validateTableQuery(table, params) {
  const allowed = ALLOWED_PARAMS[table];
  if (!allowed) return 'unknown table';
  for (const key of params.keys()) {
    if (!allowed.has(key)) return `unsupported parameter: ${key}`;
  }
  const limit = params.get('limit');
  if (limit !== null && !(Number(limit) >= 0 && Number(limit) <= MAX_LIMIT)) {
    return `limit out of range (max ${MAX_LIMIT})`;
  }
  return null;
}

// Sort params so semantically identical queries share one cache entry.
function canonicalSearch(params) {
  const sorted = [...params.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return new URLSearchParams(sorted).toString();
}

// Stable key for an RPC body: {"p_limit":6} and { "p_limit" : 6 } must collide.
function canonicalBody(raw) {
  if (!raw || !raw.trim()) return '{}';
  const parsed = JSON.parse(raw); // caller handles throw
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('bad body');
  return JSON.stringify(Object.fromEntries(Object.keys(parsed).sort().map(k => [k, parsed[k]])));
}

function upstream(env, path, search, init) {
  return fetch(env.SUPABASE_URL + path + (search ? `?${search}` : ''), {
    ...init,
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      ...(init && init.headers),
    },
  });
}

// Fetch from origin and store with our own freshness stamp. We deliberately set
// a long s-maxage (STALE_TTL) and track real age via X-Cached-At, because the
// Cache API would otherwise evict at FRESH_TTL and leave nothing to serve stale.
async function refresh(cache, cacheKey, env, path, search, init) {
  const res = await upstream(env, path, search, init);
  const body = await res.text();
  const contentType = res.headers.get('Content-Type') || 'application/json';

  // Only cache successes — an upstream blip must not be pinned for STALE_TTL
  if (res.ok) {
    await cache.put(cacheKey, new Response(body, {
      status: res.status,
      headers: {
        'Content-Type': contentType,
        'Cache-Control': `s-maxage=${STALE_TTL}`,
        'X-Cached-At': String(Date.now()),
      },
    }));
  }
  return { body, contentType, status: res.status, ok: res.ok };
}

const lockKeyFor = (cacheKey) =>
  new Request(cacheKey.url + (cacheKey.url.includes('?') ? '&' : '?') + '__lock=1');

const lockResponse = () => new Response('1', {
  headers: { 'Cache-Control': `s-maxage=${LOCK_TTL}`, 'Content-Type': 'text/plain' },
});

async function serve(env, ctx, { cacheKey, path, search, init }) {
  const cache = caches.default;
  const hit = await cache.match(cacheKey);

  if (hit) {
    const cachedAt = Number(hit.headers.get('X-Cached-At') || 0);
    const age = Math.round((Date.now() - cachedAt) / 1000);
    const contentType = hit.headers.get('Content-Type') || 'application/json';

    if (age < FRESH_TTL) {
      return clientResponse(hit.body, hit.status, contentType, 'HIT', age);
    }
    // Stale but usable: answer now, refresh behind the request. The lock is
    // best-effort (the Cache API isn't atomic), so it narrows the revalidation
    // herd rather than eliminating it — writes are deferred off the hot path.
    const lockKey = lockKeyFor(cacheKey);
    if (!(await cache.match(lockKey))) {
      ctx.waitUntil(cache.put(lockKey, lockResponse()));
      ctx.waitUntil(refresh(cache, cacheKey, env, path, search, init).catch(() => {}));
    }
    return clientResponse(hit.body, hit.status, contentType, 'STALE', age);
  }

  const fresh = await refresh(cache, cacheKey, env, path, search, init);
  return clientResponse(fresh.body, fresh.status, fresh.contentType, fresh.ok ? 'MISS' : 'BYPASS', 0);
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    const url = new URL(request.url);

    // ── Cached table reads (GET) ──
    const table = url.pathname.match(/^\/rest\/v1\/([a-z_]+)$/);
    if (request.method === 'GET' && table && ALLOWED_PARAMS[table[1]]) {
      const bad = validateTableQuery(table[1], url.searchParams);
      if (bad) return deny(bad);
      const search = canonicalSearch(url.searchParams);
      return serve(env, ctx, {
        cacheKey: new Request(`${url.origin}${url.pathname}?${search}`),
        path: url.pathname,
        search,
      });
    }

    // ── Cached read-only RPCs (POST) ──
    const rpc = url.pathname.match(/^\/rest\/v1\/rpc\/([a-z_]+)$/);
    if (request.method === 'POST' && rpc && ALLOWED_RPCS.has(rpc[1])) {
      const raw = await request.text();
      if (raw.length > MAX_BODY) return deny('body too large');
      let body;
      try { body = canonicalBody(raw); } catch { return deny('malformed body'); }
      return serve(env, ctx, {
        // POST can't be a cache key — synthesise a GET key from name + body
        cacheKey: new Request(`${url.origin}/__rpc/${rpc[1]}?b=${encodeURIComponent(body)}`),
        path: url.pathname,
        search: '',
        init: { method: 'POST', body, headers: { 'Content-Type': 'application/json' } },
      });
    }

    return deny('endpoint not cacheable');
  },
};
