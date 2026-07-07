// natpc-cache — Cloudflare edge cache in front of Supabase catalog reads.
//
// The storefront polls two identical `cards` queries every 30s from every open
// browser; at drop time that's ~20 req/s of the same public data hitting
// Supabase. This worker serves those reads from the Cloudflare edge with a
// 30s TTL, so Supabase sees ~1 request per query shape per 30s per region.
//
// Uses the Cache API keyed on our own hostname: Supabase is itself behind
// Cloudflare, so `cf: {cacheTtl}` fetch options are ignored for its zone.
//
// Scope is deliberately narrow: GET-only, and only the fully-public catalog
// tables. Per-buyer data (buyers, reservations, offers, giveaway_entries) and
// all RPCs must stay direct-to-Supabase and uncached.

const ALLOWED_TABLES = new Set(['cards', 'settings', 'reviews', 'giveaways']);
const EDGE_TTL_SECONDS = 30;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'apikey, authorization, content-type',
};

function clientResponse(body, status, contentType, edgeStatus) {
  const headers = new Headers(CORS);
  headers.set('Content-Type', contentType);
  // Browsers must not cache — every poll should reach the edge for freshness
  headers.set('Cache-Control', 'no-store');
  headers.set('X-Edge-Cache', edgeStatus);
  return new Response(body, { status, headers });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    const url = new URL(request.url);
    const match = url.pathname.match(/^\/rest\/v1\/([a-z_]+)$/);
    if (request.method !== 'GET' || !match || !ALLOWED_TABLES.has(match[1])) {
      return new Response(JSON.stringify({ error: 'not allowed' }), {
        status: 403,
        headers: { 'Content-Type': 'application/json', ...CORS },
      });
    }

    const cache = caches.default;
    const cacheKey = new Request(url.origin + url.pathname + url.search);

    const hit = await cache.match(cacheKey);
    if (hit) {
      const contentType = hit.headers.get('Content-Type') || 'application/json';
      return clientResponse(hit.body, hit.status, contentType, 'HIT');
    }

    const res = await fetch(env.SUPABASE_URL + url.pathname + url.search, {
      headers: {
        apikey: env.SUPABASE_ANON_KEY,
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      },
    });
    const contentType = res.headers.get('Content-Type') || 'application/json';
    const body = await res.text();

    // Only cache successes — an upstream blip must not be pinned for 30s
    if (res.ok) {
      const toStore = new Response(body, {
        status: res.status,
        headers: {
          'Content-Type': contentType,
          'Cache-Control': `s-maxage=${EDGE_TTL_SECONDS}`,
        },
      });
      ctx.waitUntil(cache.put(cacheKey, toStore));
    }

    return clientResponse(body, res.status, contentType, res.ok ? 'MISS' : 'BYPASS');
  },
};
