// The Worker's request rules, run in Node with fetch and the Cache API stubbed:
//   node --test test/worker.test.mjs        (from natpc-cache/)
// Nothing here reaches Supabase.
import { test } from 'node:test';
import assert from 'node:assert/strict';

const store = new Map();
globalThis.caches = {
  default: {
    async match(req) { const r = store.get(req.url); return r ? r.clone() : undefined; },
    async put(req, res) { store.set(req.url, res.clone()); },
  },
};
let upstreamCalls = [];
globalThis.fetch = async (url, init = {}) => {
  upstreamCalls.push({ url: String(url), init });
  return new Response('[]', { status: 200, headers: { 'Content-Type': 'application/json' } });
};

const { default: worker } = await import('../src/index.js');
const env = { SUPABASE_URL: 'https://origin.test', SUPABASE_ANON_KEY: 'k' };
const ctx = { waitUntil() {} };
const get = (q, headers) => worker.fetch(new Request('https://api.test' + q, { headers }), env, ctx);
const reset = () => { store.clear(); upstreamCalls = []; };

test('cards accepts offset, and it is part of the cache key', async () => {
  reset();
  const a = await get('/rest/v1/cards?select=id&order=id&limit=200&offset=0');
  const b = await get('/rest/v1/cards?select=id&order=id&limit=200&offset=180');
  assert.equal(a.status, 200);
  assert.equal(b.status, 200);
  assert.equal(upstreamCalls.length, 2, 'two offsets are two cache entries');
  assert.match(upstreamCalls[1].url, /offset=180/);
  const again = await get('/rest/v1/cards?offset=180&limit=200&order=id&select=id');
  assert.equal(again.headers.get('X-Edge-Cache'), 'HIT');
  assert.equal(upstreamCalls.length, 2);
});

test('offset must be a bounded whole number', async () => {
  reset();
  for (const bad of ['-1', '1.5', 'abc', '10001', '999999', '']) {
    const r = await get(`/rest/v1/cards?select=id&limit=10&offset=${bad}`);
    assert.equal(r.status, 403, `offset=${bad}`);
  }
  assert.equal(upstreamCalls.length, 0);
  assert.equal((await get('/rest/v1/cards?select=id&limit=10&offset=10000')).status, 200);
});

test('offset is cards-only', async () => {
  reset();
  assert.equal((await get('/rest/v1/reviews?select=text&limit=5&offset=5')).status, 403);
});

test('a read with no limit gets 200, in the upstream call and the cache key', async () => {
  reset();
  await get('/rest/v1/cards?select=id&visibility=eq.listed');
  assert.match(upstreamCalls[0].url, /[?&]limit=200(&|$)/);
  const same = await get('/rest/v1/cards?select=id&visibility=eq.listed&limit=200');
  assert.equal(same.headers.get('X-Edge-Cache'), 'HIT', 'no-limit and limit=200 share one entry');
  assert.equal((await get('/rest/v1/cards?select=id&limit=201')).status, 403);
});

test('a key may appear once (the range checks read only the first)', async () => {
  reset();
  for (const q of ['select=id&limit=10&limit=500', 'select=id&limit=10&offset=0&offset=99999',
                   'select=id&select=*&limit=10', 'select=id&visibility=eq.listed&visibility=eq.hidden&limit=10']) {
    assert.equal((await get(`/rest/v1/cards?${q}`)).status, 403, q);
  }
  assert.equal(upstreamCalls.length, 0);
});

test('prototype names are not tables', async () => {
  reset();
  for (const t of ['constructor', '__proto__', 'tostring']) {
    const r = await get(`/rest/v1/${t}?select=id`);
    assert.equal(r.status, 403, t);
  }
  assert.equal(upstreamCalls.length, 0);
});

test('the limiter keys IPv6 on its /64 and leaves IPv4 alone', async () => {
  const keys = [];
  const envRl = { ...env, CODE_RL: { async limit({ key }) { keys.push(key); return { success: true }; } } };
  const post = ip => worker.fetch(new Request('https://api.test/rest/v1/rpc/request_email_code', {
    method: 'POST', body: '{"p_email":"x"}', headers: { 'cf-connecting-ip': ip },
  }), envRl, ctx);
  for (const ip of ['2001:db8:1:2::1', '2001:0db8:0001:0002:aaaa:bbbb:cccc:dddd', '2001:DB8:1:2:ffff::9',
                    '2001:db8::1', '::1', '203.0.113.7', '::ffff:203.0.113.7']) await post(ip);
  assert.deepEqual(keys, [
    'CODE_RL:2001:db8:1:2::/64', 'CODE_RL:2001:db8:1:2::/64', 'CODE_RL:2001:db8:1:2::/64',
    'CODE_RL:2001:db8:0:0::/64', 'CODE_RL:0:0:0:0::/64',
    'CODE_RL:203.0.113.7', 'CODE_RL:::ffff:203.0.113.7',
  ]);
  // the origin still gets the full address, as before
  assert.equal(upstreamCalls.at(-1).init.headers['x-navi-ip'], '::ffff:203.0.113.7');
});

test('a signed-in checkout skips the per-network limit; a guest Express start keeps it', async () => {
  reset();
  const keys = [];
  const envRl = { ...env, EXPRESS_RL: { async limit({ key }) { keys.push(key); return { success: false }; } } };
  const post = body => worker.fetch(new Request('https://api.test/functions/v1/stripe-checkout', {
    method: 'POST', body: JSON.stringify(body), headers: { 'cf-connecting-ip': '203.0.113.9' },
  }), envRl, ctx);
  // the account lane and its release reach the origin, unlimited
  let r = await post({ account: true, token: 't', order_group_id: 'co_1_abc', items: [{ card_id: 'x' }] });
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('X-Edge-RL'), 'bound:ip:account');
  r = await post({ action: 'release_checkout', order_group_id: 'co_1_abc', token: 't' });
  assert.equal(r.status, 200);
  assert.equal(keys.length, 0);
  assert.equal(upstreamCalls.length, 2);
  assert.equal(upstreamCalls[0].init.headers['x-navi-ip'], '203.0.113.9');
  // Express, its release, an account body with no token, and junk are all limited
  for (const body of [{ express: true, items: [] }, { action: 'release_express', order_group_id: 'xp_1' },
                      { action: 'release_checkout', order_group_id: 'xp_1' }, { account: true }, { account: 'true', token: 't' }]) {
    r = await post(body);
    assert.equal(r.status, 429, JSON.stringify(body));
  }
  r = await worker.fetch(new Request('https://api.test/functions/v1/stripe-checkout', {
    method: 'POST', body: 'not json', headers: { 'cf-connecting-ip': '203.0.113.9' } }), envRl, ctx);
  assert.equal(r.status, 429);
  assert.equal(keys.length, 6);
  assert.equal(upstreamCalls.length, 2);
});

test('a fronted body over the cap is refused before anything else', async () => {
  reset();
  const big = JSON.stringify({ account: true, token: 't', pad: 'x'.repeat(20000) });
  const r = await worker.fetch(new Request('https://api.test/functions/v1/stripe-checkout', {
    method: 'POST', body: big, headers: { 'cf-connecting-ip': '203.0.113.9' } }), env, ctx);
  assert.equal(r.status, 403);
  assert.equal(upstreamCalls.length, 0);
});

test('one-click unsubscribe: POST unsubscribes by the token; GET only goes to the page', async () => {
  reset();
  const tok = '0f8c2a6e-1b2c-4d3e-8f90-123456789abc';
  const post = await worker.fetch(new Request('https://api.test/unsubscribe?t=' + tok, {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'List-Unsubscribe=One-Click',
  }), env, ctx);
  assert.equal(post.status, 200);
  assert.equal(upstreamCalls.length, 1);
  assert.equal(upstreamCalls[0].url, 'https://origin.test/rest/v1/rpc/unsubscribe_buyer');
  assert.deepEqual(JSON.parse(upstreamCalls[0].init.body), { p_unsub: tok });
  reset();
  const get = await worker.fetch(new Request('https://api.test/unsubscribe?t=' + tok), env, ctx);
  assert.equal(get.status, 302);
  assert.equal(get.headers.get('Location'), 'https://navipcshop.com/?unsub=' + tok);
  assert.equal(upstreamCalls.length, 0, 'a GET never unsubscribes');
  const bad = await worker.fetch(new Request('https://api.test/unsubscribe?t=nope', { method: 'POST' }), env, ctx);
  assert.equal(bad.status, 400);
  assert.equal(upstreamCalls.length, 0);
});
