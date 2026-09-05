// TikTok Shop integration.
//
// - OAuth token management: exchange auth_code -> access/refresh tokens,
//   auto-refresh before/when they expire, persist to disk within a session.
// - Order ingest by POLLING: while the seller is live, poll for recently
//   created orders every few seconds and push them into the queue. Polling is
//   simpler and more reliable on ephemeral/free hosting than inbound webhooks,
//   and it naturally respects the "only while live" rule.
//
// Endpoints (TikTok Shop Open API):
//   Auth:   https://auth.tiktok-shops.com/api/v2/token/get   (grant_type=authorized_code)
//           https://auth.tiktok-shops.com/api/v2/token/refresh (grant_type=refresh_token)
//   API:    https://open-api.tiktokglobalshop.com  (signed requests)

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const DATA_DIR = path.resolve(process.cwd(), 'data');
const TOKEN_FILE = path.join(DATA_DIR, 'tiktok-tokens.json');

const AUTH_BASE = process.env.TIKTOK_AUTH_BASE || 'https://auth.tiktok-shops.com';
const API_BASE = process.env.TIKTOK_API_BASE || 'https://open-api.tiktokglobalshop.com';
const API_VERSION = process.env.TIKTOK_API_VERSION || '202309';
const APP_KEY = process.env.TIKTOK_APP_KEY || '';
const APP_SECRET = process.env.TIKTOK_APP_SECRET || '';

// Token state — seeded from env, overlaid by persisted file, updated at runtime.
let tokens = {
  accessToken: process.env.TIKTOK_ACCESS_TOKEN || '',
  refreshToken: process.env.TIKTOK_REFRESH_TOKEN || '',
  shopCipher: process.env.TIKTOK_SHOP_CIPHER || '',
  shopId: process.env.TIKTOK_SHOP_ID || '',
  sellerName: '',
  accessExpireAt: 0,
  lastError: '',
};

export function tiktokEnabled() {
  return process.env.TIKTOK_ENABLED === 'true' && !!APP_KEY && !!APP_SECRET;
}

function persist() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens));
  } catch (e) { console.warn('[tiktok] token persist failed:', e.message); }
}
(function loadPersisted() {
  try {
    if (fs.existsSync(TOKEN_FILE)) tokens = { ...tokens, ...JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8')) };
  } catch { /* ignore */ }
})();

// ---------------- Token endpoints ----------------
function applyTokenData(d) {
  if (!d) return;
  if (d.access_token) tokens.accessToken = d.access_token;
  if (d.refresh_token) tokens.refreshToken = d.refresh_token;
  // TikTok returns access_token_expire_in as an ABSOLUTE unix epoch (seconds),
  // not a duration. Guard for both: a value > 1e9 is an absolute epoch, a small
  // value is a duration in seconds.
  if (d.access_token_expire_in) {
    const v = Number(d.access_token_expire_in);
    const absMs = v > 1e9 ? v * 1000 : Date.now() + v * 1000;
    tokens.accessExpireAt = absMs - 60000; // refresh a minute early
  }
  if (d.seller_name) tokens.sellerName = d.seller_name;
  tokens.lastError = '';
  persist();
}

async function tokenGet(authCode) {
  const url = `${AUTH_BASE}/api/v2/token/get?app_key=${APP_KEY}&app_secret=${APP_SECRET}` +
    `&auth_code=${encodeURIComponent(authCode)}&grant_type=authorized_code`;
  const res = await fetch(url);
  const body = await res.json().catch(() => ({}));
  if (body.code !== 0) throw new Error('token/get: ' + (body.message || JSON.stringify(body)));
  applyTokenData(body.data);
  return body.data;
}

async function tokenRefresh() {
  if (!tokens.refreshToken) throw new Error('no refresh token');
  const url = `${AUTH_BASE}/api/v2/token/refresh?app_key=${APP_KEY}&app_secret=${APP_SECRET}` +
    `&refresh_token=${encodeURIComponent(tokens.refreshToken)}&grant_type=refresh_token`;
  const res = await fetch(url);
  const body = await res.json().catch(() => ({}));
  if (body.code !== 0) throw new Error('token/refresh: ' + (body.message || JSON.stringify(body)));
  applyTokenData(body.data);
  console.log('[tiktok] access token refreshed');
  return body.data;
}

// ---------------- Signed API calls ----------------
function sign(pathName, params, body = '') {
  const keys = Object.keys(params).filter((k) => k !== 'sign' && k !== 'access_token').sort();
  let base = pathName;
  for (const k of keys) base += k + params[k];
  base += body || '';
  base = APP_SECRET + base + APP_SECRET;
  return crypto.createHmac('sha256', APP_SECRET).update(base).digest('hex');
}

async function apiCall(method, pathName, extraParams = {}, jsonBody = null, retry = true, includeShopCipher = true) {
  const params = {
    app_key: APP_KEY,
    timestamp: String(Math.floor(Date.now() / 1000)),
    // The authorization/shops endpoint rejects shop_cipher; other endpoints need it.
    ...((includeShopCipher && tokens.shopCipher) ? { shop_cipher: tokens.shopCipher } : {}),
    ...extraParams,
  };
  const bodyStr = jsonBody ? JSON.stringify(jsonBody) : '';
  params.sign = sign(pathName, params, bodyStr);
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(`${API_BASE}${pathName}?${qs}`, {
    method,
    headers: { 'x-tts-access-token': tokens.accessToken, 'content-type': 'application/json' },
    ...(jsonBody ? { body: bodyStr } : {}),
  });
  const body = await res.json().catch(() => ({}));
  // Refresh + retry once on any token-related error.
  if (retry && tokens.refreshToken && body && body.code && /token|auth|expire/i.test(JSON.stringify(body))) {
    try { await tokenRefresh(); } catch (e) { tokens.lastError = e.message; persist(); return body; }
    return apiCall(method, pathName, extraParams, jsonBody, false, includeShopCipher);
  }
  return body;
}

async function getShopCipher() {
  const pathName = `/authorization/${API_VERSION}/shops`;
  const body = await apiCall('GET', pathName, {}, null, true, false);
  const shop = body?.data?.shops?.[0];
  if (shop && shop.cipher) {
    tokens.shopCipher = shop.cipher;
    tokens.shopId = shop.id;
    if (shop.name) tokens.sellerName = tokens.sellerName || shop.name;
    tokens.lastError = '';
    persist();
    console.log('[tiktok] shop cipher acquired for', tokens.sellerName || tokens.shopId);
  } else {
    // Record why so the panel / debug route can surface it.
    tokens.lastError = 'get-shops: ' + JSON.stringify(body).slice(0, 300);
    persist();
    console.warn('[tiktok] get-shops returned no usable cipher:', JSON.stringify(body).slice(0, 500));
  }
  return shop;
}

// Debug helper: return the raw shops-endpoint response (used by an admin route).
export async function debugShops() {
  const pathName = `/authorization/${API_VERSION}/shops`;
  const raw = await apiCall('GET', pathName, {}, null, true, false);
  return {
    tokenState: {
      hasAccess: !!tokens.accessToken,
      hasRefresh: !!tokens.refreshToken,
      hasCipher: !!tokens.shopCipher,
      accessExpireAt: tokens.accessExpireAt,
      sellerName: tokens.sellerName,
      shopId: tokens.shopId,
    },
    apiBase: API_BASE,
    apiVersion: API_VERSION,
    shopsResponse: raw,
  };
}

// Debug helper: force a fresh getShopCipher using current access token.
export async function refetchShopCipher() {
  await getShopCipher();
  return tiktokStatus();
}

// ---------------- Orders ----------------
// Pull the buyer's public TikTok handle/display name — NEVER the real shipping
// name (recipient_address.name), which is private and would leak on the public
// page. We check every field TikTok has historically used for the handle, then
// fall back to a masked buyer id if none is present.
function firstStr(...vals) {
  for (const c of vals) {
    if (typeof c === 'string' && c.trim()) return c.trim();
  }
  return '';
}
// The buyer's @handle (username), preferred for the public queue line.
function pickHandle(o) {
  return firstStr(
    o.buyer_username, o.username, o.user_name, o.buyer_user_name,
    o.buyer?.username, o.user_info?.username,
  );
}
// The buyer's display name / nickname — an admin-only cross-check that can
// differ from the @handle.
function pickDisplayName(o) {
  return firstStr(
    o.nickname, o.buyer_nickname, o.display_name, o.buyer_display_name,
    o.buyer?.nickname, o.buyer?.display_name,
    o.user_info?.nickname, o.user_info?.display_name,
  );
}
// Back-compat: any handle-ish field, either kind. Used as the public label when
// no explicit @handle is present.
function pickUsername(o) {
  return pickHandle(o) || pickDisplayName(o);
}

function maskedBuyerLabel(o) {
  const id = String(o.buyer_uid || o.user_id || o.id || '');
  const last4 = id.slice(-4);
  return last4 ? `Buyer ${last4}` : 'Buyer';
}

export function normalizeOrder(o) {
  const lineItems = o.line_items || o.item_list || [];
  const items = lineItems.map((li) => {
    const product = li.product_name || '';
    // TikTok records the buyer's chosen variation/sub-variant in a SEPARATE field
    // from the product title (e.g. product "Piggy Bank Booster Pack Bundle",
    // variant "Ascended Heroes"; or product "$299 Mystery Bag", variant "Rip
    // Live"). Capture it and fold it into the display name so it (a) shows in the
    // queue for the packer and (b) is available for priority + variant-counter
    // matching, which key off the item name/SKU text.
    const variant = li.sku_name || li.variation_name || li.variation ||
      (li.sku && (li.sku.name || li.sku.sku_name)) || '';
    const base = product || li.sku_name || 'Item';
    const name = (variant && !base.toLowerCase().includes(String(variant).toLowerCase()))
      ? `${base} - ${variant}`
      : base;
    return {
      name,
      sku: li.seller_sku || li.sku_id || '',
      variant: variant || '',
      qty: li.quantity || 1,
    };
  });
  const handle = pickHandle(o);
  const display = pickDisplayName(o);
  return {
    id: o.id || o.order_id,
    buyerId: o.buyer_uid || o.user_id || o.buyer_email || o.id,
    // Queue/public label = the buyer's DISPLAY NAME (what shows in TikTok live),
    // falling back to the @username, then a masked id so a real name never leaks.
    buyer: display || handle || maskedBuyerLabel(o),
    // Admin-only cross-check = the unique @username, shown in the expanded order
    // as a tiebreaker. Only set when TikTok gives one distinct from the display name.
    buyerHandle: (handle && handle !== display) ? handle : '',
    items,
    total: Number(o.payment?.total_amount || o.total_amount || 0),
    createdAt: o.create_time ? o.create_time * 1000 : Date.now(),
    // TikTok flips an order On Hold when there's something to check before
    // shipping — most often a buyer cancellation request on an unshipped order.
    onHold: !!o.is_on_hold_order,
  };
}

async function fetchOrderDetail(orderId) {
  const body = await apiCall('GET', `/order/${API_VERSION}/orders`, { ids: orderId });
  const order = body?.data?.orders?.[0];
  if (!order) { console.warn('[tiktok] order detail empty for', orderId, JSON.stringify(body).slice(0, 200)); return null; }
  return normalizeOrder(order);
}

// Search for order IDs created since `sinceEpoch` with a given order status.
async function searchOrderIds(sinceEpoch, orderStatus) {
  const pathName = `/order/${API_VERSION}/orders/search`;
  const ids = [];
  let pageToken = '';
  for (let page = 0; page < 5; page++) {
    const extra = { page_size: 50, ...(pageToken ? { page_token: pageToken } : {}) };
    const bodyReq = { create_time_ge: sinceEpoch, order_status: orderStatus };
    const body = await apiCall('POST', pathName, extra, bodyReq);
    const orders = body?.data?.orders || [];
    for (const o of orders) ids.push(o.id || o.order_id);
    pageToken = body?.data?.next_page_token || '';
    if (!pageToken) break;
  }
  return ids;
}

// Admin-only inspector: fetch one raw order and surface any handle-ish fields so
// we can confirm which key TikTok actually populates with the public username.
// Real names (recipient_address.name/phone/address) are redacted before return.
export async function debugRawOrder() {
  const statuses = ['AWAITING_SHIPMENT', 'AWAITING_COLLECTION', 'UNPAID', 'COMPLETED', 'CANCELLED'];
  const since = Math.floor(Date.now() / 1000) - 60 * 60 * 24 * 30; // last 30 days
  let sampleId = null;
  for (const st of statuses) {
    const ids = await searchOrderIds(since, st).catch(() => []);
    if (ids && ids.length) { sampleId = ids[0]; break; }
  }
  if (!sampleId) return { ok: false, note: 'No orders found in the last 30 days to inspect.' };

  const body = await apiCall('GET', `/order/${API_VERSION}/orders`, { ids: sampleId });
  const order = body?.data?.orders?.[0];
  if (!order) return { ok: false, note: 'Order detail came back empty.', raw: JSON.stringify(body).slice(0, 300) };

  // Redact obvious PII so this route never leaks a real name/address.
  const redactKeys = /name|phone|email|address|zip|postal|region|state|city|line|full_address/i;
  const mask = (v) => {
    if (v == null) return v;
    const s = String(v);
    return s ? s[0] + '***(' + s.length + ')' : s;
  };
  const handleish = {};
  const scan = (obj, prefix = '') => {
    if (!obj || typeof obj !== 'object') return;
    for (const [k, v] of Object.entries(obj)) {
      const path = prefix ? `${prefix}.${k}` : k;
      if (v && typeof v === 'object' && !Array.isArray(v)) { scan(v, path); continue; }
      if (/user|name|nick|handle|display|buyer|account/i.test(k)) {
        handleish[path] = redactKeys.test(k) ? mask(v) : v;
      }
    }
  };
  scan(order);

  return {
    ok: true,
    sampleOrderId: sampleId,
    topLevelKeys: Object.keys(order),
    handleishFields: handleish,
    normalizedBuyer: normalizeOrder(order).buyer,
  };
}

// Admin-only probe: can this shop's authorization read buyer cancellation
// requests? Tries the Search Cancellations API and also reports whether the
// order detail already exposes on-hold / cancellation-ish fields (which need no
// extra scope). Read-only — never approves/rejects anything.
export async function debugCancellations() {
  const out = { orderFieldSignal: null, cancellationsApi: null };

  // (a) What the order feed already gives us for free (no extra scope):
  try {
    const since = Math.floor(Date.now() / 1000) - 60 * 60 * 24 * 30;
    let sampleId = null;
    for (const st of ['AWAITING_SHIPMENT', 'UNPAID', 'ON_HOLD']) {
      const ids = await searchOrderIds(since, st).catch(() => []);
      if (ids && ids.length) { sampleId = ids[0]; break; }
    }
    if (sampleId) {
      const body = await apiCall('GET', `/order/${API_VERSION}/orders`, { ids: sampleId });
      const o = body?.data?.orders?.[0] || {};
      out.orderFieldSignal = {
        sampleOrderId: sampleId,
        is_on_hold_order: o.is_on_hold_order,
        cancel_order_sla_time: o.cancel_order_sla_time,
        cancellation_ish_keys: Object.keys(o).filter((k) => /cancel|hold/i.test(k)),
      };
    } else {
      out.orderFieldSignal = { note: 'No recent orders to inspect on-hold fields.' };
    }
  } catch (e) { out.orderFieldSignal = { error: e.message }; }

  // (b) The dedicated Search Cancellations API (may require the Return/Refund
  //     authorization scope). We return the raw code+message so we can tell
  //     whether it's accessible or needs re-authorization.
  try {
    const pathName = `/return_refund/${API_VERSION}/cancellations/search`;
    const since = Math.floor(Date.now() / 1000) - 60 * 60 * 24 * 30;
    const resp = await apiCall('POST', pathName, { page_size: 20 }, { create_time_ge: since });
    out.cancellationsApi = {
      pathTried: pathName,
      code: resp?.code,
      message: resp?.message,
      count: resp?.data?.total_count ?? (resp?.data?.cancellations || []).length,
      sampleStatuses: [...new Set((resp?.data?.cancellations || []).map((c) => c.cancel_status))].slice(0, 8),
    };
  } catch (e) { out.cancellationsApi = { error: e.message }; }

  return out;
}

// ---------------- Poller ----------------
export function startPolling(queue) {
  const interval = Number(process.env.TIKTOK_POLL_MS) || 10000;
  let sinceEpoch = Math.floor(Date.now() / 1000);
  const seen = new Set();

  // Each new live starts a fresh window: only orders placed after Go Live count.
  queue.on('change', (e) => {
    if (e && e.reason === 'go-live') { sinceEpoch = Math.floor(Date.now() / 1000); seen.clear(); }
  });

  async function poll() {
    if (!tiktokEnabled() || !queue.live || !tokens.accessToken || !tokens.shopCipher) return;
    try {
      // 1) Ingest new paid orders (awaiting shipment).
      const ids = await searchOrderIds(sinceEpoch - 30, 'AWAITING_SHIPMENT'); // small overlap for safety
      for (const id of ids) {
        if (!id || seen.has(id)) continue;
        const detail = await fetchOrderDetail(id);
        if (detail) { seen.add(id); queue.upsertOrder(detail); }
      }
      // 2) Auto-remove orders that were cancelled on TikTok after entering the
      //    queue. cancelOrder() is a no-op unless the order is currently queued.
      const cancelledIds = await searchOrderIds(sinceEpoch - 30, 'CANCELLED');
      for (const id of cancelledIds) {
        if (!id) continue;
        if (queue.cancelOrder(id)) console.log('[tiktok] auto-cancelled order', id);
      }
      // 3) Refresh On Hold status for orders still queued — a buyer cancellation
      //    request on an unshipped order flips it On Hold after it's already in
      //    the queue. One batched detail call (ids endpoint takes up to 50).
      const queuedIds = queue.queuedOrderIds ? queue.queuedOrderIds() : [];
      for (let i = 0; i < queuedIds.length; i += 50) {
        const chunk = queuedIds.slice(i, i + 50);
        const body = await apiCall('GET', `/order/${API_VERSION}/orders`, { ids: chunk.join(',') });
        for (const o of (body?.data?.orders || [])) {
          const id = String(o.id || o.order_id);
          if (queue.setHold(id, !!o.is_on_hold_order)) {
            console.log('[tiktok] order', id, o.is_on_hold_order ? 'ON HOLD' : 'hold cleared');
          }
        }
      }
    } catch (e) { console.error('[tiktok] poll error:', e.message); }
  }
  const timer = setInterval(poll, interval);
  timer.unref?.();
  console.log(`[tiktok] polling every ${interval}ms while live`);
}

// ---------------- Auth routes ----------------
export function mountAuth(app, adminPath) {
  // TikTok redirects here (Redirect URL) with an auth code after the seller approves.
  app.get('/auth/tiktok/callback', async (req, res) => {
    const code = req.query.code || req.query.auth_code;
    if (!code) return res.status(400).send('Missing authorization code.');
    try {
      await tokenGet(code);
      await getShopCipher();
      res.redirect((adminPath || '/') + '?tiktok=connected');
    } catch (e) {
      console.error('[tiktok] auth callback error:', e.message);
      res.status(500).send('TikTok authorization failed: ' + e.message + ' — you can close this and try again.');
    }
  });
}

export function tiktokStatus() {
  return {
    enabled: tiktokEnabled(),
    connected: !!(tokens.accessToken && tokens.shopCipher),
    shop: tokens.sellerName || tokens.shopId || '',
    lastError: tokens.lastError || '',
    authUrl: process.env.TIKTOK_AUTH_URL || '',
  };
}

// Admin-only: expose the current live tokens so they can be pinned into Render
// env vars (which survive free-tier spin-downs). After the seller authorizes,
// copy these three values into the service's environment and redeploy — on the
// next boot the app re-seeds from env and auto-refreshes, so no re-authorization
// is ever needed. Gated behind requireAuth in server.js (never public).
export function tiktokTokensForEnv() {
  return {
    connected: !!(tokens.accessToken && tokens.shopCipher),
    shop: tokens.sellerName || tokens.shopId || '',
    note: 'Copy the three values below into Render → Environment, then Save & redeploy. This keeps the shop connected across restarts.',
    env: {
      TIKTOK_ACCESS_TOKEN: tokens.accessToken || '',
      TIKTOK_REFRESH_TOKEN: tokens.refreshToken || '',
      TIKTOK_SHOP_CIPHER: tokens.shopCipher || '',
    },
  };
}

// On boot: if enabled and we have a refresh token but no/expired access token, refresh.
export async function tiktokBoot() {
  if (!tiktokEnabled()) { console.log('[tiktok] ingest disabled'); return; }
  try {
    if (tokens.refreshToken && (!tokens.accessToken || Date.now() > tokens.accessExpireAt)) {
      await tokenRefresh();
    }
    if (tokens.accessToken && !tokens.shopCipher) await getShopCipher();
  } catch (e) {
    tokens.lastError = e.message; persist();
    console.warn('[tiktok] boot token setup failed:', e.message);
  }
  console.log('[tiktok] status:', JSON.stringify(tiktokStatus()));
}
