// Shopify order source.
//
// A second order source that feeds the SAME queue engine as tiktok.js, so the
// dashboard/priority/Piggy-Bank/Discord/CSV logic doesn't care where an order
// came from. Used by the iPoke store, where BOTH the TikTok Shop orders and the
// native Shopify-web orders funnel into one Shopify store — so Shopify is the
// single complete source and we don't call TikTok's API at all.
//
// Auth: a Dev Dashboard app (Client ID + Client Secret) in the SAME Shopify
// organization as the store. Shopify retired the old admin-created custom apps
// with permanent shpat_ tokens, so we now exchange the client id/secret for a
// short-lived (24h) Admin API access token via the client-credentials grant,
// cache it, and refresh it before expiry. Scopes (read_orders + read_customers,
// plus Protected Customer Data access) are configured on the app in the Dev
// Dashboard. For back-compat, a legacy static SHOPIFY_ADMIN_TOKEN — if one is
// set — is still used as-is. We only ever read; we never write/fulfill (in-app
// "fulfilled" is just queue management, same as the TikTok side).
//
// Ingest by POLLING the GraphQL Admin API. For iPoke the queue is PERPETUAL, so
// unlike the TikTok poller we do NOT gate on a live window — orders flow in 24/7.

const SHOP = (process.env.SHOPIFY_SHOP || '').replace(/^https?:\/\//, '').replace(/\/+$/, '');
const ADMIN_TOKEN = process.env.SHOPIFY_ADMIN_TOKEN || ''; // legacy static token (optional)
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID || '';
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET || '';
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-01';
const POLL_MS = Number(process.env.SHOPIFY_POLL_MS) || 10000;
// Perpetual queues look back this far on boot so a redeploy gap doesn't drop
// orders placed while the app restarted. Ingest is idempotent, so re-fetching is
// safe. Default 6h when PERPETUAL; 0 otherwise.
const BOOT_LOOKBACK_MIN = Number.isFinite(Number(process.env.SHOPIFY_BOOT_LOOKBACK_MIN))
  ? Number(process.env.SHOPIFY_BOOT_LOOKBACK_MIN)
  : (String(process.env.PERPETUAL || '').toLowerCase() === 'true' ? 360 : 0);
const PERPETUAL = String(process.env.PERPETUAL || '').toLowerCase() === 'true';

let lastError = '';

export function shopifyEnabled() {
  return process.env.SHOPIFY_ENABLED === 'true' && !!SHOP
    && (!!ADMIN_TOKEN || (!!CLIENT_ID && !!CLIENT_SECRET));
}

const GQL_URL = () => `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

// ---------------- Access token (client-credentials grant) ----------------
// Cached short-lived token. A legacy static token never expires (exp = Infinity).
let _token = { value: ADMIN_TOKEN, exp: ADMIN_TOKEN ? Infinity : 0 };
async function getAccessToken(force = false) {
  if (ADMIN_TOKEN) return ADMIN_TOKEN; // legacy path: use the static token as-is
  const now = Date.now();
  // Reuse the cached token until ~2 min before it expires.
  if (!force && _token.value && now < _token.exp - 120000) return _token.value;
  if (!CLIENT_ID || !CLIENT_SECRET) throw new Error('shopify: no SHOPIFY_CLIENT_ID/SECRET (or SHOPIFY_ADMIN_TOKEN) set');
  const res = await fetch(`https://${SHOP}/admin/oauth/access_token`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
    }).toString(),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok || !body.access_token) {
    lastError = 'token exchange failed: ' + (body.error_description || body.error || ('HTTP ' + res.status));
    throw new Error(lastError);
  }
  _token = { value: body.access_token, exp: now + (Number(body.expires_in) || 86399) * 1000, scope: body.scope || '' };
  console.log('[shopify] access token refreshed (client credentials); valid ~', Math.round((_token.exp - now) / 60000), 'min; scopes:', _token.scope || '(none reported)');
  return _token.value;
}

// ---------------- GraphQL helper ----------------
async function gql(query, variables = {}) {
  // Transient network failures (DNS, connection reset, the generic "fetch
  // failed") get a couple of quick retries with a short backoff before giving
  // up, so a brief blip doesn't turn into a poll error or a missed order.
  // On a 401 (token expired/revoked) we force one token refresh and retry.
  let triedRefresh = false;
  for (let attempt = 0; ; attempt++) {
    let res;
    try {
      const token = await getAccessToken(triedRefresh);
      res = await fetch(GQL_URL(), {
        method: 'POST',
        headers: {
          'X-Shopify-Access-Token': token,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ query, variables }),
      });
    } catch (e) {
      if (attempt >= 2) throw e;
      await new Promise((r) => setTimeout(r, 400 * (attempt + 1)));
      continue;
    }
    if (res.status === 401 && !triedRefresh && !ADMIN_TOKEN) {
      triedRefresh = true; // token likely expired/revoked — refresh once and retry
      continue;
    }
    const body = await res.json().catch(() => ({}));
    if (body.errors) {
      lastError = 'gql: ' + JSON.stringify(body.errors).slice(0, 300);
      throw new Error(lastError);
    }
    lastError = '';
    return body.data;
  }
}

// ---------------- Normalization ----------------
// Shopify gives real customer names (no @handle). Per the store's choice we show
// the buyer publicly as "First L." — first name + last initial — so it's
// recognizable without exposing a full legal name. Full name is never surfaced.
// (The team also masks names at the Shopify customer level on request, which
// flows through here automatically; the in-app override is a further layer.)
function firstLast(customer, fallbackName) {
  const f = (customer && customer.firstName || '').trim();
  const l = (customer && customer.lastName || '').trim();
  if (f && l) return `${f} ${l[0].toUpperCase()}.`;
  if (f) return f;
  const dn = (customer && customer.displayName || '').trim();
  if (dn) return dn;
  return fallbackName || 'Guest';
}

// Map Shopify's origin channel to a label. Native web orders come through as
// "web"; TikTok orders synced into Shopify carry a channel/source that
// identifies TikTok. The engine collapses this to a TT/SF badge; we keep the
// raw-ish label here for the CSV. Confirm the exact TikTok string against a real
// order via debugRawOrder() before fully trusting it.
function channelLabel(sourceName) {
  const s = String(sourceName || '').toLowerCase();
  if (!s) return 'Shopify';
  if (s.includes('tiktok') || s.includes('tik_tok') || s.includes('tt')) return 'TikTok';
  if (s === 'web' || s.includes('online') || s.includes('storefront')) return 'Web';
  if (s.includes('pos')) return 'POS';
  if (s.includes('draft')) return 'Draft';
  return sourceName; // surface the raw value rather than guess wrong
}

function money(priceSet) {
  const amt = priceSet && priceSet.shopMoney && priceSet.shopMoney.amount;
  return Number(amt || 0);
}

function numericId(gid) {
  // gid://shopify/Order/1234567890 -> "1234567890"
  const s = String(gid || '');
  const m = s.match(/\/(\d+)(?:\?|$)/);
  return m ? m[1] : s;
}

export function normalizeShopifyOrder(o) {
  if (!o) return null;
  const lineItems = (o.lineItems && o.lineItems.nodes) || [];
  const items = lineItems.map((li) => {
    const title = (li.title || '').trim();
    let variant = (li.variantTitle || '').trim();
    // Shopify uses the literal "Default Title" for products that have no real
    // variants — treat that as no variant so it isn't folded into the name.
    if (variant.toLowerCase() === 'default title') variant = '';
    // Fold the variant into the item name (like the TikTok side) so priority +
    // Piggy-Bank + EVENT keyword matching, which key off the item name text,
    // see the variant.
    const name = (variant && !title.toLowerCase().includes(variant.toLowerCase()))
      ? `${title} - ${variant}`
      : (title || 'Item');
    return {
      name,
      sku: li.sku || '',
      variant: variant || '',
      qty: Number(li.quantity) || 1,
    };
  });
  const orderId = 'shop:' + numericId(o.id);
  const cust = o.customer || null;
  const buyerKey = cust && cust.id
    ? 'shop:' + numericId(cust.id)
    : (cust && cust.email ? 'shop:' + String(cust.email).toLowerCase() : orderId);
  return {
    id: orderId,
    buyerId: buyerKey,
    // Queue/public label = "First L." (full name never shown publicly).
    buyer: firstLast(cust, o.name),
    buyerHandle: '', // Shopify has no @handle.
    items,
    total: money(o.currentTotalPriceSet || o.totalPriceSet),
    createdAt: o.processedAt || o.createdAt ? Date.parse(o.processedAt || o.createdAt) : Date.now(),
    onHold: String(o.displayFulfillmentStatus || '').toUpperCase() === 'ON_HOLD',
    // Where the order originated, for the on-slot badge + CSV. Everything comes
    // via Shopify, but this preserves the TikTok-vs-web distinction (engine maps
    // to TT/SF).
    source: channelLabel(o.sourceName),
    // The human-facing order number (e.g. "#1001") — the packer's reference to
    // find it in Shopify admin. Kept alongside the namespaced id.
    orderName: o.name || '',
  };
}

// ---------------- Queries ----------------
const ORDER_FIELDS = `
  id
  name
  createdAt
  processedAt
  sourceName
  displayFinancialStatus
  displayFulfillmentStatus
  cancelledAt
  currentTotalPriceSet { shopMoney { amount } }
  totalPriceSet { shopMoney { amount } }
  customer { id firstName lastName displayName email }
  lineItems(first: 50) { nodes { title variantTitle quantity sku } }
`;

async function fetchOrdersMatching(searchQuery, limit = 50) {
  const data = await gql(
    `query($q: String!, $n: Int!) {
       orders(first: $n, query: $q, sortKey: CREATED_AT) {
         nodes { ${ORDER_FIELDS} }
       }
     }`,
    { q: searchQuery, n: limit },
  );
  return (data && data.orders && data.orders.nodes) || [];
}

// ISO string for Shopify search syntax, from a JS epoch (ms).
function iso(ms) { return new Date(ms).toISOString(); }

// ---------------- Poller ----------------
export function startShopifyPolling(queue) {
  // Perpetual: look back on boot to cover any redeploy gap. Non-perpetual: start
  // from now and re-anchor on go-live (matches the TikTok poller).
  let sinceMs = Date.now() - BOOT_LOOKBACK_MIN * 60 * 1000;
  // Within-window dedup, id -> order createdAt(ms). A perpetual (always-live)
  // queue never re-anchors on Go Live, so a plain Set would grow for the entire
  // life of the process (a slow memory creep over weeks of 24/7 uptime). We key
  // by timestamp and drop ids that fall well behind the advancing query window —
  // those orders can never be returned by the search again, so forgetting them
  // is safe and keeps this bounded to roughly the last few minutes of activity.
  const seen = new Map();
  const SEEN_GRACE_MS = 10 * 60 * 1000; // keep ids until 10 min behind the window

  // For non-perpetual stores, each Go Live starts a fresh window.
  queue.on('change', (e) => {
    if (!PERPETUAL && e && e.reason === 'go-live') { sinceMs = Date.now(); seen.clear(); }
  });

  let polling = false;
  async function poll() {
    // Perpetual queues ingest regardless of live; others only while live.
    if (!shopifyEnabled()) return;
    if (!PERPETUAL && !queue.live) return;
    // Overlap guard: if Shopify is slow and a cycle runs long, don't let the
    // interval stack a second poll on top of it (compounding API load).
    if (polling) { console.log('[shopify] poll: previous cycle still running — skipping this tick'); return; }
    polling = true;
    try {
      const sinceIso = iso(sinceMs - 30000); // 30s overlap for safety
      // 1) New paid orders that still need packing → into the queue.
      //    NOTE: we deliberately do NOT filter on `fulfillment_status:unfulfilled`
      //    here. Shopify's search treats ON_HOLD (and SCHEDULED) as distinct from
      //    "unfulfilled", so that filter silently drops orders that are placed on
      //    hold the moment they're created — which is exactly what happens to
      //    giveaway orders and to live orders that get an auto-hold before the next
      //    poll tick. Instead we pull every paid, non-cancelled order and skip only
      //    the ones that are already fully handled (FULFILLED/RESTOCKED) in code, so
      //    on-hold / unfulfilled / partial / scheduled orders all reach the queue
      //    (on-hold ones keep their hold flag via normalizeShopifyOrder).
      const fresh = await fetchOrdersMatching(
        `created_at:>='${sinceIso}' financial_status:paid -status:cancelled`,
        100,
      );
      for (const node of fresh) {
        const ful = String(node.displayFulfillmentStatus || '').toUpperCase();
        if (ful === 'FULFILLED' || ful === 'RESTOCKED') continue; // already done — not a packing task
        const norm = normalizeShopifyOrder(node);
        if (!norm || seen.has(norm.id)) continue;
        seen.set(norm.id, Date.parse(node.processedAt || node.createdAt) || Date.now());
        queue.upsertOrder(norm);
      }
      // Advance the watermark past the newest order we saw, so the window moves
      // forward and doesn't re-scan the same range forever.
      if (fresh.length) {
        const newest = Math.max(...fresh.map((n) => Date.parse(n.processedAt || n.createdAt) || 0));
        if (newest > sinceMs) sinceMs = newest;
      }
      // Bound the dedup map (perpetual queues run 24/7): forget ids whose orders
      // are now more than SEEN_GRACE_MS behind the query floor (sinceMs - 30s).
      // Those can't be returned by the search again, so this is safe and keeps
      // memory flat no matter how long the process stays up.
      if (seen.size > 1000) {
        const floor = sinceMs - 30000 - SEEN_GRACE_MS;
        for (const [id, ts] of seen) { if (ts < floor) seen.delete(id); }
      }
      // 2) Auto-remove orders cancelled/refunded on Shopify after they entered
      //    the queue. cancelOrder() is a no-op unless the order is queued.
      const cancelled = await fetchOrdersMatching(
        `updated_at:>='${sinceIso}' status:cancelled`,
      );
      for (const node of cancelled) {
        const id = 'shop:' + numericId(node.id);
        if (queue.cancelOrder(id)) console.log('[shopify] auto-cancelled order', node.name || id);
      }
      // 3) Refresh On-Hold status for orders still queued (a fulfillment hold
      //    placed after the order entered the queue).
      const queuedIds = queue.queuedOrderIds ? queue.queuedOrderIds() : [];
      const shopQueued = queuedIds.filter((id) => String(id).startsWith('shop:'));
      if (shopQueued.length) {
        const idClause = shopQueued.map((id) => `id:${numericId(id)}`).join(' OR ');
        const nodes = await fetchOrdersMatching(idClause, Math.min(shopQueued.length, 100)).catch(() => []);
        for (const node of nodes) {
          const id = 'shop:' + numericId(node.id);
          const held = String(node.displayFulfillmentStatus || '').toUpperCase() === 'ON_HOLD';
          if (queue.setHold(id, held)) {
            console.log('[shopify] order', node.name || id, held ? 'ON HOLD' : 'hold cleared');
          }
        }
      }
      // 4) Safety net: sweep any queued orders that match an open event into it.
      //    Event routing happens at ingest, but an event created/edited after an
      //    order landed would otherwise strand it in the main queue forever.
      if (queue.sweepQueuedIntoEvents) {
        const moved = queue.sweepQueuedIntoEvents();
        if (moved) console.log(`[shopify] swept ${moved} queued order(s) into events`);
      }
    } catch (e) {
      console.error('[shopify] poll error:', e.message);
    } finally {
      polling = false;
    }
  }

  const timer = setInterval(poll, POLL_MS);
  timer.unref?.();
  console.log(`[shopify] polling every ${POLL_MS}ms${PERPETUAL ? ' (perpetual, 24/7)' : ' while live'}; boot lookback ${BOOT_LOOKBACK_MIN}min`);

  // Periodic event auto-sync: pull newly-listed quacks/WTAs from the catalog
  // into events automatically. Default every 5 min; set EVENT_AUTOSYNC_MS=0 to
  // disable. Runs once shortly after boot too, so a restart catches up fast.
  const AUTO_SYNC_MS = Number.isFinite(Number(process.env.EVENT_AUTOSYNC_MS))
    ? Number(process.env.EVENT_AUTOSYNC_MS) : 300000;
  if (AUTO_SYNC_MS > 0) {
    const t2 = setInterval(() => { autoSyncEvents(queue); }, AUTO_SYNC_MS);
    t2.unref?.();
    setTimeout(() => { autoSyncEvents(queue); }, 20000);
    console.log(`[shopify] event auto-sync every ${Math.round(AUTO_SYNC_MS / 1000)}s`);
  }
}

// Force a fresh token and report exactly which scopes the app now holds, plus
// live read-probes so we can confirm read_products / read_locations actually
// work (not just that they're listed). Admin-only route consumes this. Read-only.
export async function shopifyScopeCheck() {
  if (ADMIN_TOKEN) {
    return { authMode: 'static-token', note: 'Legacy static token — scopes are fixed on the token itself, not re-fetchable here.', probes: await _scopeProbes() };
  }
  await getAccessToken(true); // force refresh so a just-released scope is reflected
  const granted = String(_token.scope || '').split(/[\s,]+/).filter(Boolean);
  const wanted = ['read_orders', 'read_customers', 'read_products', 'read_inventory', 'read_locations'];
  const has = {};
  for (const w of wanted) has[w] = granted.includes(w);
  return { authMode: 'client-credentials', grantedScopes: granted, has, probes: await _scopeProbes() };
}

async function _scopeProbes() {
  const out = {};
  try { await gql(`{ products(first: 1) { nodes { id } } }`); out.products = 'ok'; }
  catch (e) { out.products = 'FAIL: ' + String(e.message || e).slice(0, 140); }
  try { await gql(`{ locations(first: 1) { nodes { id name } } }`); out.locations = 'ok'; }
  catch (e) { out.locations = 'FAIL: ' + String(e.message || e).slice(0, 140); }
  try { await gql(`{ inventoryItems(first: 1) { nodes { id } } }`); out.inventory = 'ok'; }
  catch (e) { out.inventory = 'FAIL: ' + String(e.message || e).slice(0, 140); }
  return out;
}

// ---------------- Status + diagnostics ----------------
export function shopifyStatus() {
  const authMode = ADMIN_TOKEN ? 'static-token' : ((CLIENT_ID && CLIENT_SECRET) ? 'client-credentials' : 'none');
  return {
    enabled: shopifyEnabled(),
    connected: !!(SHOP && (ADMIN_TOKEN || (CLIENT_ID && CLIENT_SECRET))),
    authMode,
    // For client-credentials: whether we currently hold a live (unexpired) token.
    hasLiveToken: ADMIN_TOKEN ? true : !!(_token.value && Date.now() < _token.exp),
    shop: SHOP || '',
    apiVersion: API_VERSION,
    perpetual: PERPETUAL,
    lastError: lastError || '',
  };
}

// ---------------- Event sync (iPoke quacks / WTAs) ----------------
// Events are VARIANTS of a single product (e.g. "iPokeTCG Quack Pack Series").
// Each variant is one quack or WTA. We read that product's variants and their
// on_hand inventory, then turn each into a candidate event for the admin's
// "Sync from Shopify" review list. Requires read_products (+ read_inventory /
// read_locations for the on_hand seat default).
const EVENT_TITLE_MATCH = (process.env.EVENT_PRODUCT_TITLE_MATCH || 'quack pack series').toLowerCase();

// Mirror queue._canon so dedup here matches the engine's keyword matching:
// lowercase, fold apostrophes away, collapse whitespace.
function _canonKw(s) {
  return String(s == null ? '' : s)
    .toLowerCase()
    .replace(/[‘’ʼ`´]/g, "'")
    .replace(/'/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function _inferEventType(title) {
  return /\bwta\b|winner[\s-]?takes?[\s-]?all/i.test(String(title || '')) ? 'wta' : 'quack';
}

// Pull the event product(s) and enumerate their variants with on_hand totals.
async function fetchEventVariants() {
  const data = await gql(
    `query($q: String!) {
       products(first: 30, query: $q) {
         nodes {
           id title status
           variants(first: 100) {
             nodes {
               id title
               inventoryItem {
                 id tracked
                 inventoryLevels(first: 20) { nodes { quantities(names: ["on_hand"]) { name quantity } } }
               }
             }
           }
         }
       }
     }`,
    { q: `title:*${EVENT_TITLE_MATCH}*` },
  );
  const products = (data && data.products && data.products.nodes) || [];
  const out = [];
  for (const p of products) {
    // Guard the wildcard search and skip draft/archived products.
    if (!String(p.title || '').toLowerCase().includes(EVENT_TITLE_MATCH)) continue;
    if (p.status && p.status !== 'ACTIVE') continue;
    for (const v of ((p.variants && p.variants.nodes) || [])) {
      let onHand = null;
      const inv = v.inventoryItem;
      if (inv && inv.tracked) {
        let sum = 0, seen = false;
        for (const lvl of ((inv.inventoryLevels && inv.inventoryLevels.nodes) || [])) {
          for (const q of (lvl.quantities || [])) {
            if (q && q.name === 'on_hand') { sum += Number(q.quantity) || 0; seen = true; }
          }
        }
        if (seen) onHand = sum;
      }
      out.push({
        productId: numericId(p.id),
        productTitle: p.title,
        variantId: numericId(v.id),
        variantTitle: (v.title || '').trim(),
        onHand, // null when the variant isn't inventory-tracked → seats left blank
      });
    }
  }
  return out;
}

// Build the review list: every event-product variant that doesn't already have
// a matching event (deduped by Shopify variant id, or by canonicalized title/
// keyword so manually-created events aren't offered again).
export async function eventSyncCandidates(queue) {
  const variants = await fetchEventVariants();
  const existing = (queue.events || []);
  const bySrc = new Set(existing.map((e) => e.sourceVariantId).filter(Boolean).map(String));
  // Exact canonical titles of existing events, plus their keywords. A variant is
  // already covered if an existing event's keyword is a SUBSTRING of the variant
  // title — that's exactly how _matchEvent routes orders, so if orders for this
  // variant would already land in an existing event, we don't offer it again.
  const titleSet = new Set(existing.map((e) => _canonKw(e.title || '')).filter(Boolean));
  const kwList = [];
  for (const e of existing) {
    for (const k of (e.keywords || [])) { const c = _canonKw(k); if (c) kwList.push(c); }
  }
  const candidates = [];
  let alreadyLinked = 0;
  for (const v of variants) {
    if (!v.variantTitle) continue;
    const cv = _canonKw(v.variantTitle);
    const linked = bySrc.has(String(v.variantId)) || titleSet.has(cv) || kwList.some((k) => cv.includes(k));
    if (linked) { alreadyLinked++; continue; }
    candidates.push({
      variantId: v.variantId,
      productId: v.productId,
      productTitle: v.productTitle,
      title: v.variantTitle,
      type: _inferEventType(v.variantTitle),
      keyword: v.variantTitle,
      onHand: v.onHand,
    });
  }
  return { ok: true, scannedVariants: variants.length, alreadyLinked, candidates };
}

// Periodic auto-sync: create events for any new active event-variant without
// one, using on_hand as the seat count (untracked → blank). addEvent is
// idempotent on the Shopify variant id, so this never duplicates an event a
// human already added or a previous run created. Errors are swallowed (e.g. a
// transient API hiccup) so they never disturb the order poller.
let _lastAutoSyncWarn = '';
export async function autoSyncEvents(queue) {
  let added = 0;
  try {
    const { candidates } = await eventSyncCandidates(queue);
    for (const c of (candidates || [])) {
      const before = queue.events.length;
      queue.addEvent({
        type: c.type || 'quack',
        title: c.title,
        description: '',
        totalSpots: (c.onHand != null ? c.onHand : 0),
        keywords: [c.keyword],
        sourceVariantId: c.variantId,
        sourceProductId: c.productId,
      });
      if (queue.events.length > before) {
        added++;
        console.log(`[shopify] auto-synced new event: ${c.title} (seats ${c.onHand != null ? c.onHand : 'untracked'})`);
      }
    }
  } catch (e) {
    const msg = String(e && e.message || e);
    if (msg !== _lastAutoSyncWarn) { console.warn('[shopify] event auto-sync skipped:', msg); _lastAutoSyncWarn = msg; }
  }
  return added;
}

// Admin-only probe: pull a few recent orders and surface exactly what Shopify
// returns — how a TikTok-synced order looks vs a native web order (buyer, source
// channel, line items). Full names/emails are masked so the route never leaks PII.
export async function debugRawOrder() {
  const data = await gql(
    `query {
       orders(first: 5, sortKey: CREATED_AT, reverse: true) {
         nodes {
           ${ORDER_FIELDS}
         }
       }
     }`,
  );
  const nodes = (data && data.orders && data.orders.nodes) || [];
  const mask = (v) => {
    const s = String(v || '');
    return s ? s[0] + '***(' + s.length + ')' : s;
  };
  return {
    ok: true,
    count: nodes.length,
    samples: nodes.map((o) => ({
      orderName: o.name,
      sourceName: o.sourceName,               // <- confirms the TikTok channel string
      channelLabel: channelLabel(o.sourceName),
      financial: o.displayFinancialStatus,
      fulfillment: o.displayFulfillmentStatus,
      cancelledAt: o.cancelledAt,
      customer: o.customer ? {
        hasFirst: !!o.customer.firstName,
        hasLast: !!o.customer.lastName,
        firstNameMasked: mask(o.customer.firstName),
        lastInitial: o.customer.lastName ? o.customer.lastName[0].toUpperCase() + '.' : '',
        displayNameMasked: mask(o.customer.displayName),
      } : null,
      normalizedBuyer: normalizeShopifyOrder(o).buyer,
      itemCount: (o.lineItems && o.lineItems.nodes || []).length,
      sampleItem: normalizeShopifyOrder(o).items[0] || null,
    })),
  };
}
