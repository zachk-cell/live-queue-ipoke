// Shopify order source.
//
// A second order source that feeds the SAME queue engine as tiktok.js, so the
// dashboard/priority/Piggy-Bank/Discord/CSV logic doesn't care where an order
// came from. Used by the iPoke store, where BOTH the TikTok Shop orders and the
// native Shopify-web orders funnel into one Shopify store — so Shopify is the
// single complete source and we don't call TikTok's API at all.
//
// Auth: an admin-created CUSTOM APP on the store, with an Admin API access token
// (read_orders + read_customers) and "Protected Customer Data" access enabled.
// We only ever read; we never write/fulfill (in-app "fulfilled" is just queue
// management, same as the TikTok side).
//
// Ingest by POLLING the GraphQL Admin API. For iPoke the queue is PERPETUAL, so
// unlike the TikTok poller we do NOT gate on a live window — orders flow in 24/7.

const SHOP = (process.env.SHOPIFY_SHOP || '').replace(/^https?:\/\//, '').replace(/\/+$/, '');
const ADMIN_TOKEN = process.env.SHOPIFY_ADMIN_TOKEN || '';
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
  return process.env.SHOPIFY_ENABLED === 'true' && !!SHOP && !!ADMIN_TOKEN;
}

const GQL_URL = () => `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

// ---------------- GraphQL helper ----------------
async function gql(query, variables = {}) {
  const res = await fetch(GQL_URL(), {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': ADMIN_TOKEN,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  });
  const body = await res.json().catch(() => ({}));
  if (body.errors) {
    lastError = 'gql: ' + JSON.stringify(body.errors).slice(0, 300);
    throw new Error(lastError);
  }
  lastError = '';
  return body.data;
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
  const seen = new Set();

  // For non-perpetual stores, each Go Live starts a fresh window.
  queue.on('change', (e) => {
    if (!PERPETUAL && e && e.reason === 'go-live') { sinceMs = Date.now(); seen.clear(); }
  });

  async function poll() {
    // Perpetual queues ingest regardless of live; others only while live.
    if (!shopifyEnabled()) return;
    if (!PERPETUAL && !queue.live) return;
    try {
      const sinceIso = iso(sinceMs - 30000); // 30s overlap for safety
      // 1) New paid, unfulfilled orders → into the queue.
      const fresh = await fetchOrdersMatching(
        `created_at:>='${sinceIso}' financial_status:paid fulfillment_status:unfulfilled -status:cancelled`,
      );
      for (const node of fresh) {
        const norm = normalizeShopifyOrder(node);
        if (!norm || seen.has(norm.id)) continue;
        seen.add(norm.id);
        queue.upsertOrder(norm);
      }
      // Advance the watermark past the newest order we saw, so the window moves
      // forward and doesn't re-scan the same range forever.
      if (fresh.length) {
        const newest = Math.max(...fresh.map((n) => Date.parse(n.processedAt || n.createdAt) || 0));
        if (newest > sinceMs) sinceMs = newest;
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
    } catch (e) {
      console.error('[shopify] poll error:', e.message);
    }
  }

  const timer = setInterval(poll, POLL_MS);
  timer.unref?.();
  console.log(`[shopify] polling every ${POLL_MS}ms${PERPETUAL ? ' (perpetual, 24/7)' : ' while live'}; boot lookback ${BOOT_LOOKBACK_MIN}min`);
}

// ---------------- Status + diagnostics ----------------
export function shopifyStatus() {
  return {
    enabled: shopifyEnabled(),
    connected: !!(SHOP && ADMIN_TOKEN),
    shop: SHOP || '',
    apiVersion: API_VERSION,
    perpetual: PERPETUAL,
    lastError: lastError || '',
  };
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
