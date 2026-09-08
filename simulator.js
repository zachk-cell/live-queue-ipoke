// Order simulator.
//
// Generates fake TikTok Shop orders so you can watch the whole system work
// (web dashboard + Discord + queue rules) before your real TikTok app is live.
// It deliberately produces: repeat buyers (to show slot-merging) and priority
// items (to show top-of-queue bumping).
//
// Enable with SIMULATE=true. Rate roughly matches a busy live (~1 order/min),
// but sped up here so you see movement immediately (default every ~2.5s).

const BUYERS = [
  'cardfiend', 'ripnship', 'mtg_maria', 'pokedad', 'slabqueen',
  'vintage_vince', 'gradedgoblin', 'holo_hunter', 'binderbabe', 'topdeck_tom',
  'chaserchloe', 'raredude', 'pullpalace', 'fatpackfrank', 'shinysam',
];

const NORMAL_ITEMS = [
  'Single: Charizard base set',
  'Single: Black Lotus (played)',
  'Pokemon 151 pack',
  'Lorcana booster',
  'Bulk common lot',
  'One Piece OP-05 pack',
  'Sports card mystery pack',
];

// These names contain the default priority trigger substrings ("break", "slab").
const PRIORITY_ITEMS = [
  'BREAK slot: PSA 10 break',
  'BREAK slot: Vintage box break',
  'Graded SLAB: PSA 9 Pikachu',
];

// iPoke demo uses real-ish "First L." names + a mix of Shopify/TikTok sources
// and event-spot purchases, so the perpetual queue, side-queues, source badges
// and combining can all be seen without a real Shopify connection.
const IPOKE_BUYERS = [
  'Alex R.', 'Bianca T.', 'Chris M.', 'Dana K.', 'Evan P.', 'Farrah L.',
  'Gabe W.', 'Hana S.', 'Iris N.', 'Jon D.', 'Kira V.', 'Leo B.',
];
const IPOKE_NORMAL = [
  'Pokemon 151 ETB', 'Prismatic Evolutions Booster Bundle', 'Surging Sparks Pack',
  'Vintage WOTC Single', 'Graded Slab PSA 9', 'Bulk Common Lot',
];
const SOURCES = ['TikTok', 'Web']; // engine maps to TT / SF

let seq = 1000;

function pick(arr) {
  // Deterministic-ish spread without Math.random (not available): rotate by seq.
  return arr[seq % arr.length];
}

export function startSimulator(queue, opts = {}) {
  const intervalMs = opts.intervalMs || 2500;
  if (opts.ipoke) return startIpokeSimulator(queue, opts);

  function makeOrder() {
    seq++;
    // Every ~4th order reuses a recent buyer -> demonstrates slot merging.
    const buyerIdx = seq % 4 === 0 ? (seq - 1) % BUYERS.length : seq % BUYERS.length;
    const buyer = BUYERS[buyerIdx];
    // Every ~5th order includes a priority item.
    const isPriority = seq % 5 === 0;
    const itemPool = isPriority ? PRIORITY_ITEMS : NORMAL_ITEMS;
    const name = itemPool[seq % itemPool.length];
    const qty = (seq % 3) + 1;
    const price = isPriority ? 45 + (seq % 40) : 5 + (seq % 30);
    return {
      id: `SIM-${seq}`,
      buyerId: `buyer-${buyerIdx}`,
      buyer,
      items: [{ name, qty }],
      total: price * qty,
      createdAt: Date.now(),
    };
  }

  const timer = setInterval(() => {
    queue.upsertOrder(makeOrder());
  }, intervalMs);
  timer.unref?.();

  console.log(`[sim] simulator running — new order every ${intervalMs}ms`);
  console.log('[sim] priority trigger words for the demo: "break", "slab"');
  return timer;
}

// iPoke DEMO simulator. This is only for demonstration — it is NOT meant to
// mimic a real 24/7 stream. It seeds two demo events (a Quack Pack + a WTA) if
// none exist, drops an initial batch of orders so the board looks populated,
// then trickles a couple of new orders on a slow timer a few times and STOPS.
// Nothing runs perpetually. Real orders come from Shopify once connected.
//   SIM_INITIAL_BATCH   orders seeded immediately        (default 12)
//   SIM_TRICKLE_PER     new orders added each round       (default 2)
//   SIM_TRICKLE_CYCLES  number of trickle rounds, then stop (default 5)
//   SIM_TRICKLE_MS      gap between rounds in ms          (default 60000)
function startIpokeSimulator(queue, opts = {}) {
  const initialBatch = Number.isFinite(Number(process.env.SIM_INITIAL_BATCH)) && process.env.SIM_INITIAL_BATCH
    ? Number(process.env.SIM_INITIAL_BATCH) : 12;
  const tricklePer = Number(process.env.SIM_TRICKLE_PER) || 2;
  const trickleCycles = Number(process.env.SIM_TRICKLE_CYCLES) || 5;
  const trickleMs = Number(process.env.SIM_TRICKLE_MS) || 60000;
  let quackId = null, wtaId = null;
  const existing = queue.events || [];
  const findKw = (kw) => existing.find((e) => (e.keywords || []).some((k) => k.includes(kw)));
  quackId = (findKw('quack') || {}).id || queue.addEvent({
    type: 'quack', title: 'Quack Pack #1', description: 'Highest value card takes the prize — keep all your cards!',
    totalSpots: 20, keywords: ['quack pack', 'quack #1'],
  }).id;
  wtaId = (findKw('wta') || {}).id || queue.addEvent({
    type: 'wta', title: 'WTA Break — Prismatic', description: 'Winner takes all.',
    totalSpots: 10, keywords: ['wta'],
  }).id;

  function makeOrder() {
    seq++;
    const buyerIdx = seq % 4 === 0 ? (seq - 1) % IPOKE_BUYERS.length : seq % IPOKE_BUYERS.length;
    const buyer = IPOKE_BUYERS[buyerIdx];
    const source = SOURCES[seq % SOURCES.length];
    const items = [];
    const roll = seq % 6;
    if (roll === 0) {
      items.push({ name: 'Quack Pack #1 Spot', qty: (seq % 3) + 1 }); // event spot ×1-3
    } else if (roll === 1) {
      items.push({ name: 'WTA Break — Prismatic Spot', qty: 1 });
    } else if (roll === 2) {
      // Mixed: a regular item AND an event spot on one order.
      items.push({ name: pick(IPOKE_NORMAL), qty: 1 });
      items.push({ name: 'Quack Pack #1 Spot', qty: 1 });
    } else if (roll === 3) {
      items.push({ name: 'Vintage WOTC Holo (vintage)', qty: 1 }); // priority
    } else {
      items.push({ name: pick(IPOKE_NORMAL), qty: (seq % 2) + 1 });
    }
    return {
      id: `SIM-${seq}`,
      buyerId: `buyer-${buyerIdx}`,
      buyer,
      items,
      total: 10 + (seq % 50),
      source,
      orderName: `#${2000 + seq}`,
      createdAt: Date.now(),
    };
  }

  // If the board already has orders (e.g. a redeploy restoring the demo from
  // disk), DON'T seed again — otherwise every deploy would stack another batch.
  // The demo only seeds onto a genuinely empty board (a fresh start / after a
  // wipe). Events are still ensured above so their definitions persist.
  const alreadyPopulated = typeof queue.activeQueue === 'function' && queue.activeQueue().length > 0;
  if (alreadyPopulated) {
    console.log('[sim] iPoke DEMO: board already populated — skipping seed (no new demo orders).');
    return null;
  }

  // 1) Seed an initial batch immediately so the board looks alive on load.
  for (let i = 0; i < initialBatch; i++) queue.upsertOrder(makeOrder());

  // 2) Trickle a couple of new orders every trickleMs, a fixed number of times,
  //    then stop entirely. No perpetual generation.
  let cyclesLeft = trickleCycles;
  const timer = setInterval(() => {
    if (cyclesLeft <= 0) { clearInterval(timer); console.log('[sim] demo trickle complete — simulator stopped'); return; }
    for (let i = 0; i < tricklePer; i++) queue.upsertOrder(makeOrder());
    cyclesLeft--;
  }, trickleMs);
  timer.unref?.();

  const total = initialBatch + tricklePer * trickleCycles;
  console.log(`[sim] iPoke DEMO: seeded ${initialBatch} orders, then +${tricklePer} every ${Math.round(trickleMs / 1000)}s × ${trickleCycles} rounds (~${total} total), then stops.`);
  console.log('[sim] demo events seeded: Quack Pack #1, WTA Break — Prismatic; priority word: "vintage"');
  return timer;
}
