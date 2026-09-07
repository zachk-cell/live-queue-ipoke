// iPoke queue-engine tests. Run: node test/queue.ipoke.test.mjs
//
// Covers the new behaviour (combine modes, perpetual ingest, events/side-queues,
// name overrides, prep state, manual combine, persistence) AND a regression
// check that the legacy 'always' combine behaviour is unchanged.
//
// Each scenario runs in its own temp data dir so state never leaks between tests.

import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import url from 'node:url';

const QUEUE_PATH = url.pathToFileURL(path.resolve(process.cwd(), 'queue.js')).href;

let passed = 0, failed = 0;
async function test(name, fn) {
  // Fresh, isolated cwd (queue writes to <cwd>/data). Module reads DATA_DIR at
  // import time, so chdir BEFORE the (cache-busted) dynamic import.
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ipoke-'));
  const prevCwd = process.cwd();
  process.chdir(dir);
  try {
    await fn(dir);
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (e) {
    console.error(`  ✗ ${name}\n     ${e.message}`);
    failed++;
  } finally {
    process.chdir(prevCwd);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

// Import a fresh QueueEngine with the given env applied (cache-busted so each
// construction re-runs the module top-level with the current cwd).
async function makeEngine(env = {}) {
  for (const k of ['PERPETUAL', 'COMBINE_MODE', 'COMBINE_WINDOW_MINUTES', 'PRIORITY_ITEMS_EXTRA', 'TRACKED_VARIANTS', 'SIMULATE']) delete process.env[k];
  Object.assign(process.env, env);
  const mod = await import(QUEUE_PATH + `?t=${Math.random()}`);
  return new mod.QueueEngine();
}

const MIN = 60 * 1000;
const ord = (id, buyerId, buyer, items, createdAt, extra = {}) =>
  ({ id, buyerId, buyer, items, total: 10, createdAt, ...extra });

// ── A. Legacy 'always' combine unchanged (regression) ──────────────────────
await test("legacy 'always': repeat orders from a buyer merge into one slot", async () => {
  const q = await makeEngine(); // no env → combineMode 'always', not perpetual
  q.goLive();
  const t = Date.now();
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'Pack A', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'Pack B', qty: 2 }], t + 5 * MIN));
  const aq = q.activeQueue();
  assert.equal(aq.length, 1, 'should be one merged slot');
  assert.equal(aq[0].orderCount, 2, 'slot has both orders');
  assert.equal(aq[0].itemCount, 3, 'items combined (1+2)');
});

await test("legacy: not live → orders ignored", async () => {
  const q = await makeEngine();
  const r = q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'Pack', qty: 1 }], Date.now()));
  assert.equal(r, null);
  assert.equal(q.activeQueue().length, 0);
});

await test("legacy: priority item jumps to front", async () => {
  const q = await makeEngine({ PRIORITY_ITEMS_EXTRA: 'vintage' });
  q.goLive();
  const t = Date.now();
  q.upsertOrder(ord('o1', 'A', 'Amy', [{ name: 'Normal Pack', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'C', 'Cal', [{ name: 'Vintage Lot', qty: 1 }], t + MIN));
  const aq = q.activeQueue();
  assert.equal(aq[0].buyer, 'Cal', 'vintage buyer is first');
  assert.ok(aq[0].isPriority);
});

// ── B. Combine 'off' (iPoke default) ───────────────────────────────────────
await test("combine 'off': each order is its own position", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const t = Date.now();
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'Pack A', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'Pack B', qty: 1 }], t + 20 * MIN));
  assert.equal(q.activeQueue().length, 2, 'two separate positions for same buyer');
});

// ── C. Combine 'time' anchored from first order ────────────────────────────
await test("combine 'time': anchored window (the spec's 4:00/4:20/4:40/4:50 case)", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'time', COMBINE_WINDOW_MINUTES: '30' });
  const t = Date.now(); // 4:00
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'P1', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'P2', qty: 1 }], t + 20 * MIN)); // 4:20 → merges
  assert.equal(q.activeQueue().length, 1, '4:20 merges into 4:00 slot');
  q.upsertOrder(ord('o3', 'B', 'Bob', [{ name: 'P3', qty: 1 }], t + 40 * MIN)); // 4:40 → new slot
  assert.equal(q.activeQueue().length, 2, '4:40 opens a new slot (>30 from 4:00)');
  q.upsertOrder(ord('o4', 'B', 'Bob', [{ name: 'P4', qty: 1 }], t + 50 * MIN)); // 4:50 → merges into 4:40
  const aq = q.activeQueue();
  assert.equal(aq.length, 2, '4:50 merges into the 4:40 slot (within 30 of its first)');
  const second = aq.find((s) => s.firstOrderAt === t + 40 * MIN);
  assert.equal(second.orderCount, 2, 'second slot holds o3 + o4');
});

// ── D. Perpetual ingest without goLive ─────────────────────────────────────
await test("perpetual: ingests without ever calling goLive", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  assert.ok(q.live, 'perpetual queue is live on boot');
  const r = q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'Pack', qty: 1 }], Date.now()));
  assert.ok(r, 'order ingested');
  assert.equal(q.activeQueue().length, 1);
});

// ── E. Events / side-queues ────────────────────────────────────────────────
await test("events: keyword routing, spots ×N, split orders, sold-out, no combine, rip", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const quack = q.addEvent({ type: 'quack', title: 'Quack #1', keywords: ['quack #1', 'quack1'], totalSpots: 5 });
  const wta = q.addEvent({ type: 'wta', title: 'WTA A', keywords: ['wta'], totalSpots: 3 });
  const t = Date.now();

  // Split order: 2 event spots + 1 regular item.
  q.upsertOrder(ord('o1', 'B', 'Bob', [
    { name: 'Quack #1 Spot', qty: 2 },
    { name: 'Booster Box', qty: 1 },
  ], t));
  assert.equal(q.activeQueue().length, 1, 'regular item still makes a main slot');
  assert.equal(q.activeQueue()[0].items[0].name, 'Booster Box', 'event item pulled out of main slot');
  let eq = q.eventQueue(quack.id);
  assert.equal(eq.spotsOrdered, 2, '2 spots ordered');
  assert.equal(eq.entries[0].spots, 2, 'shown as ×2, one entry');
  assert.equal(eq.entryCount, 1);

  // All-event order → no main slot.
  q.upsertOrder(ord('o2', 'C', 'Cal', [{ name: 'WTA slot', qty: 1 }], t + MIN));
  assert.equal(q.activeQueue().length, 1, 'all-event order adds no main slot');
  assert.equal(q.eventQueue(wta.id).spotsOrdered, 1);

  // Event entries never combine: same buyer orders again in same event.
  q.upsertOrder(ord('o3', 'B', 'Bob', [{ name: 'quack1 extra', qty: 3 }], t + 2 * MIN));
  eq = q.eventQueue(quack.id);
  assert.equal(eq.entryCount, 2, 'same buyer gets a separate event entry');
  assert.equal(eq.spotsOrdered, 5, '2 + 3 spots');
  assert.equal(eq.soldOut, true, 'quack now sold out (5/5)');

  // Rip the event: entries clear, status ripped, future matches not captured.
  q.ripEvent(quack.id);
  assert.equal(q.eventQueue(quack.id).entryCount, 0, 'entries cleared after rip');
  assert.equal(q.eventQueue(quack.id).status, 'ripped');
  q.upsertOrder(ord('o4', 'D', 'Dee', [{ name: 'Quack #1 leftover', qty: 1 }], t + 3 * MIN));
  assert.equal(q.activeQueue().length, 2, 'ripped event no longer captures; item hits main queue');
});

// ── F. Name override ───────────────────────────────────────────────────────
await test("name override: propagates to main + event views, then clears", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const ev = q.addEvent({ type: 'wta', keywords: ['wta'], totalSpots: 5 });
  const t = Date.now();
  q.upsertOrder(ord('o1', 'B', 'Bob Smith', [{ name: 'Pack', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob Smith', [{ name: 'WTA slot', qty: 1 }], t + MIN));
  q.setNameOverride('B', 'Anonymous');
  assert.equal(q.activeQueue()[0].buyer, 'Anonymous');
  assert.equal(q.activeQueue()[0].originalBuyer, 'Bob Smith');
  assert.ok(q.activeQueue()[0].nameOverridden);
  assert.equal(q.eventQueue(ev.id).entries[0].buyer, 'Anonymous', 'override reaches event view');
  q.clearNameOverride('B');
  assert.equal(q.activeQueue()[0].buyer, 'Bob Smith');
  assert.ok(!q.activeQueue()[0].nameOverridden);
});

// ── G. Prep state ──────────────────────────────────────────────────────────
await test("prep state: flag set, visible, cleared on fulfill", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'Pack', qty: 1 }], Date.now()));
  const key = q.activeQueue()[0].key;
  assert.ok(q.setPrepped(key, true));
  assert.equal(q.activeQueue()[0].prepped, true);
  q.markFulfilled(key);
  assert.ok(!q.preppedBatches.has(key), 'prepped flag cleared when slot leaves queue');
});

// ── H. Manual combine ──────────────────────────────────────────────────────
await test("manual combine: merges two same-buyer slots to earliest; refuses cross-buyer", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const t = Date.now();
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'P1', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'P2', qty: 1 }], t + 40 * MIN));
  q.upsertOrder(ord('o3', 'C', 'Cal', [{ name: 'P3', qty: 1 }], t + MIN));
  const bobKeys = q.activeQueue().filter((s) => s.buyerId === 'B').map((s) => s.key);
  assert.equal(bobKeys.length, 2);
  const res = q.combineSlots(bobKeys);
  assert.ok(res, 'combine succeeded');
  const bobSlots = q.activeQueue().filter((s) => s.buyerId === 'B');
  assert.equal(bobSlots.length, 1, 'Bob now has one slot');
  assert.equal(bobSlots[0].orderCount, 2);
  assert.equal(bobSlots[0].firstOrderAt, t, 'kept earliest position');
  // Cross-buyer refuse.
  const mixed = q.activeQueue().map((s) => s.key);
  assert.equal(q.combineSlots(mixed), null, 'refuses to combine across buyers');
});

// ── I. Persistence across restarts ─────────────────────────────────────────
await test("persistence: state survives a fresh engine on the same data dir", async () => {
  let q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'time', COMBINE_WINDOW_MINUTES: '25' });
  const ev = q.addEvent({ type: 'quack', title: 'Q', keywords: ['quack'], totalSpots: 10 });
  const t = Date.now();
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'Pack', qty: 1 }], t));
  q.upsertOrder(ord('e1', 'C', 'Cal', [{ name: 'quack spot', qty: 2 }], t)); // event-only
  const key = q.activeQueue()[0].key;
  q.setPrepped(key, true);
  q.setNameOverride('B', 'Hidden');

  // New engine, SAME data dir (no env combine override so we prove disk wins).
  q = await makeEngine({ PERPETUAL: 'true' });
  assert.equal(q.combineMode, 'time', 'combine mode restored from disk (env did not clobber)');
  assert.equal(q.combineWindowMs, 25 * MIN, 'window restored');
  assert.equal(q.activeQueue().length, 1, 'order restored');
  assert.equal(q.activeQueue()[0].buyer, 'Hidden', 'name override restored');
  assert.equal(q.activeQueue()[0].prepped, true, 'prep flag restored');
  assert.equal(q.eventQueue(ev.id).spotsOrdered, 2, 'event entries restored');
  // Re-ingest the same event-only order id → no duplicate (seen set restored).
  q.upsertOrder(ord('e1', 'C', 'Cal', [{ name: 'quack spot', qty: 2 }], t));
  assert.equal(q.eventQueue(ev.id).spotsOrdered, 2, 'no duplicate event entry after restart');
});

// ── J. Combine mode change is not retroactive ──────────────────────────────
await test("combine change is not retroactive; applies to future orders only", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const t = Date.now();
  q.upsertOrder(ord('o1', 'B', 'Bob', [{ name: 'P1', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'P2', qty: 1 }], t + MIN));
  assert.equal(q.activeQueue().length, 2, 'two separate slots while off');
  q.setCombineMode('always');
  assert.equal(q.activeQueue().length, 2, 'existing slots NOT retroactively merged');
  q.upsertOrder(ord('o3', 'B', 'Bob', [{ name: 'P3', qty: 1 }], t + 2 * MIN));
  // With 'always', o3 merges into Bob's open slot — but which one? openBatch was
  // empty (built under 'off'), so o3 opens/uses a single open slot going forward.
  // The key assertion: no crash and the new order lands as a queued slot.
  assert.ok(q.activeQueue().some((s) => s.orderIds.includes('o3')), 'new order queued under new mode');
});

// ── K. Combine 'untiltop' (iPoke default): combine before top, lock after ──
await test("combine 'untiltop': merges before the buyer hits the top, locks after", async () => {
  const q = await makeEngine({ PERPETUAL: 'true' }); // default mode is now 'untiltop'
  assert.equal(q.combineMode, 'untiltop', 'perpetual defaults to untiltop');
  const t = Date.now();
  // A is first → sits at the top. B is behind A (not at top).
  q.upsertOrder(ord('a1', 'A', 'Amy', [{ name: 'P', qty: 1 }], t));
  q.upsertOrder(ord('b1', 'B', 'Bob', [{ name: 'P1', qty: 1 }], t + 1 * MIN));
  // B (position 2, not top) orders again → should COMBINE.
  q.upsertOrder(ord('b2', 'B', 'Bob', [{ name: 'P2', qty: 1 }], t + 2 * MIN));
  let bob = q.activeQueue().filter((s) => s.buyerId === 'B');
  assert.equal(bob.length, 1, 'B still one slot while behind (combined)');
  assert.equal(bob[0].orderCount, 2, 'both B orders merged');
  // Fulfill A → B moves to the top and gets stamped reachedTop.
  const aKey = q.activeQueue().find((s) => s.buyerId === 'A').key;
  q.markFulfilled(aKey);
  assert.equal(q.activeQueue()[0].buyerId, 'B', 'B now at top');
  // B orders AGAIN while at the top → must NOT combine; own new slot + flag.
  q.upsertOrder(ord('b3', 'B', 'Bob', [{ name: 'P3', qty: 1 }], t + 3 * MIN));
  bob = q.activeQueue().filter((s) => s.buyerId === 'B');
  assert.equal(bob.length, 2, 'B now has a second, separate slot (no merge at top)');
  const topSlot = bob.find((s) => s.orderIds.includes('b1'));
  const followSlot = bob.find((s) => s.orderIds.includes('b3'));
  assert.equal(topSlot.followupCount, 1, 'top slot flags one follow-up order');
  assert.equal(followSlot.afterTop, true, 'the new slot is marked as an after-top follow-up');
  assert.equal(followSlot.orderCount, 1, 'follow-up slot holds only the new order');
});

// ── L. Event spot cap: a sold-out event stops capturing (no oversell) ──
await test("events: sold-out event stops capturing spots (respects the limit)", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const ev = q.addEvent({ type: 'quack', title: 'Cap', keywords: ['grail'], totalSpots: 2 });
  const t = Date.now();
  q.upsertOrder(ord('o1', 'A', 'Amy', [{ name: 'Grail Spot', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'Grail Spot', qty: 1 }], t + 1));
  assert.equal(q.eventQueue(ev.id).spotsOrdered, 2, '2/2 filled');
  assert.equal(q.eventQueue(ev.id).soldOut, true, 'sold out');
  // A 3rd matching order must NOT enter the event; it overflows to the main queue.
  q.upsertOrder(ord('o3', 'C', 'Cal', [{ name: 'Grail Spot', qty: 1 }], t + 2));
  assert.equal(q.eventQueue(ev.id).spotsOrdered, 2, 'still 2 — cap enforced, no oversell');
  assert.ok(q.activeQueue().some((s) => s.buyerId === 'C'), 'overflow order landed in the main queue');
});

// ── M. Overlay settings: per-overlay scale/opacity/panel, clamped + persisted ─
await test("overlays: per-overlay scale/opacity/panel are set, clamped, and persisted", async () => {
  let q = await makeEngine({ PERPETUAL: 'true' });
  // Defaults: both overlays exist, transparent, scale 1, panel on.
  assert.ok(q.overlays.queue && q.overlays.vault, 'both overlays present');
  assert.equal(q.overlays.queue.opacity, 0);
  assert.equal(q.overlays.queue.scale, 1);
  assert.equal(q.overlays.queue.panel, true);

  // Set queue: opacity 0.4, scale 1.5, panel off.
  assert.ok(q.setOverlaySetting('queue', { opacity: 0.4, scale: 1.5, panel: false }));
  assert.equal(q.overlays.queue.opacity, 0.4);
  assert.equal(q.overlays.queue.scale, 1.5);
  assert.equal(q.overlays.queue.panel, false);

  // Clamping: opacity>1 → 1, scale>3 → 3, scale<0.5 → 0.5.
  q.setOverlaySetting('vault', { opacity: 5, scale: 9 });
  assert.equal(q.overlays.vault.opacity, 1, 'opacity clamped to 1');
  assert.equal(q.overlays.vault.scale, 3, 'scale clamped to 3');
  q.setOverlaySetting('vault', { scale: 0.1 });
  assert.equal(q.overlays.vault.scale, 0.5, 'scale clamped to 0.5');

  // Unknown overlay key → no-op false.
  assert.equal(q.setOverlaySetting('bogus', { opacity: 0.5 }), false, 'unknown overlay rejected');

  // Vault opacity unchanged by queue changes (independent).
  assert.equal(q.overlays.queue.opacity, 0.4, 'queue opacity unaffected by vault edits');

  // Persists across a restart on the same data dir.
  q = await makeEngine({ PERPETUAL: 'true' });
  assert.equal(q.overlays.queue.opacity, 0.4, 'queue opacity restored from disk');
  assert.equal(q.overlays.queue.scale, 1.5, 'queue scale restored');
  assert.equal(q.overlays.queue.panel, false, 'queue panel restored');
  assert.equal(q.overlays.vault.scale, 0.5, 'vault scale restored');

  // Snapshot exposes overlays for the overlay/panel views.
  assert.ok(q.snapshot().overlays.vault, 'snapshot exposes overlays');
});

// ── N. Daily rollover: archives fulfilled+cancelled, keeps queued, resets ─────
await test("daily rollover: archives the day's fulfilled+cancelled, keeps queued orders", async () => {
  const q = await makeEngine({ PERPETUAL: 'true', COMBINE_MODE: 'off' });
  const t = Date.now();
  // First tick seeds lastRolloverDay without archiving.
  assert.equal(q.maybeDailyRollover(), false, 'first tick just seeds the day');
  assert.equal(q.history.length, 0, 'nothing archived yet');

  // Two orders: fulfill one, leave one queued.
  q.upsertOrder(ord('o1', 'A', 'Amy', [{ name: 'Pack', qty: 1 }], t));
  q.upsertOrder(ord('o2', 'B', 'Bob', [{ name: 'Pack', qty: 1 }], t + 1));
  const amyKey = q.activeQueue().find((s) => s.buyerId === 'A').key;
  q.markFulfilled(amyKey);
  assert.equal(q.activeQueue().length, 1, 'one still queued (Bob)');

  // Force a day change and roll over.
  q.lastRolloverDay = 'past-day';
  assert.equal(q.maybeDailyRollover(), true, 'rolled over on day change');

  // Fulfilled order archived to Past Days; queued order remains.
  assert.equal(q.history.length, 1, 'one day archived');
  assert.equal(q.history[0].count, 1, 'archived day has the 1 fulfilled order');
  assert.ok(q.history[0].label, 'archived day has a pretty label');
  assert.ok(Array.isArray(q.history[0].fulfilled) && q.history[0].fulfilled.length === 1, 'fulfilled records retained for CSV export');
  assert.equal(q.activeQueue().length, 1, 'unfulfilled order still on the live queue');
  assert.equal(q.activeQueue()[0].buyerId, 'B', 'Bob is still queued after rollover');

  // A same-day second call does nothing further.
  assert.equal(q.maybeDailyRollover(), false, 'no double rollover on the same day');
  assert.equal(q.history.length, 1);

  // Archived day survives a restart.
  const q2 = await makeEngine({ PERPETUAL: 'true' });
  assert.equal(q2.history.length, 1, 'Past Days entry restored from disk');
  assert.equal(q2.activeQueue().length, 1, 'queued order restored');
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
