// Core queue engine.
//
// Behaviour (per seller's rules):
//  - Orders are grouped into one SLOT per buyer while that buyer still has an
//    unfulfilled slot. A buyer who orders again before being fulfilled is
//    merged into their existing slot (items + total combined), NOT re-queued.
//    Once a slot is fulfilled, a later order from that buyer opens a fresh slot.
//  - PRIORITY is item-driven: if a slot contains any configured "priority item",
//    the whole slot jumps to the top. Multiple priority slots from different
//    buyers all sit at the top, ordered by who ordered (the priority item) first.
//  - A slot can also be manually bumped to the very top.
//  - "Mark fulfilled" closes a slot (the whole buyer batch).
//  - State persists to disk so a crash/restart mid-live never loses the queue.

import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';

const DATA_DIR = path.resolve(process.cwd(), 'data');
const STATE_FILE = path.join(DATA_DIR, 'queue-state.json');
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const HISTORY_FILE = path.join(DATA_DIR, 'history.json');
const EVENTS_FILE = path.join(DATA_DIR, 'events-state.json'); // iPoke: event entries (side-queues)
const SEALED_FILE = path.join(DATA_DIR, 'sealed-state.json'); // iPoke: Ship-Sealed side-queue (internal pack/ship)
const PAST_EVENTS_FILE = path.join(DATA_DIR, 'past-events.json'); // iPoke: archived (finished) events
const PAST_EVENTS_MAX = 200; // keep the most recent N archived events
const VAULT_LEDGER_FILE = path.join(DATA_DIR, 'vault-ledger.json'); // iPoke: durable per-order vault audit ledger
const VAULT_LEDGER_MAX = 5000; // keep the most recent N vault orders (bounded, survives rollover)
const PRIORITY_DOLLARS = 1000; // an order (or combined batch) at/above this $ skips the queue
const AGED_QUEUE_MS = 14 * 60 * 60 * 1000; // after this long in queue, a slot ranks ABOVE incoming priority (fairness for long waiters)
const GIVEAWAY_SORT_TS = 8640000000000000; // sentinel createdAt so the giveaway seat always sorts LAST
// House seats (Nikki + Greg) ride just BEFORE the giveaway on (Omega) quacks —
// always the last two before it. Real/manual seats use normal (much smaller)
// timestamps, so these two sentinels pin the pair between the roster and the
// giveaway. The smaller value sorts first; which name gets it alternates per event.
const HOUSE_SEAT_SORT_FIRST = GIVEAWAY_SORT_TS - 2;
const HOUSE_SEAT_SORT_SECOND = GIVEAWAY_SORT_TS - 1;
const HOUSE_SEAT_NAMES = ['Nikki', 'Greg'];
const MAX_HISTORY = 30; // keep the last N archived days (Past Days) / streams

// Order-combining modes (how a buyer's repeat orders are grouped into one slot):
//   'always'   — legacy PBCC/Poke Pig behaviour: every repeat order from an
//                unfulfilled buyer merges into that buyer's open slot.
//   'untiltop' — iPoke default: repeat orders combine into the buyer's slot
//                ONLY while that slot hasn't reached the top of the queue yet.
//                Once the slot has been at the top (bag likely being packed), a
//                new order does NOT merge — it takes its own new position, and
//                the top slot flashes a "they ordered again" indicator.
//   'off'      — every order gets its OWN slot/position, even from the same buyer.
//   'time'     — repeat orders combine ONLY if they arrive within a configured
//                window measured from the slot's FIRST order (anchored).
const COMBINE_MODES = new Set(['always', 'untiltop', 'off', 'time']);
const DEFAULT_COMBINE_WINDOW_MS = 30 * 60 * 1000; // 30 min ('time' mode only)
// A vault order fulfilled WITHOUT ever reaching the top still counts toward the
// vault, but only after it has stayed fulfilled this long — a grace window so a
// mistaken fulfill that's undone quickly never inflates the count. Env override:
// VAULT_FULFILL_GRACE_MIN (minutes). Default 10.
const VAULT_FULFILL_GRACE_MS = (() => {
  const m = Number(process.env.VAULT_FULFILL_GRACE_MIN);
  return (Number.isFinite(m) && m >= 0 ? m : 10) * 60 * 1000;
})();

/**
 * Order record:
 * {
 *   id, buyerId, buyer,
 *   items: [{ name, qty }], total, createdAt,
 *   status: 'queued'|'fulfilled',
 *   batchKey,            // slot this order belongs to
 *   hasPriority          // does this order contain a priority item
 * }
 */

export class QueueEngine extends EventEmitter {
  constructor() {
    super();
    this.orders = new Map(); // orderId -> order
    this.openBatch = new Map(); // buyerId -> current open batchKey (or absent)
    this.batchCounter = 0;
    this.priorityItems = []; // array of lowercased substrings that trigger priority
    // Configurable priority settings (editable from the admin "Priority Settings"
    // lightbox). The money threshold and each metric can be toggled on/off.
    this.priorityMoneyThreshold = PRIORITY_DOLLARS; // $ at/above which an order jumps the queue
    this.priorityKeywordsOn = true;  // keyword metric active?
    this.priorityMoneyOn = true;     // $-threshold metric active?
    this.trackedVariants = []; // [{ id, label, product, variant }] — per-variant sales counters
    this.variantCounts = {}; // { [variantId]: number } — units counted when a slot hits the top
    this.variantLog = []; // audit trail of manual counter edits/resets (most recent first)
    this.vaultTallyLog = []; // audit trail of AUTOMATIC vault adds: { ts, order (last 4), vault, amount }
    // Durable per-order vault audit LEDGER, keyed by order id. Observational only
    // (never changes counting): records every rip-live order that matches a tracked
    // vault — its items, the vaults + expected amounts it should contribute, and
    // whether/when/how (top vs grace) it was actually counted. Persists in its own
    // file so it survives fulfillment, the nightly rollover and restarts, making the
    // vault counter fully reconcilable (and manual corrections unnecessary).
    this.vaultLedger = {}; // { [orderId]: { orderId, orderName, buyer, source, items, vaults, expected, counted, countedAt, path, status, firstSeenAt, lastSeenAt } }
    this.live = false; // when false, incoming orders are ignored (not queued)
    this.sessionStartedAt = null; // when the current live started
    this.history = []; // archived past streams (most recent first)

    // ── iPoke additions (all inert for stores that don't configure them) ──
    // Perpetual queue: never turns off; offline orders still ingest 24/7.
    this.perpetual = false;
    // Order-combining behaviour (see COMBINE_MODES above).
    this.combineMode = 'always';
    this.combineWindowMs = DEFAULT_COMBINE_WINDOW_MS;
    // Free-text buyer name overrides: { [buyerId]: 'display name' }. Persist
    // until explicitly removed; propagate to every surface.
    this.nameOverrides = {};
    // Prep state: batchKeys marked "prepped / ready to fulfill" (visual flag,
    // does not reorder the queue).
    this.preppedBatches = new Set();
    // Events / side-queues. `events` = definitions; `eventEntries` = per-order
    // spots routed out of the main queue by keyword.
    this.events = []; // [{ id, type:'quack'|'wta', title, description, totalSpots, keywords:[], status:'open'|'ripped', createdAt }]
    this.eventEntries = new Map(); // entryId -> entry
    // Ship-Sealed side-queue (iPoke): orders whose item names contain "Ship Sealed"
    // split OUT of the main Rip-Live queue into this internal pull/pack/ship list.
    // Keyed by the order id; one record per order (no combine — each ships on its
    // own). Never shown on the public/overlay views. { id, buyerId, buyer,
    // buyerHandle, items, total, createdAt, receivedAt, status, source, orderName }.
    this.sealedOrders = new Map();
    this.pastEvents = []; // archived finished events (most recent first)
    this.eventCounter = 0;
    this.houseSeatToggle = 0; // flips each (Omega) quack so Nikki/Greg order alternates
    this.activeEventId = null; // which event side-queue is toggled to the overlay/Discord
    // Durable dedup for orders that produced ONLY event entries (and so never
    // land in the main `orders` map). Prevents double-ingest across restarts.
    this.seenEventOrders = new Set();
    // Durable "already fulfilled" event-name registry. Once an event is ripped
    // (fulfilled) or archived, its canonical title is recorded here so the
    // Shopify auto-sync never re-creates it from a product that is still listed
    // (even "sold out") in the store. The ONLY way a name comes back is deleting
    // the event BEFORE it's fulfilled — a bare removeEvent, which does not
    // register it. Backfilled from pastEvents on boot so the guard is retroactive.
    this.fulfilledEventNames = []; // [{ title, canon, at, eventId }]

    // Label for the per-variant tracker card (the "Piggy Bank" on Poke Pig).
    // Configurable per store via env so iPoke can call it its own thing
    // (e.g. "Pokemon TCG: iPoke VAULT"). Shown on the public page, admin, Discord.
    this.trackerTitle = process.env.TRACKER_TITLE || '🐷 Piggy Bank Tracker';
    this.trackerSubtitle = process.env.TRACKER_SUBTITLE || 'Persists Across Streams Until Hit';

    // Live per-overlay display settings, controlled from the admin panel and
    // pushed to the OBS overlays in real time. Two overlays: the main queue and
    // the Vault (pack-count) board. opacity 0..1 (background), scale 0.5..3,
    // panel = show background + border (single toggle).
    this.overlays = {
      queue: { opacity: 0, scale: 1, panel: true, speed: 26, textColor: '', bgColor: '' },
      vault: { opacity: 0, scale: 1, panel: true, speed: 26, textColor: '', bgColor: '' },
    };
    // Pacific-day marker for the daily "Past Days" rollover.
    this.lastRolloverDay = null;

    this._ensureDataDir();
    this._load();
    this._seedFromEnv();
    // Boot stamp: every queue mutation stamps + tallies the current top slot via
    // _markTopReached(), but a plain restart (redeploy) reloads orders without one.
    // Stamp whoever is already at #1 now so a redeploy can't drop the top order's
    // vault count — it would otherwise stay uncounted until the next queue change,
    // and be missed entirely if that order were fulfilled first. The permanent
    // reachedTopAt guard means an already-counted order is never counted twice.
    try { if (this._markTopReached()) this._persist(); } catch (e) { console.warn('[queue] boot top-stamp failed:', e.message); }
    // (#6) Give any existing open (Omega) quack its auto giveaway seat on boot, so
    // the rule applies to events that already existed before this shipped.
    this._backfillGiveawaySeats();
    // (#8) Pull Ship-Sealed items out of orders that were queued before the
    // Ship-Sealed split existed, so they land in the Ship-Sealed queue retroactively.
    try { this.sweepQueuedIntoSealed(); } catch (e) { console.warn('[queue] sealed sweep failed:', e.message); }
    // Seed the durable vault audit ledger from orders already in memory, so it's
    // populated on first deploy and after every restart (not just new orders).
    try { this._backfillVaultLedger(); } catch (e) { console.warn('[queue] vault ledger backfill failed:', e.message); }
  }

  /** Resolve the display name for a buyer, honouring a manual override. */
  _displayName(buyerId, fallback) {
    const ov = this.nameOverrides[String(buyerId)];
    return (ov && String(ov).trim()) ? String(ov).trim() : (fallback || 'Buyer');
  }

  _ensureDataDir() {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  _load() {
    try {
      if (fs.existsSync(CONFIG_FILE)) {
        const cfg = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
        this.priorityItems = (cfg.priorityItems || []).map((s) => s.toLowerCase());
        // Priority settings (optional / backward compatible — default to the
        // historical behavior: $1000 threshold, both metrics on).
        if (Number.isFinite(cfg.priorityMoneyThreshold) && cfg.priorityMoneyThreshold >= 0) {
          this.priorityMoneyThreshold = cfg.priorityMoneyThreshold;
        }
        if (typeof cfg.priorityKeywordsOn === 'boolean') this.priorityKeywordsOn = cfg.priorityKeywordsOn;
        if (typeof cfg.priorityMoneyOn === 'boolean') this.priorityMoneyOn = cfg.priorityMoneyOn;
        this.trackedVariants = Array.isArray(cfg.trackedVariants) ? cfg.trackedVariants : [];
        this.variantCounts = (cfg.variantCounts && typeof cfg.variantCounts === 'object') ? cfg.variantCounts : {};
        this.variantLog = Array.isArray(cfg.variantLog) ? cfg.variantLog : [];
        if (Array.isArray(cfg.vaultTallyLog)) this.vaultTallyLog = cfg.vaultTallyLog;
        this.batchCounter = cfg.batchCounter || 0;
        this.live = !!cfg.live;
        this.sessionStartedAt = cfg.sessionStartedAt || null;
        // iPoke fields (all optional / backward compatible).
        // Mark that config.json existed so env only seeds first-boot defaults
        // and never clobbers a later admin change persisted to disk.
        this._combineLoadedFromDisk = true;
        if (COMBINE_MODES.has(cfg.combineMode)) this.combineMode = cfg.combineMode;
        if (Number.isFinite(cfg.combineWindowMs) && cfg.combineWindowMs > 0) this.combineWindowMs = cfg.combineWindowMs;
        if (cfg.nameOverrides && typeof cfg.nameOverrides === 'object') this.nameOverrides = cfg.nameOverrides;
        if (Array.isArray(cfg.preppedBatches)) this.preppedBatches = new Set(cfg.preppedBatches);
        if (Array.isArray(cfg.events)) this.events = cfg.events;
        if (Array.isArray(cfg.fulfilledEventNames)) this.fulfilledEventNames = cfg.fulfilledEventNames;
        this.eventCounter = cfg.eventCounter || 0;
        this.houseSeatToggle = cfg.houseSeatToggle || 0;
        this.activeEventId = cfg.activeEventId || null;
        if (cfg.overlays && typeof cfg.overlays === 'object') {
          for (const k of ['queue', 'vault']) {
            if (cfg.overlays[k]) Object.assign(this.overlays[k], cfg.overlays[k]);
          }
        } else if (Number.isFinite(cfg.overlayOpacity)) {
          this.overlays.queue.opacity = Math.max(0, Math.min(1, cfg.overlayOpacity)); // migrate old single value
        }
        if (cfg.lastRolloverDay) this.lastRolloverDay = cfg.lastRolloverDay;
      }
    } catch (e) {
      console.warn('[queue] could not load config:', e.message);
    }
    try {
      if (fs.existsSync(EVENTS_FILE)) {
        const arr = JSON.parse(fs.readFileSync(EVENTS_FILE, 'utf8'));
        if (Array.isArray(arr)) {
          for (const e of arr) this.eventEntries.set(e.id, e);
          for (const e of arr) if (e.orderId) this.seenEventOrders.add(String(e.orderId));
        }
      }
    } catch (e) {
      console.warn('[queue] could not load events:', e.message);
    }
    try {
      if (fs.existsSync(SEALED_FILE)) {
        const arr = JSON.parse(fs.readFileSync(SEALED_FILE, 'utf8'));
        if (Array.isArray(arr)) for (const o of arr) {
          this.sealedOrders.set(String(o.id), o);
          this.seenEventOrders.add(String(o.id)); // dedup sealed-only orders across restarts
        }
      }
    } catch (e) {
      console.warn('[queue] could not load sealed orders:', e.message);
    }
    try {
      if (fs.existsSync(PAST_EVENTS_FILE)) {
        const arr = JSON.parse(fs.readFileSync(PAST_EVENTS_FILE, 'utf8'));
        if (Array.isArray(arr)) this.pastEvents = arr;
      }
    } catch (e) {
      console.warn('[queue] could not load past events:', e.message);
    }
    try {
      if (fs.existsSync(VAULT_LEDGER_FILE)) {
        const obj = JSON.parse(fs.readFileSync(VAULT_LEDGER_FILE, 'utf8'));
        if (obj && typeof obj === 'object' && !Array.isArray(obj)) this.vaultLedger = obj;
      }
    } catch (e) {
      console.warn('[queue] could not load vault ledger:', e.message);
    }
    // Retroactively seed the fulfilled-name registry from events that already
    // finished before this guard existed (archived → pastEvents, or still on the
    // board as "ripped"), so a first deploy immediately protects names the store
    // owner already worked through.
    this._backfillFulfilledNames();
    try {
      if (fs.existsSync(HISTORY_FILE)) {
        this.history = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')) || [];
      }
    } catch (e) {
      console.warn('[queue] could not load history:', e.message);
    }
    try {
      if (fs.existsSync(STATE_FILE)) {
        const arr = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
        for (const o of arr) this.orders.set(o.id, o);
        // Rebuild openBatch map from queued orders.
        for (const o of arr) {
          if (o.status === 'queued') this.openBatch.set(o.buyerId, o.batchKey);
        }
        // Migrate the vault "counted" flag for orders saved before it existed, so
        // the new fulfill-grace sweep never double-counts or retroactively jumps:
        //  - anything that already reached the top was counted under the old rule;
        //  - anything already fulfilled/cancelled is locked as counted (no
        //    surprise retroactive add) — only fulfills going forward use grace.
        //  - queued orders that never reached the top stay uncounted, so they
        //    still count when they reach the top or are fulfilled long enough.
        for (const o of this.orders.values()) {
          if (o.vaultCounted === undefined) {
            o.vaultCounted = !!o.reachedTopAt || o.status === 'fulfilled' || o.status === 'cancelled';
          }
        }
        console.log(`[queue] restored ${this.orders.size} orders from disk`);
      }
    } catch (e) {
      console.warn('[queue] could not load state:', e.message);
    }
  }

  /**
   * Per-store configuration lives in env vars so BOTH stores can run the exact
   * same code. Idempotent and safe to run every boot:
   *  - PRIORITY_ITEMS_EXTRA: comma/newline list of item substrings that should
   *    ALWAYS trigger priority (unioned in, never removes existing).
   *  - TRACKED_VARIANTS: JSON array of { id, label, product, variant } — the
   *    per-variant counters. Only the DEFINITIONS are seeded; live counts in
   *    variantCounts are never touched here.
   */
  _seedFromEnv() {
    try {
      const extra = String(process.env.PRIORITY_ITEMS_EXTRA || '')
        .split(/[\n,]/).map((s) => s.trim().toLowerCase()).filter(Boolean);
      if (extra.length) {
        const set = new Set(this.priorityItems);
        for (const p of extra) set.add(p);
        this.priorityItems = [...set];
      }
    } catch (e) { console.warn('[queue] PRIORITY_ITEMS_EXTRA seed failed:', e.message); }
    try {
      if (process.env.TRACKED_VARIANTS) {
        const defs = JSON.parse(process.env.TRACKED_VARIANTS);
        if (Array.isArray(defs)) {
          const byId = new Map(this.trackedVariants.map((v) => [v.id, v]));
          for (const d of defs) {
            if (!d || d.id == null) continue;
            byId.set(String(d.id), {
              id: String(d.id),
              label: String(d.label || d.id),
              product: String(d.product || ''),
              variant: String(d.variant || ''),
            });
          }
          this.trackedVariants = [...byId.values()];
        }
      }
    } catch (e) { console.warn('[queue] TRACKED_VARIANTS seed failed:', e.message); }
    // ── iPoke config from env (idempotent) ──
    // PERPETUAL=true → queue never turns off; ingest runs 24/7 (incl. offline).
    if (String(process.env.PERPETUAL || '').toLowerCase() === 'true') {
      this.perpetual = true;
      this.live = true; // perpetual queues are always "on"
      if (!this.sessionStartedAt) this.sessionStartedAt = Date.now();
    }
    // COMBINE_MODE = always | off | time  (default 'always' for legacy stores).
    // Only seeds the DEFAULT on first boot; a later admin change is preserved
    // because _load ran first and set this.combineMode from disk.
    try {
      const envMode = String(process.env.COMBINE_MODE || '').trim().toLowerCase();
      if (!this._combineLoadedFromDisk) {
        if (COMBINE_MODES.has(envMode)) this.combineMode = envMode;
        else if (this.perpetual) this.combineMode = 'off'; // iPoke default: no auto-combine
      }
      const envWin = Number(process.env.COMBINE_WINDOW_MINUTES);
      if (Number.isFinite(envWin) && envWin > 0 && !this._combineLoadedFromDisk) {
        this.combineWindowMs = Math.round(envWin * 60 * 1000);
      }
      const envOp = Number(process.env.OVERLAY_OPACITY);
      if (Number.isFinite(envOp) && !this._combineLoadedFromDisk) {
        this.overlays.queue.opacity = Math.max(0, Math.min(1, envOp));
      }
    } catch (e) { console.warn('[queue] COMBINE_MODE seed failed:', e.message); }
    // NOTE: we deliberately do NOT un-combine slots on boot. In 'off' mode the
    // ONLY way two orders share a batchKey is a deliberate MANUAL combine (ingest
    // always opens a fresh slot when auto-combine is off), so splitting shared
    // slots on restart would silently undo the admin's manual combines — exactly
    // what used to happen on every redeploy. Manual combines persist via batchKey
    // and must survive restarts. (Switching the mode TO 'off' at runtime still
    // splits existing merges, via setCombineMode — that's an explicit admin action.)
    // Clear stale TikTok auto-holds on boot. TikTok orders sync into Shopify as
    // ON_HOLD by default, which we no longer treat as a real hold — but orders
    // ingested before that change kept the flag. The onHold boolean is only ever
    // set from Shopify's fulfillment status (the manual set-aside uses status
    // 'held' instead), so clearing it here for TikTok orders is safe and removes
    // the backlog of noise badges in one pass.
    try {
      let cleared = 0;
      for (const o of this.orders.values()) {
        if (o.onHold && /tiktok|^tt$/i.test(String(o.source || ''))) { o.onHold = false; cleared++; }
      }
      if (cleared) console.log(`[queue] cleared ${cleared} stale TikTok auto-hold flag(s) on boot`);
    } catch (e) { console.warn('[queue] boot hold-clear failed:', e.message); }
    // Recompute priority flags in case the env extras changed matching, then save.
    for (const o of this.orders.values()) o.hasPriority = this._isPriorityOrder(o.items);
    this._persist();
  }

  /** Stamp the current top slot's orders with reachedTopAt (once each) and tally
   *  any tracked-variant units they contain. Called after every queue mutation
   *  so "reached the top" is detected no matter what caused the reorder. */
  _markTopReached() {
    const top = this.activeQueue()[0];
    if (!top) return false;
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === top.key && o.status === 'queued'
    );
    if (!orders.length) return false;
    const now = Date.now();
    let changed = false;
    for (const o of orders) {
      if (!o.reachedTopAt) { o.reachedTopAt = now; changed = true; } // timing stamp (once)
      if (this._countVaultOnce(o)) changed = true;                    // vault tally (once)
    }
    return changed;
  }

  /** Tally an order's vault units toward the counters exactly once, ever. The
   *  `vaultCounted` flag is the single source of truth shared by BOTH count paths
   *  (reaching the top, and the fulfill-grace sweep), so an order can never add to
   *  the vault twice — no matter how it gets counted or how often it re-tops. */
  _countVaultOnce(order, path = 'top') {
    if (order.vaultCounted) return false;
    const added = this._tallyVariants(order);
    // Only lock the order as "counted" once it ACTUALLY matched a tracked vault. If
    // nothing matched (e.g. a vault keyword wasn't set up yet when the order first
    // reached the top), leave it un-counted so it still gets picked up later — when
    // the keyword is added — by the next top-reach or the fulfill-grace sweep.
    if (added > 0) {
      order.vaultCounted = true;
      this._markVaultCounted(order, path); // audit ledger: who/when/how it counted
      return true;
    }
    return false;
  }

  /** Pure match: the tracked vaults an order's items hit, with per-match amounts.
   *  ONE definition of "what counts" shared by _orderHasVault, _tallyVariants and
   *  the audit ledger, so the flag, the counter and the audit can never disagree.
   *  Returns [] for a non-vault order (no side effects). */
  _vaultMatches(order) {
    const res = [];
    if (!this.trackedVariants.length) return res;
    for (const it of (order.items || [])) {
      const text = `${it.name || ''} ${it.sku || ''} ${it.variant || ''}`.toLowerCase();
      const qty = it.qty || 1;
      const perItem = this._vaultUnitsPerItem(it);
      for (const v of this.trackedVariants) {
        const prod = (v.product || '').toLowerCase();
        const varn = (v.variant || '').toLowerCase();
        if (!varn) continue;
        if ((!prod || text.includes(prod)) && text.includes(varn)) {
          res.push({ id: v.id, label: v.label, amount: perItem * qty, item: it.name || '' });
        }
      }
    }
    return res;
  }

  /** True if any of an order's items match a tracked (vault) variant. */
  _orderHasVault(order) {
    return this._vaultMatches(order).length > 0;
  }

  /** Fulfill-grace sweep: a vault order fulfilled WITHOUT ever reaching the top
   *  still counts, but only once it has stayed fulfilled for VAULT_FULFILL_GRACE_MS
   *  (so a mistaken fulfill that's undone in time never counts — reopen() clears
   *  fulfilledAt, dropping it out of this sweep). Runs on boot and every minute.
   *  Idempotent via the shared vaultCounted flag, so it never double-counts an
   *  order already tallied at the top. */
  sweepFulfilledVaults() {
    if (!this.trackedVariants.length) return 0;
    const now = Date.now();
    let counted = 0;
    for (const o of this.orders.values()) {
      if (o.status !== 'fulfilled' || o.vaultCounted) continue;
      if (!o.fulfilledAt || (now - o.fulfilledAt) < VAULT_FULFILL_GRACE_MS) continue;
      if (!this._orderHasVault(o)) continue;
      if (this._countVaultOnce(o, 'grace')) counted++;
    }
    if (counted) {
      this._persist();
      this.emit('change', { reason: 'vault-fulfill-grace', counted });
      console.log(`[queue] vault grace: counted ${counted} off-top fulfilled vault order(s)`);
    }
    return counted;
  }

  /** Add an order's units to any matching tracked-variant counters. Matches on
   *  the item name/SKU/variant text: requires the tracked keyword substring, and
   *  (if set) the product substring too, so a short token can't match unrelated
   *  items.
   *
   *  Vault multiplier: each Vault set is sold in tiered variants like
   *  "5 Packs (1 Vault)", "25 Packs (5 Vault)", "50 Packs (10 Vault)". One unit
   *  of a "(N Vault)" variant adds N to the counter, so it counts *vaults*, not
   *  packs — e.g. 5× "5 Packs (1 Vault)" = +5, but 1× "25 Packs (5 Vault)" = +5
   *  and 1× "50 Packs (10 Vault)" = +10. When no "(N Vault)" tier is present the
   *  multiplier is 1, preserving the simple "+1 per unit" behaviour. */
  _vaultUnitsPerItem(it) {
    // Prefer the variant field (cleanest), fall back to the folded item name.
    let m = String(it.variant || '').toLowerCase().match(/(\d+)\s*vault/);
    if (!m) m = String(it.name || '').toLowerCase().match(/(\d+)\s*vault/);
    return m ? Math.max(1, parseInt(m[1], 10)) : 1;
  }
  _tallyVariants(order) {
    const matches = this._vaultMatches(order);
    if (!matches.length) return 0;
    // Order's last-4 (digits of its order number, else last 4 of the id) for the adds log.
    const last4 = (() => {
      const digits = String(order.orderName || '').replace(/\D/g, '');
      if (digits) return digits.slice(-4);
      return String(order.id || '').slice(-4) || '—';
    })();
    let added = 0;
    for (const m of matches) {
      const before = this.variantCounts[m.id] || 0;
      this.variantCounts[m.id] = before + m.amount;
      added += m.amount;
      // Record the automatic add so the admin can see which order bumped which vault.
      this.vaultTallyLog.unshift({ ts: Date.now(), order: last4, vault: m.label, amount: m.amount, to: before + m.amount });
      if (this.vaultTallyLog.length > 200) this.vaultTallyLog.length = 200;
    }
    return added;
  }

  /** Audit ledger: record (or refresh) a rip-live order that matches a tracked
   *  vault. Observational only — never changes any counter. Captures the order's
   *  items, the vaults + expected amounts it should contribute, and its current
   *  status, so a fulfilled-but-uncounted vault order is visible as a potential
   *  miss. Called at ingest and whenever the order's status changes. Preserves any
   *  prior counted/countedAt/path already observed. */
  _recordVaultOrder(order) {
    if (!order || !this.trackedVariants.length) return;
    const matches = this._vaultMatches(order);
    if (!matches.length) return; // not a vault order — nothing to track
    const id = String(order.id);
    const now = Date.now();
    const prev = this.vaultLedger[id] || {};
    const byVault = {};
    for (const m of matches) byVault[m.id] = (byVault[m.id] || 0) + m.amount;
    const expectedTotal = matches.reduce((n, m) => n + m.amount, 0);
    this.vaultLedger[id] = {
      orderId: id,
      orderName: order.orderName || '',
      buyer: order.buyer || '',
      source: order.source || '',
      items: (order.items || []).map((it) => ({ name: it.name || '', qty: it.qty || 1 })),
      vaults: matches.map((m) => ({ id: m.id, label: m.label, amount: m.amount, item: m.item })),
      expected: { total: expectedTotal, byVault },
      // counted reflects the live order flag; once observed true it stays true.
      counted: !!(prev.counted || order.vaultCounted),
      countedAt: prev.countedAt || null,
      path: prev.path || null,
      status: order.status || 'queued',
      firstSeenAt: prev.firstSeenAt || now,
      lastSeenAt: now,
    };
    this._pruneVaultLedger();
    this._persistVaultLedger();
  }

  /** Mark a ledger entry as counted, stamping when and by which path (top-of-queue
   *  vs fulfill-grace sweep). Ensures the entry exists first. */
  _markVaultCounted(order, path) {
    if (!order) return;
    const id = String(order.id);
    if (!this.vaultLedger[id]) this._recordVaultOrder(order);
    const e = this.vaultLedger[id];
    if (!e) return;
    e.counted = true;
    e.countedAt = Date.now();
    e.path = path || e.path || 'top';
    e.status = order.status || e.status;
    e.lastSeenAt = Date.now();
    this._persistVaultLedger();
  }

  /** Reconciliation report over the durable ledger. Per tracked vault: the live
   *  counter, the sum actually counted and the sum expected from ledgered orders,
   *  plus how much is uncounted and how much of THAT is on fulfilled orders (the
   *  real "miss" signal — a vault order that shipped but never counted). Also lists
   *  those potential-miss orders. Queued-not-yet-topped orders show as uncounted
   *  but NOT as misses (they count correctly once they reach the top). */
  vaultLedgerReport() {
    const entries = Object.values(this.vaultLedger)
      .sort((a, b) => (b.firstSeenAt || 0) - (a.firstSeenAt || 0));
    const byVault = {};
    for (const v of this.trackedVariants) {
      byVault[v.id] = {
        id: v.id, label: v.label, count: this.variantCounts[v.id] || 0,
        countedSum: 0, expectedSum: 0, uncountedSum: 0, missSum: 0, orders: 0,
      };
    }
    for (const e of entries) {
      for (const vb of (e.vaults || [])) {
        const b = byVault[vb.id];
        if (!b) continue;
        b.orders += 1;
        b.expectedSum += vb.amount;
        if (e.counted) b.countedSum += vb.amount;
        else {
          b.uncountedSum += vb.amount;
          if (e.status === 'fulfilled') b.missSum += vb.amount;
        }
      }
    }
    const potentialMisses = entries.filter((e) => !e.counted && e.status === 'fulfilled');
    const pending = entries.filter((e) => !e.counted && e.status !== 'fulfilled');
    return {
      vaults: Object.values(byVault),
      totalOrders: entries.length,
      potentialMisses,
      pendingCount: pending.length,
      entries,
    };
  }

  /** Lean reconciliation summary for the live snapshot (no per-order entries):
   *  per-vault counted vs expected and the number of potential-miss orders. */
  _vaultAuditSummary() {
    const r = this.vaultLedgerReport();
    return {
      vaults: r.vaults.map((v) => ({
        id: v.id, label: v.label, count: v.count,
        countedSum: v.countedSum, expectedSum: v.expectedSum,
        missSum: v.missSum, uncountedSum: v.uncountedSum,
      })),
      potentialMissCount: r.potentialMisses.length,
      pendingCount: r.pendingCount,
      totalOrders: r.totalOrders,
    };
  }

  /** Replace the tracked-variant DEFINITIONS (admin editor). Preserves the live
   *  count for any id that still exists; drops counts for removed ids. */
  setTrackedVariants(list) {
    const defs = (Array.isArray(list) ? list : []).filter((d) => d && d.id != null).map((d) => ({
      id: String(d.id),
      label: String(d.label || d.id),
      product: String(d.product || ''),
      variant: String(d.variant || ''),
      // The "chase card" that triggers this Vault — shown under the pack name on
      // the Vault overlay. Free text, optional.
      chase: String(d.chase || ''),
    }));
    this.trackedVariants = defs;
    const keep = {};
    for (const d of defs) keep[d.id] = this.variantCounts[d.id] || 0;
    this.variantCounts = keep;
    this._persist();
    this.emit('change', { reason: 'variants-config' });
  }

  /** Manually set one variant's counter (admin correction) or reset it to 0.
   *  Every manual change is written to variantLog with the BEFORE value, so if a
   *  counter is ever changed/reset by mistake the prior number is recoverable. */
  setVariantCount(id, n) {
    const key = String(id);
    const v = this.trackedVariants.find((x) => x.id === key);
    if (!v) return false;
    const from = this.variantCounts[key] || 0;
    const to = Math.max(0, Math.floor(Number(n) || 0));
    this.variantCounts[key] = to;
    this._logVariant({
      id: key, label: v.label, from, to,
      action: (to === 0 && from !== 0) ? 'reset' : 'edit',
    });
    this._persist();
    this.emit('change', { reason: 'variant-count', id: key });
    return true;
  }

  /** Append an audit entry (most-recent-first) and cap the log size. */
  _logVariant(entry) {
    this.variantLog.unshift({ ts: Date.now(), ...entry });
    if (this.variantLog.length > 200) this.variantLog.length = 200;
  }

  /** Atomic write: write to a temp file then rename over the target, so a crash
   *  or redeploy mid-write can never leave a half-written (corrupt) state file.
   *  Critical for a perpetual queue that persists constantly. */
  _atomicWrite(file, data) {
    const tmp = file + '.tmp';
    fs.writeFileSync(tmp, data);
    fs.renameSync(tmp, file);
  }

  _persist() {
    try {
      this._atomicWrite(STATE_FILE, JSON.stringify([...this.orders.values()]));
      this._atomicWrite(
        CONFIG_FILE,
        JSON.stringify({
          priorityItems: this.priorityItems,
          priorityMoneyThreshold: this.priorityMoneyThreshold,
          priorityKeywordsOn: this.priorityKeywordsOn,
          priorityMoneyOn: this.priorityMoneyOn,
          trackedVariants: this.trackedVariants,
          variantCounts: this.variantCounts,
          variantLog: this.variantLog,
          vaultTallyLog: this.vaultTallyLog,
          batchCounter: this.batchCounter,
          live: this.live,
          sessionStartedAt: this.sessionStartedAt,
          // iPoke fields
          combineMode: this.combineMode,
          combineWindowMs: this.combineWindowMs,
          nameOverrides: this.nameOverrides,
          preppedBatches: [...this.preppedBatches],
          events: this.events,
          fulfilledEventNames: this.fulfilledEventNames,
          eventCounter: this.eventCounter,
          activeEventId: this.activeEventId,
          houseSeatToggle: this.houseSeatToggle,
          overlays: this.overlays,
          lastRolloverDay: this.lastRolloverDay,
        })
      );
      // Only write the events-state file when there are (or were) events, so
      // stores that never use events don't get an extra file.
      if (this.eventEntries.size || fs.existsSync(EVENTS_FILE)) {
        this._atomicWrite(EVENTS_FILE, JSON.stringify([...this.eventEntries.values()]));
      }
      if (this.sealedOrders.size || fs.existsSync(SEALED_FILE)) {
        this._atomicWrite(SEALED_FILE, JSON.stringify([...this.sealedOrders.values()]));
      }
    } catch (e) {
      console.warn('[queue] persist failed:', e.message);
    }
  }

  _persistHistory() {
    try {
      this._atomicWrite(HISTORY_FILE, JSON.stringify(this.history));
    } catch (e) {
      console.warn('[queue] history persist failed:', e.message);
    }
  }

  _persistPastEvents() {
    try {
      this._atomicWrite(PAST_EVENTS_FILE, JSON.stringify(this.pastEvents));
    } catch (e) {
      console.warn('[queue] past-events persist failed:', e.message);
    }
  }

  /** Keep the vault ledger bounded: retain the most recent VAULT_LEDGER_MAX orders
   *  by first-seen time (plenty for months of streams). */
  _pruneVaultLedger() {
    const ids = Object.keys(this.vaultLedger);
    if (ids.length <= VAULT_LEDGER_MAX) return;
    const sorted = ids
      .map((id) => this.vaultLedger[id])
      .sort((a, b) => (b.firstSeenAt || 0) - (a.firstSeenAt || 0))
      .slice(0, VAULT_LEDGER_MAX);
    const keep = {};
    for (const e of sorted) keep[e.orderId] = e;
    this.vaultLedger = keep;
  }

  _persistVaultLedger() {
    try {
      this._atomicWrite(VAULT_LEDGER_FILE, JSON.stringify(this.vaultLedger));
    } catch (e) {
      console.warn('[queue] vault-ledger persist failed:', e.message);
    }
  }

  /** Detailed per-order records of everything fulfilled this session. */
  fulfilledRecords() {
    return [...this.orders.values()]
      .filter((o) => o.status === 'fulfilled')
      .sort((a, b) => (b.fulfilledAt || 0) - (a.fulfilledAt || 0))
      .map((o) => ({
        orderId: o.id,
        buyerId: o.buyerId,
        buyer: o.buyer,
        items: o.items,
        total: o.total,
        fulfilledAt: o.fulfilledAt,
      }));
  }

  /** Order ids of everything still queued — used by the poller to batch-refresh
   *  On Hold status. */
  queuedOrderIds() {
    return [...this.orders.values()].filter((o) => o.status === 'queued').map((o) => o.id);
  }

  /** Flag/unflag a queued order as On Hold (TikTok-side). Returns true if it
   *  actually changed, so the poller can log only real transitions. */
  setHold(orderId, onHold) {
    const o = this.orders.get(String(orderId));
    if (!o || o.status !== 'queued') return false;
    if (!!o.onHold === !!onHold) return false;
    o.onHold = !!onHold;
    this._persist();
    this.emit('change', { reason: 'hold-change', orderId: String(orderId), onHold: !!onHold });
    return true;
  }

  /** Set/backfill the human-facing Shopify order name (e.g. "#50936") on an
   *  order already in the system. Used by the Shopify poller to fill in the name
   *  for orders that were ingested before the name was captured, so the queue can
   *  display the merchant order number instead of the internal id. Idempotent. */
  setOrderName(orderId, name) {
    const o = this.orders.get(String(orderId));
    if (!o) return false;
    const nm = String(name == null ? '' : name).trim();
    if (!nm || o.orderName === nm) return false;
    o.orderName = nm;
    this._persist();
    this.emit('change', { reason: 'ordername-backfill', orderId: String(orderId) });
    return true;
  }

  /** Set/backfill the base buyer label (e.g. "Logan Pa") on an order already in
   *  the system. Lets the Shopify poller re-apply the current name format to
   *  orders ingested under an older format. A manual name override (nameOverrides)
   *  still wins at display time via _displayName, so this never clobbers a rename.
   *
   *  Also refreshes the buyer label stored directly on this order's EVENT seats
   *  and SHIP-SEALED record. Those carry their own buyer copy captured at ingest
   *  (an all-event order never enters this.orders at all), so without this they'd
   *  keep showing the name format they were created under — which is exactly why
   *  event labels lagged on "First L." after the format moved to "First La".
   *  A manually renamed seat (nameOverridden) always wins and is left untouched.
   *  Idempotent. */
  setBuyerName(orderId, name) {
    const oid = String(orderId);
    const nm = String(name == null ? '' : name).trim();
    if (!nm) return false;
    let changed = false;
    const o = this.orders.get(oid);
    if (o && o.buyer !== nm) { o.buyer = nm; changed = true; }
    // Event seats for this order (skip seats renamed by hand).
    for (const e of this.eventEntries.values()) {
      if (String(e.orderId) === oid && !e.nameOverridden && e.buyer !== nm) {
        e.buyer = nm; changed = true;
      }
    }
    // Ship-Sealed record for this order.
    const s = this.sealedOrders.get(oid);
    if (s && !s.nameOverridden && s.buyer !== nm) { s.buyer = nm; changed = true; }
    if (!changed) return false;
    this._persist();
    this.emit('change', { reason: 'buyer-backfill', orderId: oid });
    return true;
  }

  /** Shop order ids whose buyer label may still need reformatting to the current
   *  name format: everything queued in the main Rip-Live queue, PLUS every open
   *  event seat and Ship-Sealed order. The last two matter because an order whose
   *  items all route into an event (or all into Ship-Sealed) never lands in
   *  this.orders, so the main-queue refresh alone can never reach it. Deduped;
   *  manual/giveaway seats (no real Shopify order) are excluded. */
  labelRefreshOrderIds() {
    const ids = new Set();
    for (const o of this.orders.values()) {
      if (o.status === 'queued' && o.id) ids.add(String(o.id));
    }
    for (const e of this.eventEntries.values()) {
      // Include per-slot FULFILLED seats too, not just queued ones: an event can
      // be mid-rip with some seats already fulfilled while the full roster (queued
      // + fulfilled) stays visible and printable in the admin. Matches eventQueue's
      // own roster filter, so every label that can be printed gets the current name.
      if ((e.status === 'queued' || e.status === 'fulfilled') && e.orderId && /^shop:/.test(String(e.orderId))) {
        ids.add(String(e.orderId));
      }
    }
    for (const s of this.sealedOrders.values()) {
      if (s && s.id && s.status !== 'fulfilled' && s.status !== 'cancelled' && /^shop:/.test(String(s.id))) {
        ids.add(String(s.id));
      }
    }
    return [...ids];
  }

  /** Detailed per-order records of orders still queued (unfulfilled) this
   *  session — captured when a stream is archived so nothing is silently lost. */
  queuedRecords() {
    return [...this.orders.values()]
      .filter((o) => o.status === 'queued')
      .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0))
      .map((o) => ({
        orderId: o.id,
        buyerId: o.buyerId,
        buyer: o.buyer,
        items: o.items,
        total: o.total,
        createdAt: o.createdAt,
      }));
  }

  /** Detailed per-order records of everything cancelled this session. */
  cancelledRecords() {
    return [...this.orders.values()]
      .filter((o) => o.status === 'cancelled')
      .sort((a, b) => (b.cancelledAt || 0) - (a.cancelledAt || 0))
      .map((o) => ({
        orderId: o.id,
        buyerId: o.buyerId,
        buyer: o.buyer,
        items: o.items,
        total: o.total,
        cancelledAt: o.cancelledAt,
      }));
  }

  _isPriorityOrder(items) {
    if (!this.priorityItems.length) return false;
    return (items || []).some((it) => {
      const name = (it.name || '').toLowerCase();
      const sku = (it.sku || '').toLowerCase();
      return this.priorityItems.some((p) => p && (name.includes(p) || sku.includes(p)));
    });
  }

  /** Ingest an order.
   *  - Event line items (matched by keyword) are pulled OUT into their event's
   *    side-queue; the remaining regular items form the main-queue slot.
   *  - How repeat orders from the same buyer group into a slot depends on the
   *    combine mode ('always' = legacy merge, 'off' = own position, 'time' =
   *    merge only within the window measured from the slot's first order). */
  upsertOrder(raw) {
    const id = String(raw.id);
    if (this.orders.has(id) || this.seenEventOrders.has(id)) {
      // Already known — don't re-ingest, but DO refresh the display-only fields
      // (buyer label + Shopify order name) from the latest fetch, so a change to
      // the name format or a late-populated order name reaches orders already on
      // the board (step-1's boot lookback re-sends recent orders on restart). A
      // manual name override still wins at display time via _displayName, and we
      // never touch items/total/status/position here.
      const existing = this.orders.get(id);
      if (existing) {
        let changed = false;
        const nb = String(raw.buyer == null ? '' : raw.buyer).trim();
        if (nb && existing.buyer !== nb) { existing.buyer = nb; changed = true; }
        const nn = String(raw.orderName == null ? '' : raw.orderName).trim();
        if (nn && existing.orderName !== nn) { existing.orderName = nn; changed = true; }
        if (changed) { this._persist(); this.emit('change', { reason: 'order-refresh', orderId: id }); }
      }
      // Keep the Ship-Sealed side-queue record's display fields in sync too.
      const sx = this.sealedOrders.get(id);
      if (sx) {
        let ch = false;
        const nb = String(raw.buyer == null ? '' : raw.buyer).trim();
        if (nb && sx.buyer !== nb) { sx.buyer = nb; ch = true; }
        const nn = String(raw.orderName == null ? '' : raw.orderName).trim();
        if (nn && sx.orderName !== nn) { sx.orderName = nn; ch = true; }
        if (ch) { this._persist(); this.emit('change', { reason: 'sealed-refresh', orderId: id }); }
      }
      return existing || null;
    }

    // Off-air gate. Perpetual queues (iPoke) ingest 24/7 regardless of live.
    if (!this.live && !this.perpetual) return null;

    const buyerId = String(raw.buyerId);
    const allItems = raw.items || [];

    // ── Event routing ── pull event-spot line items into their event side-queue.
    // Each matching line item becomes its own event entry (qty = spot count).
    const nonEvent = [];
    const routed = [];
    for (const it of allItems) {
      const ev = this._matchEvent(it);
      if (ev) routed.push({ it, ev });
      else nonEvent.push(it);
    }
    if (routed.length) {
      routed.forEach((r, i) => this._addEventEntry(r.ev, r.it, raw, i));
      this.seenEventOrders.add(id);
    }

    // ── Ship-Sealed routing ── line items whose name contains "Ship Sealed" go
    // into the internal Ship-Sealed side-queue instead of the main Rip-Live queue.
    // An order can split across all three (event + rip-live + ship-sealed).
    const regularItems = [];
    const sealedItems = [];
    for (const it of nonEvent) {
      if (this._isSealedItem(it)) sealedItems.push(it); else regularItems.push(it);
    }
    if (sealedItems.length) this._addSealedSlot(id, raw, sealedItems);

    // Nothing left for the main Rip-Live queue → no main slot, but event and/or
    // ship-sealed parts may still have been created. Mark the order seen so a
    // re-fetch doesn't try to re-ingest it.
    if (!regularItems.length) {
      if (routed.length || sealedItems.length) {
        this.seenEventOrders.add(id);
        this._persist();
        this.emit('change', { reason: 'side-queue-order', orderId: id });
      }
      return null;
    }

    const items = regularItems;
    const hasPriority = this._isPriorityOrder(items);

    // ── Slot selection honouring the combine mode ──
    let batchKey;
    let mergedInto = false;
    let afterTopOf = null; // (untiltop) the top-locked slot this new order follows
    if (this.combineMode === 'off') {
      // Every order is its own position — no automatic combining.
      batchKey = `${buyerId}#${++this.batchCounter}`;
    } else {
      const open = this.openBatch.get(buyerId);
      const createdAt = raw.createdAt || Date.now();
      if (open && this._canMergeInto(open, createdAt)) {
        batchKey = open;
        mergedInto = true;
      } else {
        // Not mergeable → open a fresh slot. In 'untiltop', if the reason we
        // couldn't merge is that the buyer's slot already hit the top, record
        // the link so that slot can flash a "they ordered again" indicator.
        if (this.combineMode === 'untiltop' && open && this._batchReachedTop(open)) {
          afterTopOf = open;
        }
        batchKey = `${buyerId}#${++this.batchCounter}`;
        this.openBatch.set(buyerId, batchKey);
      }
    }

    // Was this buyer already sitting at the TOP of the queue (position 1, i.e.
    // very likely already being packed) when this new order merged in? Flag it
    // so the packer is loudly told to add the new item(s) to the bag in
    // progress instead of assuming that slot is done. (Computed before the new
    // order is added; a merge never changes the slot's position.)
    let mergedWhileTop = false;
    if (mergedInto) {
      const top = this.activeQueue()[0];
      mergedWhileTop = !!(top && top.key === batchKey);
    }

    const order = {
      id,
      buyerId,
      buyer: raw.buyer || 'Buyer',
      buyerHandle: raw.buyerHandle || '',
      items,
      total: Number(raw.total || 0),
      createdAt: raw.createdAt || Date.now(),
      receivedAt: Date.now(),
      status: 'queued',
      batchKey,
      hasPriority,
      mergedWhileTop,
      onHold: !!raw.onHold,
      // iPoke: where the order originated (for the TT/SF badge) and the
      // human-facing Shopify order number (packer's reference). Empty for the
      // TikTok-only stores.
      source: raw.source || '',
      orderName: raw.orderName || '',
      // Admin-only full name + shipping address for the packing label (#2). Never
      // surfaced on the public/overlay views.
      fullName: raw.fullName || '',
      shipping: raw.shipping || null,
      // (untiltop) set when this order opened its own slot because the buyer's
      // earlier slot had already reached the top. Links back to that slot's key.
      afterTopOf: afterTopOf || null,
    };
    this.orders.set(id, order);
    this._recordVaultOrder(order); // durable vault audit ledger (observational)
    this._markTopReached();
    this._persist();

    const entry = this._entryFor(batchKey);
    this.emit('change', { reason: mergedInto ? 'merged' : 'new-slot', entry, order });
    if (!mergedInto) this.emit('new-slot', entry);
    else this.emit('merged', { entry, order });
    return order;
  }

  /** Build an aggregated slot entry from all queued orders sharing a batchKey. */
  _entryFor(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'queued'
    );
    if (!orders.length) return null;
    const first = orders.reduce((a, b) => (a.createdAt <= b.createdAt ? a : b));
    // How many orders from this same buyer were already fulfilled earlier this
    // stream — powers the "ordered earlier today" heads-up badge in the panel.
    const priorFulfilled = [...this.orders.values()].filter(
      (o) => o.buyerId === first.buyerId && o.status === 'fulfilled'
    ).length;
    // Priority is evaluated LIVE from the configurable settings (keyword list, $
    // threshold, and each metric's on/off switch), EXCEPT where an order carries a
    // freeze lock. A lock ({ kw, big }) is stamped by a "Save for new orders only"
    // settings change so that order keeps its standing regardless of the new
    // settings; "Apply to current queue" clears locks so everything re-evaluates.
    const isLocked = (o) => o.priorityLock && typeof o.priorityLock === 'object';
    const lockedOrders = orders.filter(isLocked);
    const liveOrders = orders.filter((o) => !isLocked(o));
    // Keyword metric: any locked order frozen as keyword-priority, OR (when the
    // keyword metric is on) any live order whose items matched the keyword list.
    const liveKeywordOrders = this.priorityKeywordsOn ? liveOrders.filter((o) => o.hasPriority) : [];
    const keywordPriority = lockedOrders.some((o) => o.priorityLock.kw) || liveKeywordOrders.length > 0;
    // Manual adds can be flagged priority by an admin toggle (no items/$ to go on).
    const manualPriority = orders.some((o) => o.manualPriority);
    // $ rule: the FULL pre-split order total drives this — o.total is the whole
    // Shopify order value (never reduced by the event/sealed split), so the live
    // batch sum here is the combined pre-split total. A single order or a manually
    // combined batch at/above the threshold jumps the queue. (Event seat positions
    // are never affected — they sort purely by purchase time in eventQueue.)
    const batchTotal = orders.reduce((s, o) => s + (Number(o.total) || 0), 0);
    const liveTotal = liveOrders.reduce((s, o) => s + (Number(o.total) || 0), 0);
    const liveBig = this.priorityMoneyOn && liveTotal >= this.priorityMoneyThreshold;
    const bigDollar = lockedOrders.some((o) => o.priorityLock.big) || liveBig;
    const isPriority = keywordPriority || bigDollar || manualPriority;
    // Among priority slots, keep time order: keyword-priority keeps its earliest
    // priority-item time; a $-threshold or manually-flagged slot uses its first
    // order time. Locked keyword orders count toward the earliest-time calc too.
    const keywordTimeOrders = lockedOrders.filter((o) => o.priorityLock.kw).concat(liveKeywordOrders);
    const priorityAt = keywordPriority
      ? Math.min(...keywordTimeOrders.map((o) => o.createdAt || Date.now()))
      : ((bigDollar || manualPriority) ? (first.createdAt || Date.now()) : Infinity);

    // Merge item lines (sum quantities of same-name items).
    const itemMap = new Map();
    for (const o of orders) {
      for (const it of o.items) {
        const key = it.name;
        itemMap.set(key, (itemMap.get(key) || 0) + (it.qty || 1));
      }
    }
    const items = [...itemMap.entries()].map(([name, qty]) => ({ name, qty }));

    const reachedAts = orders.map((o) => o.reachedTopAt).filter(Boolean);
    return {
      key: batchKey,
      buyerId: first.buyerId,
      buyer: this._displayName(first.buyerId, first.buyer),
      // The original name from the order, before any manual override — so the
      // admin can see what was overridden.
      originalBuyer: first.buyer,
      nameOverridden: !!(this.nameOverrides[String(first.buyerId)]),
      // Prepped / ready-to-fulfill flag (visual only; does not reorder).
      prepped: this.preppedBatches.has(batchKey),
      // (untiltop) This slot is itself a "they ordered again after hitting the
      // top" follow-up slot.
      afterTop: orders.some((o) => o.afterTopOf),
      // (untiltop) How many later orders this (top-locked) slot spawned that are
      // still queued in their own positions — drives the "＋ ORDERED AGAIN"
      // flash on the slot the buyer is currently being packed at.
      followupCount: [...this.orders.values()].filter(
        (o) => o.status === 'queued' && o.afterTopOf === batchKey
      ).length,
      // Source badge (iPoke): distinct origins in this slot, plus a short code
      // for the overlay. TikTok → 'TT', anything else (all via Shopify) → 'SF'.
      sources: [...new Set(orders.map((o) => o.source).filter(Boolean))],
      sourceShort: (() => {
        const s = new Set(orders.map((o) => String(o.source || '').toLowerCase()).filter(Boolean));
        if (!s.size) return '';
        if (s.size > 1) return 'MIX';
        const only = [...s][0];
        return only.includes('tiktok') || only === 'tt' ? 'TT' : 'SF';
      })(),
      orderName: first.orderName || '',
      // (#9) Cross-queue markers: does any order in this slot ALSO have an event
      // seat (E) and/or a Ship-Sealed part (S)? Shown as bubbles on the Rip-Live row.
      hasEventPart: (() => {
        const ids = new Set(orders.map((o) => String(o.id)));
        for (const e of this.eventEntries.values()) {
          if (e.status !== 'cancelled' && ids.has(String(e.orderId))) return true;
        }
        return false;
      })(),
      hasSealedPart: orders.some((o) => {
        const s = this.sealedOrders.get(String(o.id));
        return !!s && s.status !== 'cancelled';
      }),
      // Admin-only cross-check: the buyer's unique @username (the queue label is
      // their display name). Never surfaced on the public view.
      buyerHandle: first.buyerHandle || '',
      // Admin-only full name + shipping address for the packing label (#2).
      fullName: first.fullName || '',
      shipping: first.shipping || null,
      orderIds: orders.map((o) => o.id),
      // Human-facing Shopify order number(s) (e.g. "#50936") for each order in
      // the slot — what the queue displays. Falls back to the internal id only
      // for orders whose name hasn't been captured yet.
      orderNames: orders.map((o) => o.orderName || (String(o.id).startsWith('manual:') ? '' : ('#' + String(o.id)))),
      orderCount: orders.length,
      // Per-order breakdown (oldest first) so the panel can group the expanded
      // view by order number and separate anything added after the buyer hit #1.
      // Same-name items WITHIN an order are summed (5× pack, not 1× pack ×5).
      orderLines: orders
        .slice()
        .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0))
        .map((o) => {
          const m = new Map();
          for (const it of (o.items || [])) m.set(it.name, (m.get(it.name) || 0) + (it.qty || 1));
          return {
            id: o.id,
            items: [...m.entries()].map(([name, qty]) => ({ name, qty })),
            addedSinceTop: !!o.mergedWhileTop,
            createdAt: o.createdAt || null,
          };
        }),
      items,
      itemCount: items.reduce((n, i) => n + i.qty, 0),
      // True when any item matches a tracked Vault — drives the "V" tag on labels.
      hasVault: this._orderHasVault({ items }),
      total: orders.reduce((s, o) => s + o.total, 0),
      isPriority,
      // (#7) true when this slot is priority specifically because the order/batch
      // total is ≥ $1000 (vs a keyword priority item).
      bigDollar,
      keywordPriority,
      priorityItems: [...new Set(
        orders.flatMap((o) => o.items.filter((it) => this._isPriorityOrder([it])).map((it) => it.name))
      )],
      firstOrderAt: first.createdAt,
      // When this slot first reached the top of the queue (drives the "time at
      // top" live timer and the fulfillment-time metric). null until it hits #1.
      reachedTopAt: reachedAts.length ? Math.min(...reachedAts) : null,
      priorityAt,
      bumped: !!first.bumped,
      // Manual add: an admin-inserted slot pinned to a chosen position (1 = top,
      // above priority). pinnedAt breaks ties between two pins at the same position.
      pinnedPos: (first.pinnedPos != null ? first.pinnedPos : null),
      pinnedAt: first.pinnedAt || null,
      // Overall-position manual placement (new model): a forced spot the slot is
      // spliced into in activeQueue. insertPos = number (1 = top) or null;
      // insertBottom = always-last. Both empty → flows as a regular order.
      insertPos: (first.insertPos != null ? first.insertPos : null),
      insertBottom: !!first.insertBottom,
      manual: !!first.manual,
      manualPriority: !!first.manualPriority,
      movedFromSealed: !!first.movedFromSealed,
      priorFulfilled,
      // True when a new order merged into this slot while it was already #1 in
      // the queue — surfaced as a loud "ADDED MORE" badge in the panel. The
      // count is how many times it happened, so the badge can show ×2/×3 if the
      // buyer keeps adding after you've already started combining.
      reorderedAtTop: orders.some((o) => o.mergedWhileTop),
      reorderedAtTopCount: orders.filter((o) => o.mergedWhileTop).length,
      // TikTok put one of this slot's orders On Hold (often a buyer cancellation
      // request on an unshipped order) — flag it so it's verified before shipping.
      onHold: orders.some((o) => o.onHold),
      _bumpKey: batchKey,
    };
  }

  markFulfilled(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'queued'
    );
    if (!orders.length) return null;
    const buyerId = orders[0].buyerId;
    const now = Date.now();
    for (const o of orders) {
      o.status = 'fulfilled';
      o.fulfilledAt = now;
      o.bumped = false;
      this._recordVaultOrder(o); // refresh ledger status (→ fulfilled) for miss detection
    }
    if (this.openBatch.get(buyerId) === batchKey) this.openBatch.delete(buyerId);
    this.preppedBatches.delete(batchKey);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'fulfilled', batchKey, buyerId });
    return { batchKey, buyerId };
  }

  reopen(batchKey) {
    const orders = [...this.orders.values()].filter((o) => o.batchKey === batchKey);
    if (!orders.length) return null;
    const buyerId = orders[0].buyerId;
    // In an AUTO-COMBINE mode, refuse if the buyer already has an open slot — we
    // couldn't tell which slot new orders should merge into. With combine OFF
    // (iPoke), every order is its own independent slot, so the buyer having
    // another order in the queue is fine and reopening never conflicts.
    if (this.combineMode !== 'off' && this.openBatch.has(buyerId)) return null;
    for (const o of orders) {
      o.status = 'queued';
      o.fulfilledAt = null;
      o.bumped = false; // return to its natural position, not the top
      this._recordVaultOrder(o); // refresh ledger status (→ queued)
    }
    if (this.combineMode !== 'off') this.openBatch.set(buyerId, batchKey);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'reopened', batchKey });
    return { batchKey };
  }

  /** Force a slot to the very top. Only one slot can be bumped at a time. */
  bump(batchKey) {
    let found = false;
    for (const o of this.orders.values()) {
      if (o.batchKey === batchKey && o.status === 'queued') {
        o.bumped = true;
        found = true;
      } else if (o.bumped) {
        o.bumped = false;
      }
    }
    if (!found) return null;
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'bumped', batchKey });
    return { batchKey };
  }

  /** Manually add a buyer into the Rip-Live queue as their own slot, pinned to a
   *  chosen position (1 = top, which sits ABOVE priority slots). For off-platform
   *  or comped adds the admin enters by hand. Its own unique buyerId so it never
   *  merges with a real buyer. */
  /** Parse a free-text item list ("3x Charizard, Pikachu" or newline-separated)
   *  into [{qty, name}]. Accepts an already-structured array too. */
  _parseItemsText(input) {
    if (Array.isArray(input)) {
      return input
        .map((it) => ({ qty: Math.max(1, Math.floor(Number(it && it.qty) || 1)), name: String((it && it.name) || '').trim() }))
        .filter((it) => it.name);
    }
    const t = String(input == null ? '' : input).trim();
    if (!t) return [];
    return t.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean).map((line) => {
      const m = line.match(/^(\d+)\s*[x×]?\s+(.+)$/i); // optional leading "3x " / "3 " qty
      if (m) return { qty: Math.max(1, parseInt(m[1], 10) || 1), name: m[2].trim() };
      return { qty: 1, name: line };
    });
  }

  /** Parse the flexible `position` value a manual add / re-position can carry:
   *    - blank / null / unrecognized → regular order (flows by time + auto priority)
   *    - "top" or 1                  → forced to the very top (overall position 1)
   *    - "bottom" / "end"            → forced to the very bottom
   *    - a number N                  → forced to overall position N
   *  Returns { insertPos, insertBottom }. insertPos is a number (1 = top) or null;
   *  insertBottom is a boolean. Both null/false means "regular, no forced spot". */
  _parseManualPlacement(position) {
    if (position == null) return { insertPos: null, insertBottom: false };
    const s = String(position).trim().toLowerCase();
    if (s === '') return { insertPos: null, insertBottom: false };
    if (s === 'top') return { insertPos: 1, insertBottom: false };
    if (s === 'bottom' || s === 'bot' || s === 'end' || s === 'last') {
      return { insertPos: null, insertBottom: true };
    }
    const n = Math.floor(Number(s));
    if (Number.isFinite(n) && n >= 1) return { insertPos: n, insertBottom: false };
    return { insertPos: null, insertBottom: false };
  }

  addManualQueueOrder({ buyer, orderName, position, total, items } = {}) {
    const nm = String(buyer == null ? '' : buyer).trim() || 'Manual add';
    const place = this._parseManualPlacement(position);
    // Optional dollar total → shows in the money column and auto-flags the $1000
    // priority jump (used when the slot flows as a regular/time-based order).
    let amt = Number(total);
    if (!isFinite(amt) || amt < 0) amt = 0;
    amt = Math.round(amt * 100) / 100;
    const id = `manual:${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
    const buyerId = id;
    const parsedItems = this._parseItemsText(items);
    const order = {
      id,
      buyerId,
      buyer: nm,
      buyerHandle: '',
      items: parsedItems,
      total: amt,
      createdAt: Date.now(),
      receivedAt: Date.now(),
      status: 'queued',
      batchKey: `${buyerId}#${++this.batchCounter}`,
      // Keyword priority auto-detected from the items/note; the $1000 jump is
      // derived from `total` in _entryFor (bigDollar). Both only affect the sort
      // when this slot flows as a regular order (no forced top/bottom/N spot).
      hasPriority: this._isPriorityOrder(parsedItems),
      mergedWhileTop: false,
      onHold: false,
      source: '',
      orderName: String(orderName == null ? '' : orderName).trim(),
      fullName: '',
      shipping: null,
      afterTopOf: null,
      manual: true,
      // New overall-position model: insertPos/insertBottom force a true overall
      // spot (spliced in activeQueue). pinnedPos (the old top-block) is left null
      // for new manual adds so "position 5" actually lands at 5, not position 1.
      pinnedPos: null,
      pinnedAt: Date.now(),
      insertPos: place.insertPos,
      insertBottom: place.insertBottom,
    };
    this.orders.set(id, order);
    this._recordVaultOrder(order); // durable vault audit ledger (observational)
    this._markTopReached();
    this._persist();
    const entry = this._entryFor(order.batchKey);
    this.emit('change', { reason: 'manual-queue-add', orderId: id, entry });
    this.emit('new-slot', entry);
    return entry;
  }

  /** Change a manual slot's pinned position in the queue (1 = top, above priority).
   *  Only applies to manually-added slots (they carry pinnedPos). */
  setManualQueuePosition(batchKey, position) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === String(batchKey) && o.status === 'queued' && o.manual,
    );
    if (!orders.length) return false;
    const place = this._parseManualPlacement(position);
    for (const o of orders) {
      o.insertPos = place.insertPos;
      o.insertBottom = place.insertBottom;
      o.pinnedPos = null;      // use the overall-position model, not the old top-block
      o.pinnedAt = Date.now();
      o.bumped = false;
    }
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'manual-queue-position', batchKey: String(batchKey) });
    return true;
  }

  /** Set/clear the order number shown on a manual slot (blank by default). */
  setManualQueueOrderName(batchKey, orderName) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === String(batchKey) && o.status === 'queued' && o.manual,
    );
    if (!orders.length) return false;
    const nm = String(orderName == null ? '' : orderName).trim();
    for (const o of orders) { o.orderName = nm; }
    this._persist();
    this.emit('change', { reason: 'manual-queue-ordername', batchKey: String(batchKey) });
    return true;
  }

  /** Edit the item list/note on a manual slot (free text or [{qty,name}]). */
  setManualQueueItems(batchKey, items) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === String(batchKey) && o.status === 'queued' && o.manual,
    );
    if (!orders.length) return false;
    const parsed = this._parseItemsText(items);
    for (const o of orders) { o.items = parsed; }
    this._persist();
    this.emit('change', { reason: 'manual-queue-items', batchKey: String(batchKey) });
    return true;
  }

  /** Toggle an admin "priority" flag on a manual slot — shows the PRIORITY badge
   *  and sorts it in the priority tier. Independent of the item-keyword and $1000
   *  rules (a manual add has no real items/total). The slot's explicit pinned
   *  position still wins over priority, same as every other pinned slot. */
  setManualQueuePriority(batchKey, on) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === String(batchKey) && o.status === 'queued' && o.manual,
    );
    if (!orders.length) return false;
    const val = !!on;
    for (const o of orders) { o.manualPriority = val; }
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'manual-queue-priority', batchKey: String(batchKey), priority: val });
    return true;
  }

  /** Set the dollar total on a manual slot (what shows in the money column, and
   *  what drives the $1000 priority jump). Manual slots start at $0. */
  setManualQueueTotal(batchKey, total) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === String(batchKey) && o.status === 'queued' && o.manual,
    );
    if (!orders.length) return false;
    let amt = Number(total);
    if (!isFinite(amt) || amt < 0) amt = 0;
    amt = Math.round(amt * 100) / 100;
    // Manual slots are single-order; put it all on the first and zero any others.
    orders[0].total = amt;
    for (let i = 1; i < orders.length; i++) orders[i].total = 0;
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'manual-queue-total', batchKey: String(batchKey), total: amt });
    return true;
  }

  /** Put a slot ON HOLD: move it off the main queue into the held side-list
   *  (e.g. the buyer isn't ready, or you want to rip it later). Held orders keep
   *  their place in time and their reachedTopAt stamp, so returning one never
   *  re-tallies the Vault. */
  holdSlot(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'queued'
    );
    if (!orders.length) return null;
    const buyerId = orders[0].buyerId;
    const now = Date.now();
    for (const o of orders) {
      o.status = 'held';
      o.heldAt = now;
      o.bumped = false;
    }
    // Free the buyer's open slot so later orders don't merge into a held one.
    if (this.openBatch.get(buyerId) === batchKey) this.openBatch.delete(buyerId);
    this.preppedBatches.delete(batchKey);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'held', batchKey, buyerId });
    return { batchKey, buyerId };
  }

  /** Return a held slot to the main queue in its NATURAL position — i.e. sorted
   *  back in by its original order time (and priority), NOT bumped to the top. So
   *  briefly pulling a slot out and putting it back leaves it right where it was
   *  (e.g. #3 out and back in stays #3). Any separate manual bump on another slot
   *  is left untouched. */
  unholdSlot(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'held'
    );
    if (!orders.length) return null;
    const buyerId = orders[0].buyerId;
    for (const o of orders) {
      o.status = 'queued';
      o.bumped = false; // never jump to the top on return — sort by original time
      delete o.heldAt;
    }
    if (!this.openBatch.has(buyerId)) this.openBatch.set(buyerId, batchKey);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'unheld', batchKey, buyerId });
    return { batchKey, buyerId };
  }

  /** Condensed list of held slots for the admin side-panel (most-recent first). */
  heldSlots() {
    const byBatch = new Map();
    for (const o of this.orders.values()) {
      if (o.status !== 'held') continue;
      if (!byBatch.has(o.batchKey)) byBatch.set(o.batchKey, []);
      byBatch.get(o.batchKey).push(o);
    }
    const slots = [...byBatch.entries()].map(([key, orders]) => {
      const first = orders[0];
      return {
        key,
        buyerId: first.buyerId,
        buyer: this._displayName(first.buyerId, first.buyer),
        orderIds: orders.map((o) => o.id),
        orderNames: orders.map((o) => o.orderName || (String(o.id).startsWith('manual:') ? '' : ('#' + String(o.id)))),
        total: orders.reduce((s, o) => s + (Number(o.total) || 0), 0),
        sourceShort: (() => {
          const s = new Set(orders.map((o) => String(o.source || '').toLowerCase()).filter(Boolean));
          if (!s.size) return '';
          if (s.size > 1) return 'MIX';
          const only = [...s][0];
          return only.includes('tiktok') || only === 'tt' ? 'TT' : 'SF';
        })(),
        heldAt: Math.max(...orders.map((o) => o.heldAt || 0)),
      };
    });
    slots.sort((a, b) => (b.heldAt || 0) - (a.heldAt || 0));
    return slots;
  }

  /** Configure which item names/SKUs trigger priority. Recomputes existing orders. */
  setPriorityItems(list) {
    this.priorityItems = (list || [])
      .map((s) => String(s).trim().toLowerCase())
      .filter(Boolean);
    for (const o of this.orders.values()) {
      o.hasPriority = this._isPriorityOrder(o.items);
    }
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'priority-config', priorityItems: this.priorityItems });
  }

  /** Public, serializable view of the current priority settings (for the admin UI). */
  prioritySettingsPublic() {
    return {
      keywords: this.priorityItems,
      moneyThreshold: this.priorityMoneyThreshold,
      keywordsOn: this.priorityKeywordsOn,
      moneyOn: this.priorityMoneyOn,
    };
  }

  /** Update the priority settings (keywords, $ threshold, and each metric's on/off
   *  switch) in one shot, from the admin "Priority Settings" lightbox.
   *
   *  mode === 'apply':    re-evaluate the WHOLE current queue under the new
   *                       settings — promotes items that now qualify, demotes ones
   *                       that no longer do (back to their natural time position).
   *                       All freeze locks are cleared.
   *  mode === 'continue': save the settings but DON'T disturb the current queue —
   *                       every queued order is frozen at its current standing, so
   *                       only future orders are judged by the new settings.
   */
  setPrioritySettings(opts = {}, mode = 'continue') {
    const nextKeywords = Array.isArray(opts.keywords)
      ? opts.keywords.map((s) => String(s).trim().toLowerCase()).filter(Boolean)
      : this.priorityItems;
    let threshold = Number(opts.moneyThreshold);
    if (!isFinite(threshold) || threshold < 0) threshold = this.priorityMoneyThreshold;
    threshold = Math.round(threshold * 100) / 100;
    const kwOn = (opts.keywordsOn != null) ? !!opts.keywordsOn : this.priorityKeywordsOn;
    const moneyOn = (opts.moneyOn != null) ? !!opts.moneyOn : this.priorityMoneyOn;

    if (mode === 'continue') {
      // Freeze every currently-queued order at its CURRENT standing, judged by the
      // settings as they are RIGHT NOW (before we overwrite them below). Orders
      // already frozen keep their existing lock. Money is captured per order (the
      // common iPoke case is one order per slot, so this matches the batch total).
      for (const o of this.orders.values()) {
        if (o.status !== 'queued') continue;
        if (o.priorityLock && typeof o.priorityLock === 'object') continue;
        const kw = this.priorityKeywordsOn && !!o.hasPriority;
        const big = this.priorityMoneyOn && (Number(o.total) || 0) >= this.priorityMoneyThreshold;
        o.priorityLock = { kw, big };
      }
    } else {
      // 'apply' — drop all freezes so the whole queue re-evaluates live.
      for (const o of this.orders.values()) {
        if (o.priorityLock != null) o.priorityLock = null;
      }
    }

    // Commit the new settings.
    this.priorityItems = nextKeywords;
    this.priorityMoneyThreshold = threshold;
    this.priorityKeywordsOn = kwOn;
    this.priorityMoneyOn = moneyOn;
    // Re-stamp keyword matches from the new list. Locked orders ignore this (their
    // frozen bits govern); unlocked + future orders use it.
    for (const o of this.orders.values()) o.hasPriority = this._isPriorityOrder(o.items);

    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'priority-settings', mode });
    return this.prioritySettingsPublic();
  }

  /** Archive the current stream (fulfilled + cancelled + still-unfulfilled) to
   *  history, if it had any activity. Does NOT clear the board — callers do. */
  _archiveCurrentStream() {
    const records = this.fulfilledRecords();
    const cancelledRecs = this.cancelledRecords();
    const unfulfilledRecs = this.queuedRecords();
    if (records.length || cancelledRecs.length || unfulfilledRecs.length) {
      this.history.unshift({
        id: String(this.sessionStartedAt || Date.now()),
        startedAt: this.sessionStartedAt || null,
        endedAt: Date.now(),
        count: records.length,
        value: records.reduce((s, r) => s + r.total, 0),
        unfulfilledCount: unfulfilledRecs.length,
        fulfilled: records,
        cancelled: cancelledRecs,
        unfulfilled: unfulfilledRecs,
      });
      this.history = this.history.slice(0, MAX_HISTORY);
      this._persistHistory();
    }
  }

  /** Start a live: archives anything left from a stream that wasn't ended,
   *  clears the queue, and begins accepting orders. */
  goLive() {
    this._archiveCurrentStream(); // safety net if the previous stream wasn't ended
    this.orders.clear();
    this.openBatch.clear();
    // Per-variant counters PERSIST across streams (cumulative running totals).
    // They are only ever changed by the auto-tally or a manual admin edit/reset.
    this.sessionStartedAt = Date.now();
    this.live = true;
    this._persist();
    this.emit('change', { reason: 'go-live' });
  }

  /** End a live: archive the whole stream (fulfilled, cancelled, and any
   *  still-unfulfilled orders) to Past streams, then clear the board so the
   *  main page empties and everything moves to history. Stops new orders. */
  endLive() {
    this._archiveCurrentStream();
    this.orders.clear();
    this.openBatch.clear();
    this.live = false;
    this._persist();
    this.emit('change', { reason: 'end-live' });
  }

  /**
   * Active queue, top = next to handle:
   *   1. Manually pinned slots (dragged/added to a spot) — a SOLID BLOCK at the very
   *      top, in their relative pinned order. Nothing non-pinned (including priority)
   *      ever sits above them, so fulfilling one just shrinks the block.
   *   2. Then, among the rest: manually bumped slot → priority (earliest priority-item
   *      time) → normal (earliest order time).
   */
  activeQueue() {
    const keys = new Set(
      [...this.orders.values()].filter((o) => o.status === 'queued').map((o) => o.batchKey)
    );
    const entries = [...keys].map((k) => this._entryFor(k)).filter(Boolean);
    // Manual adds placed at an explicit OVERALL spot (top / bottom / position N)
    // are set aside here and spliced into the final list below — so a lone manual
    // add at "position 5" actually lands at 5, instead of being forced to the top.
    const inserts = entries.filter((e) => e.insertPos != null || e.insertBottom);
    const normal = entries.filter((e) => !(e.insertPos != null || e.insertBottom));
    const pinned = normal.filter((e) => e.pinnedPos != null && !e.bumped);
    const rest = normal.filter((e) => e.pinnedPos == null || e.bumped);
    // A slot that's been waiting more than 14h ranks ABOVE incoming priority — so a
    // fresh priority order can't keep burying someone who's already waited that long.
    const now = Date.now();
    const isAged = (e) => (now - (e.firstOrderAt || now)) > AGED_QUEUE_MS;
    rest.sort((a, b) => {
      if (a.bumped !== b.bumped) return a.bumped ? -1 : 1;          // manual bump always first
      const aa = isAged(a), ab = isAged(b);
      if (aa !== ab) return aa ? -1 : 1;                            // 14h+ waiters next
      if (aa && ab) return a.firstOrderAt - b.firstOrderAt;         // among aged: longest wait first
      if (a.isPriority !== b.isPriority) return a.isPriority ? -1 : 1; // then priority
      if (a.isPriority) return a.priorityAt - b.priorityAt;        //   priority tier by time
      return a.firstOrderAt - b.firstOrderAt;                       // then normal by time
    });
    // Pinned slots keep their relative order (lower pinnedPos first; ties by pin time)
    // and sit as one contiguous block at the TOP, above every non-pinned slot. This
    // is what keeps a priority order from jumping above the manually-placed ones when
    // one of them is fulfilled — the block simply gets shorter. A later drag above an
    // existing pin re-numbers the pins (see reorderQueueSelection), so "drag above"
    // still works.
    pinned.sort((a, b) => ((a.pinnedPos || 1) - (b.pinnedPos || 1)) || ((a.pinnedAt || 0) - (b.pinnedAt || 0)));
    const list = [...pinned, ...rest];
    // Splice the forced-spot manual adds into the natural list. Position-N (and
    // "top" = 1) inserts go in first, in ascending target order so earlier targets
    // aren't shifted by later ones; ties break by pin time. "Bottom" inserts are
    // appended last so they always sit at the very end. Each render re-splices at
    // the same target index, so a forced slot holds its spot as the queue churns.
    const positioned = inserts
      .filter((e) => !e.insertBottom)
      .sort((a, b) => ((a.insertPos || 1) - (b.insertPos || 1)) || ((a.pinnedAt || 0) - (b.pinnedAt || 0)));
    const bottoms = inserts
      .filter((e) => e.insertBottom)
      .sort((a, b) => (a.pinnedAt || 0) - (b.pinnedAt || 0));
    for (const e of positioned) {
      let idx = Math.floor(Number(e.insertPos) || 1) - 1;
      if (idx < 0) idx = 0;
      if (idx > list.length) idx = list.length;
      list.splice(idx, 0, e);
    }
    for (const e of bottoms) list.push(e);
    return list.map((e, i) => ({ ...e, position: i + 1 }));
  }

  /** Manually drag a selection of slots to a new position in the queue.
   *  `keys` are the batchKeys being moved (kept in their current relative order);
   *  `beforeKey` is the slot to drop them in front of ('__top__' = very top,
   *  null/'' = bottom). The moved slots become PINNED to their new absolute
   *  positions, so they hold their spot regardless of priority or incoming orders —
   *  exactly like the manual-add pins — until another slot is dragged above them.
   *  Every already-pinned slot is renumbered too, so a later drag can slot in above
   *  earlier pins. */
  reorderQueueSelection(keys, beforeKey) {
    const sel = new Set((keys || []).map(String));
    if (!sel.size) return false;
    const current = this.activeQueue();               // full ordered list (all pages)
    const moving = current.filter((e) => sel.has(String(e.key)));
    if (!moving.length) return false;
    const others = current.filter((e) => !sel.has(String(e.key)));
    let idx;
    if (beforeKey === '__top__') idx = 0;
    else if (beforeKey == null || beforeKey === '' || beforeKey === '__bottom__') idx = others.length;
    else {
      idx = others.findIndex((e) => String(e.key) === String(beforeKey));
      if (idx < 0) idx = others.length;               // target was itself moved / gone
    }
    const newOrder = [...others.slice(0, idx), ...moving, ...others.slice(idx)];
    // Pinned set = everything already pinned (keep its spot) plus the moved slots.
    const pinnedSet = new Set();
    for (const e of newOrder) {
      if (sel.has(String(e.key)) || e.pinnedPos != null) pinnedSet.add(String(e.key));
    }
    // Assign pinnedPos = 1-based index in the new arrangement. Non-pinned slots keep
    // flowing (priority/time) into the gaps. Clear `bumped` on moved slots so the pin
    // — not a stale bump — controls their placement.
    const now = Date.now();
    newOrder.forEach((e, i) => {
      if (!pinnedSet.has(String(e.key))) return;
      const orders = [...this.orders.values()].filter(
        (o) => o.batchKey === String(e.key) && o.status === 'queued',
      );
      for (const o of orders) {
        o.pinnedPos = i + 1;
        o.pinnedAt = now;
        if (sel.has(String(e.key))) o.bumped = false;
      }
    });
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'queue-reorder', count: moving.length });
    return true;
  }

  /** Revert a manually-placed slot to its natural position: drop its pin (and any
   *  bump) so it flows back in by priority/time like a normal order. */
  unpinQueueSlot(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === String(batchKey) && o.status === 'queued' &&
        (o.pinnedPos != null || o.bumped || o.insertPos != null || o.insertBottom),
    );
    if (!orders.length) return false;
    for (const o of orders) {
      o.pinnedPos = null; o.pinnedAt = null; o.bumped = false;
      o.insertPos = null; o.insertBottom = false;
    }
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'queue-unpin', batchKey: String(batchKey) });
    return true;
  }

  fulfilledSlots() {
    const byBatch = new Map();
    for (const o of this.orders.values()) {
      if (o.status !== 'fulfilled') continue;
      if (!byBatch.has(o.batchKey)) byBatch.set(o.batchKey, []);
      byBatch.get(o.batchKey).push(o);
    }
    const slots = [...byBatch.entries()].map(([key, orders]) => {
      const first = orders[0];
      const reachedAts = orders.map((o) => o.reachedTopAt).filter(Boolean);
      return {
        key,
        buyer: this._displayName(first.buyerId, first.buyer),
        buyerId: first.buyerId,
        orderIds: orders.map((o) => o.id),
        orderNames: orders.map((o) => o.orderName || (String(o.id).startsWith('manual:') ? '' : ('#' + String(o.id)))),
        itemCount: orders.reduce((n, o) => n + o.items.reduce((m, i) => m + (i.qty || 1), 0), 0),
        total: orders.reduce((s, o) => s + o.total, 0),
        fulfilledAt: Math.max(...orders.map((o) => o.fulfilledAt || 0)),
        // Timing fields for the queue metrics.
        firstOrderAt: Math.min(...orders.map((o) => o.createdAt || o.fulfilledAt || 0)),
        reachedTopAt: reachedAts.length ? Math.min(...reachedAts) : null,
      };
    });
    slots.sort((a, b) => b.fulfilledAt - a.fulfilledAt);
    return slots;
  }

  stats() {
    const active = this.activeQueue();
    const done = this.fulfilledSlots();
    // Average time-in-queue (placed -> fulfilled) and fulfillment time (reached
    // top -> fulfilled) across everything fulfilled this stream.
    const inQueue = done
      .map((s) => (s.fulfilledAt && s.firstOrderAt) ? s.fulfilledAt - s.firstOrderAt : null)
      .filter((v) => v != null && v >= 0);
    const atTop = done
      .map((s) => (s.fulfilledAt && s.reachedTopAt) ? s.fulfilledAt - s.reachedTopAt : null)
      .filter((v) => v != null && v >= 0);
    const avg = (a) => (a.length ? Math.round(a.reduce((x, y) => x + y, 0) / a.length) : null);
    return {
      activeCount: active.length,
      priorityCount: active.filter((e) => e.isPriority).length,
      heldCount: this.heldSlots().length,
      fulfilledCount: done.length,
      activeValue: active.reduce((s, e) => s + e.total, 0),
      priorityItems: this.priorityItems,
      live: this.live,
      // Queue timing metrics (admin only). Milliseconds; null when no data yet.
      avgTimeInQueueMs: avg(inQueue),
      avgFulfillmentMs: avg(atTop),
      timedCount: inQueue.length,
    };
  }

  snapshot() {
    return {
      queue: this.activeQueue(),
      held: this.heldSlots(),
      fulfilled: this.fulfilledSlots().slice(0, 50),
      cancelled: this.cancelledSlots().slice(0, 50),
      stats: this.stats(),
      config: { priorityItems: this.priorityItems, priority: this.prioritySettingsPublic() },
      variants: this.trackedVariants.map((v) => ({
        id: v.id, label: v.label, product: v.product, variant: v.variant,
        chase: v.chase || '',
        count: this.variantCounts[v.id] || 0,
      })),
      variantLog: this.variantLog.slice(0, 50),
      vaultTallyLog: this.vaultTallyLog.slice(0, 50),
      // Lean vault reconciliation summary (per-vault counted vs expected + miss
      // count). Full per-order detail is served separately via /api/vault-ledger.
      vaultAudit: this._vaultAuditSummary(),
      history: this.history.map((s) => ({
        id: s.id,
        label: s.label || null,
        startedAt: s.startedAt,
        endedAt: s.endedAt,
        count: s.count,
        value: s.value,
        unfulfilledCount: s.unfulfilledCount || 0,
      })),
      // Tracker (Piggy Bank / Vault) display labels — configurable per store.
      tracker: { title: this.trackerTitle, subtitle: this.trackerSubtitle },
      // ── iPoke additions (empty/default for the other stores) ──
      perpetual: this.perpetual,
      overlays: this.overlays,
      combine: {
        mode: this.combineMode,
        windowMs: this.combineWindowMs,
        windowMinutes: Math.round(this.combineWindowMs / 60000),
      },
      events: this.eventsSummary(),
      activeEventId: this.activeEventId,
      activeEvent: this.activeEventId ? this.eventQueue(this.activeEventId) : null,
      // Ship-Sealed internal queue (never surfaced on public/overlay views).
      sealed: this.sealedQueue(),
      sealedDone: this.sealedDoneCount(),
      sealedFulfilled: this.sealedFulfilled(),
    };
  }

  reset() {
    this.orders.clear();
    this.openBatch.clear();
    this._persist();
    this.emit('change', { reason: 'reset' });
  }

  /** Remove a whole slot from the queue WITHOUT fulfilling it (e.g. the buyer
   *  cancelled). Marked 'cancelled' so it drops out of the active queue and the
   *  fulfilled list, and shows in the Cancelled section for tracking. */
  removeSlot(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'queued'
    );
    if (!orders.length) return null;
    const buyerId = orders[0].buyerId;
    const now = Date.now();
    for (const o of orders) { o.status = 'cancelled'; o.cancelledAt = now; o.bumped = false; this._recordVaultOrder(o); }
    if (this.openBatch.get(buyerId) === batchKey) this.openBatch.delete(buyerId);
    this.preppedBatches.delete(batchKey);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'cancelled', batchKey });
    return { batchKey };
  }

  /** Cancel a SINGLE order by its order id (used by the auto re-check when an
   *  order flips to CANCELLED on TikTok). Leaves any other orders in the same
   *  buyer's slot untouched. */
  cancelOrder(orderId) {
    const id = String(orderId);
    // A full-order cancel on Shopify cancels every part of the order — including
    // its Ship-Sealed slot (which may be the only part) and any event/quack seats
    // that order bought.
    const sealedCancelled = this.removeSealedOrder(id);
    // Cancel every still-queued event/quack seat tied to this order id. (One order
    // can hold more than one seat, and an order can be ONLY event seats with no
    // regular/sealed part — in which case this is the only thing that fires.)
    let eventCancelled = 0;
    const nowC = Date.now();
    for (const e of this.eventEntries.values()) {
      if (String(e.orderId) === id && e.status === 'queued') {
        e.status = 'cancelled';
        e.cancelledAt = nowC;
        eventCancelled++;
      }
    }
    if (eventCancelled) this.emit('change', { reason: 'event-entry-cancelled', orderId: id });
    const o = this.orders.get(id);
    if (!o || o.status !== 'queued') {
      if (sealedCancelled || eventCancelled) { this._persist(); return { id, sealedOnly: !eventCancelled, eventCancelled }; }
      return null;
    }
    o.status = 'cancelled';
    o.cancelledAt = Date.now();
    o.bumped = false;
    this._recordVaultOrder(o); // refresh ledger status (→ cancelled)
    // If the buyer's open slot has no queued orders left, close it.
    const stillOpen = [...this.orders.values()].some(
      (x) => x.batchKey === o.batchKey && x.status === 'queued'
    );
    if (!stillOpen && this.openBatch.get(o.buyerId) === o.batchKey) this.openBatch.delete(o.buyerId);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'cancelled', orderId: id });
    return o;
  }

  // ── Ship-Sealed side-queue (iPoke, internal pull/pack/ship) ────────────────
  /** True if a line item belongs in the Ship-Sealed queue. Matches "ship sealed"
   *  AND "shipped sealed" (the store's actual wording), tolerant of a space,
   *  hyphen or underscore between the words. Case-insensitive. */
  _isSealedItem(it) {
    // TikTok giveaway items (tagged on ingest: the $0 Penny Sleeves Pro Support
    // accessory) are RIPPED LIVE, not shipped sealed — even though their title
    // literally says "Shipped Sealed". The giveaway tag overrides the name match
    // so these route to the rip-live active queue under "TikTok Giveaway".
    if (it && it.giveaway) return false;
    return /ship(?:ped)?[\s\-_]*sealed/i.test(String((it && it.name) || ''));
  }

  /** True if a line item is a Shopify checkout "Tip" line (exact title), which we
   *  never pull/pack. Mirror of the ingest-side filter for the retroactive sweep. */
  _isTipItem(it) {
    return String((it && it.name) == null ? '' : (it && it.name)).trim().toLowerCase() === 'tip';
  }

  /** Retroactively pull "Ship Sealed" items out of orders ALREADY in the Rip-Live
   *  queue into the Ship-Sealed queue. Covers orders that were ingested before the
   *  Ship-Sealed split existed (re-ingest dedups, so they'd otherwise be stranded)
   *  and any that slipped through. Runs on boot and each poll. Returns how many
   *  orders had sealed items split out. */
  sweepQueuedIntoSealed() {
    let moved = 0, changed = 0;
    const queued = [...this.orders.values()].filter((o) => o.status === 'queued');
    for (const o of queued) {
      // Never pull a Ship-Sealed item back out of an order that was DELIBERATELY
      // moved into Rip-Live (buyer changed their mind and wants it ripped live) —
      // otherwise this sweep would drag it straight back to the sealed queue.
      if (o.movedFromSealed) continue;
      const items = o.items || [];
      const sealedItems = items.filter((it) => this._isSealedItem(it));
      const hasTip = items.some((it) => this._isTipItem(it));
      if (!sealedItems.length && !hasTip) continue;
      if (sealedItems.length) {
        this._addSealedSlot(o.id, {
          buyerId: o.buyerId, buyer: o.buyer, buyerHandle: o.buyerHandle,
          total: o.total, createdAt: o.createdAt, source: o.source, orderName: o.orderName,
          fullName: o.fullName, shipping: o.shipping,
        }, sealedItems);
        moved++;
      }
      // Keep only the Rip-Live items: drop sealed (moved out) and tip (ignored).
      const remaining = items.filter((it) => !this._isSealedItem(it) && !this._isTipItem(it));
      if (remaining.length) {
        o.items = remaining;
        o.hasPriority = this._isPriorityOrder(remaining);
      } else {
        // Nothing left for Rip-Live (whole order was sealed and/or just a tip).
        this.orders.delete(o.id);
        if (this.openBatch.get(o.buyerId) === o.batchKey) this.openBatch.delete(o.buyerId);
      }
      changed++;
    }
    if (changed) { this._markTopReached(); this._persist(); this.emit('change', { reason: 'sealed-sweep', moved }); }
    return moved;
  }

  /** Add (or refresh) an order's Ship-Sealed slot. One record per order id — no
   *  combining, since each order is pulled, packed and shipped on its own. The
   *  record carries the FULL order total so the $1000 priority applies here too. */
  /** Merchandise value of just the sealed line items (after line discounts), so a
   *  split order shows the Ship-Sealed portion rather than the whole order total.
   *  Returns null when the line items carry no price data (older orders / non-
   *  Shopify sources) so callers can fall back to the full order total. */
  _sealedItemsValue(items) {
    const list = items || [];
    if (!list.some((it) => Number(it.lineTotal) > 0)) return null;
    const sum = list.reduce((s, it) => s + (Number(it.lineTotal) || 0), 0);
    return Math.round(sum * 100) / 100;
  }

  _addSealedSlot(id, raw, items) {
    const sid = String(id);
    const existing = this.sealedOrders.get(sid);
    if (existing) {
      existing.items = items;
      existing.total = Number(raw.total != null ? raw.total : existing.total) || 0;
      const sv = this._sealedItemsValue(items);
      if (sv != null) existing.sealedTotal = sv;
      if (raw.buyer) existing.buyer = raw.buyer;
      if (raw.orderName) existing.orderName = raw.orderName;
      return existing;
    }
    const rec = {
      id: sid,
      buyerId: String(raw.buyerId),
      buyer: raw.buyer || 'Buyer',
      buyerHandle: raw.buyerHandle || '',
      items,
      total: Number(raw.total || 0),
      // Value of just these sealed items (null when unknown → UI falls back to total).
      sealedTotal: this._sealedItemsValue(items),
      createdAt: raw.createdAt || Date.now(),
      receivedAt: Date.now(),
      status: 'queued',
      source: raw.source || '',
      orderName: raw.orderName || '',
      fullName: raw.fullName || '',
      shipping: raw.shipping || null,
    };
    this.sealedOrders.set(sid, rec);
    this.seenEventOrders.add(sid);
    return rec;
  }

  /** The internal Ship-Sealed queue: a pure pull/pack list sorted ONLY by time
   *  received — NEWEST on top, oldest on the bottom. Unlike the Rip-Live queue it
   *  deliberately ignores priority / $-threshold / keywords, so nothing ever jumps
   *  the line here. Queued records only (packed/shipped ones drop off). */
  sealedQueue() {
    const list = [...this.sealedOrders.values()].filter((o) => o.status === 'queued');
    // Newest received first (descending by order time).
    list.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
    return list.map((o, i) => ({
      id: o.id,
      position: i + 1,
      buyerId: o.buyerId,
      buyer: this._displayName(o.buyerId, o.buyer),
      items: o.items || [],
      itemCount: (o.items || []).reduce((n, it) => n + (Number(it.qty) || 1), 0),
      total: Number(o.total) || 0,
      // Value of just the sealed items (for the money column); null → UI shows total.
      sealedTotal: (o.sealedTotal != null ? Number(o.sealedTotal) : null),
      source: o.source || '',
      orderName: o.orderName || '',
      createdAt: o.createdAt || null,
      // Ship-Sealed never uses priority — it's a straight time-ordered list.
      isPriority: false,
      fullName: o.fullName || '',
      shipping: o.shipping || null,
      // Parity with Rip-Live slots so the Ship-Sealed label can carry the same
      // variables: the Vault "V" tag, and the "earlier orders today" heads-up
      // (count of this buyer's already-fulfilled Rip-Live orders).
      hasVault: this._orderHasVault({ items: o.items || [] }),
      priorFulfilled: [...this.orders.values()].filter(
        (x) => x.buyerId === o.buyerId && x.status === 'fulfilled',
      ).length,
    }));
  }

  /** How many Ship-Sealed orders were packed/shipped TODAY (Pacific day). Scoped
   *  to today so it resets on the same daily rollover as the main queue's
   *  "Fulfilled today" — older shipped records are retained internally but not
   *  counted here. */
  sealedDoneCount() {
    const today = this._laDateString();
    let n = 0;
    for (const o of this.sealedOrders.values()) {
      if (o.status === 'fulfilled' && o.fulfilledAt && this._laDateString(o.fulfilledAt) === today) n++;
    }
    return n;
  }

  /** The Ship-Sealed orders packed/shipped TODAY (Pacific day), newest first —
   *  powers the "Fulfilled Today — Ship Sealed" list with per-row undo. */
  sealedFulfilled() {
    const today = this._laDateString();
    return [...this.sealedOrders.values()]
      .filter((o) => o.status === 'fulfilled' && o.fulfilledAt && this._laDateString(o.fulfilledAt) === today)
      .sort((a, b) => (b.fulfilledAt || 0) - (a.fulfilledAt || 0))
      .map((o) => ({
        id: o.id,
        buyer: this._displayName(o.buyerId, o.buyer),
        orderName: o.orderName || '',
        itemCount: (o.items || []).reduce((n, it) => n + (Number(it.qty) || 1), 0),
        total: Number(o.total) || 0,
        fulfilledAt: o.fulfilledAt || null,
      }));
  }

  /** Mark a Ship-Sealed order packed/shipped (or undo). */
  markSealedFulfilled(id, on = true) {
    const o = this.sealedOrders.get(String(id));
    if (!o) return false;
    if (on) { if (o.status !== 'queued') return false; o.status = 'fulfilled'; o.fulfilledAt = Date.now(); }
    else { if (o.status !== 'fulfilled') return false; o.status = 'queued'; delete o.fulfilledAt; }
    this._persist();
    this.emit('change', { reason: 'sealed-fulfilled', orderId: String(id), on: !!on });
    return true;
  }

  /** Remove a Ship-Sealed order (cancelled/refunded). */
  removeSealedOrder(id) {
    const o = this.sealedOrders.get(String(id));
    if (!o || o.status === 'cancelled') return false;
    o.status = 'cancelled';
    o.cancelledAt = Date.now();
    this._persist();
    this.emit('change', { reason: 'sealed-cancelled', orderId: String(id) });
    return true;
  }

  /** Move a single line item OUT of a Ship-Sealed order and INTO the Rip-Live queue
   *  (buyer changed their mind and wants it ripped live). Creates its own standalone
   *  Rip-Live slot and removes that item from the sealed order; if the sealed order
   *  has no items left it drops off the Ship-Sealed queue. */
  moveSealedItemToLive(sealedId, itemIndex) {
    const o = this.sealedOrders.get(String(sealedId));
    if (!o || o.status !== 'queued') return false;
    const items = o.items || [];
    const idx = Number(itemIndex);
    if (!Number.isInteger(idx) || idx < 0 || idx >= items.length) return false;
    const item = items[idx];
    const buyerId = String(o.buyerId);
    // Unique id so it never collides with an existing main order of the same order id
    // (an order can have both a Rip-Live and a Ship-Sealed part sharing one id).
    const liveId = `${o.id}~rip${++this.batchCounter}`;
    const batchKey = `${buyerId}#${++this.batchCounter}`;
    const order = {
      id: liveId,
      buyerId,
      buyer: o.buyer || 'Buyer',
      buyerHandle: o.buyerHandle || '',
      items: [item],
      // Carry the order's dollar value onto the ripped-live slot so it shows the
      // right $ and keeps any value-based ($1000+) queue priority. The value is
      // MOVED off the sealed order below (not copied), so revenue never double-counts.
      total: Number(o.total) || 0,
      createdAt: Date.now(),
      receivedAt: Date.now(),
      status: 'queued',
      batchKey,
      hasPriority: this._isPriorityOrder([item]),
      mergedWhileTop: false,
      onHold: false,
      source: o.source || '',
      orderName: o.orderName || '',
      fullName: o.fullName || '',
      shipping: o.shipping || null,
      afterTopOf: null,
      movedFromSealed: true, // provenance flag (admin can see it came from Ship-Sealed)
      // Treat it like a manual slot so its queue position is editable (and so the
      // sealed-sweep leaves it alone). Not pinned by default — it enters at its
      // natural spot and the admin can set a position with the ⇅ Pos control.
      manual: true,
      pinnedPos: null,
      pinnedAt: Date.now(),
    };
    this.orders.set(liveId, order);
    // The dollar value moved with the ripped item — zero it on the sealed side so the
    // same money is never counted twice (on a partial move the remaining sealed items
    // then show $0; on a whole-order move the sealed order drops off entirely below).
    o.total = 0;
    // Remove the item from the sealed order; drop the sealed slot if nothing's left.
    items.splice(idx, 1);
    if (!items.length) { o.status = 'moved'; o.movedAt = Date.now(); }
    this._markTopReached();
    this._persist();
    const entry = this._entryFor(batchKey);
    this.emit('change', { reason: 'sealed-to-live', sealedId: String(sealedId), orderId: liveId, entry });
    this.emit('new-slot', entry);
    return true;
  }

  /** Cancelled slots (grouped by buyer batch) for the admin Cancelled section. */
  cancelledSlots() {
    const byBatch = new Map();
    for (const o of this.orders.values()) {
      if (o.status !== 'cancelled') continue;
      if (!byBatch.has(o.batchKey)) byBatch.set(o.batchKey, []);
      byBatch.get(o.batchKey).push(o);
    }
    const slots = [...byBatch.entries()].map(([key, orders]) => {
      const first = orders[0];
      return {
        key,
        buyer: this._displayName(first.buyerId, first.buyer),
        buyerId: first.buyerId,
        orderIds: orders.map((o) => o.id),
        orderNames: orders.map((o) => o.orderName || (String(o.id).startsWith('manual:') ? '' : ('#' + String(o.id)))),
        itemCount: orders.reduce((n, o) => n + o.items.reduce((m, i) => m + (i.qty || 1), 0), 0),
        total: orders.reduce((s, o) => s + o.total, 0),
        cancelledAt: Math.max(...orders.map((o) => o.cancelledAt || 0)),
      };
    });
    slots.sort((a, b) => b.cancelledAt - a.cancelledAt);
    return slots;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // iPoke: order combining
  // ─────────────────────────────────────────────────────────────────────────

  /** Has this slot ever reached the top of the queue? (Any of its queued orders
   *  carries a reachedTopAt stamp.) Used by 'untiltop' to lock a slot once the
   *  bag is likely being packed. */
  _batchReachedTop(batchKey) {
    return [...this.orders.values()].some(
      (o) => o.batchKey === batchKey && o.status === 'queued' && o.reachedTopAt
    );
  }

  /** Can a new order (createdAt) merge into the given open slot under the
   *  current combine mode? 'always' → yes; 'untiltop' → only if the slot hasn't
   *  hit the top yet; 'time' → only within the window from the slot's FIRST
   *  order; 'off' → never (handled before this is called). */
  _canMergeInto(batchKey, newCreatedAt) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'queued'
    );
    if (!orders.length) return false;
    if (this.combineMode === 'always') return true;
    if (this.combineMode === 'untiltop') return !this._batchReachedTop(batchKey);
    if (this.combineMode === 'time') {
      const firstAt = Math.min(...orders.map((o) => o.createdAt || 0));
      return (newCreatedAt - firstAt) <= this.combineWindowMs;
    }
    return false;
  }

  /** Set the combine mode (and optional window in minutes). NOT retroactive:
   *  existing slots are left as-is; the change applies to future orders. */
  /** Un-combine: give every queued order its own slot again, undoing any slots
   *  that hold more than one order. Called when auto-combine is switched OFF so
   *  previously-merged orders each show as their own position. Returns how many
   *  orders were split out. */
  _splitCombinedSlots() {
    const byBatch = new Map();
    for (const o of this.orders.values()) {
      if (o.status !== 'queued') continue;
      if (!byBatch.has(o.batchKey)) byBatch.set(o.batchKey, []);
      byBatch.get(o.batchKey).push(o);
    }
    let split = 0;
    for (const [, orders] of byBatch) {
      if (orders.length <= 1) continue;
      orders.sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
      // Earliest order keeps the existing slot; each later one gets a fresh slot.
      for (let i = 1; i < orders.length; i++) {
        orders[i].batchKey = `${orders[i].buyerId}#${++this.batchCounter}`;
        orders[i].mergedWhileTop = false;
        split++;
      }
    }
    this.openBatch.clear(); // nothing stays "open" to merge into once split
    if (split) { this._markTopReached(); this._persist(); this.emit('change', { reason: 'uncombine' }); }
    return split;
  }

  setCombineMode(mode, windowMinutes) {
    if (!COMBINE_MODES.has(mode)) return false;
    this.combineMode = mode;
    const w = Number(windowMinutes);
    if (Number.isFinite(w) && w > 0) this.combineWindowMs = Math.round(w * 60 * 1000);
    // Turning auto-combine OFF un-combines any currently-merged slots.
    if (mode === 'off') this._splitCombinedSlots();
    this._persist();
    this.emit('change', { reason: 'combine-config', combineMode: this.combineMode, combineWindowMs: this.combineWindowMs });
    return true;
  }

  /** Manually merge two or more slots (same buyer) into one, keeping the
   *  earliest slot's position. Works regardless of combine mode — the escape
   *  hatch for profile mismatches or a moderator's judgement call. */
  combineSlots(batchKeys) {
    const keys = [...new Set((batchKeys || []).map(String))];
    if (keys.length < 2) return null;
    const groups = keys
      .map((k) => ({ k, orders: [...this.orders.values()].filter((o) => o.batchKey === k && o.status === 'queued') }))
      .filter((g) => g.orders.length);
    if (groups.length < 2) return null;
    // Manual combine is name-based: the admin explicitly picks the slots, and the
    // UI only offers slots that share a buyer name, so combining across distinct
    // buyerIds is allowed as long as every selected slot resolves to the same
    // display name (case/space-insensitive). This lets one person who checked out
    // under two different accounts/names be merged into a single slot.
    const norm = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, ' ');
    const names = new Set(
      groups.flatMap((g) => g.orders.map((o) => norm(this._displayName(o.buyerId, o.buyer)))),
    );
    if (names.size !== 1) return null; // refuse combines across different names
    // Target = the slot with the earliest first order (keeps its place in line).
    let target = groups[0];
    let targetFirst = Math.min(...groups[0].orders.map((o) => o.createdAt || 0));
    for (const g of groups) {
      const f = Math.min(...g.orders.map((o) => o.createdAt || 0));
      if (f < targetFirst) { target = g; targetFirst = f; }
    }
    const targetKey = target.k;
    const targetBuyerId = target.orders[0] && target.orders[0].buyerId;
    for (const g of groups) {
      if (g.k === targetKey) continue;
      for (const o of g.orders) {
        // Clear any open-batch pointer for THIS order's buyer before re-homing it,
        // then move it onto the target slot.
        if (this.openBatch.get(o.buyerId) === g.k) this.openBatch.delete(o.buyerId);
        o.batchKey = targetKey;
      }
      if (this.openBatch.get(g.k) != null) this.openBatch.delete(g.k);
      this.preppedBatches.delete(g.k);
    }
    if (this.combineMode !== 'off' && targetBuyerId != null) this.openBatch.set(targetBuyerId, targetKey);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'manual-combine', batchKey: targetKey });
    return { batchKey: targetKey };
  }

  // ─────────────────────────────────────────────────────────────────────────
  // iPoke: buyer name overrides
  // ─────────────────────────────────────────────────────────────────────────

  /** Set (or, with an empty value, clear) a free-text name override for a buyer.
   *  Persists until removed; propagates to every surface via _displayName. */
  setNameOverride(buyerId, name) {
    const id = String(buyerId);
    const v = String(name || '').trim();
    if (v) this.nameOverrides[id] = v;
    else delete this.nameOverrides[id];
    this._persist();
    this.emit('change', { reason: 'name-override', buyerId: id, name: v });
    return true;
  }

  clearNameOverride(buyerId) { return this.setNameOverride(buyerId, ''); }

  /** Live per-overlay display setting (which = 'queue' | 'vault'), controlled
   *  from the admin panel and pushed to the OBS overlays in real time. Accepts
   *  any of { opacity 0..1, scale 0.5..3, panel bool }. Text is never affected. */
  setOverlaySetting(which, patch) {
    const s = this.overlays[which];
    if (!s || !patch || typeof patch !== 'object') return false;
    if (patch.opacity != null && Number.isFinite(Number(patch.opacity))) s.opacity = Math.max(0, Math.min(1, Number(patch.opacity)));
    if (patch.scale != null && Number.isFinite(Number(patch.scale))) s.scale = Math.max(0.5, Math.min(3, Number(patch.scale)));
    if (patch.panel != null) s.panel = !!patch.panel;
    // Reel auto-scroll speed (px/sec) — only meaningful for the queue overlay in
    // ?reel=1 mode, but stored generically so the admin slider can push it live.
    if (patch.speed != null && Number.isFinite(Number(patch.speed))) s.speed = Math.max(2, Math.min(200, Number(patch.speed)));
    // Optional custom colours (hex like #ff3b3b or #f33; '' clears → overlay default).
    const isHex = (v) => typeof v === 'string' && /^#?[0-9a-fA-F]{3}([0-9a-fA-F]{3})?$/.test(v.trim());
    if (patch.textColor != null) s.textColor = isHex(patch.textColor) ? patch.textColor.trim() : '';
    if (patch.bgColor != null) s.bgColor = isHex(patch.bgColor) ? patch.bgColor.trim() : '';
    this._persist();
    this.emit('change', { reason: 'overlay-setting', which });
    return true;
  }

  // ── Daily "Past Days" rollover (perpetual queue) ──────────────────────────
  _laDateString(ts) {
    return new Date(ts || Date.now()).toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });
  }
  _prettyDay(mdY) {
    const d = new Date(String(mdY) + ' 12:00:00');
    if (isNaN(d.getTime())) return String(mdY);
    return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
  }
  // Pacific calendar parts (year/month/day, weekday 0=Sun..6=Sat, hour 0-23).
  _laParts(ts) {
    const parts = Object.fromEntries(
      new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/Los_Angeles',
        year: 'numeric', month: 'numeric', day: 'numeric',
        weekday: 'short', hour: 'numeric', hour12: false,
      }).formatToParts(new Date(ts || Date.now())).map((p) => [p.type, p.value])
    );
    const wd = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 }[parts.weekday];
    return { y: +parts.year, m: +parts.month, d: +parts.day, weekday: wd, hour: (+parts.hour) % 24 };
  }
  /** The auto-archive runs on a stream-day cycle, NOT at plain midnight:
   *    • Mon/Tue/Wed/Thu streams archive at 3:00 AM Pacific the NEXT morning
   *      (so late-night fulfillment after midnight still lands in that day).
   *    • The Friday stream waits and archives at 12:00 AM Pacific Sunday
   *      (Fri + Sat are held together through the weekend).
   *  This returns a stable date label for the stream day the given moment belongs
   *  to; it only changes when a cycle boundary (a trigger) is crossed. Days with
   *  no stream (Sun before Mon's cycle, Sat inside Fri's cycle) fold into the
   *  neighbouring stream day's bucket. */
  _currentStreamDay(ts) {
    const p = this._laParts(ts);
    // Offset (in days) from today's Pacific date to the cycle's stream day.
    let off;
    switch (p.weekday) {
      case 0: off = 1; break;                 // Sun  → folds into Mon's cycle (ends Tue 3am)
      case 1: off = 0; break;                 // Mon
      case 2: off = p.hour < 3 ? -1 : 0; break; // Tue (before 3am still Mon's cycle)
      case 3: off = p.hour < 3 ? -1 : 0; break; // Wed (before 3am still Tue's)
      case 4: off = p.hour < 3 ? -1 : 0; break; // Thu (before 3am still Wed's)
      case 5: off = p.hour < 3 ? -1 : 0; break; // Fri (before 3am still Thu's)
      case 6: off = -1; break;                // Sat  → folds into Fri's cycle (ends Sun 12am)
      default: off = 0;
    }
    // Build the stream-day label (M/D/YYYY, matching _laDateString) via UTC noon so
    // the ±day shift is DST-safe.
    const base = new Date(Date.UTC(p.y, p.m - 1, p.d, 12));
    base.setUTCDate(base.getUTCDate() + off);
    return `${base.getUTCMonth() + 1}/${base.getUTCDate()}/${base.getUTCFullYear()}`;
  }
  /** Called on boot and every minute. When the stream-day cycle rolls over (see
   *  _currentStreamDay for the Mon–Thu 3am / Fri→Sun-midnight schedule), archive
   *  the finished cycle's fulfilled + cancelled orders into Past Days and clear
   *  them, LEAVING any still-queued (unfulfilled) orders on the board. */
  maybeDailyRollover() {
    const cur = this._currentStreamDay();
    if (!this.lastRolloverDay) { this.lastRolloverDay = cur; this._persist(); return false; }
    if (cur === this.lastRolloverDay) return false;
    this._rolloverDay(this._prettyDay(this.lastRolloverDay));
    this.lastRolloverDay = cur;
    this._persist();
    return true;
  }
  _rolloverDay(label) {
    const fulfilled = this.fulfilledRecords();
    const cancelled = this.cancelledRecords();
    if (fulfilled.length || cancelled.length) {
      this.history.unshift({
        id: String(Date.now()),
        label,
        startedAt: null,
        endedAt: Date.now(),
        count: fulfilled.length,
        value: fulfilled.reduce((s, r) => s + Number(r.total || 0), 0),
        unfulfilledCount: 0,
        fulfilled, cancelled, unfulfilled: [],
      });
      this.history = this.history.slice(0, MAX_HISTORY);
      this._persistHistory();
    }
    // Remove the archived (fulfilled + cancelled) orders; keep queued ones.
    for (const [id, o] of this.orders) {
      if (o.status === 'fulfilled' || o.status === 'cancelled') this.orders.delete(id);
    }
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'day-rollover', label });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // iPoke: prep state (visual "prepped / ready to fulfill" flag per slot)
  // ─────────────────────────────────────────────────────────────────────────

  setPrepped(batchKey, on) {
    const has = [...this.orders.values()].some((o) => o.batchKey === batchKey && o.status === 'queued');
    if (!has) return false;
    if (on) this.preppedBatches.add(batchKey);
    else this.preppedBatches.delete(batchKey);
    this._persist();
    this.emit('change', { reason: 'prep', batchKey, prepped: !!on });
    return true;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // iPoke: events / side-queues
  // ─────────────────────────────────────────────────────────────────────────

  _normKeywords(k) {
    let arr = k;
    if (typeof k === 'string') arr = k.split(/[\n,]/);
    return [...new Set((arr || []).map((s) => String(s).trim().toLowerCase()).filter(Boolean))];
  }

  /** Spots SOLD for one event (queued + fulfilled — everything except cancelled).
   *  Fulfilling a spot must NOT free capacity, or the cap could be oversold, so
   *  this counts fulfilled entries too. */
  _eventSpotsOrdered(eventId) {
    let n = 0;
    for (const e of this.eventEntries.values()) {
      if (e.eventId === eventId && (e.status === 'queued' || e.status === 'fulfilled')) n += (Number(e.spots) || 0);
    }
    return n;
  }

  /** First OPEN event whose keyword appears in the line item's name/sku/variant.
   *  Case-insensitive substring. Ripped/closed events never capture new spots.
   *  A full event STILL captures matching orders (they're flagged "oversold" in
   *  the roster) rather than dropping them into the main queue — so an oversell is
   *  visible and handled, never silently lost. */
  _matchEvent(item) {
    if (!this.events.length) return null;
    const text = this._canon(`${item.name || ''} ${item.sku || ''} ${item.variant || ''}`);
    for (const ev of this.events) {
      if (ev.status && ev.status !== 'open') continue;
      for (const kw of (ev.keywords || [])) {
        const k = this._canon(kw);
        if (k && text.includes(k)) return ev; // capture even past capacity (oversold)
      }
    }
    return null;
  }

  /** Canonicalize text for keyword matching. Shopify titles use a smart/curly
   *  apostrophe (U+2019) while keywords typed in the admin use a straight one
   *  (U+0027) — a plain substring compare would miss those, so fold every
   *  apostrophe/quote variant and drop apostrophes entirely so "baby's",
   *  "baby's" and "babys" all match. Also lowercases and collapses whitespace. */
  _canon(s) {
    return String(s == null ? '' : s)
      .toLowerCase()
      .replace(/[‘’ʼ`´]/g, "'") // smart apostrophes, backtick, acute → '
      .replace(/[“”]/g, '"')                    // smart double quotes → "
      .replace(/'/g, '')                                   // drop apostrophes so baby's == babys
      .replace(/\s+/g, ' ')
      .trim();
  }

  /** Punctuation-insensitive event-name key used ONLY for archived-event dedup
   *  (the Shopify auto-sync "don't re-add a finished event" guard). Unlike _canon
   *  — which stays as-is because it also drives order→event keyword routing — this
   *  collapses ALL punctuation to spaces, so a product title that differs from the
   *  archived event name by only a "!", "/", "(…)" etc. still matches and is NOT
   *  re-added. Mirrors shopify.js _canonKw exactly so both sides agree. */
  _looseEventName(s) {
    return String(s == null ? '' : s)
      .toLowerCase()
      .replace(/[‘’ʼ`´]/g, "'")
      .replace(/'/g, '')
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  /** Record one event spot entry (its own line, qty = spot count). Idempotent
   *  on (event, order, line index). */
  _addEventEntry(ev, item, raw, idx) {
    const entryId = `evt:${ev.id}:${String(raw.id)}:${idx}`;
    if (this.eventEntries.has(entryId)) return this.eventEntries.get(entryId);
    const entry = {
      id: entryId,
      eventId: ev.id,
      orderId: String(raw.id),
      orderName: raw.orderName || '',
      buyerId: String(raw.buyerId),
      buyer: raw.buyer || 'Buyer',
      itemName: item.name || '',
      spots: Math.max(1, Number(item.qty) || 1),
      total: Number(raw.total || 0),
      source: raw.source || '',
      createdAt: raw.createdAt || Date.now(),
      receivedAt: Date.now(),
      status: 'queued',
    };
    this.eventEntries.set(entryId, entry);
    return entry;
  }

  /** Record an event's name as "fulfilled / done" so the Shopify auto-sync will
   *  not re-create it from a product that's still listed in the store. Keyed by
   *  the canonical title, idempotent. */
  _registerFulfilledEventName(ev) {
    if (!ev) return;
    const title = String(ev.title || '').trim();
    if (!title) return;
    const canon = this._canon(title);
    if (!canon) return;
    if (!Array.isArray(this.fulfilledEventNames)) this.fulfilledEventNames = [];
    if (this.fulfilledEventNames.some((r) => r && r.canon === canon)) return;
    this.fulfilledEventNames.push({ title, canon, at: Date.now(), eventId: ev.id || '' });
  }

  /** Seed the fulfilled-name registry from events that already finished before
   *  this guard existed: archived events (pastEvents) and any event still on the
   *  board marked 'ripped'. Runs on boot; idempotent (deduped by canon title). */
  _backfillFulfilledNames() {
    try {
      if (!Array.isArray(this.fulfilledEventNames)) this.fulfilledEventNames = [];
      for (const p of (this.pastEvents || [])) this._registerFulfilledEventName({ title: p.title, id: p.id });
      for (const e of (this.events || [])) {
        if (e && e.status === 'ripped') this._registerFulfilledEventName({ title: e.title, id: e.id });
      }
    } catch (e) {
      console.warn('[queue] fulfilled-name backfill failed:', e.message);
    }
  }

  /** Seed the vault ledger from orders currently in memory (on boot), so it's
   *  populated immediately for a first deploy and after a restart — not only from
   *  new orders going forward. Idempotent: existing entries are refreshed (status/
   *  counted), new ones added with their real first-seen time. One persist at end. */
  _backfillVaultLedger() {
    try {
      if (!this.trackedVariants.length) return;
      const now = Date.now();
      for (const o of this.orders.values()) {
        const matches = this._vaultMatches(o);
        if (!matches.length) continue;
        const id = String(o.id);
        const existing = this.vaultLedger[id];
        if (existing) {
          existing.status = o.status || existing.status;
          if (o.vaultCounted && !existing.counted) existing.counted = true;
          continue;
        }
        const byVault = {};
        for (const m of matches) byVault[m.id] = (byVault[m.id] || 0) + m.amount;
        this.vaultLedger[id] = {
          orderId: id,
          orderName: o.orderName || '',
          buyer: o.buyer || '',
          source: o.source || '',
          items: (o.items || []).map((it) => ({ name: it.name || '', qty: it.qty || 1 })),
          vaults: matches.map((m) => ({ id: m.id, label: m.label, amount: m.amount, item: m.item })),
          expected: { total: matches.reduce((n, m) => n + m.amount, 0), byVault },
          counted: !!o.vaultCounted, // pre-ledger counts: flagged counted, path unknown
          countedAt: null,
          path: null,
          status: o.status || 'queued',
          firstSeenAt: o.receivedAt || o.createdAt || now,
          lastSeenAt: now,
        };
      }
      this._pruneVaultLedger();
      this._persistVaultLedger();
    } catch (e) {
      console.warn('[queue] vault ledger backfill failed:', e.message);
    }
  }

  /** True if an event with this (canonicalized) name has already been fulfilled. */
  isEventNameFulfilled(name) {
    const canon = this._canon(name);
    if (!canon) return false;
    return (this.fulfilledEventNames || []).some((r) => r && r.canon === canon);
  }

  /** Set of canonical names that have been fulfilled (for the Shopify sync). */
  fulfilledNameCanonSet() {
    return new Set((this.fulfilledEventNames || []).map((r) => r && r.canon).filter(Boolean));
  }

  addEvent({ type, title, description, totalSpots, keywords, sourceVariantId, sourceProductId, auto } = {}) {
    // Idempotent on Shopify variant: if an event for this variant already exists
    // (from an earlier manual add or the periodic auto-sync), don't create a
    // duplicate — return the existing one. Manual events (no sourceVariantId)
    // are never deduped this way.
    if (sourceVariantId) {
      const dup = this.events.find((e) => e.sourceVariantId && String(e.sourceVariantId) === String(sourceVariantId));
      if (dup) return dup;
    }
    // HARD SAFETY NET (root fix for overnight re-adds): the periodic Shopify
    // auto-sync passes auto:true. If this title was already finished — its canon
    // is in the fulfilled-name registry OR it still sits in Past Events — never
    // recreate it. A sold-out event product left listed in the store (or a
    // re-added copy an admin deleted rather than archived) must not resurrect a
    // finished event on the next sync cycle. This is independent of the sync's
    // own candidate filter, so even a filter edge-case can't slip a dup through.
    if (auto) {
      const loose = this._looseEventName(title || '');
      if (loose) {
        const done = (this.fulfilledEventNames || []).some((r) => r && this._looseEventName(r.title || r.canon || '') === loose)
          || (this.pastEvents || []).some((p) => p && this._looseEventName(p.title || '') === loose);
        if (done) {
          console.log(`[queue] auto-sync skipped already-finished event (dedup guard): ${title}`);
          return null;
        }
      }
    }
    const t = (String(type || '').toLowerCase() === 'wta') ? 'wta' : 'quack';
    const ev = {
      id: `ev${++this.eventCounter}`,
      type: t,
      title: String(title || '').trim() || (t === 'wta' ? 'WTA Event' : 'Quack Pack'),
      description: String(description || '').trim(),
      totalSpots: Math.max(0, Math.floor(Number(totalSpots) || 0)),
      keywords: this._normKeywords(keywords),
      status: 'open',
      createdAt: Date.now(),
      // Shopify origin (set when created via "Sync from Shopify") so re-syncing
      // dedups against variants that already have an event. Undefined for
      // manually-added events.
      ...(sourceVariantId ? { sourceVariantId: String(sourceVariantId) } : {}),
      ...(sourceProductId ? { sourceProductId: String(sourceProductId) } : {}),
    };
    this.events.push(ev);
    // (#6) An (Omega) quack always carries an auto giveaway seat (pinned last) plus
    // the two house seats (Nikki + Greg) just before it.
    this._ensureGiveawaySeat(ev);
    this._ensureHouseSeats(ev);
    this._persist();
    this.emit('change', { reason: 'event-add', eventId: ev.id });
    // Immediately pull any orders already on the board that match this new event
    // (no restart needed).
    this.sweepQueuedIntoEvents();
    return ev;
  }

  updateEvent(id, patch = {}) {
    const ev = this.events.find((e) => e.id === id);
    if (!ev) return null;
    if (patch.title != null) ev.title = String(patch.title).trim();
    if (patch.description != null) ev.description = String(patch.description).trim();
    if (patch.type != null) ev.type = (String(patch.type).toLowerCase() === 'wta') ? 'wta' : 'quack';
    if (patch.totalSpots != null) ev.totalSpots = Math.max(0, Math.floor(Number(patch.totalSpots) || 0));
    if (patch.keywords != null) ev.keywords = this._normKeywords(patch.keywords);
    if (patch.status != null && ['open', 'ripped'].includes(patch.status)) ev.status = patch.status;
    // Title/type may now make this an (Omega) quack — ensure its giveaway + house seats.
    if (ev.status !== 'ripped') { this._ensureGiveawaySeat(ev); this._ensureHouseSeats(ev); }
    this._persist();
    this.emit('change', { reason: 'event-update', eventId: id });
    // Edited keywords/spots/status may newly match queued orders — sweep them in.
    this.sweepQueuedIntoEvents();
    return ev;
  }

  /** Set (or clear) an event's admin win-note — e.g. who won and what they won.
   *  Stored on the event and carried into Past Events when archived. */
  setEventNote(eventId, note) {
    const ev = this.events.find((e) => e.id === eventId);
    if (!ev) return false;
    const n = String(note == null ? '' : note).trim();
    ev.note = n;
    ev.noteAt = n ? Date.now() : null;
    this._persist();
    this.emit('change', { reason: 'event-note', eventId });
    return true;
  }

  /** Set (or clear) an event's internal "Event Note" — e.g. which specific items
   *  are being used for the event. Separate from the win-note; also archived. */
  setEventItemsNote(eventId, note) {
    const ev = this.events.find((e) => e.id === eventId);
    if (!ev) return false;
    const n = String(note == null ? '' : note).trim();
    ev.eventNote = n;
    ev.eventNoteAt = n ? Date.now() : null;
    this._persist();
    this.emit('change', { reason: 'event-items-note', eventId });
    return true;
  }

  /** Re-scan queued main-queue orders and pull any items that now match an OPEN
   *  event into that event's side-queue. Event routing otherwise only happens at
   *  the moment an order is ingested, which strands orders that were already on
   *  the board when an event is created or edited (and the idempotent poller
   *  never re-checks them). Called on event add/update and each poll cycle, so a
   *  matching order is never left in the main queue. Returns the number of
   *  orders that had item(s) routed out. */
  sweepQueuedIntoEvents() {
    if (!this.events.length) return 0;
    let moved = 0;
    // Oldest first, so a limited-spot event fills in fair queue order.
    const queued = [...this.orders.values()]
      .filter((o) => o.status === 'queued')
      .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
    for (const o of queued) {
      const remaining = [];
      let routeIdx = 0;
      let routedAny = false;
      for (const it of (o.items || [])) {
        const ev = this._matchEvent(it);
        if (ev) { this._addEventEntry(ev, it, o, `sweep${routeIdx++}`); routedAny = true; }
        else remaining.push(it);
      }
      if (!routedAny) continue;
      this.seenEventOrders.add(o.id);
      if (remaining.length) {
        o.items = remaining; // keep the non-event items as the main-queue slot
      } else {
        // Whole order became event spots — drop its main-queue slot.
        this.orders.delete(o.id);
        if (this.openBatch.get(o.buyerId) === o.batchKey) this.openBatch.delete(o.buyerId);
      }
      moved++;
    }
    if (moved) { this._persist(); this.emit('change', { reason: 'event-sweep', moved }); }
    return moved;
  }

  removeEvent(id) {
    const idx = this.events.findIndex((e) => e.id === id);
    if (idx < 0) return false;
    this.events.splice(idx, 1);
    for (const [k, e] of this.eventEntries) if (e.eventId === id) this.eventEntries.delete(k);
    if (this.activeEventId === id) this.activeEventId = null;
    this._persist();
    this.emit('change', { reason: 'event-remove', eventId: id });
    return true;
  }

  /** Reposition an event within the events list (admin ordering). dir = 'up' or
   *  'down'; swaps with the adjacent event. The list order drives the admin and
   *  public events sections. No-op at the ends. */
  moveEvent(id, dir) {
    const idx = this.events.findIndex((e) => e.id === id);
    if (idx < 0) return false;
    const swap = dir === 'up' ? idx - 1 : idx + 1;
    if (swap < 0 || swap >= this.events.length) return false;
    const t = this.events[idx];
    this.events[idx] = this.events[swap];
    this.events[swap] = t;
    this._persist();
    this.emit('change', { reason: 'event-move', eventId: id, dir });
    return true;
  }

  /** Reorder the whole events list to match the given array of event ids (admin
   *  drag-and-drop). Ids not present are ignored; any current event missing from
   *  the list is appended in its existing relative order, so the list length is
   *  always preserved. Returns false if the result wouldn't cover every event. */
  reorderEvents(orderedIds) {
    if (!Array.isArray(orderedIds)) return false;
    const byId = new Map(this.events.map((e) => [e.id, e]));
    const next = [];
    for (const id of orderedIds) {
      const e = byId.get(String(id));
      if (e && !next.includes(e)) next.push(e);
    }
    for (const e of this.events) if (!next.includes(e)) next.push(e);
    if (next.length !== this.events.length) return false;
    this.events = next;
    this._persist();
    this.emit('change', { reason: 'events-reorder' });
    return true;
  }

  /** Archive a finished event to the Past Events list (full roster preserved),
   *  then remove it from the active board. The counterpart to removeEvent that
   *  keeps a record instead of discarding it. Returns the archived record. */
  archiveEvent(id) {
    const ev = this.events.find((e) => e.id === id);
    if (!ev) return null;
    // Capture the FULL roster directly (queued + fulfilled), ordered by purchase
    // time — not via eventQueue(), which hides fulfilled spots once an event is
    // ripped and would archive an empty roster.
    const all = [...this.eventEntries.values()]
      .filter((e) => e.eventId === id && (e.status === 'queued' || e.status === 'fulfilled'))
      .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
    const entries = all.map((e, i) => ({
      position: i + 1,
      buyer: this._displayName(e.buyerId, e.buyer),
      spots: e.spots,
      spotsAdjusted: !!e.spotsAdjusted,
      originalSpots: (e.originalSpots != null ? e.originalSpots : null),
      orderName: e.orderName || '',
      orderId: e.orderId || '',
      createdAt: e.createdAt || null,
      fulfilled: e.status === 'fulfilled',
      fulfilledAt: e.fulfilledAt || null, // when this spot was marked completed
    }));
    const startedAt = entries.reduce(
      (min, e) => (e.createdAt && (min == null || e.createdAt < min) ? e.createdAt : min),
      null,
    );
    const record = {
      id: ev.id,
      type: ev.type,
      title: ev.title,
      description: ev.description || '',
      totalSpots: ev.totalSpots || 0,
      spotsOrdered: entries.reduce((n, e) => n + (Number(e.spots) || 0), 0),
      entryCount: entries.length,
      startedAt,
      createdAt: ev.createdAt || null,
      rippedAt: ev.rippedAt || null, // when the event was marked fulfilled (ripped)
      note: ev.note || '', // admin win-note: who won / what they won
      noteAt: ev.noteAt || null,
      eventNote: ev.eventNote || '', // internal event note: items used, etc.
      eventNoteAt: ev.eventNoteAt || null,
      archivedAt: Date.now(),
      entries,
    };
    this.pastEvents.unshift(record);
    this.pastEvents = this.pastEvents.slice(0, PAST_EVENTS_MAX);
    this._persistPastEvents();
    // Archiving a finished event = done → remember the name so the Shopify
    // auto-sync won't re-create it from a product still listed in the store.
    this._registerFulfilledEventName(ev);
    // Now clear it off the active board (removeEvent persists + emits).
    this.removeEvent(id);
    this.emit('change', { reason: 'event-archived', eventId: id });
    return record;
  }

  /** Toggle which event side-queue is "active" (shown on overlay/Discord). */
  setActiveEvent(id) {
    this.activeEventId = (id && this.events.some((e) => e.id === id)) ? id : null;
    this._persist();
    this.emit('change', { reason: 'event-active', eventId: this.activeEventId });
    return this.activeEventId;
  }

  /** Rip an event: mark every queued spot fulfilled and close the event. */
  ripEvent(id) {
    const ev = this.events.find((e) => e.id === id);
    if (!ev) return false;
    const now = Date.now();
    for (const e of this.eventEntries.values()) {
      if (e.eventId === id && e.status === 'queued') { e.status = 'fulfilled'; e.fulfilledAt = now; }
    }
    ev.status = 'ripped';
    ev.rippedAt = now; // when the whole event was marked fulfilled (ripped)
    // Fulfilled → remember the name so the Shopify auto-sync won't re-add it.
    this._registerFulfilledEventName(ev);
    if (this.activeEventId === id) this.activeEventId = null;
    this._persist();
    this.emit('change', { reason: 'event-ripped', eventId: id });
    return true;
  }

  /** Cancel/refund a single event spot entry. */
  removeEventEntry(entryId) {
    const e = this.eventEntries.get(String(entryId));
    if (!e || e.status !== 'queued') return false;
    e.status = 'cancelled';
    e.cancelledAt = Date.now();
    this._persist();
    this.emit('change', { reason: 'event-entry-cancelled', entryId: String(entryId) });
    return true;
  }

  /** Mark one event spot fulfilled (or un-fulfill it) as the streamer works
   *  through the side-queue. A fulfilled spot still counts toward the event's
   *  capacity and stays visible on the ADMIN side (shown "done") until the whole
   *  event is marked fulfilled; the public overlay drops it so viewers see only
   *  who's still up. */
  setEventEntryFulfilled(entryId, on = true) {
    const e = this.eventEntries.get(String(entryId));
    if (!e) return false;
    if (on) {
      if (e.status !== 'queued') return false;
      e.status = 'fulfilled';
      e.fulfilledAt = Date.now();
    } else {
      if (e.status !== 'fulfilled') return false;
      e.status = 'queued';
      delete e.fulfilledAt;
    }
    this._persist();
    this.emit('change', { reason: 'event-entry-fulfilled', entryId: String(entryId), on: !!on });
    return true;
  }

  /** Mark/unmark an event seat as a WINNER. On mark, appends the seat's display name
   *  and order number to the event's Winner Note (no manual typing); on unmark, it
   *  removes that line. Toggle-safe and de-duplicated. */
  setEventWinner(entryId, on = true) {
    const e = this.eventEntries.get(String(entryId));
    if (!e) return false;
    const ev = this.events.find((x) => x.id === e.eventId);
    if (!ev) return false;
    const name = e.nameOverridden ? e.buyer : this._displayName(e.buyerId, e.buyer);
    const ord = e.orderName || (e.orderId ? ('#' + String(e.orderId)) : '');
    const line = (ord ? `${name} (${ord})` : String(name || '')).trim();
    if (!line) return false;
    const lines = String(ev.note || '').split(/\n+/).map((s) => s.trim()).filter(Boolean);
    if (on) {
      e.winner = true;
      if (!lines.includes(line)) lines.push(line);
    } else {
      e.winner = false;
      const i = lines.indexOf(line);
      if (i >= 0) lines.splice(i, 1);
    }
    ev.note = lines.join('\n');
    ev.noteAt = ev.note ? Date.now() : null;
    this._persist();
    this.emit('change', { reason: 'event-winner', eventId: ev.id, entryId: String(entryId), on: !!on });
    return true;
  }

  /** Rename a single event seat (admin). Sets the name directly on the entry and
   *  flags it so the per-event roster shows this name verbatim, independent of the
   *  buyer-level name override used by the main queue. Empty name is ignored. */
  setEventEntryBuyer(entryId, name) {
    const e = this.eventEntries.get(String(entryId));
    if (!e) return false;
    const nm = String(name == null ? '' : name).trim();
    if (!nm || (e.nameOverridden && e.buyer === nm)) return false;
    e.buyer = nm;
    e.nameOverridden = true;
    this._persist();
    this.emit('change', { reason: 'event-entry-rename', entryId: String(entryId) });
    return true;
  }

  /** Manually override how many spots a seat holds (admin) — e.g. an event change
   *  turned someone's 1 spot into 3. Flags the seat as manually adjusted (shown
   *  admin-side only) so we can tell it wasn't the count the order came in with,
   *  and remembers the original count for the note. Minimum 1 spot. */
  setEventEntrySpots(entryId, spots) {
    const e = this.eventEntries.get(String(entryId));
    if (!e) return false;
    const n = Math.max(1, Math.floor(Number(spots) || 0));
    if (n === e.spots) return false;
    if (e.originalSpots == null) e.originalSpots = e.spots; // capture pre-adjust count once
    e.spots = n;
    e.spotsAdjusted = true;
    this._persist();
    this.emit('change', { reason: 'event-entry-spots', entryId: String(entryId) });
    return true;
  }

  /** Manually add one seat (or a multi-spot block) to an event's side-queue.
   *  Name + spots + optional order number. Added to the end of the roster (newest
   *  createdAt), counts toward the event's capacity like any other spot, and is
   *  flagged `manual` so it's distinguishable. Returns the new entry or null. */
  addEventEntryManual({ eventId, buyer, spots, orderName } = {}) {
    const ev = this.events.find((e) => e.id === eventId);
    if (!ev) return null;
    const n = Math.max(1, Math.floor(Number(spots) || 1));
    const nm = String(buyer == null ? '' : buyer).trim() || 'Manual seat';
    const uniq = Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
    const entryId = `evt:${ev.id}:manual:${uniq}`;
    const entry = {
      id: entryId,
      eventId: ev.id,
      orderId: '',
      orderName: String(orderName == null ? '' : orderName).trim(),
      buyerId: `manual:${entryId}`,
      buyer: nm,
      nameOverridden: true,
      itemName: ev.title,
      spots: n,
      total: 0,
      source: 'Manual',
      createdAt: Date.now(),
      receivedAt: Date.now(),
      status: 'queued',
      manual: true,
    };
    this.eventEntries.set(entryId, entry);
    this._persist();
    this.emit('change', { reason: 'event-entry-manual-add', eventId: ev.id, entryId });
    return entry;
  }

  /** True if an event is an (Omega) tier event — matched on its title, tolerant of
   *  spacing/case, e.g. "(Omega) ...", "(OMEGA) ...". */
  _isOmegaEvent(ev) {
    return !!ev && /\(\s*omega\s*\)/i.test(String(ev.title || ''));
  }

  /** (#6) Ensure an (Omega) quack has exactly one auto giveaway seat, always
   *  pinned LAST (via a far-future sort timestamp). Filled with a GIVEAWAY
   *  placeholder you can rename to the winner. Idempotent — never adds a second.
   *  Returns true if it added one. */
  _ensureGiveawaySeat(ev) {
    if (!this._isOmegaEvent(ev)) return false;
    for (const e of this.eventEntries.values()) {
      if (e.eventId === ev.id && e.giveaway && e.status !== 'cancelled') return false;
    }
    const uniq = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    const entryId = `evt:${ev.id}:giveaway:${uniq}`;
    this.eventEntries.set(entryId, {
      id: entryId,
      eventId: ev.id,
      orderId: '',
      orderName: '',
      buyerId: `giveaway:${entryId}`,
      buyer: '🎁 GIVEAWAY',
      nameOverridden: true,
      itemName: ev.title,
      spots: 1,
      total: 0,
      source: 'Giveaway',
      createdAt: GIVEAWAY_SORT_TS, // always sorts last
      receivedAt: Date.now(),
      status: 'queued',
      manual: true,
      giveaway: true,
    });
    return true;
  }

  /** Ensure an (Omega) quack carries its two auto house seats — Nikki and Greg —
   *  pinned just BEFORE the giveaway (always the last two before it). The order of
   *  the pair alternates with each new (Omega) quack via houseSeatToggle. Idempotent:
   *  only adds the pair when the event has NO live house seat yet, so removing one
   *  by hand won't make it reappear. Returns true if it added them. */
  _ensureHouseSeats(ev) {
    if (!this._isOmegaEvent(ev)) return false;
    for (const e of this.eventEntries.values()) {
      if (e.eventId === ev.id && e.houseSeat && e.status !== 'cancelled') return false;
    }
    // Pick the order for this event, then flip the toggle for the next one.
    const flipped = (this.houseSeatToggle % 2) === 1;
    const ordered = flipped ? [HOUSE_SEAT_NAMES[1], HOUSE_SEAT_NAMES[0]] : [HOUSE_SEAT_NAMES[0], HOUSE_SEAT_NAMES[1]];
    const sortTs = [HOUSE_SEAT_SORT_FIRST, HOUSE_SEAT_SORT_SECOND];
    ordered.forEach((name, i) => {
      const uniq = Date.now().toString(36) + Math.random().toString(36).slice(2, 6) + i;
      const entryId = `evt:${ev.id}:house:${uniq}`;
      this.eventEntries.set(entryId, {
        id: entryId,
        eventId: ev.id,
        orderId: '',
        orderName: '',
        buyerId: `house:${entryId}`,
        buyer: name,
        nameOverridden: true,
        itemName: ev.title,
        spots: 1,
        total: 0,
        source: 'House',
        createdAt: sortTs[i], // pins just before the giveaway, in this order
        receivedAt: Date.now(),
        status: 'queued',
        manual: true,
        houseSeat: true,
      });
    });
    this.houseSeatToggle = (this.houseSeatToggle + 1) % 1000000;
    return true;
  }

  /** Boot backfill: give every existing open (Omega) quack its giveaway + house seats. */
  _backfillGiveawaySeats() {
    try {
      let added = 0;
      for (const ev of (this.events || [])) {
        if (ev.status === 'ripped') continue;
        if (this._ensureGiveawaySeat(ev)) added++;
        if (this._ensureHouseSeats(ev)) added++;
      }
      if (added) this._persist();
    } catch (e) {
      console.warn('[queue] giveaway/house backfill failed:', e.message);
    }
  }

  /** The ordered side-queue for one event (first-in-first-served, no skipping),
   *  plus spot totals. */
  eventQueue(id) {
    const ev = this.events.find((e) => e.id === id);
    if (!ev) return null;
    // All non-cancelled spots (queued + per-slot fulfilled), ordered by purchase
    // time. "Sold" totals come from this whole set so the cap and the meter never
    // move as spots are worked through.
    const all = [...this.eventEntries.values()]
      .filter((e) => e.eventId === id && (e.status === 'queued' || e.status === 'fulfilled'))
      .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
    const spotsOrdered = all.reduce((n, e) => n + (Number(e.spots) || 0), 0);
    const spotsUnfulfilled = all.reduce((n, e) => n + (e.status === 'fulfilled' ? 0 : (Number(e.spots) || 0)), 0);
    // Displayed rows: show every spot (per-slot fulfilled ones flagged done) for
    // both open AND ripped events, so a fulfilled event keeps its full roster
    // visible in the admin — you can review it and the count is accurate right up
    // until you Remove & Archive it. (The overlay never shows ripped events, so
    // this only affects the admin side.)
    const shown = all;
    // Flag oversold entries: walk the roster in purchase order and mark any entry
    // that holds one or more spots past the event's capacity (only when a limit
    // is set). An entry whose spots STRADDLE the cap (some within, some beyond —
    // e.g. a 4-spot order landing on seats 39–42 of a 40-cap event) is still
    // flagged, since it contains the oversold seat(s); oversoldSpotsInEntry says
    // how many of its spots are over, and oversoldPartial marks the straddler.
    let running = 0;
    const cap = ev.totalSpots > 0 ? ev.totalSpots : Infinity;
    const entries = shown.map((e, i) => {
      const sp = Number(e.spots) || 0;
      running += sp;                       // running is now this entry's end position
      const overInEntry = (running > cap) ? Math.min(sp, running - cap) : 0;
      return {
        id: e.id,
        position: i + 1,
        buyer: e.nameOverridden ? e.buyer : this._displayName(e.buyerId, e.buyer),
        buyerId: e.buyerId,
        spots: e.spots,
        spotsAdjusted: !!e.spotsAdjusted,                 // admin manually changed the spot count
        originalSpots: (e.originalSpots != null ? e.originalSpots : null),
        itemName: e.itemName,
        manual: !!e.manual,
        source: e.source || '',
        orderId: e.orderId,
        orderName: e.orderName || '',
        createdAt: (e.createdAt >= HOUSE_SEAT_SORT_FIRST) ? null : e.createdAt,
        giveaway: !!e.giveaway,
        houseSeat: !!e.houseSeat,
        winner: !!e.winner,
        fulfilled: e.status === 'fulfilled',
        fulfilledAt: e.fulfilledAt || null,
        // Oversold if any of this entry's spots land beyond the cap.
        oversold: overInEntry > 0,
        oversoldSpotsInEntry: overInEntry,          // how many of its spots are over
        oversoldPartial: overInEntry > 0 && overInEntry < sp, // straddles the cap line
      };
    });
    const oversoldSpots = ev.totalSpots > 0 ? Math.max(0, spotsOrdered - ev.totalSpots) : 0;
    return {
      id: ev.id,
      type: ev.type,
      title: ev.title,
      description: ev.description,
      totalSpots: ev.totalSpots,
      status: ev.status,
      spotsOrdered,
      spotsRemaining: Math.max(0, ev.totalSpots - spotsOrdered),
      soldOut: ev.totalSpots > 0 && spotsOrdered >= ev.totalSpots,
      oversold: oversoldSpots > 0,
      oversoldSpots,
      entryCount: entries.length,
      unfulfilledCount: entries.filter((e) => !e.fulfilled).length,
      spotsUnfulfilled,
      entries,
    };
  }

  /** Compact summary of every event (for the admin list + overlay/Discord).
   *  Sold-out events bubble to the TOP of the list, in the order they sold out
   *  (earliest first), and carry a soldOutAt stamp the UI uses to highlight them. */
  eventsSummary() {
    const now = Date.now();
    let changed = false;
    const out = this.events.map((ev) => {
      const q = this.eventQueue(ev.id);
      // Stamp the moment an event first sells out; clear it if it later drops back
      // below capacity (spots added, or a seat cancelled). This drives both the
      // sold-out-first ordering and the green highlight.
      if (q.soldOut && !ev.soldOutAt) { ev.soldOutAt = now; changed = true; }
      else if (!q.soldOut && ev.soldOutAt) { ev.soldOutAt = null; changed = true; }
      return {
        id: ev.id, type: ev.type, title: ev.title, description: ev.description,
        totalSpots: ev.totalSpots, spotsOrdered: q.spotsOrdered, spotsRemaining: q.spotsRemaining,
        soldOut: q.soldOut, soldOutAt: ev.soldOutAt || null,
        oversold: q.oversold, oversoldSpots: q.oversoldSpots,
        entryCount: q.entryCount,
        unfulfilledCount: q.unfulfilledCount, spotsUnfulfilled: q.spotsUnfulfilled,
        status: ev.status, rippedAt: ev.rippedAt || null,
        note: ev.note || '', noteAt: ev.noteAt || null,
        eventNote: ev.eventNote || '', eventNoteAt: ev.eventNoteAt || null,
        active: this.activeEventId === ev.id, keywords: ev.keywords,
        // Full ordered side-queue (admin panel consumes this; overlay uses
        // activeEvent instead, so this stays admin-only).
        entries: q.entries,
      };
    });
    // Sold-out events first, in the order they sold out (earliest soldOutAt first).
    // Everything else keeps its normal (manual-drag / creation) order — a stable
    // sort leaves non-sold-out events exactly where they were.
    out.sort((a, b) => {
      const aa = a.soldOutAt || 0, bb = b.soldOutAt || 0;
      if (!aa !== !bb) return aa ? -1 : 1;  // sold-out before not-sold-out
      if (aa && bb) return aa - bb;          // earlier sell-out sits higher
      return 0;
    });
    if (changed) this._persist();
    return out;
  }

  /** Inject a synthetic order for testing (e.g. label printing) regardless of
   *  live state. Behaves like a real ingest otherwise (merges by buyer, etc.). */
  injectTestOrder(raw) {
    const wasLive = this.live;
    this.live = true;
    const order = this.upsertOrder(raw);
    this.live = wasLive;
    this._persist();
    this.emit('change', { reason: 'test-order' });
    return order;
  }
}
