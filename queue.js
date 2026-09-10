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
    this.trackedVariants = []; // [{ id, label, product, variant }] — per-variant sales counters
    this.variantCounts = {}; // { [variantId]: number } — units counted when a slot hits the top
    this.variantLog = []; // audit trail of manual counter edits/resets (most recent first)
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
    this.eventCounter = 0;
    this.activeEventId = null; // which event side-queue is toggled to the overlay/Discord
    // Durable dedup for orders that produced ONLY event entries (and so never
    // land in the main `orders` map). Prevents double-ingest across restarts.
    this.seenEventOrders = new Set();

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
      queue: { opacity: 0, scale: 1, panel: true },
      vault: { opacity: 0, scale: 1, panel: true },
    };
    // Pacific-day marker for the daily "Past Days" rollover.
    this.lastRolloverDay = null;

    this._ensureDataDir();
    this._load();
    this._seedFromEnv();
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
        this.trackedVariants = Array.isArray(cfg.trackedVariants) ? cfg.trackedVariants : [];
        this.variantCounts = (cfg.variantCounts && typeof cfg.variantCounts === 'object') ? cfg.variantCounts : {};
        this.variantLog = Array.isArray(cfg.variantLog) ? cfg.variantLog : [];
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
        this.eventCounter = cfg.eventCounter || 0;
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
        else if (this.perpetual) this.combineMode = 'untiltop'; // iPoke default
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
      (o) => o.batchKey === top.key && o.status === 'queued' && !o.reachedTopAt
    );
    if (!orders.length) return false;
    const now = Date.now();
    for (const o of orders) {
      o.reachedTopAt = now;
      this._tallyVariants(o);
    }
    return true;
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
    if (!this.trackedVariants.length) return;
    for (const it of (order.items || [])) {
      const text = `${it.name || ''} ${it.sku || ''} ${it.variant || ''}`.toLowerCase();
      const qty = it.qty || 1;
      const perItem = this._vaultUnitsPerItem(it);
      for (const v of this.trackedVariants) {
        const prod = (v.product || '').toLowerCase();
        const varn = (v.variant || '').toLowerCase();
        if (!varn) continue;
        if ((!prod || text.includes(prod)) && text.includes(varn)) {
          this.variantCounts[v.id] = (this.variantCounts[v.id] || 0) + perItem * qty;
        }
      }
    }
  }

  /** Replace the tracked-variant DEFINITIONS (admin editor). Preserves the live
   *  count for any id that still exists; drops counts for removed ids. */
  setTrackedVariants(list) {
    const defs = (Array.isArray(list) ? list : []).filter((d) => d && d.id != null).map((d) => ({
      id: String(d.id),
      label: String(d.label || d.id),
      product: String(d.product || ''),
      variant: String(d.variant || ''),
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
          trackedVariants: this.trackedVariants,
          variantCounts: this.variantCounts,
          variantLog: this.variantLog,
          batchCounter: this.batchCounter,
          live: this.live,
          sessionStartedAt: this.sessionStartedAt,
          // iPoke fields
          combineMode: this.combineMode,
          combineWindowMs: this.combineWindowMs,
          nameOverrides: this.nameOverrides,
          preppedBatches: [...this.preppedBatches],
          events: this.events,
          eventCounter: this.eventCounter,
          activeEventId: this.activeEventId,
          overlays: this.overlays,
          lastRolloverDay: this.lastRolloverDay,
        })
      );
      // Only write the events-state file when there are (or were) events, so
      // stores that never use events don't get an extra file.
      if (this.eventEntries.size || fs.existsSync(EVENTS_FILE)) {
        this._atomicWrite(EVENTS_FILE, JSON.stringify([...this.eventEntries.values()]));
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
    if (this.orders.has(id) || this.seenEventOrders.has(id)) return this.orders.get(id) || null;

    // Off-air gate. Perpetual queues (iPoke) ingest 24/7 regardless of live.
    if (!this.live && !this.perpetual) return null;

    const buyerId = String(raw.buyerId);
    const allItems = raw.items || [];

    // ── Event routing ── split line items into event spots vs regular items.
    // Each matching line item becomes its own event entry (qty = spot count).
    const regularItems = [];
    const routed = [];
    for (const it of allItems) {
      const ev = this._matchEvent(it);
      if (ev) routed.push({ it, ev });
      else regularItems.push(it);
    }
    if (routed.length) {
      routed.forEach((r, i) => this._addEventEntry(r.ev, r.it, raw, i));
      this.seenEventOrders.add(id);
    }

    // If every line item was an event spot, there is no main-queue slot.
    if (!regularItems.length) {
      if (routed.length) {
        this._persist();
        this.emit('change', { reason: 'event-order', orderId: id });
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
      // (untiltop) set when this order opened its own slot because the buyer's
      // earlier slot had already reached the top. Links back to that slot's key.
      afterTopOf: afterTopOf || null,
    };
    this.orders.set(id, order);
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
    const priorityOrders = orders.filter((o) => o.hasPriority);
    const isPriority = priorityOrders.length > 0;
    const priorityAt = isPriority
      ? Math.min(...priorityOrders.map((o) => o.createdAt))
      : Infinity;

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
      // Admin-only cross-check: the buyer's unique @username (the queue label is
      // their display name). Never surfaced on the public view.
      buyerHandle: first.buyerHandle || '',
      orderIds: orders.map((o) => o.id),
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
          };
        }),
      items,
      itemCount: items.reduce((n, i) => n + i.qty, 0),
      total: orders.reduce((s, o) => s + o.total, 0),
      isPriority,
      priorityItems: [...new Set(
        orders.flatMap((o) => o.items.filter((it) => this._isPriorityOrder([it])).map((it) => it.name))
      )],
      firstOrderAt: first.createdAt,
      // When this slot first reached the top of the queue (drives the "time at
      // top" live timer and the fulfillment-time metric). null until it hits #1.
      reachedTopAt: reachedAts.length ? Math.min(...reachedAts) : null,
      priorityAt,
      bumped: !!first.bumped,
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
    // Only reopen if the buyer has no other open slot.
    if (this.openBatch.has(buyerId)) return null;
    for (const o of orders) {
      o.status = 'queued';
      o.fulfilledAt = null;
    }
    this.openBatch.set(buyerId, batchKey);
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

  /** Return a held slot to the main queue and bump it to the very top. */
  unholdSlot(batchKey) {
    const orders = [...this.orders.values()].filter(
      (o) => o.batchKey === batchKey && o.status === 'held'
    );
    if (!orders.length) return null;
    const buyerId = orders[0].buyerId;
    for (const o of orders) {
      o.status = 'queued';
      delete o.heldAt;
    }
    if (!this.openBatch.has(buyerId)) this.openBatch.set(buyerId, batchKey);
    // Push it to the very top (clears any other bump).
    for (const o of this.orders.values()) {
      if (o.batchKey === batchKey && o.status === 'queued') o.bumped = true;
      else if (o.bumped) o.bumped = false;
    }
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
        orderNames: orders.map((o) => o.orderName || ('#' + String(o.id))),
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
   *   1. Manually bumped slot.
   *   2. Priority slots before normal slots.
   *   3. Priority tier: earliest priority-item purchase first.
   *      Normal tier: earliest order first.
   */
  activeQueue() {
    const keys = new Set(
      [...this.orders.values()].filter((o) => o.status === 'queued').map((o) => o.batchKey)
    );
    const entries = [...keys].map((k) => this._entryFor(k)).filter(Boolean);
    entries.sort((a, b) => {
      if (a.bumped !== b.bumped) return a.bumped ? -1 : 1;
      if (a.isPriority !== b.isPriority) return a.isPriority ? -1 : 1;
      if (a.isPriority) return a.priorityAt - b.priorityAt;
      return a.firstOrderAt - b.firstOrderAt;
    });
    return entries.map((e, i) => ({ ...e, position: i + 1 }));
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
      config: { priorityItems: this.priorityItems },
      variants: this.trackedVariants.map((v) => ({
        id: v.id, label: v.label, product: v.product, variant: v.variant,
        count: this.variantCounts[v.id] || 0,
      })),
      variantLog: this.variantLog.slice(0, 50),
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
    for (const o of orders) { o.status = 'cancelled'; o.cancelledAt = now; o.bumped = false; }
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
    const o = this.orders.get(String(orderId));
    if (!o || o.status !== 'queued') return null;
    o.status = 'cancelled';
    o.cancelledAt = Date.now();
    o.bumped = false;
    // If the buyer's open slot has no queued orders left, close it.
    const stillOpen = [...this.orders.values()].some(
      (x) => x.batchKey === o.batchKey && x.status === 'queued'
    );
    if (!stillOpen && this.openBatch.get(o.buyerId) === o.batchKey) this.openBatch.delete(o.buyerId);
    this._markTopReached();
    this._persist();
    this.emit('change', { reason: 'cancelled', orderId: String(orderId) });
    return o;
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
  setCombineMode(mode, windowMinutes) {
    if (!COMBINE_MODES.has(mode)) return false;
    this.combineMode = mode;
    const w = Number(windowMinutes);
    if (Number.isFinite(w) && w > 0) this.combineWindowMs = Math.round(w * 60 * 1000);
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
    const buyerIds = new Set(groups.flatMap((g) => g.orders.map((o) => o.buyerId)));
    if (buyerIds.size !== 1) return null; // refuse cross-buyer combines
    const buyerId = [...buyerIds][0];
    // Target = the slot with the earliest first order (keeps its place in line).
    let target = groups[0];
    let targetFirst = Math.min(...groups[0].orders.map((o) => o.createdAt || 0));
    for (const g of groups) {
      const f = Math.min(...g.orders.map((o) => o.createdAt || 0));
      if (f < targetFirst) { target = g; targetFirst = f; }
    }
    const targetKey = target.k;
    for (const g of groups) {
      if (g.k === targetKey) continue;
      for (const o of g.orders) o.batchKey = targetKey;
      if (this.openBatch.get(buyerId) === g.k) this.openBatch.delete(buyerId);
      this.preppedBatches.delete(g.k);
    }
    if (this.combineMode !== 'off') this.openBatch.set(buyerId, targetKey);
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
  /** Called on boot and every minute. When the Pacific calendar day changes,
   *  archive the finished day's fulfilled + cancelled orders into Past Days and
   *  clear them, LEAVING any still-queued (unfulfilled) orders on the board. */
  maybeDailyRollover() {
    const today = this._laDateString();
    if (!this.lastRolloverDay) { this.lastRolloverDay = today; this._persist(); return false; }
    if (today === this.lastRolloverDay) return false;
    this._rolloverDay(this._prettyDay(this.lastRolloverDay));
    this.lastRolloverDay = today;
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
   *  Case-insensitive substring. Ripped/closed events never capture new spots,
   *  and a SOLD-OUT event (spots ordered ≥ its limit) stops capturing too — the
   *  item then falls through to the main queue rather than overselling. */
  _matchEvent(item) {
    if (!this.events.length) return null;
    const text = `${item.name || ''} ${item.sku || ''} ${item.variant || ''}`.toLowerCase();
    for (const ev of this.events) {
      if (ev.status && ev.status !== 'open') continue;
      for (const kw of (ev.keywords || [])) {
        if (kw && text.includes(kw)) {
          // Enforce the spot limit: once full, don't capture more into it.
          if (ev.totalSpots > 0 && this._eventSpotsOrdered(ev.id) >= ev.totalSpots) break;
          return ev;
        }
      }
    }
    return null;
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

  addEvent({ type, title, description, totalSpots, keywords } = {}) {
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
    };
    this.events.push(ev);
    this._persist();
    this.emit('change', { reason: 'event-add', eventId: ev.id });
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
    this._persist();
    this.emit('change', { reason: 'event-update', eventId: id });
    return ev;
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
    // Displayed rows: while the event is OPEN, show every spot (per-slot fulfilled
    // ones flagged done so the admin keeps them visible). Once the WHOLE event is
    // ripped/fulfilled, the done spots drop off entirely.
    const shown = (ev.status === 'ripped') ? all.filter((e) => e.status !== 'fulfilled') : all;
    const entries = shown.map((e, i) => ({
      id: e.id,
      position: i + 1,
      buyer: this._displayName(e.buyerId, e.buyer),
      buyerId: e.buyerId,
      spots: e.spots,
      itemName: e.itemName,
      source: e.source || '',
      orderId: e.orderId,
      orderName: e.orderName || '',
      createdAt: e.createdAt,
      fulfilled: e.status === 'fulfilled',
      fulfilledAt: e.fulfilledAt || null,
    }));
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
      entryCount: entries.length,
      unfulfilledCount: entries.filter((e) => !e.fulfilled).length,
      spotsUnfulfilled,
      entries,
    };
  }

  /** Compact summary of every event (for the admin list + overlay/Discord). */
  eventsSummary() {
    return this.events.map((ev) => {
      const q = this.eventQueue(ev.id);
      return {
        id: ev.id, type: ev.type, title: ev.title, description: ev.description,
        totalSpots: ev.totalSpots, spotsOrdered: q.spotsOrdered, spotsRemaining: q.spotsRemaining,
        soldOut: q.soldOut, entryCount: q.entryCount,
        unfulfilledCount: q.unfulfilledCount, spotsUnfulfilled: q.spotsUnfulfilled,
        status: ev.status,
        active: this.activeEventId === ev.id, keywords: ev.keywords,
        // Full ordered side-queue (admin panel consumes this; overlay uses
        // activeEvent instead, so this stays admin-only).
        entries: q.entries,
      };
    });
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
