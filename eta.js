// eta.js — queue wait-time estimates (iPoke).
//
// Learns from past fulfillments how long a Rip-Live slot takes (a base time
// plus a little per extra item), when the stream usually starts/ends, and how
// much of a stream day is breaks. Then, for the current queue, it walks the
// slots in order and predicts when each one will reach the top — pausing over
// breaks, nights and weekends (Mon–Fri stream; Friday runs late into Saturday).
//
// Only "on-air" data is learned from: a gap of more than BREAK_GAP between two
// fulfillments is treated as a break / off air and never counted as slot time,
// and anything outside a stream day (Sat after the Friday late stream, Sun) is
// ignored. Pure functions — no I/O — so it can be tested in isolation.

export const TZ = 'America/Los_Angeles';
const MIN = 60 * 1000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
export const BREAK_GAP = 30 * MIN;  // longer than this between fulfillments = break / off air
const MAX_SAMPLE = BREAK_GAP;       // never learn a slot time longer than this
const LOOKBACK = 60 * DAY;          // learn from the last 60 days
const FRI_LATE_HOUR = 6;            // Friday's stream can run until ~6 AM Saturday

const DEFAULTS = {
  wk: { base: 3 * MIN, per: 20 * 1000, start: 9 * 60, end: 16 * 60 + 30, bf: 1.6 },
  fri: { base: 3 * MIN, per: 20 * 1000, start: 15 * 60, end: 26 * 60, bf: 1.5 },
};

// ── Pacific-time helpers ──────────────────────────────────────────────────
const _fmt = new Intl.DateTimeFormat('en-US', {
  timeZone: TZ, hourCycle: 'h23', year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', weekday: 'short',
});
const _DOW = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
export function laParts(ts) {
  const p = {};
  for (const x of _fmt.formatToParts(new Date(ts))) p[x.type] = x.value;
  return {
    y: +p.year, m: +p.month, d: +p.day, hour: +p.hour % 24, minute: +p.minute, second: +p.second,
    dow: _DOW[p.weekday], ymd: `${p.year}-${p.month}-${p.day}`,
  };
}
/** UTC ms of Pacific midnight on the calendar day containing ts. */
export function laMidnight(ts) {
  const p = laParts(ts);
  const asUtc = Date.UTC(p.y, p.m - 1, p.d, p.hour, p.minute, p.second);
  const offset = asUtc - Math.floor(ts / 1000) * 1000;          // Pacific offset at ts
  let mid = Date.UTC(p.y, p.m - 1, p.d) - offset;
  // DST edge: if the offset changed since midnight, correct by the difference.
  const q = laParts(mid);
  if (q.hour !== 0) mid -= q.hour * HOUR - (q.hour > 12 ? DAY : 0);
  return mid;
}

/** Which stream-day profile a moment belongs to: 'wk' (Mon–Thu), 'fri' (Friday,
 *  including the small hours of Saturday), or null (not a stream day). Also
 *  returns the Pacific midnight of the stream day it belongs to. */
export function streamDay(ts) {
  const p = laParts(ts);
  if (p.dow === 6 && p.hour < FRI_LATE_HOUR) return { type: 'fri', mid: laMidnight(ts - DAY) };
  if (p.dow >= 1 && p.dow <= 4) return { type: 'wk', mid: laMidnight(ts) };
  if (p.dow === 5) return { type: 'fri', mid: laMidnight(ts) };
  return { type: null, mid: laMidnight(ts) };
}

const median = (a) => {
  if (!a.length) return null;
  const s = [...a].sort((x, y) => x - y);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

// ── Learning ──────────────────────────────────────────────────────────────
/** Collapse fulfillment records into slots: records that share a slot key, or
 *  were fulfilled within 2 s of each other, are one slot (qty summed). */
export function toSlots(records) {
  const rs = (records || [])
    .filter((r) => r && Number.isFinite(r.ts))
    .sort((a, b) => a.ts - b.ts);
  const out = [];
  for (const r of rs) {
    const last = out[out.length - 1];
    const qty = Math.max(1, Number(r.qty) || 1);
    if (last && ((r.key && last.key === r.key) || r.ts - last.ts <= 2000)) {
      last.qty += qty; last.ts = Math.max(last.ts, r.ts);
    } else out.push({ ts: r.ts, qty, key: r.key || null });
  }
  return out;
}

/** Split slots into on-air runs (no gap > BREAK_GAP, same stream day). */
export function toRuns(slots) {
  const runs = [];
  let cur = null;
  for (const s of slots) {
    const sd = streamDay(s.ts);
    if (!sd.type) { cur = null; continue; }
    if (!cur || s.ts - cur.end > BREAK_GAP || cur.mid !== sd.mid) {
      cur = { type: sd.type, mid: sd.mid, start: s.ts, end: s.ts, slots: [] };
      runs.push(cur);
    }
    cur.slots.push(s);
    cur.end = s.ts;
  }
  return runs;
}

function fitTimes(samples, fallback) {
  const singles = samples.filter((x) => x.qty <= 1).map((x) => x.gap);
  const base = singles.length >= 8 ? median(singles) : fallback.base;
  const per = [];
  for (const x of samples) if (x.qty >= 2) per.push(Math.max(0, (x.gap - base) / (x.qty - 1)));
  const perItem = per.length >= 5 ? clamp(median(per), 0, 5 * MIN) : fallback.per;
  return { base: clamp(base, 30 * 1000, 20 * MIN), per: perItem };
}

// ── Event rip time ────────────────────────────────────────────────────────
const EV_SETUP = 90 * 1000;   // opening/intro before the first spot is marked done

/** Learn how long an event takes to rip from per-spot fulfill times.
 *  events: [{ id, fulfills: [{ ts, spots }] }]. Events marked done in one click
 *  (all spots share a timestamp) carry no timing and are skipped. Returns
 *  { a, b, samples, windows } — duration ≈ EV_SETUP + a + b × spots — plus each
 *  timed event's on-air window [start, end] for separating it from Rip-Live time. */
export function learnEvents(events, now = Date.now()) {
  const pts = [];
  const windows = [];
  for (const ev of (events || [])) {
    const fs = (ev.fulfills || []).filter((f) => Number.isFinite(f.ts) && f.ts <= now && f.ts >= now - LOOKBACK)
      .sort((x, y) => x.ts - y.ts);
    if (fs.length < 3) continue;
    const uniq = new Set(fs.map((f) => Math.round(f.ts / 2000))).size;
    if (uniq <= 2) continue;
    const span = fs[fs.length - 1].ts - fs[0].ts;
    if (span <= 0 || span > 4 * HOUR) continue;
    const spots = fs.reduce((n, f) => n + (Number(f.spots) || 1), 0);
    pts.push({ x: spots, y: span });
    windows.push({ start: fs[0].ts - EV_SETUP, end: fs[fs.length - 1].ts, spots });
  }
  let a = 0, b = 35 * 1000;
  if (pts.length >= 8) {
    const mx = pts.reduce((s, p) => s + p.x, 0) / pts.length;
    const my = pts.reduce((s, p) => s + p.y, 0) / pts.length;
    let sxy = 0, sxx = 0;
    for (const p of pts) { sxy += (p.x - mx) * (p.y - my); sxx += (p.x - mx) ** 2; }
    b = sxx > 0 ? sxy / sxx : b;
    b = clamp(b, 10 * 1000, 90 * 1000);
    a = clamp(my - b * mx, -2 * MIN, 5 * MIN);
  }
  return { a, b, samples: pts.length, windows };
}

/** Predicted time to rip `spots` remaining spots of an event (setup included
 *  unless it has already started). */
export function eventWork(model, spots, started = false) {
  const e = model.event || { a: 0, b: 35 * 1000 };
  const n = Math.max(0, Number(spots) || 0);
  if (!n) return 0;
  const full = EV_SETUP + e.a + e.b * n;
  return clamp(started ? full - EV_SETUP - Math.max(0, e.a) : full, 30 * 1000, 3 * HOUR);
}

/** Learn the model.
 *  records: Rip-Live fulfillments [{ ts, qty, key? }]
 *  events:  past/current events [{ id, fulfills: [{ ts, spots }] }]
 *  Event ripping counts as on-air (not a break), and Rip-Live gaps that had an
 *  event ripped inside them are left out of the slot-time samples, so slot time
 *  and event time are learned separately. */
export function learn(records, now = Date.now(), events = []) {
  const ev = learnEvents(events, now);
  const slots = toSlots((records || []).filter((r) => r.ts >= now - LOOKBACK && r.ts <= now));
  // One activity timeline: Rip-Live slots + event spot fulfills (runs = on air).
  const evActs = [];
  for (const e of (events || [])) for (const f of (e.fulfills || [])) {
    if (Number.isFinite(f.ts) && f.ts <= now && f.ts >= now - LOOKBACK) evActs.push({ ts: f.ts, qty: 0, ev: true });
  }
  const acts = [...slots, ...evActs].sort((x, y) => x.ts - y.ts);
  const runs = toRuns(acts);
  const overlapsEvent = (t0, t1) => ev.windows.some((w) => w.start < t1 && w.end > t0);
  const samples = { wk: [], fri: [] };
  for (const r of runs) {
    let prev = null;
    for (const s of r.slots) {
      if (s.ev) continue;
      if (prev) {
        const gap = s.ts - prev.ts;
        if (gap > 0 && gap <= MAX_SAMPLE && !overlapsEvent(prev.ts, s.ts)) samples[r.type].push({ gap, qty: s.qty });
      }
      prev = s;
    }
  }
  // Day schedule, break factor and event load, per stream day.
  const days = new Map();
  for (const r of runs) {
    const k = r.type + ':' + r.mid;
    const d = days.get(k) || { type: r.type, mid: r.mid, first: r.start, last: r.end, active: 0, n: 0, evMs: 0 };
    d.first = Math.min(d.first, r.start); d.last = Math.max(d.last, r.end);
    d.active += r.end - r.start; d.n += r.slots.filter((x) => !x.ev).length;
    days.set(k, d);
  }
  for (const w of ev.windows) {
    const sd = streamDay(w.end);
    const d = sd.type && days.get(sd.type + ':' + sd.mid);
    if (d) d.evMs += w.end - w.start;
  }
  const all = [...samples.wk, ...samples.fri];
  const allFit = fitTimes(all, DEFAULTS.wk);
  const model = { builtAt: now, samples: all.length, event: { a: ev.a, b: ev.b, samples: ev.samples }, types: {} };
  for (const t of ['wk', 'fri']) {
    const ds = [...days.values()].filter((d) => d.type === t && d.n >= 5);
    const fit = samples[t].length >= 25 ? fitTimes(samples[t], allFit) : allFit;
    const starts = ds.map((d) => (d.first - d.mid) / MIN);
    const ends = ds.map((d) => (d.last - d.mid) / MIN);
    const bfs = ds.filter((d) => d.last - d.first > HOUR && d.active > 0)
      .map((d) => clamp((d.last - d.first) / d.active, 1, 3));
    // Event time per minute of Rip-Live time on a typical day (for events not
    // on the board yet).
    const shares = ds.filter((d) => d.active > HOUR)
      .map((d) => clamp(d.evMs / Math.max(30 * MIN, d.active - d.evMs), 0, 1.5));
    model.types[t] = {
      base: fit.base,
      per: fit.per,
      start: starts.length >= 2 ? median(starts) : DEFAULTS[t].start,   // minutes after midnight
      end: ends.length >= 2 ? median(ends) : DEFAULTS[t].end,
      bf: bfs.length >= 2 ? median(bfs) : DEFAULTS[t].bf,
      evShare: shares.length >= 3 ? median(shares) : 0.2,
      days: ds.length,
      samples: samples[t].length,
    };
  }
  return model;
}

// ── Estimating ────────────────────────────────────────────────────────────
function slotTime(m, qty) { return m.base + m.per * Math.max(0, (Number(qty) || 1) - 1); }

/** Today's pace vs the model, from the current on-air run (1 = normal).
 *  eventTimes: recent event spot fulfill timestamps — Rip-Live gaps with an event
 *  ripped inside them are skipped, so an event never reads as "slow" slots. */
export function paceFactor(model, recentRecords, now = Date.now(), eventTimes = []) {
  const slots = toSlots((recentRecords || []).filter((r) => r.ts <= now && r.ts >= now - 12 * HOUR));
  const evs = (eventTimes || []).filter((t) => t <= now && t >= now - 12 * HOUR);
  const acts = [...slots, ...evs.map((ts) => ({ ts, qty: 0, ev: true }))].sort((x, y) => x.ts - y.ts);
  const runs = toRuns(acts);
  const run = runs[runs.length - 1];
  if (!run || now - run.end > BREAK_GAP) return 1;
  const m = model.types[run.type];
  let actual = 0, predicted = 0, n = 0;
  for (let i = run.slots.length - 1; i >= 1 && n < 12; i--) {
    const cur = run.slots[i], prev = run.slots[i - 1];
    if (cur.ev || prev.ev) continue;               // event in between → not a clean slot time
    actual += cur.ts - prev.ts;
    predicted += slotTime(m, cur.qty);
    n++;
  }
  if (n < 3 || !predicted) return 1;
  const raw = clamp(actual / predicted, 0.6, 1.8);
  return 1 + (raw - 1) * Math.min(1, n / 12);
}

/** Stream window [start, end] (ms) for the stream day at/after `ts`, skipping
 *  non-stream days. If ts is already inside a window, that window is returned. */
export function windowAt(model, ts) {
  for (let i = 0; i < 9; i++) {
    const probe = laMidnight(ts) + i * DAY + 12 * HOUR; // midday of the i-th day
    const sd = streamDay(probe);
    if (!sd.type) continue;
    const t = model.types[sd.type];
    const start = sd.mid + t.start * MIN;
    const end = sd.mid + t.end * MIN;
    if (ts < end) return { type: sd.type, start, end };
  }
  return null;
}

/** On-air status right now. lastActivity = latest fulfillment (any kind). */
export function liveStatus(model, lastActivity, now = Date.now()) {
  const sd = streamDay(now);
  const recent = lastActivity && now - lastActivity <= BREAK_GAP;
  if (sd.type && recent) return 'live';
  const w = sd.type ? windowAt(model, now) : null;
  if (w && now >= w.start && now < w.end) return 'break';
  return 'off';
}

/**
 * Estimate when each queued slot reaches the top.
 * slots: [{ key, qty, reachedTopAt }] in queue order.
 * ctx: { now, lastActivity, pace, due }
 *   due: events expected to be ripped before the queue moves on —
 *        [{ work (ms), front: bool }] — front = on the overlay right now (ripped
 *        before the top slot); otherwise ripped right after the current top slot.
 * Returns { status, byKey: { key: { mid, lo, hi } }, clearAt } (ms timestamps).
 */
export function estimate(model, slots, ctx = {}) {
  const now = ctx.now || Date.now();
  const status = liveStatus(model, ctx.lastActivity, now);
  const pace = clamp(Number(ctx.pace) || 1, 0.6, 1.8);
  const due = (ctx.due || []).filter((d) => d && d.work > 0);
  const front = due.filter((d) => d.front);
  const after = due.filter((d) => !d.front);
  const sim = (paceMul, useBreaks) => {
    const out = [];
    let w = windowAt(model, now);
    if (!w) return out;
    let cursor = (status === 'live') ? now : Math.max(now, w.start);
    if (status === 'live' && now >= w.end) w = { ...w, end: now + 30 * MIN }; // running late: overtime
    // Advance the clock by `step`, rolling past stream ends into the next window.
    const advance = (step) => {
      while (cursor + step > w.end) {
        step -= Math.max(0, w.end - cursor);
        const next = windowAt(model, w.end + MIN);
        if (!next) break;
        w = next;
        cursor = w.start;
      }
      cursor += step;
    };
    let work = 0;        // Rip-Live work so far
    let known = 0;       // event time already placed (due events)
    let allowed = 0;     // event allowance already added
    for (const d of front) { known += d.work; advance(d.work * paceMul); }
    slots.forEach((s, i) => {
      out.push(cursor);
      const t = model.types[w.type];
      let svc = slotTime(t, s.qty) * pace * paceMul;
      if (i === 0 && status === 'live' && !front.length) {
        const since = Math.max(s.reachedTopAt || 0, ctx.lastActivity || 0);
        if (since) svc = Math.max(svc * 0.25, svc - (now - since));
      }
      work += svc;
      const ramp = Math.min(1, work / (3 * HOUR)); // short waits rarely hit a break / surprise event
      const g = useBreaks ? 1 + (t.bf - 1) * ramp : 1;
      let step = svc * g;
      // Events not on the board yet: typical event load, minus due events already counted.
      if (useBreaks) {
        const want = (t.evShare || 0) * work * ramp - known;
        if (want > allowed) { step += want - allowed; allowed = want; }
      }
      advance(step);
      if (i === 0) for (const d of after) { known += d.work; advance(d.work * paceMul); }
    });
    if (!slots.length) for (const d of after) advance(d.work * paceMul);
    out.push(cursor); // time the whole queue clears
    return out;
  };
  const mid = sim(1, true);
  const lo = sim(0.8, false);
  const hi = sim(1.25, true);
  const byKey = {};
  slots.forEach((s, i) => {
    if (mid[i] == null) return;
    byKey[s.key] = { mid: Math.round(mid[i]), lo: Math.round(Math.min(lo[i], mid[i])), hi: Math.round(Math.max(hi[i], mid[i])) };
  });
  const n = slots.length;
  const clearAt = mid[n] != null ? { mid: Math.round(mid[n]), lo: Math.round(Math.min(lo[n], mid[n])), hi: Math.round(Math.max(hi[n], mid[n])) } : null;
  return { status, pace, byKey, clearAt };
}

/** Human label for an estimate { mid, lo, hi }, relative to now (Pacific). */
export function etaLabel(e, now = Date.now()) {
  if (!e) return '';
  const span = e.mid - now;
  if (span <= 60 * 1000) return 'any minute';
  const sameDay = laParts(e.hi).ymd === laParts(now).ymd;
  if (sameDay && e.hi - now <= 3 * HOUR) {
    const f = (ms) => {
      const m = Math.max(1, Math.round(ms / MIN));
      return m < 60 ? `${m}m` : `${Math.floor(m / 60)}h${m % 60 ? ' ' + (m % 60) + 'm' : ''}`;
    };
    const a = f(e.lo - now), b = f(e.hi - now);
    return a === b ? `~${a}` : `~${a}–${b}`;
  }
  const t = new Date(e.mid).toLocaleTimeString('en-US', { timeZone: TZ, hour: 'numeric', minute: '2-digit' });
  const pm = laParts(e.mid), pn = laParts(now);
  if (pm.ymd === pn.ymd) return `today ~${t}`;
  if (pm.ymd === laParts(now + DAY).ymd) return `tomorrow ~${t}`;
  const wd = new Date(e.mid).toLocaleDateString('en-US', { timeZone: TZ, weekday: 'short' });
  return `${wd} ~${t}`;
}

/** Conservative single-number label for viewers (overlay / public page): a point
 *  a quarter of the way from the estimate toward its upper end, rounded UP to the
 *  next 5 minutes (next 15 for waits past 2 h; clock times rounded up to :15). */
export function publicLabel(e, now = Date.now(), isTop = false) {
  if (!e) return '';
  if (isTop) return 'up now';
  const t = e.mid + 0.25 * Math.max(0, e.hi - e.mid);
  const mins = (t - now) / MIN;
  if (mins <= 5) return 'up next';
  const sameDay = laParts(t).ymd === laParts(now).ymd;
  if (sameDay && mins <= 180) {
    const m = mins <= 120 ? Math.ceil(mins / 5) * 5 : Math.ceil(mins / 15) * 15;
    return m < 60 ? `~${m} min` : `~${Math.floor(m / 60)}h${m % 60 ? ' ' + (m % 60) + 'm' : ''}`;
  }
  const q = Math.ceil(t / (15 * MIN)) * 15 * MIN;
  const clock = new Date(q).toLocaleTimeString('en-US', { timeZone: TZ, hour: 'numeric', minute: '2-digit' }).replace(':00', '');
  if (laParts(q).ymd === laParts(now).ymd) return `today ~${clock}`;
  if (laParts(q).ymd === laParts(now + DAY).ymd) return `tomorrow ~${clock}`;
  const wd = new Date(q).toLocaleDateString('en-US', { timeZone: TZ, weekday: 'short' });
  return `${wd} ~${clock}`;
}
