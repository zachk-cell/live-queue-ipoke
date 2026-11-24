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

/** Learn the model from fulfillment records [{ ts, qty, key? }]. */
export function learn(records, now = Date.now()) {
  const slots = toSlots((records || []).filter((r) => r.ts >= now - LOOKBACK && r.ts <= now));
  const runs = toRuns(slots);
  const samples = { wk: [], fri: [] };
  for (const r of runs) {
    for (let i = 1; i < r.slots.length; i++) {
      const gap = r.slots[i].ts - r.slots[i - 1].ts;
      if (gap > 0 && gap <= MAX_SAMPLE) samples[r.type].push({ gap, qty: r.slots[i].qty });
    }
  }
  // Day schedule + break factor, per stream day.
  const days = new Map();
  for (const r of runs) {
    const k = r.type + ':' + r.mid;
    const d = days.get(k) || { type: r.type, mid: r.mid, first: r.start, last: r.end, active: 0, n: 0 };
    d.first = Math.min(d.first, r.start); d.last = Math.max(d.last, r.end);
    d.active += r.end - r.start; d.n += r.slots.length;
    days.set(k, d);
  }
  const all = [...samples.wk, ...samples.fri];
  const allFit = fitTimes(all, DEFAULTS.wk);
  const model = { builtAt: now, samples: all.length, types: {} };
  for (const t of ['wk', 'fri']) {
    const ds = [...days.values()].filter((d) => d.type === t && d.n >= 5);
    const fit = samples[t].length >= 25 ? fitTimes(samples[t], allFit) : allFit;
    const starts = ds.map((d) => (d.first - d.mid) / MIN);
    const ends = ds.map((d) => (d.last - d.mid) / MIN);
    const bfs = ds.filter((d) => d.last - d.first > HOUR && d.active > 0)
      .map((d) => clamp((d.last - d.first) / d.active, 1, 3));
    model.types[t] = {
      base: fit.base,
      per: fit.per,
      start: starts.length >= 2 ? median(starts) : DEFAULTS[t].start,   // minutes after midnight
      end: ends.length >= 2 ? median(ends) : DEFAULTS[t].end,
      bf: bfs.length >= 2 ? median(bfs) : DEFAULTS[t].bf,
      days: ds.length,
      samples: samples[t].length,
    };
  }
  return model;
}

// ── Estimating ────────────────────────────────────────────────────────────
function slotTime(m, qty) { return m.base + m.per * Math.max(0, (Number(qty) || 1) - 1); }

/** Today's pace vs the model, from the current on-air run (1 = normal). */
export function paceFactor(model, recentRecords, now = Date.now()) {
  const slots = toSlots((recentRecords || []).filter((r) => r.ts <= now && r.ts >= now - 12 * HOUR));
  const runs = toRuns(slots);
  const run = runs[runs.length - 1];
  if (!run || now - run.end > BREAK_GAP || run.slots.length < 3) return 1;
  const m = model.types[run.type];
  let actual = 0, predicted = 0, n = 0;
  for (let i = run.slots.length - 1; i >= 1 && n < 12; i--, n++) {
    actual += run.slots[i].ts - run.slots[i - 1].ts;
    predicted += slotTime(m, run.slots[i].qty);
  }
  if (!predicted) return 1;
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
 * ctx: { now, lastActivity, pace }
 * Returns { status, byKey: { key: { mid, lo, hi } }, clearAt: { mid, lo, hi } } (ms timestamps).
 */
export function estimate(model, slots, ctx = {}) {
  const now = ctx.now || Date.now();
  const status = liveStatus(model, ctx.lastActivity, now);
  const pace = clamp(Number(ctx.pace) || 1, 0.6, 1.8);
  const sim = (paceMul, useBreaks) => {
    const out = [];
    let w = windowAt(model, now);
    if (!w) return out;
    // Where the clock starts: now if on air (or inside today's window, on a break);
    // otherwise the next stream start.
    let cursor = (status === 'live') ? now : Math.max(now, w.start);
    if (status === 'live' && now >= w.end) w = { ...w, end: now + 30 * MIN }; // running late: give it overtime
    let work = 0;
    slots.forEach((s, i) => {
      out.push(cursor);
      const t = model.types[w.type];
      let svc = slotTime(t, s.qty) * pace * paceMul;
      if (i === 0 && status === 'live') {
        const since = Math.max(s.reachedTopAt || 0, ctx.lastActivity || 0);
        if (since) svc = Math.max(svc * 0.25, svc - (now - since));
      }
      work += svc;
      // Breaks: short waits rarely hit one, long waits usually do.
      const g = useBreaks ? 1 + (t.bf - 1) * Math.min(1, work / (3 * HOUR)) : 1;
      let step = svc * g;
      // Roll past the end of the stream day into the next stream window.
      while (cursor + step > w.end) {
        const used = Math.max(0, w.end - cursor);
        step -= used;
        const next = windowAt(model, w.end + MIN);
        if (!next) break;
        w = next;
        cursor = w.start;
      }
      cursor += step;
    });
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
