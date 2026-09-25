# iPoke Live Queue

> **Which queue is this?** The **iPoke** queue — the advanced fork of the fleet. It is
> its own deployment with its own identity; do not confuse it with PBCC or Poke Pig.
>
> | | |
> |---|---|
> | **Brand (public page)** | iPoke Live Queue |
> | **Platform** | **Shopify** — TikTok Shop *and* web orders both funnel into one Shopify store, so Shopify is the single source and the app polls the **Shopify Admin API** (it does **not** call TikTok's API directly) |
> | **Timezone** | Pacific (PST/PDT, `America/Los_Angeles`) |
> | **Public page** | https://live-queue-ipoke.onrender.com/ (admin at `/ipokeadmin`) |
> | **GitHub repo** | `zachk-cell/live-queue-ipoke` |
> | **Render service** | `srv-daen10ou01pc73f6ltl0` |
> | **Live model** | **Perpetual** — ingest runs 24/7 (`PERPETUAL=true`), not just during a live |
> | **Queue-specific** | Events/Quacks & WTAs (with oversold capture + Shopify catalog sync & periodic auto-sync), Vault tracker, stream overlay (reel mode) |
>
> **The fleet (three separate queues — never cross-feed branding/platform/timezone/credentials):**
>
> | Queue | Brand | Repo | Platform | Timezone |
> |---|---|---|---|---|
> | **PBCC** | PBCC Live Queue | `tcg-live-queue` | TikTok | Eastern |
> | **Poke Pig** | Poke Pig Live Queue | `live-queue-store2` | TikTok | Mountain |
> | **iPoke** (this one) | iPoke Live Queue | `live-queue-ipoke` | Shopify (TikTok+web → Shopify) | Pacific |

An automated running-order queue for a Shopify-based trading-card seller. Both
TikTok Shop and native web orders land in one Shopify store; this app polls the
**Shopify Admin API**, groups and prioritizes by your rules, and shows a
self-updating live feed on a **web dashboard**, a **public page**, a **stream
overlay**, and mirrored into **Discord**. Built to handle a busy multi-hour live
(hundreds of orders), running 24/7.

## What it does

- **Auto-ingest (perpetual)** — the app polls the **Shopify Admin API** every
  ~10s for new paid, non-cancelled orders and drops each into the queue. Because
  iPoke is perpetual, ingest runs 24/7 (not gated on a Live toggle). On boot it
  looks back a few hours so a redeploy never drops orders; ingest is idempotent.
- **Item-driven priority** — orders containing a configured *priority item* jump
  to the top (contiguous, in-order substring match); always-on phrases via
  `PRIORITY_ITEMS_EXTRA`.
- **Events — Quacks & WTAs** *(iPoke-specific)* — line items matching an event's
  keyword are pulled out of the main queue into that event's side-queue, with a
  spot cap. Oversells are **captured and flagged** (never dropped), including
  orders that straddle the cap. Events can be **synced from the Shopify catalog**
  (each event is a variant of the "Quack Pack Series" product) via a review list,
  with a **periodic auto-sync** that pulls in newly-listed events; seat counts
  default from Shopify **on_hand** inventory.
- **Vault per-variant counters** *(via `TRACKED_VARIANTS`)* — admin card + public
  tracker tally each tracked bundle variant, cumulative across streams, editable/
  resettable with a recoverable log, mirrored in Discord. A fulfill-grace sweep
  counts vault units even when an order is fulfilled off the top of the queue.
- **Stream overlay** — an OBS-ready overlay (`/overlay`) with a **reel mode**
  that scrolls the whole queue (and an active event's roster) seamlessly.
- **Apostrophe-safe keyword matching** — event keywords match regardless of
  straight vs curly apostrophes (Shopify titles use curly ones).
- **On-hold handling** — Shopify marks TikTok-channel orders `ON_HOLD` by
  default; iPoke treats that as noise and only flags genuine **web-order** holds.
- **Same-name safety** — only *different* buyers sharing a display name are
  flagged (keyed on buyer id), never one buyer's multiple orders.
- **Queue & fulfillment metrics**, **one-tap fulfill**, **manual bump**,
  **on-hold / set-aside**, **page-size + order-number search**, **prepped**
  tracking, and a **Past Events** archive.
- **Durable** — state written to `data/`; on Render, mount a **persistent disk**
  at `/opt/render/project/src/data`.

## Try it locally (simulator — no accounts needed)

```bash
npm install
npm run demo
```

Open **http://localhost:3000**. Fake orders flow every ~2.5s with repeat buyers
(slots merge) and priority items (jump to top).

## How the queue orders slots

1. A manually **bumped** slot.
2. Priority-item buyers (earliest priority purchase first).
3. Normal buyers (earliest order first).

Event line items are routed out of the main queue into their event side-queue.

## Connecting Shopify (how ingest actually works)

iPoke authenticates to Shopify with a **Dev Dashboard app** using the
**client-credentials** grant (Client ID + Secret exchanged for a short-lived
Admin API token, auto-refreshed). It does **not** use TikTok's API.

1. Create/most-likely reuse the Shopify **Dev Dashboard** app for the store.
2. Grant Admin API scopes: `read_orders`, `read_customers` (required for the
   queue), plus `read_products`, `read_inventory`, `read_locations` (required for
   the event catalog sync + on_hand seat defaults). After changing scopes, the
   app's **installation on the store must be updated/re-authorized** for the new
   token to include them.
3. Set env: `SHOPIFY_ENABLED=true`, `SHOPIFY_SHOP`, `SHOPIFY_CLIENT_ID`,
   `SHOPIFY_CLIENT_SECRET`, `PERPETUAL=true`. (A legacy static
   `SHOPIFY_ADMIN_TOKEN` is still honored if present.)
4. Restart. Orders flow in 24/7.

> Shopify field mapping lives in `shopify.js` → `normalizeShopifyOrder()`. The
> variant is folded into the item name so priority / event / vault matching sees
> it. Admin probes: `/api/shopify-status`, `/api/shopify-scope-check`.

## Discord mirror (optional)

Set `DISCORD_ENABLED=true`, `DISCORD_BOT_TOKEN`, `DISCORD_CHANNEL_ID` and restart;
the bot posts/pins a live queue message, the Vault tracker, and per-event roster
posts (the event currently on the overlay is marked 🔴 LIVE).

## Deploying

Auto-deploy is **off**. Push to `zachk-cell/live-queue-ipoke`, then on Render
service `srv-daen10ou01pc73f6ltl0` use **Manual Deploy → Deploy latest commit**
and confirm the served page updated. State persists across the restart.
Always syntax-check `index.html`'s inline JS before deploying (a JS error there
takes the whole admin page down).

## File map

```
server.js            Express app, routes, socket.io, static pages, event/vault/clip APIs
queue.js             QueueEngine: ingest, ordering, merge, events, vault, oversold, metrics
shopify.js           Shopify Admin API auth (client-credentials) + poller + event catalog sync
tiktok.js            legacy TikTok helpers (not the primary ingest path for iPoke)
discord.js           Discord mirror (queue + Vault + event rosters)
index.html           admin dashboard
public.html          public live page ("iPoke Live Queue")
overlay.html         OBS stream overlay (reel mode)
vault-overlay.html   OBS vault overlay
events-history.html  Past Events archive page
data/                persisted state (queue, config, history, events, past-events)
```
