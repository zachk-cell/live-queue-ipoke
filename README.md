# TCG Live Queue

An automated running-order queue for selling trading cards on TikTok Shop lives.
Orders are pulled in automatically (no typing), grouped and prioritized by your
rules, and shown as a self-updating live feed on a **web dashboard** and mirrored
into **Discord**. Built to handle a busy 6-hour live (hundreds of orders).

## What it does

- **Auto-ingest** — while a live is running the app polls the TikTok Shop Order
  API every ~10s, fetches each new paid order's detail, and drops it into the
  queue. Nothing is typed by hand. Ingest only runs while you're Live.
- **Item-driven priority** — if an order contains one of your configured
  *priority items* (e.g. "break", "slab", "PSA 10"), that buyer jumps to the top.
  Multiple priority buyers all sit at the top, ordered by who bought first.
  Matching is a contiguous, in-order substring (an item counts only if its name
  contains the exact phrase). A set of **always-on** priority phrases can also be
  baked in per store via `PRIORITY_ITEMS_EXTRA`.
- **Buyer slot-merging** — if a buyer orders again *before* their slot is
  fulfilled, the new order is merged into their existing slot (items + total
  combined). They are **not** added to the queue a second time. After a slot is
  fulfilled, a later order from that buyer opens a fresh slot.
- **Queue & fulfillment metrics** — each slot shows time-in-queue; the stats bar
  shows the average wait and the average fulfillment time (how long an order sat
  at #1 before being fulfilled). The live "at top" timer runs only for the current
  #1 and clears if a bump/priority pushes a slot down.
- **Display name vs @username** — the queue, labels, and public page show the
  buyer's public display name; the admin expanded view also shows their actual
  @username for cross-referencing against TikTok. The username never appears
  publicly.
- **🐷 Piggy Bank per-variant counters** *(store-specific, via `TRACKED_VARIANTS`)*
  — an admin card + a public tracker tally each tracked bundle variant, +1 per
  unit when an order reaches the top. Counts are cumulative and persist across
  streams; each is manually editable (Save) and resettable (with a second
  confirmation), and every edit/reset is written to a recoverable log. Also
  mirrored as a separate always-visible pinned message in Discord.
- **One-tap fulfill** — mark a slot done from the dashboard or Discord; it drops
  off the active queue into "recently fulfilled".
- **Manual bump** — force any buyer to the very top when you need to.
- **Durable** — queue, history, priority config, Piggy Bank counts, and TikTok
  tokens are written to `data/`. On Render, mount a **persistent disk** at
  `/opt/render/project/src/data` so all of it survives deploys and restarts.

## Try it right now (simulator — no accounts needed)

```bash
npm install
npm run demo
```

Open **http://localhost:3000**. Fake orders start flowing every ~2.5s. The
simulator intentionally creates repeat buyers (watch slots merge) and priority
items (watch them jump to the top). Default priority triggers are `break` and
`slab` — click **⭐ Priority items** to change them.

## How the queue orders slots

Top of the queue = next to handle:

1. A manually **bumped** slot.
2. **Priority** slots (contain a priority item) before normal slots.
3. Within priority: earliest priority-item purchase first.
   Within normal: earliest order first.

## Going live with real TikTok Shop orders

1. **Register a developer app** in the [TikTok Shop Partner Center](https://partner.tiktokshop.com/).
   Create your app and enable **both** the **Order Information** (`seller.order.info`)
   and **Shop Authorization** (`seller.authorization.info`) scopes — with only the
   first, connecting fails with `105005 access scope`. Set the app's **Redirect URL**
   to `https://YOUR-DOMAIN/auth/tiktok/callback`. (The region you pick in Partner
   Center is permanent, so choose your shop's region.)
2. **Host this app** somewhere with a public HTTPS URL (Render recommended — see
   the Setup SOP). It polls TikTok's Order API on a timer, so it just needs to be
   online during lives; no inbound webhook is required.
3. Copy `.env.example` to `.env` and fill in:
   - `TIKTOK_ENABLED=true`, `SIMULATE=false`
   - `TIKTOK_APP_KEY`, `TIKTOK_APP_SECRET` (from your app)
   - `TIKTOK_AUTH_URL` (the Partner Center authorize link — powers the Reconnect button)
4. Have the shop owner open the authorize link and **Confirm to install**. The app
   captures the shop cipher + tokens. Then seed `TIKTOK_REFRESH_TOKEN` and
   `TIKTOK_SHOP_CIPHER` into the env — this is the **durability seed** that lets the
   app auto-reconnect on a fresh instance without re-authorizing.
5. Restart. Real orders now flow into the queue automatically while you're Live.

> The TikTok field names can vary slightly by API version. If a live order shows
> the wrong buyer/total — or a variant isn't being captured — adjust the mapping in
> `tiktok.js` → `normalizeOrder()`. The item variant lives in TikTok's `sku_name`
> field, which `normalizeOrder()` folds into the item name so priority/variant
> matching can see it.

## Adding the Discord mirror (optional)

1. Create a bot at the [Discord Developer Portal](https://discord.com/developers/applications),
   copy its **token**, and invite it to your server with permission to read/send
   messages in one channel.
2. In `.env` set `DISCORD_ENABLED=true`, `DISCORD_BOT_TOKEN=...`,
   `DISCORD_CHANNEL_ID=...` (right-click the channel → Copy ID; enable Developer
   Mode in Discord settings first).
3. Restart. The bot posts and pins a live-updating queue message and adds
   `/queue`, `/fulfill`, and `/bump` commands for your mods. On stores that set
   `TRACKED_VARIANTS`, it also keeps a **separate, always-visible pinned message**
   — a 🐷 Piggy Bank Tracker of the variant counts — kept apart from the queue
   message so it's easy to pause or remove.

## Store-specific features (optional env)

The same codebase powers multiple stores; two optional env vars switch on
per-store behavior. Leave them empty (`[]`) and the features stay dormant.

- `TRACKED_VARIANTS` — JSON array of Piggy Bank counters:
  `[{"id","label","product","variant"}]`. `product` and `variant` are lowercased
  substrings that **both** must appear in an item's name for a unit to count.
  The counts themselves are stored in `data/config.json` (persistent disk), not
  in the env — the env only defines *which* variants to track.
- `PRIORITY_ITEMS_EXTRA` — JSON array of always-on priority trigger phrases,
  merged on top of the admin-panel priority words and surviving redeploys.

## Deploying updates

Auto-deploy may not be wired (if the repo isn't connected through the host's
GitHub App, a plain `git push` won't deploy). On Render, after committing, use
**Manual Deploy → Deploy latest commit**, then confirm the served page actually
changed. Saving env vars also triggers a deploy.

## Project layout

```
server.js          wiring: web + API + poller + discord + simulator
queue.js           the queue engine (grouping, priority, metrics, variants, persistence)
tiktok.js          TikTok Shop OAuth + Order API poller + normalizeOrder()
discord.js         Discord live-mirror bot (+ Piggy Bank tracker message)
simulator.js       fake order feed for the demo
index.html         the admin dashboard
public.html        the public buyer-facing view (+ Piggy Bank tracker)
guide.html         in-app operator guide
sandbox.html       self-contained practice sandbox (fake data, no backend)
data/              persisted state: queue-state.json, config.json, history.json, tiktok-tokens.json
```

## Notes on scale

At ~400 orders / 6 hours you're near one order per minute — far under TikTok's
API limits (~50 req/s, ~1,000/day per endpoint) and trivial for the dashboard.
The Discord mirror batches its edits every few seconds to stay clear of Discord's
rate limits during busy stretches.
