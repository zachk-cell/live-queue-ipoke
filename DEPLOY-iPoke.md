# iPoke Live Queue — Setup & Deploy Guide

This is the **third** queue (after PBCC and Poke Pig). It runs the **same codebase**
as the other two, switched into "iPoke mode" by environment variables. Nothing
here changes how PBCC or Poke Pig behave.

## What's different about iPoke

- **Shopify is the order source** (not TikTok directly). TikTok orders already sync
  into Shopify, so Shopify is the single, complete source.
- **Perpetual queue** — it never turns off. Orders flow in 24/7, including while the
  stream is offline. There's no "Go Live" button.
- **Events / side-queues** — Quack Pack and WTA events pull their spots out of the
  main queue into their own side-queues, toggled onto the overlay/Discord on demand.
- **Order combining is automatic ("combine until top")** — a buyer's repeat
  orders merge into their slot until it reaches the top; after that the bag is
  being packed, so a new order takes its own position and the top slot flashes a
  "they ordered again" alert. Plus a manual-combine tool for edge cases.
- **Name overrides, prep state, and a TT/SF source badge.**

---

## 1. Create the repo

The three stores are separate repos so each deploys independently. Create a **new,
empty GitHub repo** (e.g. `live-queue-ipoke`) and upload this entire folder to it
(GitHub → Add file → Upload files). It's the same set of files as the other stores
plus `shopify.js` and `overlay.html`.

## 2. Create the Render service

1. Render → **New → Web Service**, connect the `live-queue-ipoke` repo.
2. Runtime **Node**, Build command `npm install`, Start command `npm start`.
3. Plan: **Starter** (needed for the persistent disk + always-on).
4. **Add a Persistent Disk** — this is essential for a perpetual queue:
   - Mount path: `/opt/render/project/src/data`
   - Size: 1 GB is plenty.
   - (This is where the queue, events, name overrides and settings are stored so
     they survive restarts/redeploys.)

## 3. Environment variables

Set these in Render → the service → **Environment**. Copy `.env.example` for the
full annotated list; the essentials:

| Key | Value |
|---|---|
| `PERPETUAL` | `true` (also sets the "combine until top" behaviour) |
| `PANEL_PASSWORD` | a strong admin password |
| `ADMIN_PATH` | a secret path segment for the admin panel (e.g. a random word) |
| `SESSION_SECRET` | a long random string (keeps logins valid across restarts) |
| `GUIDE_PASSWORD` | shareable password for the operator guide (optional) |
| `SIMULATE` | `true` to demo without Shopify; set `false` for real orders |

**Shopify (add once the token exists — see step 5):**

| Key | Value |
|---|---|
| `SHOPIFY_ENABLED` | `true` |
| `SHOPIFY_SHOP` | `the-store.myshopify.com` |
| `SHOPIFY_ADMIN_TOKEN` | the custom-app Admin API token (secret — you paste it) |

Leave all the `TIKTOK_*` keys unset — iPoke does not talk to TikTok directly.

## 4. First boot — run it on the simulator

Before Shopify is connected, set `SIMULATE=true` and deploy. You'll get a fully
working demo: a perpetual queue populating with fake orders across TT/SF sources,
two demo events (a Quack Pack + a WTA), vintage priority, and the overlay. Use this
to train the team and confirm everything looks right. Then flip `SIMULATE=false`
when the real Shopify token is in.

## 5. Connect Shopify (when your friend provides access)

Your friend (the store owner) creates a **custom app** in Shopify admin:

1. Shopify admin → **Settings → Apps and sales channels → Develop apps → Create an app.**
2. **Configure Admin API scopes:** enable `read_orders` and `read_customers`.
3. Request/approve **Protected Customer Data** access (needed to read customer names).
4. **Install** the app, then copy the **Admin API access token**.
5. In Render, set `SHOPIFY_SHOP`, `SHOPIFY_ADMIN_TOKEN`, `SHOPIFY_ENABLED=true`,
   `SIMULATE=false`, and redeploy.

To sanity-check the connection, an admin can hit `/api/shopify-status` and
`/api/shopify-raw-order` (logged into the panel) — the latter shows how a
TikTok-synced order vs a native web order actually looks, with names masked. That
also confirms the exact TikTok channel string so the TT/SF badge is right.

## 6. OBS overlay

Add a **Browser Source** in OBS pointing at:

```
https://<your-app>.onrender.com/overlay
```

Optional query params:

- `?rows=12` — how many positions to show
- `?side=left` or `?side=right`
- `?scale=1.2` — size it up/down
- `?bg=0` — remove the dark backing panel (pure transparent)
- `?title=Live%20Queue` — custom title

The overlay shows the live queue (name + position + TT/SF badge) and automatically
switches to an event's side-queue when you toggle that event "on overlay" from the
admin panel.

## 7. Discord (optional)

Same as the other stores: set `DISCORD_ENABLED=true`, `DISCORD_BOT_TOKEN`,
`DISCORD_CHANNEL_ID`. The bot keeps the main queue pinned, and when you toggle an
event active it posts a **separate pinned event post** that's removed automatically
when the event is un-toggled or ripped.

## 8. Day-to-day (admin panel)

- **Events:** the "🎟 Events & side-queues" panel — Add event (Quack/WTA, title,
  description, total spots, keywords), toggle it onto the overlay, Rip it when done,
  or Edit/Delete. Expand any event to see its ordered spots.
- **Combining:** automatic — repeat orders merge until the buyer hits the top,
  then their slot flashes **"＋ ORDERED AGAIN"** and the new order sits in its own
  position (nothing to configure).
- **Per order:** ✏️ rename the buyer, 📦 mark prepped, ⛓ select two+ orders from the
  same buyer then **Combine** (manual override).
- **Vintage / priority:** the ⭐ Priority items button — add `vintage` (or any word)
  to jump matching orders to the front.

---

### Notes / still open (from the spec)

- Confirm WTA rips all-at-once (like Quack) vs as orders come in.
- The "ship sealed" side-queue is not built yet (parked as a future item).
- Source badge is TT / SF — change `SF` in the code if the team prefers `SH`/`WEB`.
