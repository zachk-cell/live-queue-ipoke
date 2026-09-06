// Discord mirror.
//
// Keeps a single auto-updating "live queue" message in a channel, and gives
// mods slash-style commands to fulfill / bump slots without opening the web app.
//
// Stays dormant unless DISCORD_ENABLED=true and a bot token + channel id are set.
// The web dashboard is the primary control surface; this mirrors it so mods and
// buyers can follow along in Discord.

import {
  Client,
  GatewayIntentBits,
  Events,
  REST,
  Routes,
  SlashCommandBuilder,
  PermissionFlagsBits,
} from 'discord.js';

const MAX_SHOWN = 40; // max upcoming slots to list; also length-guarded (see CHAR_BUDGET)
// Discord rejects messages over 2000 chars. We fill up to MAX_SHOWN names but stop
// early if we'd approach this budget, leaving headroom for the header + footer, so
// the message can never overflow no matter how long the usernames are.
const CHAR_BUDGET = 1850;

export function discordEnabled() {
  return process.env.DISCORD_ENABLED === 'true' && !!process.env.DISCORD_BOT_TOKEN;
}

function buildQueueMessage(queue) {
  const snap = queue.snapshot();
  if (!snap.stats.live) {
    return '**🔴 Queue closed**\n_The live isn\'t running right now. The queue reopens when the next live starts._\n' +
      `_Updated <t:${Math.floor(Date.now() / 1000)}:R>_`;
  }
  const q = snap.queue;
  const lines = [];
  lines.push(`**🟢 LIVE QUEUE — ${q.length} in line**`);
  if (snap.stats.priorityCount) {
    lines.push(`⭐ ${snap.stats.priorityCount} priority`);
  }
  lines.push('');
  if (!q.length) {
    lines.push('_Queue is empty — waiting on orders._');
  } else {
    // Public-safe: username + position + priority only. No totals, items, or
    // order counts — matches the public web view. Fill up to MAX_SHOWN names,
    // but stop early if we'd approach the character budget so Discord never
    // rejects the message for being too long.
    let used = lines.join('\n').length; // chars already used by the header
    let shown = 0;
    for (const e of q) {
      const star = e.bumped ? '🔺' : e.isPriority ? '⭐' : '　';
      const line = `\`${String(e.position).padStart(2)}\` ${star} **${e.buyer}**`;
      if (shown >= MAX_SHOWN || used + line.length + 1 > CHAR_BUDGET) break;
      lines.push(line);
      used += line.length + 1;
      shown++;
    }
    if (q.length > shown) lines.push(`_…and ${q.length - shown} more_`);
  }
  lines.push('');
  lines.push('_Order totals and personal details are private and never shown here._');
  lines.push(`_Updated <t:${Math.floor(Date.now() / 1000)}:R>_`);
  return lines.join('\n');
}

// Piggy Bank tracker — a SEPARATE auto-updating message so it can be paused or
// removed without touching the queue message. Returns null when the store has no
// tracked variants (e.g. the PBCC store), in which case no piggy message is
// created at all. Always rendered regardless of live/offline state, since the
// counts are cumulative and persist across streams.
function buildPiggyMessage(queue) {
  const snap = queue.snapshot();
  const vs = snap.variants || [];
  if (!vs.length) return null;
  const title = (snap.tracker && snap.tracker.title) || '🐷 Piggy Bank Tracker';
  const subtitle = snap.tracker ? snap.tracker.subtitle : 'Persists Across Streams Until Hit';
  const lines = [];
  lines.push(`**${title}**`);
  if (subtitle) lines.push(`_${subtitle}_`);
  lines.push('');
  const width = Math.max(...vs.map((v) => String(Number(v.count) || 0).length));
  for (const v of vs) {
    const n = String(Number(v.count) || 0).padStart(width);
    lines.push(`\`${n}\` ${v.label}`);
  }
  lines.push('');
  lines.push(`_Updated <t:${Math.floor(Date.now() / 1000)}:R>_`);
  return lines.join('\n');
}

// Event side-queue post (iPoke). A SEPARATE message that only exists while an
// event is toggled "active" on the admin panel; it's removed automatically when
// the event is un-toggled or ripped. Returns null when no event is active, which
// tells refresh() to delete the message if one is currently up.
function buildEventMessage(queue) {
  const snap = queue.snapshot();
  const ev = snap.activeEvent; // full event queue object, or null
  if (!ev) return null;
  const lines = [];
  const type = ev.type === 'wta' ? 'WTA' : 'Quack Pack';
  lines.push(`**🎟 ${ev.title}**  \`${type}\``);
  if (ev.description) lines.push(`_${ev.description}_`);
  if (ev.totalSpots > 0) {
    lines.push(ev.soldOut
      ? `**SOLD OUT** — ${ev.spotsOrdered}/${ev.totalSpots} spots`
      : `${ev.spotsOrdered}/${ev.totalSpots} spots — **${ev.spotsRemaining} left**`);
  } else {
    lines.push(`${ev.spotsOrdered} spots ordered`);
  }
  lines.push('');
  if (!ev.entries.length) {
    lines.push('_No spots yet._');
  } else {
    let used = lines.join('\n').length;
    let shown = 0;
    for (const e of ev.entries) {
      const sp = e.spots > 1 ? ` ×${e.spots}` : '';
      const line = `\`${String(e.position).padStart(2)}\` **${e.buyer}**${sp}`;
      if (shown >= MAX_SHOWN || used + line.length + 1 > CHAR_BUDGET) break;
      lines.push(line);
      used += line.length + 1;
      shown++;
    }
    if (ev.entries.length > shown) lines.push(`_…and ${ev.entries.length - shown} more_`);
  }
  lines.push('');
  lines.push('_Pulled in the order spots were bought. Order details are private._');
  lines.push(`_Updated <t:${Math.floor(Date.now() / 1000)}:R>_`);
  return lines.join('\n');
}

export async function startDiscord(queue) {
  if (!discordEnabled()) {
    console.log('[discord] disabled (set DISCORD_ENABLED=true + token to enable)');
    return null;
  }

  const client = new Client({ intents: [GatewayIntentBits.Guilds] });
  const channelId = process.env.DISCORD_CHANNEL_ID;
  let liveMessage = null;
  let pigMessage = null;
  let eventMessage = null; // iPoke: the active event's side-queue post (or none)
  let dirty = false;

  const commands = [
    new SlashCommandBuilder().setName('queue').setDescription('Show/refresh the live queue'),
    new SlashCommandBuilder()
      .setName('fulfill')
      .setDescription('Mark the top (or a named buyer) slot fulfilled')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageMessages)
      .addStringOption((o) => o.setName('buyer').setDescription('Buyer name (optional; defaults to top)')),
    new SlashCommandBuilder()
      .setName('bump')
      .setDescription('Bump a buyer to the top')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageMessages)
      .addStringOption((o) => o.setName('buyer').setDescription('Buyer name').setRequired(true)),
  ].map((c) => c.toJSON());

  client.once(Events.ClientReady, async (c) => {
    console.log(`[discord] logged in as ${c.user.tag}`);
    try {
      const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_BOT_TOKEN);
      await rest.put(Routes.applicationCommands(c.user.id), { body: commands });
    } catch (e) {
      console.warn('[discord] command registration failed:', e.message);
    }
    await refresh(true);
  });

  function findByBuyer(name) {
    const q = queue.activeQueue();
    if (!name) return q[0];
    const lower = name.toLowerCase();
    return q.find((e) => e.buyer.toLowerCase().includes(lower));
  }

  client.on(Events.InteractionCreate, async (interaction) => {
    if (!interaction.isChatInputCommand()) return;
    try {
      if (interaction.commandName === 'queue') {
        await interaction.reply({ content: 'Refreshing…', ephemeral: true });
        await refresh(true);
      } else if (interaction.commandName === 'fulfill') {
        const target = findByBuyer(interaction.options.getString('buyer'));
        if (!target) return interaction.reply({ content: 'No matching slot.', ephemeral: true });
        queue.markFulfilled(target.key);
        await interaction.reply({ content: `✅ Fulfilled **${target.buyer}**.`, ephemeral: true });
      } else if (interaction.commandName === 'bump') {
        const target = findByBuyer(interaction.options.getString('buyer'));
        if (!target) return interaction.reply({ content: 'No matching slot.', ephemeral: true });
        queue.bump(target.key);
        await interaction.reply({ content: `🔺 Bumped **${target.buyer}** to top.`, ephemeral: true });
      }
    } catch (e) {
      console.error('[discord] interaction error:', e.message);
    }
  });

  async function refresh(force = false) {
    if (!channelId) return;
    if (!force && !dirty) return;
    dirty = false;
    try {
      const channel = await client.channels.fetch(channelId);
      const queueContent = buildQueueMessage(queue);
      const pigContent = buildPiggyMessage(queue); // null when no tracked variants
      const eventContent = buildEventMessage(queue); // null when no active event

      if (!liveMessage) {
        // Fresh start (e.g. after a redeploy): clear any previous messages this
        // bot left behind so the channel keeps a single queue message plus, on
        // stores that track variants, a single Piggy Bank message above it.
        try {
          const recent = await channel.messages.fetch({ limit: 25 });
          for (const m of recent.values()) {
            if (m.author.id === client.user.id) { try { await m.delete(); } catch {} }
          }
        } catch {}
        pigMessage = null;
        eventMessage = null;
        // Post the Piggy Bank tracker first so it sits ABOVE the queue message.
        if (pigContent) {
          pigMessage = await channel.send(pigContent);
          try { await pigMessage.pin(); } catch {}
        }
        liveMessage = await channel.send(queueContent);
        try { await liveMessage.pin(); } catch {}
        // An event that's already active on boot gets its own pinned post below.
        if (eventContent) {
          eventMessage = await channel.send(eventContent);
          try { await eventMessage.pin(); } catch {}
        }
      } else {
        await liveMessage.edit(queueContent);
        if (pigContent) {
          if (pigMessage) {
            try { await pigMessage.edit(pigContent); } catch { pigMessage = null; }
          } else {
            // Variants appeared after the queue message was already up.
            pigMessage = await channel.send(pigContent);
            try { await pigMessage.pin(); } catch {}
          }
        }
        // Event post lifecycle: create when an event goes active, edit while it
        // stays active, and delete when it's un-toggled or ripped.
        if (eventContent) {
          if (eventMessage) {
            try { await eventMessage.edit(eventContent); } catch { eventMessage = null; }
          } else {
            eventMessage = await channel.send(eventContent);
            try { await eventMessage.pin(); } catch {}
          }
        } else if (eventMessage) {
          try { await eventMessage.unpin(); } catch {}
          try { await eventMessage.delete(); } catch {}
          eventMessage = null;
        }
      }
    } catch (e) {
      console.warn('[discord] refresh failed:', e.message);
      liveMessage = null;
      pigMessage = null;
      eventMessage = null;
    }
  }

  // Coalesce rapid changes: mark dirty on change, flush on an interval so we
  // never hit Discord's edit rate limits during a busy live.
  queue.on('change', () => { dirty = true; });
  const timer = setInterval(() => refresh(false), 4000);
  timer.unref?.();

  await client.login(process.env.DISCORD_BOT_TOKEN);
  return client;
}
