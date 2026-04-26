# Discord Voice-Bot Setup

Second entry point next to the browser UI. Same agent, same prompt, same
Qontext backend — it just rides Discord voice instead of a browser WebSocket.
The two can run side-by-side.

## 1. Create the Discord application

1. Go to <https://discord.com/developers/applications> and click **New Application**.
   Name it whatever (e.g. "Qontext Voice Agent").
2. In the left sidebar, open **Bot**. Click **Reset Token** and copy the token —
   you'll only see it once. This is your `DISCORD_BOT_TOKEN`.
3. Still on the **Bot** page, scroll to **Privileged Gateway Intents** and
   enable:
   - **Server Members Intent** (so the bot can see who's in voice)
   - You do **not** need *Message Content* — slash commands carry their own payload.
   - **Voice State** is non-privileged and on by default; nothing to flip.
4. Save changes.

## 2. Bot permissions & invite URL

In the sidebar, open **OAuth2 → URL Generator**.

**Scopes:**
- `bot`
- `applications.commands`

**Bot Permissions:**
- View Channels
- Send Messages
- Use Slash Commands
- Connect (voice)
- Speak (voice)
- Use Voice Activity

Copy the generated URL at the bottom. It will look like:

```
https://discord.com/api/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=3214336&scope=bot+applications.commands
```

Open that URL in a browser, pick the server (you must have *Manage Server*
on it), and authorize. The bot will now show up offline in the member list.

## 3. (Optional but recommended) Get your guild ID

Guild-scoped slash commands sync **instantly**. Global commands can take up
to an hour. For a hackathon you want guild-scoped.

1. In Discord, **User Settings → Advanced → Developer Mode → On**.
2. Right-click your server icon → **Copy Server ID** → that's `DISCORD_GUILD_ID`.

## 4. Configure `.env.local`

Add to `/Users/jasperkallflez/caffeinate/voice-agent/.env.local` (NOT `.env`,
which is committed with placeholders):

```env
# Discord
DISCORD_BOT_TOKEN=MTI3...your_real_token...XYZ
DISCORD_GUILD_ID=123456789012345678        # optional but recommended
DISCORD_IDLE_SECONDS=45                    # auto-leave after N seconds of silence
DISCORD_MAX_CALL_SECONDS=600               # hard cap per call
```

`GRADIUM_API_KEY` and `LLM_API_KEY` should already be there from the browser
setup — the Discord bot reuses them.

## 5. Install & run

```bash
cd /Users/jasperkallflez/caffeinate/voice-agent
uv sync
uv run python discord_bot.py
```

You should see `logged in as <BotName>#1234`. The bot will appear online in
your server.

## 6. Usage

In any text channel where the bot can post:

- `/call` — bot joins the voice channel **you're currently in** and starts
  the call. It pulls the next pending question from `qontext_tools` and
  posts a context line in the text channel ("Calling about Q2 revenue
  targets…").
- `/end` — bot leaves cleanly.
- `/status` — show what's queued and whether a call is in progress.

The bot uses the same `submit_answer` tool as the browser. When it's called,
the answer is appended to `submitted_answers.jsonl` and a confirmation is
posted in the text channel.

## 7. Run alongside the browser server

The two processes are independent. In one terminal:

```bash
uv run uvicorn main:app --reload --port 8001
```

In another:

```bash
uv run python discord_bot.py
```

Both share `qontext_tools`'s audit log, so an answer captured via Discord
counts as "answered" for the next browser-launched call too.

## Troubleshooting

- **`DISCORD_BOT_TOKEN not set`** — token isn't in `.env.local` or you typoed
  the variable name.
- **Bot joins voice but never speaks** — check `GRADIUM_API_KEY` /
  `LLM_API_KEY`. Run the browser version first to confirm gradbot works at
  all.
- **`opus` errors on macOS** — install opus: `brew install opus`. PyNaCl
  needs libsodium, which it bundles, so that should be fine.
- **`discord.py is conflicting`** — `pip uninstall discord.py` (or
  `uv remove discord.py`). Keep only `py-cord`.
- **Slash commands missing** — set `DISCORD_GUILD_ID` for instant sync. Or
  wait up to an hour for global sync, then `/` the bot in any channel.
- **Bot disconnects after ~25s** — usually a token / intent issue. Confirm
  *Server Members Intent* is enabled on the dev portal.
