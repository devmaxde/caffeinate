# Discord-Bot Handoff für neue Claude-Code-Instanz

Kopier das hier in den ersten Prompt einer frischen Claude-Code-Session im Verzeichnis `~/caffeinate/voice-agent/`.

---

## Auftrag

Du übernimmst einen Hackathon-Voice-Agent mit fertig implementierter Discord-Integration. Der Code ist da, aber nicht live-getestet. Bring den Discord-Bot online und debugge bis er funktioniert.

## Was schon existiert

- Browser-Voice-Agent läuft auf `http://127.0.0.1:8001` (FastAPI, gradbot, Gradium TTS/STT, Groq Llama 3.1 8b)
- `discord_bot.py` (~280 Zeilen, py-cord 2.7.2) — standalone-Process als zweiter Entry-Point
- `pyproject.toml` mit allen Deps (py-cord, PyNaCl, numpy, scipy) — `uv sync` ist durch
- `qontext_tools.py` — geteilt zwischen Browser & Discord (audit log, fetch_pending_question, submit_answer)
- `mock_questions.json` — 3 Demo-Fragen (Inan, Sarah, Marcus)
- `.env.local` mit Gradium + Groq Keys (gitignored, NICHT anzeigen)
- `DISCORD_SETUP.md` — Setup-Schritte
- `submitted_answers.jsonl` — Audit-Log

## Files die du zuerst lesen sollst

```
README.md
DISCORD_SETUP.md
discord_bot.py
main.py            # damit du Patterns kennst
qontext_tools.py
.env.example
```

## Was noch zu tun ist

1. **`brew install opus`** — libopus fehlt für Voice-Encoding, ohne crasht der Bot bei Connect
2. **User muss Discord-Bot anlegen:**
   - https://discord.com/developers/applications → New Application
   - Sidebar "Bot" → Reset Token → kopieren
   - Privileged Gateway Intents: Server Members ON, Message Content ON
3. **User muss Bot inviten:**
   - Application Client ID kopieren (General Information)
   - URL: `https://discord.com/api/oauth2/authorize?client_id=CLIENT_ID&permissions=3214336&scope=bot+applications.commands`
   - Auf Test-Server authorisieren
4. **User muss `.env.local` ergänzen:**
   ```
   DISCORD_BOT_TOKEN=...
   DISCORD_GUILD_ID=...    # Server-ID, Rechtsklick auf Server → Copy ID (Developer-Mode an)
   ```
5. **Starten:**
   ```bash
   cd ~/caffeinate/voice-agent
   uv run python discord_bot.py
   ```
   Erwartet: Log-Zeile "Bot ready as ..."
6. **Live-Test:**
   - User joined einen Voice-Channel im Server
   - In Text-Channel: `/call`
   - Bot joined, ruft Inan an (Q2 revenue-Frage)
   - Audio fließt durch gradbot → Groq → Gradium → Bot redet zurück
   - User antwortet, `submit_answer` Tool feuert
   - Eintrag in `submitted_answers.jsonl` checken

## Debug-Tabelle

| Symptom | Ursache | Fix |
|---|---|---|
| `OpusNotLoaded` Error | libopus fehlt | `brew install opus` |
| `ImportError: PyNaCl` | Dep fehlt | `uv sync` |
| Bot connected, sagt nix | Intents fehlen | Server Members + Message Content in Discord-Dashboard ON |
| Slash-Commands fehlen | OAuth-Scope falsch | Re-invite mit `applications.commands` scope |
| Bot leavt sofort | `DISCORD_IDLE_SECONDS` zu niedrig | bump auf 60s in `.env.local` |
| Audio knirscht | Resample-Artefakt | scipy installiert? `uv pip list \| grep scipy` |
| Groq 429 rate limit | Free-Tier TPD voll | Modell kleiner: `LLM_MODEL=llama-3.1-8b-instant` |
| Gradium 401 | Falscher Header (Bearer statt x-api-key) | siehe qontext_tools.py |

## Architektur-Recap

- discord_bot.py = separater Prozess, **NICHT** über die FastAPI WS — nutzt `gradbot.run()` direkt
- Audio: PCM end-to-end, py-cord Sink → scipy resample 48k→24k → gradbot Input → Bot Audio → resample 24k→48k → Discord VoiceClient
- Beide Entry-Points (Browser & Discord) teilen sich `qontext_tools.submit_answer` und das Audit-Log

## Wichtige Workflow-Regeln (aus Memory)

- **Sprache:** Deutsch, informell
- **Background-Agents:** für non-triviale Tasks immer `Agent` mit `run_in_background=true` spawnen, damit User parallel weiterchatten kann
- **Memory speichern:** alles Erkenntnisreiche in `~/.claude/projects/-Users-jasperkallflez/memory/`
- **Keys:** niemals echten Key in Chat schreiben, nur in `.env.local`. Gradium-Keys haben gleiches `gsk_`-Prefix wie Groq — leicht zu verwechseln. Gradium nutzt `x-api-key` Header, Groq Bearer.

## Bekannte Quirks

- `discord` Python-Modul ist py-cord, NICHT discord.py — nicht beide installieren
- Mac: `brew install opus` ist nicht in `uv sync` enthalten
- Gradium-Keys können von Groq-Keys verwechselt werden (beide `gsk_...`)
- Groq `llama-3.3-70b-versatile` hat 100K TPD — schnell voll. `llama-3.1-8b-instant` hat 500K
- Discord-Slash-Commands brauchen guild_id für sofortiges Sync (sonst bis zu 1h Delay)

## Memory-Files (für Kontext)

```
~/.claude/projects/-Users-jasperkallflez/memory/project_qontext_voice_agent.md
~/.claude/projects/-Users-jasperkallflez/memory/feedback_parallel_agents.md
~/.claude/projects/-Users-jasperkallflez/memory/feedback_language.md
~/.claude/projects/-Users-jasperkallflez/memory/feedback_save_everything.md
```

Nach jedem Schritt was Erkenntnisreiches dazukommt → Memory updaten.
