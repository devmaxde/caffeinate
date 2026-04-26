"""Discord voice-bot entry point for the Qontext voice agent.

This is a SECOND entry point next to the FastAPI server in `main.py`.
Both can run side by side. Same backend (`qontext_tools`), same prompt,
same tool. The only thing that changes is the audio transport: instead
of a browser WebSocket, we bridge a Discord voice channel.

Run:

    cp .env.example .env             # base config
    # edit .env.local: DISCORD_BOT_TOKEN=...
    uv sync
    uv run python discord_bot.py

Slash commands (sync to a guild via `DISCORD_GUILD_ID` for instant updates):
- /call        — bot joins your voice channel and starts the call
- /end         — bot disconnects and ends the call
- /status      — show current pending question / call state

Audio bridging:
    Discord receive: 48kHz stereo s16le PCM (via custom Sink)
        → average channels to mono → resample_poly 48k→24k
        → gradbot.SessionInputHandle.send_audio(pcm_bytes)
    gradbot output: 24kHz mono s16le PCM
        → resample_poly 24k→48k → duplicate to stereo
        → 20ms frame queue → custom discord.AudioSource → vc.play()

We use AudioFormat.Pcm for BOTH directions because:
- gradbot supports it natively (per its README API table)
- avoids re-encoding Opus on either side (simpler + lower latency)
- discord.py's voice receive already gives us decoded PCM

Approx. 280 lines. Keep it boring.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import time
from collections import deque
from typing import Optional

import dotenv
import numpy as np

# IMPORTANT: this `discord` is py-cord (`py-cord` on PyPI, imported as `discord`).
# If `discord.py` is also installed it will conflict — uninstall it first.
import discord
from discord import sinks
from scipy.signal import resample_poly

import gradbot
import qontext_tools

# Mirror main.py: prompt + tool definitions live there. Importing them keeps
# the agent identical across browser and Discord — single source of truth.
from main import SYSTEM_PROMPT_TEMPLATE, build_tools

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

_HERE = pathlib.Path(__file__).parent
dotenv.load_dotenv(_HERE / ".env")
dotenv.load_dotenv(_HERE / ".env.local", override=True)

gradbot.init_logging()
logger = logging.getLogger("qontext.discord")
logging.basicConfig(level=logging.INFO)

DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
DISCORD_GUILD_ID = os.environ.get("DISCORD_GUILD_ID")  # optional: instant slash sync

DEMO_LANGUAGE = os.environ.get("DEMO_LANGUAGE", "en")
DEMO_VOICE_ID = os.environ.get("DEMO_VOICE_ID", "YTpq7expH9539ERJ")

# Bot leaves the channel after this many seconds of no incoming audio.
IDLE_SECONDS = float(os.environ.get("DISCORD_IDLE_SECONDS", "45"))
# Hard cap on call duration regardless of activity.
MAX_CALL_SECONDS = float(os.environ.get("DISCORD_MAX_CALL_SECONDS", "600"))

# Audio constants (all PCM is signed 16-bit little-endian).
DISCORD_SR = 48_000   # Discord native rate
DISCORD_CH = 2        # Discord native channel count (stereo)
GRADBOT_SR = 24_000   # what gradbot wants/produces
GRADBOT_CH = 1        # mono
FRAME_MS = 20
DISCORD_FRAME_BYTES = (DISCORD_SR * FRAME_MS // 1000) * DISCORD_CH * 2  # 3840

cfg = gradbot.config.from_env()


# ---------------------------------------------------------------------------
# Audio plumbing
# ---------------------------------------------------------------------------


def downsample_48k_stereo_to_24k_mono(pcm_48k_stereo: bytes) -> bytes:
    """Discord -> gradbot. Stereo 48kHz s16 PCM bytes -> mono 24kHz s16 PCM."""
    if not pcm_48k_stereo:
        return b""
    arr = np.frombuffer(pcm_48k_stereo, dtype=np.int16).reshape(-1, 2)
    mono = arr.astype(np.int32).mean(axis=1).astype(np.int16)
    # 48k -> 24k, integer ratio (down by 2). resample_poly does AA filtering.
    out = resample_poly(mono.astype(np.float32), up=1, down=2)
    return np.clip(out, -32768, 32767).astype(np.int16).tobytes()


def upsample_24k_mono_to_48k_stereo(pcm_24k_mono: bytes) -> bytes:
    """gradbot -> Discord. Mono 24kHz s16 PCM bytes -> stereo 48kHz s16 PCM."""
    if not pcm_24k_mono:
        return b""
    arr = np.frombuffer(pcm_24k_mono, dtype=np.int16)
    up = resample_poly(arr.astype(np.float32), up=2, down=1)
    up_i16 = np.clip(up, -32768, 32767).astype(np.int16)
    # mono -> stereo: duplicate channel
    stereo = np.repeat(up_i16[:, None], 2, axis=1).reshape(-1)
    return stereo.tobytes()


class GradbotSink(sinks.Sink):
    """Custom py-cord Sink that forwards every incoming voice frame straight
    into gradbot. We do NOT buffer to a file — we stream live.

    py-cord calls `write(data, user)` per voice packet. `data.decoded_data`
    is already 48kHz stereo s16 PCM (decrypted + Opus-decoded by py-cord).
    """

    def __init__(self, on_pcm, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self._on_pcm = on_pcm  # async callable: bytes -> None
        self._loop = loop
        self.last_audio_ts: float = time.monotonic()
        self.encoding = "pcm"  # py-cord checks this for file naming on cleanup

    def write(self, data, user):  # noqa: D401 — py-cord override
        # `data` is RawData with .decoded_data set when packet was decoded.
        pcm = getattr(data, "decoded_data", None)
        if not pcm:
            return
        self.last_audio_ts = time.monotonic()
        # Hop to the asyncio loop. We don't await here because py-cord runs
        # this from its receive thread.
        asyncio.run_coroutine_threadsafe(self._on_pcm(bytes(pcm)), self._loop)


class GradbotAudioSource(discord.AudioSource):
    """A discord.AudioSource that pulls 20ms 48kHz stereo PCM frames out of
    a deque. py-cord pulls a frame every 20ms via read().

    We accept arbitrary-length 48kHz stereo PCM via push() and slice it into
    20ms (3840-byte) chunks, padding with silence on underflow so playback
    keeps ticking and the encoder stays alive.
    """

    SILENCE = b"\x00" * DISCORD_FRAME_BYTES

    def __init__(self):
        self._buf = bytearray()
        self._frames: deque[bytes] = deque()
        self._closed = False

    def push(self, pcm_48k_stereo: bytes) -> None:
        if self._closed or not pcm_48k_stereo:
            return
        self._buf.extend(pcm_48k_stereo)
        while len(self._buf) >= DISCORD_FRAME_BYTES:
            chunk = bytes(self._buf[:DISCORD_FRAME_BYTES])
            del self._buf[:DISCORD_FRAME_BYTES]
            self._frames.append(chunk)

    def read(self) -> bytes:
        if self._closed:
            return b""
        if self._frames:
            return self._frames.popleft()
        # Underflow: return silence so play() keeps going. Returning b"" would
        # signal end-of-stream and discord would stop playing.
        return self.SILENCE

    def is_opus(self) -> bool:
        return False

    def cleanup(self) -> None:
        self._closed = True
        self._frames.clear()
        self._buf.clear()


# ---------------------------------------------------------------------------
# Per-call session
# ---------------------------------------------------------------------------


class CallSession:
    """One live call in one Discord guild. Wraps voice client + gradbot."""

    def __init__(self, vc: discord.VoiceClient, text_channel: discord.abc.Messageable):
        self.vc = vc
        self.text_channel = text_channel
        self.input_handle: Optional[gradbot.SessionInputHandle] = None
        self.output_handle: Optional[gradbot.SessionOutputHandle] = None
        self.audio_source = GradbotAudioSource()
        self.sink: Optional[GradbotSink] = None
        self.pending: Optional[dict] = None
        self.started_at = time.monotonic()
        self.last_activity = time.monotonic()
        self._tasks: list[asyncio.Task] = []
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        # 1. Pull the next pending question from qontext.
        self.pending = await qontext_tools.fetch_pending_question()
        if self.pending:
            await self.text_channel.send(
                f"Calling about **{self.pending['topic']}** "
                f"(asking {self.pending['person_name']}, "
                f"{self.pending['person_role']})…"
            )
            instructions = SYSTEM_PROMPT_TEMPLATE.format(
                person_name=self.pending["person_name"],
                person_role=self.pending["person_role"],
                topic=self.pending["topic"],
                context=self.pending["context"],
                ask=self.pending["ask"],
                expected_schema=json.dumps(self.pending["expected_schema"]),
                question_id=self.pending["question_id"],
            )
        else:
            await self.text_channel.send(
                "No pending questions in the queue. I'll say hi and hang up."
            )
            instructions = (
                "There are no pending questions right now. "
                "Politely say so and end the call."
            )

        # 2. Spin up gradbot. PCM both directions, aggressive turn-taking.
        session_config = gradbot.SessionConfig(
            voice_id=DEMO_VOICE_ID,
            language=gradbot.LANGUAGES.get(DEMO_LANGUAGE),
            instructions=instructions,
            tools=build_tools(),
            **(
                {"assistant_speaks_first": True}
                | cfg.session_kwargs
                | {"flush_duration_s": 0.25, "silence_timeout_s": 0.0}
            ),
        )
        self.input_handle, self.output_handle = await gradbot.run(
            **cfg.client_kwargs,
            session_config=session_config,
            input_format=gradbot.AudioFormat.Pcm,
            output_format=gradbot.AudioFormat.Pcm,
        )

        # 3. Start receiving Discord audio into gradbot.
        loop = asyncio.get_running_loop()
        self.sink = GradbotSink(on_pcm=self._on_discord_pcm, loop=loop)
        # py-cord's start_recording requires a callback; we don't really use it
        # since we drain in real time, but it must be a coroutine.
        self.vc.start_recording(self.sink, self._on_recording_done, self.text_channel)

        # 4. Start playing gradbot output through Discord.
        self.vc.play(self.audio_source)

        # 5. Pump gradbot output messages and watch for idle.
        self._tasks.append(asyncio.create_task(self._output_loop()))
        self._tasks.append(asyncio.create_task(self._idle_watchdog()))

    async def _on_discord_pcm(self, pcm_48k_stereo: bytes) -> None:
        if self.input_handle is None:
            return
        self.last_activity = time.monotonic()
        try:
            pcm_24k_mono = downsample_48k_stereo_to_24k_mono(pcm_48k_stereo)
            await self.input_handle.send_audio(pcm_24k_mono)
        except Exception:
            logger.exception("forwarding mic -> gradbot failed")

    async def _output_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                msg = await self.output_handle.receive()
                if msg is None:
                    break
                if msg.msg_type == "audio":
                    pcm_48k_stereo = upsample_24k_mono_to_48k_stereo(bytes(msg.data))
                    self.audio_source.push(pcm_48k_stereo)
                elif msg.msg_type == "tool_call":
                    asyncio.create_task(self._handle_tool_call(msg))
                # We ignore stt_text/tts_text/event for the Discord transport;
                # add discord.Message logging here if you want a transcript.
        except Exception:
            logger.exception("output loop crashed")
        finally:
            await self.stop(reason="gradbot stream ended")

    async def _handle_tool_call(self, msg: gradbot.MsgOut) -> None:
        info = msg.tool_call
        handle = msg.tool_call_handle
        if info is None or handle is None:
            return
        if info.tool_name != "submit_answer":
            await handle.send_error(f"unknown tool: {info.tool_name}")
            return
        try:
            args = json.loads(info.args_json) if info.args_json else {}
        except json.JSONDecodeError:
            args = {}
        try:
            result = await qontext_tools.submit_answer(
                question_id=args.get("question_id", ""),
                answer=args.get("answer") or {},
                confidence=float(args.get("confidence", 0.0)),
                transcript_excerpt=args.get("transcript_excerpt", ""),
                status=args.get("status", "answered"),
            )
        except Exception as exc:
            logger.exception("submit_answer failed")
            await handle.send_error(f"submit failed: {exc}")
            return
        try:
            await self.text_channel.send(
                f"Answer logged for **{args.get('question_id')}** "
                f"({args.get('status')}, conf={args.get('confidence')})."
            )
        except Exception:
            logger.exception("failed to post answer-confirm to text channel")
        await handle.send(json.dumps({
            "ok": True,
            "message": "Answer recorded. Say a short warm goodbye and stop.",
            "result": result,
        }))

    async def _idle_watchdog(self) -> None:
        while not self._stopped.is_set():
            await asyncio.sleep(2)
            now = time.monotonic()
            if now - self.last_activity > IDLE_SECONDS:
                await self.stop(reason=f"idle for {IDLE_SECONDS:.0f}s")
                return
            if now - self.started_at > MAX_CALL_SECONDS:
                await self.stop(reason="max call duration reached")
                return

    async def _on_recording_done(self, sink, channel, *args):
        # py-cord requires this callback. We do nothing because we already
        # drained PCM live in `GradbotSink.write`.
        pass

    async def stop(self, reason: str = "stopped") -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        logger.info("stopping call: %s", reason)
        for t in self._tasks:
            t.cancel()
        # Best-effort cleanup. Each step is independent so a failure in one
        # doesn't block the others.
        try:
            if self.vc.recording:
                self.vc.stop_recording()
        except Exception:
            pass
        try:
            if self.vc.is_playing():
                self.vc.stop()
        except Exception:
            pass
        try:
            if self.input_handle is not None:
                await self.input_handle.close()
        except Exception:
            pass
        try:
            await self.vc.disconnect(force=True)
        except Exception:
            pass
        try:
            await self.text_channel.send(f"Call ended ({reason}).")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Bot wiring
# ---------------------------------------------------------------------------


# Voice receive needs the privileged voice_states intent. We don't need
# message_content for slash commands.
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True

bot_kwargs: dict = {"intents": intents}
if DISCORD_GUILD_ID:
    # debug_guilds = guild-scoped slash command sync. Updates show up
    # immediately in that guild; global sync can take up to an hour.
    bot_kwargs["debug_guilds"] = [int(DISCORD_GUILD_ID)]

bot = discord.Bot(**bot_kwargs)

# guild_id -> CallSession
_calls: dict[int, CallSession] = {}


@bot.event
async def on_ready():
    logger.info("logged in as %s (id=%s)", bot.user, bot.user.id if bot.user else "?")


@bot.slash_command(description="Have the agent join your voice channel and start a call.")
async def call(ctx: discord.ApplicationContext):
    if not DISCORD_BOT_TOKEN:
        await ctx.respond("DISCORD_BOT_TOKEN missing.", ephemeral=True)
        return
    if not isinstance(ctx.author, discord.Member) or ctx.author.voice is None:
        await ctx.respond("Join a voice channel first, then run /call.", ephemeral=True)
        return
    if ctx.guild is None:
        await ctx.respond("/call only works in a server.", ephemeral=True)
        return
    if ctx.guild.id in _calls:
        await ctx.respond("Already in a call here. Use /end first.", ephemeral=True)
        return

    await ctx.defer()
    try:
        vc = await ctx.author.voice.channel.connect()
    except Exception as exc:
        logger.exception("voice connect failed")
        await ctx.followup.send(f"Couldn't join voice: {exc}")
        return

    session = CallSession(vc=vc, text_channel=ctx.channel)
    _calls[ctx.guild.id] = session
    try:
        await session.start()
    except Exception as exc:
        logger.exception("session start failed")
        await ctx.followup.send(f"Failed to start call: {exc}")
        await session.stop(reason="start failed")
        _calls.pop(ctx.guild.id, None)
        return

    await ctx.followup.send("Call started. Speak whenever you're ready.")


@bot.slash_command(description="End the current call.")
async def end(ctx: discord.ApplicationContext):
    if ctx.guild is None or ctx.guild.id not in _calls:
        await ctx.respond("No active call here.", ephemeral=True)
        return
    session = _calls.pop(ctx.guild.id)
    await ctx.defer(ephemeral=True)
    await session.stop(reason="user requested /end")
    await ctx.followup.send("Call ended.", ephemeral=True)


@bot.slash_command(description="Show the current pending question and call state.")
async def status(ctx: discord.ApplicationContext):
    pending = await qontext_tools.fetch_pending_question()
    in_call = ctx.guild is not None and ctx.guild.id in _calls
    lines = [f"In call: **{in_call}**"]
    if pending:
        lines.append(
            f"Next question: **{pending['topic']}** "
            f"(asking {pending['person_name']})"
        )
    else:
        lines.append("Queue empty.")
    await ctx.respond("\n".join(lines), ephemeral=True)


def main() -> None:
    if not DISCORD_BOT_TOKEN:
        raise SystemExit(
            "DISCORD_BOT_TOKEN not set. Add it to .env.local. "
            "See DISCORD_SETUP.md."
        )
    bot.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
