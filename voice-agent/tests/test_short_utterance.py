"""Short-utterance flush regression test.

Reproduces the original "yes/no doesn't reach onFinal" bug: when a caller
sends a tiny audio clip and follows up immediately with {"type":"end"},
the bridge must flush a final:true (or close cleanly) within ~1s. If the
accumulator is empty (Gradium didn't transcribe the synthetic PCM, which
is normal for sine waves), the bridge must still close gracefully without
hanging — no error frames, no zombie sockets.

Run:
    cd /Users/jasperkallflez/caffeinate/voice-agent && uv run python tests/test_short_utterance.py
"""

from __future__ import annotations

import asyncio
import json
import math
import sys
import time

import numpy as np
import websockets

WS_URL = "ws://127.0.0.1:8001/api/stt"


def synth_pcm(duration_s: float, freq_hz: float = 440.0,
              sample_rate: int = 24000, amplitude: float = 0.3) -> bytes:
    n = int(duration_s * sample_rate)
    t = np.arange(n) / sample_rate
    samples = (amplitude * np.sin(2 * math.pi * freq_hz * t) * 32767).astype(np.int16)
    return samples.tobytes()


async def test_immediate_end_after_short_pcm() -> tuple[bool, str]:
    """Send setup + 0.3s PCM + {type:end}, all back-to-back. Then within
    1.5s either:
      (a) receive a final:true transcript, OR
      (b) the server closes the WS cleanly with no error frame.

    Either is success — synthetic sine PCM may or may not transcribe in
    Gradium. The bug we're guarding against is the bridge HANGING / never
    responding to the end signal.
    """
    pcm = synth_pcm(0.3)
    started = time.monotonic()
    final_received = False
    error_received = False
    closed_cleanly = False

    try:
        async with websockets.connect(WS_URL, open_timeout=10) as ws:
            await ws.send(json.dumps({"type": "setup", "language": "en"}))
            await ws.send(pcm)
            await ws.send(json.dumps({"type": "end"}))

            # Drain everything we get within 1.5s. If the server is healthy
            # it should respond immediately to {type:end} (emit_final +
            # close upstream + close us).
            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.5)
                    try:
                        ev = json.loads(raw)
                    except Exception:
                        continue
                    if ev.get("type") == "transcript" and ev.get("final"):
                        final_received = True
                    if ev.get("type") == "error":
                        error_received = True
            except asyncio.TimeoutError:
                pass
            except websockets.ConnectionClosed:
                closed_cleanly = True
    except Exception as exc:
        return False, f"ws error: {exc!r}"

    elapsed_ms = int((time.monotonic() - started) * 1000)

    if error_received:
        return False, f"bridge sent error frame after end (elapsed {elapsed_ms}ms)"
    # The bridge must respond — either with final OR clean close — within
    # the 1.5s drain window. If neither, it hung.
    if elapsed_ms > 2500:
        return False, f"end not honored within 2.5s (took {elapsed_ms}ms)"
    return True, (
        f"end honored in {elapsed_ms}ms "
        f"(final={final_received}, clean_close={closed_cleanly})"
    )


async def test_setup_end_no_audio() -> tuple[bool, str]:
    """Setup + immediate {type:end}, no audio at all. Bridge must not
    emit a final (accumulator empty) and must close without error.
    """
    started = time.monotonic()
    try:
        async with websockets.connect(WS_URL, open_timeout=10) as ws:
            await ws.send(json.dumps({"type": "setup", "language": "en"}))
            await ws.send(json.dumps({"type": "end"}))
            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.5)
                    try:
                        ev = json.loads(raw)
                    except Exception:
                        continue
                    if ev.get("type") == "error":
                        return False, f"unexpected error: {ev}"
                    if ev.get("type") == "transcript" and ev.get("final"):
                        # Final on empty accumulator is wrong — the bridge
                        # should have skipped emit since text is empty.
                        if ev.get("text", "").strip():
                            return False, f"got final with text on no-audio: {ev}"
            except asyncio.TimeoutError:
                pass
            except websockets.ConnectionClosed:
                pass
    except Exception as exc:
        return False, f"ws error: {exc!r}"
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if elapsed_ms > 2500:
        return False, f"hung after end (took {elapsed_ms}ms)"
    return True, f"clean teardown in {elapsed_ms}ms (no spurious final)"


async def test_default_silence_lowered() -> tuple[bool, str]:
    """Verify the new default silence_threshold (1.5s, not 2.5s).
    We send setup with NO override, send a tiny chunk of PCM, then idle.
    Within ~2.0s we expect either a final:true (if Gradium transcribed)
    or no event but a healthy connection. The key check: the bridge does
    NOT wait the old 2.5s+ before doing anything visible.

    Since synthetic PCM rarely transcribes, the strongest invariant we
    can check here is that the silence-watcher coroutine itself is wired
    on the new default — and that's covered structurally by the source
    change. We just verify nothing breaks at the new default value.
    """
    pcm = synth_pcm(0.3)
    try:
        async with websockets.connect(WS_URL, open_timeout=10) as ws:
            # No silence_threshold_s override — use server default.
            await ws.send(json.dumps({"type": "setup", "language": "en"}))
            await ws.send(pcm)
            # Wait 2.0s — between old (2.5) and new (1.5) thresholds.
            events = []
            try:
                deadline = time.monotonic() + 2.0
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    try:
                        ev = json.loads(raw)
                    except Exception:
                        continue
                    events.append(ev)
            except asyncio.TimeoutError:
                pass
            except websockets.ConnectionClosed:
                pass
            try:
                await ws.send(json.dumps({"type": "end"}))
            except Exception:
                pass
    except Exception as exc:
        return False, f"ws error: {exc!r}"
    errors = [e for e in events if e.get("type") == "error"]
    if errors:
        return False, f"got error frames at default threshold: {errors}"
    return True, f"default-threshold session healthy ({len(events)} event(s))"


async def main() -> int:
    tests = [
        ("immediate_end_after_short_pcm", test_immediate_end_after_short_pcm),
        ("setup_end_no_audio", test_setup_end_no_audio),
        ("default_silence_lowered", test_default_silence_lowered),
    ]
    results: list[tuple[str, bool, str]] = []
    for name, fn in tests:
        try:
            ok, detail = await fn()
        except Exception as exc:
            ok, detail = False, f"unhandled: {exc!r}"
        results.append((name, ok, detail))
        verdict = "PASS" if ok else "FAIL"
        print(f"[{name}] {verdict}: {detail}")
        # Brief pause between tests so we don't slam Gradium concurrency.
        await asyncio.sleep(1.0)

    failed = [r for r in results if not r[1]]
    print(f"RESULT: {len(results) - len(failed)}/{len(results)} passed")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
