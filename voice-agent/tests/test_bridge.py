"""Smoke test for the browser-bridge endpoints (/api/tts and /api/stt).

This is a *live* test — it talks to the running server at 127.0.0.1:8001,
which in turn hits the real Gradium API. We don't mock; the whole point
is to verify the integration end-to-end with the real upstream.

Run:
    cd /Users/jasperkallflez/caffeinate/voice-agent && uv run python tests/test_bridge.py
"""

from __future__ import annotations

import asyncio
import json
import sys

import httpx
import websockets

BASE = "http://127.0.0.1:8001"
WS_URL = "ws://127.0.0.1:8001/api/stt"


async def test_tts() -> tuple[bool, str]:
    body = {"text": "oregon integration test", "voice_id": None, "language": "en"}
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(f"{BASE}/api/tts", json=body)
    except Exception as exc:
        return False, f"request failed: {exc!r}"
    if r.status_code != 200:
        return False, f"expected 200, got {r.status_code}: {r.text[:200]}"
    ctype = r.headers.get("content-type", "")
    if "audio/wav" not in ctype:
        return False, f"expected audio/wav, got {ctype!r}"
    if len(r.content) <= 10_000:
        return False, f"wav body too small: {len(r.content)} bytes"
    return True, f"audio/wav, {len(r.content)} bytes"


async def test_tts_empty() -> tuple[bool, str]:
    """Empty text → 400."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{BASE}/api/tts", json={"text": ""})
    except Exception as exc:
        return False, f"request failed: {exc!r}"
    if r.status_code != 400:
        return False, f"expected 400, got {r.status_code}"
    return True, "400 as expected"


async def test_stt_setup() -> tuple[bool, str]:
    """Connect, send setup, then immediately end. We're just verifying
    that the bridge accepts the handshake — not that Gradium transcribes.
    """
    try:
        async with websockets.connect(WS_URL, open_timeout=10) as ws:
            await ws.send(json.dumps({"type": "setup", "language": "en"}))
            # Send end immediately. We don't expect any transcript since
            # we sent no audio, but the bridge should not error out.
            await ws.send(json.dumps({"type": "end"}))
            # Drain any response within a short window.
            try:
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    # If we get an error frame, surface it.
                    try:
                        ev = json.loads(msg)
                    except Exception:
                        continue
                    if ev.get("type") == "error":
                        return False, f"bridge sent error: {ev}"
            except asyncio.TimeoutError:
                pass
            except websockets.ConnectionClosed:
                pass
    except Exception as exc:
        return False, f"ws error: {exc!r}"
    return True, "setup → end roundtrip ok"


async def main() -> int:
    results: list[tuple[str, bool, str]] = []
    for name, fn in [
        ("tts_basic", test_tts),
        ("tts_empty", test_tts_empty),
        ("stt_setup", test_stt_setup),
    ]:
        ok, detail = await fn()
        results.append((name, ok, detail))
        verdict = "PASS" if ok else "FAIL"
        print(f"[{name}] {verdict}: {detail}")

    failed = [r for r in results if not r[1]]
    print(f"RESULT: {len(results) - len(failed)}/{len(results)} passed")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
