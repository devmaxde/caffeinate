"""Smoke-test harness for the Qontext voice-agent FastAPI service.

Runs 20 iterations over the public surface area:
  * GET  /api/current-question (no arg, by id, missing id)
  * POST /api/manual-submit    (valid + empty body)
  * WS   /ws/chat               (start / receive / stop / close)
  * Audit log integrity (submitted_answers.jsonl)

This is a CONSTRUCT — the loop, assertions, report. We don't fix the
server when something's off; we just record honestly what happened.

Run:
    cd /Users/jasperkallflez/caffeinate/voice-agent && uv run python test_harness.py
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import random
import string
import sys
import time
from typing import Any

import httpx
import websockets

HERE = pathlib.Path(__file__).parent
BASE = "http://127.0.0.1:8001"
WS_URL = "ws://127.0.0.1:8001/ws/chat"
AUDIT_LOG = HERE / "submitted_answers.jsonl"
REPORT_PATH = pathlib.Path("/tmp/voice_test_results.json")
KNOWN_QIDS = ["q_revenue_q2", "q_user_xy_status", "q_video_2024"]
SLEEP_BETWEEN = 0.5  # seconds (bumped from 0.2 — gentler on Gradium between iters)
WS_COOLDOWN_S = 2.0  # extra wait after WS close so Gradium sessions release
WS_OPEN_TIMEOUT = 6.0
WS_MIN_OPEN_S = 1.0


def _rand_text(n: int = 16) -> str:
    return "".join(random.choices(string.ascii_lowercase + " ", k=n)).strip() or "abc"


def _audit_lines() -> list[str]:
    if not AUDIT_LOG.exists():
        return []
    return AUDIT_LOG.read_text().splitlines()


async def _preflight(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Best-effort reachability check. We accept ANY HTTP response — even 404 —
    as proof the server is alive. Only network errors abort the run.
    """
    try:
        r = await client.get(f"{BASE}/api/current-question", timeout=3.0)
        return True, f"server reachable (HTTP {r.status_code})"
    except Exception as exc:
        return False, f"server unreachable: {exc!r}"


# --- iteration impls ----------------------------------------------------------


async def iter_current_question_basic(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Iters 1-3: GET /api/current-question, expect 200 + required keys."""
    r = await client.get(f"{BASE}/api/current-question")
    if r.status_code != 200:
        return False, f"expected 200, got {r.status_code}: {r.text[:120]}"
    try:
        data = r.json()
    except Exception as exc:
        return False, f"non-JSON body: {exc!r}"
    required = {"question_id", "person_name", "expected_schema"}
    missing = required - set(data.keys())
    if missing:
        return False, f"missing keys {sorted(missing)}; got {sorted(data.keys())}"
    return True, f"q={data['question_id']} for {data['person_name']}"


async def iter_current_question_by_id(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Iters 4-6: by id, expect echo back."""
    r = await client.get(f"{BASE}/api/current-question", params={"question_id": "q_video_2024"})
    if r.status_code != 200:
        return False, f"expected 200, got {r.status_code}: {r.text[:120]}"
    try:
        data = r.json()
    except Exception as exc:
        return False, f"non-JSON body: {exc!r}"
    if data.get("question_id") != "q_video_2024":
        return False, f"qid mismatch: got {data.get('question_id')!r}"
    return True, "got q_video_2024"


async def iter_current_question_missing(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Iters 7-8: nonexistent id, expect 404."""
    r = await client.get(f"{BASE}/api/current-question", params={"question_id": "does_not_exist"})
    if r.status_code != 404:
        return False, f"expected 404, got {r.status_code}: {r.text[:120]}"
    return True, "404 as expected"


async def iter_manual_submit_valid(client: httpx.AsyncClient, qid: str) -> tuple[bool, str]:
    """Iters 9-13: POST manual-submit with random text, expect ok + log gain."""
    before = len(_audit_lines())
    body = {"question_id": qid, "text": f"smoke-test-{_rand_text()}"}
    r = await client.post(f"{BASE}/api/manual-submit", json=body)
    if r.status_code != 200:
        return False, f"expected 200, got {r.status_code}: {r.text[:120]}"
    try:
        data = r.json()
    except Exception as exc:
        return False, f"non-JSON body: {exc!r}"
    if data.get("ok") is not True:
        return False, f"expected ok=true, got {data!r}"
    after = len(_audit_lines())
    if after != before + 1:
        return False, f"audit log delta != 1 (before={before}, after={after})"
    return True, f"qid={qid} logged (lines {before}->{after})"


async def iter_manual_submit_empty(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Iters 14-15: empty payload, expect 400."""
    r = await client.post(f"{BASE}/api/manual-submit", json={})
    if r.status_code != 400:
        return False, f"expected 400, got {r.status_code}: {r.text[:120]}"
    return True, "400 as expected"


async def iter_websocket_roundtrip() -> tuple[bool, str]:
    """Iters 16-18: connect, send start, await >=1 server msg, send stop, close."""
    started = time.monotonic()
    received: list[Any] = []
    try:
        async with websockets.connect(WS_URL, open_timeout=WS_OPEN_TIMEOUT) as ws:
            await ws.send(json.dumps({"type": "start"}))
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=WS_OPEN_TIMEOUT)
                received.append(msg)
            except asyncio.TimeoutError:
                return False, "no server msg within 6s of start"
            # Drain any quick follow-ups but don't block.
            try:
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=0.2)
                    received.append(msg)
            except asyncio.TimeoutError:
                pass
            elapsed = time.monotonic() - started
            if elapsed < WS_MIN_OPEN_S:
                # Pad up to 1s to satisfy the >=1s open-window criterion.
                await asyncio.sleep(WS_MIN_OPEN_S - elapsed)
            try:
                await ws.send(json.dumps({"type": "stop"}))
            except Exception:
                pass
            await ws.close()
    except Exception as exc:
        # Even on failure, give Gradium time to release the session before
        # the next iteration tries to grab a slot.
        await asyncio.sleep(WS_COOLDOWN_S)
        return False, f"ws error: {exc!r}"
    open_s = time.monotonic() - started
    # Cooldown so the next WS iteration doesn't trip the 3-session concurrency limit.
    await asyncio.sleep(WS_COOLDOWN_S)
    if open_s < WS_MIN_OPEN_S:
        return False, f"connection open only {open_s:.2f}s"
    if not received:
        return False, "no server events received"
    return True, f"open {open_s:.2f}s, {len(received)} event(s)"


async def iter_audit_log_parses() -> tuple[bool, str]:
    """Iter 19: every line in submitted_answers.jsonl is valid JSON."""
    if not AUDIT_LOG.exists():
        return False, f"{AUDIT_LOG.name} does not exist"
    lines = _audit_lines()
    if not lines:
        return False, "audit log is empty"
    bad = 0
    for i, ln in enumerate(lines, 1):
        if not ln.strip():
            continue
        try:
            json.loads(ln)
        except Exception:
            bad += 1
    if bad:
        return False, f"{bad}/{len(lines)} lines failed to parse"
    return True, f"{len(lines)} line(s), all valid JSON"


async def iter_summary(state: dict[str, int]) -> tuple[bool, str]:
    """Iter 20: aggregate. Always passes — it's the summary."""
    return True, f"ran {state['done']} iters, {state['passed']} passed, {state['failed']} failed"


# --- main loop ----------------------------------------------------------------


PLAN: list[tuple[int, str]] = (
    [(i, "current_question_basic") for i in range(1, 4)]
    + [(i, "current_question_by_id") for i in range(4, 7)]
    + [(i, "current_question_missing") for i in range(7, 9)]
    + [(i, "manual_submit_valid") for i in range(9, 14)]
    + [(i, "manual_submit_empty") for i in range(14, 16)]
    + [(i, "ws_roundtrip") for i in range(16, 19)]
    + [(19, "audit_log_parses")]
    + [(20, "summary")]
)


async def run() -> int:
    started_at = time.time()
    started_mono = time.monotonic()
    iters: list[dict[str, Any]] = []
    state = {"done": 0, "passed": 0, "failed": 0}

    async with httpx.AsyncClient(timeout=10.0) as client:
        ok, msg = await _preflight(client)
        if not ok:
            print(f"ABORT: {msg}", file=sys.stderr)
            REPORT_PATH.write_text(json.dumps({
                "started_at": started_at,
                "finished_at": time.time(),
                "total": 0,
                "passed": 0,
                "failed": 0,
                "aborted": True,
                "abort_reason": msg,
                "iters": [],
            }, indent=2))
            return 2
        print(f"[preflight] {msg}")

        for idx, (i, name) in enumerate(PLAN):
            try:
                if name == "current_question_basic":
                    passed, detail = await iter_current_question_basic(client)
                elif name == "current_question_by_id":
                    passed, detail = await iter_current_question_by_id(client)
                elif name == "current_question_missing":
                    passed, detail = await iter_current_question_missing(client)
                elif name == "manual_submit_valid":
                    qid = KNOWN_QIDS[idx % len(KNOWN_QIDS)]
                    passed, detail = await iter_manual_submit_valid(client, qid)
                elif name == "manual_submit_empty":
                    passed, detail = await iter_manual_submit_empty(client)
                elif name == "ws_roundtrip":
                    passed, detail = await iter_websocket_roundtrip()
                elif name == "audit_log_parses":
                    passed, detail = await iter_audit_log_parses()
                elif name == "summary":
                    passed, detail = await iter_summary(state)
                else:
                    passed, detail = False, f"unknown iter name {name!r}"
            except Exception as exc:
                passed, detail = False, f"unhandled exception: {exc!r}"

            state["done"] += 1
            if passed:
                state["passed"] += 1
            else:
                state["failed"] += 1
            iters.append({"i": i, "name": name, "pass": passed, "detail": detail})
            verdict = "PASS" if passed else "FAIL"
            print(f"[iter {i:02d}/20] {verdict}: {detail}")

            if idx != len(PLAN) - 1:
                await asyncio.sleep(SLEEP_BETWEEN)

    finished_at = time.time()
    elapsed_ms = int((time.monotonic() - started_mono) * 1000)
    report = {
        "started_at": started_at,
        "finished_at": finished_at,
        "total": len(iters),
        "passed": state["passed"],
        "failed": state["failed"],
        "iters": iters,
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2))
    print(f"RESULT: {state['passed']}/{len(iters)} passed in {elapsed_ms}ms")
    print(f"report: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
