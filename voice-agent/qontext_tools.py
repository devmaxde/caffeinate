"""Qontext backend bridge: get gap-questions, submit structured answers.

Tries the real qontext-api first (HTTP). Falls back to mock_questions.json.
Submitted answers are appended to ./submitted_answers.jsonl as a transcript.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_THIS_DIR = pathlib.Path(__file__).parent
_MOCK_PATH = _THIS_DIR / "mock_questions.json"
_ANSWERS_LOG = _THIS_DIR / "submitted_answers.jsonl"

QONTEXT_URL = os.environ.get("QONTEXT_API_URL", "http://localhost:8080")
USE_MOCK = os.environ.get("QONTEXT_USE_MOCK", "1") == "1"


def _load_mock() -> list[dict[str, Any]]:
    return json.loads(_MOCK_PATH.read_text())["queue"]


def _already_answered_ids() -> set[str]:
    """Question IDs that have at least one entry in the audit log.
    Used to skip past already-handled questions when serving 'next'.
    """
    if not _ANSWERS_LOG.exists():
        return set()
    ids: set[str] = set()
    for line in _ANSWERS_LOG.read_text().splitlines():
        try:
            d = json.loads(line)
        except Exception:
            continue
        if d.get("status") in {"answered", "answered_text"} and d.get("question_id"):
            ids.add(d["question_id"])
    return ids


async def fetch_pending_question(question_id: str | None = None) -> dict[str, Any] | None:
    """Get the next open knowledge-graph gap (or a specific one by id).

    Returns dict with keys: question_id, person_name, person_role, topic,
    context, ask, expected_schema. Returns None if nothing is pending.
    """
    if not USE_MOCK:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                path = f"/api/pending_questions/{question_id}" if question_id else "/api/pending_questions/next"
                r = await client.get(f"{QONTEXT_URL}{path}")
                if r.status_code == 200:
                    return r.json()
                logger.warning("qontext-api returned %s, falling back to mock", r.status_code)
        except Exception as exc:
            logger.warning("qontext-api unreachable (%s), falling back to mock", exc)

    queue = _load_mock()
    if not queue:
        return None
    if question_id:
        for q in queue:
            if q["question_id"] == question_id:
                return q
        return None
    answered = _already_answered_ids()
    for q in queue:
        if q["question_id"] not in answered:
            return q
    return None  # everything answered


async def submit_answer(
    question_id: str,
    answer: dict[str, Any],
    confidence: float,
    transcript_excerpt: str,
    status: str = "answered",
) -> dict[str, Any]:
    """Persist a structured answer back to Qontext.

    status: "answered" | "deferred" | "declined" | "unknown"
    """
    payload = {
        "question_id": question_id,
        "answer": answer,
        "confidence": confidence,
        "status": status,
        "transcript_excerpt": transcript_excerpt,
        "source": f"voice_call_{int(time.time())}",
        "submitted_at": time.time(),
    }

    # Always append to local audit log so we can show it during the demo.
    with _ANSWERS_LOG.open("a") as f:
        f.write(json.dumps(payload) + "\n")
    logger.info("answer logged: %s -> %s (conf=%s)", question_id, status, confidence)

    if not USE_MOCK:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                r = await client.post(f"{QONTEXT_URL}/api/answer", json=payload)
                r.raise_for_status()
                return {"ok": True, "remote": r.json()}
        except Exception as exc:
            logger.warning("qontext-api submit failed (%s), kept local log only", exc)
            return {"ok": True, "remote": None, "warning": str(exc)}

    return {"ok": True, "remote": None}
