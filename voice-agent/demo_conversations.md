# Demo Conversations

Ten scripted scenarios showing the voice agent in action. Use these as:
- demo scripts for the hackathon pitch
- expected-behavior fixtures for testing
- talking points for the judges

Each conversation ends with the structured `submit_answer` payload that gets
written back into the Qontext knowledge graph (and `submitted_answers.jsonl`).

Format: **`B:`** = bot, **`U:`** = user, `[tool]` = tool call moment.

---

## 1. Clean answer — high confidence

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** en

```
B: Hi Inan, sorry to interrupt — quick one on Q2 revenue targets.
   Do you have an explicit dollar figure for next quarter?
U: Yeah, we're aiming for two point five million for Q2.
B: Got it — 2.5 million for Q2 2026. That's exactly what I needed.
[tool: submit_answer]
B: Perfect, thanks Inan. Have a great one.
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {"revenue_target_usd": 2500000, "quarter": "Q2 2026",
             "confidence_source": "Inan (Head of Management) voice call"},
  "confidence": 0.92,
  "status": "answered",
  "transcript_excerpt": "we're aiming for two point five million for Q2"
}
```

---

## 2. Fuzzy answer — one follow-up

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** en

```
B: Hi Inan, quick question on Q2 revenue — what's the explicit target?
U: Uhh, somewhere around two and a half million I think.
B: Got it, around 2.5M. Is that the global target, or just North America?
U: Global. Yeah, 2.5 million globally for Q2.
B: Crystal clear.
[tool: submit_answer]
B: Appreciate it Inan.
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {"revenue_target_usd": 2500000, "quarter": "Q2 2026",
             "confidence_source": "Inan voice call, confirmed global"},
  "confidence": 0.78,
  "status": "answered",
  "transcript_excerpt": "Global. 2.5 million globally for Q2"
}
```

---

## 3. Don't know — points to a better source

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** en

```
B: Hi Inan, quick one — what's the explicit revenue target for Q2?
U: Honestly, that's not my number. Sarah Chen on finance owns Q2 forecasts.
B: Got it — Sarah Chen on finance is the right person to ask?
U: Yeah, she has the actual targets.
B: Perfect, will reroute to Sarah.
[tool: submit_answer]
B: Thanks Inan, sorry to bug you.
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {},
  "confidence": 0.0,
  "status": "unknown",
  "transcript_excerpt": "not my number. Sarah Chen on finance owns Q2 forecasts"
}
```

→ Qontext can use the `transcript_excerpt` to enqueue a follow-up call to Sarah.

---

## 4. Deferred — call back later

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** en

```
B: Hi Inan, super quick one on Q2 revenue —
U: I'm walking into a board meeting in thirty seconds. Call me at 3pm.
B: No worries, I'll catch you at 3.
[tool: submit_answer]
B: Have a good meeting Inan.
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {},
  "confidence": 0.0,
  "status": "deferred",
  "transcript_excerpt": "walking into board meeting, call at 3pm"
}
```

---

## 5. Declined — confidentiality

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** en

```
B: Hi Inan, calling on behalf of Qontext — quick one on Q2 revenue numbers.
U: That's confidential, I can't share that over a recorded call.
B: Completely understood. I'll mark this as off-limits.
[tool: submit_answer]
B: Thanks for your time Inan.
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {},
  "confidence": 0.0,
  "status": "declined",
  "transcript_excerpt": "confidential, can't share over a recorded call"
}
```

---

## 6. Employee cleanup — clean archive

**Q:** `q_user_xy_status` · **Person:** Sarah · **Lang:** en

```
B: Hi Sarah, quick HR question — do we still have an active record for
   X. Yamamoto, employee ID 4421? No activity in nine months.
U: Oh yeah, Yamamoto-san left in July last year. Should be archived.
B: Got it — left July 2024, archive the record.
[tool: submit_answer]
B: Perfect, thanks Sarah.
```

```json
{
  "question_id": "q_user_xy_status",
  "answer": {"status": "former", "action": "archive",
             "notes": "Left July 2024 per HR Lead Sarah"},
  "confidence": 0.95,
  "status": "answered",
  "transcript_excerpt": "Yamamoto-san left in July last year, archive"
}
```

---

## 7. Conditional answer — captures the IF

**Q:** `q_video_2024` · **Person:** Marcus · **Lang:** en

```
B: Hi Marcus, quick one on the 2024 Q3 video assets — re-cut for the
   2026 launch, or archive?
U: Depends on the new product line. If we launch the Mark IV in May,
   we re-cut. If that slips, archive everything.
B: Got it — recut conditional on Mark IV May launch, otherwise archive.
   What's the deadline to decide?
U: We'll know by April 15th from product.
B: Crystal clear.
[tool: submit_answer]
B: Thanks Marcus, we'll check back after the 15th.
```

```json
{
  "question_id": "q_video_2024",
  "answer": {"decision": "hold",
             "reason": "Conditional: recut if Mark IV launches in May 2026, otherwise archive",
             "deadline": "2026-04-15"},
  "confidence": 0.88,
  "status": "answered",
  "transcript_excerpt": "If Mark IV in May, recut. Otherwise archive. Know by April 15"
}
```

---

## 8. Honesty challenge — "are you an AI?"

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** en

```
B: Hi Inan, quick one on Q2 revenue —
U: Wait, hold on. Is this an actual person or a bot?
B: I'm Qontext's voice assistant — the system flagged a knowledge gap
   and your name was the best match. Want me to skip and have a human
   reach out instead?
U: No it's fine, what's the question?
B: Just the explicit dollar figure for Q2 revenue.
U: 2.5 million globally.
B: Got it.
[tool: submit_answer]
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {"revenue_target_usd": 2500000, "quarter": "Q2 2026",
             "confidence_source": "Inan voice call after AI disclosure"},
  "confidence": 0.9,
  "status": "answered",
  "transcript_excerpt": "2.5 million globally"
}
```

---

## 9. Off-topic ramble — gentle redirect

**Q:** `q_video_2024` · **Person:** Marcus · **Lang:** en

```
B: Hi Marcus, quick one on the 2024 Q3 video assets — re-cut or archive?
U: Yeah those videos, you know what really bugged me about that shoot,
   the lighting on day three was a mess and we lost half the b-roll —
B: Totally get it. Just to close the loop on the system though — recut
   for 2026, or archive?
U: Oh, archive. We're shooting fresh.
B: Got it, archiving.
[tool: submit_answer]
B: Thanks Marcus.
```

```json
{
  "question_id": "q_video_2024",
  "answer": {"decision": "archive",
             "reason": "Team is shooting fresh assets for 2026 (Marcus)",
             "deadline": ""},
  "confidence": 0.9,
  "status": "answered",
  "transcript_excerpt": "archive. We're shooting fresh"
}
```

---

## 10. German call — same question, native voice

**Q:** `q_revenue_q2` · **Person:** Inan · **Lang:** de · **Voice:** German voice ID

```
B: Hi Inan, kurze Frage zum Q2-Revenue-Ziel. Hast du eine konkrete
   Dollar-Zahl für nächstes Quartal?
U: Ja, wir zielen auf zweieinhalb Millionen global.
B: Verstanden, 2,5 Millionen global für Q2 2026.
[tool: submit_answer]
B: Perfekt, danke Inan.
```

```json
{
  "question_id": "q_revenue_q2",
  "answer": {"revenue_target_usd": 2500000, "quarter": "Q2 2026",
             "confidence_source": "Inan voice call (DE), global"},
  "confidence": 0.9,
  "status": "answered",
  "transcript_excerpt": "2,5 Millionen global"
}
```

---

## Coverage matrix

| # | Status     | Confidence band | Tests                                                  |
|---|------------|-----------------|--------------------------------------------------------|
| 1 | answered   | high (0.9+)     | Happy path, single-shot answer                         |
| 2 | answered   | mid (0.7–0.85)  | Vague answer + clarifying follow-up                    |
| 3 | unknown    | 0.0             | User points to better source — feeds re-routing        |
| 4 | deferred   | 0.0             | Time pressure, call-back time captured                 |
| 5 | declined   | 0.0             | Confidentiality refusal handled gracefully             |
| 6 | answered   | very high (0.95)| Different question (HR) + entity context               |
| 7 | answered   | high (0.85+)    | Conditional answer captured with deadline              |
| 8 | answered   | high            | AI-honesty disclosure mid-call, conversation continues |
| 9 | answered   | high            | User rambles, bot redirects without being rude         |
| 10| answered   | high            | German language, native voice, same schema             |

All ten map to existing pending questions in `mock_questions.json` and produce
valid records in `submitted_answers.jsonl`.
