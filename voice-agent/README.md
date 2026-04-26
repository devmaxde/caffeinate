# Qontext Voice Agent

The Human-in-the-Loop execution layer for the Qontext knowledge graph.

When Qontext detects a gap it can't fill from the data, it identifies the
right human and this service calls them. The bot asks the question, listens,
and writes a structured answer back into the graph.

## Stack

- **Voice loop**: [gradbot](https://github.com/gradium-ai/gradbot) (Rust core, Python bindings)
- **STT + TTS**: Gradium (5 languages, voice cloning, low latency)
- **LLM**: Hybrid — Groq Llama 3.3 70B via OpenAI-compatible endpoint
- **Backend bridge**: HTTP to `qontext-api:8080`, mock JSON fallback

## Run

```bash
cp .env.example .env
# Edit .env: set GRADIUM_API_KEY and LLM_API_KEY
uv sync
uv run uvicorn main:app --reload --port 8001
```

Then open <http://localhost:8001> and start the call.

## Files

- `main.py` — FastAPI + WebSocket, system prompt, tool wiring
- `qontext_tools.py` — fetches gaps, submits answers (real API or mock)
- `mock_questions.json` — demo gaps including the `revenue_goals_hitl` eval
- `submitted_answers.jsonl` — local audit log of every answer (gitignored)

## Demo flow

1. Open the page → bot calls Inan about Q2 revenue targets
2. You answer in voice as "Inan"
3. Bot probes if needed, then calls `submit_answer` tool
4. Browser shows the structured JSON written back to Qontext
5. `submitted_answers.jsonl` has the audit trail
