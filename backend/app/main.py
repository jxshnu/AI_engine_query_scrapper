"""FastAPI service: ingest, normalize, persist, broadcast.

Run:  uvicorn app.main:app --reload --port 8000   (from backend/)
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .models import InteractionEvent, ToolCall
from .parsers import (
    ClaudeToolAssembler,
    detect_engine,
    extract_outbound_prompt,
    parse_chatgpt_sse_payload,
    parse_gemini_rpc_line,
    parse_perplexity_frame,
    parse_sse_block,
)
from .sanitize import sanitize_text
from .store import store
from .live import manager as live_manager

app = FastAPI(title="Fan-Out Interceptor", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_subscribers: List[WebSocket] = []
_claude_sessions: Dict[str, ClaudeToolAssembler] = {}


async def broadcast(event: Dict[str, Any]) -> None:
    dead = []
    for ws in _subscribers:
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in _subscribers:
            _subscribers.remove(ws)


def _get_or_create(engine: str, session_key: Optional[str] = None) -> InteractionEvent:
    if session_key:
        ev = store.get(session_key)
        if ev:
            return ev
    latest = store.latest_for_engine(engine)  # type: ignore[arg-type]
    # Reuse the latest open interaction for streaming fan-outs; else create new.
    if latest and latest.agentic_retrieval.query_fanout_count < 20:
        return latest
    ev = InteractionEvent(engine=engine)  # type: ignore[arg-type]
    store.upsert(ev)
    return ev


# ------------------------------------------------- ingest helpers (shared) ---
def ingest_user_prompt(engine: str, prompt_text: str,
                       model_id: Optional[str] = None) -> InteractionEvent:
    """Normalize + persist one outbound user prompt. Broadcast is done by callers."""
    clean, pii = sanitize_text(prompt_text or "")
    ev = InteractionEvent(engine=engine)  # type: ignore[arg-type]
    if model_id:
        ev.session_metadata.model_id = model_id
    ev.user_interaction.raw_prompt_text = prompt_text or ""
    ev.user_interaction.sanitized_prompt_text = clean
    ev.user_interaction.pii_detected = pii
    store.upsert(ev)
    return ev


def ingest_fanout(engine: str, subqueries: List[str], citations: List[str],
                  tool: Optional[str] = None, tokens: int = 0,
                  session_key: Optional[str] = None) -> InteractionEvent:
    """Attach subqueries/citations to the open interaction for `engine`."""
    ev = _get_or_create(engine, session_key)
    if subqueries:
        ev.add_subqueries(subqueries)
    if citations:
        ev.add_citations(citations)
    if tokens:
        ev.model_response.total_token_count += tokens
    if tool and subqueries and all(t.tool_name != tool for t in ev.agentic_retrieval.intermediate_tools_called):
        ev.agentic_retrieval.intermediate_tools_called.append(
            ToolCall(tool_name=tool, latency_ms=420))
    store.upsert(ev)
    return ev


async def live_sink(event: Dict[str, Any]) -> None:
    """Async bridge: live CDP wire events -> store + websocket broadcast."""
    etype = event.get("type")
    if etype == "user_prompt":
        text = str(event.get("prompt") or "")
        if not text:
            # preview fallback only when the body looks message-ish —
            # metadata shells like /conversation/init must stay empty.
            preview = str(event.get("post_data_preview") or "")
            if any(k in preview for k in ("messages", "prompt", "parts",
                                          "content", "query", "text")):
                text = preview
        if not text:
            return
        ev = ingest_user_prompt(str(event.get("engine", "CHATGPT")), text)
        await broadcast({"type": "user_prompt", "interaction": ev.model_dump()})
    elif etype == "fanout":
        ev = ingest_fanout(str(event.get("engine", "CHATGPT")),
                           [q for q in event.get("subqueries", []) if isinstance(q, str)],
                           [c for c in event.get("citations", []) if isinstance(c, str)],
                           tool=event.get("tool") or "web_search",
                           session_key=event.get("request_id"))
        await broadcast({"type": "fanout", "interaction": ev.model_dump(),
                         "added_subqueries": event.get("subqueries", []),
                         "added_citations": event.get("citations", [])})
    elif etype == "citations":
        ev = ingest_fanout(str(event.get("engine", "CHATGPT")), [],
                           [c for c in event.get("citations", []) if isinstance(c, str)])
        await broadcast({"type": "citations", "interaction": ev.model_dump()})
    elif etype in ("live_started", "live_stopped", "stream_done", "debug_request"):
        await broadcast({**event})


live_manager.set_sink(live_sink)


# ------------------------------------------------------------------ schemas
class PromptIngest(BaseModel):
    url: str = ""
    post_data: str = ""
    engine: Optional[str] = None
    model_id: Optional[str] = None


class StreamIngest(BaseModel):
    engine: str = "CHATGPT"
    request_id: Optional[str] = None
    interaction_id: Optional[str] = None
    sse_event: Optional[str] = None
    sse_data: Optional[str] = None
    raw_chunk: Optional[str] = None
    ws_payload: Optional[str] = None
    url: Optional[str] = None


class SimulateRequest(BaseModel):
    engine: str = "PERPLEXITY"
    prompt: str = "What is the best HIPAA compliant CRM for a 15 person healthtech startup integrating with Snowflake?"


# -------------------------------------------------------------------- routes
@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/interactions")
def list_interactions(limit: int = 50) -> List[Dict[str, Any]]:
    return [e.model_dump() for e in store.list(limit=limit)]


@app.get("/api/interactions/{interaction_id}")
def get_interaction(interaction_id: str) -> Dict[str, Any]:
    ev = store.get(interaction_id)
    if not ev:
        return {"error": "not found"}
    return ev.model_dump()


@app.delete("/api/interactions")
async def clear_interactions() -> Dict[str, str]:
    store.clear()
    await broadcast({"type": "cleared"})
    return {"status": "cleared"}


@app.post("/api/capture/prompt")
async def capture_prompt(body: PromptIngest) -> Dict[str, Any]:
    engine = (body.engine or detect_engine(body.url)).upper()
    if engine == "UNKNOWN":
        engine = "CHATGPT"
    _, prompt = extract_outbound_prompt(body.url, body.post_data)
    if not prompt and body.post_data and len(body.post_data) < 2000:
        prompt = body.post_data
    ev = ingest_user_prompt(engine, prompt or "", model_id=body.model_id)
    out = ev.model_dump()
    await broadcast({"type": "user_prompt", "interaction": out})
    return out


@app.post("/api/capture/stream")
async def capture_stream(body: StreamIngest) -> Dict[str, Any]:
    """Feed one raw SSE chunk / RPC line / WS frame for parsing (demo + tests)."""
    engine = body.engine.upper()
    ev = _get_or_create(engine, body.interaction_id)
    added_q: List[str] = []
    added_c: List[str] = []

    if body.ws_payload:
        sig = parse_perplexity_frame(body.ws_payload)
        added_q, added_c = sig.subqueries, sig.citations
        if sig.tool_name and not ev.agentic_retrieval.intermediate_tools_called:
            ev.agentic_retrieval.intermediate_tools_called.append(ToolCall(tool_name=sig.tool_name))
    elif body.sse_data is not None:
        asm = _claude_sessions.setdefault(body.request_id or ev.interaction_id, ClaudeToolAssembler())
        sig, _ = parse_sse_block(body.sse_event, body.sse_data, asm)
        added_q, added_c = sig.subqueries, sig.citations
        ev.model_response.total_token_count += sig.tokens
        if sig.reasoning_present:
            ev.model_response.reasoning_trace_present = True
        if sig.tool_name and all(t.tool_name != sig.tool_name for t in ev.agentic_retrieval.intermediate_tools_called):
            ev.agentic_retrieval.intermediate_tools_called.append(ToolCall(tool_name=sig.tool_name, latency_ms=420))
    elif body.raw_chunk:
        # Gemini path or raw SSE blob
        if "wrb.fr" in body.raw_chunk or engine == "GEMINI":
            for line in body.raw_chunk.splitlines():
                sig = parse_gemini_rpc_line(line)
                added_q.extend([q for q in sig.subqueries if q not in added_q])
                added_c.extend([c for c in sig.citations if c not in added_c])
        else:
            # split SSE blob into event/data pairs
            cur: Optional[str] = None
            asm = _claude_sessions.setdefault(body.request_id or ev.interaction_id, ClaudeToolAssembler())
            for line in body.raw_chunk.splitlines():
                s = line.strip()
                if s.startswith("event:"):
                    cur = s[6:].strip()
                elif s.startswith("data:"):
                    sig, _ = parse_sse_block(cur, s[5:].strip(), asm)
                    added_q.extend([q for q in sig.subqueries if q not in added_q])
                    added_c.extend([c for c in sig.citations if c not in added_c])
                    ev.model_response.total_token_count += sig.tokens
                    cur = None
    if added_q:
        ev.add_subqueries(added_q)
    if added_c:
        ev.add_citations(added_c)
    store.upsert(ev)
    out = ev.model_dump()
    await broadcast({"type": "fanout", "interaction": out,
                     "added_subqueries": added_q, "added_citations": added_c})
    return {"interaction": out, "added_subqueries": added_q, "added_citations": added_c}


DEMO_FANOUTS: Dict[str, List[str]] = {
    "CHATGPT": [
        "best HIPAA compliant CRM small healthtech",
        "CRM Snowflake integration native connector",
        "HIPAA CRM pricing 15 users comparison",
    ],
    "CLAUDE": [
        "HIPAA compliant CRM requirements healthtech startup",
        "Snowflake CRM integration options",
    ],
    "GEMINI": [
        "HIPAA CRM grounding search healthtech",
        "Snowflake native CRM connectors",
    ],
    "PERPLEXITY": [
        "best HIPAA compliant CRM small healthtech",
        "CRM Snowflake integration native connector",
        "HIPAA CRM pricing 15 users comparison",
    ],
}

DEMO_CITATIONS = [
    "https://www.leadsq.com/healthcare/hipaa-compliant-crm",
    "https://www.salesforce.com/solutions/health-cloud/",
]


@app.post("/api/simulate")
async def simulate(body: SimulateRequest) -> Dict[str, Any]:
    """Deterministic demo (no browser/login needed) mirroring Section 7 output."""
    engine = body.engine.upper()
    clean, pii = sanitize_text(body.prompt)
    ev = InteractionEvent(engine=engine)  # type: ignore[arg-type]
    ev.user_interaction.raw_prompt_text = body.prompt
    ev.user_interaction.sanitized_prompt_text = clean
    ev.user_interaction.pii_detected = pii
    ev.add_subqueries(DEMO_FANOUTS.get(engine, DEMO_FANOUTS["PERPLEXITY"]))
    ev.add_citations(DEMO_CITATIONS)
    from .models import ToolCall
    ev.agentic_retrieval.intermediate_tools_called.append(ToolCall(tool_name="web_search", latency_ms=420))
    ev.model_response.total_token_count = 482
    ev.model_response.reasoning_trace_present = True
    store.upsert(ev)
    out = ev.model_dump()
    await broadcast({"type": "fanout", "interaction": out,
                     "added_subqueries": ev.agentic_retrieval.intercepted_subqueries,
                     "added_citations": DEMO_CITATIONS})
    return out


class LiveStart(BaseModel):
    engine: str = "PERPLEXITY"


@app.post("/api/live/start")
async def live_start(body: LiveStart) -> Dict[str, Any]:
    """Open the engine site in a controlled Chrome window and tap its wire.

    Chat inside the opened window — prompts + fan-outs stream back into the UI.
    Log in there once; the profile persists across restarts.
    """
    try:
        return await live_manager.start(body.engine)
    except (ValueError, RuntimeError) as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/live/stop")
async def live_stop() -> Dict[str, Any]:
    return await live_manager.stop()


@app.get("/api/live/status")
def live_status() -> Dict[str, Any]:
    return live_manager.status()


@app.get("/api/live/events")
def live_events(limit: int = 50) -> List[Dict[str, Any]]:
    """Recent raw wire events (newest first) — same feed the WS pushes live."""
    return live_manager.recent_events(limit)


@app.get("/api/live/log")
def live_log(tail: int = 200) -> Dict[str, str]:
    """Human-readable backend log tail (mirrors logs/fanout.log)."""
    return {"log": live_manager.tail_log(tail)}


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket) -> None:
    await ws.accept()
    _subscribers.append(ws)
    try:
        while True:
            await ws.receive_text()  # keep-alive; client messages ignored
    except WebSocketDisconnect:
        pass
    finally:
        if ws in _subscribers:
            _subscribers.remove(ws)
