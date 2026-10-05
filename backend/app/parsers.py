"""Engine-specific deserializers — Section 4 platform extraction rules.

All functions are pure (no I/O) so they can be unit-tested and reused by both
the live CDP interceptor and the REST / demo ingest path.
"""
from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

EngineName = Literal["CHATGPT", "CLAUDE", "GEMINI", "PERPLEXITY", "UNKNOWN"]


def detect_engine(url: str, ws_payload: str = "") -> EngineName:
    u = url or ""
    # ChatGPT 2026 client posts to /backend-api/f/conversation (the /f/ is
    # significant — plain substring "backend-api/conversation" does NOT match
    # "backend-api/f/conversation"). Legacy /backend-api/conversation kept.
    if "/backend-api/f/conversation" in u or "/backend-api/conversation" in u:
        return "CHATGPT"
    if "/chat_conversations" in u and "/completion" in u:
        return "CLAUDE"
    if "BardFrontendService/StreamGenerate" in u or "StreamGenerate" in u:
        return "GEMINI"
    if "socket.io" in u or (ws_payload.startswith("42")):
        return "PERPLEXITY"
    return "UNKNOWN"


# ---------------------------------------------------------------- outbound ---

def extract_chatgpt_prompt(post_data: str) -> Optional[str]:
    """Latest user-role message's parts (2026 /f/conversation shape).

    Returns None when the body carries no message content at all (e.g. the
    /conversation/init metadata shell) so callers never store raw metadata
    as a "prompt".
    """
    try:
        body = json.loads(post_data)
    except Exception:
        return None
    try:
        messages = body.get("messages", [])
        if messages:
            user_msgs = [m for m in messages
                         if isinstance(m, dict) and m.get("author", {}).get("role") == "user"]
            msg = (user_msgs or messages)[-1]
            content = msg.get("content", {}) if isinstance(msg.get("content"), dict) else {}
            parts = content.get("parts", [])
            if parts and isinstance(parts[0], str) and parts[0].strip():
                return parts[0]
            if parts:
                return json.dumps(parts[0]) if not isinstance(parts[0], str) else str(parts[0])
        # fallback: common alternates
        for key in ("prompt", "input", "message", "text"):
            if isinstance(body.get(key), str) and body[key].strip():
                return body[key]
    except Exception:
        return None
    return None


def extract_claude_prompt(post_data: str) -> Optional[str]:
    try:
        body = json.loads(post_data)
    except Exception:
        return None
    # Anthropic-style: {"messages": [{"content": "..."/[{"text":...}]}]}
    try:
        messages = body.get("messages", [])
        if messages:
            content = messages[-1].get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                texts = [
                    b.get("text", "") for b in content
                    if isinstance(b, dict) and b.get("type") in ("text", "input") and b.get("text")
                ]
                if texts:
                    return "\n".join(texts)
        if isinstance(body.get("prompt"), str):
            return body["prompt"]
    except Exception:
        return None
    return None


def extract_gemini_prompt(post_data: str) -> Optional[str]:
    """Gemini dispatches application/x-www-form-urlencoded with f.req nested arrays.

    Verified 2026 shape (live capture 2026-09-30): f.req decodes to
    [null, "<INNER JSON STRING>", null, "generic"] where the inner string
    decodes AGAIN to [["<PROMPT>",0,null,...],["en"],null,"<token>"].
    Prompt is at inner[0][0].
    """
    raw = post_data or ""
    # Try form-encoded first
    try:
        parsed_qs = urllib.parse.parse_qs(raw, keep_blank_values=True)
        if "f.req" in parsed_qs:
            fraw = parsed_qs["f.req"][0]
            # f.req is JSON-encoded nested array string
            try:
                outer = json.loads(fraw)
                # Doubly-encoded fast path: outer[1] is itself a JSON string
                if isinstance(outer, list) and len(outer) > 1 and isinstance(outer[1], str):
                    try:
                        inner = json.loads(outer[1])
                        if (isinstance(inner, list) and inner
                                and isinstance(inner[0], list) and inner[0]
                                and isinstance(inner[0][0], str)
                                and inner[0][0].strip()):
                            return inner[0][0].strip()
                    except Exception:
                        pass
                # Walk nested lists to find first meaningful string.
                # Also descends into doubly-encoded JSON strings so a shape
                # drift still resolves instead of leaking the raw blob.
                def walk(node: Any, depth: int = 0) -> Optional[str]:
                    if isinstance(node, str):
                        s = node.strip()
                        if s.startswith("[") and len(s) > 2:
                            try:
                                return walk(json.loads(s), depth)
                            except Exception:
                                pass
                        if len(s) > 1 and depth >= 2:
                            # skip rpc ids like "wrb.fr", hashes
                            if s not in ("wrb.fr",):
                                return s
                        return None
                    if isinstance(node, list):
                        for child in node:
                            found = walk(child, depth + 1)
                            if found:
                                return found
                    return None
                found = walk(outer)
                if found:
                    return found
            except Exception:
                # URL-decoded raw fallback: longest human-like fragment
                decoded = urllib.parse.unquote_plus(fraw)
                candidates = re.findall(r"[A-Za-z][A-Za-z0-9 ,.?!\-]{8,200}", decoded)
                if candidates:
                    return max(candidates, key=len).strip()
                return decoded[:2000]
    except Exception:
        pass
    # Plain JSON fallback
    try:
        body = json.loads(raw)
        return extract_claude_prompt(raw) or json.dumps(body)[:2000]
    except Exception:
        pass
    # Last resort: human-readable slice of the raw string
    cleaned = urllib.parse.unquote_plus(raw)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:2000] if cleaned else None


def extract_outbound_prompt(url: str, post_data: str) -> Tuple[EngineName, Optional[str]]:
    engine = detect_engine(url)
    if engine == "CHATGPT":
        return engine, extract_chatgpt_prompt(post_data)
    if engine == "CLAUDE":
        return engine, extract_claude_prompt(post_data)
    if engine == "GEMINI":
        return engine, extract_gemini_prompt(post_data)
    return engine, None


# ---------------------------------------------------------------- inbound ----

@dataclass
class StreamSignal:
    subqueries: List[str] = field(default_factory=list)
    citations: List[str] = field(default_factory=list)
    tokens: int = 0
    reasoning_present: bool = False
    done: bool = False
    tool_name: Optional[str] = None
    # Set when an outbound prompt is observed on the wire (e.g. Perplexity
    # `perplexity_ask` socket frames carry the user query).
    prompt: Optional[str] = None


_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)

# Embedded widget/asset URLs that are never answer citations (verified in
# live Gemini StreamGenerate frames 2026-09-30: branding SVGs, favicon
# proxy URLs, shopping widget payloads with prds=catalog params).
_JUNK_URL_RES = ("prds=", "shopping_content", "shopping_previous_content",
                 "gstatic.com", "faviconV2", "fonts.g")


def _urls_from_text(text: str) -> List[str]:
    urls = []
    # Ad tokens glue URLs with '&' ("...campaign&https://...") — split first
    # so one match never spans two links (seen live 2026-10-05, bazaar ads).
    text = re.sub(r"&(?=https?://)", " ", text or "")
    for m in _URL_RE.finditer(text):
        u = m.group(0).rstrip(".,);]}")
        low = u.lower()
        if any(j in low for j in _JUNK_URL_RES):
            continue
        if "utm_medium=paid" in low:
            continue  # sponsored ad card, not a source citation
        if "utm_source=chat" in low:
            continue  # client-injected traffic tag (chatgpt click), not a source
        urls.append(u)
    return urls


_QUERY_KEYS = ("query", "search_query", "queries", "search_queries", "q", "text_query")


def _looks_like_query(s: str) -> bool:
    s = (s or "").strip()
    if len(s) < 4 or len(s) > 300 or "\n" in s:
        return False
    low = s.lower()
    if low.startswith(("http://", "https://", "{", "[", "<")):
        return False
    # must contain a letter and a space (i.e. phrase-like, not an id/token)
    return bool(re.search(r"[a-zA-Z]", s)) and (" " in s or len(s) > 12)


def _harvest_queries(node: Any, out: List[str], _depth: int = 0) -> None:
    """Recursively collect query-like strings under query-ish keys.

    Shape-agnostic fallback: works for ChatGPT patch envelopes, Claude tool
    args, Gemini grounding blobs, and Perplexity frames without knowing the
    exact nesting the vendor currently uses.
    """
    if _depth > 8:
        return
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str) and k.lower() in _QUERY_KEYS:
                if isinstance(v, str) and _looks_like_query(v) and v.strip() not in out:
                    out.append(v.strip())
                elif isinstance(v, list):
                    for x in v:
                        if isinstance(x, str) and _looks_like_query(x) and x.strip() not in out:
                            out.append(x.strip())
                        elif isinstance(x, dict):
                            _harvest_queries(x, out, _depth + 1)
            else:
                _harvest_queries(v, out, _depth + 1)
    elif isinstance(node, list):
        for child in node:
            _harvest_queries(child, out, _depth + 1)


def _unwrap_chatgpt_patch(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap /f/conversation JSON-patch envelopes: {"p","o","v":{"message"}}."""
    v = payload.get("v")
    if isinstance(v, dict) and isinstance(v.get("message"), dict):
        merged = {k: val for k, val in payload.items() if k != "v"}
        merged["message"] = v["message"]
        for k in ("conversation_id", "error"):
            if k in v:
                merged[k] = v[k]
        return merged
    return payload


_SEARCH_RECIPIENTS = {"browser.search", "web.search", "search", "browser",
                      "browser.open", "commentary"}


def parse_chatgpt_sse_payload(payload: Dict[str, Any]) -> StreamSignal:
    """Section 4A, updated for the 2026 client.

    Handles both the legacy top-level {"message"} shape and the current
    /f/conversation patch shape {"p","o","v":{"message"}}. Search fan-out
    turns are tool-role messages (recipient browser.search / web.search)
    whose parts carry the rewritten queries.
    """
    sig = StreamSignal()
    if not isinstance(payload, dict):
        return sig
    payload = _unwrap_chatgpt_patch(payload)
    msg = payload.get("message", {})
    if not isinstance(msg, dict):
        # token-only delta shape
        delta = payload.get("delta") or payload.get("choices")
        if delta:
            sig.tokens = 1
        return sig
    content = msg.get("content", {}) if isinstance(msg.get("content"), dict) else {}
    metadata = msg.get("metadata", {}) if isinstance(msg.get("metadata"), dict) else {}
    author = msg.get("author", {}) if isinstance(msg.get("author"), dict) else {}
    ctype = content.get("content_type")
    recipient = str(metadata.get("recipient") or msg.get("recipient") or "").lower()
    # System scaffolding is never a search (verified 2026-10-05: hidden
    # system messages carry metadata.command="prompt" — not a query).
    if author.get("role") == "system" and recipient not in _SEARCH_RECIPIENTS:
        return sig
    is_tool_turn = (
        author.get("role") == "tool"
        or recipient in _SEARCH_RECIPIENTS
        or ctype == "tool_use"
        or "search_result" in metadata
        or bool(metadata.get("command"))
    )

    # tool fan-out
    if is_tool_turn:
        cmd = metadata.get("command")
        parts = content.get("parts")
        queries: List[str] = []
        # Every candidate below passes the phrase-shape guard: bare markers
        # like "prompt", ids, or JSON blobs must never become "queries".
        if isinstance(cmd, str) and _looks_like_query(cmd):
            queries.append(cmd.strip())
        elif isinstance(cmd, dict):
            for v in cmd.values():
                if isinstance(v, str) and _looks_like_query(v):
                    queries.append(v.strip())
                elif isinstance(v, list):
                    queries.extend([x.strip() for x in v
                                    if isinstance(x, str) and _looks_like_query(x)])
        if isinstance(parts, list):
            for p in parts:
                if isinstance(p, str) and p.strip():
                    # parts may be JSON-encoded tool call
                    try:
                        pj = json.loads(p)
                        if isinstance(pj, dict):
                            _harvest_queries(pj, queries)
                            if not queries and _looks_like_query(p):
                                queries.append(p.strip())
                        elif _looks_like_query(p):
                            queries.append(p.strip())
                    except Exception:
                        if _looks_like_query(p):
                            queries.append(p.strip())
                elif isinstance(p, dict):
                    _harvest_queries(p, queries)
        # dedicated search_query field
        sq = metadata.get("search_query")
        if isinstance(sq, str) and _looks_like_query(sq):
            queries.append(sq.strip())
        elif isinstance(sq, list):
            queries.extend([x.strip() for x in sq
                            if isinstance(x, str) and _looks_like_query(x)])
        # shape-agnostic sweep over the whole turn (catches renamed nestings)
        _harvest_queries(msg, queries)
        sig.subqueries.extend([q for q in queries if q])
        sig.tool_name = recipient if recipient in _SEARCH_RECIPIENTS else "web_search"
        # citations possibly embedded
        for u in _urls_from_text(json.dumps(payload)):
            if u not in sig.citations:
                sig.citations.append(u)
        return sig

    # plain assistant token
    parts = content.get("parts")
    if isinstance(parts, list) and parts:
        sig.tokens = sum(len(str(p).split()) for p in parts if isinstance(p, str))
        for p in parts:
            if isinstance(p, str):
                for u in _urls_from_text(p):
                    if u not in sig.citations:
                        sig.citations.append(u)
    if isinstance(author, dict) and author.get("role") == "tool":
        sig.tool_name = "web_search"
    return sig


class ClaudeToolAssembler:
    """Buffers input_json_delta fragments until content_block_stop (Section 4B)."""

    def __init__(self) -> None:
        self.active: Dict[str, Dict[str, str]] = {}

    def handle(self, event_type: Optional[str], payload: Dict[str, Any]) -> StreamSignal:
        sig = StreamSignal()
        if event_type == "content_block_start":
            block = payload.get("content_block", {})
            # 2026 Claude web uses server_tool_use for built-in web_search
            # (id prefix "srvtoolu_"); classic tool_use still supported.
            if isinstance(block, dict) and block.get("type") in ("tool_use", "server_tool_use"):
                tid = str(block.get("id", f"tool_{len(self.active)}"))
                self.active[tid] = {"name": str(block.get("name", "web_search")), "args": ""}
                sig.tool_name = self.active[tid]["name"]
            elif isinstance(block, dict) and block.get("type") == "web_search_tool_result":
                # search results stream with title+url entries — harvest cites
                for u in _urls_from_text(json.dumps(block)):
                    if u not in sig.citations:
                        sig.citations.append(u)
                sig.tool_name = "web_search"
        elif event_type == "content_block_delta":
            delta = payload.get("delta", {})
            if isinstance(delta, dict) and delta.get("type") == "input_json_delta":
                frag = str(delta.get("partial_json", ""))
                for tid in self.active:
                    self.active[tid]["args"] += frag
                sig.tokens = 1
            elif isinstance(delta, dict) and delta.get("type") == "text_delta":
                sig.tokens = len(str(delta.get("text", "")).split())
        elif event_type == "content_block_stop":
            for tid, data in list(self.active.items()):
                raw = data.get("args", "")
                sig.tool_name = data.get("name", "web_search")
                if raw.strip():
                    try:
                        args = json.loads(raw)
                        if isinstance(args, dict):
                            _harvest_queries(args, sig.subqueries)
                            if not sig.subqueries:
                                sig.subqueries.append(raw.strip()[:500])
                        elif isinstance(args, list):
                            sig.subqueries.extend([str(x) for x in args if str(x).strip()])
                        else:
                            sig.subqueries.append(str(args)[:500])
                    except Exception:
                        # fragmented JSON that never completed — keep raw text
                        cleaned = raw.strip().strip('"')[:500]
                        if cleaned:
                            sig.subqueries.append(cleaned)
            self.active.clear()
        elif event_type == "message_stop":
            sig.done = True
        elif event_type in ("message_delta", "content_block_delta"):
            sig.tokens = 1
        # citations in any payload
        for u in _urls_from_text(json.dumps(payload)):
            if u not in sig.citations:
                sig.citations.append(u)
        return sig


def parse_gemini_rpc_line(line: str) -> StreamSignal:
    """Section 4C: envelopes containing wrb.fr, JSON-escaped string at [0][2].

    Verified against 2026 clients: response is anti-XSSI prefixed (`)]}'`)
    length-prefixed frames; envelope lines look like `[["wrb.fr",...]]`,
    inner payload at outer[0][2].

    Answer frames carry a 60-element inner list; sources live at
    inner[4][0][37][1][0][7] as [favicon, url, title] entries (verified
    live 2026-09-30). Structured extraction first, blind URL harvest
    only as fallback.
    """
    sig = StreamSignal()
    s = (line or "").strip().rstrip(",")
    if not s or "wrb.fr" not in s:
        return sig
    if s.startswith(")]}"):
        return sig  # anti-XSSI prefix line, not a frame
    # Strip length prefix ("123\n[...]") if present
    if s[0].isdigit():
        nl = s.find("\n")
        if nl != -1:
            s = s[nl + 1:].strip()
    if not s.lstrip().startswith("["):
        return sig
    try:
        outer = json.loads(s)
        inner_raw = outer[0][2]
        data = json.loads(inner_raw) if isinstance(inner_raw, str) else inner_raw
    except Exception:
        return sig
    blob = json.dumps(data)
    try:
        for u in _gemini_structured_sources(data):
            if u not in sig.citations:
                sig.citations.append(u)
        # candidate search grounding under relative index [4]-style nesting:
        # shape-agnostic sweep for query-ish keys, plus URL harvesting
        _harvest_queries(data, sig.subqueries)
        if not sig.citations:
            # fallback: blind harvest (junk-filtered) for frame types without
            # the structured sources block
            def _walk_urls(node: Any, _d: int = 0) -> None:
                if _d > 8:
                    return
                if isinstance(node, dict):
                    for v in node.values():
                        _walk_urls(v, _d + 1)
                elif isinstance(node, list):
                    for child in node:
                        _walk_urls(child, _d + 1)
                elif isinstance(node, str):
                    for u in _urls_from_text(node):
                        if u not in sig.citations:
                            sig.citations.append(u)
            _walk_urls(data)
    except Exception:
        pass
    # Fallback regex for grounding queries
    if not sig.subqueries:
        for m in re.finditer(r'"(?:query|search_query)"\s*:\s*"([^"]{4,200})"', blob):
            q = m.group(1).strip()
            if q and q not in sig.subqueries:
                sig.subqueries.append(q)
    if sig.subqueries:
        sig.tool_name = "web_search"
    sig.tokens = 1
    return sig


def _gemini_structured_sources(data: Any) -> List[str]:
    """Extract real source URLs from a Gemini answer frame.

    Verified shape (live 2026-09-30): inner[4] is the candidate list;
    each candidate's [37][1][0][7] holds [favicon, url, title] entries.
    The favicon check keeps us from grabbing unrelated string pairs.
    """
    out: List[str] = []
    try:
        cands = data[4] if isinstance(data, list) and len(data) > 4 else []
    except Exception:
        return out
    if not isinstance(cands, list):
        return out
    for cand in cands:
        try:
            entries = cand[37][1][0][7]
        except (IndexError, TypeError, KeyError):
            continue
        if not isinstance(entries, list):
            continue
        for e in entries:
            if (isinstance(e, list) and len(e) >= 2
                    and isinstance(e[0], str) and "favicon" in e[0]
                    and isinstance(e[1], str) and e[1].startswith("http")
                    and e[1] not in out):
                out.append(e[1])
    return out


def parse_perplexity_frame(payload_data: str) -> StreamSignal:
    """Section 4D: Socket.IO 42-frames (verified 2026: still EIO=4 websocket).

    - Outbound `42["perplexity_ask", "<query>", {...}]` carries the user prompt
      (-> sig.prompt).
    - Inbound progress frames carry search_queries under various event names;
      harvested recursively so renames don't silently break capture.
    """
    sig = StreamSignal()
    p = (payload_data or "").strip()
    if not p.startswith("42"):
        return sig
    try:
        packet = json.loads(p[2:])
    except Exception:
        return sig
    if not isinstance(packet, list) or len(packet) < 1:
        return sig
    event_name, body = packet[0], (packet[1] if len(packet) > 1 else None)
    # Outbound user prompt
    if event_name == "perplexity_ask":
        q = body if isinstance(body, str) else (
            body.get("query") if isinstance(body, dict) else None)
        if isinstance(q, str) and q.strip():
            sig.prompt = q.strip()
        return sig
    if not isinstance(body, dict):
        return sig
    if event_name == "query_progress":
        if body.get("status") == "completed":
            sig.done = True
    _harvest_queries(body, sig.subqueries)
    if sig.subqueries:
        sig.tool_name = "web_search"
    for u in _urls_from_text(json.dumps(body)):
        if u not in sig.citations:
            sig.citations.append(u)
    if event_name in ("response", "answer", "citations"):
        sig.tokens = 1
    return sig


def parse_sse_block(event_type: Optional[str], data_str: str,
                    claude: Optional[ClaudeToolAssembler] = None) -> Tuple[StreamSignal, bool]:
    """Parse one SSE data: line. Returns (signal, is_done_sentinel)."""
    if data_str.strip() == "[DONE]":
        return StreamSignal(done=True), True
    try:
        payload = json.loads(data_str)
    except Exception:
        return StreamSignal(), False
    if not isinstance(payload, dict):
        # bare JSON scalar ("prompt", 42, ...) — never a message; must not
        # crash the chunk loop (live 2026-10-05: AttributeError on .get).
        return StreamSignal(), False
    if event_type and event_type.startswith("content_block") or event_type in (
        "message_start", "message_delta", "message_stop"):
        asm = claude or ClaudeToolAssembler()
        return asm.handle(event_type, payload), False
    if "message" in payload or "delta" in payload or "choices" in payload or (
        isinstance(payload.get("v"), dict) and "message" in payload["v"]
    ):
        return parse_chatgpt_sse_payload(payload), False
    # unknown typed event — still harvest urls
    sig = StreamSignal()
    for u in _urls_from_text(data_str):
        sig.citations.append(u)
    return sig, False
