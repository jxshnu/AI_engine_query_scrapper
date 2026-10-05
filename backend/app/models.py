"""Normalized ingestion schema — Section 7 of the blueprint."""
from __future__ import annotations

import urllib.parse
from datetime import datetime, timezone
from typing import List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


Engine = Literal["CHATGPT", "CLAUDE", "GEMINI", "PERPLEXITY", "CHATGPT_SEARCH"]


_TRACKING_PARAMS = {"bc", "eid", "ic", "pp", "d", "sc", "hl", "usg", "sa", "ved"}


def _canonical_url(url: str) -> str:
    """Dedupe key: lowercase host+path, tracking params (utm_*, bc, ...) removed.

    ChatGPT appends `?utm_source=chatgpt.com` (and mirrors) to every citation,
    which otherwise explodes one source into dozens of 'different' URLs.
    """
    try:
        parts = urllib.parse.urlsplit(url)
        host = parts.netloc.lower()
        path = parts.path.rstrip("/") or "/"
        qs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        kept = sorted((k, v) for k, v in qs
                      if not k.lower().startswith("utm_") and k not in _TRACKING_PARAMS)
        return urllib.parse.urlunsplit(("", host, path,
                                        urllib.parse.urlencode(kept), ""))
    except Exception:
        return url


def _is_noise_url(url: str) -> bool:
    """Favicon/telemetry thumbnails riding along in streams, not citations."""
    try:
        parts = urllib.parse.urlsplit(url.lower())
        return (parts.netloc == "www.google.com" and parts.path.startswith("/s2/")) \
            or parts.path.endswith((".ico", ".png", ".svg"))
    except Exception:
        return False


class SessionMetadata(BaseModel):
    model_id: str = "unknown"
    country_iso: str = "USA"
    proxy_asn: str = "direct"
    headless_fingerprint_id: str = "default"


class UserInteraction(BaseModel):
    raw_prompt_text: str = ""
    sanitized_prompt_text: str = ""
    pii_detected: bool = False


class ToolCall(BaseModel):
    tool_name: str = "web_search"
    latency_ms: int = 0


class AgenticRetrieval(BaseModel):
    query_fanout_count: int = 0
    intercepted_subqueries: List[str] = Field(default_factory=list)
    intermediate_tools_called: List[ToolCall] = Field(default_factory=list)


class Citation(BaseModel):
    domain: str = ""
    url: str = ""
    citation_index: int = 0


class ModelResponse(BaseModel):
    total_token_count: int = 0
    reasoning_trace_present: bool = False
    attributed_citations: List[Citation] = Field(default_factory=list)
    shopping_modules_triggered: bool = False


class InteractionEvent(BaseModel):
    interaction_id: str = Field(default_factory=lambda: str(uuid4()))
    engine: Engine = "CHATGPT"
    captured_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    session_metadata: SessionMetadata = Field(default_factory=SessionMetadata)
    user_interaction: UserInteraction = Field(default_factory=UserInteraction)
    agentic_retrieval: AgenticRetrieval = Field(default_factory=AgenticRetrieval)
    model_response: ModelResponse = Field(default_factory=ModelResponse)

    def add_subqueries(self, queries: List[str]) -> None:
        for q in queries:
            q = (q or "").strip()
            if q and q not in self.agentic_retrieval.intercepted_subqueries:
                self.agentic_retrieval.intercepted_subqueries.append(q)
        self.agentic_retrieval.query_fanout_count = len(
            self.agentic_retrieval.intercepted_subqueries
        )

    def add_citations(self, urls: List[str]) -> None:
        seen = {_canonical_url(c.url) for c in self.model_response.attributed_citations}
        for u in urls:
            u = (u or "").strip()
            if not u or _is_noise_url(u):
                continue
            key = _canonical_url(u)
            if key in seen:
                continue
            try:
                domain = u.split("//", 1)[1].split("/", 1)[0].lower()
            except IndexError:
                domain = u
            self.model_response.attributed_citations.append(
                Citation(
                    domain=domain,
                    url=u,
                    citation_index=len(self.model_response.attributed_citations) + 1,
                )
            )
            seen.add(key)
