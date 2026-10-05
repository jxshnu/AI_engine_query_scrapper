"""Live CDP interception harness — Section 6, production-hardened.

Fixes applied over the blueprint draft:
- CDP listeners are synchronous callbacks; async `streamResourceContent`
  calls are scheduled via the running loop instead of `await`-in-callback.
- Per-request URL is tracked (report version dropped it after the first chunk).
- `webSocketFrameSent` is also tapped (outbound Perplexity prompts).
- All extracted signals are forwarded to an `on_event` hook + asyncio.Queue
  so the FastAPI layer can persist + broadcast without blocking the wire loop.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from .parsers import (
    ClaudeToolAssembler,
    detect_engine,
    extract_outbound_prompt,
    parse_gemini_rpc_line,
    parse_perplexity_frame,
    parse_sse_block,
)

logger = logging.getLogger("StreamInterceptor")


class ConversationalStreamInterceptor:
    def __init__(
        self,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        queue: Optional[asyncio.Queue] = None,
    ) -> None:
        self.stream_buffers: Dict[str, str] = {}
        self.request_urls: Dict[str, str] = {}
        # requestId -> engine, learned from the outbound POST. For fetch-based
        # streams the inbound stream shares the POST's requestId, so this is
        # the reliable engine label (URL sniffing alone mislabels traffic).
        self.request_engines: Dict[str, str] = {}
        self.claude_asm: Dict[str, ClaudeToolAssembler] = {}
        self._sampled_sizes: Dict[str, int] = {}
        self.cdp = None
        self.on_event = on_event
        self.queue: asyncio.Queue = queue or asyncio.Queue()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------ attach ---
    async def attach(self, cdp_session) -> None:
        self.cdp = cdp_session
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        await self.cdp.send(
            "Network.enable",
            {
                "maxPostDataSize": 2097152,
                "maxResourceBufferSize": 10485760,
                "maxTotalBufferSize": 52428800,
            },
        )
        # Capture service/worker targets too (Section 8 hardening)
        try:
            await self.cdp.send("Target.setAutoAttach", {"autoAttach": True, "waitForDebuggerOnStart": False, "flatten": True})
        except Exception:
            pass
        self.cdp.on("Network.requestWillBeSent", self._on_request_will_be_sent)
        self.cdp.on("Network.responseReceived", self._on_response_received)
        self.cdp.on("Network.dataReceived", self._on_data_received)
        self.cdp.on("Network.loadingFinished", self._on_loading_finished)
        self.cdp.on("Network.loadingFailed", self._on_loading_failed)
        self.cdp.on("Network.webSocketFrameReceived", self._on_ws_frame)
        try:
            self.cdp.on("Network.webSocketFrameSent", self._on_ws_frame)
        except Exception:
            pass

    # ------------------------------------------------------------- emit ---
    def _emit(self, event: Dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(event)
        except Exception:
            pass
        if self.on_event:
            try:
                self.on_event(event)
            except Exception as exc:  # never break the wire loop
                logger.error("on_event handler failed: %s", exc)

    # Keywords used to spot conversational traffic for debug logging.
    # The tap itself keys off exact routes in detect_engine(); this wider net
    # only produces low-volume "debug_request" events so unknown/changed
    # endpoint shapes show up in logs/fanout.log instead of silently missing.
    _DEBUG_URL_HINTS = ("conversation", "completion", "streamgenerate",
                        "generate", "backend-api", "chat_conversations")

    def _engine_for(self, request_id: Optional[str], url: str,
                    current_event: Optional[str] = None) -> str:
        if request_id and request_id in self.request_engines:
            return self.request_engines[request_id]
        engine = detect_engine(url)
        if engine != "UNKNOWN":
            return engine
        if "completion" in url or current_event:
            return "CLAUDE"
        return "CHATGPT"

    # ---------------------------------------------------------- handlers ---
    def _on_request_will_be_sent(self, params: Dict[str, Any]) -> None:
        request = params.get("request", {}) or {}
        url = request.get("url", "") or ""
        method = str(request.get("method", "")).upper()
        request_id = params.get("requestId")
        post_data = request.get("postData") or request.get("post_data")

        # Learn engine per requestId even when there is no body (e.g. the
        # later stream for the same request), so inbound chunks label right.
        known = detect_engine(url)
        if known != "UNKNOWN" and request_id:
            self.request_engines[request_id] = known

        # Debug: surface candidate conversational requests (low volume).
        if method == "POST" and any(h in url.lower() for h in self._DEBUG_URL_HINTS):
            preview = ""
            if isinstance(post_data, str):
                preview = post_data[:300].replace("\n", " ")
            self._emit({
                "type": "debug_request",
                "engine": known,
                "url": url,
                "method": method,
                "has_post_data": bool(post_data),
                "post_data_preview": preview,
                "request_id": request_id,
            })

        if not post_data or not isinstance(post_data, str):
            return
        engine, prompt = extract_outbound_prompt(url, post_data)
        if engine == "UNKNOWN":
            return
        # ChatGPT housekeeping endpoints submit no user message (/init shell,
        # /prepare, title gen, ads) — never mint prompts from them.
        low_url = url.lower()
        if engine == "CHATGPT" and any(
            h in low_url for h in ("/init", "/prepare", "gen_title",
                                   "bazaar", "feedback", "abuse", "/limit")
        ):
            return
        logger.info("CAPTURED OUTBOUND PROMPT -> %s [%s]", url, engine)
        self._emit({
            "type": "user_prompt",
            "engine": engine,
            "url": url,
            "prompt": prompt,
            # raw preview fallback: if extraction missed a renamed shape,
            # the backend can still show/store something instead of "".
            "post_data_preview": post_data[:500] if isinstance(post_data, str) else "",
            "request_id": request_id,
        })

    def _on_response_received(self, params: Dict[str, Any]) -> None:
        request_id = params.get("requestId")
        response = params.get("response", {}) or {}
        if not request_id:
            return
        mime = str(response.get("mimeType", "")).lower()
        headers = {str(k).lower(): v for k, v in (response.get("headers", {}) or {}).items()}
        url = response.get("url", "") or ""
        ctype = str(headers.get("content-type", "")).lower()
        is_sse = "text/event-stream" in mime or "text/event-stream" in ctype
        is_chunked = headers.get("transfer-encoding") == "chunked"
        is_gemini = "StreamGenerate" in url
        # Perf: only buffer conversational traffic. Arming streamResourceContent
        # on EVERY chunked response (telemetry, static assets, beacons) forces
        # Chromium to retain megabytes per request and routes every chunk
        # through our Python callbacks — that is what made pages feel slow.
        low = url.lower()
        is_conversational = (
            "conversation" in low or "completion" in low
            or "streamgenerate" in low or "socket.io" in low
        )
        if not (is_sse or is_gemini or (is_chunked and is_conversational)):
            return
        self.stream_buffers[request_id] = ""
        self.request_urls[request_id] = url
        if self.cdp is None:
            return

        async def _arm() -> None:
            try:
                result = await self.cdp.send("Network.streamResourceContent", {"requestId": request_id})
                initial = result.get("bufferedData", "") if isinstance(result, dict) else ""
                if initial:
                    decoded = base64.b64decode(initial).decode("utf-8", errors="replace")
                    self._parse_stream_data(request_id, decoded)
            except Exception:
                pass

        if self._loop and self._loop.is_running():
            self._loop.create_task(_arm())

    def _on_data_received(self, params: Dict[str, Any]) -> None:
        request_id = params.get("requestId")
        data_b64 = params.get("data")
        if not data_b64 or request_id not in self.stream_buffers:
            return
        try:
            chunk = base64.b64decode(data_b64).decode("utf-8", errors="replace")
            self._parse_stream_data(request_id, chunk)
        except Exception as exc:
            logger.error("Failed to decode chunk: %s", exc)

    # -------------------------------------------------------------- parse ---
    _SAMPLE_DIR = "logs/raw_samples"
    _SAMPLE_CAP = 200 * 1024  # first 200KB per request — enough for shapes
    # TEMPORARY 15 (was 3): late StreamGenerate frames carry Gemini grounding
    # queries — need them sampled once to finalize the grounding parser, then
    # lower back to 3. Raised 2026-09-30.
    _SAMPLE_WRITES = 15  # perf: only the first chunks matter for shapes

    def _save_sample(self, request_id: str, url: str, chunk: str) -> None:
        """Persist raw wire chunks for ground-truth parser debugging.

        Only for conversational endpoints, first few chunks per request.
        Sizes tracked in memory so capped-out requests cost zero disk I/O —
        previously every chunk did makedirs + getsize + open + write.
        """
        try:
            n = self._sampled_sizes.get(request_id, 0)
            if n >= self._SAMPLE_WRITES:
                return
            low = (url or "").lower()
            if not any(k in low for k in ("conversation", "completion",
                                          "streamgenerate", "socket.io")):
                self._sampled_sizes[request_id] = self._SAMPLE_WRITES  # don't recheck
                return
            import os as _os
            base = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                                 "..", self._SAMPLE_DIR)
            _os.makedirs(base, exist_ok=True)
            safe = "".join(c if c.isalnum() else "_" for c in (request_id or "noid"))[:40]
            path = _os.path.join(base, f"{safe}.txt")
            if n == 0 and _os.path.exists(path) and _os.path.getsize(path) >= self._SAMPLE_CAP:
                self._sampled_sizes[request_id] = self._SAMPLE_WRITES
                return
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(f"\n### URL: {url}\n{chunk[:20000]}")
            self._sampled_sizes[request_id] = n + 1
            # TEMPORARY (2026-09-30): rolling latest-chunk file per request.
            # Gemini's query-bearing frame arrives at END of stream, past any
            # first-N cap on long answers. Overwrite keeps one small file per
            # request; remove once the grounding shape is finalized.
            try:
                if "streamgenerate" in low:
                    latest = _os.path.join(base, f"{safe}_latest.txt")
                    with open(latest, "w", encoding="utf-8") as fh:
                        fh.write(f"### URL: {url}\n{chunk[:50000]}")
            except Exception:
                pass
        except Exception:
            pass

    def _parse_stream_data(self, request_id: str, chunk: str) -> None:
        url = self.request_urls.get(request_id, "")
        self._save_sample(request_id, url, chunk)
        self.stream_buffers[request_id] = self.stream_buffers.get(request_id, "") + chunk
        buffer = self.stream_buffers[request_id]

        # 1. Gemini length-prefixed RPC arrays
        if "StreamGenerate" in url or "wrb.fr" in buffer:
            lines = buffer.split("\n")
            self.stream_buffers[request_id] = lines[-1]  # keep tail (Section 8: fragmentation)
            for line in lines[:-1]:
                line = line.strip()
                if not line or "wrb.fr" not in line:
                    continue
                sig = parse_gemini_rpc_line(line)
                if sig.subqueries:
                    self._emit({
                        "type": "fanout", "engine": "GEMINI",
                        "request_id": request_id, "url": url,
                        "subqueries": sig.subqueries, "citations": sig.citations,
                        "tool": sig.tool_name or "web_search",
                    })
                elif sig.citations:
                    self._emit({
                        "type": "citations", "engine": "GEMINI",
                        "request_id": request_id, "url": url,
                        "citations": sig.citations,
                    })
            return

        # 2. Standard / typed SSE (ChatGPT + Claude)
        if "\n" not in buffer:
            return
        lines = buffer.split("\n")
        self.stream_buffers[request_id] = lines[-1]
        current_event: Optional[str] = None
        for line in lines[:-1]:
            s = line.strip()
            if not s:
                continue
            if s.startswith("event:"):
                current_event = s[len("event:"):].strip()
                continue
            if s.startswith("data:"):
                data_str = s[len("data:"):].strip()
                asm = self.claude_asm.setdefault(request_id, ClaudeToolAssembler())
                sig, _ = parse_sse_block(current_event, data_str, asm)
                engine = self._engine_for(request_id, url, current_event)
                if sig.subqueries:
                    self._emit({
                        "type": "fanout", "engine": engine,
                        "request_id": request_id, "url": url,
                        "subqueries": sig.subqueries, "citations": sig.citations,
                        "tool": sig.tool_name or "web_search",
                    })
                elif sig.citations:
                    self._emit({
                        "type": "citations", "engine": engine,
                        "request_id": request_id, "url": url,
                        "citations": sig.citations,
                    })
                if sig.done:
                    self._emit({"type": "stream_done", "engine": engine,
                                "request_id": request_id, "url": url})
                current_event = None

    def _on_ws_frame(self, params: Dict[str, Any]) -> None:
        response = params.get("response", {}) or {}
        payload = response.get("payloadData", "") or ""
        if not isinstance(payload, str) or not payload.startswith("42"):
            return
        sig = parse_perplexity_frame(payload)
        if sig.prompt:
            self._emit({"type": "user_prompt", "engine": "PERPLEXITY",
                        "prompt": sig.prompt, "url": "socket.io"})
        if sig.subqueries:
            self._emit({
                "type": "fanout", "engine": "PERPLEXITY",
                "subqueries": sig.subqueries, "citations": sig.citations,
                "tool": sig.tool_name or "web_search",
            })
        elif sig.citations:
            self._emit({"type": "citations", "engine": "PERPLEXITY",
                        "citations": sig.citations})

    def _drop_request(self, request_id: Optional[str]) -> None:
        self.stream_buffers.pop(request_id, None)
        self.request_urls.pop(request_id, None)
        self.request_engines.pop(request_id, None)
        self.claude_asm.pop(request_id, None)
        self._sampled_sizes.pop(request_id, None)

    def _on_loading_finished(self, params: Dict[str, Any]) -> None:
        self._drop_request(params.get("requestId"))

    def _on_loading_failed(self, params: Dict[str, Any]) -> None:
        # net::ERR_ABORTED is normal for client-aborted streams (Section 8)
        self._drop_request(params.get("requestId"))


async def run_live_capture(
    start_url: str = "https://www.perplexity.ai",
    headless: bool = False,
    on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> None:
    """Entry point for real browser interception (Section 5, steps 1-2)."""
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-web-security",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context()
        page = await context.new_page()
        cdp = await context.new_cdp_session(page)
        interceptor = ConversationalStreamInterceptor(on_event=on_event)
        await interceptor.attach(cdp)
        logger.info("Interceptor active across ChatGPT, Claude, Gemini, Perplexity.")
        await page.goto(start_url)
        while True:
            await asyncio.sleep(1)
