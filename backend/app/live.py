"""Backend-managed live interception.

The FastAPI server itself launches and owns the Playwright browser, so the UI
can start/stop tapping with a button — no separate terminal needed.

State + logs live here:
- `logs/fanout.log`        human-readable backend log (same lines you see in console)
- `logs/wire_events.jsonl` raw wire events, one JSON object per line
- in-memory ring buffer     last 200 events, served via GET /api/live/events
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional

logger = logging.getLogger("LiveManager")

_BASE = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(_BASE, "..", "logs")
HUMAN_LOG = os.path.join(LOG_DIR, "fanout.log")
WIRE_JSONL = os.path.join(LOG_DIR, "wire_events.jsonl")
PROFILE_DIR = os.path.join(_BASE, "..", ".pw-profile")

START_URLS = {
    "PERPLEXITY": "https://www.perplexity.ai",
    "CHATGPT": "https://chat.openai.com",
    "CLAUDE": "https://claude.ai",
    "GEMINI": "https://gemini.google.com",
}

Sink = Callable[[Dict[str, Any]], Awaitable[None]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LiveManager:
    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._sink: Optional[Sink] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.running_engine: Optional[str] = None
        self.running_url: Optional[str] = None
        self.started_at: Optional[str] = None
        self.event_count = 0
        self.last_error: Optional[str] = None
        self.events: Deque[Dict[str, Any]] = deque(maxlen=200)
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(PROFILE_DIR, exist_ok=True)

    # ------------------------------------------------------------- wiring ---
    def set_sink(self, sink: Sink) -> None:
        self._sink = sink

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def status(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "engine": self.running_engine,
            "url": self.running_url,
            "started_at": self.started_at,
            "event_count": self.event_count,
            "last_error": self.last_error,
        }

    # -------------------------------------------------------------- logging ---
    def _write_logs(self, event: Dict[str, Any]) -> None:
        etype = event.get("type")
        line: Optional[str] = None
        if etype == "user_prompt":
            line = f"[{_now()}] USER_PROMPT engine={event.get('engine')} url={event.get('url')} prompt={(event.get('prompt') or '')[:500]}"
        elif etype == "fanout":
            qs = " | ".join(event.get("subqueries", []))[:800]
            line = f"[{_now()}] FANOUT engine={event.get('engine')} tool={event.get('tool')} queries=[{qs}]"
        elif etype == "citations":
            line = f"[{_now()}] CITATIONS engine={event.get('engine')} urls={event.get('citations')}"
        elif etype == "stream_done":
            line = f"[{_now()}] STREAM_DONE engine={event.get('engine')} request={event.get('request_id')}"
        elif etype == "live_started":
            line = f"[{_now()}] LIVE_STARTED engine={event.get('engine')} url={event.get('url')}"
        elif etype == "live_stopped":
            line = f"[{_now()}] LIVE_STOPPED engine={event.get('engine')} reason={event.get('reason')}"
        elif etype == "debug_request":
            line = (f"[{_now()}] DEBUG_REQUEST engine={event.get('engine')} "
                    f"has_post_data={event.get('has_post_data')} url={event.get('url')} "
                    f"body={event.get('post_data_preview', '')[:200]}")
        if line:
            try:
                with open(HUMAN_LOG, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except Exception as exc:
                logger.warning("Could not write backend log: %s", exc)
        try:
            with open(WIRE_JSONL, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({**event, "logged_at": _now()}, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def on_wire_event(self, event: Dict[str, Any]) -> None:
        """Sync CDP-thread entry: buffer + log immediately, forward async."""
        try:
            self.events.appendleft(dict(event))
            self.event_count += 1
            self._write_logs(event)
        except Exception as exc:
            logger.error("on_wire_event bookkeeping failed: %s", exc)
        if self._sink and self._loop and self._loop.is_running():
            try:
                self._loop.create_task(self._sink(dict(event)))
            except Exception as exc:
                logger.error("Failed to schedule sink: %s", exc)

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        return list(self.events)[: max(0, min(limit, 200))]

    def tail_log(self, n: int = 200) -> str:
        try:
            with open(HUMAN_LOG, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
            return "".join(lines[-n:])
        except FileNotFoundError:
            return "(backend log is empty — start a live session first)"
        except Exception as exc:
            return f"(could not read backend log: {exc})"

    # ----------------------------------------------------------- lifecycle ---
    async def start(self, engine: str) -> Dict[str, Any]:
        engine = (engine or "").upper()
        if engine not in START_URLS:
            raise ValueError(f"Unknown engine '{engine}'. Use one of: {sorted(START_URLS)}")
        if self.running:
            return {**self.status(), "note": "already running"}
        self._stop.clear()
        self.last_error = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self._task = asyncio.create_task(self._run(engine))
        # Wait briefly so immediate launch failures surface in the response.
        await asyncio.sleep(4)
        if self._task.done():
            try:
                self._task.result()
            except Exception as exc:
                self.last_error = str(exc)[:500]
                self._write_logs({"type": "live_stopped", "engine": engine,
                                  "reason": f"launch failed: {self.last_error}"})
        if not self.running and self.last_error:
            raise RuntimeError(self.last_error)
        return self.status()

    async def stop(self) -> Dict[str, Any]:
        if not self.running:
            return {**self.status(), "note": "not running"}
        self._stop.set()
        try:
            await asyncio.wait_for(self._task, timeout=15)
        except (asyncio.TimeoutError, Exception):
            if self._task:
                self._task.cancel()
        return self.status()

    async def _run(self, engine: str) -> None:
        from playwright.async_api import async_playwright

        from .interceptor import ConversationalStreamInterceptor

        url = START_URLS[engine]
        self.running_engine = engine
        self.running_url = url
        self.started_at = _now()
        self.on_wire_event({"type": "live_started", "engine": engine, "url": url})
        logger.info("Live interception starting: %s -> %s", engine, url)
        try:
            async with async_playwright() as pw:
                try:
                    context = await pw.chromium.launch_persistent_context(
                        PROFILE_DIR, channel="chrome",
                        headless=False,
                        args=["--disable-web-security",
                              "--disable-blink-features=AutomationControlled"],
                    )
                except Exception:
                    logger.info("Real Chrome unavailable, falling back to bundled Chromium.")
                    context = await pw.chromium.launch_persistent_context(
                        PROFILE_DIR, headless=False,
                        args=["--disable-web-security",
                              "--disable-blink-features=AutomationControlled"],
                    )

                async def _tap(page) -> None:
                    try:
                        cdp = await context.new_cdp_session(page)
                        interceptor = ConversationalStreamInterceptor(on_event=self.on_wire_event)
                        await interceptor.attach(cdp)
                        logger.info("Tapped tab: %s", page.url)
                    except Exception as exc:
                        logger.warning("Could not tap tab: %s", exc)

                context.on("page", lambda p: asyncio.get_running_loop().create_task(_tap(p)))
                for page in context.pages:
                    await _tap(page)
                page = context.pages[0] if context.pages else await context.new_page()
                if not context.pages:
                    await _tap(page)
                await page.goto(url)
                while not self._stop.is_set():
                    await asyncio.sleep(1)
                await context.close()
        except Exception as exc:
            self.last_error = str(exc)[:500]
            logger.error("Live run failed: %s", exc)
            raise
        finally:
            self.on_wire_event({"type": "live_stopped", "engine": engine,
                                "reason": "stopped" if self._stop.is_set() else "crashed"})
            self.running_engine = None
            self.running_url = None
            self.started_at = None
            self._task = None


manager = LiveManager()
