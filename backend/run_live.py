"""Live interception runner — opens a real Chromium window and taps CDP wire.

Usage (from backend/):
    $env:PYTHONPATH = (Get-Location).Path
    python run_live.py                          # opens Perplexity (no login needed)
    python run_live.py --url https://chat.openai.com   # needs login in the opened window
    python run_live.py --headless               # headless (logins unlikely to survive bot checks)

How it works:
- Uses a PERSISTENT browser profile (backend/.pw-profile) so your logins survive restarts.
- Attaches ConversationalStreamInterceptor to EVERY page/tab you open.
- Prints captured user prompts + subquery fan-outs to the console.
- If the FastAPI server is running on :8000, events are also POSTed to it so the
  React frontend shows them live. Otherwise they are appended to live_events.jsonl.

Just log in (if needed) and chat normally in the opened window.
Press Ctrl+C in this terminal to stop.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LiveRunner")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

API_BASE = os.environ.get("FANOUT_API", "http://127.0.0.1:8000")
PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".pw-profile")
OUT_JSONL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_events.jsonl")

START_URLS = {
    "perplexity": "https://www.perplexity.ai",
    "chatgpt": "https://chat.openai.com",
    "claude": "https://claude.ai",
    "gemini": "https://gemini.google.com",
}


def api_up() -> bool:
    try:
        with urllib.request.urlopen(API_BASE + "/api/health", timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


def post_json(path: str, payload: dict) -> None:
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            API_BASE + path, data=data, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as exc:
        logger.warning("API POST %s failed (frontend won't update): %s", path, exc)


def save_local(event: dict) -> None:
    try:
        with open(OUT_JSONL, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass


def on_event(event: dict) -> None:
    """Called on the CDP wire loop — must never block or raise."""
    etype = event.get("type")
    if etype == "user_prompt":
        print(f"\n{'='*70}\nUSER PROMPT [{event.get('engine')}]\n{(event.get('prompt') or '')[:2000]}\n{'='*70}", flush=True)
        if API_UP:
            post_json("/api/capture/prompt", {
                "url": event.get("url", ""),
                "post_data": json.dumps({"messages": [{"content": {"parts": [event.get("prompt", "")]}}]}),
                "engine": event.get("engine"),
            })
        else:
            save_local(event)
    elif etype == "fanout":
        print(f"\n  FAN-OUT [{event.get('engine')}] tool={event.get('tool')}", flush=True)
        for i, q in enumerate(event.get("subqueries", []), 1):
            print(f"    {i}. {q}", flush=True)
        if event.get("citations"):
            for c in event["citations"]:
                print(f"       cite: {c}", flush=True)
        if API_UP:
            if event.get("engine") == "PERPLEXITY":
                frame = json.dumps(["query_progress", {
                    "status": "searching", "search_queries": event.get("subqueries", [])}])
                post_json("/api/capture/stream", {
                    "engine": "PERPLEXITY", "ws_payload": "42" + frame})
            else:
                post_json("/api/capture/stream", {
                    "engine": event.get("engine", "CHATGPT"),
                    "request_id": event.get("request_id", "live"),
                    "sse_event": None,
                    "sse_data": json.dumps({"message": {
                        "content": {"content_type": "tool_use",
                                    "parts": event.get("subqueries", [])},
                        "metadata": {"command": (event.get("subqueries", [""]) or [""])[0]}}}),
                })
        else:
            save_local(event)
    elif etype == "citations":
        print(f"  CITATIONS [{event.get('engine')}]: {event.get('citations')}", flush=True)
        save_local(event)


async def attach_to_page(context, page) -> None:
    from app.interceptor import ConversationalStreamInterceptor

    try:
        cdp = await context.new_cdp_session(page)
        interceptor = ConversationalStreamInterceptor(on_event=on_event)
        await interceptor.attach(cdp)
        logger.info("Tapped tab: %s", page.url)
    except Exception as exc:
        logger.warning("Could not tap tab %s: %s", getattr(page, "url", "?"), exc)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=START_URLS["perplexity"])
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    for key, short in (("chatgpt", "chatgpt"), ("claude", "claude"),
                       ("gemini", "gemini"), ("perplexity", "perplexity")):
        if args.url.strip().lower() == key:
            args.url = START_URLS[short]

    global API_UP
    API_UP = api_up()
    if API_UP:
        logger.info("Backend API reachable at %s — frontend will update live.", API_BASE)
    else:
        logger.info("Backend API NOT reachable — events print here + %s. (Start it: python -m uvicorn app.main:app --port 8000)", OUT_JSONL)

    from playwright.async_api import async_playwright

    os.makedirs(PROFILE_DIR, exist_ok=True)
    async with async_playwright() as pw:
        try:
            context = await pw.chromium.launch_persistent_context(
                PROFILE_DIR,
                channel="chrome",  # real Google Chrome: fewer bot checks, no extra download
                headless=args.headless,
                args=["--disable-web-security",
                      "--disable-blink-features=AutomationControlled"],
            )
        except Exception as exc:
            logger.info("Real-Chrome launch failed (%s), falling back to bundled Chromium.", exc)
            context = await pw.chromium.launch_persistent_context(
                PROFILE_DIR,
                headless=args.headless,
                args=["--disable-web-security",
                      "--disable-blink-features=AutomationControlled"],
            )
        context.on("page", lambda p: asyncio.get_running_loop().create_task(attach_to_page(context, p)))
        for page in context.pages:
            await attach_to_page(context, page)
        if not context.pages:
            page = await context.new_page()
            await attach_to_page(context, page)
        else:
            page = context.pages[0]
        await page.goto(args.url)
        logger.info("Interceptor LIVE on %s. Chat normally — prompts/fan-outs print here. Ctrl+C to stop.", args.url)
        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    API_UP = False
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
