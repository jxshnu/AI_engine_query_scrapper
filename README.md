# Fan-Out Interceptor — User Prompt + Subquery Scraper

Captures, from your own live AI-chat sessions, the **exact user prompt**, the **internal search queries the model actually issued** (fan-outs), and the **cited sources** — for ChatGPT, Claude, Gemini, and Perplexity. Every recorded query is byte-traceable to a saved server payload: observed, never guessed.

![Live interception UI — Claude capture with prompt, fan-out query, tool call, citations, history and wire-event feed](<docs/Screenshot 2026-10-05 170044.png>)

## Tech stack

| Layer | Technology |
|---|---|
| Backend | Python 3.10, FastAPI + Uvicorn |
| Browser automation | Playwright driving real Google Chrome with a persistent profile (login once, cookies survive restarts) |
| Interception | Chrome DevTools Protocol — `Network.requestWillBeSent`, `responseReceived`, `dataReceived`, `streamResourceContent`, WebSocket frames |
| Parsing | Hand-written per-engine parsers (`backend/app/parsers.py`). No ML/LLM in the loop |
| Frontend | React + TypeScript, Vite, shadcn-style minimal UI, WebSocket live feed |
| Storage | Plain JSONL + text logs (no database): `interactions.jsonl`, `logs/wire_events.jsonl`, `logs/fanout.log`, `logs/raw_samples/` |
| Tests | pytest — 34 tests, all passing |

## Architecture

```
CONTROLLED CHROME ──CDP (requests, SSE chunks, RPC frames, socket frames)──▶ INTERCEPTOR
  (user chats here)                                                          (interceptor.py, keyed by requestId)
                                                                                       │
                                                                              PARSERS (parsers.py)
                                                              ChatGPT │ Claude │ Gemini │ Perplexity
                                                                                       │
REACT UI ◀── WebSocket push ── LIVE SINK (main.py) ──▶ JSONL store + fanout.log + raw_samples/
(results, history, wire feed)      (REST: /api/live/*, /api/capture/*, /api/simulate)
```

- **Controlled browser, not a redirect.** The backend opens its own Chrome window and attaches via CDP — that attachment is what makes interception possible.
- **Request-ID correlation** joins outbound prompts, stream chunks, and citations (plus per-engine fallback). This fixed real misattribution bugs (e.g. ChatGPT citations labeled as Claude).
- **Raw-sample audit trail** (`logs/raw_samples/`) stores verbatim wire chunks per request. Every parser fix in this project was derived from these bytes — never from documentation alone.

## Per-engine wire formats (verified against 2026 live traffic)

| Engine | Prompt | Fan-out queries | Citations |
|---|---|---|---|
| **ChatGPT** | `POST …/backend-api/f/conversation` → last `role:user` message's `parts[]` | Tool-role turns in JSON-patch envelopes (`v.message`, `recipient: browser.search`); queries in `command` / JSON `parts` / `search_query` | URLs in tool turns (ads + widget junk filtered) |
| **Claude** | Completion endpoint → `messages[-1].content` | Typed SSE: `server_tool_use` + index-keyed `input_json_delta` assembly → `.query`; results in `web_search_tool_result` | `content[].url` — cleanest protocol of the four |
| **Gemini** | `StreamGenerate` form body, **doubly-encoded** `f.req` → `inner[0][0]` | ⚠️ Not sent to the web client (status labels + sources only; one ambiguous `"11"` metadata frame under investigation) | Structured `[favicon, url, title]` triples at `inner[4][0][37][1][0][7]` |
| **Perplexity** | Socket.IO `42["perplexity_ask", "<query>"]` | Explicit `search_queries` array in `query_progress` frames | `response` / `citations` frames |

Guards that keep captures honest: system-role turns are never tool turns; every query candidate passes a phrase-shape filter (bare markers like `"prompt"`, IDs, URLs, JSON blobs rejected); ChatGPT housekeeping endpoints (`/init`, `/prepare`, title-gen, ads) never mint prompts; citations-only frames never record tool calls.

## Key findings

1. **The wire beats the docs.** ChatGPT's endpoint moved (`/conversation` → `/f/conversation`), Gemini's `f.req` is doubly-encoded, and 2025-era write-ups describe dead clients. Capture → read bytes → code → test is the enforced method.
2. **Gemini withholds queries client-side.** Full-stream scans of complete conversations show status + sources only — a per-engine reality, not a tap failure. Its citations, however, extract cleanly and structurally.
3. **Actual vs. probabilistic is provable:** dead-end queries absent from the answer, rewrite fingerprints (words in neither prompt nor answer), replay variance across identical prompts, and byte provenance — each recorded query is a verbatim substring of a saved server payload.
4. **Known limits.** Server-side-only activity (e.g. internal reranking never sent to the client) is unobservable by any client-side scraper. The system proves everything it records is real; it makes no claim to see everything real.

## Run it

Prerequisites: Windows, Python 3.10+, Node.js 18+, Google Chrome installed. No Playwright browser download needed (we drive your real Chrome).

**1. Backend** (terminal 1):

```powershell
cd "D:\AI scraper\backend"
python -m pip install -r requirements.txt
$env:PYTHONPATH = (Get-Location).Path
python -m pytest tests -q          # expect: 34 passed
python -m uvicorn app.main:app --port 8000
```

**2. Frontend** (terminal 2):

```powershell
cd "D:\AI scraper\frontend"
npm install
npm run dev                        # open http://localhost:5173
```

**3. Intercept:**

1. In the UI's **Live interception** card, click **Open & Intercept** for an engine. A controlled Chrome window opens with the tap attached.
2. Log in once inside that window (never in your normal browser) — the profile persists, so next time there's no login.
3. Chat normally. Prompts, fan-outs, tool calls, and citations stream into the UI live; history persists in `backend/interactions.jsonl`.
4. **Stop** ends the tap and closes the window. Backend logs mirror everything at `backend/logs/fanout.log`.

Notes:

- The backend runs **without auto-reload**: after any code change, `Ctrl+C` and rerun the uvicorn command, then Stop/Start the tap in the UI.
- `Clear` in History wipes `interactions.jsonl` only — not `logs/` or the login profile.
- Everything is stored **plaintext locally** (raw prompts included). Don't share `.pw-profile/` or `logs/`.
- Offline demo (no login): the **Demo scrape** card runs the same parsers over sample traffic per engine.

## Requirements & limitations — read before running

- **You must be logged in to every provider you tap** — inside the controlled Chrome window, not your normal browser. Perplexity works without login; ChatGPT, Claude, and Gemini require one. Login once per provider; the persistent profile keeps you signed in until the session/cookie expires, then you log in again in the tapped window.
- **Google Chrome only.** The tap launches real Chrome (`channel="chrome"`) and depends on its CDP behavior. Other browsers (Edge, Brave, Firefox) are unsupported; the bundled-Chromium fallback exists but is untested and will hit harder bot checks.
- **One tap at a time.** A single persistent profile means a second tap would fight over the profile lock. Stop the current tap before starting another engine.
- **Ports 8000 (backend) and 5173 (frontend) must be free.**
- **No backend auto-reload.** After any code change: `Ctrl+C`, rerun uvicorn, then Stop/Start the tap in the UI — otherwise you're testing stale code.
- **Tested on Windows + PowerShell.** Paths and commands above assume that environment.
- **Heavy by design.** Expect two Chrome instances (yours + tapped), plus both servers. Close your personal Chrome while capturing if pages feel slow.
- **Bot defenses still apply.** It's a real browser, so logins/2FA work normally — but providers can still throttle, CAPTCHA, or force re-login on automated-profile traffic. If a site suddenly logs you out in the tapped window, that's the provider, not a bug.
- **Gemini fan-out gap.** Gemini's web client does not receive its search queries over the wire (status + sources only), so fan-out lists stay empty there by evidence, not by defect. ChatGPT, Claude, and Perplexity expose theirs.
- **Local plaintext storage.** Prompts (raw + sanitized), queries, citations, logs, and login cookies all sit unencrypted on disk. Single-user research tool — do not expose the backend port or share the folder.

## Repo hygiene

Committed: code, tests, this README, `implementation (2).md` (design blueprint). Deliberately **not** committed (see `.gitignore`): login profile, all logs and raw wire samples, stored interactions, `node_modules/`, build output.
