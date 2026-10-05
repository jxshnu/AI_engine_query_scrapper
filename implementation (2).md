# **Implementation Guide: Optimizer 360 Automated AI Stream Interception & Audit Engine**

## **0\. Scope & Operating Model**

This specification details the automated **AI Engine Query Interception Subsystem** for **Optimizer 360**. Unlike a personal diagnostic tool that passively watches a human browse, this is a **fully automated, hands-off cloud audit pipeline**:

* Automated worker nodes fetch prioritized canonical prompts from an orchestrator queue (Temporal.io).  
* Headless Chromium instances spin up in isolated container workers, route egress through rotating residential proxies, and inject prompts directly into target web interfaces (ChatGPT, Claude, Gemini, Perplexity).  
* Low-level Chrome DevTools Protocol (CDP) listeners intercept outbound requests, raw Server-Sent Events (SSE), and WebSocket frames in real time.  
* The system extracts internal machine query fan-outs, intermediate tool executions, attributed citation URLs, and final text tokens before persisting sanitized data to a central PostgreSQL/pgvector datastore.

The web UI of conversational engines surfaces background search sub-queries, tool execution traces, and citation metadata that are often absent or shaped differently in public APIs. This subsystem intercepts those signals directly at the browser transport layer at scale.

## **1\. System Overview**

The production architecture consists of four distributed layers:

&nbsp;

&nbsp;

&nbsp;

&nbsp;┌─────────────────────────────────────────────────────────┐  
&nbsp;│       ORCHESTRATOR & PROMPT QUEUE (Temporal.io)         │  
&nbsp;│  Dispatches scheduled audits based on PDI prioritization│  
&nbsp;└───────────────────────────┬─────────────────────────────┘  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ Audit Job: {prompt, engine, geo}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;▼  
&nbsp;┌─────────────────────────────────────────────────────────┐  
&nbsp;│       HEADLESS WORKER FLEET (Playwright \+ Stealth)      │  
&nbsp;│  • Ephemeral Chromium containers with residential proxy │  
&nbsp;│  • Automated session injection & prompt typing          │  
&nbsp;│  • CDP wire interception (Network domain enabled)       │  
&nbsp;└───────────────────────────┬─────────────────────────────┘  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ Intercepted wire streams (Prompts, Fan-outs, Citations)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;▼  
&nbsp;┌─────────────────────────────────────────────────────────┐  
&nbsp;│       INGESTION & SANITATION GATEWAY (FastAPI)          │  
&nbsp;│  • Microsoft Presidio PII redaction container           │  
&nbsp;│  • Canonical payload normalization                      │  
&nbsp;└───────────────────────────┬─────────────────────────────┘  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ Normalized JSON events  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;▼  
&nbsp;┌─────────────────────────────────────────────────────────┐  
&nbsp;│       ENTERPRISE DATASTORE (PostgreSQL \+ pgvector)      │  
&nbsp;│  Stores prompt clusters, fan-out queries, citations     │  
&nbsp;└───────────────────────────┬─────────────────────────────┘  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ Real-time WebSocket \+ REST  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;▼  
&nbsp;┌─────────────────────────────────────────────────────────┐  
&nbsp;│       OPTIMIZER 360 UI (React \+ Vite \+ shadcn/ui)       │  
&nbsp;│  Citation share, fan-out trees, and visibility scores   │  
&nbsp;└─────────────────────────────────────────────────────────┘

&nbsp;

## **2\. Tech Stack**

&nbsp;

| Layer | Choice | Rationale |
| :---- | :---- | :---- |
| **Worker Runtime** | Python 3.11+ / asyncio | High-throughput asynchronous event loop for concurrent network streaming. |
| **Automation** | Playwright (async\_api) \+ playwright-stealth | Modern headless browser automation with evasions for canvas, WebGL, and webdriver leaks. |
| **Wire Protocol** | Native CDP via context.new\_cdp\_session(page) | Low-level transport hooks into raw chunks prior to DOM rendering or stream closure. |
| **Network Egress** | Rotating Residential Proxies (Bright Data / Oxylabs) | Bypasses datacenter ASN blocks and Cloudflare Turnstile bot challenges. |
| **Task Orchestration** | Temporal.io | Resilient, distributed execution of scheduled audit workflows with auto-retry logic. |
| **Data Sanitation** | Microsoft Presidio (presidio-analyzer / anonymizer) | Air-gapped container running localized NER models to redact PII before DB persistence. |
| **Backend & Storage** | FastAPI \+ PostgreSQL 16 (pgvector) | Enterprise ACID storage, vector similarity search, and high-speed API ingestion. |
| **Frontend** | React \+ Vite \+ Tailwind CSS \+ shadcn/ui | Real-time monitoring dashboard displaying live audit runs and citation telemetry. |

## **3\. Component 1: Headless Cloud Worker & Session Management**

Automated cloud workers boot ephemeral browser contexts equipped with residential proxy authentication and fingerprint cloaking:

&nbsp;

&nbsp;

&nbsp;

Python

\# worker/browser.py  
from playwright.async\_api import async\_playwright  
import os

PROXY\_SERVER \= os.getenv("RESIDENTIAL\_PROXY\_URL")  \# e.g., "http://pr.oxylabs.io:7777"  
PROXY\_USERNAME \= os.getenv("PROXY\_USER")  
PROXY\_PASSWORD \= os.getenv("PROXY\_PASS")

async def launch\_automated\_browser(playwright, country\_iso="US"):  
&nbsp;&nbsp;&nbsp;&nbsp;proxy\_config \= None  
&nbsp;&nbsp;&nbsp;&nbsp;if PROXY\_SERVER:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;proxy\_config \= {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"server": PROXY\_SERVER,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"username": f"{PROXY\_USERNAME}-country-{country\_iso}",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"password": PROXY\_PASSWORD,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}

&nbsp;&nbsp;&nbsp;&nbsp;\# Launch Chromium with anti-detection flags  
&nbsp;&nbsp;&nbsp;&nbsp;browser \= await playwright.chromium.launch(  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;headless=True,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;args=\[  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"--disable-blink-features=AutomationControlled",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"--disable-web-security",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"--no-sandbox",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\],  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;proxy=proxy\_config,  
&nbsp;&nbsp;&nbsp;&nbsp;)

&nbsp;&nbsp;&nbsp;&nbsp;context \= await browser.new\_context(  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;viewport={"width": 1920, "height": 1080},  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;user\_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10\_15\_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",  
&nbsp;&nbsp;&nbsp;&nbsp;)  
&nbsp;&nbsp;&nbsp;&nbsp;return browser, context

&nbsp;

## **4\. Component 2: Production CDP Interception Harness**

This production harness resolves four critical limitations of basic listeners:

> 1. **Multi-Turn User Prompt Resolution:** Correctly extracts the latest active user prompt.  
> 2. **Deterministic Stream Ordering:** Uses synchronous buffer accumulation to eliminate race conditions between incoming network packets.  
> 3. **Multi-Tab Dynamic Tapping:** Uses context.on("page") to intercept across tabs.  
> 4. **Citation Extraction:** Captures citation links across ChatGPT, Claude, Gemini, and Perplexity.

&nbsp;

&nbsp;

&nbsp;

Python

\# worker/harness.py  
import asyncio  
import base64  
import json  
import logging  
from typing import Any, Dict, Optional  
import httpx

logger \= logging.getLogger("OptimizerInterceptor")  
GATEWAY\_URL \= "http://gateway:8000/api/v1/intercepted-events"

PLATFORM\_MATCHERS \= {  
&nbsp;&nbsp;&nbsp;&nbsp;"CHATGPT": lambda url: "/backend-api/conversation" in url,  
&nbsp;&nbsp;&nbsp;&nbsp;"CLAUDE": lambda url: "/chat\_conversations" in url and "completion" in url,  
&nbsp;&nbsp;&nbsp;&nbsp;"GEMINI": lambda url: "StreamGenerate" in url,  
&nbsp;&nbsp;&nbsp;&nbsp;"PERPLEXITY": lambda url: False,  \# Managed via WebSocket frame interception  
}

def detect\_platform(url: str) \-\> Optional\[str\]:  
&nbsp;&nbsp;&nbsp;&nbsp;for name, matcher in PLATFORM\_MATCHERS.items():  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if matcher(url):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return name  
&nbsp;&nbsp;&nbsp;&nbsp;return None

class StreamInterceptor:  
&nbsp;&nbsp;&nbsp;&nbsp;def \_\_init\_\_(self, run\_id: str):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.run\_id \= run\_id  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.buffers: Dict\[str, str\] \= {}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.request\_platform: Dict\[str, str\] \= {}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.active\_tool\_calls: Dict\[str, Dict\[str, str\]\] \= {}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.http \= httpx.AsyncClient(timeout=10.0)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp \= None

&nbsp;&nbsp;&nbsp;&nbsp;async def attach(self, cdp\_session):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp \= cdp\_session  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;await self.cdp.send("Network.enable", {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxPostDataSize": 4\_194\_304,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxResourceBufferSize": 20\_971\_520,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxTotalBufferSize": 104\_857\_600,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp.on("Network.requestWillBeSent", self.\_on\_request)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp.on("Network.responseReceived", self.\_wrap(self.\_on\_response))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp.on("Network.dataReceived", self.\_on\_data)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp.on("Network.loadingFinished", self.\_on\_finished)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp.on("Network.loadingFailed", self.\_on\_finished)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.cdp.on("Network.webSocketFrameReceived", self.\_on\_ws\_frame)

&nbsp;&nbsp;&nbsp;&nbsp;def \_wrap(self, coro):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;def handler(params):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(coro(params))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return handler

&nbsp;&nbsp;&nbsp;&nbsp;def \_on\_request(self, params: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;request \= params.get("request", {})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;url \= request.get("url", "")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;post\_data \= request.get("postData")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;platform \= detect\_platform(url)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if not platform or not post\_data:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;req\_id \= params\["requestId"\]  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.request\_platform\[req\_id\] \= platform  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;prompt\_text \= self.\_extract\_user\_prompt(platform, post\_data)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if prompt\_text:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "USER\_PROMPT",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"run\_id": self.run\_id,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"engine": platform,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": req\_id,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"payload": {"prompt\_text": prompt\_text}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))

&nbsp;&nbsp;&nbsp;&nbsp;def \_extract\_user\_prompt(self, platform: str, post\_data: str) \-\> Optional\[str\]:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;payload \= json.loads(post\_data)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;except Exception:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return post\_data\[:2000\]

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if platform \== "CHATGPT":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Multi-turn safe: Grab the last message submitted by the user  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;messages \= \[m for m in payload.get("messages", \[\]) if m.get("author", {}).get("role") \== "user"\]  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if messages:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;parts \= messages\[-1\].get("content", {}).get("parts", \[\])  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return parts\[0\] if parts else None

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;elif platform \== "CLAUDE":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return payload.get("prompt") or str(payload)\[:2000\]

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return json.dumps(payload)\[:2000\]

&nbsp;&nbsp;&nbsp;&nbsp;async def \_on\_response(self, params: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;req\_id \= params.get("requestId")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;response \= params.get("response", {})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;headers \= {k.lower(): v for k, v in response.get("headers", {}).items()}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;mime \= response.get("mimeType", "").lower()  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;url \= response.get("url", "")

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;is\_sse \= "text/event-stream" in mime or "text/event-stream" in headers.get("content-type", "")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;is\_chunked \= headers.get("transfer-encoding") \== "chunked"  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;is\_gemini \= "StreamGenerate" in url

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if (is\_sse or is\_chunked or is\_gemini) and req\_id:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.buffers\[req\_id\] \= ""  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;result \= await self.cdp.send("Network.streamResourceContent", {"requestId": req\_id})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;initial \= result.get("bufferedData", "")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if initial:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;decoded \= base64.b64decode(initial).decode("utf-8", errors="replace")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.\_parse\_sync(req\_id, decoded, url)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;except Exception:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;pass

&nbsp;&nbsp;&nbsp;&nbsp;def \_on\_data(self, params: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;req\_id \= params.get("requestId")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;data\_b64 \= params.get("data")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if not data\_b64 or req\_id not in self.buffers:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;chunk \= base64.b64decode(data\_b64).decode("utf-8", errors="replace")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Run synchronous framing to prevent out-of-order execution race conditions  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.\_parse\_sync(req\_id, chunk)

&nbsp;&nbsp;&nbsp;&nbsp;def \_on\_finished(self, params: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;req\_id \= params.get("requestId")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.buffers.pop(req\_id, None)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.request\_platform.pop(req\_id, None)

&nbsp;&nbsp;&nbsp;&nbsp;def \_parse\_sync(self, req\_id: str, chunk: str, url: str \= ""):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.buffers\[req\_id\] \= self.buffers.get(req\_id, "") \+ chunk  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;buf \= self.buffers\[req\_id\]  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;platform \= self.request\_platform.get(req\_id, detect\_platform(url) or "")

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if platform \== "GEMINI" or "wrb.fr" in buf:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;lines \= buf.split("\\n")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.buffers\[req\_id\] \= lines\[-1\]  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;for line in lines\[:-1\]:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if '\[\[\["wrb.fr"' in line:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;outer \= json.loads(line)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;inner \= json.loads(outer\[0\]\[2\])  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "GEMINI\_FRAME",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"run\_id": self.run\_id,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"engine": "GEMINI",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": req\_id,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"payload": inner  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;except Exception:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;pass  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Standard SSE Parsing (ChatGPT & Claude)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if "\\n" in buf:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;lines \= buf.split("\\n")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.buffers\[req\_id\] \= lines\[-1\]  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;current\_event \= None  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;for line in lines\[:-1\]:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;line \= line.strip()  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if not line:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;continue  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if line.startswith("event:"):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;current\_event \= line\[6:\].strip()  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;continue  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if line.startswith("data:"):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;data\_str \= line\[5:\].strip()  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if data\_str \== "\[DONE\]":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "STREAM\_END", "run\_id": self.run\_id,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"engine": platform, "request\_id": req\_id, "payload": {}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;continue  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;payload \= json.loads(data\_str)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.\_process\_sse\_payload(req\_id, platform, current\_event, payload)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;except json.JSONDecodeError:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;continue

&nbsp;&nbsp;&nbsp;&nbsp;def \_process\_sse\_payload(self, req\_id: str, platform: str, event\_type: Optional\[str\], payload: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if platform \== "CLAUDE":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if event\_type \== "content\_block\_start":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;block \= payload.get("content\_block", {})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if block.get("type") \== "tool\_use":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.active\_tool\_calls\[block.get("id")\] \= {"name": block.get("name"), "args": ""}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;elif event\_type \== "content\_block\_delta":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;delta \= payload.get("delta", {})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if delta.get("type") \== "input\_json\_delta":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;for tid in self.active\_tool\_calls:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.active\_tool\_calls\[tid\]\["args"\] \+= delta.get("partial\_json", "")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;elif event\_type \== "content\_block\_stop":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;for tid, data in list(self.active\_tool\_calls.items()):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "QUERY\_FANOUT", "run\_id": self.run\_id, "engine": "CLAUDE",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": req\_id, "payload": {"tool": data\["name"\], "query": data\["args"\]}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;self.active\_tool\_calls.clear()

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;elif platform \== "CHATGPT":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;msg \= payload.get("message", {})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;content \= msg.get("content", {})  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;metadata \= msg.get("metadata", {})

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Intercept Query Fan-Out  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if content.get("content\_type") \== "tool\_use" or "search\_result" in metadata:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;subquery \= metadata.get("command") or content.get("parts")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "QUERY\_FANOUT", "run\_id": self.run\_id, "engine": "CHATGPT",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": req\_id, "payload": {"subquery": subquery}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Intercept Citations & Footnotes  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;citations \= metadata.get("citations") or metadata.get("content\_references")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if citations:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "CITATION\_CAPTURED", "run\_id": self.run\_id, "engine": "CHATGPT",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": req\_id, "payload": {"citations": citations}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))

&nbsp;&nbsp;&nbsp;&nbsp;def \_on\_ws\_frame(self, params: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;payload \= params.get("response", {}).get("payloadData", "")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if not payload.startswith("42"):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;name, body \= json.loads(payload\[2:\])  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if name \== "query\_progress":  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Intercept Perplexity Fan-Out  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;queries \= body.get("search\_queries")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if queries:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "QUERY\_FANOUT", "run\_id": self.run\_id, "engine": "PERPLEXITY",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": params.get("requestId", "ws-socket"),  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"payload": {"subqueries": queries}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\# Intercept Perplexity Citations  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;results \= body.get("web\_results") or body.get("extra\_web\_results")  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if results:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;asyncio.create\_task(self.\_emit({  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "CITATION\_CAPTURED", "run\_id": self.run\_id, "engine": "PERPLEXITY",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"request\_id": params.get("requestId", "ws-socket"),  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"payload": {"citations": results}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}))  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;except Exception:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;pass

&nbsp;&nbsp;&nbsp;&nbsp;async def \_emit(self, event: Dict\[str, Any\]):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;await self.http.post(GATEWAY\_URL, json=event)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;except Exception as e:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;logger.error(f"Event emission failed: {e}")

&nbsp;

## **5\. Normalized Enterprise Event Schema**

The ingestion gateway receives intercepted frames, redacts PII using Microsoft Presidio, and writes normalized audit events to storage:

&nbsp;

&nbsp;

&nbsp;

JSON

{  
&nbsp;&nbsp;"audit\_run\_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",  
&nbsp;&nbsp;"engine": "CHATGPT",  
&nbsp;&nbsp;"captured\_at": "2026-09-17T12:45:00.102Z",  
&nbsp;&nbsp;"session\_telemetry": {  
&nbsp;&nbsp;&nbsp;&nbsp;"target\_model": "gpt-4o",  
&nbsp;&nbsp;&nbsp;&nbsp;"country\_iso": "US",  
&nbsp;&nbsp;&nbsp;&nbsp;"proxy\_provider": "OXYLABS\_RESIDENTIAL"  
&nbsp;&nbsp;},  
&nbsp;&nbsp;"user\_prompt": {  
&nbsp;&nbsp;&nbsp;&nbsp;"raw": "Best HIPAA compliant CRM for 10 person team",  
&nbsp;&nbsp;&nbsp;&nbsp;"sanitized": "Best HIPAA compliant CRM for 10 person team",  
&nbsp;&nbsp;&nbsp;&nbsp;"pii\_flagged": false  
&nbsp;&nbsp;},  
&nbsp;&nbsp;"agentic\_retrieval": {  
&nbsp;&nbsp;&nbsp;&nbsp;"query\_fanout\_count": 3,  
&nbsp;&nbsp;&nbsp;&nbsp;"intercepted\_subqueries": \[  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"best HIPAA compliant CRM small team",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"healthcare CRM software pricing 10 users",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"HIPAA CRM reviews"  
&nbsp;&nbsp;&nbsp;&nbsp;\]  
&nbsp;&nbsp;},  
&nbsp;&nbsp;"citations": \[  
&nbsp;&nbsp;&nbsp;&nbsp;{  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"domain": "leadsq.com",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"url": "https://www.leadsq.com/healthcare/hipaa-crm",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"citation\_rank": 1  
&nbsp;&nbsp;&nbsp;&nbsp;},  
&nbsp;&nbsp;&nbsp;&nbsp;{  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"domain": "salesforce.com",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"url": "https://www.salesforce.com/solutions/health-cloud",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"citation\_rank": 2  
&nbsp;&nbsp;&nbsp;&nbsp;}  
&nbsp;&nbsp;\]  
}

&nbsp;

## **6\. Component 3: Enterprise Datastore & Ingestion API**

&nbsp;

&nbsp;

&nbsp;

Python

\# gateway/main.py  
from fastapi import FastAPI, BackgroundTasks  
from pydantic import BaseModel  
from typing import Dict, Any, Optional  
import asyncpg  
import os  
import json

app \= FastAPI(title="Optimizer 360 Telemetry Gateway")  
DB\_URL \= os.getenv("DATABASE\_URL", "postgresql://postgres:postgres@db:5432/optimizer360")

pool: Optional\[asyncpg.Pool\] \= None

@app.on\_event("startup")  
async def startup():  
&nbsp;&nbsp;&nbsp;&nbsp;global pool  
&nbsp;&nbsp;&nbsp;&nbsp;pool \= await asyncpg.create\_pool(DB\_URL, min\_size=5, max\_size=20)

class IngestEvent(BaseModel):  
&nbsp;&nbsp;&nbsp;&nbsp;type: str  
&nbsp;&nbsp;&nbsp;&nbsp;run\_id: str  
&nbsp;&nbsp;&nbsp;&nbsp;engine: str  
&nbsp;&nbsp;&nbsp;&nbsp;request\_id: str  
&nbsp;&nbsp;&nbsp;&nbsp;payload: Dict\[str, Any\]

async def process\_and\_persist(event: IngestEvent):  
&nbsp;&nbsp;&nbsp;&nbsp;async with pool.acquire() as conn:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;await conn.execute(  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"""  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;INSERT INTO engine\_audit\_events (run\_id, engine, event\_type, request\_id, payload)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;VALUES ($1, $2, $3, $4, $5)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;""",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;event.run\_id, event.engine, event.type, event.request\_id, json.dumps(event.payload)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;)

@app.post("/api/v1/intercepted-events")  
async def receive\_event(event: IngestEvent, background\_tasks: BackgroundTasks):  
&nbsp;&nbsp;&nbsp;&nbsp;background\_tasks.add\_task(process\_and\_persist, event)  
&nbsp;&nbsp;&nbsp;&nbsp;return {"status": "QUEUED"}

**PostgreSQL Storage DDL Schema:**

&nbsp;

&nbsp;

&nbsp;

SQL

\-- db/schema.sql  
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE engine\_audit\_runs (  
&nbsp;&nbsp;&nbsp;&nbsp;run\_id UUID PRIMARY KEY,  
&nbsp;&nbsp;&nbsp;&nbsp;canonical\_prompt TEXT NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;engine VARCHAR(32) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;country\_iso VARCHAR(2) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;pdi\_score NUMERIC(5, 2),  
&nbsp;&nbsp;&nbsp;&nbsp;status VARCHAR(32) DEFAULT 'RUNNING',  
&nbsp;&nbsp;&nbsp;&nbsp;created\_at TIMESTAMPTZ DEFAULT NOW()  
);

CREATE TABLE engine\_audit\_events (  
&nbsp;&nbsp;&nbsp;&nbsp;id BIGSERIAL PRIMARY KEY,  
&nbsp;&nbsp;&nbsp;&nbsp;run\_id UUID REFERENCES engine\_audit\_runs(run\_id) ON DELETE CASCADE,  
&nbsp;&nbsp;&nbsp;&nbsp;engine VARCHAR(32) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;event\_type VARCHAR(64) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;request\_id VARCHAR(128) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;payload JSONB NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;created\_at TIMESTAMPTZ DEFAULT NOW()  
);

CREATE TABLE engine\_citations (  
&nbsp;&nbsp;&nbsp;&nbsp;id BIGSERIAL PRIMARY KEY,  
&nbsp;&nbsp;&nbsp;&nbsp;run\_id UUID REFERENCES engine\_audit\_runs(run\_id) ON DELETE CASCADE,  
&nbsp;&nbsp;&nbsp;&nbsp;engine VARCHAR(32) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;domain VARCHAR(255) NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;target\_url TEXT NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;citation\_rank INT NOT NULL,  
&nbsp;&nbsp;&nbsp;&nbsp;created\_at TIMESTAMPTZ DEFAULT NOW()  
);

CREATE INDEX idx\_audit\_events\_run ON engine\_audit\_events(run\_id, event\_type);  
CREATE INDEX idx\_citations\_domain ON engine\_citations(domain);

## **7\. Component 4: Optimizer 360 Real-Time Audit Dashboard**

&nbsp;

&nbsp;

&nbsp;

JavaScript

// frontend/src/components/AuditRunView.jsx  
import React, { useEffect, useState } from "react";  
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";  
import { Badge } from "@/components/ui/badge";  
import { ScrollArea } from "@/components/ui/scroll-area";

export default function AuditRunView({ runId }) {  
&nbsp;&nbsp;const \[events, setEvents\] \= useState(\[\]);  
&nbsp;&nbsp;const \[citations, setCitations\] \= useState(\[\]);  
&nbsp;&nbsp;const \[fanouts, setFanouts\] \= useState(\[\]);

&nbsp;&nbsp;useEffect(() \=\> {  
&nbsp;&nbsp;&nbsp;&nbsp;fetch(\`/api/v1/runs/${runId}/events\`)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.then(res \=\> res.json())  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.then(data \=\> {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;setEvents(data);  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;const extractedFanouts \= \[\];  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;const extractedCitations \= \[\];

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;data.forEach(e \=\> {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if (e.event\_type \=== "QUERY\_FANOUT") {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;extractedFanouts.push(e.payload.subquery || e.payload.query || e.payload.subqueries);  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if (e.event\_type \=== "CITATION\_CAPTURED") {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;extractedCitations.push(...(e.payload.citations || \[\]));  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;});

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;setFanouts(extractedFanouts.flat().filter(Boolean));  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;setCitations(extractedCitations);  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;});  
&nbsp;&nbsp;}, \[runId\]);

&nbsp;&nbsp;return (  
&nbsp;&nbsp;&nbsp;&nbsp;\<div className\="grid grid-cols-1 md:grid-cols-2 gap-6 p-6"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<Card\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<CardHeader\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<CardTitle className\="text-sm font-semibold uppercase tracking-wider text-neutral-500"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Intercepted Query Fan-Outs  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</CardTitle\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</CardHeader\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<CardContent\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<div className\="flex flex-wrap gap-2"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{fanouts.length ? fanouts.map((q, i) \=\> (  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<Badge key\={i} variant\="secondary" className\="px-3 py-1 text-sm bg-neutral-100 text-neutral-800"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{q}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</Badge\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;)) : \<span className\="text-neutral-400 text-sm"\>No fan-outs triggered for this prompt.\</span\>}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</div\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</CardContent\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</Card\>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<Card\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<CardHeader\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<CardTitle className\="text-sm font-semibold uppercase tracking-wider text-neutral-500"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Attributed Engine Citations  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</CardTitle\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</CardHeader\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<CardContent\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<ScrollArea className\="h-64"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<div className\="space-y-3"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{citations.map((c, i) \=\> (  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<div key\={i} className\="flex items-center justify-between border-b pb-2 text-sm"\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<span className\="font-medium text-neutral-700"\>{c.domain || c.url}\</span\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\<Badge variant\="outline"\>Rank \#{i \+ 1}\</Badge\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</div\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;))}  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</div\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</ScrollArea\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</CardContent\>  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\</Card\>  
&nbsp;&nbsp;&nbsp;&nbsp;\</div\>  
&nbsp;&nbsp;);  
}

&nbsp;

## **8\. Running the System End-to-End**

&nbsp;

&nbsp;

&nbsp;

\[ Temporal Workflow Triggered \]  
&nbsp;&nbsp;│ Pulls canonical prompt from PostgreSQL  
&nbsp;&nbsp;▼  
\[ Launch Worker Container \]  
&nbsp;&nbsp;│ Launches Playwright Chromium with residential proxy & anti-fingerprint args  
&nbsp;&nbsp;▼  
\[ Attach CDP Session \]  
&nbsp;&nbsp;│ Calls context.new\_cdp\_session(page) & Network.enable  
&nbsp;&nbsp;▼  
\[ Automated Prompt Injection \]  
&nbsp;&nbsp;│ Types prompt into target UI (\#prompt-textarea) and clicks send  
&nbsp;&nbsp;▼  
\[ Real-Time Interception \]  
&nbsp;&nbsp;│ Wire listeners extract:  
&nbsp;&nbsp;│   1\. Outbound prompt JSON  
&nbsp;&nbsp;│   2\. SSE / WebSocket search tool fan-out calls  
&nbsp;&nbsp;│   3\. Response text & cited URLs  
&nbsp;&nbsp;▼  
\[ Gateway Sanitation & Ingestion \]  
&nbsp;&nbsp;│ Microsoft Presidio scrubs PII; writes audit to PostgreSQL  
&nbsp;&nbsp;▼  
\[ Tear Down \]  
&nbsp;&nbsp;│ Browser context closed; worker container reclaimed

&nbsp;

## **9\. Production Hardening & Operational Safeguards**

* **Cloudflare Turnstile & Bot Mitigation:** In production, automated headless instances running in cloud datacenters are intercepted by Cloudflare Turnstile when hitting chatgpt.com or claude.ai. The system uses **rotating residential proxy pools** (routing each session through unique residential ISP ASNs) combined with playwright-stealth evasions (masking navigator.webdriver, canvas fingerprints, and WebGL renderers).  
* **Session Persistence via Vault:** Workers avoid repeated automated logins (which trigger 2FA and CAPTCHAs) by mounting pre-authenticated session profiles and refresh tokens securely retrieved from HashiCorp Vault or AWS Secrets Manager.  
* **Stream Buffer Decoupling:** When models stream at high token frequencies (e.g., GPT-4o streaming up to 100 tokens/sec), synchronous parsing within CDP event callbacks can cause Chromium's WebSocket buffer to overflow. All received network chunks are appended immediately to a connection-keyed buffer, with event emission handled asynchronously.  
* **Connection Abort Handling:** Conversational frontends routinely terminate network streams via client-side abort controllers when token synthesis completes. The harness listens to Network.loadingFailed alongside Network.loadingFinished, safely reclaiming the buffer without surfacing false-positive connection errors.