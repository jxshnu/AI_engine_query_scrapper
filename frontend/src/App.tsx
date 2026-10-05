import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Activity, Trash2, User, Zap, Search, ExternalLink, Radio, MonitorPlay, Square, FlaskConical } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/input";
import { api, type Interaction, type LiveStatus } from "@/lib/api";
import { cn } from "@/lib/utils";

const ENGINES = [
  { id: "PERPLEXITY", url: "https://www.perplexity.ai", hint: "no login needed" },
  { id: "CHATGPT", url: "https://chat.openai.com", hint: "login once" },
  { id: "CLAUDE", url: "https://claude.ai", hint: "login once" },
  { id: "GEMINI", url: "https://gemini.google.com", hint: "login once" },
] as const;
type Engine = (typeof ENGINES)[number]["id"];

const DEFAULT_PROMPT =
  "What is the best HIPAA compliant CRM for a 15 person healthtech startup integrating with Snowflake?";

function eventLabel(e: Record<string, unknown>): string {
  const t = new Date().toLocaleTimeString();
  const type = String(e.type ?? "event");
  const eng = e.engine ? ` (${e.engine})` : "";
  if (type === "fanout" && Array.isArray(e.added_subqueries)) {
    const n = (e.added_subqueries as unknown[]).length;
    return `${t} · fanout${eng} +${n} subqueries`;
  }
  return `${t} · ${type}${eng}`;
}

export default function App() {
  const [engine, setEngine] = useState<Engine>("PERPLEXITY");
  const [prompt, setPrompt] = useState(DEFAULT_PROMPT);
  const [items, setItems] = useState<Interaction[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [live, setLive] = useState(false);
  const [backendOk, setBackendOk] = useState<boolean | null>(null);
  const [busy, setBusy] = useState(false);
  const [events, setEvents] = useState<string[]>([]);
  const [tap, setTap] = useState<LiveStatus | null>(null);
  const [tapBusy, setTapBusy] = useState<string | null>(null);
  const [tapError, setTapError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  const selected = useMemo(
    () => items.find((i) => i.interaction_id === selectedId) ?? items[0] ?? null,
    [items, selectedId]
  );

  const refresh = useCallback(async () => {
    try {
      const data = await api.list(50);
      setItems(data);
      setBackendOk(true);
    } catch {
      setBackendOk(false);
    }
  }, []);

  const refreshTap = useCallback(async () => {
    try {
      setTap(await api.liveStatus());
    } catch {
      /* backend down — backendOk badge covers it */
    }
  }, []);

  useEffect(() => {
    refresh();
    refreshTap();
    api.liveEvents(30)
      .then((evts) => setEvents(evts.reverse().map(eventLabel)))
      .catch(() => {});
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/live`);
    wsRef.current = ws;
    ws.onopen = () => setLive(true);
    ws.onclose = () => setLive(false);
    ws.onerror = () => setLive(false);
    ws.onmessage = (msg) => {
      try {
        const data = JSON.parse(msg.data);
        setEvents((prev) => [eventLabel(data), ...prev].slice(0, 60));
      } catch {
        /* ignore */
      }
      refresh();
      refreshTap();
    };
    const poll = setInterval(refreshTap, 3000);
    return () => {
      ws.close();
      clearInterval(poll);
    };
  }, [refresh, refreshTap]);

  async function onOpenAndIntercept(id: Engine) {
    setTapBusy(id);
    setTapError(null);
    try {
      const st = await api.liveStart(id);
      setTap(st);
    } catch (e) {
      setTapError(e instanceof Error ? e.message : "failed to start");
    } finally {
      setTapBusy(null);
    }
  }

  async function onStopTap() {
    setTapBusy("stop");
    try {
      setTap(await api.liveStop());
    } finally {
      setTapBusy(null);
    }
  }

  async function onSimulate() {
    if (!prompt.trim() || busy) return;
    setBusy(true);
    try {
      const out = await api.simulate(engine, prompt.trim());
      setItems((prev) => [out, ...prev.filter((i) => i.interaction_id !== out.interaction_id)]);
      setSelectedId(out.interaction_id);
      setEvents((prev) => [`${new Date().toLocaleTimeString()} · fanout (${engine}) demo`, ...prev].slice(0, 60));
    } catch {
      setEvents((prev) => [`${new Date().toLocaleTimeString()} · backend unreachable`, ...prev].slice(0, 60));
    } finally {
      setBusy(false);
    }
  }

  async function onClear() {
    try {
      await api.clear();
      setItems([]);
      setSelectedId(null);
    } catch {
      /* ignore */
    }
  }

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      {/* header */}
      <header className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Fan-Out Interceptor</h1>
          <p className="text-xs text-zinc-500">User prompt + subquery scraper · ChatGPT · Claude · Gemini · Perplexity</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={backendOk ? "success" : "warning"}>
            <span className={cn("mr-1.5 inline-block h-1.5 w-1.5 rounded-full", backendOk ? "bg-emerald-600" : "bg-amber-500")} />
            {backendOk ? "backend" : backendOk === false ? "backend offline" : "…"}
          </Badge>
          <Badge variant={live ? "success" : "secondary"}>
            <Radio className="mr-1 h-3 w-3" />
            {live ? "live" : "polling"}
          </Badge>
          {tap?.running && (
            <Badge variant="success">
              <span className="mr-1.5 inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-600" />
              tapping {tap.engine}
            </Badge>
          )}
        </div>
      </header>

      {/* live interception */}
      <Card className="mb-4">
        <CardHeader>
          <CardTitle className="flex items-center gap-2"><MonitorPlay className="h-4 w-4" /> Live interception</CardTitle>
          <CardDescription>
            Opens the site in a controlled Chrome window with the CDP wire tap attached.
            Log in there once, chat there — prompts + fan-outs stream into this UI.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {ENGINES.map((e) => {
              const active = tap?.running && tap.engine === e.id;
              return (
                <div key={e.id} className={cn("rounded-lg border p-3", active && "border-emerald-500 bg-emerald-50/50")}>
                  <div className="mb-1 flex items-center justify-between">
                    <span className="text-sm font-semibold">{e.id}</span>
                    {active && <Badge variant="success">tapping</Badge>}
                  </div>
                  <p className="mb-2 truncate text-[11px] text-zinc-500" title={e.url}>{e.url} · {e.hint}</p>
                  <Button
                    size="sm"
                    className="w-full"
                    variant={active ? "secondary" : "default"}
                    disabled={tapBusy !== null || (tap?.running && !active)}
                    onClick={() => onOpenAndIntercept(e.id)}
                  >
                    {tapBusy === e.id ? "Opening…" : active ? "Open" : "Open & Intercept"}
                  </Button>
                </div>
              );
            })}
          </div>
          {tapError && <p className="mt-2 text-xs text-red-600">Could not start: {tapError}</p>}
          {tap?.running && (
            <div className="mt-2 flex items-center justify-between rounded-lg bg-zinc-50 px-3 py-2 text-xs text-zinc-600">
              <span>Tapping <b className="font-mono">{tap.engine}</b> since {tap.started_at} · {tap.event_count} wire events. Chat in the opened Chrome window.</span>
              <Button size="sm" variant="outline" onClick={onStopTap} disabled={tapBusy !== null}>
                <Square className="h-3 w-3" /> {tapBusy === "stop" ? "Stopping…" : "Stop"}
              </Button>
            </div>
          )}
          {tap?.last_error && !tap.running && (
            <p className="mt-2 text-xs text-red-600">Last error: {tap.last_error}</p>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-[360px_1fr]">
        {/* demo panel */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><FlaskConical className="h-4 w-4" /> Demo scrape</CardTitle>
            <CardDescription>No login needed — normalized sample output for the picked engine.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <div className="flex flex-wrap gap-1.5">
              {ENGINES.map((e) => (
                <button
                  key={e.id}
                  onClick={() => setEngine(e.id)}
                  className={cn(
                    "rounded-full border px-3 py-1 text-xs font-medium transition-colors",
                    engine === e.id ? "border-zinc-900 bg-zinc-900 text-white" : "border-zinc-200 bg-white hover:bg-zinc-100"
                  )}
                >
                  {e.id}
                </button>
              ))}
            </div>
            <Textarea value={prompt} onChange={(e) => setPrompt(e.target.value)} rows={5} placeholder="Enter user prompt…" />
            <Button onClick={onSimulate} disabled={busy || !prompt.trim()} variant="secondary">
              <Search className="h-4 w-4" /> {busy ? "Scraping…" : `Demo scrape ${engine}`}
            </Button>
          </CardContent>
        </Card>

        {/* result panel */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-4 w-4" />
              {selected ? (
                <><span className="font-mono">{selected.engine}</span><span className="text-zinc-400">·</span>
                <span className="truncate text-xs font-normal text-zinc-500">{selected.interaction_id.slice(0, 8)}</span></>
              ) : "No captures yet"}
            </CardTitle>
            <CardDescription>
              {selected ? new Date(selected.captured_at).toLocaleString() : "Run a live tap or demo scrape to see fan-out."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {!selected ? (
              <div className="rounded-lg border border-dashed p-8 text-center text-sm text-zinc-400">
                Nothing captured. Open & Intercept an engine above, or run a demo scrape.
              </div>
            ) : (
              <div className="flex flex-col gap-4">
                <section>
                  <div className="mb-1.5 flex items-center gap-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
                    <User className="h-3.5 w-3.5" /> User prompt
                    {selected.user_interaction.pii_detected && <Badge variant="warning">PII scrubbed</Badge>}
                  </div>
                  <p className="rounded-lg bg-zinc-50 p-3 text-sm leading-relaxed">{selected.user_interaction.sanitized_prompt_text}</p>
                </section>
                <section>
                  <div className="mb-1.5 flex items-center gap-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
                    <Zap className="h-3.5 w-3.5" /> Subquery fan-out
                    <Badge>{selected.agentic_retrieval.query_fanout_count}</Badge>
                  </div>
                  <ul className="flex flex-col gap-1.5">
                    {selected.agentic_retrieval.intercepted_subqueries.map((q, i) => (
                      <li key={i} className="flex items-start gap-2 rounded-lg border px-3 py-2 text-sm">
                        <span className="mt-0.5 rounded bg-zinc-900 px-1.5 py-0.5 font-mono text-[10px] text-white">{i + 1}</span>
                        <span>{q}</span>
                      </li>
                    ))}
                    {selected.agentic_retrieval.intercepted_subqueries.length === 0 && (
                      <li className="text-sm text-zinc-400">No subqueries intercepted yet — chat in the tapped window.</li>
                    )}
                  </ul>
                  {selected.agentic_retrieval.intermediate_tools_called.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {selected.agentic_retrieval.intermediate_tools_called.map((t, i) => (
                        <Badge key={i} variant="outline">{t.tool_name} · {t.latency_ms}ms</Badge>
                      ))}
                    </div>
                  )}
                </section>
                <section>
                  <div className="mb-1.5 flex items-center gap-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
                    <CitationsIcon /> Citations
                    <Badge variant="secondary">{selected.model_response.attributed_citations.length}</Badge>
                  </div>
                  <ul className="flex flex-col gap-1">
                    {selected.model_response.attributed_citations.map((c) => (
                      <li key={c.citation_index}>
                        <a href={c.url} target="_blank" rel="noreferrer"
                           className="flex items-center gap-1.5 text-sm text-zinc-700 hover:underline">
                          <ExternalLink className="h-3.5 w-3.5 shrink-0 text-zinc-400" />
                          <span className="truncate">{c.domain} — {c.url}</span>
                        </a>
                      </li>
                    ))}
                    {selected.model_response.attributed_citations.length === 0 && (
                      <li className="text-sm text-zinc-400">No citations.</li>
                    )}
                  </ul>
                </section>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* history + live log */}
      <div className="mt-4 grid gap-4 md:grid-cols-[1fr_320px]">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>History</CardTitle>
              <Button variant="ghost" size="sm" onClick={onClear}><Trash2 className="h-3.5 w-3.5" /> Clear</Button>
            </div>
          </CardHeader>
          <CardContent>
            {items.length === 0 ? (
              <p className="text-sm text-zinc-400">Empty.</p>
            ) : (
              <ul className="flex flex-col gap-1.5">
                {items.map((i) => (
                  <li key={i.interaction_id}>
                    <button
                      onClick={() => setSelectedId(i.interaction_id)}
                      className={cn(
                        "flex w-full items-center gap-2 rounded-lg border px-3 py-2 text-left text-sm transition-colors",
                        (selected?.interaction_id === i.interaction_id) ? "border-zinc-900 bg-zinc-50" : "hover:bg-zinc-50"
                      )}
                    >
                      <Badge variant="secondary" className="shrink-0 font-mono text-[10px]">{i.engine}</Badge>
                      <span className="min-w-0 flex-1 truncate">{i.user_interaction.sanitized_prompt_text}</span>
                      <Badge className="shrink-0">{i.agentic_retrieval.query_fanout_count} sub</Badge>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle>Wire events</CardTitle><CardDescription>Backend log tail · <span className="font-mono">logs/fanout.log</span></CardDescription></CardHeader>
          <CardContent>
            {events.length === 0 ? <p className="text-sm text-zinc-400">No events yet.</p> : (
              <ul className="flex max-h-64 flex-col gap-1 overflow-auto font-mono text-[11px] text-zinc-600">
                {events.map((e, i) => <li key={i} className="rounded bg-zinc-50 px-2 py-1">{e}</li>)}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

// lucide-react has no CitationsIcon; alias locally to keep imports tidy
function CitationsIcon() {
  return <ExternalLink className="h-3.5 w-3.5" />;
}
