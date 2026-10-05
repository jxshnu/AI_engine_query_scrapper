export interface Citation { domain: string; url: string; citation_index: number }
export interface ToolCall { tool_name: string; latency_ms: number }
export interface Interaction {
  interaction_id: string;
  engine: "CHATGPT" | "CLAUDE" | "GEMINI" | "PERPLEXITY" | string;
  captured_at: string;
  session_metadata: { model_id: string; country_iso: string; proxy_asn: string; headless_fingerprint_id: string };
  user_interaction: { raw_prompt_text: string; sanitized_prompt_text: string; pii_detected: boolean };
  agentic_retrieval: { query_fanout_count: number; intercepted_subqueries: string[]; intermediate_tools_called: ToolCall[] };
  model_response: { total_token_count: number; reasoning_trace_present: boolean; attributed_citations: Citation[]; shopping_modules_triggered: boolean };
}

async function req<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, { headers: { "Content-Type": "application/json" }, ...init });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export interface LiveStatus {
  running: boolean;
  engine: string | null;
  url: string | null;
  started_at: string | null;
  event_count: number;
  last_error: string | null;
  note?: string;
}

export const api = {
  health: () => req<{ status: string }>("/api/health"),
  list: (limit = 50) => req<Interaction[]>(`/api/interactions?limit=${limit}`),
  clear: () => req<{ status: string }>("/api/interactions", { method: "DELETE" }),
  simulate: (engine: string, prompt: string) =>
    req<Interaction>("/api/simulate", { method: "POST", body: JSON.stringify({ engine, prompt }) }),
  capturePrompt: (url: string, post_data: string, engine?: string) =>
    req<Interaction>("/api/capture/prompt", { method: "POST", body: JSON.stringify({ url, post_data, engine }) }),
  liveStart: (engine: string) =>
    req<LiveStatus>("/api/live/start", { method: "POST", body: JSON.stringify({ engine }) }),
  liveStop: () => req<LiveStatus>("/api/live/stop", { method: "POST" }),
  liveStatus: () => req<LiveStatus>("/api/live/status"),
  liveEvents: (limit = 50) => req<Array<Record<string, unknown>>>(`/api/live/events?limit=${limit}`),
  liveLog: (tail = 200) => req<{ log: string }>(`/api/live/log?tail=${tail}`),
};
