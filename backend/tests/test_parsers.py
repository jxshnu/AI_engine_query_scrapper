import json

from app.parsers import (
    ClaudeToolAssembler,
    detect_engine,
    extract_chatgpt_prompt,
    extract_claude_prompt,
    extract_gemini_prompt,
    parse_chatgpt_sse_payload,
    parse_gemini_rpc_line,
    parse_perplexity_frame,
    parse_sse_block,
)
from app.sanitize import sanitize_text


def test_detect_engine():
    assert detect_engine("https://chatgpt.com/backend-api/conversation") == "CHATGPT"
    assert detect_engine("https://claude.ai/api/chat_conversations/123/completion") == "CLAUDE"
    assert detect_engine("https://gemini.google.com/_/BardFrontendService/StreamGenerate") == "GEMINI"
    assert detect_engine("wss://x/socket.io/?EIO=4&transport=websocket") == "PERPLEXITY"


def test_chatgpt_prompt():
    post = json.dumps({"messages": [{"content": {"parts": ["hello world"]}}]})
    assert extract_chatgpt_prompt(post) == "hello world"


def test_claude_prompt():
    post = json.dumps({"messages": [{"role": "user", "content": [{"type": "text", "text": "hi claude"}]}]})
    assert extract_claude_prompt(post) == "hi claude"


def test_gemini_prompt_form():
    import urllib.parse
    freq = json.dumps([[["wrb.fr", None, json.dumps([[None, None, None, None, ["my gemini question"]]])]]])
    raw = urllib.parse.urlencode({"f.req": freq})
    out = extract_gemini_prompt(raw)
    assert out and "my gemini question" in out


def test_chatgpt_fanout():
    payload = {"message": {"content": {"content_type": "tool_use", "parts": ['{"query": "crm snowflake"}']},
                           "metadata": {"command": "search it"}}}
    sig = parse_chatgpt_sse_payload(payload)
    assert "search it" in sig.subqueries
    assert any("crm" in q for q in sig.subqueries)


def test_claude_assembler_roundtrip():
    asm = ClaudeToolAssembler()
    asm.handle("content_block_start", {"content_block": {"type": "tool_use", "id": "t1", "name": "web_search"}})
    asm.handle("content_block_delta", {"delta": {"type": "input_json_delta", "partial_json": '{"query": "hipaa'}})
    asm.handle("content_block_delta", {"delta": {"type": "input_json_delta", "partial_json": ' crm"}'}})
    sig = asm.handle("content_block_stop", {})
    assert sig.subqueries and any("hipaa" in q for q in sig.subqueries)


def test_perplexity_frame():
    frame = '42["query_progress", {"status": "searching", "search_queries": ["hipaa crm vendors", "snowflake native connectors"]}]'
    sig = parse_perplexity_frame(frame)
    assert sig.subqueries == ["hipaa crm vendors", "snowflake native connectors"]


def test_gemini_rpc_line():
    inner = json.dumps({"query": "grounding search", "other": "https://example.com/x"})
    outer = json.dumps([["wrb.fr", None, inner]])
    sig = parse_gemini_rpc_line(outer)
    assert "grounding search" in sig.subqueries
    assert "https://example.com/x" in sig.citations


def test_sse_done():
    sig, done = parse_sse_block(None, "[DONE]")
    assert done and sig.done


def test_sanitize_email():
    clean, found = sanitize_text("contact me at a@b.com please")
    assert found and "<EMAIL>" in clean
