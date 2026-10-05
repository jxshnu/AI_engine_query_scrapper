import json

from app.parsers import (
    ClaudeToolAssembler,
    _harvest_queries,
    _looks_like_query,
    _urls_from_text,
    detect_engine,
    extract_chatgpt_prompt,
    parse_chatgpt_sse_payload,
    parse_gemini_rpc_line,
    parse_perplexity_frame,
    parse_sse_block,
)


def test_detect_f_conversation_endpoint():
    assert detect_engine("https://chatgpt.com/backend-api/f/conversation") == "CHATGPT"
    assert detect_engine("https://chatgpt.com/backend-api/conversation") == "CHATGPT"


def test_real_prompt_body_shape():
    # byte-shape from live DEBUG_REQUEST 2026-09-17
    body = json.dumps({
        "action": "next",
        "messages": [{
            "id": "ef91fa98",
            "author": {"role": "user"},
            "content": {"content_type": "text",
                        "parts": ["What is the best HIPAA compliant CRM?"]},
        }],
    })
    assert extract_chatgpt_prompt(body) == "What is the best HIPAA compliant CRM?"


def test_patch_envelope_search_turn():
    payload = {"p": "", "o": "add", "c": 3, "v": {"message": {
        "author": {"role": "tool", "name": "browser.search"},
        "content": {"content_type": "code",
                    "parts": [json.dumps({"query": "hipaa crm pricing comparison"})]},
        "metadata": {"recipient": "browser.search"},
    }, "conversation_id": "abc"}}
    sig = parse_chatgpt_sse_payload(payload)
    assert "hipaa crm pricing comparison" in sig.subqueries
    assert sig.tool_name == "browser.search"


def test_tool_turn_query_sweep_without_known_keys():
    payload = {"message": {
        "author": {"role": "tool"},
        "content": {"content_type": "text",
                    "parts": ["searching now"]},
        "metadata": {"recipient": "web.search",
                     "extra": {"nested": {"search_query": "snowflake crm connector"}}},
    }}
    sig = parse_chatgpt_sse_payload(payload)
    assert "snowflake crm connector" in sig.subqueries


def test_claude_server_tool_use_roundtrip():
    asm = ClaudeToolAssembler()
    asm.handle("content_block_start", {"content_block": {
        "type": "server_tool_use", "id": "srvtoolu_1", "name": "web_search"}})
    asm.handle("content_block_delta", {"delta": {
        "type": "input_json_delta",
        "partial_json": '{"query": "latest quantum breakthroughs"}'}})
    sig = asm.handle("content_block_stop", {})
    assert "latest quantum breakthroughs" in sig.subqueries


def test_claude_search_result_block_urls():
    asm = ClaudeToolAssembler()
    sig = asm.handle("content_block_start", {"content_block": {
        "type": "web_search_tool_result", "tool_use_id": "srvtoolu_1",
        "content": [{"type": "web_search_result", "title": "t",
                     "url": "https://example.com/q"}]}})
    assert "https://example.com/q" in sig.citations


def test_gemini_two_bracket_envelope_and_xssi_skip():
    assert parse_gemini_rpc_line(")]}'").subqueries == []
    inner = json.dumps({"answer": "x", "ground": {"query": "gemini grounding probe"}})
    outer = json.dumps([["wrb.fr", None, inner]])  # two-bracket variant
    sig = parse_gemini_rpc_line(outer)
    assert "gemini grounding probe" in sig.subqueries


def test_perplexity_ask_carries_prompt_and_nested_queries():
    ask = parse_perplexity_frame('42["perplexity_ask", "is crm hipaa compliant?", {}]')
    assert ask.prompt == "is crm hipaa compliant?"
    prog = parse_perplexity_frame(
        '42["some_renamed_event", {"data": {"search_queries": ["renamed q one"]}}]')
    assert "renamed q one" in prog.subqueries


def test_query_shape_guards():
    assert not _looks_like_query("https://example.com/x")
    assert not _looks_like_query("abc123")
    assert not _looks_like_query('{"a": 1}')
    assert _looks_like_query("best hipaa compliant crm 2026")
    out: list = []
    _harvest_queries({"query": "https://example.com"}, out)
    assert out == []


def test_init_shell_body_yields_no_prompt():
    # byte-shape of POST /backend-api/conversation/init — metadata only
    body = json.dumps({"conversation_id": None, "conversation_origin": None,
                       "gizmo_id": None, "requested_default_model": None,
                       "system_hints": None, "timezone": "Asia/Calcutta",
                       "timezone_offset_min": -330})
    assert extract_chatgpt_prompt(body) is None


def test_last_user_message_wins():
    body = json.dumps({"action": "next", "messages": [
        {"author": {"role": "user"},
         "content": {"content_type": "text", "parts": ["first question"]}},
        {"author": {"role": "assistant"},
         "content": {"content_type": "text", "parts": ["answer"]}},
        {"author": {"role": "user"},
         "content": {"content_type": "text", "parts": ["follow-up question"]}},
    ]})
    assert extract_chatgpt_prompt(body) == "follow-up question"


def test_hidden_system_command_is_not_a_query():
    # live 2026-10-05: system scaffolding with metadata.command="prompt"
    payload = {"message": {
        "author": {"role": "system", "name": None},
        "content": {"content_type": "text", "parts": [""]},
        "metadata": {"command": "prompt",
                     "is_visually_hidden_from_conversation": True},
    }}
    sig = parse_chatgpt_sse_payload(payload)
    assert sig.subqueries == []


def test_bare_marker_strings_never_become_queries():
    payload = {"message": {
        "author": {"role": "tool"},
        "content": {"content_type": "code", "parts": ['"prompt"']},
        "metadata": {"recipient": "browser.search",
                     "command": {"action": "prompt", "q": "real hipaa crm search"}},
    }}
    sig = parse_chatgpt_sse_payload(payload)
    assert sig.subqueries == ["real hipaa crm search"]
    assert "prompt" not in sig.subqueries


def test_glued_ad_urls_split_and_paid_dropped():
    blob = ("https://melento.ai/en-in/clm/?utm_source=chat+gpt&utm_medium=Paid"
            "&utm_campaign=X&https://melento.ai/other/?utm_source=chat+gpt "
            "https://example.com/real-source")
    urls = _urls_from_text(blob)
    assert urls == ["https://example.com/real-source"]
