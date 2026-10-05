"""Gemini fixes verified against live 2026 captures (2026-09-30)."""
import json
import urllib.parse

from app.parsers import (
    _gemini_structured_sources,
    _urls_from_text,
    extract_gemini_prompt,
    parse_gemini_rpc_line,
)


def _real_post_body(prompt: str) -> str:
    # byte-shape of the live DEBUG_REQUEST body: f.req is doubly-encoded
    inner_s = json.dumps([[prompt, 0, None, None, None, None, 0],
                          ["en"], None, "!kZKtokengoeshere"])
    return ("f.req=" + urllib.parse.quote(json.dumps([None, inner_s, None, "generic"]))
            + "&at=xyz&hl=en")


def test_doubly_encoded_prompt_extraction():
    prompt = ("What is the best HIPAA compliant CRM for a 15 person "
              "healthtech startup integrating with Snowflake?")
    assert extract_gemini_prompt(_real_post_body(prompt)) == prompt


def test_prompt_extraction_ignores_token_blob():
    # the giant base64 token must never win over the prompt
    prompt = "best show brand in india"
    assert extract_gemini_prompt(_real_post_body(prompt)) == prompt


def test_status_frames_yield_no_phantom_subqueries():
    line = ('[["wrb.fr",null,"[null,[\\"c_08493fadfb8bbbd0\\",\\"r_2456484a02fa704a\\"],'
            '{\\"7\\":[null,[\\"google\\",[null,null,\\"Google Search\\",'
            '\\"https://www.gstatic.com/images/branding/productlogos/googleg/v6/192px.svg\\",'
            'null,\\"Searching the web\\"],1]],\\"44\\":true}]"]]')
    sig = parse_gemini_rpc_line(line)
    assert sig.subqueries == []
    # branding SVG must not leak in as a citation either
    assert all("gstatic.com" not in c for c in sig.citations)


def test_shopping_widget_urls_filtered():
    blob = ("see http://googleusercontent.com/shopping_content/11252233364900173159 "
            "and https://google.com/search?q=Skechers&prds=catalogid3A123 "
            "and https://t2.gstatic.com/faviconV2?url=https://www.walkaroo.in/ "
            "and https://example.com/real-article")
    urls = _urls_from_text(blob)
    assert urls == ["https://example.com/real-article"]


def _real_answer_frame() -> str:
    """Mirror of the live 2026-09-30 answer frame: 60-element inner list,
    candidate at [4][0], sources block at [37][1][0][7]."""
    entries = [
        ["https://t2.gstatic.com/faviconV2?url=https://www.walkaroo.in/",
         "https://www.walkaroo.in/blogs/all/top-10-footwear-brands-in-india",
         "Top 10 Footwear Brands In India - Walkaroo", None, []],
        ["https://t3.gstatic.com/faviconV2?url=https://www.regalshoes.in/",
         "https://www.regalshoes.in/blogs/blog/top-sneaker-brands-in-india",
         "Top 10 Sneaker Brands in India (2026) - Regal Shoes", None, []],
    ]
    searches = [[[], "Searched the web", "Google Search",
                 "https://www.gstatic.com/images/branding/productlogos/googleg/v6/192px.svg",
                 [], "Searched the web", "", entries]]
    cand = (["rc_03442a29040e381f", "Here are the top shoe brands in India...",
             None, None, None, None, None, None, [1], "en"]
            + [None] * 27 + [[None, searches]])
    assert len(cand) == 38
    inner = [None] * 4 + [[cand]] + [None] * 55
    assert len(inner) == 60
    return '[["wrb.fr",null,' + json.dumps(json.dumps(inner)) + ']]'


def test_structured_sources_beat_blind_harvest():
    sig = parse_gemini_rpc_line(_real_answer_frame())
    assert sig.citations == [
        "https://www.walkaroo.in/blogs/all/top-10-footwear-brands-in-india",
        "https://www.regalshoes.in/blogs/blog/top-sneaker-brands-in-india",
    ]
    # no favicon proxy, no logo SVG, no junk
    assert all("gstatic" not in c for c in sig.citations)


def test_structured_sources_index_guard():
    assert _gemini_structured_sources([None, None]) == []
    assert _gemini_structured_sources(None) == []
