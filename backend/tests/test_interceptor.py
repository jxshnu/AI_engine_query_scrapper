import json

from app.interceptor import ConversationalStreamInterceptor
from app.models import InteractionEvent


def _tap():
    seen = []
    ic = ConversationalStreamInterceptor(on_event=seen.append)
    return ic, seen


def test_request_engine_mapping_drives_stream_label():
    ic, _ = _tap()
    ic._on_request_will_be_sent({
        "requestId": "req-1",
        "request": {"url": "https://chatgpt.com/backend-api/conversation",
                    "method": "POST",
                    "postData": json.dumps({"messages": [{"content": {"parts": ["hi"]}}]})},
    })
    assert ic.request_engines["req-1"] == "CHATGPT"
    # Even if the stream URL is unrecognizable and chunks carry event: lines,
    # the learned mapping wins over the old event-presence heuristic.
    assert ic._engine_for("req-1", "https://unknown-host/stream", "content_block_start") == "CHATGPT"
    assert ic._engine_for("req-1", "https://unknown-host/stream", None) == "CHATGPT"


def test_debug_request_emitted_for_conversation_posts():
    ic, seen = _tap()
    ic._on_request_will_be_sent({
        "requestId": "req-2",
        "request": {"url": "https://chatgpt.com/backend-api/conversation",
                    "method": "POST",
                    "postData": json.dumps({"messages": [{"content": {"parts": ["hi"]}}]})},
    })
    kinds = [e["type"] for e in seen]
    assert "debug_request" in kinds
    assert "user_prompt" in kinds
    dbg = next(e for e in seen if e["type"] == "debug_request")
    assert dbg["has_post_data"] is True


def test_no_post_data_means_no_prompt_but_mapping_kept():
    ic, seen = _tap()
    ic._on_request_will_be_sent({
        "requestId": "req-3",
        "request": {"url": "https://chatgpt.com/backend-api/conversation", "method": "POST"},
    })
    assert ic.request_engines["req-3"] == "CHATGPT"
    assert [e for e in seen if e["type"] == "user_prompt"] == []


def test_citation_tracking_params_deduped_and_favicons_dropped():
    ev = InteractionEvent(engine="CHATGPT")
    ev.add_citations([
        "https://www.salesforce.com/healthcare/best-hipaa-compliant-crm/?bc=OTH",
        "https://www.salesforce.com/healthcare/best-hipaa-compliant-crm/?bc=OTH&utm_source=chatgpt.com",
        "https://www.salesforce.com/healthcare/best-hipaa-compliant-crm/?utm_source=chatgpt.com",
        "https://www.google.com/s2/favicons?domain=salesforce.com&sz=128",
    ])
    assert len(ev.model_response.attributed_citations) == 1
    assert ev.model_response.attributed_citations[0].domain == "www.salesforce.com"
