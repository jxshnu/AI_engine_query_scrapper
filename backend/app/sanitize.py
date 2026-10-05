"""Lightweight in-stream PII sanitation.

Production calls for Microsoft Presidio; this module provides a zero-dependency
regex fallback with the same function signature so the pipeline runs anywhere.
If presidio is installed, it is used automatically.
"""
from __future__ import annotations

import re
from typing import Tuple

_PATTERNS = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "<EMAIL>"),
    (re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"), "<PHONE>"),
    (re.compile(r"\b(?:sk-[A-Za-z0-9-_]{8,}|xox[bpas]-[A-Za-z0-9-]{8,}|ghp_[A-Za-z0-9]{8,})\b"), "<API_KEY>"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "<US_SSN>"),
    (re.compile(r"\b(?:\d[ -]*?){13,19}\b"), "<CARD_NUMBER>"),
]


def _regex_scrub(text: str) -> Tuple[str, bool]:
    detected = False
    out = text
    for rx, token in _PATTERNS:
        if rx.search(out):
            detected = True
            out = rx.sub(token, out)
    return out, detected


def sanitize_text(text: str) -> Tuple[str, bool]:
    """Return (sanitized_text, pii_detected)."""
    if not text:
        return text, False
    # Prefer Presidio when available, else regex fallback.
    try:
        from presidio_analyzer import AnalyzerEngine  # type: ignore
        from presidio_anonymizer import AnonymizerEngine  # type: ignore

        analyzer = AnalyzerEngine()
        anonymizer = AnonymizerEngine()
        results = analyzer.analyze(text=text, language="en")
        if not results:
            return text, False
        anon = anonymizer.anonymize(text=text, analyzer_results=results)
        return anon.text, True
    except Exception:
        return _regex_scrub(text)
