"""In-memory interaction store with JSONL persistence."""
from __future__ import annotations

import json
import os
import threading
from typing import Dict, List, Optional

from .models import InteractionEvent

_PERSIST = os.path.join(os.path.dirname(__file__), "..", "interactions.jsonl")


class InteractionStore:
    def __init__(self, persist_path: str = _PERSIST) -> None:
        self._items: Dict[str, InteractionEvent] = {}
        self._order: List[str] = []
        self._lock = threading.Lock()
        self._persist_path = os.path.abspath(persist_path)
        self._load()

    def _load(self) -> None:
        try:
            if os.path.exists(self._persist_path):
                with open(self._persist_path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            ev = InteractionEvent.model_validate(json.loads(line))
                            self._items[ev.interaction_id] = ev
                            if ev.interaction_id not in self._order:
                                self._order.append(ev.interaction_id)
                        except Exception:
                            continue
        except Exception:
            pass

    def _append_disk(self, ev: InteractionEvent) -> None:
        try:
            os.makedirs(os.path.dirname(self._persist_path), exist_ok=True)
            with open(self._persist_path, "a", encoding="utf-8") as fh:
                fh.write(ev.model_dump_json() + "\n")
        except Exception:
            pass

    def upsert(self, ev: InteractionEvent) -> InteractionEvent:
        with self._lock:
            is_new = ev.interaction_id not in self._items
            self._items[ev.interaction_id] = ev
            if is_new:
                self._order.append(ev.interaction_id)
                self._append_disk(ev)
            else:
                self._rewrite_disk()
            return ev

    def _rewrite_disk(self) -> None:
        try:
            with open(self._persist_path, "w", encoding="utf-8") as fh:
                for iid in self._order:
                    fh.write(self._items[iid].model_dump_json() + "\n")
        except Exception:
            pass

    def get(self, interaction_id: str) -> Optional[InteractionEvent]:
        return self._items.get(interaction_id)

    def list(self, limit: int = 50) -> List[InteractionEvent]:
        ids = self._order[-limit:][::-1]
        return [self._items[i] for i in ids]

    def latest_for_engine(self, engine: str) -> Optional[InteractionEvent]:
        for iid in reversed(self._order):
            if self._items[iid].engine == engine:
                return self._items[iid]
        return None

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self._order.clear()
            try:
                if os.path.exists(self._persist_path):
                    os.remove(self._persist_path)
            except Exception:
                pass


store = InteractionStore()
