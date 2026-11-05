"""
Plaid token store
=================

JSON-file-backed store for the Plaid ``access_token`` + ``item_id`` +
institution metadata. One file per theta instance holding every linked item (one per bank) —
theta is a single-user app, so we don't need a multi-tenant key/value layer.

Why not the SQLite ledger store?
--------------------------------
An ``access_token`` is a credential, not a ledger entry. It must not be
listable via ``GET /ledger`` or appear in any normalized operator
output. Keeping it in a separate file (mirroring the ``WALLET_PATH``
pattern theta already uses for the card wallet) makes that boundary
obvious and impossible to violate by accident.

The file path defaults to ``./plaid_token.json`` in the cwd; in
production set ``PLAID_TOKEN_PATH`` to a path on the Render persistent
disk (``/data/plaid_token.json``) so the token survives restarts.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any


class PlaidTokenStore:
    """Tiny thread-safe JSON store. Read/write per call; no caching."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    # File shape: {"items": {item_id: record}}. A legacy single-record
    # file (top-level "access_token") is read as a one-item store.

    def _read(self) -> dict[str, dict[str, Any]]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                # Treat a corrupt file as "no tokens" — caller can re-connect.
                return {}
        if "access_token" in data:
            return {data["item_id"]: data}
        return data.get("items", {})

    def _write(self, items: dict[str, dict[str, Any]]) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"items": items}, f, indent=2)
        os.replace(tmp, self.path)

    def items(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._read().values())

    def get(self, item_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            items = self._read()
        if item_id is not None:
            return items.get(item_id)
        return next(iter(items.values()), None)

    def save(
        self,
        *,
        access_token: str,
        item_id: str,
        institution_id: str | None = None,
        institution_name: str | None = None,
    ) -> dict[str, Any]:
        record = {
            "access_token": access_token,
            "item_id": item_id,
            "institution_id": institution_id,
            "institution_name": institution_name,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            items = self._read()
            # Re-linking the same bank yields a new item; drop the old one.
            if institution_id:
                items = {k: v for k, v in items.items()
                         if v.get("institution_id") != institution_id}
            items[item_id] = record
            self._write(items)
        return record

    def clear(self, item_id: str | None = None) -> bool:
        """Remove one item, or every item when ``item_id`` is None."""
        with self._lock:
            items = self._read()
            if item_id is None:
                if not os.path.exists(self.path):
                    return False
                os.remove(self.path)
                return bool(items)
            if items.pop(item_id, None) is None:
                return False
            self._write(items)
            return True
