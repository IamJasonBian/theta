"""
Plaid token store
=================

JSON-file-backed store for the Plaid ``access_token`` + ``item_id`` +
institution metadata. One file per theta instance — theta is a
single-user app, so we don't need a multi-tenant key/value layer.

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

    def get(self) -> dict[str, Any] | None:
        with self._lock:
            if not os.path.exists(self.path):
                return None
            with open(self.path, "r", encoding="utf-8") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    # Treat a corrupt file as "no token" — surface via
                    # logs rather than crash. Caller can re-connect.
                    return None

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
            tmp = f"{self.path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2)
            os.replace(tmp, self.path)
        return record

    def clear(self) -> bool:
        with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                return True
            return False
