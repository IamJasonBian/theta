"""
Ledger API
==========

Tiny stdlib HTTP server exposing the operators as idempotent PUT upserts.
No third-party dependencies (not even Airflow — we stub BaseOperator if it
isn't installed so the whole thing runs in a plain `python:3-slim` image).

Endpoints:
  GET  /                       single-page frontend (HTML)
  GET  /docs                   Swagger UI
  GET  /openapi.json           OpenAPI 3.0 spec
  GET  /research               JSON listing of artifacts in $RESEARCH_DIR
  GET  /research/<path>        static artifact (svg/png/json/…)
  PUT  /ledger/payment/<id>    body: Payment JSON
  PUT  /ledger/debt/<id>       body: Debt JSON
  PUT  /ledger/equity/<id>     body: Equity JSON
  PUT  /ledger/sub/<id>        body: Subscription JSON
  GET  /ledger/<kind>/<id>     read back the normalized entry
  GET  /ledger                 list all entries grouped by kind
  DELETE /ledger/<kind>/<id>   remove an entry

Run directly:
    python3 operators/api.py                 # binds 127.0.0.1:8765
    python3 operators/api.py 0.0.0.0 9000    # custom host/port

The wallet (known cards) is loaded from a JSON file if WALLET_PATH is set,
otherwise an empty wallet is used. Payment/Sub operators need a populated
wallet to resolve card_id references.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import types
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol

# Allow running this file directly without going through the package __init__
# (which would import Airflow eagerly).
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)


def _install_airflow_stub() -> None:
    """Stub airflow.models.BaseOperator / airflow.utils.context.Context if
    Airflow isn't installed. Lets the API run in a plain Python container.
    When Airflow *is* installed (e.g. running inside an Airflow worker), the
    real modules win and nothing here fires."""
    try:
        import airflow.models  # noqa: F401
        import airflow.utils.context  # noqa: F401
        return
    except ImportError:
        pass

    import logging

    class _BaseOperator:
        template_fields = ()

        def __init__(self, *args, **kwargs):
            self.log = logging.getLogger("BaseOperator")

    airflow_mod = types.ModuleType("airflow")
    models_mod = types.ModuleType("airflow.models")
    utils_mod = types.ModuleType("airflow.utils")
    ctx_mod = types.ModuleType("airflow.utils.context")
    models_mod.BaseOperator = _BaseOperator
    ctx_mod.Context = dict
    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.models"] = models_mod
    sys.modules["airflow.utils"] = utils_mod
    sys.modules["airflow.utils.context"] = ctx_mod


_install_airflow_stub()

from payment_operator import Card, NormalizePaymentOperator, Payment  # noqa: E402
from debt_operator import AddDebtOperator, Debt  # noqa: E402
from equity_operator import AddEquityOperator, Equity  # noqa: E402
from sub_operator import AddSubscriptionOperator, Subscription  # noqa: E402
from openapi_spec import build_openapi_spec  # noqa: E402


# ---------- frontend file loading ----------

_FRONTEND_DIR = os.path.join(_HERE, "frontend")


def _load_static(name: str) -> bytes:
    path = os.path.join(_FRONTEND_DIR, name)
    with open(path, "rb") as f:
        return f.read()


# ---------- research artifact serving ----------
#
# Files produced by the weekly research cron land in $RESEARCH_DIR. On Render
# that's the mounted disk (/data/research_out); locally it falls back to
# operators/research_out/. The /research/<path> route serves anything there
# as a static artifact — SVG, PNG, JSON, etc.

_DEFAULT_RESEARCH_DIR = "/data/research_out" \
    if os.path.isdir("/data") else os.path.join(_HERE, "research_out")
RESEARCH_DIR = os.environ.get("RESEARCH_DIR", _DEFAULT_RESEARCH_DIR)

_CONTENT_TYPES = {
    ".svg":  "image/svg+xml",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".json": "application/json",
    ".html": "text/html; charset=utf-8",
    ".txt":  "text/plain; charset=utf-8",
    ".css":  "text/css",
    ".js":   "application/javascript; charset=utf-8",
}


def _research_content_type(path: str) -> str:
    _, ext = os.path.splitext(path.lower())
    return _CONTENT_TYPES.get(ext, "application/octet-stream")


def _safe_research_path(rel: str) -> str | None:
    """Resolve rel against RESEARCH_DIR, refusing traversal escapes."""
    base = os.path.realpath(RESEARCH_DIR)
    target = os.path.realpath(os.path.join(base, rel))
    if target != base and not target.startswith(base + os.sep):
        return None
    return target


def _list_research_dir() -> list[dict[str, Any]]:
    """Flat listing of research_dir — name, size, mtime."""
    if not os.path.isdir(RESEARCH_DIR):
        return []
    out = []
    for name in sorted(os.listdir(RESEARCH_DIR)):
        full = os.path.join(RESEARCH_DIR, name)
        if not os.path.isfile(full):
            continue
        st = os.stat(full)
        out.append({
            "name": name,
            "size": st.st_size,
            "mtime": datetime.utcfromtimestamp(st.st_mtime).isoformat() + "Z",
            "url": f"/research/{name}",
        })
    return out


# ---------- stores ----------

_KINDS = ("payment", "debt", "equity", "sub")


class LedgerStore(Protocol):
    """Minimal interface every store implementation must honor."""

    def put(self, kind: str, entry_id: str, value: dict[str, Any]) -> None: ...
    def get(self, kind: str, entry_id: str) -> dict[str, Any] | None: ...
    def delete(self, kind: str, entry_id: str) -> bool: ...
    def all(self) -> dict[str, dict[str, dict[str, Any]]]: ...


class InMemoryLedgerStore:
    """Thread-safe dict-backed store. Lost on process restart. Used for
    local dev and tests (and as a fallback when LEDGER_DB_PATH is unset)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, dict[str, dict[str, Any]]] = {
            k: {} for k in _KINDS
        }

    def put(self, kind: str, entry_id: str, value: dict[str, Any]) -> None:
        with self._lock:
            self._entries[kind][entry_id] = value

    def get(self, kind: str, entry_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._entries[kind].get(entry_id)

    def delete(self, kind: str, entry_id: str) -> bool:
        with self._lock:
            return self._entries[kind].pop(entry_id, None) is not None

    def all(self) -> dict[str, dict[str, dict[str, Any]]]:
        with self._lock:
            return {k: dict(v) for k, v in self._entries.items()}


class SqliteLedgerStore:
    """SQLite-backed store. Survives restarts when `path` points at a
    persistent volume (on Render, that's the mounted disk at /data).

    Schema is intentionally dumb — one row per (kind, id), value is the
    operator's JSON output stored as a TEXT blob. We're not doing any
    SQL on the fields inside, just key-value lookups; so no migrations
    as the operator output shape evolves.

    Concurrency: we open a fresh connection per call and let SQLite's
    file lock + WAL handle concurrent writers. ThreadingHTTPServer
    throughput is low enough that connection churn is not a concern.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ledger_entries (
                    kind       TEXT NOT NULL,
                    entry_id   TEXT NOT NULL,
                    data       TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (kind, entry_id)
                )
                """
            )
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None → autocommit; every statement commits.
        return sqlite3.connect(self.path, isolation_level=None, timeout=5.0)

    def put(self, kind: str, entry_id: str, value: dict[str, Any]) -> None:
        blob = json.dumps(value, default=str)
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ledger_entries "
                "(kind, entry_id, data, updated_at) VALUES (?, ?, ?, ?)",
                (kind, entry_id, blob, now),
            )

    def get(self, kind: str, entry_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT data FROM ledger_entries WHERE kind=? AND entry_id=?",
                (kind, entry_id),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def delete(self, kind: str, entry_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM ledger_entries WHERE kind=? AND entry_id=?",
                (kind, entry_id),
            )
            return cur.rowcount > 0

    def all(self) -> dict[str, dict[str, dict[str, Any]]]:
        result: dict[str, dict[str, dict[str, Any]]] = {k: {} for k in _KINDS}
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT kind, entry_id, data FROM ledger_entries"
            ).fetchall()
        for kind, entry_id, data in rows:
            result.setdefault(kind, {})[entry_id] = json.loads(data)
        return result


def build_store() -> LedgerStore:
    """Pick the store implementation based on env."""
    path = os.environ.get("LEDGER_DB_PATH")
    if path:
        return SqliteLedgerStore(path)
    return InMemoryLedgerStore()


# ---------- wallet loader ----------

def _default_card() -> Card:
    """Synthetic placeholder card. When a user PUTs a payment or sub
    without specifying a card_id, the API coerces it to "default" and
    resolves against this card. It's a debit card so the payment
    operator short-circuits the temporal/billing-cycle logic — no APR,
    no statement close, no interest math. Use it as a cashflow bucket
    until the user configures a real wallet."""
    return Card(
        id="default",
        name="Default",
        kind="debit",
        apr=Decimal("0"),
        balance=Decimal("0"),
    )


def load_wallet(path: str | None) -> dict[str, Card]:
    wallet: dict[str, Card] = {}
    if path and os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        for raw in data:
            wallet[raw["id"]] = Card(
                id=raw["id"],
                name=raw["name"],
                kind=raw["kind"],
                apr=Decimal(str(raw.get("apr", "0"))),
                balance=Decimal(str(raw.get("balance", "0"))),
                statement_close_day=raw.get("statement_close_day"),
                grace_period_days=raw.get("grace_period_days", 21),
                last_statement_balance=Decimal(str(raw.get("last_statement_balance", "0"))),
                last_statement_close=(
                    date.fromisoformat(raw["last_statement_close"])
                    if raw.get("last_statement_close") else None
                ),
                credit_limit=(
                    Decimal(str(raw["credit_limit"]))
                    if raw.get("credit_limit") is not None else None
                ),
            )
    # Always guarantee a "default" card exists — lets the frontend send
    # payments / subs with a blank card_id field and get a valid journal
    # without the user having to seed a wallet file first.
    wallet.setdefault("default", _default_card())
    return wallet


# ---------- payload -> operator dispatch ----------

def _run_payment(entry_id: str, body: dict, wallet: dict[str, Card]) -> dict:
    p = Payment(
        payee=body["payee"],
        amount=Decimal(str(body["amount"])),
        card_id=body["card_id"],
        txn_date=date.fromisoformat(body["txn_date"]),
        memo=body.get("memo", ""),
    )
    op = NormalizePaymentOperator(
        task_id=f"api_payment_{entry_id}",
        payment=p,
        cards=wallet,
        expense_account=body.get("expense_account", "Expenses:Uncategorized"),
    )
    return op.execute(context={})


def _run_debt(entry_id: str, body: dict, wallet: dict[str, Card]) -> dict:
    d = Debt(
        id=entry_id,
        creditor=body["creditor"],
        principal=Decimal(str(body["principal"])),
        due_date=date.fromisoformat(body["due_date"]),
        apr=Decimal(str(body.get("apr", "0"))),
        memo=body.get("memo", ""),
    )
    op = AddDebtOperator(
        task_id=f"api_debt_{entry_id}",
        debt=d,
        as_of=(date.fromisoformat(body["as_of"]) if body.get("as_of") else None),
        offset_account=body.get("offset_account", "Expenses:Uncategorized"),
    )
    return op.execute(context={})


def _run_equity(entry_id: str, body: dict, wallet: dict[str, Card]) -> dict:
    e = Equity(
        id=entry_id,
        source=body["source"],
        amount=Decimal(str(body["amount"])),
        received_on=date.fromisoformat(body["received_on"]),
        target_bucket=body.get("target_bucket"),
        memo=body.get("memo", ""),
    )
    op = AddEquityOperator(task_id=f"api_equity_{entry_id}", equity=e)
    return op.execute(context={})


def _run_sub(entry_id: str, body: dict, wallet: dict[str, Card]) -> dict:
    s = Subscription(
        id=entry_id,
        service=body["service"],
        amount=Decimal(str(body["amount"])),
        frequency=body["frequency"],
        next_charge_date=date.fromisoformat(body["next_charge_date"]),
        funding_card_id=body["funding_card_id"],
        memo=body.get("memo", ""),
    )
    op = AddSubscriptionOperator(
        task_id=f"api_sub_{entry_id}",
        subscription=s,
        cards=wallet,
        horizon=int(body.get("horizon", 12)),
    )
    return op.execute(context={})


DISPATCH = {
    "payment": _run_payment,
    "debt": _run_debt,
    "equity": _run_equity,
    "sub": _run_sub,
}


# ---------- HTTP handler ----------

def make_handler(store: LedgerStore, wallet: dict[str, Card]):
    class Handler(BaseHTTPRequestHandler):
        server_version = "LedgerAPI/0.1"

        def log_message(self, format, *args):  # quiet default logging
            if os.environ.get("LEDGER_API_VERBOSE"):
                super().log_message(format, *args)

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, default=str).encode()
            self._send_bytes(status, body, "application/json")

        def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _parse_path(self) -> tuple[str, str, str | None]:
            """Return (prefix, kind, id_or_none)."""
            parts = [p for p in self.path.split("/") if p]
            if len(parts) < 1 or parts[0] != "ledger":
                return ("", "", None)
            if len(parts) == 1:
                return ("ledger", "", None)
            kind = parts[1]
            entry_id = parts[2] if len(parts) >= 3 else None
            return ("ledger", kind, entry_id)

        def do_GET(self) -> None:  # noqa: N802
            # static / meta routes
            if self.path in ("/", "/index.html"):
                return self._send_bytes(
                    200, _load_static("index.html"), "text/html; charset=utf-8",
                )
            if self.path == "/docs":
                return self._send_bytes(
                    200, _load_static("swagger.html"), "text/html; charset=utf-8",
                )
            if self.path == "/manifest.json":
                return self._send_bytes(
                    200, _load_static("manifest.json"),
                    "application/manifest+json",
                )
            if self.path == "/icon.svg":
                return self._send_bytes(
                    200, _load_static("icon.svg"), "image/svg+xml",
                )
            if self.path == "/sw.js":
                # Service worker must be served with a JS content-type and
                # from the same scope (root) it controls.
                return self._send_bytes(
                    200, _load_static("sw.js"),
                    "application/javascript; charset=utf-8",
                )
            if self.path == "/openapi.json":
                return self._send_json(200, build_openapi_spec())
            if self.path == "/healthz":
                return self._send_json(200, {"ok": True})

            # research artifacts
            if self.path == "/research" or self.path == "/research/":
                return self._send_json(200, {
                    "research_dir": RESEARCH_DIR,
                    "files": _list_research_dir(),
                })
            if self.path.startswith("/research/"):
                rel = self.path[len("/research/"):].split("?", 1)[0]
                # strip any query string; drop leading slashes
                rel = rel.lstrip("/")
                target = _safe_research_path(rel)
                if target is None:
                    return self._send_json(400, {"error": "bad path"})
                if not os.path.isfile(target):
                    return self._send_json(404, {"error": "not found"})
                try:
                    with open(target, "rb") as f:
                        blob = f.read()
                except OSError as e:
                    return self._send_json(500, {"error": str(e)})
                return self._send_bytes(
                    200, blob, _research_content_type(target),
                )

            prefix, kind, entry_id = self._parse_path()
            if prefix != "ledger":
                return self._send_json(404, {"error": "not found"})
            if not kind:
                return self._send_json(200, store.all())
            if kind not in DISPATCH:
                return self._send_json(404, {"error": f"unknown kind {kind!r}"})
            if entry_id is None:
                return self._send_json(404, {"error": "missing id"})
            entry = store.get(kind, entry_id)
            if entry is None:
                return self._send_json(404, {"error": "not found"})
            self._send_json(200, entry)

        def do_PUT(self) -> None:  # noqa: N802
            prefix, kind, entry_id = self._parse_path()
            if prefix != "ledger" or kind not in DISPATCH or entry_id is None:
                return self._send_json(404, {"error": "bad route"})
            length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError as e:
                return self._send_json(400, {"error": f"bad json: {e}"})
            try:
                result = DISPATCH[kind](entry_id, body, wallet)
            except (KeyError, ValueError) as e:
                return self._send_json(400, {"error": str(e)})
            store.put(kind, entry_id, result)
            self._send_json(200, result)

        def do_DELETE(self) -> None:  # noqa: N802
            prefix, kind, entry_id = self._parse_path()
            if prefix != "ledger" or kind not in DISPATCH or entry_id is None:
                return self._send_json(404, {"error": "bad route"})
            existed = store.delete(kind, entry_id)
            self._send_json(200 if existed else 404,
                            {"deleted": existed, "id": entry_id})

    return Handler


def build_server(host: str = "127.0.0.1", port: int = 8765,
                 wallet: dict[str, Card] | None = None,
                 store: LedgerStore | None = None) -> ThreadingHTTPServer:
    store = store if store is not None else build_store()
    wallet = wallet if wallet is not None else load_wallet(os.environ.get("WALLET_PATH"))
    handler = make_handler(store, wallet)
    server = ThreadingHTTPServer((host, port), handler)
    server.store = store  # type: ignore[attr-defined]
    server.wallet = wallet  # type: ignore[attr-defined]
    return server


def main() -> None:
    # argv wins, then $PORT / $HOST, then sensible local defaults.
    host = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("HOST", "127.0.0.1")
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    else:
        port = int(os.environ.get("PORT", "8765"))
    server = build_server(host, port)
    print(f"ledger api listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
