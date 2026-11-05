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
  PUT  /ledger/payment/<id>    body: Payment JSON
  PUT  /ledger/debt/<id>       body: Debt JSON
  PUT  /ledger/equity/<id>     body: Equity JSON
  PUT  /ledger/sub/<id>        body: Subscription JSON
  GET  /ledger/<kind>/<id>     read back the normalized entry
  GET  /ledger                 list all entries grouped by kind
  DELETE /ledger/<kind>/<id>   remove an entry

  GET  /plaid                  Plaid connect frontend (HTML)
  GET  /plaid/link-token       short-lived token for the Plaid Link widget
  POST /plaid/exchange         body: {"public_token": "..."}
  GET  /plaid/status           connected? institution name
  POST /plaid/disconnect       revoke item + clear local token
  GET  /plaid/liabilities      normalized upcoming-payment view (read-only)
  POST /plaid/sync             upsert liabilities as ledger debt entries

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
from plaid_operator import (  # noqa: E402
    Autopay,
    CardAccount,
    SyncCardScheduleOperator,
)
from plaid_client import (  # noqa: E402
    PlaidAPIError,
    PlaidClient,
    PlaidConnectionError,
    PlaidError,
    PlaidTimeoutError,
)
from plaid_store import PlaidTokenStore  # noqa: E402
from openapi_spec import build_openapi_spec  # noqa: E402


# ---------- frontend file loading ----------

_FRONTEND_DIR = os.path.join(_HERE, "frontend")


def _load_static(name: str) -> bytes:
    path = os.path.join(_FRONTEND_DIR, name)
    with open(path, "rb") as f:
        return f.read()


# ---------- stores ----------

_KINDS = ("payment", "debt", "equity", "sub", "card")


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
        rail=body.get("rail") or None,
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


def _run_card(entry_id: str, body: dict, wallet: dict[str, Card]) -> dict:
    ap_raw = body.get("autopay") or {}
    autopay = Autopay(
        enabled=bool(ap_raw.get("enabled", False)),
        basis=ap_raw.get("basis", "statement_balance"),
        amount=(Decimal(str(ap_raw["amount"]))
                if ap_raw.get("amount") is not None else None),
        lead_days=int(ap_raw.get("lead_days", 0)),
    )
    card = CardAccount(
        id=entry_id,
        name=body["name"],
        next_due_date=date.fromisoformat(body["next_due_date"]),
        issuer=body.get("issuer", ""),
        apr=Decimal(str(body.get("apr", "0"))),
        statement_balance=Decimal(str(body.get("statement_balance", "0"))),
        minimum_payment=Decimal(str(body.get("minimum_payment", "0"))),
        grace_period_days=int(body.get("grace_period_days", 21)),
        last_payment_date=(date.fromisoformat(body["last_payment_date"])
                           if body.get("last_payment_date") else None),
        last_payment_amount=Decimal(str(body.get("last_payment_amount", "0"))),
        is_overdue=bool(body.get("is_overdue", False)),
        autopay=autopay,
    )
    op = SyncCardScheduleOperator(
        task_id=f"api_card_{entry_id}",
        card=card,
        as_of=(date.fromisoformat(body["as_of"]) if body.get("as_of") else None),
        lookback=int(body.get("lookback", 6)),
        lookahead=int(body.get("lookahead", 6)),
    )
    return op.execute(context={})


DISPATCH = {
    "payment": _run_payment,
    "debt": _run_debt,
    "equity": _run_equity,
    "sub": _run_sub,
    "card": _run_card,
}


# ---------- Plaid helpers ----------

def _purchase_apr(aprs: list[dict[str, Any]]) -> Decimal:
    """Pick the purchase APR from a Plaid credit liability, falling back
    to the first APR present. Plaid returns percentages (19.99 = 19.99%)
    while theta's Debt model expects a decimal fraction (0.1999), so we
    divide by 100 here."""
    if not aprs:
        return Decimal("0")
    pick = next(
        (a for a in aprs if (a.get("apr_type") or "").lower() == "purchase_apr"),
        aprs[0],
    )
    pct = pick.get("apr_percentage") or 0
    return (Decimal(str(pct)) / Decimal("100")).quantize(Decimal("0.0001"))


def _normalize_liabilities(
    liabilities_response: dict[str, Any],
    *,
    today: date,
) -> list[dict[str, Any]]:
    """Project Plaid's /liabilities/get into theta-shaped Debt records.

    Returns a list of dicts ready to feed AddDebtOperator. ID format is
    ``plaid-<kind>-<account_id>`` so re-syncing produces UPDATEs against
    the same ledger slot rather than duplicates.
    """
    accounts = {a["account_id"]: a for a in liabilities_response.get("accounts", [])}
    liabilities = liabilities_response.get("liabilities") or {}
    out: list[dict[str, Any]] = []

    for kind, rows in (
        ("credit", liabilities.get("credit") or []),
        ("mortgage", liabilities.get("mortgage") or []),
        ("student", liabilities.get("student") or []),
    ):
        for row in rows:
            acct = accounts.get(row.get("account_id"), {})
            balances = acct.get("balances") or {}
            principal = balances.get("current")
            if principal is None:
                # No balance → nothing to bill for; skip rather than
                # write a zero-principal debt that misleads the chart.
                continue

            # Pick the most informative due-date field per liability kind.
            if kind == "credit":
                due = row.get("next_payment_due_date") or row.get("last_statement_issue_date")
                apr = _purchase_apr(row.get("aprs") or [])
                min_pmt = row.get("minimum_payment_amount")
            elif kind == "mortgage":
                due = row.get("next_payment_due_date")
                apr = Decimal(str(row.get("interest_rate", {}).get("percentage") or 0)) / Decimal("100")
                min_pmt = row.get("next_monthly_payment")
            else:  # student
                due = row.get("next_payment_due_date")
                apr = Decimal(str(row.get("interest_rate_percentage") or 0)) / Decimal("100")
                min_pmt = row.get("minimum_payment_amount")

            if not due:
                # Without a due date the row can't slot into the upcoming-
                # payments view. Skip rather than fabricate.
                continue

            creditor = (
                acct.get("official_name")
                or acct.get("name")
                or f"plaid-{kind}"
            )

            out.append({
                "id": f"plaid-{kind}-{row['account_id']}",
                "creditor": creditor,
                "principal": str(principal),
                "due_date": due,
                "apr": str(apr),
                "as_of": today.isoformat(),
                "memo": (
                    f"plaid-synced kind={kind}"
                    + (f" min={min_pmt}" if min_pmt is not None else "")
                ),
            })
    return out


# ---------- HTTP handler ----------

def make_handler(
    store: LedgerStore,
    wallet: dict[str, Card],
    plaid_client: PlaidClient | None = None,
    plaid_store: PlaidTokenStore | None = None,
    plaid_user_id: str = "theta-user",
):
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
            if self.path == "/topline":
                return self._send_bytes(
                    200, _load_static("topline.html"), "text/html; charset=utf-8",
                )
            if self.path == "/plaid":
                return self._send_bytes(
                    200, _load_static("plaid.html"), "text/html; charset=utf-8",
                )
            if self.path.startswith("/plaid/"):
                return self._handle_plaid_get()
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

        # ---------- Plaid routes ----------
        #
        # Best-practice posture: the secret never reaches the browser
        # (it lives in os.environ on the server only); the access_token
        # also stays server-side (persisted via plaid_store). The
        # frontend handles only link_token (short-lived, scoped to one
        # Link session) and public_token (single-use, exchanged here).

        def _read_json_body(self) -> dict[str, Any] | None:
            length = int(self.headers.get("Content-Length") or 0)
            try:
                return json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError as e:
                self._send_json(400, {"error": f"bad json: {e}"})
                return None

        def _plaid_unavailable(self) -> bool:
            if plaid_client is None or plaid_store is None:
                self._send_json(503, {"error": (
                    "Plaid not configured — set PLAID_CLIENT_ID and "
                    "PLAID_SECRET in the environment and restart."
                )})
                return True
            return False

        def _plaid_error(self, e: Exception) -> None:
            if isinstance(e, PlaidAPIError):
                self._send_json(e.status or 502, {
                    "error": str(e),
                    "error_type": e.error_type,
                    "error_code": e.error_code,
                    "request_id": e.request_id,
                })
            elif isinstance(e, PlaidConnectionError):
                self._send_json(502, {"error": str(e)})
            elif isinstance(e, PlaidTimeoutError):
                self._send_json(504, {"error": str(e)})
            else:
                self._send_json(500, {"error": str(e)})

        def _handle_plaid_get(self) -> None:
            if self._plaid_unavailable():
                return
            try:
                if self.path == "/plaid/link-token":
                    resp = plaid_client.link_token_create(
                        user_id=plaid_user_id,
                        products=("liabilities",),
                    )
                    return self._send_json(200, {
                        "link_token": resp.get("link_token"),
                        "expiration": resp.get("expiration"),
                    })

                if self.path == "/plaid/status":
                    tok = plaid_store.get()
                    if not tok:
                        return self._send_json(200, {
                            "connected": False, "message": "Not connected",
                        })
                    return self._send_json(200, {
                        "connected": True,
                        "item_id": tok.get("item_id"),
                        "institution_id": tok.get("institution_id"),
                        "institution_name": tok.get("institution_name"),
                        "connected_at": tok.get("created_at"),
                    })

                if self.path == "/plaid/liabilities":
                    tok = plaid_store.get()
                    if not tok:
                        return self._send_json(401, {
                            "error": "not connected — POST /plaid/exchange first",
                        })
                    resp = plaid_client.liabilities_get(tok["access_token"])
                    normalized = _normalize_liabilities(resp, today=date.today())
                    return self._send_json(200, {
                        "upcoming": normalized,
                        "count": len(normalized),
                    })

                self._send_json(404, {"error": "unknown plaid route"})
            except PlaidError as e:
                self._plaid_error(e)

        def do_POST(self) -> None:  # noqa: N802
            if not self.path.startswith("/plaid/"):
                return self._send_json(404, {"error": "bad route"})
            if self._plaid_unavailable():
                return
            try:
                if self.path == "/plaid/exchange":
                    body = self._read_json_body()
                    if body is None:
                        return
                    public_token = body.get("public_token")
                    if not public_token:
                        return self._send_json(
                            400, {"error": "public_token is required"})
                    exch = plaid_client.item_public_token_exchange(public_token)
                    access_token = exch["access_token"]
                    item_id = exch["item_id"]
                    inst_id = inst_name = None
                    try:
                        item = plaid_client.item_get(access_token)
                        inst_id = item.get("item", {}).get("institution_id")
                        if inst_id:
                            inst = plaid_client.institutions_get_by_id(inst_id)
                            inst_name = inst.get("institution", {}).get("name")
                    except PlaidError:
                        # Institution name is decorative — never let a
                        # lookup failure break the actual exchange.
                        pass
                    plaid_store.save(
                        access_token=access_token,
                        item_id=item_id,
                        institution_id=inst_id,
                        institution_name=inst_name,
                    )
                    return self._send_json(200, {
                        "connected": True,
                        "item_id": item_id,
                        "institution_id": inst_id,
                        "institution_name": inst_name,
                    })

                if self.path == "/plaid/disconnect":
                    tok = plaid_store.get()
                    if tok:
                        try:
                            plaid_client.item_remove(tok["access_token"])
                        except PlaidError:
                            # Plaid-side remove is best-effort — if it
                            # fails we still clear our local token so
                            # the user can reconnect cleanly.
                            pass
                    plaid_store.clear()
                    return self._send_json(200, {
                        "connected": False, "message": "Disconnected",
                    })

                if self.path == "/plaid/sync":
                    tok = plaid_store.get()
                    if not tok:
                        return self._send_json(
                            401, {"error": "not connected"})
                    resp = plaid_client.liabilities_get(tok["access_token"])
                    rows = _normalize_liabilities(resp, today=date.today())
                    synced: list[str] = []
                    for row in rows:
                        result = _run_debt(row["id"], row, wallet)
                        store.put("debt", row["id"], result)
                        synced.append(row["id"])
                    return self._send_json(200, {
                        "synced": synced, "count": len(synced),
                    })

                self._send_json(404, {"error": "unknown plaid route"})
            except PlaidError as e:
                self._plaid_error(e)

    return Handler


def build_plaid() -> tuple[PlaidClient | None, PlaidTokenStore | None, str]:
    """Build the Plaid client + token store from env vars.

    Returns (None, None, _) when PLAID_CLIENT_ID/PLAID_SECRET aren't set,
    which makes /plaid/* routes 503 with a helpful message instead of
    crashing startup. theta should run fine without Plaid configured."""
    client_id = os.environ.get("PLAID_CLIENT_ID")
    secret = os.environ.get("PLAID_SECRET")
    user_id = os.environ.get("PLAID_USER_ID", "theta-user")
    if not client_id or not secret:
        return None, None, user_id

    env = os.environ.get("PLAID_ENV", "sandbox")
    timeout = float(os.environ.get("PLAID_TIMEOUT", "10"))
    client = PlaidClient(client_id, secret, env=env, timeout=timeout)
    token_path = os.environ.get("PLAID_TOKEN_PATH", "plaid_token.json")
    return client, PlaidTokenStore(token_path), user_id


def build_server(host: str = "127.0.0.1", port: int = 8765,
                 wallet: dict[str, Card] | None = None,
                 store: LedgerStore | None = None) -> ThreadingHTTPServer:
    store = store if store is not None else build_store()
    wallet = wallet if wallet is not None else load_wallet(os.environ.get("WALLET_PATH"))
    plaid_client, plaid_store, plaid_user_id = build_plaid()
    handler = make_handler(store, wallet, plaid_client, plaid_store, plaid_user_id)
    server = ThreadingHTTPServer((host, port), handler)
    server.store = store  # type: ignore[attr-defined]
    server.wallet = wallet  # type: ignore[attr-defined]
    server.plaid_client = plaid_client  # type: ignore[attr-defined]
    server.plaid_store = plaid_store  # type: ignore[attr-defined]
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
