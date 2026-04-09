# operators/

Airflow-style operators that wrap ledger actions as composable tasks, plus a
zero-dependency HTTP API (with Swagger UI) and a single-file frontend for
entering transactions.

Each operator owns one domain action, validates its inputs at the boundary,
and returns a JSON-serializable dict. The same operators back both an Airflow
DAG (where `BaseOperator` is real) and the standalone API (where it's stubbed
at runtime) so you can develop locally without installing Airflow.

## Layout

```
operators/
├── api.py                     # stdlib HTTP server, routes, dispatch
├── openapi_spec.py            # OpenAPI 3.0 spec builder
├── frontend/
│   ├── index.html             # single-file transaction UI (vanilla JS)
│   └── swagger.html           # Swagger UI shell (loads /openapi.json)
├── payment_operator.py        # NormalizePaymentOperator
├── debt_operator.py           # AddDebtOperator
├── equity_operator.py         # AddEquityOperator
├── sub_operator.py            # AddSubscriptionOperator
├── test_payment_operator.py
├── test_ledger_operators.py
├── Dockerfile
└── README.md
```

## Operators

### `NormalizePaymentOperator` — one-off payment
Double-entry journal + card enrichment + **temporal dependency report** against
the source card's billing cycle (statement close, grace period, daily periodic
rate, projected interest if unpaid by due).

| Journal | Dr `<expense_account>` | Cr `Liabilities:CreditCard:<card>` (credit) or `Assets:Bank:<card>` (debit) |
|---|---|---|

Output adds `card`, `temporal { next_statement_close, days_until_next_due, carried_balance_daily_interest, projected_interest_on_this_charge_if_unpaid_by_due, ... }`.

### `AddDebtOperator` — payment to make
Registers a debt obligation and computes temporal info against its due date
(daily interest accrual, whether it's overdue, accrued overdue interest).

| Journal | Dr `<offset_account>` | Cr `Liabilities:Debt:<creditor>` |
|---|---|---|

### `AddEquityOperator` — capital to deploy
Registers deployable capital. Supports an optional `target_bucket` that nests
the asset account (e.g. `Assets:Cash:Deployable:sp500`).

| Journal | Dr `Assets:Cash:Deployable[:<bucket>]` | Cr `Equity:Contributed:<source>` |
|---|---|---|

### `AddSubscriptionOperator` — recurring service
Registers a subscription, projects the next N charge dates (`horizon`, default
12), and reports `annualized_cost`. Handles weekly/monthly/yearly, clamps
month rollover for short months, and rolls Feb 29 → Feb 28 in non-leap years.

| Journal | Dr `Expenses:Subscriptions:<service>` | Cr card side (credit or debit) |
|---|---|---|

### Conventions
- Dataclass inputs, typed kwargs, `execute(context) -> dict`.
- **Hard failure at the boundary** — unknown `card_id` / unbalanced journal → `ValueError`.
- **Soft failure in output** — e.g. debit cards get `temporal.applicable=false` instead of an exception.
- All `Decimal` values serialized as strings (XCom / JSON safe).

### Airflow usage
```python
from datetime import date
from decimal import Decimal
from operators import Card, NormalizePaymentOperator, Payment

wallet = {
    "sapphire": Card(
        id="sapphire", name="Chase Sapphire", kind="credit",
        apr=Decimal("0.2199"), balance=Decimal("2450.00"),
        statement_close_day=15, grace_period_days=21,
        last_statement_balance=Decimal("1800.00"),
        last_statement_close=date(2026, 3, 15),
    ),
}

normalize = NormalizePaymentOperator(
    task_id="normalize_groceries",
    payment=Payment(
        payee="Whole Foods", amount=Decimal("184.32"),
        card_id="sapphire", txn_date=date(2026, 4, 9),
    ),
    cards=wallet,
    expense_account="Expenses:Groceries",
)
```

## HTTP API (`api.py`)

Stdlib `http.server` — no third-party deps, not even Airflow (we stub
`BaseOperator` at import time when it isn't present).

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Single-file frontend |
| `/docs` | GET | Swagger UI (loads `/openapi.json`) |
| `/openapi.json` | GET | OpenAPI 3.0 spec |
| `/healthz` | GET | Liveness check (`{"ok": true}`) |
| `/ledger` | GET | All entries grouped by kind |
| `/ledger/<kind>/<id>` | GET | Read one entry |
| `/ledger/payment/<id>` | PUT | Normalize + upsert a payment |
| `/ledger/debt/<id>` | PUT | Register a debt |
| `/ledger/equity/<id>` | PUT | Register equity |
| `/ledger/sub/<id>` | PUT | Register a subscription |
| `/ledger/<kind>/<id>` | DELETE | Remove an entry |

`PUT` is idempotent upsert keyed by client-supplied id — the same id twice
replaces the stored entry. Storage is currently **in-memory** (a dict behind
a lock); a Redis/Postgres backend is a drop-in swap on `LedgerStore`.

### Run locally
```bash
python3 operators/api.py                 # 127.0.0.1:8765
python3 operators/api.py 0.0.0.0 8765    # bind all interfaces
```

### Inject a wallet
Payment and sub operators need to resolve `card_id` against a wallet. Point
at a JSON file via `WALLET_PATH`:
```bash
WALLET_PATH=./wallet.json python3 operators/api.py
```
`wallet.json` is a list of card objects — see `load_wallet()` in `api.py` for
the full shape.

### curl example
```bash
curl -X PUT http://127.0.0.1:8765/ledger/debt/d1 \
  -H 'Content-Type: application/json' \
  -d '{"creditor":"IRS","principal":"1500.00","due_date":"2026-04-15",
       "apr":"0.06","as_of":"2026-04-09","offset_account":"Expenses:Taxes"}'
```

### Frontend
Open `http://127.0.0.1:8765/` — single-file HTML, no build step. Tabs for
payment / debt / equity / sub, a form per tab, live ledger view underneath
that refreshes after every PUT and supports per-entry delete.

> Pragmatic note: this is vanilla JS (under 250 lines) rather than a real
> Svelte build. Every other piece of the stack is zero-dep Python, so adding
> Vite + an npm toolchain for four forms felt like the wrong shape. If you
> actually want Svelte with reactivity primitives / stores, swap `frontend/`
> for a compiled Svelte bundle and keep everything else unchanged — the API
> is pure JSON.

### Swagger
`http://127.0.0.1:8765/docs` — standard Swagger UI loaded from unpkg, points
at `/openapi.json`. The spec is built in Python (`openapi_spec.py`) and
covers every route including request schemas for all four operators.

## Tests

Stdlib `unittest`, no pytest, no Airflow:
```bash
python3 operators/test_payment_operator.py     # 8 tests
python3 operators/test_ledger_operators.py     # 16 tests
```
The ledger test suite spins up `api.py` on a random port in a background
thread and exercises every PUT / GET / DELETE plus `/openapi.json`, `/docs`,
`/`, and `/healthz` with real HTTP.

## Deploy

### Docker
```bash
cd operators
docker build -t ledger-api .
docker run --rm -p 8765:8765 ledger-api
# → http://localhost:8765/
```
The image is `python:3.11-slim` + the `operators/` tree. No requirements
file — stdlib only. A `HEALTHCHECK` hits `/healthz`.

### Where to host
Any platform that runs a container works: Fly.io, Railway, Render, Cloud Run,
ECS, a VPS + `docker run`. The only runtime state is the in-memory ledger
store — if you need persistence across restarts, swap `LedgerStore` for a
Redis/Postgres-backed equivalent before you ship.

### Environment
- `WALLET_PATH` — optional path to a JSON wallet file.
- `LEDGER_API_VERBOSE=1` — enable verbose request logging.

## Adding a new operator
1. Drop `my_operator.py` alongside the others.
2. Subclass `airflow.models.BaseOperator`, accept typed kwargs, return a
   JSON-serializable dict from `execute`.
3. Re-export from `__init__.py`.
4. Validate inputs at the boundary; raise for hard failures, surface soft
   ones in the output.
5. Add a `_run_<kind>` dispatch function and a route in `api.py`.
6. Add a request schema + path entry in `openapi_spec.py`.
7. Add a tab + form block in `frontend/index.html` and a `buildBody()` case.
8. Write tests in `test_ledger_operators.py`.
