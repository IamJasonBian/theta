"""
OpenAPI 3.0 spec for the Ledger API.

Kept as a plain dict so it stays zero-dep — no pydantic, no fastapi.
Served from `api.py` at GET /openapi.json and rendered by Swagger UI at /docs.
"""

from __future__ import annotations

from typing import Any


def _decimal_str(desc: str) -> dict[str, Any]:
    return {"type": "string", "description": desc,
            "example": "184.32", "pattern": r"^-?\d+(\.\d+)?$"}


def _iso_date(desc: str) -> dict[str, Any]:
    return {"type": "string", "format": "date", "description": desc,
            "example": "2026-04-09"}


SCHEMAS: dict[str, Any] = {
    "JournalLine": {
        "type": "object",
        "required": ["account", "debit", "credit"],
        "properties": {
            "account": {"type": "string",
                        "example": "Liabilities:CreditCard:Chase Sapphire"},
            "debit":  _decimal_str("debit amount"),
            "credit": _decimal_str("credit amount"),
        },
    },
    "PaymentRequest": {
        "type": "object",
        "required": ["payee", "amount", "card_id", "txn_date"],
        "properties": {
            "payee": {"type": "string", "example": "Whole Foods"},
            "amount": _decimal_str("payment amount"),
            "card_id": {"type": "string", "example": "sapphire"},
            "txn_date": _iso_date("transaction date"),
            "memo": {"type": "string", "default": ""},
            "expense_account": {"type": "string",
                                "default": "Expenses:Uncategorized"},
        },
    },
    "DebtRequest": {
        "type": "object",
        "required": ["creditor", "principal", "due_date"],
        "properties": {
            "creditor": {"type": "string", "example": "IRS"},
            "principal": _decimal_str("amount owed"),
            "due_date": _iso_date("date payment is due"),
            "apr": _decimal_str("annual interest rate (default 0)"),
            "as_of": _iso_date("valuation date (optional)"),
            "offset_account": {"type": "string",
                               "default": "Expenses:Uncategorized"},
            "memo": {"type": "string", "default": ""},
        },
    },
    "EquityRequest": {
        "type": "object",
        "required": ["source", "amount", "received_on"],
        "properties": {
            "source": {"type": "string", "example": "bonus"},
            "amount": _decimal_str("amount of capital"),
            "received_on": _iso_date("date capital became available"),
            "target_bucket": {"type": "string", "nullable": True,
                              "example": "sp500"},
            "memo": {"type": "string", "default": ""},
        },
    },
    "SubscriptionRequest": {
        "type": "object",
        "required": ["service", "amount", "frequency",
                     "next_charge_date", "funding_card_id"],
        "properties": {
            "service": {"type": "string", "example": "netflix"},
            "amount": _decimal_str("per-charge amount"),
            "frequency": {"type": "string",
                          "enum": ["weekly", "monthly", "yearly"]},
            "next_charge_date": _iso_date("date of the next charge"),
            "funding_card_id": {"type": "string", "example": "sapphire"},
            "horizon": {"type": "integer", "default": 12,
                        "description": "how many future charges to project"},
            "memo": {"type": "string", "default": ""},
        },
    },
    "LedgerEntry": {
        "type": "object",
        "description": "Normalized operator output — shape depends on kind. "
                       "Always contains `journal` and the operator-specific "
                       "top-level record (payment/debt/equity/subscription).",
        "additionalProperties": True,
        "properties": {
            "journal": {"type": "array",
                        "items": {"$ref": "#/components/schemas/JournalLine"}},
        },
    },
    "Error": {
        "type": "object",
        "properties": {"error": {"type": "string"}},
    },
    "DeleteResult": {
        "type": "object",
        "properties": {
            "deleted": {"type": "boolean"},
            "id": {"type": "string"},
        },
    },
}


def _put_path(kind: str, req_schema: str, tag: str, summary: str) -> dict[str, Any]:
    id_param = {
        "name": "id", "in": "path", "required": True,
        "schema": {"type": "string"},
        "description": f"client-supplied id for this {kind} entry",
    }
    return {
        "put": {
            "tags": [tag],
            "summary": summary,
            "parameters": [id_param],
            "requestBody": {
                "required": True,
                "content": {"application/json": {
                    "schema": {"$ref": f"#/components/schemas/{req_schema}"}}},
            },
            "responses": {
                "200": {"description": "upserted",
                        "content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/LedgerEntry"}}}},
                "400": {"description": "bad request",
                        "content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/Error"}}}},
            },
        },
        "get": {
            "tags": [tag], "summary": f"read a {kind} entry",
            "parameters": [id_param],
            "responses": {
                "200": {"description": "ok",
                        "content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/LedgerEntry"}}}},
                "404": {"description": "not found"},
            },
        },
        "delete": {
            "tags": [tag], "summary": f"delete a {kind} entry",
            "parameters": [id_param],
            "responses": {
                "200": {"description": "deleted",
                        "content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/DeleteResult"}}}},
                "404": {"description": "not found"},
            },
        },
    }


def build_openapi_spec() -> dict[str, Any]:
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "Ledger API",
            "version": "0.1.0",
            "description": (
                "PUT-based upsert API over the operator suite. Each PUT runs "
                "the corresponding operator, stores the normalized result "
                "in-memory, and returns it."
            ),
        },
        "tags": [
            {"name": "payment", "description": "one-off payments"},
            {"name": "debt", "description": "debt obligations"},
            {"name": "equity", "description": "deployable capital"},
            {"name": "sub", "description": "recurring subscriptions"},
            {"name": "ledger", "description": "full ledger view"},
        ],
        "paths": {
            "/ledger": {
                "get": {
                    "tags": ["ledger"],
                    "summary": "list all entries grouped by kind",
                    "responses": {"200": {"description": "ok"}},
                },
            },
            "/ledger/payment/{id}": _put_path(
                "payment", "PaymentRequest", "payment",
                "normalize a payment and upsert it"),
            "/ledger/debt/{id}": _put_path(
                "debt", "DebtRequest", "debt",
                "register a debt obligation"),
            "/ledger/equity/{id}": _put_path(
                "equity", "EquityRequest", "equity",
                "register deployable capital"),
            "/ledger/sub/{id}": _put_path(
                "sub", "SubscriptionRequest", "sub",
                "register a recurring subscription"),
        },
        "components": {"schemas": SCHEMAS},
    }
