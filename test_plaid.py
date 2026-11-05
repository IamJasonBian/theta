"""
Stdlib-only tests for the Plaid integration.

The Plaid client is exercised by monkey-patching ``urllib.request.urlopen``
so the suite never makes a real network call. Each test composes its own
fake response (or HTTPError) and verifies that the client surfaces the
right exception class and that the liabilities → debt mapping does what
the sync endpoint relies on.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import types
import unittest
import urllib.error
import urllib.request
from datetime import date


# --- stub airflow so operator imports don't pull a real Airflow ---
def _install_airflow_stub() -> None:
    if "airflow" in sys.modules:
        return
    airflow = types.ModuleType("airflow")
    airflow_models = types.ModuleType("airflow.models")
    airflow_utils = types.ModuleType("airflow.utils")
    airflow_utils_context = types.ModuleType("airflow.utils.context")

    class _BaseOperator:
        template_fields = ()

        def __init__(self, *args, **kwargs):
            import logging
            self.log = logging.getLogger("BaseOperator")

    airflow_models.BaseOperator = _BaseOperator
    airflow_utils_context.Context = dict
    sys.modules["airflow"] = airflow
    sys.modules["airflow.models"] = airflow_models
    sys.modules["airflow.utils"] = airflow_utils
    sys.modules["airflow.utils.context"] = airflow_utils_context


_install_airflow_stub()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from plaid_client import (  # noqa: E402
    PlaidAPIError,
    PlaidClient,
    PlaidConnectionError,
    PlaidTimeoutError,
)
from plaid_store import PlaidTokenStore  # noqa: E402
from api import _normalize_liabilities, _purchase_apr  # noqa: E402


# ---------- fakes for urlopen ----------

class _FakeResponse:
    def __init__(self, body: bytes, status: int = 200, headers: dict | None = None):
        self._body = body
        self.status = status
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _ok_response(payload: dict) -> _FakeResponse:
    return _FakeResponse(json.dumps(payload).encode())


def _http_error(status: int, body: dict | str) -> urllib.error.HTTPError:
    blob = json.dumps(body).encode() if isinstance(body, dict) else body.encode()
    return urllib.error.HTTPError(
        url="https://sandbox.plaid.com/x",
        code=status,
        msg="error",
        hdrs={},  # type: ignore[arg-type]
        fp=io.BytesIO(blob),
    )


# ---------- PlaidClient ----------

class PlaidClientTests(unittest.TestCase):
    def _client(self) -> PlaidClient:
        return PlaidClient("cid", "sec", env="sandbox", timeout=2)

    def test_unknown_env_raises(self):
        with self.assertRaises(ValueError):
            PlaidClient("cid", "sec", env="mars")

    def test_link_token_create_includes_credentials_and_user(self):
        client = self._client()
        captured = {}

        def fake_urlopen(req, timeout):
            captured["url"] = req.full_url
            captured["body"] = json.loads(req.data.decode())
            return _ok_response({"link_token": "link-sand-xyz", "expiration": "2026-05-20T00:00:00Z"})

        with _patched(urllib.request, "urlopen", fake_urlopen):
            resp = client.link_token_create(
                user_id="theta-user", products=("liabilities",),
            )
        self.assertEqual(resp["link_token"], "link-sand-xyz")
        self.assertEqual(captured["url"], "https://sandbox.plaid.com/link/token/create")
        body = captured["body"]
        self.assertEqual(body["client_id"], "cid")
        self.assertEqual(body["secret"], "sec")
        self.assertEqual(body["user"], {"client_user_id": "theta-user"})
        self.assertEqual(body["products"], ["liabilities"])

    def test_api_error_extracts_plaid_error_fields(self):
        client = self._client()

        def fake_urlopen(req, timeout):
            raise _http_error(400, {
                "error_type": "INVALID_REQUEST",
                "error_code": "INVALID_FIELD",
                "error_message": "client_id must not be empty",
                "request_id": "req-abc",
            })

        with _patched(urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(PlaidAPIError) as ctx:
                client.link_token_create(user_id="u")
        err = ctx.exception
        self.assertEqual(err.status, 400)
        self.assertEqual(err.error_type, "INVALID_REQUEST")
        self.assertEqual(err.error_code, "INVALID_FIELD")
        self.assertEqual(err.error_message, "client_id must not be empty")
        self.assertEqual(err.request_id, "req-abc")
        self.assertIn("INVALID_FIELD", str(err))

    def test_connection_error_classified(self):
        client = self._client()

        def fake_urlopen(req, timeout):
            raise urllib.error.URLError("Name or service not known")

        with _patched(urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(PlaidConnectionError):
                client.item_get("access-sand-xyz")

    def test_timeout_classified(self):
        client = self._client()
        import socket

        def fake_urlopen(req, timeout):
            raise socket.timeout("timed out")

        with _patched(urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(PlaidTimeoutError):
                client.item_get("access-sand-xyz")

    def test_unparseable_body_surfaces_as_api_error(self):
        client = self._client()

        def fake_urlopen(req, timeout):
            return _FakeResponse(b"<html>oops</html>", status=200)

        with _patched(urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(PlaidAPIError) as ctx:
                client.item_get("access-sand-xyz")
        self.assertEqual(ctx.exception.error_code, "INVALID_JSON_RESPONSE")


# ---------- PlaidTokenStore ----------

class PlaidTokenStoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "plaid_token.json")

    def tearDown(self):
        self.dir.cleanup()

    def test_get_missing_returns_none(self):
        self.assertIsNone(PlaidTokenStore(self.path).get())

    def test_save_get_roundtrip(self):
        store = PlaidTokenStore(self.path)
        store.save(
            access_token="access-sand-xyz",
            item_id="item-1",
            institution_id="ins_1",
            institution_name="Bank of Theta",
        )
        tok = store.get()
        self.assertEqual(tok["access_token"], "access-sand-xyz")
        self.assertEqual(tok["item_id"], "item-1")
        self.assertEqual(tok["institution_name"], "Bank of Theta")
        self.assertIn("created_at", tok)

    def test_clear(self):
        store = PlaidTokenStore(self.path)
        store.save(access_token="a", item_id="b")
        self.assertTrue(store.clear())
        self.assertIsNone(store.get())
        self.assertFalse(store.clear())  # idempotent

    def test_corrupt_file_returns_none(self):
        with open(self.path, "w") as f:
            f.write("{not json")
        self.assertIsNone(PlaidTokenStore(self.path).get())


# ---------- Liability normalization (the /plaid/sync engine) ----------

class NormalizeLiabilitiesTests(unittest.TestCase):
    def test_credit_card_with_purchase_apr(self):
        resp = {
            "accounts": [{
                "account_id": "acc_cc_1",
                "name": "Plaid Credit Card",
                "official_name": "Plaid Diamond 12.5% APR Interest Credit Card",
                "balances": {"current": 1708.77},
                "subtype": "credit card",
                "type": "credit",
            }],
            "liabilities": {
                "credit": [{
                    "account_id": "acc_cc_1",
                    "aprs": [
                        {"apr_percentage": 19.99, "apr_type": "purchase_apr"},
                        {"apr_percentage": 27.95, "apr_type": "cash_apr"},
                    ],
                    "minimum_payment_amount": 25.0,
                    "next_payment_due_date": "2026-05-28",
                    "last_payment_amount": 168.25,
                    "last_payment_date": "2026-04-22",
                }],
                "mortgage": [],
                "student": [],
            },
        }
        out = _normalize_liabilities(resp, today=date(2026, 5, 20))
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertEqual(row["id"], "plaid-credit-acc_cc_1")
        self.assertEqual(row["due_date"], "2026-05-28")
        self.assertEqual(row["principal"], "1708.77")
        # 19.99% → 0.1999 (purchase_apr picked over cash_apr)
        self.assertEqual(row["apr"], "0.1999")
        self.assertEqual(row["creditor"],
                         "Plaid Diamond 12.5% APR Interest Credit Card")
        self.assertIn("plaid-synced", row["memo"])
        self.assertIn("min=25.0", row["memo"])

    def test_skips_account_with_no_balance(self):
        resp = {
            "accounts": [{"account_id": "x", "name": "X", "balances": {"current": None}}],
            "liabilities": {"credit": [{
                "account_id": "x", "aprs": [],
                "next_payment_due_date": "2026-06-01",
            }]},
        }
        self.assertEqual(_normalize_liabilities(resp, today=date(2026, 5, 20)), [])

    def test_skips_liability_with_no_due_date(self):
        resp = {
            "accounts": [{"account_id": "x", "name": "X", "balances": {"current": 100}}],
            "liabilities": {"credit": [{
                "account_id": "x", "aprs": [],
                # no next_payment_due_date, no last_statement_issue_date
            }]},
        }
        self.assertEqual(_normalize_liabilities(resp, today=date(2026, 5, 20)), [])

    def test_purchase_apr_fallback_to_first(self):
        # When no apr_type is "purchase_apr", fall back to the first APR
        self.assertEqual(
            str(_purchase_apr([{"apr_percentage": 6.5, "apr_type": "balance_transfer_apr"}])),
            "0.0650",
        )

    def test_purchase_apr_empty_is_zero(self):
        self.assertEqual(str(_purchase_apr([])), "0")


# ---------- helper ----------

import contextlib  # noqa: E402


@contextlib.contextmanager
def _patched(obj, name, replacement):
    original = getattr(obj, name)
    setattr(obj, name, replacement)
    try:
        yield
    finally:
        setattr(obj, name, original)


if __name__ == "__main__":
    unittest.main(verbosity=2)
