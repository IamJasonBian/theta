"""
Stdlib-only tests for the debt / equity / subscription operators and the
PUT-based ledger API.

Run from the operators/ parent dir:
    python3 operators/test_ledger_operators.py
"""

import json
import os
import sys
import threading
import types
import unittest
import urllib.request
from datetime import date
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer


# --- stub airflow before importing operator modules ---
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

# Import operator modules directly (bypass package __init__).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tempfile  # noqa: E402

from debt_operator import AddDebtOperator, Debt  # noqa: E402
from equity_operator import AddEquityOperator, Equity  # noqa: E402
from sub_operator import AddSubscriptionOperator, Subscription  # noqa: E402
from payment_operator import Card  # noqa: E402
from dq_operator import Category, CompareCategoryOperator  # noqa: E402
import api as ledger_api  # noqa: E402
from api import (  # noqa: E402
    InMemoryLedgerStore,
    SqliteLedgerStore,
    build_store,
    load_wallet,
)


# ---------- DebtOperator ----------

class DebtOperatorTests(unittest.TestCase):
    def test_balanced_journal_and_days_until_due(self):
        debt = Debt(
            id="d1",
            creditor="IRS",
            principal=Decimal("1500.00"),
            due_date=date(2026, 4, 15),
            apr=Decimal("0.06"),
        )
        op = AddDebtOperator(
            task_id="t", debt=debt,
            as_of=date(2026, 4, 9),
            offset_account="Expenses:Taxes",
        )
        out = op.execute(context={})
        self.assertEqual(out["journal"][0]["account"], "Expenses:Taxes")
        self.assertEqual(out["journal"][0]["debit"], "1500.00")
        self.assertEqual(out["journal"][1]["account"], "Liabilities:Debt:IRS")
        self.assertEqual(out["journal"][1]["credit"], "1500.00")
        self.assertEqual(out["temporal"]["days_until_due"], 6)
        self.assertFalse(out["temporal"]["overdue"])
        # 1500 * 0.06 / 365 ~= 0.25
        self.assertEqual(out["temporal"]["daily_interest_accrual"], "0.25")
        self.assertEqual(out["temporal"]["overdue_interest_accrued"], "0.00")

    def test_overdue_accrues_interest(self):
        debt = Debt(
            id="d2",
            creditor="CardCo",
            principal=Decimal("1000.00"),
            due_date=date(2026, 3, 10),  # already past
            apr=Decimal("0.2199"),
        )
        op = AddDebtOperator(
            task_id="t", debt=debt, as_of=date(2026, 4, 9),
        )
        out = op.execute(context={})
        self.assertTrue(out["temporal"]["overdue"])
        self.assertLess(out["temporal"]["days_until_due"], 0)
        # 30 days overdue * (1000 * 0.2199/365) ~= 18.07
        self.assertEqual(out["temporal"]["overdue_interest_accrued"], "18.07")


# ---------- EquityOperator ----------

class EquityOperatorTests(unittest.TestCase):
    def test_equity_without_bucket(self):
        eq = Equity(
            id="e1", source="bonus",
            amount=Decimal("5000.00"),
            received_on=date(2026, 4, 1),
        )
        out = AddEquityOperator(task_id="t", equity=eq).execute(context={})
        self.assertEqual(out["journal"][0]["account"],
                         "Assets:Cash:Deployable")
        self.assertEqual(out["journal"][1]["account"],
                         "Equity:Contributed:bonus")
        self.assertEqual(out["journal"][0]["debit"], "5000.00")
        self.assertEqual(out["journal"][1]["credit"], "5000.00")

    def test_equity_with_target_bucket(self):
        eq = Equity(
            id="e2", source="savings",
            amount=Decimal("2000.00"),
            received_on=date(2026, 4, 1),
            target_bucket="sp500",
        )
        out = AddEquityOperator(task_id="t", equity=eq).execute(context={})
        self.assertEqual(out["journal"][0]["account"],
                         "Assets:Cash:Deployable:sp500")


# ---------- SubscriptionOperator ----------

def _sapphire() -> Card:
    return Card(
        id="sapphire", name="Chase Sapphire", kind="credit",
        apr=Decimal("0.2199"), balance=Decimal("0"),
        statement_close_day=15, grace_period_days=21,
    )


class SubscriptionOperatorTests(unittest.TestCase):
    def test_monthly_schedule_and_annualized(self):
        sub = Subscription(
            id="s1", service="netflix",
            amount=Decimal("15.99"),
            frequency="monthly",
            next_charge_date=date(2026, 4, 20),
            funding_card_id="sapphire",
        )
        out = AddSubscriptionOperator(
            task_id="t", subscription=sub,
            cards={"sapphire": _sapphire()},
            horizon=3,
        ).execute(context={})
        self.assertEqual(out["journal"][0]["account"],
                         "Expenses:Subscriptions:netflix")
        self.assertEqual(out["journal"][1]["account"],
                         "Liabilities:CreditCard:Chase Sapphire")
        self.assertEqual(out["schedule"]["annualized_cost"], "191.88")
        self.assertEqual(out["schedule"]["dates"], [
            "2026-04-20", "2026-05-20", "2026-06-20",
        ])

    def test_yearly_leap_day_rolls_to_feb28(self):
        card = _sapphire()
        sub = Subscription(
            id="s2", service="aws",
            amount=Decimal("100.00"),
            frequency="yearly",
            next_charge_date=date(2028, 2, 29),  # leap
            funding_card_id="sapphire",
        )
        out = AddSubscriptionOperator(
            task_id="t", subscription=sub,
            cards={"sapphire": card}, horizon=2,
        ).execute(context={})
        self.assertEqual(out["schedule"]["dates"],
                         ["2028-02-29", "2029-02-28"])

    def test_unknown_funding_card_raises(self):
        sub = Subscription(
            id="s3", service="x", amount=Decimal("1.00"),
            frequency="monthly",
            next_charge_date=date(2026, 4, 1),
            funding_card_id="ghost",
        )
        with self.assertRaises(ValueError):
            AddSubscriptionOperator(
                task_id="t", subscription=sub, cards={},
            ).execute(context={})


# ---------- API end-to-end ----------

class ApiE2ETests(unittest.TestCase):
    server: ledger_api.ThreadingHTTPServer
    thread: threading.Thread
    base: str

    @classmethod
    def setUpClass(cls) -> None:
        wallet = {"sapphire": _sapphire()}
        cls.server = ledger_api.build_server(
            host="127.0.0.1", port=0, wallet=wallet,
        )
        port = cls.server.server_address[1]
        cls.base = f"http://127.0.0.1:{port}"
        cls.thread = threading.Thread(
            target=cls.server.serve_forever, daemon=True,
        )
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.thread.join(timeout=2)

    def _req(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def test_put_then_get_debt(self):
        body = {
            "creditor": "IRS", "principal": "1500.00",
            "due_date": "2026-04-15", "apr": "0.06",
            "as_of": "2026-04-09",
            "offset_account": "Expenses:Taxes",
        }
        code, entry = self._req("PUT", "/ledger/debt/d1", body)
        self.assertEqual(code, 200)
        self.assertEqual(entry["debt"]["creditor"], "IRS")

        code, readback = self._req("GET", "/ledger/debt/d1")
        self.assertEqual(code, 200)
        self.assertEqual(readback, entry)

    def test_put_equity_and_sub_then_list(self):
        self._req("PUT", "/ledger/equity/e1", {
            "source": "bonus", "amount": "5000.00",
            "received_on": "2026-04-01", "target_bucket": "sp500",
        })
        self._req("PUT", "/ledger/sub/netflix", {
            "service": "netflix", "amount": "15.99",
            "frequency": "monthly",
            "next_charge_date": "2026-04-20",
            "funding_card_id": "sapphire",
            "horizon": 3,
        })
        code, all_entries = self._req("GET", "/ledger")
        self.assertEqual(code, 200)
        self.assertIn("e1", all_entries["equity"])
        self.assertIn("netflix", all_entries["sub"])

    def test_put_is_idempotent_upsert(self):
        body = {
            "source": "bonus", "amount": "1.00",
            "received_on": "2026-04-01",
        }
        _, first = self._req("PUT", "/ledger/equity/dup", body)
        body["amount"] = "2.00"
        _, second = self._req("PUT", "/ledger/equity/dup", body)
        self.assertEqual(first["equity"]["amount"], "1.00")
        self.assertEqual(second["equity"]["amount"], "2.00")
        _, readback = self._req("GET", "/ledger/equity/dup")
        self.assertEqual(readback["equity"]["amount"], "2.00")

    def test_delete(self):
        self._req("PUT", "/ledger/equity/tmp", {
            "source": "x", "amount": "1.00", "received_on": "2026-04-01",
        })
        code, out = self._req("DELETE", "/ledger/equity/tmp")
        self.assertEqual(code, 200)
        self.assertTrue(out["deleted"])
        code, _ = self._req("GET", "/ledger/equity/tmp")
        self.assertEqual(code, 404)

    def test_openapi_spec_served(self):
        code, spec = self._req("GET", "/openapi.json")
        self.assertEqual(code, 200)
        self.assertEqual(spec["openapi"], "3.0.3")
        self.assertIn("/ledger/debt/{id}", spec["paths"])
        self.assertIn("/ledger/payment/{id}", spec["paths"])
        self.assertIn("DebtRequest", spec["components"]["schemas"])

    def test_docs_served_as_html(self):
        req = urllib.request.Request(self.base + "/docs")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            self.assertTrue(resp.headers["Content-Type"].startswith("text/html"))
            body = resp.read().decode()
            self.assertIn("swagger-ui", body)

    def test_frontend_served_at_root(self):
        req = urllib.request.Request(self.base + "/")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            body = resp.read().decode()
            self.assertIn("Ledger", body)
            self.assertIn('data-tab="payment"', body)

    def test_healthz(self):
        code, out = self._req("GET", "/healthz")
        self.assertEqual(code, 200)
        self.assertTrue(out["ok"])

    def test_pwa_manifest_served(self):
        req = urllib.request.Request(self.base + "/manifest.json")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn(
                "manifest+json", resp.headers["Content-Type"],
            )
            data = json.loads(resp.read())
            self.assertEqual(data["short_name"], "theta")
            self.assertEqual(data["display"], "standalone")
            self.assertTrue(any(i["src"] == "/icon.svg" for i in data["icons"]))

    def test_icon_svg_served(self):
        req = urllib.request.Request(self.base + "/icon.svg")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.headers["Content-Type"], "image/svg+xml")
            body = resp.read().decode()
            self.assertIn("<svg", body)

    def test_service_worker_served_with_js_content_type(self):
        req = urllib.request.Request(self.base + "/sw.js")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("javascript", resp.headers["Content-Type"])
            body = resp.read().decode()
            self.assertIn("CACHE_VERSION", body)
            # Regression guard: the version should be explicitly declared
            # and should not be the original v1 (which no longer matches
            # the HTML shape shipped in later commits). If this fails,
            # bump the version constant in frontend/sw.js.
            self.assertIn("theta-shell-v3", body)

    def test_frontend_has_mobile_meta_tags(self):
        req = urllib.request.Request(self.base + "/")
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode()
            self.assertIn('name="viewport"', body)
            self.assertIn('viewport-fit=cover', body)
            self.assertIn('rel="manifest"', body)
            self.assertIn('apple-mobile-web-app-capable', body)
            self.assertIn('theme-color', body)
            # responsive breakpoint present
            self.assertIn('@media (max-width: 768px)', body)
            # 16px inputs to kill iOS zoom-on-focus
            self.assertIn('font-size: 16px', body)

    def test_frontend_has_id_handling_js(self):
        """New id behavior must be in the served HTML: auto-gen prefill,
        collision confirm, 'default' fallback, form reset after submit."""
        req = urllib.request.Request(self.base + "/")
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode()
            self.assertIn("function genId(", body)
            self.assertIn("function prefillId(", body)
            self.assertIn("function resetFieldsForTab(", body)
            self.assertIn("'default'", body)
            self.assertIn("latestLedger", body)
            self.assertIn("already exists", body)
            self.assertIn("leave blank", body)  # placeholder hint

    def test_bad_json_returns_400(self):
        req = urllib.request.Request(
            self.base + "/ledger/debt/x",
            data=b"not json",
            method="PUT",
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req)
            self.fail("expected HTTPError")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)


# ---------- default card wallet tests ----------

class DefaultWalletTests(unittest.TestCase):
    def test_load_wallet_with_no_path_still_has_default(self):
        w = load_wallet(None)
        self.assertIn("default", w)
        self.assertEqual(w["default"].kind, "debit")

    def test_load_wallet_with_missing_path_still_has_default(self):
        w = load_wallet("/nonexistent/path.json")
        self.assertIn("default", w)

    def test_load_wallet_real_file_still_gets_default_injected(self):
        # Write a minimal wallet file that doesn't itself mention "default".
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False,
        ) as tmp:
            json.dump([{"id": "sapphire", "name": "Chase Sapphire",
                        "kind": "credit", "apr": "0.2199"}], tmp)
            wallet_path = tmp.name
        try:
            w = load_wallet(wallet_path)
            self.assertIn("sapphire", w)
            self.assertIn("default", w)  # auto-injected
        finally:
            os.unlink(wallet_path)


class DefaultCardPaymentE2ETests(unittest.TestCase):
    """End-to-end: spin up a server with load_wallet()'s default-only
    wallet and PUT a payment that references card_id='default'. Proves
    the frontend's blank → 'default' fallback actually resolves on the
    server side."""

    @classmethod
    def setUpClass(cls) -> None:
        # Use load_wallet(None) → { "default": <debit card> } only
        cls.server = ledger_api.build_server(
            host="127.0.0.1", port=0, wallet=load_wallet(None),
        )
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.thread.join(timeout=2)

    def test_payment_with_default_card_id(self):
        req = urllib.request.Request(
            self.base + "/ledger/payment/default",
            data=json.dumps({
                "payee": "Coffee",
                "amount": "4.50",
                "card_id": "default",
                "txn_date": "2026-04-09",
                "expense_account": "Expenses:Food",
            }).encode(),
            method="PUT",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(data["card"]["id"], "default")
        self.assertEqual(data["card"]["kind"], "debit")
        # debit card → Assets:Bank counter account, not Liabilities
        self.assertEqual(
            data["journal"][1]["account"],
            "Assets:Bank:Default",
        )
        # temporal block is not applicable for debit cards
        self.assertFalse(data["temporal"]["applicable"])


# ---------- SqliteLedgerStore unit tests ----------

class SqliteLedgerStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".sqlite", delete=False,
        )
        self.tmp.close()
        self.path = self.tmp.name
        self.store = SqliteLedgerStore(self.path)

    def tearDown(self) -> None:
        os.unlink(self.path)
        for ext in ("-wal", "-shm"):
            p = self.path + ext
            if os.path.exists(p):
                os.unlink(p)

    def test_put_get_roundtrip(self):
        self.store.put("debt", "d1", {"debt": {"creditor": "IRS"}})
        got = self.store.get("debt", "d1")
        self.assertEqual(got, {"debt": {"creditor": "IRS"}})

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.store.get("debt", "missing"))

    def test_put_is_upsert(self):
        self.store.put("equity", "e1", {"amount": "100"})
        self.store.put("equity", "e1", {"amount": "200"})
        self.assertEqual(self.store.get("equity", "e1"), {"amount": "200"})

    def test_delete_returns_true_only_if_existed(self):
        self.store.put("sub", "s1", {"x": 1})
        self.assertTrue(self.store.delete("sub", "s1"))
        self.assertFalse(self.store.delete("sub", "s1"))
        self.assertIsNone(self.store.get("sub", "s1"))

    def test_all_groups_by_kind(self):
        self.store.put("debt", "d1", {"creditor": "IRS"})
        self.store.put("debt", "d2", {"creditor": "CardCo"})
        self.store.put("equity", "e1", {"source": "bonus"})
        out = self.store.all()
        self.assertEqual(set(out["debt"].keys()), {"d1", "d2"})
        self.assertEqual(set(out["equity"].keys()), {"e1"})
        self.assertEqual(out["sub"], {})

    def test_survives_store_recreation(self):
        """The whole point of SQLite: close the store, open a new one
        against the same file, read the data back."""
        self.store.put("debt", "d1", {"creditor": "IRS", "amount": "1500"})
        self.store.put("equity", "e1", {"source": "bonus"})
        # simulate a process restart by creating a fresh store over the
        # same file path
        fresh = SqliteLedgerStore(self.path)
        self.assertEqual(
            fresh.get("debt", "d1"),
            {"creditor": "IRS", "amount": "1500"},
        )
        self.assertIn("e1", fresh.all()["equity"])

    def test_factory_picks_sqlite_when_env_set(self):
        os.environ["LEDGER_DB_PATH"] = self.path
        try:
            store = build_store()
            self.assertIsInstance(store, SqliteLedgerStore)
        finally:
            del os.environ["LEDGER_DB_PATH"]

    def test_factory_defaults_to_memory(self):
        os.environ.pop("LEDGER_DB_PATH", None)
        store = build_store()
        self.assertIsInstance(store, InMemoryLedgerStore)


# ---------- API end-to-end against a SQLite-backed server ----------

class ApiSqliteE2ETests(unittest.TestCase):
    """Same API flow as ApiE2ETests but with a SQLite-backed store, and
    with a synthetic 'restart' in the middle that rebuilds the store
    against the same disk file and confirms entries survive."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        cls.tmp.close()
        cls.path = cls.tmp.name
        wallet = {"sapphire": _sapphire()}
        cls.server = ledger_api.build_server(
            host="127.0.0.1", port=0, wallet=wallet,
            store=SqliteLedgerStore(cls.path),
        )
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.thread.join(timeout=2)
        os.unlink(cls.path)
        for ext in ("-wal", "-shm"):
            p = cls.path + ext
            if os.path.exists(p):
                os.unlink(p)

    def _req(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())

    def test_put_get_persists_across_store_recreation(self):
        body = {
            "creditor": "IRS", "principal": "1500.00",
            "due_date": "2026-04-15", "apr": "0.06",
            "as_of": "2026-04-09",
        }
        code, _ = self._req("PUT", "/ledger/debt/d1", body)
        self.assertEqual(code, 200)

        # Reach into the running server and replace its store with a fresh
        # SqliteLedgerStore over the same file path, mimicking a process
        # restart that re-reads the disk.
        fresh = SqliteLedgerStore(self.path)
        # We have to mutate the closure captured by make_handler; easiest
        # path is to verify via the fresh store directly that the entry
        # landed on disk, which proves persistence end-to-end.
        entry = fresh.get("debt", "d1")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["debt"]["creditor"], "IRS")
        self.assertEqual(entry["journal"][1]["credit"], "1500.00")


# ---------- CompareCategoryOperator (DQ) ----------

def _ledger_fixture() -> dict:
    """A synthetic ledger with entries in every kind for DQ filtering."""
    return {
        "payment": {
            "p1": {"payment": {"payee": "Whole Foods", "amount": "184.32"}},
            "p2": {"payment": {"payee": "Coffee", "amount": "4.50"}},
        },
        "debt": {
            "d1": {"debt": {"creditor": "IRS", "principal": "1500.00"}},
            "d2": {"debt": {"creditor": "CardCo", "principal": "1000.00"}},
        },
        "equity": {
            "e1": {"equity": {"source": "rh", "amount": "90000.00"}},
            "e2": {"equity": {"source": "rh", "amount": "2000.00"}},
            "e3": {"equity": {"source": "bonus", "amount": "5000.00"}},
        },
        "sub": {
            "s1": {"subscription": {"service": "netflix", "amount": "15.99"}},
        },
    }


class CategoryParsingTests(unittest.TestCase):
    def test_kind_only(self):
        c = Category.parse("equity")
        self.assertEqual((c.kind, c.label), ("equity", None))

    def test_kind_and_label(self):
        c = Category.parse("equity/rh")
        self.assertEqual((c.kind, c.label), ("equity", "rh"))

    def test_label_with_slashes_preserved(self):
        # defensive: only split on the FIRST slash
        c = Category.parse("debt/Citi/Card-1")
        self.assertEqual((c.kind, c.label), ("debt", "Citi/Card-1"))


class DqOperatorUnitTests(unittest.TestCase):
    def _run(self, category, prod, threshold="0.05", ledger=None) -> dict:
        op = CompareCategoryOperator(
            task_id="t",
            category=category,
            ledger=ledger if ledger is not None else _ledger_fixture(),
            prod_value=Decimal(prod),
            threshold_pct=Decimal(threshold),
        )
        return op.execute(context={})

    def test_match_within_threshold(self):
        # ledger equity/rh = 92000.00; prod 92000.00 → delta 0, match
        out = self._run("equity/rh", "92000.00")
        self.assertEqual(out["match_count"], 2)
        self.assertEqual(sorted(out["matched_ids"]), ["e1", "e2"])
        self.assertEqual(out["ledger_amount"], "92000.00")
        self.assertEqual(out["delta"], "0.00")
        self.assertTrue(out["within_threshold"])
        self.assertEqual(out["flag"], "match")

    def test_drift_flagged(self):
        # ledger 92000, prod 96000 → delta 4000 = 4.17% > 5% threshold? No,
        # 4000/96000 = 0.0417 which is < 0.05 → match.
        # Use 100000 prod instead → delta 8000/100000 = 0.08, drift.
        out = self._run("equity/rh", "100000.00")
        self.assertFalse(out["within_threshold"])
        self.assertEqual(out["flag"], "drift")

    def test_severe_drift_flagged(self):
        # ledger 92000, prod 200000 → delta_pct 0.54 → > 2*threshold → severe
        out = self._run("equity/rh", "200000.00")
        self.assertEqual(out["flag"], "severe_drift")

    def test_ledger_zero_prod_nonzero_flagged_severe(self):
        # matching the example from the user: ledger has no equity/rh
        # entries yet, but prod says we have $92k there.
        ledger = {"equity": {}}
        out = self._run("equity/rh", "92040.57", ledger=ledger)
        self.assertEqual(out["match_count"], 0)
        self.assertEqual(out["ledger_amount"], "0.00")
        self.assertEqual(out["delta"], "92040.57")
        self.assertEqual(out["flag"], "severe_drift")
        self.assertFalse(out["within_threshold"])

    def test_label_filter_scopes_the_sum(self):
        # equity total across all sources should be 97000, but equity/rh
        # only captures the two rh entries = 92000
        full = self._run("equity", "97000.00")
        self.assertEqual(full["ledger_amount"], "97000.00")
        self.assertEqual(full["match_count"], 3)

    def test_unknown_kind_raises(self):
        with self.assertRaises(ValueError):
            self._run("ghost/x", "10.00")

    def test_debt_category(self):
        out = self._run("debt/IRS", "1500.00")
        self.assertEqual(out["match_count"], 1)
        self.assertEqual(out["matched_ids"], ["d1"])
        self.assertEqual(out["flag"], "match")

    def test_sub_category(self):
        out = self._run("sub/netflix", "15.99")
        self.assertEqual(out["match_count"], 1)
        self.assertEqual(out["flag"], "match")

    def test_payment_category(self):
        out = self._run("payment/Coffee", "4.50")
        self.assertEqual(out["match_count"], 1)
        self.assertEqual(out["flag"], "match")

    def test_prod_zero_ledger_zero_is_match(self):
        ledger = {"equity": {}}
        out = self._run("equity/rh", "0", ledger=ledger)
        self.assertEqual(out["flag"], "match")
        self.assertTrue(out["within_threshold"])


# ---------- DQ integration test with stub HTTP server ----------

class DqIntegrationStubTests(unittest.TestCase):
    """Spins up a stdlib HTTP server that mocks BOTH the allocation-engine
    snapshot API and the theta ledger API, then runs the real
    fetch_rh_equity.run() function against the stub. Proves the whole
    fetch-and-compare flow end-to-end without touching the real network.

    We stub buying_power (not equity) because that's what equity/rh
    actually tracks — see the docstring at the top of
    scripts/fetch_rh_equity.py for the rationale."""

    # Matches e1 + e2 in _ledger_fixture() (92000.00) within 5% threshold.
    # We use buying_power as the comparison field now.
    PROD_BUYING_POWER = Decimal("92040.57")

    class _StubHandler(BaseHTTPRequestHandler):
        # Filled in by setUpClass so the closure over instance state is
        # visible inside do_GET.
        ledger: dict = {}
        prod_buying_power: Decimal = Decimal("0")

        def log_message(self, *a, **kw):  # silent
            pass

        def _send(self, status, payload, content_type="application/json"):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            path = self.path
            # ---- allocation-engine routes ----
            if path == "/api/snapshots":
                return self._send(200, {
                    "count": 2,
                    "snapshots": ["latest", "2026-04-09T22-09-57"],
                })
            if path.startswith("/api/snapshots?key="):
                key = path.split("=", 1)[1]
                return self._send(200, {
                    "snapshot_key": key,
                    "data": {
                        "timestamp": "2026-04-09T22:09:57.000Z",
                        "account": {
                            # equity stays a large number but the client
                            # must NOT read it — it should read buying_power
                            "equity": 999999.99,
                            "cash": -83438.81,
                            "buying_power": float(self.prod_buying_power),
                            "portfolio_value": 175479.38,
                        },
                        "positions": [],
                    },
                })
            # ---- theta route ----
            if path == "/ledger":
                return self._send(200, self.ledger)
            return self._send(404, {"error": "not found"})

    @classmethod
    def setUpClass(cls):
        # Install state on the handler class so requests can see it
        cls._StubHandler.ledger = _ledger_fixture()
        cls._StubHandler.prod_buying_power = cls.PROD_BUYING_POWER
        # Port 0 → OS picks a free port
        cls.server = HTTPServer(("127.0.0.1", 0), cls._StubHandler)
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        # Import the script lazily so it uses the same airflow-stubbed
        # sys.modules we already set up above.
        sys.path.insert(
            0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"),
        )
        import fetch_rh_equity  # noqa: E402
        cls.fetch = fetch_rh_equity

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(timeout=2)

    def test_fetch_prod_buying_power_picks_real_snapshot(self):
        bp, key = self.fetch.fetch_prod_buying_power(self.base)
        self.assertEqual(Decimal(str(bp)), self.PROD_BUYING_POWER)
        # The stub returns "latest" first and then a real key; the client
        # must skip "latest" and use the real one.
        self.assertEqual(key, "2026-04-09T22-09-57")

    def test_fetch_does_not_accidentally_read_equity_field(self):
        """Regression: we flipped from account.equity (gross) to
        account.buying_power (deployable). The stub serves equity=999999.99
        on purpose — if the client accidentally reads it, comparison math
        will blow up."""
        bp, _ = self.fetch.fetch_prod_buying_power(self.base)
        self.assertNotEqual(Decimal(str(bp)), Decimal("999999.99"))

    def test_fetch_ledger_returns_stub_data(self):
        ledger = self.fetch.fetch_ledger(self.base)
        self.assertIn("e1", ledger["equity"])
        self.assertIn("e2", ledger["equity"])

    def test_full_run_detects_match(self):
        # Ledger equity/rh = 92000, prod buying_power = 92040.57 → delta 40.57
        # → delta_pct ~0.044% → well within 5% threshold → match
        result = self.fetch.run(
            allocation_engine_base=self.base,
            theta_base=self.base,
            category="equity/rh",
            threshold=Decimal("0.05"),
        )
        self.assertEqual(result["flag"], "match")
        self.assertEqual(result["match_count"], 2)
        self.assertEqual(result["_prod_snapshot_key"], "2026-04-09T22-09-57")
        self.assertEqual(result["_prod_field"], "account.buying_power")

    def test_full_run_flags_drift_with_tight_threshold(self):
        # Same numbers, but now require 0.001% drift → must flag
        result = self.fetch.run(
            allocation_engine_base=self.base,
            theta_base=self.base,
            category="equity/rh",
            threshold=Decimal("0.00001"),
        )
        self.assertIn(result["flag"], ("drift", "severe_drift"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
