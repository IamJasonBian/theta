"""
Stdlib-only tests for SyncCardScheduleOperator and the Plaid liabilities
fetch script's pure helpers.

Run from repo root:
    python3 operators/test_plaid_operator.py

No Airflow install required — the two airflow modules the operator imports
are stubbed before importing it.
"""

import os
import sys
import types
import unittest
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


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

from plaid_operator import (  # noqa: E402
    Autopay,
    CardAccount,
    SyncCardScheduleOperator,
    _add_months,
)

# Make `scripts/` importable for the fetch-script helper tests.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
import fetch_plaid_liabilities as fetch  # noqa: E402


def _run(card, as_of, lookback=6, lookahead=6):
    op = SyncCardScheduleOperator(
        task_id="t", card=card, as_of=as_of,
        lookback=lookback, lookahead=lookahead,
    )
    return op.execute(context={})


def _due_events(result):
    return [e for e in result["events"] if e["type"] == "payment_due"]


def _autopay_events(result):
    return [e for e in result["events"] if e["type"] == "autopay"]


class TestAddMonths(unittest.TestCase):
    def test_forward_and_back(self):
        self.assertEqual(_add_months(date(2026, 6, 15), 1), date(2026, 7, 15))
        self.assertEqual(_add_months(date(2026, 6, 15), -1), date(2026, 5, 15))

    def test_year_rollover_both_directions(self):
        self.assertEqual(_add_months(date(2026, 12, 10), 1), date(2027, 1, 10))
        self.assertEqual(_add_months(date(2026, 1, 10), -1), date(2025, 12, 10))

    def test_day_clamped_to_short_month(self):
        # Jan 31 - 12 months lands on a real day; Mar 31 -> Feb clamps to 28
        self.assertEqual(_add_months(date(2026, 3, 31), -1), date(2026, 2, 28))
        self.assertEqual(_add_months(date(2026, 1, 31), 1), date(2026, 2, 28))


class TestScheduleShape(unittest.TestCase):
    def setUp(self):
        self.card = CardAccount(
            id="5th-street-capital",
            name="5th Street Capital",
            next_due_date=date(2026, 8, 5),
            apr=Decimal("0.1999"),
            statement_balance=Decimal("1240.55"),
            minimum_payment=Decimal("35.00"),
            grace_period_days=21,
            last_payment_date=date(2026, 7, 3),
            last_payment_amount=Decimal("1100.00"),
            is_overdue=False,
        )

    def test_due_event_count_matches_window(self):
        result = _run(self.card, as_of=date(2026, 7, 21), lookback=6, lookahead=6)
        # lookback + current + lookahead = 13 due events
        self.assertEqual(len(_due_events(result)), 13)

    def test_current_cycle_is_seq_zero_and_dated_next_due(self):
        result = _run(self.card, as_of=date(2026, 7, 21))
        seq0 = [e for e in _due_events(result) if e["seq"] == 0]
        self.assertEqual(len(seq0), 1)
        self.assertEqual(seq0[0]["date"], "2026-08-05")

    def test_phase_split_historical_vs_upcoming(self):
        result = _run(self.card, as_of=date(2026, 7, 21))
        for e in _due_events(result):
            expected = "historical" if e["date"] < "2026-07-21" else "upcoming"
            self.assertEqual(e["phase"], expected, e)

    def test_statement_close_precedes_due_by_grace(self):
        result = _run(self.card, as_of=date(2026, 7, 21))
        seq0 = [e for e in _due_events(result) if e["seq"] == 0][0]
        # 2026-08-05 minus 21 days
        self.assertEqual(seq0["statement_close"], "2026-07-15")

    def test_only_current_cycle_has_amount(self):
        result = _run(self.card, as_of=date(2026, 7, 21))
        for e in _due_events(result):
            if e["seq"] == 0:
                self.assertEqual(e["amount"], "1240.55")
                self.assertFalse(e["estimated"])
            else:
                self.assertIsNone(e["amount"])
                self.assertTrue(e["estimated"])

    def test_summary_counts(self):
        result = _run(self.card, as_of=date(2026, 7, 21))
        s = result["summary"]
        self.assertEqual(s["historical_due_count"], 6)
        self.assertEqual(s["upcoming_due_count"], 7)  # current + 6 ahead
        self.assertEqual(s["next_due_date"], "2026-08-05")
        self.assertFalse(s["autopay_enabled"])


class TestReconciliationAndOverdue(unittest.TestCase):
    def test_recent_cycle_marked_paid(self):
        card = CardAccount(
            id="c", name="5th Street Capital",
            next_due_date=date(2026, 8, 5),
            last_payment_date=date(2026, 7, 3),
            last_payment_amount=Decimal("1100.00"),
        )
        result = _run(card, as_of=date(2026, 7, 21))
        # The cycle whose window (close..due) contains 2026-07-03 is seq -1
        # (due 2026-07-05, close 2026-06-14). It should be marked paid.
        paid = [e for e in _due_events(result) if e["paid"]]
        self.assertEqual([e["seq"] for e in paid], [-1])

    def test_no_last_payment_means_nothing_paid(self):
        card = CardAccount(id="c", name="X", next_due_date=date(2026, 8, 5))
        result = _run(card, as_of=date(2026, 7, 21))
        self.assertFalse(any(e["paid"] for e in _due_events(result)))

    def test_overdue_only_flags_current_cycle(self):
        card = CardAccount(
            id="c", name="X", next_due_date=date(2026, 8, 5), is_overdue=True,
        )
        result = _run(card, as_of=date(2026, 7, 21))
        overdue = [e for e in _due_events(result) if e["overdue"]]
        self.assertEqual([e["seq"] for e in overdue], [0])


class TestAutopay(unittest.TestCase):
    def test_autopay_events_emitted_with_lead_days(self):
        card = CardAccount(
            id="c", name="X", next_due_date=date(2026, 8, 5),
            statement_balance=Decimal("1240.55"),
            autopay=Autopay(enabled=True, basis="statement_balance", lead_days=2),
        )
        result = _run(card, as_of=date(2026, 7, 21))
        aps = _autopay_events(result)
        self.assertEqual(len(aps), 13)
        seq0 = [e for e in aps if e["seq"] == 0][0]
        self.assertEqual(seq0["due_date"], "2026-08-05")
        self.assertEqual(seq0["date"], "2026-08-03")  # 2 days before due
        self.assertEqual(seq0["amount"], "1240.55")

    def test_fixed_autopay_amount_known_for_all_cycles(self):
        card = CardAccount(
            id="c", name="X", next_due_date=date(2026, 8, 5),
            autopay=Autopay(enabled=True, basis="fixed",
                            amount=Decimal("200.00")),
        )
        result = _run(card, as_of=date(2026, 7, 21))
        for e in _autopay_events(result):
            self.assertEqual(e["amount"], "200.00")
            self.assertFalse(e["estimated"])

    def test_minimum_basis_only_current_cycle_known(self):
        card = CardAccount(
            id="c", name="X", next_due_date=date(2026, 8, 5),
            minimum_payment=Decimal("35.00"),
            autopay=Autopay(enabled=True, basis="minimum"),
        )
        result = _run(card, as_of=date(2026, 7, 21))
        seq0 = [e for e in _autopay_events(result) if e["seq"] == 0][0]
        self.assertEqual(seq0["amount"], "35.00")
        others = [e for e in _autopay_events(result) if e["seq"] != 0]
        self.assertTrue(all(e["amount"] is None for e in others))

    def test_no_autopay_events_when_disabled(self):
        card = CardAccount(id="c", name="X", next_due_date=date(2026, 8, 5))
        result = _run(card, as_of=date(2026, 7, 21))
        self.assertEqual(_autopay_events(result), [])
        self.assertIsNone(result["summary"]["next_autopay_date"])


class TestNegativeWindowRejected(unittest.TestCase):
    def test_negative_lookback(self):
        card = CardAccount(id="c", name="X", next_due_date=date(2026, 8, 5))
        with self.assertRaises(ValueError):
            SyncCardScheduleOperator(task_id="t", card=card, lookback=-1)


# --- fetch script pure-helper tests ---

_LIABILITIES_RESPONSE = {
    "accounts": [
        {"account_id": "acc_1", "name": "Everyday Checking",
         "official_name": "Bank Checking"},
        {"account_id": "acc_2", "name": "5th Street Capital Card",
         "official_name": "5th Street Capital Rewards Visa"},
    ],
    "liabilities": {
        "credit": [
            {"account_id": "acc_2",
             "last_statement_balance": 1240.55,
             "minimum_payment_amount": 35,
             "next_payment_due_date": "2026-08-05",
             "last_payment_date": "2026-07-03",
             "last_payment_amount": 1100,
             "is_overdue": False,
             "aprs": [
                 {"apr_type": "balance_transfer_apr", "apr_percentage": 0},
                 {"apr_type": "purchase_apr", "apr_percentage": 19.99},
             ]},
        ],
    },
}


class TestFetchHelpers(unittest.TestCase):
    def test_find_credit_account_by_substring(self):
        acct, credit = fetch.find_credit_account(
            _LIABILITIES_RESPONSE, "5th street capital")
        self.assertEqual(acct["account_id"], "acc_2")
        self.assertEqual(credit["next_payment_due_date"], "2026-08-05")

    def test_find_credit_account_no_match_raises(self):
        with self.assertRaises(RuntimeError):
            fetch.find_credit_account(_LIABILITIES_RESPONSE, "nonexistent bank")

    def test_purchase_apr_normalized(self):
        _, credit = fetch.find_credit_account(_LIABILITIES_RESPONSE, "5th street")
        self.assertEqual(fetch._purchase_apr(credit), Decimal("0.199900"))

    def test_build_card_maps_all_fields(self):
        acct, credit = fetch.find_credit_account(_LIABILITIES_RESPONSE, "5th street")
        card = fetch.build_card(acct, credit, "5th-street-capital",
                                Autopay(enabled=True, basis="statement_balance"))
        self.assertEqual(card.next_due_date, date(2026, 8, 5))
        self.assertEqual(card.statement_balance, Decimal("1240.55"))
        self.assertEqual(card.minimum_payment, Decimal("35"))
        self.assertEqual(card.last_payment_date, date(2026, 7, 3))
        self.assertTrue(card.autopay.enabled)

    def test_build_card_requires_due_date(self):
        acct = {"account_id": "a", "name": "X"}
        credit = {"account_id": "a"}  # no next_payment_due_date
        with self.assertRaises(RuntimeError):
            fetch.build_card(acct, credit, "x", Autopay())

    def test_resolve_credentials_prefers_secrets_url(self):
        def fake_fetch(url):
            self.assertEqual(url, "https://example/secrets")
            return {"client_id": "cid", "secret": "sec",
                    "access_token": "tok", "env": "sandbox"}
        creds = fetch.resolve_credentials("https://example/secrets",
                                          fetch_json=fake_fetch)
        self.assertEqual(creds["client_id"], "cid")
        self.assertEqual(creds["env"], "sandbox")

    def test_resolve_credentials_missing_raises(self):
        # No secrets url and (in CI) no PLAID_* env → should raise.
        saved = {k: os.environ.pop(k, None)
                 for k in ("PLAID_CLIENT_ID", "PLAID_SECRET", "PLAID_ACCESS_TOKEN")}
        try:
            with self.assertRaises(RuntimeError):
                fetch.resolve_credentials(None)
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    def test_run_end_to_end_with_stubs(self):
        put_calls = []

        def fake_get(url):
            return {"client_id": "cid", "secret": "sec",
                    "access_token": "tok", "env": "sandbox"}

        def fake_post(url, payload):
            self.assertTrue(url.endswith("/liabilities/get"))
            self.assertEqual(payload["access_token"], "tok")
            return _LIABILITIES_RESPONSE

        def fake_put(url, payload):
            put_calls.append((url, payload))
            return {"ok": True}

        result = fetch.run(
            secrets_url="https://example/secrets",
            card_name="5th Street Capital",
            card_id="5th-street-capital",
            autopay=Autopay(enabled=True, basis="statement_balance", lead_days=1),
            theta_base="https://theta.example",
            as_of=date(2026, 7, 21),
            fetch_json=fake_get, post_json=fake_post, put_json=fake_put,
        )
        self.assertEqual(result["_plaid_account_id"], "acc_2")
        self.assertEqual(len(put_calls), 1)
        url, body = put_calls[0]
        self.assertEqual(url, "https://theta.example/ledger/card/5th-street-capital")
        self.assertEqual(body["next_due_date"], "2026-08-05")
        self.assertTrue(body["autopay"]["enabled"])
        self.assertEqual(result["summary"]["next_due_date"], "2026-08-05")


if __name__ == "__main__":
    unittest.main(verbosity=2)
