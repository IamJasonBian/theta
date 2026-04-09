"""
Stdlib-only tests for NormalizePaymentOperator.

Run from repo root:
    python3 operators/test_payment_operator.py

No Airflow install required — we stub the two airflow modules the operator
imports before importing it. We also import payment_operator.py directly
(bypassing the package __init__) so the stub is in place first.
"""

import os
import sys
import types
import unittest
from datetime import date
from decimal import Decimal

# Make `import payment_operator` resolve to ./payment_operator.py without
# going through `operators/__init__.py` (which would trigger the real airflow
# import before we can stub it).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# --- stub airflow before importing the operator module ---
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

from payment_operator import (  # noqa: E402
    Card,
    NormalizePaymentOperator,
    Payment,
)


# --- fixtures ---

def _sapphire() -> Card:
    return Card(
        id="sapphire",
        name="Chase Sapphire",
        kind="credit",
        apr=Decimal("0.2199"),
        balance=Decimal("2450.00"),
        statement_close_day=15,
        grace_period_days=21,
        last_statement_balance=Decimal("1800.00"),
        last_statement_close=date(2026, 3, 15),
        credit_limit=Decimal("12000.00"),
    )


def _checking() -> Card:
    return Card(
        id="checking",
        name="Ally Checking",
        kind="debit",
        apr=Decimal("0.0"),
        balance=Decimal("5400.00"),
    )


def _run(payment: Payment, cards: dict[str, Card], expense="Expenses:Test") -> dict:
    op = NormalizePaymentOperator(
        task_id="t",
        payment=payment,
        cards=cards,
        expense_account=expense,
    )
    return op.execute(context={})


class JournalTests(unittest.TestCase):
    def test_credit_card_journal_is_balanced_and_uses_liability_account(self):
        out = _run(
            Payment(
                payee="Whole Foods",
                amount=Decimal("184.32"),
                card_id="sapphire",
                txn_date=date(2026, 4, 9),
            ),
            {"sapphire": _sapphire()},
            expense="Expenses:Groceries",
        )
        lines = out["journal"]
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0]["account"], "Expenses:Groceries")
        self.assertEqual(lines[0]["debit"], "184.32")
        self.assertEqual(lines[0]["credit"], "0.00")
        self.assertEqual(lines[1]["account"], "Liabilities:CreditCard:Chase Sapphire")
        self.assertEqual(lines[1]["debit"], "0.00")
        self.assertEqual(lines[1]["credit"], "184.32")

    def test_debit_card_journal_uses_assets_account(self):
        out = _run(
            Payment(
                payee="Landlord LLC",
                amount=Decimal("2200.00"),
                card_id="checking",
                txn_date=date(2026, 4, 9),
            ),
            {"checking": _checking()},
            expense="Expenses:Housing:Rent",
        )
        self.assertEqual(
            out["journal"][1]["account"],
            "Assets:Bank:Ally Checking",
        )

    def test_unknown_card_raises(self):
        with self.assertRaises(ValueError):
            _run(
                Payment(
                    payee="X", amount=Decimal("1.00"),
                    card_id="ghost", txn_date=date(2026, 4, 9),
                ),
                {"sapphire": _sapphire()},
            )


class TemporalTests(unittest.TestCase):
    def test_credit_card_temporal_block_populated(self):
        out = _run(
            Payment(
                payee="Whole Foods", amount=Decimal("184.32"),
                card_id="sapphire", txn_date=date(2026, 4, 9),
            ),
            {"sapphire": _sapphire()},
        )
        t = out["temporal"]
        self.assertTrue(t["applicable"])
        # txn 04-09, close_day 15 -> next close 04-15
        self.assertEqual(t["next_statement_close"], "2026-04-15")
        self.assertEqual(t["days_until_next_close"], 6)
        # +21 day grace -> due 05-06
        self.assertEqual(t["next_payment_due"], "2026-05-06")
        self.assertEqual(t["days_until_next_due"], 27)
        # prior close 03-15 + 21 = 04-05; txn 04-09 is *after* -> not in grace
        self.assertFalse(t["in_prior_statement_grace_period"])
        self.assertEqual(t["prior_due_date"], "2026-04-05")
        # carried interest = 1800 * 0.2199/365 ~= 1.08
        self.assertEqual(t["carried_balance_daily_interest"], "1.08")

    def test_in_prior_grace_period_flag(self):
        # txn 04-04 lands inside the 03-15..04-05 grace window
        out = _run(
            Payment(
                payee="W", amount=Decimal("50.00"),
                card_id="sapphire", txn_date=date(2026, 4, 4),
            ),
            {"sapphire": _sapphire()},
        )
        self.assertTrue(out["temporal"]["in_prior_statement_grace_period"])

    def test_close_day_clamped_to_short_month(self):
        # close_day 31 in February should clamp to Feb 28/29
        card = Card(
            id="c", name="C", kind="credit",
            apr=Decimal("0.20"), balance=Decimal("0"),
            statement_close_day=31, grace_period_days=21,
        )
        out = _run(
            Payment(
                payee="X", amount=Decimal("10.00"),
                card_id="c", txn_date=date(2026, 2, 10),
            ),
            {"c": card},
        )
        self.assertEqual(out["temporal"]["next_statement_close"], "2026-02-28")

    def test_debit_card_temporal_not_applicable(self):
        out = _run(
            Payment(
                payee="L", amount=Decimal("100.00"),
                card_id="checking", txn_date=date(2026, 4, 9),
            ),
            {"checking": _checking()},
        )
        self.assertFalse(out["temporal"]["applicable"])


class ProjectedInterestTests(unittest.TestCase):
    def test_projected_interest_matches_dpr_times_days(self):
        out = _run(
            Payment(
                payee="W", amount=Decimal("612.00"),
                card_id="sapphire", txn_date=date(2026, 4, 9),
            ),
            {"sapphire": _sapphire()},
        )
        # dpr = 0.2199/365; days_to_next_due = 27; charge = 612
        expected = (Decimal("612.00") * (Decimal("0.2199") / Decimal("365")) *
                    Decimal("27")).quantize(Decimal("0.01"))
        self.assertEqual(
            out["temporal"]["projected_interest_on_this_charge_if_unpaid_by_due"],
            str(expected),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
