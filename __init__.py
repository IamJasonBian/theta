"""
operators
=========

Airflow-style operators that wrap ledger actions as composable tasks.

Each operator codifies a single ledger action (normalize a payment, add a
debt, register deployable capital, register a recurring subscription) and
pushes a structured result to XCom for downstream consumption.
"""

from .payment_operator import (
    Card,
    JournalLine,
    NormalizePaymentOperator,
    Payment,
)
from .debt_operator import AddDebtOperator, Debt
from .equity_operator import AddEquityOperator, Equity
from .sub_operator import AddSubscriptionOperator, Subscription
from .dq_operator import Category, CompareCategoryOperator

__all__ = [
    "AddDebtOperator",
    "AddEquityOperator",
    "AddSubscriptionOperator",
    "Card",
    "Category",
    "CompareCategoryOperator",
    "Debt",
    "Equity",
    "JournalLine",
    "NormalizePaymentOperator",
    "Payment",
    "Subscription",
]
