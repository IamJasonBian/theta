"""
AddDebtOperator
===============

Registers a debt obligation (a payment to make) on the ledger and reports
its temporal dependencies against its due date.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


@dataclass(frozen=True)
class Debt:
    id: str
    creditor: str
    principal: Decimal           # amount owed
    due_date: date
    apr: Decimal = Decimal("0.0")   # annual interest rate on unpaid balance
    memo: str = ""


class AddDebtOperator(BaseOperator):
    """
    Register a debt obligation:
      Dr <offset_account>   (default Expenses:Uncategorized)
      Cr Liabilities:Debt:<creditor>

    :param debt: the Debt obligation
    :param as_of: valuation date for temporal report (defaults to today)
    :param offset_account: account to debit (default Expenses:Uncategorized)
    """

    template_fields = ("offset_account",)

    def __init__(
        self,
        *,
        debt: Debt,
        as_of: date | None = None,
        offset_account: str = "Expenses:Uncategorized",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.debt = debt
        self.as_of = as_of
        self.offset_account = offset_account

    @staticmethod
    def _q(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def execute(self, context: Context) -> dict[str, Any]:
        amt = self._q(self.debt.principal)
        journal = [
            {"account": self.offset_account,
             "debit": str(amt), "credit": "0.00"},
            {"account": f"Liabilities:Debt:{self.debt.creditor}",
             "debit": "0.00", "credit": str(amt)},
        ]

        as_of = self.as_of or date.today()
        days_until_due = (self.debt.due_date - as_of).days
        overdue = days_until_due < 0

        dpr = self.debt.apr / Decimal("365")
        daily_accrual = self._q(self.debt.principal * dpr)
        # if already overdue, how much interest has piled up
        overdue_interest = (
            self._q(self.debt.principal * dpr * Decimal(-days_until_due))
            if overdue else Decimal("0.00")
        )

        result = {
            "debt": {
                **asdict(self.debt),
                "principal": str(amt),
                "apr": str(self.debt.apr),
                "due_date": self.debt.due_date.isoformat(),
            },
            "journal": journal,
            "temporal": {
                "as_of": as_of.isoformat(),
                "days_until_due": days_until_due,
                "overdue": overdue,
                "daily_interest_accrual": str(daily_accrual),
                "overdue_interest_accrued": str(overdue_interest),
            },
        }
        self.log.info("registered debt: %s", result)
        return result
