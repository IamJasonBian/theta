"""
AddEquityOperator
=================

Registers capital available to deploy on the ledger.

Journal:
  Dr Assets:Cash:Deployable
  Cr Equity:Contributed:<source>
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


@dataclass(frozen=True)
class Equity:
    id: str
    source: str                 # e.g. "bonus", "savings", "brokerage-sweep"
    amount: Decimal
    received_on: date
    target_bucket: str | None = None   # optional earmark, e.g. "crypto", "sp500"
    memo: str = ""


class AddEquityOperator(BaseOperator):
    """
    Register deployable capital:
      Dr Assets:Cash:Deployable[:<target_bucket>]
      Cr Equity:Contributed:<source>
    """

    def __init__(
        self,
        *,
        equity: Equity,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.equity = equity

    @staticmethod
    def _q(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def execute(self, context: Context) -> dict[str, Any]:
        amt = self._q(self.equity.amount)
        asset_account = "Assets:Cash:Deployable"
        if self.equity.target_bucket:
            asset_account = f"{asset_account}:{self.equity.target_bucket}"
        journal = [
            {"account": asset_account,
             "debit": str(amt), "credit": "0.00"},
            {"account": f"Equity:Contributed:{self.equity.source}",
             "debit": "0.00", "credit": str(amt)},
        ]

        result = {
            "equity": {
                **asdict(self.equity),
                "amount": str(amt),
                "received_on": self.equity.received_on.isoformat(),
            },
            "journal": journal,
        }
        self.log.info("registered equity: %s", result)
        return result
