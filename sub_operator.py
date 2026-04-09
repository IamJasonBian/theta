"""
AddSubscriptionOperator
=======================

Registers a recurring service (subscription) on the ledger. Produces one
journal entry representing the upcoming charge plus a forward-looking
schedule of the next N charge dates and the annualized cost.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Literal

from airflow.models import BaseOperator
from airflow.utils.context import Context


Frequency = Literal["weekly", "monthly", "yearly"]


@dataclass(frozen=True)
class Subscription:
    id: str
    service: str                    # "netflix", "spotify", "aws"
    amount: Decimal                 # per-charge amount
    frequency: Frequency
    next_charge_date: date
    funding_card_id: str            # references a Card in the wallet
    memo: str = ""


# Reuse the Card type from payment_operator to share the wallet model.
from payment_operator import Card  # noqa: E402


class AddSubscriptionOperator(BaseOperator):
    """
    Register a recurring subscription:
      Dr Expenses:Subscriptions:<service>
      Cr Liabilities:CreditCard:<card>   (credit funding)
        or
      Cr Assets:Bank:<card>              (debit funding)

    :param subscription: the Subscription record
    :param cards: wallet keyed by card_id (to resolve funding side)
    :param horizon: number of future charge dates to project (default 12)
    """

    def __init__(
        self,
        *,
        subscription: Subscription,
        cards: dict[str, Card],
        horizon: int = 12,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.subscription = subscription
        self.cards = cards
        self.horizon = horizon

    @staticmethod
    def _q(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _step(self, d: date) -> date:
        if self.subscription.frequency == "weekly":
            return d + timedelta(days=7)
        if self.subscription.frequency == "monthly":
            # simple month step: roll the month, clamp day to month length
            from calendar import monthrange
            y, m = d.year, d.month
            if m == 12:
                y, m = y + 1, 1
            else:
                m += 1
            last = monthrange(y, m)[1]
            return date(y, m, min(d.day, last))
        if self.subscription.frequency == "yearly":
            try:
                return d.replace(year=d.year + 1)
            except ValueError:
                # Feb 29 -> Feb 28 in non-leap year
                return d.replace(year=d.year + 1, day=28)
        raise ValueError(f"unknown frequency: {self.subscription.frequency}")

    def _annualized(self) -> Decimal:
        per = self.subscription.amount
        if self.subscription.frequency == "weekly":
            return self._q(per * Decimal("52"))
        if self.subscription.frequency == "monthly":
            return self._q(per * Decimal("12"))
        if self.subscription.frequency == "yearly":
            return self._q(per)
        raise ValueError(self.subscription.frequency)

    def execute(self, context: Context) -> dict[str, Any]:
        if self.subscription.funding_card_id not in self.cards:
            raise ValueError(
                f"subscription references unknown card_id="
                f"{self.subscription.funding_card_id!r}"
            )
        card = self.cards[self.subscription.funding_card_id]

        amt = self._q(self.subscription.amount)
        counter = (
            f"Liabilities:CreditCard:{card.name}"
            if card.kind == "credit"
            else f"Assets:Bank:{card.name}"
        )
        journal = [
            {"account": f"Expenses:Subscriptions:{self.subscription.service}",
             "debit": str(amt), "credit": "0.00"},
            {"account": counter,
             "debit": "0.00", "credit": str(amt)},
        ]

        # forward schedule
        schedule: list[str] = []
        cursor = self.subscription.next_charge_date
        for _ in range(self.horizon):
            schedule.append(cursor.isoformat())
            cursor = self._step(cursor)

        result = {
            "subscription": {
                **asdict(self.subscription),
                "amount": str(amt),
                "next_charge_date": self.subscription.next_charge_date.isoformat(),
            },
            "funding_card": {
                "id": card.id, "name": card.name, "kind": card.kind,
            },
            "journal": journal,
            "schedule": {
                "horizon": self.horizon,
                "dates": schedule,
                "annualized_cost": str(self._annualized()),
            },
        }
        self.log.info("registered subscription: %s", result)
        return result
