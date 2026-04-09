"""
NormalizePaymentOperator
========================

Airflow operator that takes a single payment and codifies it as a double-entry
journal *while resolving its temporal dependencies against the source card's
billing cycle*.

The interest impact of a payment is a function of timing, not just amount:

  - Did the payment land before or after statement close?
  - Does it fall within the grace period of the prior statement?
  - How many days of the current cycle will the new charge accrue interest for
    if the next statement isn't paid in full?
  - When is the next due date the user must hit to avoid interest entirely?

This operator answers all of those and pushes the enriched record to XCom so
downstream tasks (ledger writers, reminders, payoff planners) can act on it.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


# ---------- domain types ----------

@dataclass(frozen=True)
class Card:
    id: str
    name: str
    kind: str                       # "credit" | "debit"
    apr: Decimal                    # e.g. Decimal("0.2199")
    balance: Decimal                # current running balance (post last statement + new charges)
    # temporal anchors — only meaningful for credit cards
    statement_close_day: int | None = None   # day-of-month statement closes
    grace_period_days: int = 21               # days from close to due
    last_statement_balance: Decimal = Decimal("0.00")
    last_statement_close: date | None = None  # most recent close that produced last_statement_balance
    credit_limit: Decimal | None = None


@dataclass(frozen=True)
class Payment:
    payee: str
    amount: Decimal
    card_id: str
    txn_date: date
    memo: str = ""


@dataclass(frozen=True)
class JournalLine:
    account: str
    debit: Decimal
    credit: Decimal


# ---------- the operator ----------

class NormalizePaymentOperator(BaseOperator):
    """
    Normalize a payment into:
      1. a balanced double-entry journal
      2. an enriched card snapshot
      3. a temporal-dependency report tying the payment to the card's cycle

    :param payment: the Payment being made
    :param cards:   wallet keyed by card_id
    :param expense_account: account name to debit (default Expenses:Uncategorized)
    """

    template_fields = ("expense_account",)

    def __init__(
        self,
        *,
        payment: Payment,
        cards: dict[str, Card],
        expense_account: str = "Expenses:Uncategorized",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.payment = payment
        self.cards = cards
        self.expense_account = expense_account

    # --- money helpers ---

    @staticmethod
    def _q(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # --- temporal helpers ---

    @staticmethod
    def _next_close_after(d: date, close_day: int) -> date:
        """Return the next statement-close date on/after d."""
        # clamp close_day to month length
        from calendar import monthrange
        year, month = d.year, d.month
        last = monthrange(year, month)[1]
        candidate = date(year, month, min(close_day, last))
        if candidate >= d:
            return candidate
        # roll to next month
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
        last = monthrange(year, month)[1]
        return date(year, month, min(close_day, last))

    def _temporal_report(self, card: Card) -> dict[str, Any]:
        """
        Build the temporal-dependency view linking this payment to the card's
        billing cycle. Empty/cash-flavored for debit cards.
        """
        if card.kind != "credit" or card.statement_close_day is None:
            return {"applicable": False, "reason": "non-credit or no cycle defined"}

        txn = self.payment.txn_date
        next_close = self._next_close_after(txn, card.statement_close_day)
        next_due = next_close + timedelta(days=card.grace_period_days)

        # days the new charge will sit on the books before the next statement closes
        days_until_close = (next_close - txn).days

        # if a previous statement exists, are we still inside its grace period?
        in_prior_grace = False
        prior_due: date | None = None
        if card.last_statement_close is not None:
            prior_due = card.last_statement_close + timedelta(days=card.grace_period_days)
            in_prior_grace = card.last_statement_close <= txn <= prior_due

        # daily periodic rate
        dpr = card.apr / Decimal("365")

        # If the user does NOT pay the next statement in full, the new charge
        # will accrue interest from txn_date forward at dpr. Estimate the
        # interest cost on this single charge over the cycle-to-due window.
        days_to_next_due = (next_due - txn).days
        projected_interest_on_this_charge = self._q(
            self.payment.amount * dpr * Decimal(days_to_next_due)
        )

        # Interest already ticking on the carried balance, per day, today.
        carried_daily_interest = self._q(card.last_statement_balance * dpr)

        return {
            "applicable": True,
            "txn_date": txn.isoformat(),
            "next_statement_close": next_close.isoformat(),
            "next_payment_due": next_due.isoformat(),
            "days_until_next_close": days_until_close,
            "days_until_next_due": days_to_next_due,
            "in_prior_statement_grace_period": in_prior_grace,
            "prior_due_date": prior_due.isoformat() if prior_due else None,
            "daily_periodic_rate": str(dpr),
            "carried_balance_daily_interest": str(carried_daily_interest),
            "projected_interest_on_this_charge_if_unpaid_by_due": str(
                projected_interest_on_this_charge
            ),
            "pay_in_full_by_to_avoid_interest": next_due.isoformat(),
        }

    # --- journal ---

    def _journal(self, card: Card) -> list[JournalLine]:
        amt = self._q(self.payment.amount)
        if card.kind == "credit":
            counter = f"Liabilities:CreditCard:{card.name}"
        else:
            counter = f"Assets:Bank:{card.name}"
        return [
            JournalLine(self.expense_account, debit=amt, credit=Decimal("0.00")),
            JournalLine(counter, debit=Decimal("0.00"), credit=amt),
        ]

    # --- airflow entrypoint ---

    def execute(self, context: Context) -> dict[str, Any]:
        if self.payment.card_id not in self.cards:
            raise ValueError(
                f"payment references unknown card_id={self.payment.card_id!r}"
            )
        card = self.cards[self.payment.card_id]

        journal = self._journal(card)
        total_dr = sum((l.debit for l in journal), Decimal("0.00"))
        total_cr = sum((l.credit for l in journal), Decimal("0.00"))
        if total_dr != total_cr:
            raise ValueError(f"unbalanced journal: dr={total_dr} cr={total_cr}")

        result = {
            "payment": {
                **asdict(self.payment),
                "amount": str(self._q(self.payment.amount)),
                "txn_date": self.payment.txn_date.isoformat(),
            },
            "card": {
                "id": card.id,
                "name": card.name,
                "kind": card.kind,
                "apr": str(card.apr),
                "balance": str(self._q(card.balance)),
                "last_statement_balance": str(self._q(card.last_statement_balance)),
                "credit_limit": (
                    str(self._q(card.credit_limit))
                    if card.credit_limit is not None else None
                ),
            },
            "journal": [
                {"account": l.account,
                 "debit": str(l.debit),
                 "credit": str(l.credit)}
                for l in journal
            ],
            "temporal": self._temporal_report(card),
        }

        self.log.info("normalized payment: %s", result)
        return result
