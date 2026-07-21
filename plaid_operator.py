"""
SyncCardScheduleOperator
========================

Airflow-style operator that turns a credit-card liability snapshot (the kind
Plaid's ``/liabilities/get`` returns) into a timeline of **card due-date and
autopay events**, both historical and future.

Motivation
----------
Plaid hands us a single point-in-time view of a card: the *next* payment due
date, the last statement balance, the minimum payment, when the last payment
landed, and whether the account is overdue. What the user actually reasons
about is a *series* of events — "when were the last N due dates, did autopay
cover them, and when are the next N due, and what will autopay pull?".

This operator expands that single snapshot into a monthly series of events
anchored on the reported ``next_due_date`` and the card's statement cycle:

  - ``payment_due``  — the statement's due date each cycle
  - ``autopay``      — if autopay is enabled, the date the autopay draft posts
                       (``lead_days`` before the due date)

Each event is tagged with a ``phase`` (``historical`` / ``due_today`` /
``upcoming``) relative to ``as_of``. Historical due-date events are
reconciled against the reported last-payment info to mark whether they were
paid; the current cycle carries the ``is_overdue`` flag straight from Plaid.

Honesty about what we know
--------------------------
Plaid only gives us real dollar figures for the *current* cycle (its
``last_statement_balance`` / ``minimum_payment``). We do **not** invent
amounts for other cycles: their ``amount`` is ``null`` and ``estimated`` is
``true``. The one exception is a *fixed*-amount autopay, whose draft amount is
known for every cycle. This keeps the emitted schedule truthful — dates are
projected deterministically, dollars are only reported where they're real.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


# ---------- domain types ----------

# Autopay draft basis: what the autopay pulls each cycle.
AUTOPAY_STATEMENT = "statement_balance"   # pay the full statement balance
AUTOPAY_MINIMUM = "minimum"               # pay just the minimum due
AUTOPAY_FIXED = "fixed"                   # pay a fixed dollar amount
_AUTOPAY_KINDS = (AUTOPAY_STATEMENT, AUTOPAY_MINIMUM, AUTOPAY_FIXED)


@dataclass(frozen=True)
class Autopay:
    """Autopay configuration for a card.

    Plaid's liabilities product does not expose autopay settings, so these
    are user-supplied (or inferred elsewhere) and travel alongside the
    liability snapshot.

    - ``enabled``   — whether autopay is on at all
    - ``basis``     — one of ``statement_balance`` / ``minimum`` / ``fixed``
    - ``amount``    — the fixed draft amount; required iff ``basis == fixed``
    - ``lead_days`` — how many days before the due date the draft posts
                      (many issuers post autopay on the due date → 0)
    """

    enabled: bool = False
    basis: str = AUTOPAY_STATEMENT
    amount: Decimal | None = None
    lead_days: int = 0


@dataclass(frozen=True)
class CardAccount:
    """A credit-card liability snapshot, shaped after Plaid's
    ``/liabilities/get`` credit account plus a user-supplied autopay config.

    Only ``id``, ``name`` and ``next_due_date`` are strictly required; the
    rest default to zero/None so a partial Plaid payload still produces a
    usable date timeline.
    """

    id: str
    name: str                                   # e.g. "5th Street Capital"
    next_due_date: date                         # Plaid: next_payment_due_date
    issuer: str = ""
    apr: Decimal = Decimal("0")
    statement_balance: Decimal = Decimal("0")   # Plaid: last_statement_balance
    minimum_payment: Decimal = Decimal("0")     # Plaid: minimum_payment_amount
    grace_period_days: int = 21                  # close -> due offset
    last_payment_date: date | None = None        # Plaid: last_payment_date
    last_payment_amount: Decimal = Decimal("0")  # Plaid: last_payment_amount
    is_overdue: bool = False                      # Plaid: is_overdue
    autopay: Autopay = field(default_factory=Autopay)


# ---------- date helpers ----------

def _add_months(d: date, n: int) -> date:
    """Shift ``d`` by ``n`` calendar months (``n`` may be negative),
    clamping the day to the target month's length (so Jan 31 + 1mo -> Feb 28).
    """
    # zero-based month index math keeps the negative case clean
    idx = (d.year * 12 + (d.month - 1)) + n
    year, month = divmod(idx, 12)
    month += 1
    last = monthrange(year, month)[1]
    return date(year, month, min(d.day, last))


# ---------- the operator ----------

class SyncCardScheduleOperator(BaseOperator):
    """
    Expand a card liability snapshot into a historical+future schedule of
    due-date and autopay events.

    :param card:      the :class:`CardAccount` snapshot
    :param as_of:     valuation date splitting historical from upcoming
                      (defaults to today)
    :param lookback:  how many past cycles to emit (default 6)
    :param lookahead: how many future cycles to emit (default 6)
    """

    template_fields = ()

    def __init__(
        self,
        *,
        card: CardAccount,
        as_of: date | None = None,
        lookback: int = 6,
        lookahead: int = 6,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.card = card
        self.as_of = as_of
        if lookback < 0 or lookahead < 0:
            raise ValueError("lookback and lookahead must be non-negative")
        self.lookback = lookback
        self.lookahead = lookahead

    @staticmethod
    def _q(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # --- phase classification ---

    def _phase(self, d: date, as_of: date) -> str:
        if d < as_of:
            return "historical"
        if d == as_of:
            return "due_today"
        return "upcoming"

    # --- autopay amount resolution ---

    def _autopay_amount(self, seq: int) -> tuple[str | None, bool]:
        """Return ``(amount_str_or_None, estimated)`` for the autopay draft on
        a given cycle. Only the current cycle (seq 0) has real statement/
        minimum figures; a fixed-basis autopay is known for every cycle."""
        ap = self.card.autopay
        if ap.basis == AUTOPAY_FIXED:
            amt = ap.amount if ap.amount is not None else Decimal("0")
            return str(self._q(amt)), False
        if seq == 0:
            if ap.basis == AUTOPAY_MINIMUM:
                return str(self._q(self.card.minimum_payment)), False
            return str(self._q(self.card.statement_balance)), False
        # statement/minimum basis on a non-current cycle → unknown amount
        return None, True

    def _due_amount(self, seq: int) -> tuple[str | None, bool]:
        """Return ``(amount_str_or_None, estimated)`` for a due event. Only
        the current cycle carries a real statement balance."""
        if seq == 0:
            return str(self._q(self.card.statement_balance)), False
        return None, True

    # --- reconciliation of historical due events against last payment ---

    def _was_paid(self, close: date, due: date) -> bool:
        """Best-effort: consider a past cycle paid if the reported last
        payment landed within that cycle's close→due window. Plaid only gives
        us one last-payment datapoint, so this reliably marks at most the most
        recent settled cycle; older cycles fall through to ``paid=False``
        (unknown) rather than being guessed."""
        lp = self.card.last_payment_date
        if lp is None or self.card.last_payment_amount <= 0:
            return False
        return close <= lp <= due

    # --- airflow entrypoint ---

    def execute(self, context: Context) -> dict[str, Any]:
        as_of = self.as_of or date.today()
        card = self.card
        ap = card.autopay

        events: list[dict[str, Any]] = []
        # seq 0 == the cycle whose due date is the reported next_due_date.
        for seq in range(-self.lookback, self.lookahead + 1):
            due = _add_months(card.next_due_date, seq)
            close = due - timedelta(days=card.grace_period_days)

            due_amount, due_estimated = self._due_amount(seq)
            phase = self._phase(due, as_of)
            paid = self._was_paid(close, due) if phase == "historical" else False
            # Only the current cycle can be flagged overdue from Plaid's snapshot.
            overdue = bool(seq == 0 and card.is_overdue)

            events.append({
                "seq": seq,
                "type": "payment_due",
                "date": due.isoformat(),
                "statement_close": close.isoformat(),
                "phase": phase,
                "amount": due_amount,
                "estimated": due_estimated,
                "paid": paid,
                "overdue": overdue,
                "autopay_covered": bool(ap.enabled),
            })

            if ap.enabled:
                post = due - timedelta(days=ap.lead_days)
                amount, estimated = self._autopay_amount(seq)
                ap_phase = self._phase(post, as_of)
                events.append({
                    "seq": seq,
                    "type": "autopay",
                    "date": post.isoformat(),
                    "due_date": due.isoformat(),
                    "phase": ap_phase,
                    "basis": ap.basis,
                    "amount": amount,
                    "estimated": estimated,
                    # A past autopay is assumed to have posted; future ones are
                    # scheduled. This mirrors the due-event paid/unpaid split.
                    "posted": ap_phase == "historical",
                })

        events.sort(key=lambda e: (e["date"], 0 if e["type"] == "autopay" else 1))

        historical_due = [e for e in events
                          if e["type"] == "payment_due" and e["phase"] == "historical"]
        upcoming_due = [e for e in events
                        if e["type"] == "payment_due" and e["phase"] != "historical"]
        next_autopay = next(
            (e["date"] for e in events
             if e["type"] == "autopay" and e["phase"] != "historical"),
            None,
        )

        result = {
            "card": {
                "id": card.id,
                "name": card.name,
                "issuer": card.issuer,
                "apr": str(card.apr),
                "statement_balance": str(self._q(card.statement_balance)),
                "minimum_payment": str(self._q(card.minimum_payment)),
                "next_due_date": card.next_due_date.isoformat(),
                "grace_period_days": card.grace_period_days,
                "last_payment_date": (
                    card.last_payment_date.isoformat()
                    if card.last_payment_date else None
                ),
                "last_payment_amount": str(self._q(card.last_payment_amount)),
                "is_overdue": card.is_overdue,
                "autopay": {
                    "enabled": ap.enabled,
                    "basis": ap.basis,
                    "amount": (str(self._q(ap.amount))
                               if ap.amount is not None else None),
                    "lead_days": ap.lead_days,
                },
            },
            "as_of": as_of.isoformat(),
            "window": {"lookback": self.lookback, "lookahead": self.lookahead},
            "events": events,
            "summary": {
                "historical_due_count": len(historical_due),
                "upcoming_due_count": len(upcoming_due),
                "autopay_enabled": ap.enabled,
                "next_due_date": card.next_due_date.isoformat(),
                "next_autopay_date": next_autopay,
                "is_overdue": card.is_overdue,
            },
        }
        self.log.info("synced card schedule: %s", result["summary"])
        return result
