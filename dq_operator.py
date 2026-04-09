"""
CompareCategoryOperator — DQ operator
=====================================

Given a category like "equity/rh" and a production value fetched from an
external system, compute the delta between what our ledger says we have
in that category and what production says, and flag any drift that
exceeds a configurable threshold.

The operator itself is pure — it takes an already-loaded ledger dict and
an already-fetched production value. Network I/O lives in the one-off
script at scripts/fetch_rh_equity.py, which assembles both sides and
calls this operator to do the actual comparison.

Category format
---------------
  "<kind>[/<label>]"

  "equity"         → all equity entries, any source
  "equity/rh"      → equity entries where source == "rh"
  "debt/IRS"       → debt entries where creditor == "IRS"
  "payment/Coffee" → payment entries where payee == "Coffee"
  "sub/netflix"    → subscription entries where service == "netflix"

The label maps to a kind-appropriate field (source / creditor / payee /
service) because those are the human-meaningful "attribution" fields on
each operator's dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


# Per-kind mapping: where does the record live inside a stored entry,
# which field holds the amount, and which field holds the human label
# used for category filtering (e.g. source for equity, creditor for debt).
_KIND_SCHEMA: dict[str, dict[str, str]] = {
    "equity":  {"record_key": "equity",       "amount_field": "amount",    "label_field": "source"},
    "debt":    {"record_key": "debt",         "amount_field": "principal", "label_field": "creditor"},
    "payment": {"record_key": "payment",      "amount_field": "amount",    "label_field": "payee"},
    "sub":     {"record_key": "subscription", "amount_field": "amount",    "label_field": "service"},
}


@dataclass(frozen=True)
class Category:
    kind: str
    label: str | None      # None → no label filter, match the whole kind

    @classmethod
    def parse(cls, raw: str) -> "Category":
        if "/" in raw:
            kind, label = raw.split("/", 1)
            return cls(kind=kind.strip(), label=label.strip() or None)
        return cls(kind=raw.strip(), label=None)

    def __str__(self) -> str:
        return f"{self.kind}/{self.label}" if self.label else self.kind


class CompareCategoryOperator(BaseOperator):
    """
    Compare a category's aggregated amount in the local ledger against an
    authoritative value from an external system.

    :param category: category spec, e.g. "equity/rh"
    :param ledger:   dict returned by LedgerStore.all() or GET /ledger
    :param prod_value: the external authoritative amount, as Decimal
    :param threshold_pct: fractional drift allowed before we flag.
                          0.05 = 5% either direction; defaults to 0.05.
                          Drift > 2*threshold is flagged severe_drift.
    """

    def __init__(
        self,
        *,
        category: str,
        ledger: dict[str, dict[str, dict[str, Any]]],
        prod_value: Decimal,
        threshold_pct: Decimal = Decimal("0.05"),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.category = category
        self.ledger = ledger
        self.prod_value = prod_value
        self.threshold_pct = threshold_pct

    # ---- helpers ----

    @staticmethod
    def _q(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _aggregate(self) -> tuple[Decimal, int, list[str]]:
        """Filter ledger entries matching self.category and return
        (total, count, matched_ids)."""
        cat = Category.parse(self.category)
        if cat.kind not in _KIND_SCHEMA:
            raise ValueError(
                f"unknown category kind {cat.kind!r} — "
                f"expected one of {list(_KIND_SCHEMA)}"
            )
        schema = _KIND_SCHEMA[cat.kind]
        entries = self.ledger.get(cat.kind, {}) or {}

        total = Decimal("0")
        count = 0
        matched: list[str] = []
        for entry_id, entry in entries.items():
            record = entry.get(schema["record_key"])
            if not isinstance(record, dict):
                continue
            if cat.label is not None:
                if record.get(schema["label_field"]) != cat.label:
                    continue
            amt_raw = record.get(schema["amount_field"])
            if amt_raw is None:
                continue
            try:
                total += Decimal(str(amt_raw))
            except (ValueError, ArithmeticError):
                continue
            count += 1
            matched.append(entry_id)
        return total, count, matched

    # ---- airflow entrypoint ----

    def execute(self, context: Context) -> dict[str, Any]:
        ledger_amount, match_count, matched_ids = self._aggregate()
        prod = self.prod_value
        delta = prod - ledger_amount

        if prod != 0:
            delta_pct = (delta / prod)
        elif ledger_amount != 0:
            # prod=0 but ledger nonzero → infinite relative drift; caller
            # should read this as "everything in the ledger is untracked in prod"
            delta_pct = None
        else:
            # both zero — perfect match by definition
            delta_pct = Decimal("0")

        if delta_pct is None:
            within = False
            flag = "severe_drift"
        else:
            abs_pct = abs(delta_pct)
            within = abs_pct <= self.threshold_pct
            if within:
                flag = "match"
            elif abs_pct > self.threshold_pct * Decimal("2"):
                flag = "severe_drift"
            else:
                flag = "drift"

        result = {
            "category": self.category,
            "match_count": match_count,
            "matched_ids": matched_ids,
            "ledger_amount": str(self._q(ledger_amount)),
            "prod_amount": str(self._q(prod)),
            "delta": str(self._q(delta)),
            "delta_pct": (
                f"{delta_pct.quantize(Decimal('0.0001'))}"
                if delta_pct is not None else None
            ),
            "threshold_pct": str(self.threshold_pct),
            "within_threshold": within,
            "flag": flag,
        }
        self.log.info("dq comparison: %s", result)
        return result
