"""
AddCreditScoreOperator
======================

Records a credit score observation on the ledger. Phase 1 of the credit
integration (see issue #4): a manually-entered score the user reads from
their Credit Karma / bureau app once a month. No external API call —
later phases may automate the pull via Plaid Liabilities or Experian
Connect.

A credit score is not money, so this operator produces an empty journal.
The normalized record carries a derived ``band`` so the frontend doesn't
have to re-implement the range buckets.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

# The three US nationwide consumer reporting agencies. A score is always
# tied to exactly one — they each hold a separate file and can differ.
BUREAUS = ("experian", "equifax", "transunion")

# FICO and VantageScore (3.0 and 4.0) both use the 300-850 range, so a
# single bound covers every model we expect. Score bands below are the
# conventional lender buckets shared by both scoring families.
SCORE_MIN = 300
SCORE_MAX = 850

_BANDS = (
    (580, "poor"),         # 300-579
    (670, "fair"),         # 580-669
    (740, "good"),         # 670-739
    (800, "very_good"),    # 740-799
    (SCORE_MAX + 1, "excellent"),  # 800-850
)


def score_band(score: int) -> str:
    for ceiling, label in _BANDS:
        if score < ceiling:
            return label
    return "excellent"


@dataclass(frozen=True)
class CreditScore:
    id: str
    bureau: str            # one of BUREAUS
    score: int
    model: str             # e.g. "vantage_4.0", "fico_8"
    as_of: date
    memo: str = ""


class AddCreditScoreOperator(BaseOperator):
    """
    Record a credit score observation. Produces no journal lines — a
    score is informational, not a balance, and must not feed net-worth
    math.
    """

    def __init__(self, *, credit_score: CreditScore, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.credit_score = credit_score

    def execute(self, context: Context) -> dict[str, Any]:
        cs = self.credit_score

        bureau = cs.bureau.strip().lower()
        if bureau not in BUREAUS:
            raise ValueError(
                f"unknown bureau {cs.bureau!r}; "
                f"expected one of {sorted(BUREAUS)}"
            )

        if not isinstance(cs.score, int):
            raise ValueError(f"score must be an integer, got {cs.score!r}")
        if not SCORE_MIN <= cs.score <= SCORE_MAX:
            raise ValueError(
                f"score {cs.score} out of range "
                f"[{SCORE_MIN}, {SCORE_MAX}]"
            )

        if not cs.model.strip():
            raise ValueError("model is required (e.g. 'fico_8', 'vantage_4.0')")

        result = {
            "credit_score": {
                **asdict(cs),
                "bureau": bureau,
                "as_of": cs.as_of.isoformat(),
                "band": score_band(cs.score),
            },
            # A score has no double-entry effect. The key is kept so the
            # record matches the shape of every other ledger entry.
            "journal": [],
        }
        self.log.info("recorded credit score: %s", result["credit_score"])
        return result
