"""
Topline liquid + net worth calculator
=====================================

Pulls the full ledger snapshot from a running theta API and prints two
numbers a human can sanity-check at a glance:

    LIQUID    = sum(equity where bucket in {cash, savings})
              - sum(debt where due_date <= today + 30 days)
              # i.e. what you could turn into spendable cash within a
              # month after settling near-term liabilities.

    NET WORTH = sum(equity)             # all assets, liquid + illiquid
              - sum(debt principal)     # all liabilities (current + long)

Only ``equity`` and ``debt`` entries are considered. ``payment`` and
``sub`` records are flows, not positions, and would double-count if
included.

Run:
    python3 scripts/topline.py [--base-url URL] [--as-of YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import urllib.request
from decimal import Decimal
from typing import Any

LIQUID_BUCKETS = {"cash", "savings"}
NEAR_TERM_DAYS = 30


def fetch_ledger(base_url: str) -> dict[str, dict[str, Any]]:
    with urllib.request.urlopen(f"{base_url}/ledger", timeout=10) as resp:
        return json.loads(resp.read())


def _equity_amount(entry: dict) -> Decimal:
    eq = entry.get("equity") or {}
    return Decimal(str(eq.get("amount") or "0"))


def _equity_bucket(entry: dict) -> str:
    return (entry.get("equity") or {}).get("target_bucket") or "uncategorized"


def _debt_principal(entry: dict) -> Decimal:
    d = entry.get("debt") or {}
    return Decimal(str(d.get("principal") or "0"))


def _debt_due(entry: dict) -> dt.date | None:
    d = entry.get("debt") or {}
    raw = d.get("due_date")
    return dt.date.fromisoformat(raw) if raw else None


def compute(ledger: dict, as_of: dt.date) -> dict:
    equity_entries = list(ledger.get("equity", {}).values())
    debt_entries = list(ledger.get("debt", {}).values())

    by_bucket: dict[str, Decimal] = {}
    for e in equity_entries:
        by_bucket[_equity_bucket(e)] = by_bucket.get(_equity_bucket(e), Decimal("0")) + _equity_amount(e)

    total_assets = sum(by_bucket.values(), Decimal("0"))
    total_debt = sum((_debt_principal(d) for d in debt_entries), Decimal("0"))

    near_term_cutoff = as_of + dt.timedelta(days=NEAR_TERM_DAYS)
    near_term_debt = sum(
        (_debt_principal(d) for d in debt_entries
         if (_debt_due(d) is not None and _debt_due(d) <= near_term_cutoff)),
        Decimal("0"),
    )
    liquid_assets = sum(
        (v for k, v in by_bucket.items() if k in LIQUID_BUCKETS),
        Decimal("0"),
    )

    return {
        "as_of": as_of.isoformat(),
        "by_bucket": {k: str(v) for k, v in by_bucket.items()},
        "total_assets": str(total_assets),
        "total_debt": str(total_debt),
        "near_term_debt": str(near_term_debt),
        "liquid": str(liquid_assets - near_term_debt),
        "net_worth": str(total_assets - total_debt),
        "counts": {"equity": len(equity_entries), "debt": len(debt_entries)},
    }


def _money(s: str) -> str:
    return f"${Decimal(s):>14,.2f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8765")
    ap.add_argument("--as-of", default=dt.date.today().isoformat(),
                    help="valuation date for near-term debt cutoff")
    ap.add_argument("--json", action="store_true", help="emit JSON only")
    args = ap.parse_args()

    ledger = fetch_ledger(args.base_url)
    result = compute(ledger, dt.date.fromisoformat(args.as_of))

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    print(f"=== Topline as of {result['as_of']} ===")
    print(f"  equity entries: {result['counts']['equity']}")
    print(f"  debt entries:   {result['counts']['debt']}")
    print()
    print("  Assets by bucket:")
    for bucket, amount in sorted(result["by_bucket"].items()):
        tag = " (liquid)" if bucket in LIQUID_BUCKETS else ""
        print(f"    {bucket:14}  {_money(amount)}{tag}")
    print(f"    {'TOTAL':14}  {_money(result['total_assets'])}")
    print()
    print("  Liabilities:")
    print(f"    {'all debt':14}  {_money(result['total_debt'])}")
    print(f"    {'within 30d':14}  {_money(result['near_term_debt'])}")
    print()
    print(f"  LIQUID     = {_money(result['liquid'])}   (cash+savings − near-term debt)")
    print(f"  NET WORTH  = {_money(result['net_worth'])}   (all assets − all debt)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
