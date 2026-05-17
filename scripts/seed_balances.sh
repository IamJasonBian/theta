#!/usr/bin/env bash
# Seed end-of-period balance snapshots so the topline calculator has
# something to sum. These are NOT transactional flows — they are
# point-in-time positions pulled from the source statements:
#
#   - Capital One 360 — closing balances Apr 30, 2026
#   - Robinhood Individual Account #110916194 — Apr 30, 2026
#
# Equity = positive asset balance. Debt = liability (incl. margin).
# `target_bucket` tags assets as cash / savings / brokerage so the
# topline calc can split liquid vs. illiquid without re-parsing names.
set -euo pipefail

BASE="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"
AS_OF="2026-04-30"

put_equity() {
  local id="$1" source="$2" amount="$3" bucket="$4"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"source\":\"$source\",\"amount\":\"$amount\",\"received_on\":\"$AS_OF\",\"target_bucket\":\"$bucket\"}" \
    "$BASE/ledger/equity/$id" >/dev/null
  printf "  PUT  equity   %-32s  %14s  bucket=%s\n" "$id" "$amount" "$bucket"
}

put_debt() {
  local id="$1" creditor="$2" principal="$3" due="$4"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"creditor\":\"$creditor\",\"principal\":\"$principal\",\"due_date\":\"$due\"}" \
    "$BASE/ledger/debt/$id" >/dev/null
  printf "  PUT  debt     %-32s  %14s  due=%s\n" "$id" "$principal" "$due"
}

echo "=== Capital One 360 closing balances (Apr 30, 2026) ==="
put_equity bal-capone-checking-5718-2026-04-30  "Capital One 360 Checking 5718"   3293.04  cash
put_equity bal-capone-savings-6779-2026-04-30   "Capital One 360 Savings 6779"    4622.64  savings

echo
echo "=== Robinhood Individual Account #110916194 (Apr 30, 2026) ==="
put_equity bal-robinhood-securities-2026-04-30  "Robinhood securities (margin)" 158423.55  brokerage
# Brokerage Cash Balance = ($72,232.96) — i.e. margin loan owed to RH.
# Modeled as a debt so the liabilities side stays sign-positive.
put_debt   bal-robinhood-margin-2026-04-30      "Robinhood Margin"               72232.96  2026-12-31

echo
echo "Done. Compute topline with:"
echo "  python3 /Users/jasonzb/Desktop/apollo/gamma/theta/scripts/topline.py"
