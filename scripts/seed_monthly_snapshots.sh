#!/usr/bin/env bash
# Seed 12 monthly balance snapshots × 2 scenarios for FY2026 so the
# topline chart at /topline has data to render. Anchored on the real
# Apr 30, 2026 closing balances:
#   Capone Checking 5718:    3,293.04 (cash)
#   Capone Savings 6779:     4,622.64 (savings)
#   Robinhood securities:  158,423.55 (brokerage)
#   Robinhood margin:       72,232.96 (debt, due 2026-12-31)
#
# Two scenarios are written:
#   base    — flat hold at April values for every month of FY2026
#   stretch — securities grow ~2%/mo, cash drifts down ~5%/mo, margin paid
#             down 5%/mo. (Toy numbers for demoing the scenario filter.)
#
# Scenario tag rides on the `memo` field of every entry. The chart reads
# memo to populate its Scenario dropdown — empty memo defaults to "base".
set -euo pipefail

BASE_URL="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"
YEAR="${YEAR:-2026}"

# Anchor balances (Apr 30, 2026 from the source statements).
A_CASH=3293.04
A_SAV=4622.64
A_SEC=158423.55
A_MGN=72232.96

put_equity() {
  local id="$1" source="$2" amount="$3" date="$4" bucket="$5" memo="$6"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"source\":\"$source\",\"amount\":\"$amount\",\"received_on\":\"$date\",\"target_bucket\":\"$bucket\",\"memo\":\"$memo\"}" \
    "$BASE_URL/ledger/equity/$id" >/dev/null
}

put_debt() {
  local id="$1" creditor="$2" principal="$3" due="$4" memo="$5"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"creditor\":\"$creditor\",\"principal\":\"$principal\",\"due_date\":\"$due\",\"memo\":\"$memo\"}" \
    "$BASE_URL/ledger/debt/$id" >/dev/null
}

# month -> last day of that month. February uses 28; chart only cares
# about month-of-`received_on`, not the day.
last_day() {
  case "$1" in
    01|03|05|07|08|10|12) echo 31 ;;
    04|06|09|11) echo 30 ;;
    02) echo 28 ;;
  esac
}

# Geometric step. Usage: step BASE FACTOR N_STEPS
# Returns BASE * FACTOR^N_STEPS to 2dp, computed in pure bash via python.
step() {
  python3 -c "print(f'{$1 * ($2 ** $3):.2f}')"
}

echo "Seeding monthly snapshots for FY${YEAR}..."
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  d=$(last_day "$m")
  date="${YEAR}-${m}-${d}"
  i=$((10#$m - 1))   # 0..11 step index from Jan

  # ---- base: flat hold ----
  put_equity "snap-base-checking-${YEAR}-${m}"   "Capital One 360 Checking 5718"  "$A_CASH" "$date" cash       base
  put_equity "snap-base-savings-${YEAR}-${m}"    "Capital One 360 Savings 6779"   "$A_SAV"  "$date" savings    base
  put_equity "snap-base-securities-${YEAR}-${m}" "Robinhood securities (margin)"  "$A_SEC"  "$date" brokerage  base
  put_debt   "snap-base-margin-${YEAR}-${m}"     "Robinhood Margin"               "$A_MGN"  "${YEAR}-12-31" base

  # ---- stretch: gentle growth + paydown ----
  s_sec=$(step "$A_SEC" 1.02 "$i")
  s_cash=$(step "$A_CASH" 0.95 "$i")
  s_sav=$(step "$A_SAV" 1.01 "$i")
  s_mgn=$(step "$A_MGN" 0.95 "$i")
  put_equity "snap-stretch-checking-${YEAR}-${m}"   "Capital One 360 Checking 5718"  "$s_cash" "$date" cash      stretch
  put_equity "snap-stretch-savings-${YEAR}-${m}"    "Capital One 360 Savings 6779"   "$s_sav"  "$date" savings   stretch
  put_equity "snap-stretch-securities-${YEAR}-${m}" "Robinhood securities (margin)"  "$s_sec"  "$date" brokerage stretch
  put_debt   "snap-stretch-margin-${YEAR}-${m}"     "Robinhood Margin"               "$s_mgn"  "${YEAR}-12-31" stretch

  printf "  %s  base=cash$A_CASH/sec$A_SEC  stretch=cash$s_cash/sec$s_sec/mgn$s_mgn\n" "$date"
done

echo
echo "Done. Open the chart:"
echo "  open ${BASE_URL}/topline"
