#!/usr/bin/env bash
# Seed the theta ledger with the line items from the March + April 2026
# Capital One 360 statements so a human can verify against the PDFs while
# the iterator/handler print events live.
#
# IDs are stable (statement-period prefix + slugged source) so re-running
# this script produces UPDATEs rather than duplicate NEWs — useful for
# exercising the handler's diff path. Pass CLEAR to delete everything we
# seeded and exercise the DELETE path.
#
# Field names per kind (from openapi_spec.py):
#   payment : payee, amount, card_id, txn_date  [+ memo, expense_account]
#   debt    : creditor, principal, due_date     [+ apr, as_of, offset_account, memo]
#   equity  : source, amount, received_on       [+ target_bucket, memo]
#   sub     : service, amount, frequency, next_charge_date, funding_card_id
#                                               [+ horizon, memo]
set -euo pipefail

BASE="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"
# Use the wallet's seeded "default" card — `api.py` always provisions one
# so this script doesn't depend on a WALLET_PATH file. Override here if
# you've populated a real wallet and want richer per-card journals.
CARD_CHECKING="${LEDGER_CARD_ID:-default}"

put_payment() {
  local id="$1" payee="$2" amount="$3" txn_date="$4"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"payee\":\"$payee\",\"amount\":\"$amount\",\"card_id\":\"$CARD_CHECKING\",\"txn_date\":\"$txn_date\"}" \
    "$BASE/ledger/payment/$id" >/dev/null
  printf "  PUT  payment  %-32s  %10s  %s\n" "$id" "$amount" "$payee"
}

put_debt() {
  local id="$1" creditor="$2" principal="$3" due_date="$4"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"creditor\":\"$creditor\",\"principal\":\"$principal\",\"due_date\":\"$due_date\"}" \
    "$BASE/ledger/debt/$id" >/dev/null
  printf "  PUT  debt     %-32s  %10s  %s\n" "$id" "$principal" "$creditor"
}

put_sub() {
  local id="$1" service="$2" amount="$3" next_date="$4"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"service\":\"$service\",\"amount\":\"$amount\",\"frequency\":\"monthly\",\"next_charge_date\":\"$next_date\",\"funding_card_id\":\"$CARD_CHECKING\"}" \
    "$BASE/ledger/sub/$id" >/dev/null
  printf "  PUT  sub      %-32s  %10s  %s\n" "$id" "$amount" "$service"
}

del() {
  local kind="$1" id="$2"
  curl -sf -X DELETE "$BASE/ledger/$kind/$id" >/dev/null || true
  printf "  DEL  %-8s %s\n" "$kind" "$id"
}

if [[ "${1:-}" == "CLEAR" ]]; then
  echo "Clearing seeded entries..."
  for kind in payment debt sub equity; do
    curl -s "$BASE/ledger" \
      | python3 -c "import sys,json; d=json.load(sys.stdin).get('$kind',{}); print('\n'.join(d.keys()))" \
      | while read -r id; do
          [[ -z "$id" ]] && continue
          del "$kind" "$id"
        done
  done
  exit 0
fi

echo "=== March 2026 — Capital One 360 (5718 + 6779) ==="
# Inflows (treated as payments with the employer/agency as payee — direction
# is captured by the source doc; the ledger doesn't model sign per kind).
put_payment 202603-payroll-2026-03-26  "PROFESSIONAL SEARCH GROUP PAYROLL"  2213.31  2026-03-26
put_payment 202603-uidd-2026-03-16-a   "NYS DOL UI DD"                       869.00  2026-03-16
put_payment 202603-uidd-2026-03-16-b   "NYS DOL UI DD"                       869.00  2026-03-16
put_payment 202603-uidd-2026-03-16-c   "NYS DOL UI DD"                       869.00  2026-03-16
put_payment 202603-uidd-2026-03-23     "NYS DOL UI DD"                       869.00  2026-03-23
# Outflows that map to other domain lines
put_sub     202603-anthropic-2026-03-30-a  "Anthropic"             5.44  2026-04-30
put_sub     202603-anthropic-2026-03-30-b  "Anthropic"             7.62  2026-04-30
put_sub     202603-hellointerview-30-a     "HelloInterview"      160.65  2026-04-30
put_sub     202603-hellointerview-30-b     "HelloInterview"      160.65  2026-04-30
put_sub     202603-hellointerview-30-c     "HelloInterview"      457.85  2026-04-30
put_debt    202603-applecard-2026-03-30    "Apple Card (GS Bank)"   74.99  2026-04-30
# Generic outflow payments
put_payment 202603-zelle-vincent-2026-03-25  "Zelle: Vincent On"   66.25  2026-03-25
put_payment 202603-zelle-neo-2026-03-26      "Zelle: Neo Kairos"  350.00  2026-03-26
put_payment 202603-coinbase-2026-03-18       "Coinbase"           150.00  2026-03-18

echo
echo "=== April 2026 — Capital One 360 (5718 + 6779) ==="
put_debt    202604-bilt-2026-04-01         "Bilt Card (Housing)"  3949.00  2026-05-01
put_debt    202604-amex-2026-04-20         "AMEX EPAYMENT"        1342.40  2026-05-20
put_payment 202604-payroll-2026-04-01      "PROFESSIONAL SEARCH GROUP PAYROLL"  2213.31  2026-04-01
put_payment 202604-payroll-2026-04-08      "PROFESSIONAL SEARCH GROUP PAYROLL"  2213.31  2026-04-08
put_payment 202604-payroll-2026-04-15      "PROFESSIONAL SEARCH GROUP PAYROLL"  2213.31  2026-04-15
put_payment 202604-payroll-2026-04-22      "PROFESSIONAL SEARCH GROUP PAYROLL"  2213.31  2026-04-22
put_payment 202604-payroll-2026-04-29      "PROFESSIONAL SEARCH GROUP PAYROLL"  2213.31  2026-04-29
put_payment 202604-zelle-jacob-2026-04-05-a  "Zelle: Jacob Nelsen-Epstein"   73.00  2026-04-05
put_payment 202604-zelle-jacob-2026-04-05-b  "Zelle: Jacob Nelsen-Epstein"   62.00  2026-04-05
put_payment 202604-zelle-yufei-2026-04-06    "Zelle: Yufei Guan"            340.00  2026-04-06
put_payment 202604-zelle-thach-2026-04-19    "Zelle: Thach Bui"             435.00  2026-04-19
put_payment 202604-mta-2026-04-12            "MTA NYCT PAYGO"                 3.00  2026-04-12
put_payment 202604-amc-2026-04-23            "AMC 9640 ONLINE"               10.24  2026-04-23

echo
echo "Done. Inspect with:"
echo "  curl -s $BASE/ledger | python3 -m json.tool | head -60"
