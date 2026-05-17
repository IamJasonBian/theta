#!/usr/bin/env bash
# Seed a few monthly credit-score observations so the credit_score kind
# has something to show in the dashboard. Phase 1 of the credit
# integration (issue #4) is manual entry — these are sample values, not
# pulled from any bureau.
#
# IDs are stable (cs-<bureau>-<YYYY-MM>) so re-running produces UPDATEs,
# not duplicates. Pass CLEAR to delete everything this script seeded.
set -euo pipefail

BASE="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"

put() {
  local id="$1" bureau="$2" score="$3" model="$4" as_of="$5"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"bureau\":\"$bureau\",\"score\":$score,\"model\":\"$model\",\"as_of\":\"$as_of\"}" \
    "$BASE/ledger/credit_score/$id" >/dev/null
  printf "  PUT  credit_score  %-22s  %s  %s\n" "$id" "$score" "$bureau"
}

if [[ "${1:-}" == "CLEAR" ]]; then
  echo "Clearing seeded credit scores..."
  curl -s "$BASE/ledger" \
    | python3 -c "import sys,json; print('\n'.join(json.load(sys.stdin).get('credit_score',{}).keys()))" \
    | while read -r id; do
        [[ -z "$id" ]] && continue
        curl -sf -X DELETE "$BASE/ledger/credit_score/$id" >/dev/null || true
        echo "  DEL  credit_score  $id"
      done
  exit 0
fi

echo "=== Experian VantageScore 4.0 — monthly observations ==="
put cs-experian-2026-02  experian  771  vantage_4.0  2026-02-28
put cs-experian-2026-03  experian  776  vantage_4.0  2026-03-31
put cs-experian-2026-04  experian  784  vantage_4.0  2026-04-30

echo
echo "Done. View: ${BASE}/  (credit tab)  or  curl -s ${BASE}/ledger/credit_score/cs-experian-2026-04"
