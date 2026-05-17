#!/usr/bin/env bash
# Smoke-test the LedgerClient against a running API. Reads the same env
# vars as the handler/iterator so a single .env (or Render env group)
# drives every entrypoint.
set -euo pipefail

LEDGER_BASE_URL="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"
LEDGER_TIMEOUT="${LEDGER_TIMEOUT:-10}"

cd "$(dirname "$0")/.."
exec python3 client.py \
  --base-url "$LEDGER_BASE_URL" \
  --timeout "$LEDGER_TIMEOUT" \
  "$@"
