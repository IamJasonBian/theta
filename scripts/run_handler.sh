#!/usr/bin/env bash
# Production entrypoint for the LedgerEventHandler — long-running poller
# that consumes events, maintains state, and fires alert callbacks. This
# is what Render's "theta-handler" worker service should boot.
#
# All knobs come from the environment so the same image can run with no
# code change across local dev, staging, and prod.
set -euo pipefail

# Force line-buffered stdout so callbacks ([new]/[updated]/[deleted]/
# [ALERT]) flush immediately. Without this, Python block-buffers when
# stdout isn't a TTY and the log goes silent for long stretches.
export PYTHONUNBUFFERED=1

LEDGER_BASE_URL="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"
LEDGER_TIMEOUT="${LEDGER_TIMEOUT:-10}"
LEDGER_INTERVAL="${LEDGER_INTERVAL:-2.0}"
LEDGER_MAX_BACKOFF="${LEDGER_MAX_BACKOFF:-30.0}"
LEDGER_ALERT_THRESHOLD="${LEDGER_ALERT_THRESHOLD:-3}"
LEDGER_DURATION="${LEDGER_DURATION:-0}"
LEDGER_SKIP_INITIAL_SNAPSHOT="${LEDGER_SKIP_INITIAL_SNAPSHOT:-0}"

cd "$(dirname "$0")/.."

args=(
  --base-url "$LEDGER_BASE_URL"
  --timeout "$LEDGER_TIMEOUT"
  --interval "$LEDGER_INTERVAL"
  --max-backoff "$LEDGER_MAX_BACKOFF"
  --alert-after-failures "$LEDGER_ALERT_THRESHOLD"
)
if [[ "$LEDGER_DURATION" != "0" ]]; then
  args+=( --duration "$LEDGER_DURATION" )
fi
if [[ "$LEDGER_SKIP_INITIAL_SNAPSHOT" == "1" ]]; then
  args+=( --skip-initial-snapshot )
fi

exec python3 handler.py "${args[@]}" "$@"
