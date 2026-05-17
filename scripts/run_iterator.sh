#!/usr/bin/env bash
# Tail Ledger events using the async polling iterator. Suitable as a
# foreground tail in a terminal or as the CMD of a sidecar container.
# Set LEDGER_DURATION to a non-zero number of seconds for time-boxed
# runs (smoke tests); leave at 0 / unset to poll forever.
set -euo pipefail

# Force line-buffered stdout so the event stream is readable in real time
# when piped to `tee`, redirected to a file, or read by a sidecar like
# Render's log shipper. Without this, `print()` block-buffers (~4-8KB)
# and operators see nothing for minutes between heartbeats.
export PYTHONUNBUFFERED=1

LEDGER_BASE_URL="${LEDGER_BASE_URL:-http://127.0.0.1:8765}"
LEDGER_TIMEOUT="${LEDGER_TIMEOUT:-10}"
LEDGER_INTERVAL="${LEDGER_INTERVAL:-2.0}"
LEDGER_DURATION="${LEDGER_DURATION:-0}"

cd "$(dirname "$0")/.."

args=(
  --base-url "$LEDGER_BASE_URL"
  --timeout "$LEDGER_TIMEOUT"
  --interval "$LEDGER_INTERVAL"
)
if [[ "$LEDGER_DURATION" != "0" ]]; then
  args+=( --duration "$LEDGER_DURATION" )
fi

exec python3 iterator.py "${args[@]}" "$@"
