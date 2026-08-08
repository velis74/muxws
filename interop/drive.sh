#!/usr/bin/env bash
# Drive one interop pair: start an acceptor, wait for the port it printed, run the dialer against it.
#
#   interop/drive.sh python ts     Python acceptor, TypeScript dialer
#   interop/drive.sh ts python     the reverse
#
# Both role assignments run the same scenario, which is the point (WSM-TST-004's first half).
set -euo pipefail
cd "$(dirname "$0")/.."

VENV="${MUXWS_VENV:-/home/jure/.venv/muxws/bin}"
ACCEPTOR="$1"
DIALER="$2"
LOG="$(mktemp)"
trap 'kill "${PID:-}" 2>/dev/null || true; rm -f "$LOG"' EXIT

if [ "$ACCEPTOR" = python ]; then
  "$VENV/python" interop/runner.py accept 0 > "$LOG" 2>&1 &
else
  npx tsx interop/runner.ts accept 0 > "$LOG" 2>&1 &
fi
PID=$!

for _ in $(seq 1 100); do
  PORT="$(grep -oE "\"port\": *[0-9]+" "$LOG" 2>/dev/null | head -1 | grep -oE "[0-9]+" || true)"
  [ -n "${PORT:-}" ] && break
  sleep 0.1
done
if [ -z "${PORT:-}" ]; then
  echo "interop FAILED: the $ACCEPTOR acceptor never reported a port"; cat "$LOG"; exit 1
fi

if [ "$DIALER" = python ]; then
  "$VENV/python" interop/runner.py dial "ws://127.0.0.1:$PORT"
else
  npx tsx interop/runner.ts dial "ws://127.0.0.1:$PORT"
fi
