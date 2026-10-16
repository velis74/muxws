#!/usr/bin/env bash
# Drive one interop pair: own the acceptor *process*, start it, run the dialer against it.
#
#   interop/drive.sh python ts                     Python acceptor, TypeScript dialer, WSM-TST-004
#   interop/drive.sh ts python                     the reverse
#   interop/drive.sh python ts reconnect           the same pair, WSM-TST-005
#   interop/drive.sh python ts main msgpack        the same, with the codec pinned independently
#   interop/drive.sh python ts unix json           WSM-TST-004 again, over a Unix domain socket
#   interop/drive.sh python ts corpus msgpack 13   the sequence corpus, cross-language (WSM-CDC-007)
#
# Both role assignments run the same scenario (WSM-TST-004). The reconnect scenario kills and
# restarts the acceptor, so this script manages the acceptor process: it starts it, reads the port it
# bound, SIGKILLs it with streams open, and starts a second acceptor on the same port (WSM-TST-005).
#
# `unix` runs the `main` scenario (WSM-TST-004) with the acceptor bound to a socket file and the dialer
# given a `ws+unix://<path>:/<route>` URL. It runs in both role assignments because the URL is parsed by
# two different libraries, which must agree on where the filesystem path ends and the HTTP request
# target begins; neither language's own test suite can check that agreement. `unix` is not available
# for `reconnect`: a SIGKILLed process does not unlink its socket file, so the restarted acceptor
# would fail with EADDRINUSE. Avoiding that requires either the driver unlinking another process's
# socket or runners unlinking before binding, which can take over a live peer's address.
#
# The codec comes from MUXWS_CODEC / VITE_MUXWS_CODEC; this script passes the environment through
# unchanged. The optional fourth argument is the codec the caller expects. If both variable names
# are misspelt, every process falls back to `json` and the msgpack jobs still pass, so the expected
# codec must come from the caller (in CI, the workflow step) rather than from the same environment.
#
# The optional fifth argument, read only by the `corpus` scenario, is the number of fixtures the
# caller expects the run to execute. The conductor reports its own count; this argument comes from
# the workflow step, independently of the conductor.
set -euo pipefail
cd "$(dirname "$0")/.."

VENV="${MUXWS_VENV:-/home/jure/.venv/muxws/bin}"
USAGE="usage: drive.sh <python|ts> <python|ts> [main|unix|reconnect|corpus] [expected-codec] [expected-fixtures]"
ACCEPTOR="${1:?$USAGE}"
DIALER="${2:?$USAGE}"
SCENARIO="${3:-main}"
#: Unset means no expected codec. Passed and empty means the caller's interpolation produced an empty
#: string, and is an error. The same holds for the fixture count below.
EXPECT_CODEC="${4-}"
EXPECT_FIXTURES="${5-}"
EXPECT_GIVEN="$#"

# Long enough for the backoff schedule to reach its cap at least twice, so the dispersion assertion in
# the runners has more than one capped sample (initial_delay 0.05, factor 2, max 0.5: after
# 0.05+0.1+0.2+0.4 every jittered delay is capped). The job takes seconds. Override for slow runners.
DOWNTIME="${MUXWS_INTEROP_DOWNTIME:-3}"

ACCEPTOR_LOG="$(mktemp)"
DIALER_LOG="$(mktemp)"
ACCEPTOR_PID=""
DIALER_PID=""
SOCKET_DIR=""
SOCKET=""

# `if` rather than `[ ... ] && kill`: under `set -e` a false test is a failing command, so an exit trap
# would abort on the first empty pid and leave the other process running; in the reconnect scenario
# that acceptor keeps holding the port.
cleanup() {
  if [ -n "$ACCEPTOR_PID" ]; then kill -9 "$ACCEPTOR_PID" 2>/dev/null || true; fi
  if [ -n "$DIALER_PID" ]; then kill -9 "$DIALER_PID" 2>/dev/null || true; fi
  rm -f "$ACCEPTOR_LOG" "$DIALER_LOG"
  # A SIGKILLed acceptor does not unlink its socket file, so the trap removes the directory.
  if [ -n "$SOCKET_DIR" ]; then rm -rf "$SOCKET_DIR"; fi
  return 0
}
trap cleanup EXIT

fail() {
  echo "interop FAILED: $*"
  # `-s`: an empty log file means the process was not started in this scenario, and is not printed.
  if [ -s "$ACCEPTOR_LOG" ]; then echo "--- acceptor log"; cat "$ACCEPTOR_LOG"; fi
  if [ -s "$DIALER_LOG" ]; then echo "--- dialer log"; cat "$DIALER_LOG"; fi
  exit 1
}

# Which entry point the acceptor process runs. `accept` serves the hand-written WSM-TST-004/005
# scripts; `corpus-accept` serves the sequence corpus and reports a **control** port rather than a
# WebSocket one (WSM-CDC-007); `accept-unix` is `accept` bound to a socket file.
ACCEPTOR_MODE=accept
if [ "$SCENARIO" = corpus ]; then ACCEPTOR_MODE=corpus-accept; fi
if [ "$SCENARIO" = unix ]; then ACCEPTOR_MODE=accept-unix; fi

# `node --import tsx` and not `npx tsx`: the tsx CLI runs the program in a child process, so SIGKILL to
# the recorded pid would leave the acceptor running and holding the port, and the reconnect scenario
# would reconnect to it.
start_acceptor() {  # $1 = the address to bind: a port, 0 to let the kernel choose, or a socket path
  if [ "$ACCEPTOR" = python ]; then
    "$VENV/python" interop/runner.py "$ACCEPTOR_MODE" "$1" >> "$ACCEPTOR_LOG" 2>&1 &
  else
    node --import tsx interop/runner.ts "$ACCEPTOR_MODE" "$1" >> "$ACCEPTOR_LOG" 2>&1 &
  fi
  ACCEPTOR_PID=$!
}

run_dialer() {  # $1 = mode, $2 = url; runs in the foreground
  if [ "$DIALER" = python ]; then
    "$VENV/python" interop/runner.py "$1" "$2"
  else
    node --import tsx interop/runner.ts "$1" "$2"
  fi
}

wait_for() {  # $1 = file, $2 = ERE, $3 = how many matches, $4 = what we are waiting for
  for _ in $(seq 1 300); do
    if [ "$(grep -Ec "$2" "$1" || true)" -ge "$3" ]; then return 0; fi
    sleep 0.1
  done
  fail "timed out waiting for $4"
}

# Both ports emit one JSON object per line; Python's `json.dumps` puts a space after the colon and
# `JSON.stringify` does not, hence the ` *`.
PORT_LINE='"port": *[0-9]+'
# The Unix acceptors report the socket file instead, after `listen`. The file exists from `bind`
# onward, so waiting for the path to exist can connect before `listen` and get ECONNREFUSED.
PATH_LINE='"path": *"[^"]+"'
CODEC_LINE='"codec": *"[a-z0-9_-]+"'

# CI passes `MUXWS_VENV=$(dirname $(which python))`. An empty value selects the default above under
# `:-`; if that path does not exist, the acceptor fails to start and the only symptom is a 30-second
# `wait_for` timeout.
if [ "$ACCEPTOR" = python ] || [ "$DIALER" = python ]; then
  if [ ! -x "$VENV/python" ]; then fail "no python interpreter at $VENV/python (MUXWS_VENV=${MUXWS_VENV:-unset})"; fi
fi

# Checked before any process is started: an empty expectation means the caller's interpolation failed.
if [ "$EXPECT_GIVEN" -ge 4 ] && [ -z "$EXPECT_CODEC" ]; then
  fail "an expected codec was passed but came out empty - check the caller's interpolation"
fi

# The bound address is read from the acceptor's ready line. For TCP the kernel chooses the port; for
# Unix sockets this script chooses the path. Readiness is taken only from that line, which the
# acceptor prints after `listen`.
PORT=""
if [ "$SCENARIO" = unix ]; then
  # A Unix socket address is limited to 108 bytes on Linux and 104 on macOS, and TMPDIR is set by the
  # caller. An overlong path fails inside the acceptor (`OSError: AF_UNIX path too long` from Python,
  # `EINVAL` from node); this check fails earlier and reports the length.
  SOCKET_DIR="$(mktemp -d "${TMPDIR:-/tmp}/muxws.XXXXXX")"
  SOCKET="$SOCKET_DIR/s.sock"
  if [ "${#SOCKET}" -gt 100 ]; then
    fail "the socket path is ${#SOCKET} bytes, near the ~108 the kernel allows: $SOCKET (set TMPDIR shorter)"
  fi
  start_acceptor "$SOCKET"
  wait_for "$ACCEPTOR_LOG" "$PATH_LINE" 1 "the $ACCEPTOR acceptor to report the socket file it bound"
  BOUND="$(grep -oE "$PATH_LINE" "$ACCEPTOR_LOG" | head -1 | sed -E 's/.*"([^"]+)"$/\1/' || true)"
  # The dialer is given the path this script composed. If the acceptor bound a different path, the
  # dialer would fail with ENOENT; this check reports the mismatch at the acceptor instead.
  if [ "$BOUND" != "$SOCKET" ]; then
    fail "the $ACCEPTOR acceptor bound '$BOUND', not the '$SOCKET' this driver gave it"
  fi
else
  start_acceptor 0
  wait_for "$ACCEPTOR_LOG" "$PORT_LINE" 1 "the $ACCEPTOR acceptor to report a port"
  PORT="$(grep -oE "$PORT_LINE" "$ACCEPTOR_LOG" | head -1 | grep -oE '[0-9]+' || true)"
  if [ -z "$PORT" ]; then fail "the $ACCEPTOR acceptor reported a port line this driver cannot parse"; fi
fi

# Compares the codec the acceptor reports with the one the caller expects. Only the acceptor reports
# its codec; a dialer configured for another codec fails the `muxws.v1.<codec>` check at the handshake
# (WSM-CDC-020/022), so a completed run uses the same codec on both ends.
REPORTED="$(grep -oE "$CODEC_LINE" "$ACCEPTOR_LOG" | head -1 | grep -oE '"[a-z0-9_-]+"$' | tr -d '"' || true)"
if [ "$EXPECT_GIVEN" -ge 4 ]; then
  if [ -z "$EXPECT_CODEC" ]; then
    fail "an expected codec was passed but came out empty - check the caller's interpolation"
  fi
  if [ "$REPORTED" != "$EXPECT_CODEC" ]; then
    fail "the $ACCEPTOR acceptor is running codec '$REPORTED', not the '$EXPECT_CODEC' this run pinned"
  fi
fi
# Printed whether or not an expected codec was given, so the CI log records the codec the run used.
echo "{\"driver\": \"codec\", \"acceptor\": \"$ACCEPTOR\", \"running\": \"$REPORTED\", \"pinned\": \"$EXPECT_CODEC\"}"

# A runner that completes its scenario prints an `ok` line on stdout. Exit status 0 alone is
# insufficient: a runner that returns early without asserting anything also exits 0. Every scenario
# below checks for the runner's `ok` line in addition to the exit status.
OK_LINE='"ok": *true'

if [ "$SCENARIO" = main ] || [ "$SCENARIO" = unix ]; then
  # The transport changes only the URL. The route is one variable because the acceptor's reported
  # request target is compared with it below.
  #
  # The route contains a query string, and the query contains a colon. The request target is the URL's
  # pathname plus search; a dialer that uses only the pathname sends `/ws`. The URL is split at the
  # first colon after the socket path; a dialer that splits at every colon (the default in the `ws`
  # package, overridden by `muxws/node`) sends `/ws?probe=1`. The comparison below detects both.
  UNIX_ROUTE='/ws?probe=1:2'
  URL="ws://127.0.0.1:$PORT"
  if [ "$SCENARIO" = unix ]; then URL="ws+unix://$SOCKET:$UNIX_ROUTE"; fi
  # `if !` rather than `set -e`, so that a dialer failure also prints the acceptor's log, which shows
  # handler exceptions, refused upgrades and codec registration errors.
  if ! run_dialer dial "$URL" > "$DIALER_LOG" 2>&1; then
    fail "the $DIALER dialer failed against the $ACCEPTOR acceptor at $URL"
  fi
  cat "$DIALER_LOG"
  if ! grep -Eq "$OK_LINE" "$DIALER_LOG"; then
    fail "the $DIALER dialer exited 0 without reporting the WSM-TST-004 script complete"
  fi
  if [ "$SCENARIO" = unix ]; then
    # Both acceptors report the request target they received. A dialer that split the URL at a
    # different colon reaches the same socket with a different target, and the assertions above still
    # pass; this comparison checks the request target across the two implementations.
    REPORTED_TARGET="$(grep -oE '"target": *("[^"]*"|null)' "$ACCEPTOR_LOG" | head -1 || true)"
    if [ -z "$REPORTED_TARGET" ]; then
      fail "the $ACCEPTOR acceptor reported no request target; this driver cannot check the URL split"
    fi
    # Anchored at the key and stripped of the surrounding quotes, because the target contains a
    # colon: matching "everything after the last colon" would read `/ws?probe=1:2` as `2`.
    TARGET="$(printf '%s' "$REPORTED_TARGET" | sed -E 's/^"target": *//; s/^"//; s/"$//')"
    if [ "$TARGET" != "$UNIX_ROUTE" ]; then
      fail "the $ACCEPTOR acceptor saw request target '$TARGET'; the $DIALER dialer was given '$UNIX_ROUTE'"
    fi
    echo "{\"driver\": \"unix\", \"acceptor\": \"$ACCEPTOR\", \"dialer\": \"$DIALER\", \"target\": \"$TARGET\"}"
  fi
  exit 0
fi

if [ "$SCENARIO" = corpus ]; then
  # ------------------------------------------------------------ WSM-CDC-007, cross-language
  # The conductor is the dialer process: it reads `conformance/sequences/`, runs the steps whose
  # `peer` is its own role and sends the rest to the acceptor over the control channel whose port was
  # read above. Afterwards this driver checks the fixture count, so a run that executed no fixtures
  # fails.
  if ! run_dialer corpus-dial "127.0.0.1:$PORT" > "$DIALER_LOG" 2>&1; then
    fail "the $DIALER corpus conductor failed against the $ACCEPTOR acceptor"
  fi
  cat "$DIALER_LOG"
  if ! grep -Eq "$OK_LINE" "$DIALER_LOG"; then
    fail "the $DIALER corpus conductor exited 0 without reporting the corpus complete"
  fi

  RAN="$(grep -oE '"ran": *[0-9]+' "$DIALER_LOG" | tail -1 | grep -oE '[0-9]+' || true)"
  if [ -z "$RAN" ]; then fail "the $DIALER corpus conductor reported no fixture count"; fi
  if [ "$RAN" -lt 1 ]; then fail "the corpus run exercised no fixture at all"; fi
  if [ "$EXPECT_GIVEN" -ge 5 ]; then
    if [ -z "$EXPECT_FIXTURES" ]; then
      fail "an expected fixture count was passed but came out empty - check the caller's interpolation"
    fi
    if [ "$RAN" != "$EXPECT_FIXTURES" ]; then
      fail "the corpus run exercised $RAN fixtures, not the $EXPECT_FIXTURES this run pinned"
    fi
  fi
  echo "{\"driver\": \"corpus\", \"acceptor\": \"$ACCEPTOR\", \"dialer\": \"$DIALER\", \"ran\": $RAN, \"pinned\": \"$EXPECT_FIXTURES\"}"
  exit 0
fi

if [ "$SCENARIO" != reconnect ]; then
  fail "unknown scenario '$SCENARIO'"
fi

# ---------------------------------------------------------------- WSM-TST-005

run_dialer reconnect-dial "ws://127.0.0.1:$PORT" >> "$DIALER_LOG" 2>&1 &
DIALER_PID=$!

# The dialer reports when its streams are open, and the acceptor is killed only after that report.
# Killing on a timer could kill the acceptor before the streams are open.
wait_for "$DIALER_LOG" '"event": *"streams-open"' 1 "the $DIALER dialer to open its streams"

# SIGKILL, not SIGTERM: WSM-TST-005 tests process death. A graceful shutdown sends a close frame and
# tests `goaway` handling instead.
kill -9 "$ACCEPTOR_PID"
wait "$ACCEPTOR_PID" 2>/dev/null || true
sleep "$DOWNTIME"
start_acceptor "$PORT"
wait_for "$ACCEPTOR_LOG" "$PORT_LINE" 2 "the restarted $ACCEPTOR acceptor to rebind port $PORT"

if ! wait "$DIALER_PID"; then
  DIALER_PID=""
  fail "the $DIALER dialer did not survive the acceptor restart"
fi
DIALER_PID=""
cat "$DIALER_LOG"
if ! grep -Eq "$OK_LINE" "$DIALER_LOG"; then
  fail "the $DIALER dialer exited 0 without reporting the WSM-TST-005 script complete"
fi

# The byte-identical requirement of WSM-TST-005, checked on the acceptor side across two processes.
# Each acceptor logs the first `open` it receives, re-encoded; the first open of a connection is the
# hello (WSM-RCN-023). The two logged lines must be equal; a replay that rebuilt the payload, dropped
# the headers or marked itself as a replay produces a different line.
readarray -t ENCODINGS < <(grep -oE '"encoding": *"[0-9a-f]+"' "$ACCEPTOR_LOG" | grep -oE '"[0-9a-f]+"$' | tr -d '"')
if [ "${#ENCODINGS[@]}" -ne 2 ]; then
  fail "expected two hellos at the $ACCEPTOR acceptor, one per connection, got ${#ENCODINGS[@]}"
fi
if [ "${ENCODINGS[0]}" != "${ENCODINGS[1]}" ]; then
  fail "the hello the $ACCEPTOR acceptor accepted was not byte-identical across the reconnect (WSM-RCN-027)"
fi
echo "{\"scenario\": \"reconnect\", \"acceptor\": \"$ACCEPTOR\", \"dialer\": \"$DIALER\", \"hello_identical\": true}"
