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
# Both role assignments run the same scenario, which is the point (WSM-TST-004). The reconnect
# scenario needs the acceptor **killed and restarted**, so this script owns the process rather than
# the socket: it starts it, reads the port it bound, SIGKILLs it with streams open, and starts a
# second one on that same port (WSM-TST-005).
#
# `unix` is `main` with the acceptor bound to a socket file and the dialer given a
# `ws+unix://<path>:/<route>` URL: the same WSM-TST-004 script, deliberately not a new one, because the
# claim being tested is that nothing above the socket noticed. It runs in both role assignments for
# the reason the matrix exists at all - the URL is parsed by two different libraries, one of which
# has to agree with the other about where the filesystem path ends and the HTTP request target
# begins, and that agreement is invisible to either language's own test suite. It is deliberately
# **not** offered for `reconnect`: that scenario SIGKILLs the acceptor, a killed process cannot
# unlink its socket file, and the restart would then meet EADDRINUSE - which would be a fact about
# stale inodes rather than about reconnection, and the only ways out of it (a driver that unlinks
# another process's socket, or runners that unlink before binding and can therefore steal a live
# peer's address) are both worse than not making the claim.
#
# The codec is whatever MUXWS_CODEC / VITE_MUXWS_CODEC say; this script passes the environment
# through untouched, so one driver serves every CI job. The optional fourth argument is the
# codec the *caller* believes it configured, and it exists because that pass-through is exactly what
# makes a misconfiguration undetectable: misspell both variable names and every process falls back
# to `json`, the msgpack jobs run green, and CI reports that a codec has a live cross-language pair
# (WSM-CDC-007) when it has nothing of the kind. Checking against the environment cannot catch that -
# the check would read the same misspelt variables - so the expectation has to arrive from somewhere
# the mistake cannot reach, which for CI is the job matrix.
#
# The optional fifth argument is the same idea one level up, and only the `corpus` scenario reads it:
# how many fixtures the caller expects that run to have exercised. The conductor counts its own
# fixtures and asserts its own bookkeeping, but a driver that counted itself is a driver that can
# report three fixtures as thirteen; the number pinned here comes from the job matrix instead, which
# is somewhere the conductor cannot reach.
set -euo pipefail
cd "$(dirname "$0")/.."

VENV="${MUXWS_VENV:-/home/jure/.venv/muxws/bin}"
USAGE="usage: drive.sh <python|ts> <python|ts> [main|unix|reconnect|corpus] [expected-codec] [expected-fixtures]"
ACCEPTOR="${1:?$USAGE}"
DIALER="${2:?$USAGE}"
SCENARIO="${3:-main}"
#: Unset means "no independent expectation"; **passed and empty** means a caller tried to pin one and
#: its own interpolation came out blank, which is the workflow typo this argument exists to catch and
#: is therefore an error rather than a shrug. The same holds for the fixture count below.
EXPECT_CODEC="${4-}"
EXPECT_FIXTURES="${5-}"
EXPECT_GIVEN="$#"

# Long enough for the backoff schedule to reach its cap twice over, so the dispersion assertion in
# the runners has more than one capped sample to look at (initial_delay 0.05, factor 2, max 0.5:
# 0.05+0.1+0.2+0.4 gets there, and every jittered delay after that is a capped one). Short enough
# that the whole job is seconds. Overridable so a slow runner can be given room without an edit.
DOWNTIME="${MUXWS_INTEROP_DOWNTIME:-3}"

ACCEPTOR_LOG="$(mktemp)"
DIALER_LOG="$(mktemp)"
ACCEPTOR_PID=""
DIALER_PID=""
SOCKET_DIR=""
SOCKET=""

# `if` rather than `[ ... ] && kill`: under `set -e` a false test is a failing command, and an exit
# trap that aborted on its first empty pid would leave the other process running - which for the
# reconnect scenario means an acceptor still holding the port the next run wants.
cleanup() {
  if [ -n "$ACCEPTOR_PID" ]; then kill -9 "$ACCEPTOR_PID" 2>/dev/null || true; fi
  if [ -n "$DIALER_PID" ]; then kill -9 "$DIALER_PID" 2>/dev/null || true; fi
  rm -f "$ACCEPTOR_LOG" "$DIALER_LOG"
  # The socket file outlives the acceptor - a SIGKILLed process unlinks nothing - so the directory
  # goes with the run rather than with the process that bound it.
  if [ -n "$SOCKET_DIR" ]; then rm -rf "$SOCKET_DIR"; fi
  return 0
}
trap cleanup EXIT

fail() {
  echo "interop FAILED: $*"
  # `-s`, because a log file that was never written to reads as "the process said nothing", which is
  # a different and much more alarming fact than "there was no such process in this scenario".
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

# `node --import tsx` and not `npx tsx`: the tsx CLI runs the program in a *child* process, so a
# SIGKILL aimed at the pid this script recorded would leave the real acceptor alive and holding the
# port - and the reconnect scenario would then reconnect to the process it believes it killed.
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
# The Unix acceptors report the socket file instead, and they report it *after* `listen` - the file
# appears at `bind`, one syscall earlier, so a driver that waited for the path to exist on disk would
# race the listen and meet ECONNREFUSED on a socket that is about to be fine.
PATH_LINE='"path": *"[^"]+"'
CODEC_LINE='"codec": *"[a-z0-9_-]+"'

# CI passes `MUXWS_VENV=$(dirname $(which python))`; an empty result there is falsy for `:-`, so a
# broken interpreter lookup silently becomes this developer default and the acceptor then fails to
# start with nothing but a 30-second `wait_for` timeout to show for it.
if [ "$ACCEPTOR" = python ] || [ "$DIALER" = python ]; then
  if [ ! -x "$VENV/python" ]; then fail "no python interpreter at $VENV/python (MUXWS_VENV=${MUXWS_VENV:-unset})"; fi
fi

# Before anything is started: a caller whose interpolation came out blank has a bug in the caller,
# and answering it with a process and a socket would only bury the message.
if [ "$EXPECT_GIVEN" -ge 4 ] && [ -z "$EXPECT_CODEC" ]; then
  fail "an expected codec was passed but came out empty - check the caller's interpolation"
fi

# What the acceptor bound, read back from the acceptor itself rather than assumed. The two
# transports answer different questions here - the kernel chose the port, this script chose the
# path - but both answers arrive the same way, in the acceptor's own line, because a driver that
# inferred readiness from anything else is a driver that races the listen.
PORT=""
if [ "$SCENARIO" = unix ]; then
  # Short on purpose, and measured rather than assumed: a Unix socket address is capped at 108 bytes
  # on Linux and 104 on macOS, and TMPDIR belongs to the caller. Nothing is truncated silently - an
  # overrun is `OSError: AF_UNIX path too long` from Python and `EINVAL` quoting the whole path from
  # node - but it surfaces inside an acceptor this script only sees the log of, and neither message
  # says which component to shorten. Failing here says it, with the number.
  SOCKET_DIR="$(mktemp -d "${TMPDIR:-/tmp}/muxws.XXXXXX")"
  SOCKET="$SOCKET_DIR/s.sock"
  if [ "${#SOCKET}" -gt 100 ]; then
    fail "the socket path is ${#SOCKET} bytes, near the ~108 the kernel allows: $SOCKET (set TMPDIR shorter)"
  fi
  start_acceptor "$SOCKET"
  wait_for "$ACCEPTOR_LOG" "$PATH_LINE" 1 "the $ACCEPTOR acceptor to report the socket file it bound"
  BOUND="$(grep -oE "$PATH_LINE" "$ACCEPTOR_LOG" | head -1 | sed -E 's/.*"([^"]+)"$/\1/' || true)"
  # Checked and not merely read: the dialer below is handed the path this script composed, so an
  # acceptor that bound somewhere else would leave the dialer meeting ENOENT and the log blaming the
  # dial for a mistake made one process earlier.
  if [ "$BOUND" != "$SOCKET" ]; then
    fail "the $ACCEPTOR acceptor bound '$BOUND', not the '$SOCKET' this driver gave it"
  fi
else
  start_acceptor 0
  wait_for "$ACCEPTOR_LOG" "$PORT_LINE" 1 "the $ACCEPTOR acceptor to report a port"
  PORT="$(grep -oE "$PORT_LINE" "$ACCEPTOR_LOG" | head -1 | grep -oE '[0-9]+' || true)"
  if [ -z "$PORT" ]; then fail "the $ACCEPTOR acceptor reported a port line this driver cannot parse"; fi
fi

# What the acceptor says it is actually running, against what the caller pinned. Only the acceptor is
# checked because only the acceptor reports; a dialer configured for another codec cannot get past
# the `muxws.v1.<codec>` assertion at the handshake (WSM-CDC-020/022), so a run that completes at all
# pins both ends to this one name.
REPORTED="$(grep -oE "$CODEC_LINE" "$ACCEPTOR_LOG" | head -1 | grep -oE '"[a-z0-9_-]+"$' | tr -d '"' || true)"
if [ "$EXPECT_GIVEN" -ge 4 ]; then
  if [ -z "$EXPECT_CODEC" ]; then
    fail "an expected codec was passed but came out empty - check the caller's interpolation"
  fi
  if [ "$REPORTED" != "$EXPECT_CODEC" ]; then
    fail "the $ACCEPTOR acceptor is running codec '$REPORTED', not the '$EXPECT_CODEC' this run pinned"
  fi
fi
# Announced whether or not it was pinned, so a CI log records which codec a job *proved* rather than
# which one its name claims - and so a pin quietly dropped from the workflow is visible in the diff
# of one line rather than nowhere at all.
echo "{\"driver\": \"codec\", \"acceptor\": \"$ACCEPTOR\", \"running\": \"$REPORTED\", \"pinned\": \"$EXPECT_CODEC\"}"

# A scenario that ran to completion says so, on stdout, in a line the driver reads. Exit 0 is not
# that statement: it is what a runner also reports when it returns early from a path it never
# asserted anything on, and "the process did not fail" is a weaker claim than "the process finished
# the script". Every scenario below therefore greps for the runner's own `ok` line as well.
OK_LINE='"ok": *true'

if [ "$SCENARIO" = main ] || [ "$SCENARIO" = unix ]; then
  # The only difference the transport makes to this scenario, and it is one string. The route is a
  # variable rather than two literals because the acceptor's report is checked against it below, and
  # a check whose expectation can drift away from the URL it checks is not a check.
  #
  # It carries a query string on purpose, and a colon inside that query for a second purpose. The
  # request target is the URL's pathname **and search**, so a dialer that built it from the pathname
  # alone - the obvious mistake, and the one a single-language test never notices because nothing
  # routes on the target - would arrive here with `/ws`. And the whole URL is split on the *first*
  # colon, so a dialer that split on every one of them - which is what the `ws` package does on its
  # own, and what `muxws/node` overrides it to stop doing - would arrive with `/ws?probe=1`. Both
  # are caught by the comparison below, in the only place either can be seen: with the two
  # implementations at opposite ends of one connection.
  UNIX_ROUTE='/ws?probe=1:2'
  URL="ws://127.0.0.1:$PORT"
  if [ "$SCENARIO" = unix ]; then URL="ws+unix://$SOCKET:$UNIX_ROUTE"; fi
  # `if !` rather than letting `set -e` abort here: an abort prints the dialer's own message and
  # nothing else, and the cause of a cross-language failure lives in the acceptor's log at least as
  # often - a handler that raised, an upgrade it refused, a codec it could not register.
  if ! run_dialer dial "$URL" > "$DIALER_LOG" 2>&1; then
    fail "the $DIALER dialer failed against the $ACCEPTOR acceptor at $URL"
  fi
  cat "$DIALER_LOG"
  if ! grep -Eq "$OK_LINE" "$DIALER_LOG"; then
    fail "the $DIALER dialer exited 0 without reporting the WSM-TST-004 script complete"
  fi
  if [ "$SCENARIO" = unix ]; then
    # The half of the URL grammar that reaching the socket does not prove. Both acceptors report the
    # request target they were handed; a dialer that split the URL anywhere but at the first colon
    # would arrive on the same socket carrying a different target, and every assertion above would
    # still pass. This is the only place the two languages' parsers are compared with each other.
    REPORTED_TARGET="$(grep -oE '"target": *("[^"]*"|null)' "$ACCEPTOR_LOG" | head -1 || true)"
    if [ -z "$REPORTED_TARGET" ]; then
      fail "the $ACCEPTOR acceptor reported no request target; this driver cannot check the URL split"
    fi
    # Anchored at the front and unquoted from the ends, rather than "everything after the colon":
    # the target itself contains one, and a greedy match reported `2` for `/ws?probe=1:2` - the
    # driver failing a pair that had agreed perfectly, which is the worst kind of red.
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
  # `peer` is its own role and ships the rest to the acceptor over the control channel whose port
  # was read above. What this driver checks afterwards is the *count*, because a conductor that
  # silently ran nothing is the failure mode a green job hides best.
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

# The dialer says when its streams are open. Killing on a timer instead would race the opens, and a
# reconnect scenario that killed the acceptor before there was anything to lose asserts nothing.
wait_for "$DIALER_LOG" '"event": *"streams-open"' 1 "the $DIALER dialer to open its streams"

# SIGKILL, not SIGTERM: WSM-TST-005 is about the process dying, and a graceful shutdown would send a
# close frame and make this a `goaway` test instead.
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

# The byte-identical half of WSM-TST-005, checked on the **acceptor's** side and across two
# processes: each acceptor logs the first `open` it receives, re-encoded, and the first open of a
# connection is the hello and nothing else (WSM-RCN-023). Two runs, two lines, and they must agree -
# a replay that rebuilt the payload, dropped the headers or marked itself would differ here.
readarray -t ENCODINGS < <(grep -oE '"encoding": *"[0-9a-f]+"' "$ACCEPTOR_LOG" | grep -oE '"[0-9a-f]+"$' | tr -d '"')
if [ "${#ENCODINGS[@]}" -ne 2 ]; then
  fail "expected two hellos at the $ACCEPTOR acceptor, one per connection, got ${#ENCODINGS[@]}"
fi
if [ "${ENCODINGS[0]}" != "${ENCODINGS[1]}" ]; then
  fail "the hello the $ACCEPTOR acceptor accepted was not byte-identical across the reconnect (WSM-RCN-027)"
fi
echo "{\"scenario\": \"reconnect\", \"acceptor\": \"$ACCEPTOR\", \"dialer\": \"$DIALER\", \"hello_identical\": true}"
