# Demos

The repository contains three demos, all started through `demo.py`. They use muxws but are not part of
it: no module under `muxws/` imports them, no library test depends on them, and neither published
package contains them.

`python demo.py` without arguments prints the help and exits without starting anything. Exactly one of
`--browser`, `--uds` and `--bench` is required; they are mutually exclusive:

```bash
python demo.py                    # the help, and nothing else
python demo.py --browser          # the Vue application and the Python backend under uvicorn
python demo.py --browser node     # the same frontend, the TypeScript backend under tsx
python demo.py --browser --no-fe  # either backend alone
python demo.py --uds              # the Unix-socket pair from docs/examples/
python demo.py --bench            # the throughput report
python demo.py --bench --full     # every combination rather than the trimmed default
```

The positional backend argument and `--no-fe` apply only to `--browser`, and `--full` only to
`--bench`; passing one of them with another demo is an error. Each demo checks its dependencies before
starting anything and, if one is missing, prints the install command for it. The Node backend does not
require uvicorn, and neither the socket demo nor the benchmark requires `node_modules`.

| | command | needs |
|---|---|---|
| A browser against either language's backend | `--browser`, `--browser node` | `pip install -e ".[demo,starlette]"` and `npm install` |
| A socket file, its permissions and its caller's credentials | `--uds` | `pip install -e ".[websockets]"` |
| The throughput of muxws compared with the same bytes without muxws | `--bench` | `pip install -e ".[websockets]"` |

## The browser demo

```bash
python demo.py --browser
```

A Vue application on `http://127.0.0.1:5173` and a backend on `:8020`. The Vite dev server proxies
`/ws` to the backend, so the page and the WebSocket have the same origin and no CORS configuration is
needed. Ctrl-C stops both.

All of the following run concurrently on one socket: twenty symbols, each pushed on its own stream at
four ticks a second; a quote as a unary `reply()`; a history response that arrives point by point and
can be cancelled before it completes; a 1500-level depth book that exceeds the frame cap; and a
one-million-byte export that is split into about sixteen fragments. While the export's fragments are
being sent, the ticks continue to arrive. This shows the round-robin writer on a real network
connection, which the in-memory test transport cannot show. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation).

The tick board is not requested by the browser. The backend calls `peer.open()` itself and the browser
receives the stream through its own `on_stream` handler; push uses the same peer type and mechanism as a
request. See [Rationale](/guide/rationale#symmetry-one-peer-type-per-language).

**The second backend tests that both implementations are interchangeable.**

```bash
python demo.py --browser node
```

This runs the TypeScript backend in `demo/backend_node/` under `tsx`, serving the same frontend. The
frontend code is identical for both backends, so the frontend depends only on the wire protocol, not
on either implementation. The TypeScript backend needs `npm install` and no Python packages beyond the
interpreter running `demo.py`; the dependency check therefore determines the backend before it checks
for missing packages.

`--no-fe` starts either backend without the dev server, for use with another client. Nothing then
listens on `:5173`. `MUXWS_DEMO_PORT` changes the backend port from 8020; the Vite proxy in
`demo/frontend/vite.config.ts` must be changed to match.

## The Unix-socket pair

```bash
python demo.py --uds
```

No browser and no TCP port: an acceptor bound to a socket file in a temporary directory, and a client
that dials it as `ws+unix:///…/muxws.sock:/ws`. The demo runs `docs/examples/uds_server.py` and
`docs/examples/uds_client.py` as subprocesses, so it runs exactly the files the guide documents. It
needs `pip install -e ".[websockets]"` and a platform that has `AF_UNIX`.

The demo shows two properties of this transport that involve nothing on the wire:

- **File permissions control who can connect.** The socket file has an owner, a group and a mode, so
  the kernel checks access before any HTTP is sent. Placing the socket in a directory that only the
  intended callers can traverse restricts access with `chmod`.
- **`SO_PEERCRED` identifies the caller.** On an open connection, the acceptor receives the caller's
  pid, uid and gid from the kernel. The caller cannot forge these values, and they are not sent over
  the connection. The demo prints what the acceptor received after the client's own output.
  `SO_PEERCRED` is Linux-specific; macOS and the BSDs have `getpeereid()`, which CPython does not
  expose, so on those platforms only the file permissions apply.

A web page cannot open a file as a socket, so this demo cannot be part of the browser demo. The URL
grammar, the split at the first colon and the platform differences are in
[Transports](/guide/transports#unix-domain-sockets).

## The throughput report

```bash
python demo.py --bench
```

`muxws/throughput_test.py` measures frames per second over the in-memory pair, without a baseline:
it does not measure the same payload on the same machine without muxws. This demo measures both in
one run, so muxws throughput is reported as a fraction of the throughput of the underlying transport.

### The three modes

Every cell is run in three modes, each carrying **the same application payload bytes**. Under a text
codec all three modes send text, so no mode includes an encoding step that the others omit.

| mode | what it puts on the socket | what it isolates |
|---|---|---|
| `raw-socket` | a socket with a 4-byte big-endian length prefix, no WebSocket at all | the baseline: what this machine and this transport carry with nothing between them |
| `raw-websocket` | one WebSocket message per payload through the `websockets` library, no muxws | the cost of the WebSocket itself |
| `muxws` | `open()` once, then `send()` per payload | the cost of the muxws envelope on top of that |

Three modes separate the cost into two parts. The difference between `raw-socket` and `raw-websocket`
is the WebSocket library's framing, masking and message assembly; the difference between
`raw-websocket` and `muxws` is the envelope, the codec and the stream bookkeeping. Only the second part
is caused by muxws.

The `raw-socket` receiver finds frame boundaries and counts bytes; it does not reassemble payloads, so
its figure is an upper bound. A receiver that copied each payload out of a `StreamReader` would be
limited by that buffer above about 64 KiB, and the baseline would fall below the `raw-websocket` mode.
`raw-socket` is the highest figure in every row that measures it; a row where it is not indicates that
the baseline measured something other than the transport.

### The axes

The default matrix is transport (`tcp` loopback, `unix` socket file) × payload (128 B, 4 KiB,
256 KiB) × codec (json) × one stream, plus one muxws-only cell with twenty concurrent streams for the
4 KiB payload. `--full` adds msgpack and twenty streams for every cell.

Cells that cannot run are skipped, and the report lists them as skipped: the `unix` rows where
`socket.AF_UNIX` does not exist, and the msgpack rows where msgpack is not installed
(`pip install -e ".[msgpack]"`). A shortened table is therefore never presented as complete.

### The method

Each cell runs its acceptor in a separate process, so the two ends use two cores. With both peers in
one event loop they would share one core and slow each other down. The dialer sends a warmup, waits
until the connection is empty, starts the clock, sends for a fixed wall-clock budget, ends the run and
waits for the acceptor to report how many payloads it received. The clock stops when that report
arrives, so the time to drain buffers is included in the elapsed time and a mode cannot gain by
leaving data buffered. Payloads from the warmup that are still in transit when the clock starts are
subtracted from the count.

Throughput is payloads sent × encoded payload length ÷ elapsed time. **MB means 1,000,000 bytes.** Each
cell is measured once, so compare columns rather than the last digits of a row.

Every cell runs for its full budget, and the run prints nothing until the matrix is complete. The
report goes to stdout and the single line announcing the start of the measurement goes to stderr, so
`python demo.py --bench > report.txt` writes only the report to the file.

### How to read the ratio

`muxws` as a percentage of `raw-socket`, per cell, is the cost of the envelope, the codec and the
peer's bookkeeping relative to the transport. This holds only when CPU is the limit, and the report
states which regime applies. Over loopback and over a socket file the limit is CPU. On a link slow
enough to saturate, every mode gives the same figure and the ratio does not measure muxws.

Compare the rows of one payload column rather than a single figure. The per-message envelope is a fixed
cost, so it has the largest effect in the 128-byte rows and a much smaller one in the 4 KiB rows. Under
a text codec the 256 KiB rows also include fragmentation: a payload over the 65,536-byte frame cap is
fragmented, and the splitter processes a text payload one codepoint at a time. `--full` measures the
same cell under msgpack, whose encoded payload is bytes and is sliced at any byte.

Two figures are derived from the measurement rather than measured separately. A delaying proxy would
add its own cost to every byte, and `tc netem` requires root.

- **The crossover**, per cell: the measured muxws throughput in MB/s, expressed in Gbit/s. On a link
  slower than that, the link is the bottleneck and the envelope has no measurable cost.
- **A round trip**, per transport: `peer.request()` measured locally, reported in microseconds and as
  a fraction of a 40 ms link round trip.

`demo/bench_test.py` checks the structure of a run (three modes in, three results out, a report that
renders) and never a rate, because a rate threshold would fail on a loaded CI runner.

## See also

- [Getting Started](/guide/getting-started) — the three-file version, without a repository checkout
- [Transports](/guide/transports#unix-domain-sockets) — the `ws+unix:` grammar the socket demo dials
- [Comparison to HTTP/2 and HTTP/3](/guide/comparison#measuring-the-cost-yourself) — what the report does and does not measure
- [Sizes & fragmentation](/guide/sizes-and-fragmentation) — the frame cap and the writer the export panel exercises
