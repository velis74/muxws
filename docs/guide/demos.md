# Demos

Three demos ship with the repository, and one entry point reaches all three. They are consumers of
muxws rather than parts of it: nothing under `muxws/` imports them, no test in the library depends on
them, and neither published artefact contains them.

`python demo.py` with no arguments prints the help and exits. It starts nothing. Exactly one of
`--browser`, `--uds` and `--bench` is required, and they are mutually exclusive:

```bash
python demo.py                    # the help, and nothing else
python demo.py --browser          # the Vue application and the Python backend under uvicorn
python demo.py --browser node     # the same frontend, the TypeScript backend under tsx
python demo.py --browser --no-fe  # either backend alone
python demo.py --uds              # the Unix-socket pair from docs/examples/
python demo.py --bench            # the throughput report
python demo.py --bench --full     # every combination rather than the trimmed default
```

The positional backend argument and `--no-fe` belong to `--browser`, and `--full` to `--bench`;
passing one to a demo it does not belong to is an error rather than a silently ignored word. All
three check what they need before they start anything and print the install line that would fix the
run in front of it — the Node backend is never asked for uvicorn, and neither the socket demo nor the
measurement is ever asked for `node_modules`.

| | command | needs |
|---|---|---|
| A browser against either language's backend | `--browser`, `--browser node` | `pip install -e ".[demo,starlette]"` and `npm install` |
| A socket file, its permissions and its caller's credentials | `--uds` | `pip install -e ".[websockets]"` |
| What the envelope costs, against the same bytes with muxws removed | `--bench` | `pip install -e ".[websockets]"` |

## The browser demo

```bash
python demo.py --browser
```

A Vue application on `http://127.0.0.1:5173` and a backend on `:8020`, with the Vite dev server
proxying `/ws` to it — one origin, so there is no CORS story. Ctrl-C stops both.

On one socket, at once: twenty symbols each pushed on a stream of its own at four ticks a second, a
quote as a unary `reply()`, a history response that arrives point by point and can be cancelled
halfway through, a 1500-level depth book that crosses the frame cap, and a million-byte export that
does the same sixteen times over. The export is the panel to watch, because the ticks keep their turn
while its fragments are on the wire — that is the round-robin writer, and it is the one guarantee a
test transport could not see. See [Sizes & fragmentation](/guide/sizes-and-fragmentation).

The board is not a request. The backend calls `peer.open()` on its own initiative and the browser
receives it through its own `on_stream` handler: one peer type, one mechanism, no second correlation
story for push. See [Rationale](/guide/rationale#symmetry-one-peer-type-per-language).

**The second backend is the point of having two.**

```bash
python demo.py --browser node
```

The TypeScript backend in `demo/backend_node/` under `tsx`, serving the same frontend — and the
frontend does not change by a single line between them. If a Vue application cannot tell which
language answered, the wire really is the contract rather than one implementation's habits. That
backend needs `npm install` and no Python packages at all beyond the interpreter running `demo.py`,
which is why the dependency check asks which backend before it asks what is missing.

`--no-fe` starts either backend without the dev server, for a client of your own. Nothing then serves
`:5173`. `MUXWS_DEMO_PORT` moves the backend off 8020; the Vite proxy in
`demo/frontend/vite.config.ts` has to be told as well.

## The Unix-socket pair

```bash
python demo.py --uds
```

No browser and no port: an acceptor bound to a socket file in a temporary directory, and a client
dialling it as `ws+unix:///…/muxws.sock:/ws`. It runs the two shipped files
`docs/examples/uds_server.py` and `docs/examples/uds_client.py` as subprocesses rather than a copy of
them, so what it demonstrates is what the guide documents. It needs `pip install -e ".[websockets]"`
and nothing else, and it needs a platform that has `AF_UNIX`.

Two things only this transport can show, and neither has anything on the wire:

- **The permissions gate the dial.** The socket file has an owner, a group and a mode, so the kernel
  decides who may open the connection before a single byte of HTTP is written. Put it in a directory
  only the intended callers can traverse and the question is settled by `chmod`.
- **`SO_PEERCRED` names the caller.** Once the connection is open, the acceptor is handed the
  caller's pid, uid and gid straight from the kernel — a credential the other end cannot forge and
  which never travels over the connection. The demo prints what the acceptor saw after the client's
  own output. It is a Linux call; macOS and the BSDs have `getpeereid()`, which CPython does not
  expose, so there the file's permissions are the whole of the gate.

A page has no way to open a file as a socket, which is why this cannot be a panel in the demo above.
The URL grammar, the first-colon split and the platform differences are in
[Transports](/guide/transports#unix-domain-sockets).

## The throughput report

```bash
python demo.py --bench
```

`muxws/throughput_test.py` measures frames a second over the in-memory pair, which is a numerator
with no denominator: nothing there measures the same payload over the same machine with muxws taken
out of the path. This demo measures both halves in one run, so the figure can be stated as a fraction
of what the transport underneath it can do.

### The three modes

Every cell is run three ways, carrying the **same application payload bytes** in each — and under a
text codec all three send text, so the comparison is not one mode paying for an encoding the others
skip.

| mode | what it puts on the socket | what it isolates |
|---|---|---|
| `raw-socket` | a socket with a 4-byte big-endian length prefix, no WebSocket at all | the denominator: what this machine and this transport carry with nothing between them |
| `raw-websocket` | one WebSocket message per payload through the `websockets` library, no muxws | the cost of the WebSocket itself |
| `muxws` | `open()` once, then `send()` per payload | the cost of the muxws envelope on top of that |

Three modes rather than two, because a single ratio states the cost without decomposing it. The step
from `raw-socket` to `raw-websocket` is the WebSocket library's framing, its masking and the message
it assembles for its caller; the step from `raw-websocket` to `muxws` is the envelope, the codec and
the stream bookkeeping. Only the second one is this library's to answer for.

The baseline finds the frame boundaries and counts the bytes; it reassembles no payload. That is
deliberate, and it is what makes the column a ceiling: a receiver that copied each payload out of a
`StreamReader` would be measuring that buffer above about 64 KiB, and the denominator would fall
under the WebSocket mode it exists to bound. `raw-socket` is the largest figure in every row it is
measured in, and a row where it is not is a row whose denominator measured something other than the
transport.

### The axes

The default matrix is transport (`tcp` loopback, `unix` socket file) × payload (128 B, 4 KiB,
256 KiB) × codec (json) × one stream, plus one muxws-only cell at twenty concurrent streams for the
4 KiB payload. `--full` adds msgpack and twenty streams everywhere.

Cells that cannot run are skipped and the report says so: the `unix` rows where `socket.AF_UNIX` does
not exist, and the msgpack rows where msgpack is not installed (`pip install -e ".[msgpack]"`). A
skipped row is named as skipped rather than left out, so a short table is never mistaken for a
complete one.

### The method

Each cell puts its acceptor in a process of its own, so the two ends get two cores — both peers in
one event loop share a core and serialise against each other, which lands the figure low and blames
neither side. The dialer warms up and waits for the pipe to empty, then the clock starts, then it
sends for a fixed wall-clock budget, then it terminates the run and waits for the acceptor to report
how many payloads arrived. The clock stops when that answer arrives, so draining is inside the
elapsed time and a mode cannot score well by leaving work in a buffer. Emptying the pipe first is
what keeps that honest in the other direction: a backlog left by the warmup is drained inside the
measured interval, and its payloads are subtracted from the count that interval is divided by.

Throughput is payloads sent × encoded payload length ÷ elapsed. **MB means 1,000,000 bytes.** Each
cell is measured once, so a column is worth reading and the last digit of a row is not.

Every cell is measured for its full budget, so the run is silent for as long as the matrix takes. The
report goes to stdout and the one line saying the measuring has begun goes to stderr, so
`python demo.py --bench > report.txt` leaves the report alone in the file.

### How to read the ratio

`muxws` as a percentage of `raw-socket`, per cell, is what the envelope, the codec and the peer's
bookkeeping cost against the transport underneath them — but only in the regime the report was taken
in, and the report says which that is. Over loopback and over a socket file the limit is CPU, so the
percentage answers the question. On a link slow enough to saturate, every mode scores the same and
the ratio says nothing at all about this library.

Read down a payload column rather than at one number. The per-message envelope is a fixed cost, so
the 128-byte rows are where it weighs most and the 4 KiB rows are where it has largely amortised. The
256 KiB rows are not the end of that trend under a text codec: a payload over the 65,536-byte frame
cap is fragmented, and the splitter takes a text payload one codepoint at a time, so those rows carry
the fragmenter as well as the envelope. `--full` measures the same cell under msgpack, whose encoded
payload is bytes and is sliced whole.

Two figures are derived from the measurement rather than measured separately, because a delaying
proxy would add its own cost to every byte and corrupt the run it sits in, and `tc netem` needs root:

- **The crossover**, per cell: muxws's measured MB/s expressed in Gbit/s. Below that link speed the
  wire is the bottleneck and the envelope costs nothing measurable.
- **A round trip**, per transport: `peer.request()` measured locally, reported as microseconds and
  then against a 40 ms link, so what fraction of a real round trip this library is can be read
  directly.

`demo/bench_test.py` asserts the shape of a run — three modes in, three results out, a report that
renders — and never a rate. A benchmark that fails on a loaded CI runner has its floors lowered until
it asserts nothing, and the report is worth more than the assertion would be.

## See also

- [Getting Started](/guide/getting-started) — the three-file version, without a repository checkout
- [Transports](/guide/transports#unix-domain-sockets) — the `ws+unix:` grammar the socket demo dials
- [Comparison to HTTP/2 and HTTP/3](/guide/comparison#measuring-the-cost-yourself) — what the report does and does not answer
- [Sizes & fragmentation](/guide/sizes-and-fragmentation) — the frame cap and the writer the export panel exercises
