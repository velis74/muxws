# muxws M3 - Real transports, codec selection, and the TypeScript port

## 1. Goal

At the end of M3 muxws talks over real WebSockets in both languages, and the TypeScript port of the
peer core exists. A Starlette/FastAPI endpoint accepts a connection with `muxws.accept(websocket)`; a
Python process dials with `muxws.connect(url)` over the `websockets` library; a browser dials with the
same `connect(url)` in TypeScript; a Node process accepts over `ws` behind the
`muxws/node` subpath. The codec is chosen by deployment configuration
(`MUXWS_CODEC` / `VITE_MUXWS_CODEC`), an unregistered name fails loudly **before any socket opens**,
and a codec disagreement between the two ends is a named handshake rejection rather than undecodable
bytes later. A Python peer and a TypeScript peer complete a real conversation over a real socket.

## 2. Prerequisites

**M0** (layout, lint, build), **M1** (`frames`, `errors`, `codecs` + `JsonCodec`, `fragment`, the
conformance corpus - in both languages), **M2** (Python `Peer`, `Stream`, the state machine, id
allocation, `on_stream` dispatch, the awaitable handle, `muxws/transports/__init__.py`'s
`SocketAdapter` protocol and `muxws/transports/memory.py`'s `memory_pair()`).

M3 ports M2 to TypeScript; every M2 rule listed in that brief applies here verbatim to the TS
implementation and is not repeated below except where TypeScript differs.

## 3. Files to create or modify

```
muxws/conf.py                       muxws/conf_test.py
muxws/codec_test.py                 # the two spec-named selection tests
muxws/transports/starlette.py       muxws/transports/starlette_test.py
muxws/transports/websockets_.py     muxws/transports/websockets_test.py
muxws/__init__.py                   # connect / accept / serve / select_subprotocol
ts/conf.ts                          ts/conf.spec.ts
ts/stream.ts                        ts/stream.spec.ts
ts/peer.ts                          ts/peer.spec.ts
ts/transports/memory.ts             ts/transports/memory.spec.ts
ts/transports/browser-socket.ts     ts/transports/browser-socket.spec.ts
ts/transports/ws-socket.ts          # node only; imported solely by ts/node.ts
ts/index.ts                         # connect, Peer, Stream, errors, registerCodec, selectSubprotocol
ts/node.ts                          # accept / serve over `ws`
interop/                            # docker-free CI script: python peer <-> ts peer, both roles
```

## 4. Normative rules in force

### 4.1 Codec selection (§2.2)

- **WSM-CDC-010** The codec name MUST be read from deployment configuration, not from a call argument:
  Python `os.environ["MUXWS_CODEC"]` via a `muxws.conf.settings` singleton (default `"json"`),
  TypeScript `import.meta.env.VITE_MUXWS_CODEC` (default `"json"`).
- **WSM-CDC-011** `settings.codec` MUST be writable at runtime so an application may set it during
  bootstrap before connecting.
- **WSM-CDC-012** `connect()`, `accept()` and the peer constructor MUST accept a `codec=` / `codec:`
  override. It is documented as a test override and an escape hatch for a process holding two
  connections needing different codecs. No example outside the test suite may use it.
- **WSM-CDC-013** Registration MUST be explicit: `register_codec(name, codec)` /
  `registerCodec(name, codec)`. There MUST NOT be dynamic imports, lazy auto-registration, entry-point
  scanning, or any probing of whether a module happens to be installed.
- **WSM-CDC-016** A configured codec name that is not registered MUST raise `CodecNotRegistered` on the
  first connection attempt, **before any socket is opened**. The message MUST name the environment
  variable, the value found, and the registered set. The peer MUST NOT fall back to JSON, ever.
  Test: `codec_test.py::test_unregistered_name_raises_before_socket` (asserts no socket was opened).
- **WSM-INV-015** An unregistered codec name MUST be a loud startup failure, never a silent JSON
  fallback - or a deployment believes it is running msgpack, is not, and may never find out because
  both ends fell back.

### 4.2 Subprotocol assertion (§2.3)

- **WSM-CDC-020** The dialer MUST offer `muxws.v1.<codec>` (e.g. `muxws.v1.json`,
  `muxws.v1.msgpack`) as its **first** WebSocket subprotocol entry, where `<codec>` is its configured
  codec name.
- **WSM-CDC-021** The application MAY append further subprotocol entries (a bearer token is the common
  case). The acceptor MUST match only the entry carrying the `muxws.v1.` prefix and MUST ignore every
  other offered value entirely, leaving them for the application's authentication.
- **WSM-CDC-022** The acceptor MUST accept the connection only if the offered `muxws.v1.<codec>`
  name equals its own configured codec name, and MUST select exactly that value as the negotiated
  subprotocol. Otherwise it MUST refuse the WebSocket handshake: it MUST select **no** subprotocol
  and MUST answer the upgrade with HTTP **400**. It MUST NOT complete the handshake and close
  afterwards where the transport gives it the choice (WSM-CDC-028 is the exception, and only for
  transports that give it none).
- **WSM-CDC-023** This is an assertion, not a negotiation. There MUST NOT be a fallback encoding, a
  list of acceptable alternatives, per-connection multi-codec support, or any runtime codec branching
  in the peer.
- **WSM-CDC-024** A dialer whose handshake is refused MUST surface `CodecMismatch`, **composed by the
  dialer itself from the codec name it offered** - a browser cannot read the rejection body, so the
  diagnostic cannot come from the server. The message MUST name the offered codec and **both**
  environment variables, `VITE_MUXWS_CODEC` and `MUXWS_CODEC`, so the reader knows where to look on
  each side. It MUST NOT surface a bare connection failure.
  Test: `codec_test.py::test_mismatched_codecs_reject_handshake` (asserts no frame was exchanged).
- **WSM-CDC-025** A peer offering a different generation (`muxws.v2.<codec>`) MUST be rejected by a v1
  acceptor at the handshake.
- **WSM-CDC-026** `accept()` MUST perform the WebSocket accept itself (it is the only party that knows
  which subprotocol to select). An application MUST NOT accept the socket before calling it.
- **WSM-CDC-027** For transports that complete the handshake before invoking the handler (the
  `websockets` library), the library MUST expose `select_subprotocol`, a plain callable installable in
  that transport's handshake hook, implementing WSM-CDC-021/022.
- **WSM-CDC-028** Where a transport offers neither hook, the peer MUST verify the negotiated
  subprotocol on the already-open socket and close it with the WebSocket policy-violation close code.
- **WSM-CDC-029** The acceptor MUST log the same failure at refusal time, naming the offered codec,
  its own configured codec and both environment variables. This is the half of the diagnostic that is
  readable where a response body is readable, and it MUST NOT be omitted on the grounds that
  WSM-CDC-024 already reports it - neither message is complete on its own.
- **WSM-CDC-002** `binary` MUST be declared, not inferred from a value's type; the peer uses it to
  select the socket's text or binary send method and the expected inbound message type. A peer MUST
  NOT sniff incoming messages to decide which codec branch to take.
- **WSM-AUT-001** Authentication MUST happen at the WebSocket upgrade, before `accept()` is called.
  muxws MUST NOT interpret credentials anywhere.
- **WSM-AUT-002** muxws MUST NOT interpret per-stream `headers`. They exist for the application, and
  MUST NOT be used for re-authentication by the library.

### 4.3 The adapter seam and packaging (§9.4, §15)

- **WSM-API-021** The socket adapter protocol (`send_text`/`sendText`, `send_bytes`/`sendBytes`,
  `receive`, `close`, plus the handshake hook) MUST be the **only** place transport-specific code
  lives. Text and binary sends MUST be separate methods, never one polymorphic `send`.
- **WSM-API-022** The Node acceptor MUST live behind the `muxws/node` subpath export so
  the browser entry point never pulls in `ws`. The peer implementation MUST be shared; only the socket
  adapter differs.
- **WSM-PKG-002/003** Python keeps zero required runtime dependencies (`starlette` and `websockets` are
  extras selected by which transport module is imported); the TypeScript browser entry keeps zero
  runtime dependencies, with `ws` an optional peer dependency reachable only through `/node`.
- **WSM-CON-011** Native WebSocket ping/pong control frames MUST NOT be used for liveness (browsers do
  not expose them to JavaScript). *(In force here only as a prohibition; the `ping` frame itself is
  M4.)*

### 4.4 TypeScript-specific API rules (§9.2, §9.4)

- **WSM-API-015** In TypeScript `Stream<T>` MUST implement `PromiseLike<T>` (`then`/`catch`/`finally`
  delegating to an internal promise) and MUST NOT subclass `Promise` (species semantics would make
  every derived call construct a bogus `Stream`).
- **WSM-API-016** In TypeScript the implementation MUST attach a default no-op rejection handler to
  the internal promise **at construction time** (not lazily on first `then`), and MUST surface the
  failure through the peer's frame/error hook instead.
  Test: `stream.spec.ts::reset stream nobody consumed reports no unhandled rejection and does reach the
  error hook`.
- **WSM-API-020** `open` and `request` MUST accept `(payload?)`, `(payload?, options?)` **or**
  `(options)`, following the repository's existing positional-or-options-object overload convention.
  `open`'s options object MUST NOT carry `timeoutMs`; `request`'s MUST (WSM-API-018).
- **WSM-API-018** `open()` MUST NOT take a `timeout` argument in either language. `open()` returns
  immediately, so there is nothing for a deadline on it to bound; deadlines live on the awaits,
  `stream.result(timeout=)` and `peer.request(timeout=)`.
- **WSM-API-023** `stream.closed` MUST be `asyncio.Event` in Python and **`Promise<void>` in
  TypeScript**, resolving when the stream closes, including on socket death. TypeScript has no Event
  primitive, and a promise is what composes with `await` and `Promise.race`. The promise MUST resolve
  and MUST NOT reject - a stream that closed by being reset still closed, and the reset reaches the
  awaits and the iterator instead - so WSM-API-016's unhandled-rejection precaution does not apply to
  it.
- **WSM-ERR-004** TypeScript MUST mirror the exception hierarchy with classes of the same names,
  delivered as promise rejections and as `throw` inside `for await`. Both languages MUST set a `name` /
  `__class__` discriminator so cross-language tests can assert on error identity.
- **WSM-ERR-013** On the remote side of a `reset(CANCELLED)`, the handler task MUST be cancelled: in
  Python by `task.cancel()` (the handler observes `asyncio.CancelledError` at its next `await`); **in
  TypeScript by aborting `stream.signal`, with any subsequent `await stream.send(...)` rejecting with
  `StreamReset`.** The TypeScript half is this milestone's; M2 built the Python half.
  Test: `stream.spec.ts` - "remote cancel aborts signal and the next send rejects with StreamReset".
- Naming: module-level factories keep the Python name verbatim (`connect`, `accept`, `serve`);
  `registerCodec` and `selectSubprotocol` are the method-shaped exceptions; classes are PascalCase;
  methods and options-object keys are camelCase; **wire field names stay snake_case**. Durations are
  seconds as floats in Python, milliseconds as integers in TypeScript.

### 4.5 Declarations to implement

```python
async def connect(url: str, *, headers: dict[str, str] | None = None, hello: Any = None,
                  hello_headers: dict[str, Any] | None = None, reconnect: Reconnect | None = None,
                  ping_interval: float = 20.0, ping_timeout: float = 10.0,
                  hello_timeout: float = 10.0,
                  codec: Codec | None = None) -> Peer: ...   # raises if the FIRST attempt fails
async def accept(socket: SocketAdapter, *, codec: Codec | None = None) -> Peer: ...
async def serve(socket: SocketAdapter, *, handler: StreamHandler) -> None: ...
def select_subprotocol(connection: Any, subprotocols: list[str]) -> str: ...  # raises to refuse
```

```ts
export function connect(url: string, options?: ConnectOptions): Promise<Peer>;
export function accept(socket: SocketAdapter, options?: { codec?: Codec }): Promise<Peer>;
export function serve(socket: SocketAdapter, options: { handler: StreamHandler }): Promise<void>;
export function registerCodec(name: string, codec: Codec): void;
export function selectSubprotocol(offered: string[]): string | null;

export interface SocketAdapter {
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}
```

`ConnectOptions` accepts `headers` (node only), `hello`, `helloHeaders`, `reconnect`, `pingIntervalMs`,
`pingTimeoutMs`, `helloTimeoutMs`, `codec`, `onStream`, `onClose`, `onReconnect`. The hello, reconnect
and heartbeat **options are accepted and stored in M3 but not acted on** - they are M5b's. Accepting
them now keeps the signature stable and stops a later milestone rewriting every call site.
`OpenOptions` carries no `timeoutMs` in any milestone (WSM-API-018).

## 5. Decisions this brief takes

Two of the three decisions this brief used to carry are now normative and are reproduced above
instead: `stream.closed` is `Promise<void>` in TypeScript (WSM-API-023, §4.4), and `connect()` raises
when the **first** attempt fails regardless of the reconnect configuration (WSM-RCN-006, below). What
remains is one transport-level detail:

- **D1 - the refusal is an HTTP 400 upgrade denial in both Python transports.** Under `websockets`,
  `select_subprotocol` **raises `NegotiationError`** and the library answers the upgrade with 400.
  Returning `None` does **not** refuse: `websockets` reads it as "no subprotocol selected" and
  completes the handshake with 101, which is what this rule forbids. Only an `InvalidHandshake`
  subclass produces the 400 (any other exception renders 500). This sentence said the opposite until
  M6 checked it against a running server. Under Starlette,
  the endpoint must **deny** the upgrade rather than accept-then-close: send the ASGI denial response
  (`{"type": "websocket.http.response.start", "status": 400}` plus an empty body) before any
  `websocket.accept()`. `websocket.close()` before accept renders 403 and is **not** what
  WSM-CDC-022 asks for. In both cases the acceptor also logs one ERROR line under the `muxws.codec`
  logger naming the offered codec, its own, and both environment variables (WSM-CDC-029) - the
  diagnostic does not travel to the browser, which is exactly why WSM-CDC-024 has the dialer compose
  its own. Where neither hook exists (WSM-CDC-028) the socket is closed with code **1008** (policy
  violation).

Reproduced here because it constrains what `connect()` may return, and this milestone is where
`connect()` is written:

- **WSM-RCN-006** `connect()` MUST await the first attempt and MUST **raise** if it fails, with the
  underlying error, **regardless of the reconnect configuration**. Reconnection applies to connections
  that were established and then lost; it MUST NOT apply to establishing the first one. `connect()`
  MUST NOT return a peer that is retrying in the background.
  Test: `reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`.

## 6. Implementation notes and skeletons

### `muxws/conf.py`

```python
class Settings:
    def __init__(self) -> None:
        self.codec: str = os.environ.get("MUXWS_CODEC", "json")


settings = Settings()
```

Read `settings.codec` at connection time, never at import time, or WSM-CDC-011's "an application may
set it during bootstrap" stops working.

### Resolution order, and where it happens

`connect()` / `accept()` resolve the codec **before** touching a socket:

1. an explicit `codec=` argument wins (WSM-CDC-012);
2. otherwise `get_codec(settings.codec)` - which raises `CodecNotRegistered` naming `MUXWS_CODEC`, the
   value found, and the registered set (WSM-CDC-016).

Only then is the URL dialled or the upgrade accepted. Putting the lookup after `websocket.accept()` is
the single most likely wrong implementation and test 1 below exists to catch it.

### `muxws/transports/starlette.py`

```python
class StarletteSocket:
    """SocketAdapter over starlette.websockets.WebSocket. Imports starlette lazily, inside the
    module, so the package keeps zero required dependencies (WSM-PKG-002)."""

    async def send_text(self, text: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def receive(self) -> str | bytes: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...
```

`accept(websocket)` performs the upgrade itself (WSM-CDC-026): it reads
`websocket.scope["subprotocols"]`, finds the single entry starting `muxws.v1.` (ignoring all others -
WSM-CDC-021), compares its suffix with the configured codec name, then calls
`await websocket.accept(subprotocol=f"muxws.v1.{name}")` on a match, or **denies the upgrade with
HTTP 400** without accepting on a mismatch or on a `muxws.v2.` offer (D1). An application that already
called `websocket.accept()` must get a clear error, not a silent second accept.

### `muxws/transports/websockets_.py`

The `websockets` library completes the handshake before calling the handler, so the decision is handed
to it up front:

```python
def select_subprotocol(connection: Any, subprotocols: list[str]) -> str:  # raises to refuse
    """Installable as websockets.serve(..., select_subprotocol=muxws.select_subprotocol).
    Returns the matching muxws.v1.<codec> value, or None to refuse the handshake (WSM-CDC-027)."""
```

The dialer side offers `[f"muxws.v1.{codec.name}", *application_subprotocols]` - the muxws entry
**first** (WSM-CDC-020) - and, after the socket opens, verifies `connection.subprotocol` and raises
`CodecMismatch` if the server negotiated something else or nothing (WSM-CDC-024/028).

### TypeScript port

`ts/peer.ts` and `ts/stream.ts` are a line-for-line port of M2's `peer.py` / `stream.py` - same state
machine, same id parity, same dispatch rules, same claim-on-first-use handle - with these differences
and no others:

1. `Stream<T> implements PromiseLike<T>, AsyncIterable<T>`. `then` returns an ordinary `Promise`. Do
   **not** `extends Promise` (WSM-API-015).
2. The internal promise gets `void internal.catch(() => {})` **in the constructor**, and the rejection
   is routed to the peer's error hook (WSM-API-016). Attaching it on first `then` is too late: the
   rejection may already have been reported.
3. `closed` is a `Promise<void>` that resolves and never rejects; `signal` is an `AbortSignal`,
   aborted at the same instant, and is what a TS handler observes in place of Python's
   `CancelledError` (WSM-API-023).
4. Durations in options are milliseconds and integers.
5. `ts/conf.ts` reads `import.meta.env.VITE_MUXWS_CODEC` with a `'json'` default, guarding for the
   Node/vitest case where `import.meta.env` may be undefined.
6. `ts/transports/browser-socket.ts` wraps the global `WebSocket`: set `binaryType = 'arraybuffer'`,
   push messages into a queue that `receive()` drains, and reject a pending `receive()` on `close`.
   `new WebSocket(url, ['muxws.v1.json', ...extra])`; on a refused handshake the browser reports only a
   generic error, so throw `CodecMismatch` naming the offered value (WSM-CDC-024).
7. `ts/transports/ws-socket.ts` wraps `ws` and is imported **only** by `ts/node.ts`. Nothing reachable
   from `ts/index.ts` may import it, or the browser bundle pulls in `ws` (WSM-API-022).

### Lint rules that bite in this milestone

- `unicorn/filename-case: kebabCase` - `browser-socket.ts`, `ws-socket.ts`, never `browserSocket.ts`.
- `import/order` with `newlines-between: 'always'` and case-insensitive alphabetical ordering; type-only
  imports still count as imports.
- `no-restricted-syntax` forbids `for...in`: iterate offered subprotocols with `Array.prototype.find`,
  and envelope objects with `Object.entries`.
- `@typescript-eslint/no-unused-vars` is an error; `no-explicit-any` is off, so the handler signature
  `(payload: any, stream: Stream)` is fine as specified.
- Python `B008` fires on a FastAPI `Depends()` in a default argument - the sample endpoint in the docs
  and in `starlette_test.py` needs `# noqa: B008`.
- Python `S101` still bans `assert` outside `*_test.py`; the interop script under `interop/` must
  therefore raise explicitly rather than assert.
- `muxws/transports/websockets_.py` keeps its trailing underscore: a module named `websockets.py`
  inside the package would shadow the library it imports.

## 7. Tests to write

### Python

| # | Test | Asserts |
|---|---|---|
| 1 | `codec_test.py::test_unregistered_name_raises_before_socket` | With `MUXWS_CODEC=msgpack` unregistered, `connect()` raises `CodecNotRegistered` and **no socket was opened** (a dial double records zero calls). The message names the variable, the value and the registered set (WSM-CDC-016). |
| 2 | `codec_test.py::test_mismatched_codecs_reject_handshake` | A `muxws.v1.msgpack` dialer against a `json` acceptor: handshake refused, dialer raises `CodecMismatch`, and **no frame was exchanged** (WSM-CDC-022/024). |
| 3 | `codec_test.py::test_v2_generation_is_rejected` | An offer of `muxws.v2.json` is refused by the v1 acceptor at the handshake (WSM-CDC-025). |
| 4 | `codec_test.py::test_extra_subprotocols_are_ignored` | `['muxws.v1.json', 'bearer.abc123']` is accepted, `muxws.v1.json` is the negotiated value, and the token entry is untouched (WSM-CDC-021). |
| 5 | `codec_test.py::test_no_fallback_to_json_ever` | A mismatch never results in an established connection under any code path (WSM-INV-015). |
| 6 | `conf_test.py::test_env_default_and_runtime_override` | `settings.codec` defaults to `"json"`, reads `MUXWS_CODEC`, and is writable at runtime (WSM-CDC-010/011). |
| 7 | `conf_test.py::test_codec_argument_overrides_settings` | `connect(..., codec=Fake())` wins over `settings.codec` (WSM-CDC-012). |
| 8 | `transports/starlette_test.py::test_accept_performs_the_upgrade` | Against a real Starlette app + `httpx`/`TestClient`: the endpoint never calls `websocket.accept()` itself, the negotiated subprotocol is `muxws.v1.json`, and a unary `request` round-trips (WSM-CDC-026). |
| 9 | `transports/starlette_test.py::test_application_accepting_first_is_an_error` | Calling `accept()` on an already-accepted socket raises a named error, not a silent double accept. |
| 10 | `transports/websockets_test.py::test_select_subprotocol_hook` | `websockets.serve(..., select_subprotocol=muxws.select_subprotocol)` accepts a matching dialer and refuses a mismatched one (WSM-CDC-027). |
| 11 | `transports/websockets_test.py::test_dialer_offers_muxws_entry_first` | The offered list's first entry is `muxws.v1.json` even when the application appended its own (WSM-CDC-020). |
| 12 | `transports/websockets_test.py::test_post_open_verification_closes_with_1008` | A server negotiating a different subprotocol makes the peer close with code 1008 and raise `CodecMismatch` (WSM-CDC-028, D1). |
| 12a | `codec_test.py::test_refusal_is_http_400_and_is_logged_on_both_sides` | The mismatched upgrade is answered with **400** and no subprotocol, under both Starlette and `websockets`; the acceptor emits one `muxws.codec` ERROR naming both codecs and both environment variables (WSM-CDC-022/029), and the `CodecMismatch` the dialer raises names the offered codec plus `VITE_MUXWS_CODEC` and `MUXWS_CODEC` (WSM-CDC-024). |
| 13 | `transports/websockets_test.py::test_real_socket_carries_the_m2_shapes` | Over a real socket: unary, streaming response, bidirectional, notify, cancel - each behaving as M2's memory tests asserted. |
| 14 | `transports/starlette_test.py::test_binary_flag_selects_the_send_method` | A codec double with `binary=True` causes `send_bytes` and never `send_text`; no message sniffing anywhere (WSM-CDC-002, WSM-API-021). |

### TypeScript

`stream.spec.ts` and `peer.spec.ts` mirror **every** M2 test (state-table cells, id parity and
monotonicity, dispatch, claim-on-first-use, socket death failing every shape) against
`ts/transports/memory.ts`. TypeScript-only additions:

| # | Test | Asserts |
|---|---|---|
| 15 | `stream.spec.ts` - "reset stream nobody consumed reports no unhandled rejection and does reach the error hook" | The exact spec-named test: reset a stream nobody awaited or iterated; the `unhandledrejection` listener records nothing and the peer's error hook fired once (WSM-API-016). |
| 16 | `stream.spec.ts` - "is a thenable, not a Promise subclass" | `stream instanceof Promise` is false; `stream.then(...)` returns a plain `Promise`; `Stream` has no `Symbol.species` behaviour (WSM-API-015). |
| 17 | `stream.spec.ts` - "closed resolves and signal aborts on every close path" | Clean end, remote reset, and socket death each resolve `closed` (never reject it) and abort `signal` (WSM-API-023). |
| 18 | `peer.spec.ts` - "open accepts payload, payload+options, or options alone" | All three overload forms produce the same `open` frame, and `OpenOptions` has no `timeoutMs` while `RequestOptions` does (WSM-API-020, WSM-API-018). |
| 19 | `conf.spec.ts` - "defaults to json and honours VITE_MUXWS_CODEC" | Including the case where `import.meta.env` is undefined (WSM-CDC-010). |
| 20 | `browser-socket.spec.ts` - "offers muxws.v1.json first and reports CodecMismatch on refusal" | Against a mock `WebSocket`; `binaryType` is `'arraybuffer'` (WSM-CDC-020/024). |
| 21 | `node.spec.ts` - "index does not import ws" | Static assertion over the built `dist/index.js` that neither `ws` nor `ws-socket` appears in it (WSM-API-022, WSM-PKG-003). |

### Cross-language interop (CI)

| # | Test | Asserts |
|---|---|---|
| 22 | `interop/` - Python acceptor, TypeScript dialer | A real socket carries a unary request, a streaming export, a server push and a cancel; every payload arrives equal on both sides under the `json` codec. |
| 23 | `interop/` - TypeScript acceptor (`ws`), Python dialer | The same script in the reverse role assignment (WSM-TST-004's first half; the full matrix is M6). |
| 24 | `interop/` - codec mismatch | A `msgpack`-configured dialer against a `json` acceptor fails the handshake in both role assignments, and neither side exchanges a frame. |

## 8. Done when

- [ ] `ruff check .`, `ruff format --check .`, `pytest --cov=muxws` all pass.
- [ ] `npm run lint`, `npm test`, `npm run build` all pass; TS coverage of `ts/peer.ts` and
      `ts/stream.ts` is above 95 %.
- [ ] The TypeScript suite runs the **same** M2 assertions as Python and both pass on
      `conformance/frames/` and `conformance/invalid/`.
- [ ] `grep -R "ws" dist/index.js` finds no import of the `ws` package.
- [ ] `MUXWS_CODEC=nope python -c "import asyncio, muxws; asyncio.run(muxws.connect('ws://localhost:1'))"`
      prints a `CodecNotRegistered` naming `MUXWS_CODEC`, `nope` and `json`, and makes no TCP
      connection.
- [ ] Both interop role assignments run green in CI.
- [ ] No `if codec == ...` branch exists anywhere in `peer.py` / `peer.ts` (WSM-CDC-023).

## 9. Out of scope

`ping`/`pong` frames and `peer.ping()`, `goaway` with drain, `error_serializer` on `connect()` /
`accept()`, and graceful `close()` - all M4. Fragmentation wired into the send path, the
one-unsent-fragment rule, the round-robin writer, the three receive-side caps (frame size,
`max_payload_bytes`, the local concurrency limit answering with `REFUSED`) and the `on_frame` logging
shape - M5a. `PeerRegistry`, `peer.tags`, socket-death fan-out and the whole reconnect helper (hello
replay, backoff, heartbeat) - M5b. M3 accepts their options and stores them without acting on them.
The msgpack codec and its cross-language pair, `conformance/sequences/`, and the full both-roles
conformance matrix - M6. There is **no post-socket handshake phase at all**: a connection is
established when the socket is open and the subprotocol was accepted (WSM-CON-030), and a peer may
open a stream on its first frame.
