---
title: muxws M7 - documentation
sidebar: false
search: false
outline: deep
---

# muxws M7 - developer documentation

## 1. Goal

At the end of M7 the repository ships a VitePress documentation site in a `docs/` workspace, laid out
exactly like `@dynamicforms/fastapi-viewsets` - a home page, a `guide/` narrative and an `api/`
reference, with a configured sidebar. A developer who has never seen muxws can read the rationale,
copy the quick start into two files, run them, and watch a stream open; and a developer who already
uses muxws can look up **any** public symbol in either language and find its signature, every
parameter, the return value, what it raises, and one example that runs. "Complete" is not a judgement
call here: §5 defines it, and §7 turns it into a test that fails when a symbol is added without a
page entry.

This milestone was not in the original design document's milestone list; M6 there read "conformance,
documentation, wire freeze". The documentation half is split out because it is the only milestone
whose output is read by humans rather than executed by CI, and because it must document the frozen
1.0 surface rather than a moving one.

## 2. Prerequisites

M0-M6 are done, the wire is frozen and the repository is tagged `v1.0.0`. **M0 already created the
`docs/` workspace, `docs/package.json`, `docs/.vitepress/config.ts`, `docs/index.md` and the three
stub pages, and already added `docs` to the root `workspaces` array with the three delegating
scripts.** This milestone fills them in; the rows below marked *modify* say so. Everything this milestone
documents already exists and is tested. In particular M6 left `SPEC.md` at the repository root (the
normative rules, with rule ids) and `conformance/README.md` (the fixture schemas). **This site does
not replace `SPEC.md` and MUST NOT restate its rule ids as prose** - it links to it once, from the
architecture page, and otherwise documents behaviour rather than obligations.

The public surface being documented is the one declared in §9 of the specification. It has not
changed since M6.

## 3. Files to create or modify

| Path | Action |
|---|---|
| `docs/package.json` | modify (M0 created it) - confirm the workspace name and the three scripts |
| `docs/.vitepress/config.ts` | modify (M0 created it) - fill in the two sidebars from §4 and §5 |
| `docs/index.md` | modify (M0 created it) - `layout: home` hero + six features |
| `docs/guide/*.md` | create - the pages listed in §4 (`getting-started.md` already exists as a stub) |
| `docs/api/*.md` | create - the pages listed in §5 (`index.md` already exists as a stub) |
| `docs/examples/*` | create - the copy-pasteable quick-start sources, imported into the guide via VitePress code snippet includes |
| `package.json` (root) | modify - `docs` is already in `workspaces` and the three `docs:*` scripts already delegate; add only `docs:check` running `node docs/check-docs.mjs` |
| `docs/check-docs.mjs` | create - the completeness checker of §7 |
| `docs/examples/run_examples_test.py` | create - executes the Python examples |
| `docs/examples/examples.spec.ts` | create - executes the TypeScript examples |
| `.github/workflows/docs.yml` | create - builds the site and runs both example suites |
| `README.md` (root) | modify - a 30-line front door that links to the site, not a second manual |

Mirror the reference repository exactly: `docs/` is a workspace inside the same repo, VitePress lives
under `docs/.vitepress/`, and the root `package.json` scripts delegate. TypeScript file names stay
kebab-case; Markdown file names are kebab-case too (`getting-started.md`, `connection-lifecycle.md`).

## 4. The guide - what each page must contain

Every page ends with a "See also" line linking the API pages for the symbols it used. Every code
block is either runnable as shown or explicitly marked `// fragment`.

| Page | Must contain |
|---|---|
| `guide/rationale.md` | Why one WebSocket carrying many independent, individually cancellable streams beats N sockets or a request/response envelope of your own. The three things muxws is not: not a router, not a serializer of domain objects, not an RPC framework. The symmetry claim: one `Peer` type per language, so server push is the same mechanism as a client request, with the same correlation and cancellation story. Ends with the one link to `SPEC.md`. |
| `guide/getting-started.md` | `## Installation` (pip and npm, plus the extras: `muxws[starlette]`, `[websockets]`, `[msgpack]`; `muxws` with `ws` and `@msgpack/msgpack` as optional peers) and `## Quick Start` - see §6, which is normative for this section. |
| `guide/architecture.md` | The five moving parts and where each lives: the codec seam, the frame layer, the stream state machine, the writer, the reconnect helper. One diagram of a frame's path: application value → codec encode → fragmentation → writer round-robin → socket adapter → wire, and the mirror image inbound. The `SocketAdapter` port as the *only* transport-specific code. |
| `guide/call-shapes.md` | The four shapes as a table (unary, streaming response, bidirectional, one-shot push) with the wire sequence and both languages' code side by side. That `open()` is **synchronous** and returns a `Stream`. That a `Stream` is *both* awaitable (first payload) and async-iterable (every payload), that the first use claims it, and that the other then raises `StreamAlreadyConsumed`. That `request()` additionally fails if the remote sent more than one payload while `await stream` does not. |
| `guide/streams-and-cancellation.md` | The five stream states and what moves between them, as a table. `end` as a flag, never a frame. Trailers riding the `end` frame. `cancel()` closing locally and immediately without waiting for an ack, and what the remote handler observes (`asyncio.CancelledError` in Python, an aborted `stream.signal` in TypeScript). Local `CancelledError` propagating out of an `await` sending `reset(CANCELLED)` and re-raising. |
| `guide/connection-lifecycle.md` | That a connection is established the moment the socket is open with the `muxws.v1.<codec>` subprotocol accepted, and that there is **no** handshake phase, capability exchange or frame either side must send first - a peer may open a stream on its first frame. That no limit is ever negotiated: `MAX_FRAME_BYTES` is a protocol constant, and `max_payload_bytes` and `max_concurrent_streams` are each one peer's own defence, which the other learns about only from the reset it provokes. `ping`/`pong` and `peer.ping()`. `goaway`, `last_stream`, the drain window, and `peer.close()`. |
| `guide/sizes-and-fragmentation.md` | That every limit is counted in bytes of **encoded output**, not payload characters - with the worked example of a non-BMP string where `str.length` and Python's byte count disagree. That `MAX_FRAME_BYTES` is a **protocol constant of 64 KiB and deliberately not configurable**, with the reason stated plainly: a larger frame is worse, not better, because the cap bounds how long one stream can monopolise a socket that has a single global message order - making it negotiable would optimise the wrong direction. That fragmentation is automatic and mandatory, that a stream holds at most one unsent fragment, and that the writer round-robins - so a 1 MB export does not stall a 200-byte progress update. What `PAYLOAD_TOO_LARGE` means on each side, that `max_payload_bytes` (64 MiB) is the *receiver's* own limit and is checked as fragments arrive rather than after reassembly, and how to raise it - on the receiving peer, since the sender has no say. |
| `guide/reconnect.md` | The dialer-only helper, and the first thing it does not do: `connect()` **raises** when the first attempt fails, whatever reconnection is configured, because a typo in the URL that retried forever would never surface. The backoff formula with the defaults table. That the attempt counter resets only on an **established** connection (socket open with the subprotocol accepted, plus the hello acknowledged) and why resetting on socket-open turns backoff into a hammer. The heartbeat and its `ping_interval + ping_timeout` detection bound. The `hello`: captured once at `connect()`, replayed verbatim, delivered to the acceptor's ordinary `on_stream` handler, acknowledged simply by that handler returning. What a reconnect restores - a live socket and an accepted identity, and **nothing else**: no stream survives, `tags` start empty. The documented, deliberately un-implemented recommendation for client identity: `sessionStorage`, not `localStorage` (shared across tabs) and not module scope (dies on reload). |
| `guide/errors.md` | The exception tree as a tree. The nine reset codes with the *required reaction* column, verbatim from the specification's table - a reader must be able to decide "retry now / back off / do not retry" from this page alone - and one line saying that `5` is a retired number that never appears. The three ways a send can fail on a stream that is no longer open, and why they are different classes: `StreamClosed` (it ended normally - an expected race, not a bug), that stream's own `StreamReset` (the remote objected), `ConnectionLost` (the socket died). Why `ConnectionLost` is a `StreamReset` and `ConnectionClosed` is not. What every stream-shaped call does when the socket dies, and that nothing is buffered while the peer is between sockets. That `error_serializer` is chosen per peer, so one process can redact on its browser-facing connection and not on its internal one. |
| `guide/registry.md` | `peer.tags` as an ordinary dict that dies with the socket. `PeerRegistry.register` / `registered` / `peers_for`. The usage rule in bold: **look up on keys you do not mutate, and mutate keys you do not look up**; if you need both on one key, call `register(peer)` after each write. That the registry is per-process, with one paragraph on what a multi-process deployment must do itself. |
| `guide/codecs.md` | Selecting a codec by environment (`MUXWS_CODEC`, `VITE_MUXWS_CODEC`), that both ends must agree because the `muxws.v1.<codec>` subprotocol asserts it, and that an unregistered name fails loudly at startup instead of falling back. Registering `msgpack`. Writing your own `Codec`. That bytes are a payload type under a binary codec and are *not* base64-encoded for you under JSON. |
| `guide/transports.md` | One runnable snippet each: FastAPI/Starlette acceptor, `websockets` acceptor with `select_subprotocol`, `websockets` dialer, browser dialer, Node `ws` acceptor via `muxws/node`. Where authentication belongs - at the upgrade, before `accept()`, never in the hello and never in per-stream `headers`. |
| `guide/observability.md` | The `muxws.frames` logger and its one-line format, with a sample. `peer.on_frame(handler)` and its `(direction, frame, byte_length)` signature. The explicit statement that payload contents are never logged, and the matching warning that `error_serializer` **does** put exception text on the wire, so a public-facing deployment should replace it with a redacting one. |
| `guide/interop.md` | That the JSON wire form is frozen at 1.0, and that the `muxws.v1.` subprotocol prefix is the **only** version on the wire: it pins the breaking-change generation, and additive revisions are announced nowhere because unknown frame types and unknown envelope fields are already required to be ignored. How to read `conformance/` if you are writing a third implementation. |

## 5. The API reference - and what "complete" means

`docs/api/` is complete when **every public symbol in both languages** has an entry, and every entry
carries all five of:

1. **Signature** - the full declaration, in a fenced `python` or `ts` block, exactly as it appears in
   the source (including keyword-only markers and default values).
2. **Parameters** - a table with every parameter: name, type, default, and what it does. No parameter
   may be omitted, including the ones the reader will never pass.
3. **Return** - the type and what it means; `None` / `void` stated explicitly rather than left out.
4. **Raises** - every exception class that can come out of that call, each with the condition. A
   symbol that raises nothing says "Raises: nothing."
5. **Example** - one fenced block that **runs as written** against the in-memory transport or a
   localhost peer, and is executed by the test of §7. Not a fragment, not pseudocode, no `...`.

Duration parameters must state their unit at every occurrence: seconds as floats in Python,
milliseconds as integers in TypeScript. Do not write "timeout" without a unit anywhere on the site.

The pages and the symbols each one must cover:

| Page | Python symbols | TypeScript symbols |
|---|---|---|
| `api/connect.md` | `connect` | `connect`, `ConnectOptions` |
| `api/accept.md` | `accept`, `serve`, `select_subprotocol` | `accept`, `serve`, `selectSubprotocol` |
| `api/peer.md` | `Peer.open`, `.notify`, `.request`, `.on_stream`, `.on_close`, `.on_reconnect`, `.on_frame`, `.ping`, `.close`, `.serve`, `.id`, `.tags`, `.streams`, `.is_open` | `Peer` with the same members, camelCase (`onStream`, `isOpen`, …), plus `OpenOptions` |
| `api/stream.md` | `Stream.id`, `.headers`, `.payload`, `.send`, `.end`, `.reply`, `.__await__`, `.result`, `.cancel`, `.reset`, `.__aiter__`, `.trailers`, `.closed` | `Stream` with the same members plus `then`, `[Symbol.asyncIterator]`, `signal` |
| `api/reconnect.md` | `Reconnect` and all five fields | `ReconnectOptions` and all five fields |
| `api/errors.md` | `MuxwsError`, `ProtocolError`, `ConnectionClosed`, `ConnectionGoingAway`, `StreamAlreadyConsumed`, `StreamClosed`, `CodecError`, `CodecNotRegistered`, `CodecMismatch`, `StreamReset`, `RemoteError`, `StreamTimeout`, `StreamRefused`, `ConnectionLost`, `ResetCode` (all nine members), `default_error_serializer` | the same class names, `ResetCode`, `defaultErrorSerializer` |
| `api/codec.md` | `Codec` protocol, `JsonCodec`, `MsgpackCodec`, `register_codec`, `muxws.conf.settings` | `Codec`, `JsonCodec`, `MsgpackCodec`, `registerCodec`, the `/msgpack` subpath |
| `api/registry.md` | `PeerRegistry.register`, `.registered`, `.peers_for` | `PeerRegistry` with the same members |
| `api/transports.md` | `SocketAdapter` protocol, the Starlette adapter, the `websockets` adapter, the in-memory adapter | `SocketAdapter`, the browser adapter, the `ws` adapter behind `muxws/node` |
| `api/types.md` | `CloseReason` (all four fields), `ErrorSerializer`, `MAX_FRAME_BYTES`, `StreamHandler`, `Frame` and every envelope field | `CloseReason` (the same four), `ErrorSerializer`, `MAX_FRAME_BYTES`, `StreamHandler`, `Frame` |

A symbol exported from `muxws/__init__.py` or `ts/index.ts` (or a documented subpath) and absent from
this table is a bug in this brief - add the row rather than skipping the symbol.

Four places where a page written from memory rather than from the source will be wrong, and where
test 4 (documented signatures against `inspect.signature`) is the backstop:

- **`Peer.open` takes no `timeout`**, and `OpenOptions` carries no `timeoutMs`. Deadlines belong to
  the calls that wait: `stream.result(timeout=)` / `result({ timeoutMs })` and
  `peer.request(timeout=)` / `request(..., { timeoutMs })`. The "Raises" section of `open` lists
  exactly `ConnectionGoingAway` and `ConnectionLost` - nothing about concurrency.
- **`connect()` and `accept()` carry `max_payload_bytes`, `max_concurrent_streams` and
  `error_serializer`**, each documented as a limit or hook belonging to *this peer alone*, never
  announced and never negotiated. `connect()`'s "Raises" says plainly that a failed first attempt
  raises even with reconnection configured.
- **`Stream.closed` is `asyncio.Event` in Python and `Promise<void>` in TypeScript**, and the
  TypeScript promise resolves rather than rejecting on a reset.
- **`peer.id`** is documented as a per-process prefix plus a per-connection counter, with the note
  that it is not unique across processes and that it changes when a reconnected peer takes a new
  connection.

## 6. The quick start - normative

`guide/getting-started.md`'s `## Quick Start` must be copy-pasteable into a fresh directory and work.
It is built from files under `docs/examples/`, included with VitePress snippet syntax
(`<<< @/examples/quickstart_server.py`) so the page and the executed file can never drift.

The quick start must, in this order:

1. Install both packages (two `bash` blocks).
2. `docs/examples/quickstart_server.py` - a FastAPI app with one WebSocket route that calls
   `accept(...)`, registers an `on_stream` handler which replies to a unary request **and**
   streams three chunks for a different payload, and runs under `uvicorn`. Complete file, imports
   included.
3. `docs/examples/quickstart_client.py` - a `websockets` dialer that calls `connect(...)`, does one
   `await peer.request(...)`, then one `async for` over `peer.open(...)`, and prints both. Complete
   file.
4. `docs/examples/quickstart-client.ts` - the same two calls from Node against the same server, with
   the import from `muxws` and the socket from `muxws/node`.
5. The exact expected output of each client, in a fenced block.
6. `## Next: server push` - three more lines showing the acceptor calling `peer.open(...)` on its own
   and the dialer receiving it through *its* `on_stream` handler. This is the symmetry claim made
   concrete, and it belongs in the quick start rather than in an advanced page.
7. `## Next: surviving a disconnect` - adding `hello=` and `reconnect=Reconnect()` to `connect()`,
   with a one-sentence statement of what that buys and what it does not (streams do not survive).

No step may say "see the guide". A reader who stops after the quick start must have a working
bidirectional connection.

## 7. Tests to write

Documentation gets tests because the alternative is documentation that rots.

1. `docs/check-docs.mjs::api-coverage` - imports `ts/index.ts` (plus the `/node` and `/msgpack`
   subpaths) and enumerates its exports; parses every `docs/api/*.md`; fails naming any exported
   symbol with no entry, and any documented symbol that no longer exists. Run from `npm run
   docs:check`.
2. `docs/check-docs.mjs::entry-shape` - for every symbol entry, asserts the presence of all five
   required sections (signature, parameters, return, raises, example) by heading. A missing "Raises"
   is a failure even when the answer is "nothing".
3. `muxws/docs_test.py::test_every_public_python_symbol_is_documented` - walks `muxws/__init__.py`'s
   `__all__` and the public members of `Peer`, `Stream`, `PeerRegistry` and the error classes, and
   asserts each appears in `docs/api/`. The Python mirror of test 1.
4. `muxws/docs_test.py::test_documented_signatures_match_the_source` - compares each documented
   Python signature against `inspect.signature`; a renamed parameter or a changed default fails here
   rather than in a user's editor.
5. `docs/examples/run_examples_test.py::test_quickstart_runs_end_to_end` - starts
   `quickstart_server.py` on an ephemeral port, runs `quickstart_client.py` against it, and asserts
   stdout equals the block printed in `getting-started.md`.
6. `docs/examples/examples.spec.ts::quickstart client produces the documented output` - the same
   against the TypeScript client, asserting the identical output (this is also a cheap cross-language
   check).
7. `docs/examples/run_examples_test.py::test_every_api_example_executes` - extracts every fenced
   `python` block under an `### Example` heading in `docs/api/*.md`, runs each in a subprocess with a
   memory-transport peer pair available, and fails on a non-zero exit. The matching
   `examples.spec.ts::every api example executes` does the same for `ts` blocks.
8. `docs/check-docs.mjs::no-unitless-durations` - greps the site for `timeout`, `interval` and
   `delay` in parameter tables and fails any row whose type column does not say `seconds` or
   `milliseconds`.
9. `docs/check-docs.mjs::no-dead-links` - VitePress's own build already fails on dead internal links;
   assert the build runs with `ignoreDeadLinks` limited to `[/^http:\/\/localhost/]`, exactly as the
   reference repository does, so a broken guide link is a build failure.
10. `docs/check-docs.mjs::spec-is-linked-once` - `SPEC.md` is linked from `guide/architecture.md` and
    from nowhere else, so the site never becomes a second, drifting copy of the normative rules.

## 8. Implementation notes

- **Copy the reference site's configuration shape, not its content.** `docs/.vitepress/config.ts`
  uses `defineConfig`, a `themeConfig.nav` of Home / Guide / API Reference, a `sidebar` keyed by
  `'/guide/'` and `'/api/'` with nested `items`, `socialLinks` pointing at the muxws GitHub
  repository, and a footer reading "Released under the MIT License." with
  "Copyright © 2025 Jure Erznožnik". muxws has no Vue components, so the reference repo's
  `vite-plugin-vuetify` block is **omitted** - do not copy it in.
- **`docs/index.md` is `layout: home`** with a hero (name, text, tagline, three actions: Get Started
  → `/guide/getting-started`, API Reference → `/api/connect`, GitHub) and six `features` entries.
  Write the features from what muxws does: many streams on one socket, symmetric peers, automatic
  fragmentation, reconnect with hello replay, pluggable codec, two languages one wire.
- **Examples are files, not prose.** Every example that the tests of §7 execute lives under
  `docs/examples/` and is included with `<<< @/examples/name.py`. A code block typed directly into a
  Markdown page cannot be executed and will rot; the only exception is a `// fragment`-marked
  block that is explicitly not runnable.
- **Sidebar order is reading order.** Guide: Rationale, Getting Started, then Concepts (Architecture,
  Call shapes, Streams & cancellation, Connection lifecycle, Sizes & fragmentation, Reconnect,
  Errors), then Integration (Transports, Codecs, Registry, Observability, Interop). API: the ten
  pages of §5 in the order listed.
- **Do not restate rule ids.** The guide explains behaviour; `SPEC.md` carries `WSM-*` ids. A reader
  who needs the id follows the one link. Test 10 enforces this.
- **Lint that will bite here:**
  - `docs/examples/*.py` are shipped source, so `S101` applies: no bare `assert` in an example -
    print instead. The `*_test.py` runner may assert.
  - Example Python must satisfy the same ruff config as the library: 120 columns, double quotes,
    `X | None`, isort with a blank line between `import x` and `from x import y`. Add
    `docs/examples` to the ruff target paths so `ruff check .` covers it.
  - Example TypeScript must satisfy the same eslint config: **single quotes**, `printWidth: 120`,
    `import/order` with `newlines-between: 'always'` and alphabetized imports, no `for...in`.
  - `unicorn/filename-case: kebabCase` - `quickstart-client.ts`, never `quickstartClient.ts`. Python
    example files stay snake_case (`quickstart_client.py`); this asymmetry is correct and matches the
    rest of the repository.
  - `[tool.coverage.run] omit` must exclude `docs/examples/*` so example files do not dilute library
    coverage.

## 9. Done when

```bash
ruff check . && ruff format --check .
pytest muxws docs -q
npm run lint
npm test
npm run docs:check          # docs/check-docs.mjs
npm run docs:build          # vitepress build, dead links fatal
```

- [ ] All six commands pass.
- [ ] `npm run docs:dev` serves a site with a working sidebar for both `/guide/` and `/api/`, and a
      home page with the hero and six features.
- [ ] Every page listed in §4 exists and contains what its row requires.
- [ ] Every symbol in the §5 table has an entry with all five sections; tests 1-4 prove it.
- [ ] Observable: a reader copies the three quick-start files into an empty directory, runs
      `pip install muxws[starlette,websockets]` and `npm i muxws ws`,
      starts the server, runs either client, and sees exactly the documented output. Tests 5 and 6
      run this same path in CI.
- [ ] Every `### Example` block in `docs/api/` executes (test 7).
- [ ] No duration appears in any parameter table without `seconds` or `milliseconds` (test 8).
- [ ] `SPEC.md` is linked exactly once, from `guide/architecture.md` (test 10).
- [ ] The `error_serializer` redaction warning appears in `guide/observability.md` **and** on the
      `error_serializer` parameter row in `api/accept.md` and `api/connect.md`.
- [ ] The root `README.md` is under 40 lines and links to the site rather than duplicating it.

## 10. Out of scope

- **Changing the library.** If documenting a symbol reveals a bad signature, file it; do not change
  the frozen 1.0 surface in a documentation milestone.
- **`SPEC.md` and `conformance/README.md`** - written in M6 and unchanged here.
- **Tutorials for backchannel or any consumer of muxws.** They document themselves; this site
  documents the transport.
- **A hosted deployment, a custom theme, versioned docs, i18n, or search beyond VitePress's
  default.**
- **Implementing client identity storage.** `sessionStorage` is a *recommendation* on the reconnect
  page; muxws mints and stores nothing.
