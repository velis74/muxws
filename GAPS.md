# Gaps

Open gaps between `SPEC.md`, the briefs under `docs/design/briefs/`, the implementations and their
tests, and known deviations of v1 from `SPEC.md`. A gap is deleted when it is closed.

## Known deviations

### `WSM-SID-001`: `Stream`'s constructor takes a stream id

`Stream` is a value export from `ts/index.ts` and a member of `muxws.__all__`, and its constructor
takes a stream id as its second positional argument in both ports. A caller can therefore put an id
on the wire that the peer never allocated:

```python
forged = Stream(peer, 999, local=True)
await forged.send({"forged": True})
# tx {"type":"data","stream":999} -> rx goaway: data on stream 999, above the high-water mark 0
```

No stream-originating call (`open`, `request`, `notify`, `send`, `end`) takes an id.
`peer_test.py::_ID_ARGUMENTS_THAT_ARE_NOT_AN_ALLOCATION` pins the constructor as the single
exception, so a further id-taking entry point fails the test. Closing the gap requires an internal
`Stream` constructor and a factory owned by the peer, which is a public API change.

### `WSM-ERR-016`: two unparseable addresses do not raise a `MuxwsError`

- The `connect()` exported from `muxws` calls `new WebSocket(url, …)` and lets the constructor's
  exception propagate. A malformed URL arrives as a `DOMException` named `SyntaxError`
  (`The URL 'nonsense' is invalid.` under jsdom) or as `TypeError: Invalid URL` (undici). Neither is
  a `MuxwsError`. A `try`/`catch` around the constructor call is not a fix: jsdom throws the same
  shape for an invalid subprotocol token. A conforming implementation claims the exception only when
  the URL is independently unparseable as a `ws:`/`wss:` address, rethrows everything else, and keeps
  the platform's wording on `cause`. `docs/api/errors.md` and `docs/api/connect.md` document the
  deviation.
- In Python, `parse_unix_url` reads the scheme with `urllib.parse.urlsplit`, which raises a bare
  `ValueError` for an authority with an unbalanced `[`. `ws://[::1` and `ws+unix://[x` leave
  `connect()` as a `ValueError` that is not a `MuxwsError`. Every other unparseable address reaches
  `verify_dialable_url` and is raised as `WebsocketUrlError`.

### `WSM-OBS-001`: the two ports quote `reason=` differently

Python renders `reason=` with `repr()`, TypeScript with `JSON.stringify`. Each port's test asserts its
own form. `SPEC.md` does not state which form is correct.

## Rules with an incomplete witness

- **`WSM-CDC-007`'s quantifier.** The cross-language workflow runs legs for `json` and `msgpack`, and
  `conformance_test.py` pins the configured set as `{JsonCodec.name, MsgpackCodec.name}`. No test
  asserts that `registered_codecs()` equals the set of codecs the CI legs cover, so a third codec
  registered in both ports passes every test without a leg.
- **`WSM-RCN-028`, library clause, TypeScript.** No TypeScript test asserts that the shipped bundle
  references neither `localStorage` nor `sessionStorage`.
- **`WSM-FRG-019`, TypeScript.** `ts/writer.spec.ts` asserts the absence of a send-path FIFO by
  counting the annotation `Frame[]` in `ts/writer.ts`. An array whose element type is inferred is not
  counted.
- **`WSM-FRG-019`, interop.** The interleaving assertion in `interop/runner.py` and
  `interop/runner.ts` is satisfied by the handlers' sleeps and passes without round-robin selection.
  The rule's witnesses are `writer_test.py` and
  `conformance/sequences/small-frame-overtakes-a-fragmented-payload.json`.
- **`WSM-FRM-006`, Python.** `peer_test.py::test_an_envelope_lookalike_payload_survives_being_fragmented`
  compares against the object passed to the library, so a library that deletes a reserved key from
  the caller's own dict passes it. The TypeScript counterpart snapshots the payload before the send.
- **`WSM-AUT-002`, TypeScript.** `ts/peer.spec.ts` *"never interprets per-stream headers"* compares
  stream-level frames only, so an acceptor that emits an extra connection-level frame on reading a
  header passes it. The Python counterpart compares the whole outbound frame list.
- **`WSM-ERR-010`.** The Python witness detects a deadline armed through `asyncio.wait_for`, and the
  TypeScript witness one armed through `setTimeout`. A default deadline built another way is not
  detected. A complete witness needs a controllable clock in both ports.
- **`WSM-ERR-003`.** The witness pins the set of hook names. It does not assert that an observer
  cannot consume an error; the rule holds because no hook can receive one.
- **Fragmented `reset` frames.** No fixture asserts that the splitter puts `reason` on every fragment
  of a `reset`. `fragmented-reset-repeats-code-and-reason-on-every-fragment` pins how such a frame
  decodes, and `v1-fragment-boundaries.json` covers payload cut points only.

## Rules witnessed in one port only

- `WSM-API-016` is a TypeScript rule whose only citation is in `stream_test.py`. The TypeScript test,
  `ts/stream.spec.ts` *"reaches the error hook and raises no unhandled rejection"*, does not name
  it.
- `WSM-CDC-026` has no TypeScript witness: no TypeScript test asserts that `accept()` performs the
  upgrade or that the application must not accept first.
- `WSM-AUT-003`, library clause: no TypeScript test asserts that each reconnect of `muxws/node`'s
  `connect()` presents its handshake `headers` at a fresh upgrade.
- `WSM-API-008`, `WSM-AUT-004`, `WSM-ERR-003`, `WSM-ERR-007` have Python witnesses only.
- Cited in one port, with an uncited equivalent in the other: `WSM-API-012`, `WSM-FRG-011`,
  `WSM-SID-008`, `WSM-STM-010`, `WSM-TST-001` (cited in Python); `WSM-API-002`, `WSM-API-023`,
  `WSM-AUT-001` (cited in TypeScript).

## Rules held by something other than a citing test

- `WSM-PKG-004` is enforced by `eslint` and `ruff` (`npm run lint:ci`, `ruff check .`), not by a test.
- The "zero mandatory arguments" half of `WSM-API-003` is enforced in TypeScript by `tsc --noEmit`.
- `WSM-TST-004` and `WSM-TST-005` are enforced by the `cross-language` workflow's steps.
- These rules have no citation and are held by a test that does not name them: `WSM-API-013`,
  `WSM-BPR-002`, `WSM-CDC-003`, `WSM-FRG-033`, `WSM-FRM-010`, `WSM-FRM-011`, `WSM-FRM-014`,
  `WSM-INV-003`, `WSM-INV-009`, `WSM-SID-003`, `WSM-STM-012`, `WSM-STM-013`, `WSM-STM-015`. Renaming
  or changing such a test does not report that the rule lost its witness.

## Platforms and environments not exercised

- **Windows.** Every workflow runs on `ubuntu-latest`. On Windows, Python's `connect()` is specified
  to raise `UnixSocketsUnsupportedError` for a `ws+unix:` URL, and a `muxws/node` dial of a
  POSIX-style path to fail with a connect error naming the path. Neither behaviour runs in CI. The
  Unix tests skip where `AF_UNIX` is absent, so a Windows run reports green without executing the
  transport. Closing this needs a `windows-latest` leg in `ci.yml` and two Windows-only tests.
- **A real browser.** `ts/browser-socket-live.spec.ts` runs `BrowserSocket` against Node's global
  `WebSocket`. No test runs in a browser, and no test exchanges raw bytes under msgpack over a
  platform `WebSocket`; `browser-socket.spec.ts` asserts only that `binaryType` is assigned
  `'arraybuffer'`.
- **`websockets` older than 14.** The first dial fails with
  `TypeError: BaseEventLoop.create_connection() got an unexpected keyword argument 'additional_headers'`,
  which names neither muxws nor the required version. `WebsocketsNotInstalledError` does not cover it,
  because the import succeeds.
- **`muxws.select_subprotocol()` without `websockets`** raises a bare `ImportError`. Its only caller is
  `websockets.serve(...)`, which cannot run without the package.

## Open design gaps

- **`refuseMismatchedUpgrade` is opt-in.** A `ws` server that installs only `handleProtocols` answers
  HTTP 101 where `WSM-CDC-022` requires 400. `accept()` does not detect that the hook is missing.
- **Fragmentation cost.** The reservation `min(512, cap // 2)` (`WSM-FRG-013`) is smaller than the
  escaping a 64 KiB JSON-in-JSON fragment needs, so `_largest_fitting_count` runs its binary search on
  nearly every fragment. A larger reservation changes the cut points, which `WSM-FRG-016` and
  `conformance/frames/` freeze; it is a wire change for a new generation.
  `fragment_test.py::test_encode_calls_to_split_one_megabyte_stay_under_the_bound` sets a ceiling on the cost.
- **No per-type codec extension.** `register_codec` takes a whole codec. There is no
  `register_type(cls, tag, encode, decode)` that would map to msgpack ext types under msgpack and to a
  codec-owned sentinel elsewhere. Such a registry makes the tag numbering part of the wire contract,
  with its own fixtures (`WSM-CDC-006`) and cross-language pair (`WSM-CDC-007`). `MsgpackCodec` does
  not enable msgpack's timestamp type; enabling it changes what `muxws.v1.msgpack` carries.
- **No non-WebSocket adapter.** Every shipped adapter carries a WebSocket. An adapter over a framed
  byte stream (raw AF_UNIX, stdio, plain TCP) or `MessagePort` has two problems the four
  `SocketAdapter` signatures do not show: codec agreement, carried today by the subprotocol
  (`WSM-CDC-022/027/028`), and a mapping for the WebSocket close codes the close path uses
  (`WSM-RCN-011`, `WSM-CDC-028`). Neither is specified.
- **No rate bound.** `WSM-BPR-002`'s two mechanisms bound concurrency and per-frame latency, not rate.
  A producer faster than its consumer fills the socket's send buffer, the writer's queues and then
  memory. Per-stream credit needs a `window_update` frame and a protocol-constant initial window,
  which is a new generation (`WSM-CON-009`). Exposing `writer.depth` would let an application observe
  its own queue without a wire change.
- **`V1_FRAME_TYPES` mutability.** It is a `frozenset` in Python and a mutable `Set` typed
  `ReadonlySet` in TypeScript, so `(V1_FRAME_TYPES as Set<string>).add(…)` succeeds in TypeScript. No
  rule requires immutability.
- **`_give_up()` in the cap-exhausted branch** of `muxws/reconnect.py` and `ts/reconnect.ts` has no
  observable effect: at `max_attempts=0` the loss has already reported `will_retry=False`. No test
  fails without it.
