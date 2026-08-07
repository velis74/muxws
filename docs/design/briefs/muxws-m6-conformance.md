---
title: muxws M6 - conformance and wire freeze
sidebar: false
search: false
outline: deep
---

# muxws M6 - conformance, second codec, wire freeze

## 1. Goal

At the end of M6 the two implementations are provably the same protocol, and the wire is frozen. The
`conformance/sequences/` corpus is complete and replayed by both `pytest` and `vitest` against the
in-memory transport; CI runs a **live** Python↔TypeScript matrix in *both* role assignments over one
scenario script, plus a reconnect scenario that kills and restarts the acceptor process. `msgpack`
ships in both languages as an optional extra - the honest proof that the codec seam built in M1 is a
seam - with its own cross-language CI pair and round-trip-only assertions. The extension points are
tested rather than merely asserted: an unknown frame type is ignored and an unknown envelope field
changes nothing - which is the whole of the forward-compatibility story now that nothing is
advertised. `SPEC.md` sits at the repository root as the normative
document for the **v1 generation** - the only version on the wire is the `muxws.v1.` subprotocol
prefix. The repository is tagged 1.0 and the JSON wire form is frozen from here.

## 2. Prerequisites

M1-M5b are done and their tests pass in both languages. They left behind:

- **M1** - the frame model, the `Codec` port (`name`, `binary`, `encode`, `decode`),
  `register_codec` / `registerCodec`, `JsonCodec`, the error hierarchy, and the first
  `conformance/frames/` and `conformance/invalid/` fixtures.
- **M2** - `Peer` / `Stream` / the state machine over `muxws/transports/memory.py`.
- **M3** - real transports in both languages, `MUXWS_CODEC` / `VITE_MUXWS_CODEC` selection, the
  `muxws.v1.<codec>` subprotocol assertion, `CodecNotRegistered` / `CodecMismatch`.
- **M4** - `ping`/`pong` and `peer.ping()`, `goaway` + drain + `last_stream`, `peer.close()`, the
  per-peer `error_serializer`.
- **M5a** - fragmentation on the send path with the round-robin writer, all three receive-side caps
  (frame size against `MAX_FRAME_BYTES`, `max_payload_bytes` enforced as fragments accumulate, the
  local concurrency limit answering with `REFUSED`), `on_frame` and the `muxws.frames` logger.
- **M5b** - the reconnect helper (backoff, heartbeat, hello), the socket-death fan-out,
  `PeerRegistry` and `peer.tags`.

Nothing in M6 changes peer behaviour. If a conformance fixture fails, the fix belongs to the
milestone that owns the rule, not to a fixture edit.

## 3. Files to create or modify

| Path | Action |
|---|---|
| `conformance/sequences/*.json` | create - the full corpus, listed in §7 |
| `conformance/frames/*.json` | modify - extend to cover every v1 frame type and every optional field |
| `conformance/invalid/*.json` | modify - complete the eight cases WSM-TST-003 enumerates |
| `conformance/README.md` | create - the fixture schemas and how each runner consumes them |
| `muxws/conformance_test.py` | create - the Python runner for `sequences/` and `invalid/` |
| `ts/conformance.spec.ts` | create - the TypeScript runner, reading the same files |
| `muxws/codecs/msgpack_.py` | create - `MsgpackCodec` (`binary = True`) |
| `muxws/codecs/msgpack__test.py` | create |
| `ts/msgpack.ts` | create - `MsgpackCodec`, reachable only through the `/msgpack` subpath |
| `ts/msgpack.spec.ts` | create |
| `pyproject.toml` | modify - `msgpack` optional extra |
| `package.json` | modify - `@msgpack/msgpack` optional peer dependency, `/msgpack` export subpath, `"sideEffects": false` |
| `SPEC.md` | create - the normative specification at the repository root |
| `.github/workflows/cross-language.yml` | create - the live matrix |
| `interop/runner.py`, `interop/runner.ts` | create - the two scenario drivers CI wires together |
| `muxws/peer_test.py`, `ts/peer.spec.ts` | modify - the extension-point tests |

TypeScript file names are kebab-case; `msgpack.ts` is already one word.

## 4. Normative rules in force

### The corpus

- **WSM-TST-001** `conformance/frames/*.json` MUST be a list of `{"name", "frame", "json_wire"}`
  triples - a logical frame plus the JSON rendering pinned alongside it - read verbatim by both
  `pytest` and `vitest`.
- **WSM-TST-002** `conformance/sequences/*.json` MUST be scripted exchanges replayed by both
  implementations against the in-memory transport. Fixtures MUST refer to streams by `stream_ref`
  (an ordinal the runner resolves), never by a raw id. A fixture's optional top-level
  `max_frame_bytes` is an instruction to the **runner** to construct both peers with a lowered cap
  (WSM-FRG-005); it is not a wire value and MUST NOT be encoded into any frame.

```json
{
  "name": "unary-request-with-server-push-interleaved",
  "max_frame_bytes": 64,
  "steps": [
    {"peer": "dialer",   "call": "request", "payload": {"action": "list"}, "as": "r1"},
    {"expect_frame": {"type": "open", "stream": 1, "end": true}},
    {"peer": "acceptor", "call": "open",    "payload": {"event": "tick"}},
    {"expect_frame": {"type": "open", "stream": 2}},
    {"peer": "acceptor", "call": "reply",   "stream_ref": 1, "payload": {"items": []}},
    {"expect_frame": {"type": "data", "stream": 1, "end": true}},
    {"expect_result": {"ref": "r1", "value": {"items": []}}}
  ]
}
```

- **WSM-TST-003** `conformance/invalid/*.json` MUST cover, each asserting which frame goes out and
  whether the connection survives: wrong parity; an `open` id not greater than that peer's highest
  previous open; `data` after `end`; an over-cap encoded message; a message the configured codec
  refuses to decode; a fragment sequence interrupted by a non-fragment frame; a stream-level frame
  above the high-water mark (connection dies); a `data` frame for an already-closed id (connection
  survives, nothing goes out).
- **WSM-TST-004** CI MUST run the live cross-language matrix in **both role assignments** (Python
  acceptor with TypeScript dialer, and TypeScript acceptor with Python dialer) over the same scenario
  script: concurrent unary requests interleaved with a streaming export and a server push, one
  cancelled mid-flight, and a `goaway` shutdown.
- **WSM-TST-005** The same matrix MUST include one reconnect scenario: kill the acceptor process with
  streams open, restart it, and assert the dialer re-dials on a jittered delay, replays a
  byte-identical hello the other language's acceptor accepts, fires `on_reconnect` exactly once after
  it, and that every open stream raised `ConnectionLost` in the meantime.

### Codec conformance

- **WSM-CDC-004** The library MUST ship and MUST itself register a `json` codec, and JSON MUST be the
  default. JSON is the interoperability baseline: both language ports MUST produce a JSON wire form
  that the other port decodes to an equal logical frame.
  Test: `conformance/frames/*.json` replayed by `frames_test.py` and `frames.spec.ts`.
- **WSM-CDC-005** Conformance for the JSON codec MUST be asserted as `decode(json_wire) == frame` and
  `decode(encode(frame)) == frame`, comparing parsed objects, never byte-identical output. Exactly one
  separate test MAY pin a canonical key order (`type`, then `stream`, then the remaining keys
  alphabetically) for the benefit of log diffing; no other test may depend on key order or whitespace.
- **WSM-CDC-006** Any codec other than `json` MUST be asserted by round-trip over the same logical
  frame corpus (`decode(encode(frame)) == frame`) and MUST NOT have wire bytes pinned in a fixture.
- **WSM-CDC-007** Every codec that ships MUST additionally have a live cross-language pair in CI (a
  Python peer and a TypeScript peer, both configured with that codec, running the sequence corpus). A
  codec without that pair MUST NOT ship.
- **WSM-CDC-008** Under a binary codec, raw bytes (`bytes` / `ArrayBuffer`) are a first-class payload
  type. Under JSON they are not, and muxws MUST NOT base64-encode bytes on the application's behalf.
- **WSM-CDC-013** Registration MUST be explicit: `register_codec(name, codec)` /
  `registerCodec(name, codec)`. There MUST NOT be dynamic imports, lazy auto-registration,
  entry-point scanning, or any probing of whether a module happens to be installed.
- **WSM-CDC-014** A codec module MUST NOT register itself at import time (a side-effecting import can
  never be tree-shaken out).
- **WSM-CDC-015** The npm package MUST declare `"sideEffects": false` (at minimum for the codec
  subpaths).
- **WSM-CDC-003** A peer MUST use exactly one codec for the life of its connection. There is no
  per-frame, per-stream or per-connection codec switching.
- **WSM-CDC-020** The dialer MUST offer `muxws.v1.<codec>` (e.g. `muxws.v1.json`,
  `muxws.v1.msgpack`) as its **first** WebSocket subprotocol entry, where `<codec>` is its configured
  codec name.
- **WSM-FRG-003** Under a binary codec both ports MUST take the length of the produced buffer.

### Extension points

- **WSM-FRM-001** A receiver MUST ignore unknown envelope fields.
- **WSM-FRM-002** A receiver MUST ignore unknown *frame types*, logging once, and MUST NOT treat them
  as any kind of error. Test: `peer_test.py::test_unknown_frame_type_is_ignored`.
- **WSM-FRM-003** *Retired.* It required a peer not to send an extension frame type unless the remote
  had advertised that extension in `settings.extensions`. There is no `settings` frame and no
  extension advertisement (WSM-CON-031); a v1 peer sends only the frame types in §3.2, and a frame
  type the remote must *act* on requires a new generation (WSM-CON-009). The id is not reused, and
  `test_unadvertised_extension_is_never_sent` is not written.
- **WSM-CON-009** The version component of the subprotocol name (`muxws.v1.`) is the **only** version
  on the wire and pins the breaking-change generation. Additive revisions - new frame types, new
  fields - MUST NOT be announced anywhere, because WSM-FRM-001/002 already make them safe to receive.
  A change that requires the remote to *act* on a new frame type rather than tolerate it MUST bump the
  generation, which a v1 acceptor rejects at the handshake (WSM-CDC-025).
- **WSM-CON-031** There MUST NOT be a `settings` frame. Every limit is either a protocol constant
  (`MAX_FRAME_BYTES`, WSM-FRG-004) or a local receiver-side defence (`max_payload_bytes`,
  WSM-FRG-035; the concurrency limit, WSM-STM-036). A limit MUST NOT appear on the wire in any form.
- **WSM-BPR-001** v1 MUST NOT implement per-stream flow control. `window_update` is reserved as a
  frame type and MUST NOT be sent.

### Packaging and the freeze

- **WSM-PKG-001** Both packages MUST ship from one repository on one version stream, with identical
  version numbers in `pyproject.toml` and `package.json`.
- **WSM-PKG-002** The Python package MUST have **zero required runtime dependencies**. `starlette`
  and `websockets` are optional extras selected by which transport is imported; `msgpack` is an
  optional extra selected by which codec is registered.
- **WSM-PKG-003** The TypeScript browser entry point MUST have zero runtime dependencies. `ws` is an
  optional peer dependency for `node.ts`; `@msgpack/msgpack` an optional peer dependency reachable
  only through the `/msgpack` subpath.
- **WSM-PKG-005** The wire format is versioned by the generation integer in the subprotocol name
  (`muxws.v1.<codec>`) - a single monotonically increasing integer, bumped only for a breaking wire
  change - independently of the packages' semver. There is no second, finer version anywhere
  (WSM-CON-009).
- **WSM-INV-015** An unregistered codec name MUST be a loud startup failure, never a silent JSON
  fallback (WSM-CDC-016) - or a deployment believes it is running msgpack, is not, and may never find
  out because both ends fell back.

## 5. Implementation notes

- **One fixture schema, two runners, zero divergence.** Write `conformance/README.md` first, pinning
  the JSON schema of all three fixture kinds, then write the two runners against it. If a runner
  needs a field the other does not read, the fixture is wrong.
- **`stream_ref` is an ordinal, never a raw id** (WSM-TST-002). The runner keeps `refs: list[Stream]`
  in the order streams appear in the script and resolves `stream_ref: 1` to the first. This is what
  makes a fixture replayable when the roles are swapped - a raw id bakes in one side's parity.
- **`expect_frame` is a subset match, not equality.** Assert the listed keys; ignore everything else,
  so adding an optional field in a later revision does not invalidate the corpus (that is the point
  of WSM-FRM-001).
- **JSON conformance compares parsed objects** (WSM-CDC-005). Exactly one test - name it
  `test_canonical_key_order_for_log_diffing` - may assert key order (`type`, `stream`, then the rest
  alphabetically). Every other assertion parses first. A byte-comparison anywhere else will break the
  moment either language's JSON serializer changes its spacing.
- **msgpack is asserted by round-trip only** (WSM-CDC-006). Do **not** pin msgpack bytes in a
  fixture: the two libraries make different but equally valid choices about int width and map format,
  and a pinned-bytes fixture would make a legal encoder fail.
- **msgpack must not self-register** (WSM-CDC-014). `muxws/codecs/msgpack_.py` defines
  `MsgpackCodec` and nothing else; the application calls
  `register_codec("msgpack", MsgpackCodec())`. Same in TypeScript, plus `"sideEffects": false` in
  `package.json` so the subpath is tree-shakeable (WSM-CDC-015). An `import` that registers is a
  bundle-size regression nobody can undo.
- **`binary = True` on msgpack changes the send path, not the peer.** The peer selects
  `send_bytes`/`sendBytes` from `codec.binary`; it MUST NOT sniff the encoded value's type
  (WSM-CDC-002). Add one test that a binary codec never reaches `send_text`.
- **Bytes are a payload type under msgpack** (WSM-CDC-008). Add a corpus entry whose payload is
  `b"\x00\xff"` / `new Uint8Array([0, 255]).buffer`, and assert that the *JSON* codec refuses it
  rather than base64-ing it.
- **The live matrix is four jobs, not two**: {Python acceptor, TS dialer} × {json, msgpack} and
  {TS acceptor, Python dialer} × {json, msgpack}. Each job runs `interop/runner.py` and
  `interop/runner.ts` as two processes over a real socket on localhost with `MUXWS_CODEC` /
  `VITE_MUXWS_CODEC` set. The reconnect scenario (WSM-TST-005) needs the acceptor process
  **killed and restarted**, so the driver must own the process, not just the socket.
- **Bound the reconnect job's wall clock.** Set `initial_delay=0.05`, `max_delay=0.5` in the interop
  scenario so a jittered retry is observable in a CI job without a 30 s wait - but assert that jitter
  *dispersed* the delay, not that it equalled a constant.
- **The freeze is a test, not a promise.** Add `test_json_wire_is_frozen` that hashes the sorted
  `conformance/frames/*.json` corpus and compares against a checked-in digest. Changing the JSON wire
  after 1.0 must require deliberately editing that digest.
- **Lint that will bite here:**
  - `S101` - `assert` is banned outside `*_test.py`. The conformance runners are `*_test.py` /
    `*.spec.ts` files, so the per-file-ignore applies; the `interop/runner.py` driver is **not**, so
    it must raise instead of asserting.
  - `PT` - pytest style: the corpus must be fed through `@pytest.mark.parametrize` with an `ids=`
    that names the fixture file, so a failure names the fixture rather than an index.
  - `ARG` - the runner's step handlers will share a signature with unused parameters; underscore them.
  - `S301`/`S403` do not apply to msgpack, but pass `strict_map_key=False` and `raw=False`
    deliberately and comment why.
  - `UP` - `str | bytes`, never `Union[str, bytes]`.
  - `no-restricted-syntax` forbids `for...in` - iterate fixture steps with `for (const step of ...)`.
  - `unicorn/filename-case: kebabCase` for any new TS file.

## 6. The sequence corpus

Each of these is one file under `conformance/sequences/`, replayed by both runners in both role
assignments.

| File | Asserts |
|---|---|
| `unary-request-with-server-push-interleaved.json` | the example in WSM-TST-002 verbatim |
| `streaming-response-until-end.json` | `open` → n × `data` → `data(end)`; iteration yields n payloads |
| `bidirectional-interleaved.json` | both sides send after `open`; both end independently |
| `notify-is-open-end-with-nothing-awaited.json` | one `open(end)` and no reply expected |
| `small-frame-overtakes-a-fragmented-payload.json` | WSM-FRG-019 round-robin (created in M5a, verified here in both languages) |
| `cancel-mid-stream-stops-the-producer.json` | `reset(CANCELLED)` out; the remote handler is cancelled and sends nothing further |
| `timeout-sends-reset-timeout.json` | a local deadline produces `reset(TIMEOUT)` and `StreamTimeout` |
| `handler-raises-produces-application-error.json` | `reset(APPLICATION_ERROR)` with the serialized payload |
| `concurrency-limit-rejection.json` | the open beyond the **receiver's** limit gets `reset(REFUSED)`, the opener's `open()` raised nothing locally, and the connection survives (WSM-STM-036) |
| `goaway-drains-then-closes.json` | streams ≤ `last_stream` finish, those above are locally `REFUSED` |
| `unknown-frame-type-is-ignored.json` | an injected `{"type":"widget"}` changes nothing |
| `unknown-envelope-field-is-ignored.json` | an injected `"colour": "red"` on a `data` frame changes nothing |
| `bytes-payload-under-binary-codec.json` | msgpack only; skipped by the JSON runner with a recorded reason |

`conformance/invalid/` must, at the end of M6, contain exactly the eight cases of WSM-TST-003, each
declaring `expect_frame` and `connection_survives: true|false`.

## 7. Tests to write

1. `muxws/conformance_test.py::test_frames_corpus_decodes_and_round_trips` /
   `ts/conformance.spec.ts` - for every triple in `conformance/frames/`:
   `decode(json_wire) == frame` **and** `decode(encode(frame)) == frame`, comparing parsed objects
   (WSM-CDC-004/005).
2. `test_canonical_key_order_for_log_diffing` - the **only** test permitted to assert key order
   (WSM-CDC-005).
3. `test_sequence_corpus_replays_in_both_role_assignments` - parametrized over every file in
   `conformance/sequences/`, run twice with the roles swapped (WSM-TST-002).
4. `test_invalid_corpus_produces_the_declared_frame_and_survival` - parametrized over
   `conformance/invalid/`; asserts both the outgoing frame and whether the connection lived
   (WSM-TST-003).
5. `test_every_invalid_case_of_wsm_tst_003_has_a_fixture` - a meta-test enumerating the eight
   required cases by name and failing if a file is missing. (Without it, a deleted fixture is a
   silently passing suite.)
6. `msgpack__test.py::test_round_trip_over_the_whole_frame_corpus` / `ts/msgpack.spec.ts` -
   `decode(encode(frame)) == frame` for every logical frame; **no** pinned bytes (WSM-CDC-006).
7. `test_msgpack_codec_does_not_register_itself_on_import` - import the module, assert the name is
   absent from the registry until `register_codec` is called (WSM-CDC-013/014).
8. `test_binary_codec_uses_send_bytes_never_send_text` - assert on the `SocketAdapter` spy
   (WSM-CDC-002).
9. `test_bytes_payload_survives_msgpack_round_trip` and
   `test_json_codec_refuses_bytes_rather_than_base64_encoding_them` (WSM-CDC-008).
10. `test_msgpack_fragment_boundaries_are_byte_boundaries` - sizes measured as the buffer length
    (WSM-FRG-003).
11. `peer_test.py::test_unknown_frame_type_is_ignored` **(named in the spec)** - the connection
    survives, one log line is emitted, and nothing goes out (WSM-FRM-002).
12. `test_no_limit_and_no_version_appears_on_the_wire` - replay the whole sequence corpus and assert
    that no frame carries a `settings` type or any of `ack`, `protocol_version`, `extensions`,
    `max_frame_bytes`, `max_concurrent_streams`, `max_payload_bytes`; the `muxws.v1.` subprotocol
    prefix is the only version anywhere (WSM-CON-031, WSM-CON-009, WSM-PKG-005). This replaces the
    retired extension-advertisement test.
13. `test_window_update_is_never_sent` - reserved in v1 (WSM-BPR-001).
14. `test_unknown_envelope_field_is_ignored` (WSM-FRM-001).
15. `test_json_wire_is_frozen` - the corpus digest matches the checked-in value (the freeze).
16. `test_python_and_typescript_versions_match` - reads `pyproject.toml` and `package.json`
    (WSM-PKG-001).
17. `test_python_package_has_no_required_runtime_dependencies` - parses `pyproject.toml`
    `[project] dependencies` and asserts it is empty (WSM-PKG-002).
18. `test_browser_entry_point_imports_nothing_optional` - a bundle of `ts/index.ts` contains no `ws`
    and no `@msgpack/msgpack` (WSM-PKG-003).
19. **CI job** `cross-language (python-acceptor, ts-dialer, json)` - the WSM-TST-004 scenario:
    concurrent unary requests interleaved with a streaming export and a server push, one cancelled
    mid-flight, and a `goaway` shutdown.
20. **CI job** `cross-language (ts-acceptor, python-dialer, json)` - the same script, roles swapped
    (WSM-TST-004).
21. **CI jobs** the same two with `msgpack` (WSM-CDC-007 - a codec without its pair MUST NOT ship).
22. **CI job** `cross-language-reconnect` - kill the acceptor process with streams open, restart it,
    and assert: the dialer re-dialled after a jittered delay, the replayed hello was byte-identical
    and accepted by the other language's acceptor, `on_reconnect` fired exactly once after the ack,
    and every stream open at the kill raised `ConnectionLost` (WSM-TST-005).

## 8. Done when

```bash
ruff check . && ruff format --check .
pytest muxws -q --cov=muxws --cov-report=term-missing
npm run lint
npm test
npm run build && python -m build          # both artefacts build clean
```

- [ ] All commands pass; the conformance runners report the same number of fixtures in both
      languages, and that number is asserted by test 5.
- [ ] The five CI jobs of tests 19-22 are green, in both role assignments and both codecs.
- [ ] `conformance/invalid/` contains exactly the eight cases of WSM-TST-003; the meta-test proves it.
- [ ] No msgpack byte string appears anywhere under `conformance/`.
- [ ] `grep -rn "register_codec\|registerCodec" muxws/codecs ts/msgpack.ts` shows no call at module
      scope.
- [ ] `package.json` declares `"sideEffects": false` and a `/msgpack` export subpath; the browser
      bundle size does not change when msgpack is installed but unused.
- [ ] `SPEC.md` exists at the repository root, is scoped to the `muxws.v1.` generation (and carries no
      second version number of any kind), and every rule id in it resolves to at least one test (add
      `test_every_rule_id_is_referenced_by_a_test` if you want this enforced rather than reviewed).
- [ ] `pyproject.toml` and `package.json` both read `1.0.0`; the repository is tagged `v1.0.0`.
- [ ] The JSON wire form is frozen: `test_json_wire_is_frozen` passes and its digest is committed.

## 9. Out of scope

- **The documentation site.** M6 in the original milestone list said "conformance, documentation, wire
  freeze"; the documentation half is now **M7** (`muxws-m7-documentation.md`) and is the last
  milestone before release. M6 writes `SPEC.md` and `conformance/README.md` only.
- **A third codec.** Every codec that ships costs a cross-language CI pair (WSM-CDC-007); two is the
  proof the seam works.
- **`window_update` / flow control** - reserved in v1, never sent.
- **Stream resumption across a reconnect** - forbidden in v1 (WSM-RCN-031).
- **A cross-process registry backplane** (WSM-REG-018).
- **Any change to peer behaviour.** A failing fixture means a rule was implemented wrongly in
  M1-M5b; fix it there and keep the fixture.
