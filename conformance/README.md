# The muxws conformance corpus

One corpus, two runners, zero divergence.

Everything under this directory is plain JSON, read verbatim by both the Python suite and the
TypeScript suite. Nothing here is generated at test time and nothing here is language-specific: a
fixture is the contract, and the two ports are the things being held to it. That is the whole reason
the corpus exists. A rule that only one port checks is a rule that drifts, and a wire disagreement
between the ports is invisible until two peers of different languages try to talk to each other, at
which point it looks like corruption rather than like a bug.

**This document is the schema.** It is written so that two runners implemented independently from it
cannot end up reading the same file differently (M6 §5). If a runner needs a field the other does not
read, the fixture is wrong. If this document and a runner disagree, one of them is a defect - say
which, do not quietly pick.

## Who reads what

| Directory | Kind | Python | TypeScript |
|---|---|---|---|
| `frames/v1-frames.json` | frame triples | `muxws/frames_test.py`, `muxws/fragment_test.py`, `muxws/codecs/msgpack__test.py`, `muxws/conformance_schema_test.py` | `ts/frames.spec.ts`, `ts/fragment.spec.ts`, `ts/msgpack.spec.ts` |
| `frames/v1-fragment-boundaries.json` | boundary cases | `muxws/fragment_test.py`, `muxws/conformance_schema_test.py` | `ts/fragment.spec.ts` |
| `sequences/*.json` | scripted exchanges | `muxws/conformance_test.py` | `ts/conformance.spec.ts` |
| `invalid/*.json` | misbehaviour cases | `muxws/conformance_test.py`, `muxws/conformance_schema_test.py` | `ts/conformance.spec.ts` |

**`muxws/conformance_test.py` and `ts/conformance.spec.ts` are the two runners of record**, and they
are twins: the Python file reads the TypeScript one's fixture counts and the TypeScript file reads
the Python one's frozen digest, so neither can drift alone.

Both suites resolve this directory relative to the repository root. `pytest` walks up from the module
file; `vitest` uses `process.cwd()`.

## Rules that hold for every fixture

- **JSON, UTF-8, no comments, no trailing commas.** A `description` field is where a fixture explains
  itself; every kind accepts one and every runner ignores it.
- **A fixture that is one object per file carries a `name`, and it equals the file basename.** Both
  sequence runners and the invalid runner assert this, so a renamed file cannot silently become a
  second copy of a case that is already covered. Files that hold a *list* of cases
  (`frames/`) instead require each entry's `name` to be unique within the file, because those names
  become test ids.
- **No floats.** The two ports render them differently - Python writes `1.0`, `-0.0`, `1e+16`,
  `1e-07` where JavaScript writes `1`, `0`, `10000000000000000`, `1e-7` - and fragment boundaries are
  cut over the encoded form, so a payload carrying a float is cut differently by the two ports. See
  `GAPS.md`. Integers are safe only within ±(2^53 − 1); beyond that JavaScript rounds to the nearest
  double. `NaN` and `Infinity` are not JSON and the codec refuses them (`allow_nan=False`).
- **No bytes anywhere.** JSON has no byte type, and muxws MUST NOT base64-encode bytes on the
  application's behalf (WSM-CDC-008). Bytes are a payload type only under a binary codec, and no
  msgpack byte string may be pinned in a fixture at all (WSM-CDC-006): the two msgpack libraries make
  different but equally valid choices about int width and map format, and pinned bytes would make a
  legal encoder fail.
- **No limit and no version appears in any frame.** There is no `settings` frame (WSM-CON-031) and no
  `ack`, `protocol_version`, `extensions`, `max_concurrent_streams`, `max_payload_bytes` or
  `max_frame_bytes` key inside a frame. The `muxws.v1.` subprotocol prefix is the only version
  anywhere (WSM-CON-009, WSM-PKG-005). `muxws/conformance_schema_test.py` greps the corpus text for
  these words, so do not put them in a `description` or a `reason` string with quotes around them
  either.
- **Reset code 5 is retired** (it was `STREAM_LIMIT`, WSM-STM-022). It must never appear. Code 9
  (`CONNECTION_CLOSED`) is synthesised locally and must never appear on the wire either
  (WSM-ERR-002).
- **Every duration is milliseconds and its key ends in `_ms`.** The corpus is written once and read
  by a language whose durations are milliseconds and a language whose durations are seconds
  (WSM-CON-012); the runner converts, the fixture does not carry both.

---

## Kind 1 - `frames/v1-frames.json`: the frame triples

A JSON **list** of `{"name", "frame", "json_wire"}` objects and nothing else - exactly those three
keys (WSM-TST-001).

```json
{
  "name": "data-end-with-trailers",
  "frame": { "type": "data", "stream": 7, "payload": { "rows": 4 }, "end": true, "trailers": { "checksum": "deadbeef" } },
  "json_wire": "{\"type\":\"data\",\"stream\":7,\"end\":true,\"payload\":{\"rows\":4},\"trailers\":{\"checksum\":\"deadbeef\"}}"
}
```

| Key | Type | Meaning |
|---|---|---|
| `name` | string | Unique within the file; becomes the test id in both languages. |
| `frame` | object | A **logical frame spelled as its envelope**. |
| `json_wire` | string | The JSON rendering of that frame, pinned as a string. |

### `frame` is an envelope, not a constructor call

Runners build the frame with `from_mapping` / `fromMapping`, which is what makes the corpus a
receiver-side contract rather than a transcription of one language's dataclass:

- **An absent `payload` key means `ABSENT`, and an explicit `"payload": null` means null** (D1). The
  two are different frames and both appear in the corpus; a peer that conflated them could not tell
  "no payload" from "a payload that is null".
- Every other key absent means that field's default (`stream: null`, `more: false`, `end: false`,
  `headers: null`, …).
- Unknown keys are **dropped**, never preserved (WSM-FRM-001, D2). Do not add one to a triple
  expecting it to survive - `decode(encode(frame)) == frame` would then pass on garbage.
- `type` is required. `payload` and `fragment` together is a protocol error (WSM-FRM-004) and cannot
  appear in a triple.

The twelve envelope fields are `type`, `stream`, `payload`, `fragment`, `more`, `headers`, `end`,
`trailers`, `code`, `reason`, `nonce`, `last_stream`. The corpus covers **all twelve** and all six v1
frame types (`open`, `data`, `reset`, `ping`, `pong`, `goaway`).

### What `json_wire` is asserted against - and what it is not

Three assertions per triple, in both languages (WSM-CDC-004/005):

1. `decode(json_wire) == frame`
2. `decode(encode(frame)) == frame`
3. `JSON.parse(encode(frame))` deep-equals `JSON.parse(json_wire)`

All three compare **parsed objects**. A byte comparison would break the moment either language's JSON
serializer changed its spacing, and would make a legal encoder fail. Exactly one test in the whole
repository may assert key order - `test_canonical_key_order_for_log_diffing`, which pins `type`, then
`stream`, then the remaining keys alphabetically for the benefit of log diffing - and it builds its
own frame rather than reading the corpus.

`json_wire` is therefore **authored by hand, not generated from our own encoder**; a wire generated
from the thing it is meant to police proves nothing. By convention it is written in canonical key
order and with the codec's separators (`,` and `:`, no spaces, `ensure_ascii=False`), because a
corpus that already looks like the output is far easier to diff by eye - but no test depends on that.

### Consequences of adding a triple

Anything with a `payload` key is also fed to the fragmenter at a hostile 96-byte cap by
`muxws/fragment_test.py` and `ts/fragment.spec.ts`, which assert that every fragment fits under the
cap and that reassembly reproduces the payload exactly. Adding a triple therefore adds a
fragmentation case for free - and a payload that cannot be fragmented at 96 bytes will fail there,
not here.

## Kind 2 - `frames/v1-fragment-boundaries.json`: the boundary cases

A JSON list of `{"name", "cap", "payload", "fragments"}` objects and nothing else. This is the one
fixture kind that pins **exact strings** rather than parsed equality, because the exact cut points
*are* the contract (WSM-FRG-016): both ports must run the same search over the same encoded form and
therefore cut at the same boundary.

| Key | Type | Meaning |
|---|---|---|
| `name` | string | Unique within the file. |
| `cap` | integer > 0 | The frame cap the splitter is given, in bytes. |
| `payload` | any | The payload to split. |
| `fragments` | list of strings, length > 1 | The exact `fragment` value of each part, in order. |

The runner splits `{"type": "data", "stream": 1, "payload": payload, "end": true}` at `cap` and
asserts the produced `fragment` values equal `fragments` element for element, then that reassembly
reproduces `payload`.

Cases must keep covering the three ways this drifts, and `test_every_boundary_hazard_has_a_case`
enforces it: `astral-plane` (a non-BMP character is 2 in JavaScript's `.length` and 4 on the wire),
`two-byte-codepoints` and `three-byte-codepoints` (a cut inside a multi-byte sequence must move
backwards), and `control-characters` (JSON escaping expands one byte to six, so a proportional guess
at the cut point converges differently in the two ports).

---

## Kind 3 - `sequences/*.json`: scripted exchanges

One fixture per file, replayed by both implementations against the in-memory transport, in **both
role assignments** (WSM-TST-002). Both peers are real, nothing is injected, and the fixture is a
script of ordinary API calls with assertions interleaved. What this proves is that the ports agree
about *sequences* - which frame goes out when, which stream survives which event - and not merely
about how one frame is spelled.

```json
{
  "name": "unary-request-with-server-push-interleaved",
  "description": "why this fixture exists, and which rules it pins",
  "max_frame_bytes": 64,
  "steps": [ ... ]
}
```

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Equals the file basename; both runners assert it. |
| `description` | no | Prose. Ignored by both runners. |
| `max_frame_bytes` | no | **An instruction to the runner**, not a wire value. |
| `max_concurrent_streams` | no | The same: the receiver's local limit (WSM-STM-036), never announced. |
| `requires_codec` | no | A codec name. Each runner replays the corpus once per shipped codec; a pass configured with another codec skips the fixture, loudly. The name must be one some pass is configured with (WSM-CDC-007). |
| `steps` | yes | A non-empty list, executed strictly in order. |

### `max_frame_bytes` and `max_concurrent_streams` are instructions to the runner

When present, the runner constructs **both** peers with that cap so that a fixture can exercise
fragmentation without carrying a 64 KiB payload (WSM-FRG-005, WSM-TST-002), or the concurrency limit
without opening a hundred streams (WSM-STM-036). Neither may be encoded into any frame, and neither
may appear inside a step: a limit never appears on the wire in any form (WSM-CON-031).
`muxws/conformance_schema_test.py` asserts this for the invalid corpus, and both runners assert it
for this one by replaying the whole corpus and reading the raw messages
(`test_no_limit_and_no_version_appears_on_the_wire`, `puts no limit and no version on the wire`).

### `requires_codec` is the only reason a fixture is ever skipped

Each runner replays this corpus **once per shipped codec**: a `json` pass and a `msgpack` pass, each
in both role assignments. A fixture written for a codec a given pass is not configured with is
skipped by that pass **with a recorded reason**, never quietly passed - and the set of skipped
fixtures is itself pinned by a test in each runner, because a skip that spread to a second fixture
would be a corpus quietly shrinking. Nothing else in this corpus may be skipped: an unrecognised step
kind or an unimplemented `call` is a hard failure.

A `requires_codec` naming a codec **no** pass is configured with is a defect, not a skip: the fixture
is then reachable by no line of the library while reading in the report as covered. Both runners
assert against it by name (`test_the_binary_codec_fixture_is_replayed_rather_than_skipped_everywhere`,
`runs every fixture under some configured codec, skipping none everywhere`), because WSM-CDC-007
requires every codec that ships to have a peer at each end running this corpus.

### `{"$bytes": [...]}` is a byte string, and is notation rather than a payload shape

JSON has no byte type, but under a binary codec raw bytes are a first-class payload type
(WSM-CDC-008). A fixture spells one as an object whose **sole** key is `$bytes`, whose value is a
list of integers in `0..255`:

```json
{ "peer": "dialer", "call": "request", "payload": { "$bytes": [0, 255, 16] }, "as": "echo" }
```

- Both runners resolve it **when the fixture is loaded**, before the first step runs: Python to
  `bytes`, TypeScript to an `ArrayBuffer`. It is never a value on the wire, and a runner that left
  the object unresolved would send an ordinary map that both codecs carry happily - the fixture would
  pass while asserting nothing about bytes. Each runner therefore asserts that the corpus still
  carries the placeholder *and* that loading resolved it
  (`test_the_bytes_placeholder_is_resolved_before_a_step_runs`, `resolves the $bytes placeholder
  before a step runs`).
- The substitution is recursive and applies to the whole fixture, so a placeholder is legal anywhere
  a payload, a matcher value or an `expect_result` value is written.
- `$bytes` alongside any other key is **not** a placeholder but an ordinary payload key, so an
  application object may still carry one.
- Only a fixture that also declares `"requires_codec": "msgpack"` may use it. Under JSON, bytes are
  not a payload type at all and muxws must not base64-encode them on the application's behalf
  (WSM-CDC-008), so a JSON pass must fail on one rather than silently encode it.
- **A byte comparison must be structural.** `assert a == b` is exact in Python, but vitest's
  `toEqual` compares two `ArrayBuffer`s by byte *length* and calls buffers of equal length equal
  whatever they contain. The TypeScript runner compares with `deepEqual` from `ts/frames.ts` for
  exactly this reason; a runner that reaches for `toEqual` here has a fixture that cannot fail.

### Streams are named by ordinal, never by a raw id

This is the property that makes a fixture replayable when the roles are swapped, and it is the single
easiest thing to get wrong.

- The runner keeps an ordered list of the streams the script opens. **Every step that opens a stream
  appends exactly one entry, in step order**, whatever call opened it. Ordinal 1 is the first stream
  the script opened, ordinal 2 the second, and so on. Ordinals are global to the fixture, not
  per-peer.
- `"stream_ref": n` in any matcher resolves to the `stream` field of the *n*-th such stream; in any
  call it selects that stream's handle **on the peer the step names**.
- `"last_stream_ref": n` is the same resolution applied to `goaway.last_stream`, which carries the
  *other* peer's parity (WSM-CON-020) and so is the one place a raw number is most tempting and most
  wrong.
- **A raw `"stream": 3` in a matcher is a defect**, even when it happens to pass: it bakes one side's
  parity into the corpus, and the fixture will fail the moment the roles are swapped. The brief's own
  illustration of WSM-TST-002 writes raw ids; the rule wins over the illustration (see *Known
  divergences* below).

### Step vocabulary

Every step is an object with exactly one *kind* key. The runners dispatch on the first kind they
recognise in this order, so do not combine two in one step. **A step whose kind is not in this table,
or a `call` this runner does not implement, is a hard failure - never a skip.** A silently skipped
step is a fixture that passes by testing nothing.

| Kind key | Other keys | What happens |
|---|---|---|
| `settle` | *(the value is an integer)* | Yield the event loop that many turns. |
| `call` | `peer`, plus the call's own | Invoke an API method on that peer. See below. |
| `inject` | `peer` | Deliver one raw envelope **to** that peer, as if its remote had sent it. |
| `expect_frame` | `peer` | Subset-match the next matching frame on that peer's wire. |
| `expect_no_frame` | `peer` | Assert that peer's whole wire contains no matching frame. |
| `expect_result` | — | `{"ref", "value"}`: the named stream completes with that value. |
| `expect_error` | — | `{"ref", "error", "code"?, "payload"?}`: the named stream fails with that class. |
| `expect_closed` | — | `{"peer", "socket"?}`: that peer is no longer open. |

`peer` is `"dialer"` or `"acceptor"` and is **required** on `call`, `inject`, `expect_frame` and
`expect_no_frame` - an `expect_frame` without it cannot say whose wire to search, and a runner that
guessed would be asserting something different in each language.

#### `inject`

`{"peer": "dialer", "inject": {"type": "data", "stream_ref": 1, "colour": "red"}}` encodes the
envelope **exactly as written** and hands it to that peer's socket as an inbound message. Ordinals
are resolved first; nothing else is.

The envelope deliberately does *not* pass through `from_mapping` / `fromMapping`, and that is the
whole point for the two extension-point fixtures: those functions drop unknown keys (WSM-FRM-001), so
a frame built through them could never carry the unknown field or the unknown type whose toleration
is the thing being asserted (WSM-FRM-001, WSM-FRM-002).

This is the one step kind a sequence fixture shares with `invalid/`, and the difference is intent:
here the injected message is *legal but unrecognised*, and the assertion is that nothing happens. A
message no correct implementation would send belongs in `invalid/`, where the survival column exists
to say what it costs.

#### `settle`

`{"settle": 12}` yields the event loop twelve **turns** - not milliseconds. Python runs
`await asyncio.sleep(0)` that many times; TypeScript awaits that many `setTimeout(…, 0)` macrotasks.
These are not the same amount of progress, so a `settle` count is a *lower bound on progress, never a
duration*. A fixture that only passes at exactly 12 and fails at 24 is a fixture that is racing, and
the defect is the fixture's.

#### `call`

Implemented today by both runners. The three that allocate an ordinal are marked; every other call
addresses a stream that already exists, by `stream_ref`.

| `call` | Keys | Maps to |
|---|---|---|
| `open` | `payload`?, `headers`?, `end`?, `as`? | `peer.open(...)`. **Allocates an ordinal.** Synchronous; never queues. |
| `request` | `payload`?, `headers`?, `timeout_ms`?, `as`? | `peer.request(...)`, **started and not awaited** (WSM-API-006). **Allocates an ordinal.** |
| `notify` | `payload`?, `headers`?, `as`? | `peer.notify(...)`, awaited. **Allocates an ordinal.** |
| `send` | `stream_ref`, `payload`?, `end`? | `stream.send(payload, end)`. |
| `end` | `stream_ref`, `payload`?, `trailers`? | `stream.end(...)`. An **absent** `payload` key is `ABSENT`, an explicit `null` is null (D1). |
| `reply` | `stream_ref`, `payload`?, `trailers`? | `stream.reply(payload)` on that peer's handle for that ordinal. |
| `cancel` | `stream_ref`, `reason`? | `stream.cancel(reason)` - `reset(CANCELLED)`, sent without waiting (WSM-ERR-013). |
| `iterate` | `stream_ref`, `as` | Start consuming the stream as an async iterator; the collected list is the label's value. |
| `close` | `code`?, `reason`?, `drain_ms`? | `peer.close(...)`, **started and not awaited** - `close()` sends `goaway` and *then* drains, and the steps after it are what the drain window exists to let happen (WSM-CON-025). Defaults: `code` 0 (`NO_ERROR`), `drain_ms` 10000. |
| `await_close` | — | Await the `close()` this peer already has in flight. Fails if there is none. |

`request` and `notify` do not hand back a handle - `request` deliberately returns a *value* - so both
runners discover the stream those calls allocated through the public `peer.streams` map, by a bounded
poll. A poll and not a single read, because Python reaches the `open()` inside `request()` only once
the task is first scheduled while TypeScript reaches it synchronously: exactly the kind of difference
a shared corpus must not be able to see.

`timeout_ms` and `drain_ms` are **milliseconds**, like every duration in this corpus; the Python
runner divides by 1000 because Python's durations are seconds (WSM-CON-012).

`"as": "label"` binds the stream that step produced to a label, for a later `expect_result` or
`expect_error`. Labels are separate from ordinals: an ordinal identifies a stream on the wire, a
label identifies a result the script wants to await. On `request` and `iterate` the label is bound to
the **call in flight** rather than to the stream, because the value those steps assert is the call's
return.

#### `"raises"` on a `call`

`{"peer": "acceptor", "call": "send", "stream_ref": 1, "payload": {...}, "raises": "StreamReset"}`
states that the call itself fails, with an instance of the named class from the `expect_error` list.
It is not a matcher on frames and could not be one: what it pins is that the *local handle* refuses
to send at all (WSM-ERR-009), and a peer that reset the stream and then went on producing is
indistinguishable from a correct one on the wire alone.

Adding a call name is a change to **both runners and this table in the same commit**. A call one
runner knows and the other does not is exactly the divergence this document exists to prevent.

#### `expect_frame` and `expect_no_frame` - subset, not equality

The listed keys must hold on the frame and **everything else about the frame is ignored**. This is
deliberate: adding an optional field in a later revision must not invalidate the corpus, which is
precisely what WSM-FRM-001 asks of a receiver, applied to the test suite. Never write a matcher that
tries to say "and nothing else"; there is no such matcher and there should not be.

- `expect_frame` searches **forward from a per-peer cursor** over the frames that peer has sent, and
  advances the cursor past the frame it matched. Successive `expect_frame` steps on one peer
  therefore assert *relative order*, and the same fixture says nothing about the interleaving of the
  two peers' wires - which is the only claim that survives a role swap.
- `expect_no_frame` searches the **whole** of that peer's wire and ignores the cursor. It means "not
  at all", not "not yet".
- Both resolve `stream_ref` → `stream` and `last_stream_ref` → `last_stream` before matching. Every
  other key is compared to the envelope field of the same name, by value.

#### `expect_result`, `expect_error`, `expect_closed`

- `{"expect_result": {"ref": "r1", "value": {...}}}` awaits the labelled stream's result and compares
  `value` by deep equality.
- `{"expect_error": {"ref": "above", "error": "StreamRefused", "code": 4}}` awaits it and requires the
  failure to be an instance of the named class. `error` names a **class**, not a code, because the
  class is the part WSM-ERR-004 fixes across the two languages; `code` is an optional extra
  assertion. The accepted names are `ConnectionLost`, `RemoteError`, `StreamRefused`, `StreamReset`
  and `StreamTimeout`.
- `{"expect_closed": {"peer": "acceptor", "socket": true}}` polls, bounded, until that peer is no
  longer open - and additionally until its socket is closed when `socket` is true. A bounded poll and
  not a single read, because the other end learns of a close one turn later.

Every waiting step is bounded - `expect_result`, `expect_error` and `await_close` by a wall-clock
ceiling, `expect_closed` by a turn count - so a fixture that cannot make progress fails by name
instead of hanging the suite.

---

## Kind 4 - `invalid/*.json`: misbehaviour cases

One case per file. Where a sequence fixture drives two well-behaved peers through their public API,
an invalid fixture **injects raw messages** no correct implementation would ever send, and asserts
two things: which frame goes out, and whether the connection survives (WSM-TST-003).

```json
{
  "name": "data-after-end",
  "description": "...: stream-level (WSM-STM-020/021).",
  "max_frame_bytes": 256,
  "inbound": [ { "type": "open", "stream": 1, "payload": { "action": "upload" } } ],
  "expect_out": [ { "type": "reset", "stream": 1, "code": 3 } ],
  "connection_survives": true
}
```

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Equals the file basename. |
| `description` | yes | Non-empty. Why this case is stream-level or connection-level; it is quoted in the assertion message. |
| `inbound` | yes | Non-empty list of messages to inject, in order. |
| `expect_out` | yes | Ordered subset matchers over what the peer under test sent. May be empty. |
| `connection_survives` | yes | Boolean. |
| `max_frame_bytes` | no | Same runner instruction as for sequences; never a wire value. |

### The peer under test is an acceptor, and ids here are raw

The runner builds one real peer as an **acceptor** (even ids) and injects the wire of a misbehaving
**dialer** (odd ids). Unlike a sequence fixture, an invalid fixture is deliberately **not**
role-swappable and deliberately uses raw stream ids: the whole point is to hand-write bytes that no
API call on either side could have produced, and an ordinal cannot name a stream that was never
opened. `stream_ref` has no meaning in this kind and must not appear.

### `inbound` entries

Each entry is one WebSocket message and is one of two shapes:

- **A frame envelope** - it has a `type`. The runner builds it with `from_mapping` / `fromMapping`
  and encodes it with the configured codec, so it goes over the wire exactly as a real frame would.
- **`{"raw": "..."}`** - the string is injected verbatim, bypassing the codec. This is the only way to
  express a message the codec refuses to decode (`undecodable-message`), which by definition cannot
  be built from a frame.

The runner yields the event loop between entries, so `inbound` order is delivery order.

### `expect_out` is an ordered subset match

Each entry lists envelope keys that must all hold on some frame the peer sent, and matching consumes
that frame: the *n*-th entry matches at or after the position where the (*n*−1)-th matched. Keys not
listed are ignored, for the same WSM-FRM-001 reason as `expect_frame`.

`"expect_out": []` is not "no assertion". It means **nothing at all went out**, and the runner
asserts the sent list is empty. `data-for-closed-id` is the case that needs it: a late frame for a
stream that closed below the high-water mark is expected during a normal race and is silently
ignored - no reset, no connection error, at most a counter (WSM-STM-002). An empty `expect_out` is
the only way to say that, and dropping it would let a peer that answered with a reset pass.

Entries are restricted to `reset` and `goaway` with an integer `code`, which is a schema-level way of
saying that these are the only two frames an invalid message can provoke.

### `connection_survives` is the column that carries the meaning

It is the difference between "the peers can still agree about every other stream" and "they cannot"
(WSM-STM-024), and it is checked against `peer.is_open` after the injection settles.

Two invariants tie the two columns together, and both are asserted:

- A fixture with `connection_survives: true` must never expect a `goaway`. `goaway` ends the
  connection by definition, so the two columns cannot disagree.
- A stream-level failure emits a `reset` and survives; a connection-level failure emits a `goaway`
  and does not.

### The eight required cases

`invalid/` contains **exactly** these eight, no more and no fewer, and
`test_every_invalid_case_of_wsm_tst_003_has_a_fixture` proves it - without that meta-test, a deleted
fixture is a silently passing suite.

| File | The misbehaviour | Level | Survives |
|---|---|---|---|
| `open-wrong-parity.json` | the dialer opens an even id | connection (WSM-SID-005) | no |
| `open-id-not-monotonic.json` | an `open` id not greater than that peer's highest previous open | connection (WSM-SID-005) | no |
| `data-after-end.json` | `data` on a stream that peer already ended | stream (WSM-STM-020/021) | yes |
| `frame-over-max-frame-bytes.json` | an encoded message beyond the receiver's cap | stream (WSM-FRG-031) | yes |
| `undecodable-message.json` | a message the configured codec refuses to decode | connection (WSM-FRM-005) | no |
| `fragment-interrupted-by-non-fragment.json` | a non-fragment frame lands mid-reassembly | stream (WSM-FRG-033) | yes |
| `data-above-high-water-mark.json` | a stream-level frame for an id never opened | connection (WSM-STM-003) | no |
| `data-for-closed-id.json` | `data` for an already-closed id below the mark | neither - ignored (WSM-STM-002) | yes, silently |

---

## Changing the corpus

1. **A failing fixture is a defect in the implementation, not in the fixture.** Nothing in M6 changes
   peer behaviour. If a conformance fixture fails, the fix belongs to the milestone that owns the
   rule. Do not edit a fixture to make a peer pass, and do not change a peer to suit a fixture that
   was just written.
2. **Both runners, one commit.** A new step kind, a new `call`, a new key: change this document and
   both runners together, or the corpus has quietly become two corpora.
3. **The frame corpus is frozen at 1.0.** M6 adds `test_json_wire_is_frozen`, which hashes
   `conformance/frames/*.json` against a committed digest, so changing the JSON wire after 1.0
   requires deliberately editing that digest. Adding a triple changes the digest; that is the point.
4. **Both runners pin the fixture count.** Adding a sequence fixture means raising
   `EXPECTED_SEQUENCE_FIXTURES` in `muxws/conformance_test.py` and in `ts/conformance.spec.ts`, and
   the two numbers must agree - `test_both_runners_collect_the_same_number_of_fixtures` reads the
   TypeScript declaration out of the file to prove they still do, because two independent constants
   drift silently. A fixture that stopped being collected would otherwise be a suite that passes by
   testing nothing. A **third** number lives outside both runners: `fixtures:` in the
   `cross-language.yml` matrix, which pins how many fixtures each live leg must exercise (one fewer
   under `json`, which cannot carry the bytes fixture). The cross-language conductor reads
   `EXPECTED_SEQUENCE_FIXTURES` rather than restating it, so this is the only count a fixture author
   has to remember separately - and it is deliberately outside the driver, because a driver that
   counted its own fixtures could report three of them as thirteen.

## Known divergences from the M6 brief

Recorded here so a runner author does not spend an afternoon on them. Both are reported as gaps
rather than resolved unilaterally.

- **`expect_out` vs `expect_frame` in `invalid/`.** M6 §6 says each invalid case declares
  `expect_frame`; the fixtures written in M1 and both Python runners that read them spell it
  `expect_out` and make it a **list**, so that a case may assert an ordered sequence of outgoing
  frames or - crucially - that *nothing* went out. `expect_frame` in the singular cannot express the
  empty case without a second convention. This document describes what the runners actually read.
  Renaming is a coordinated change across the eight fixtures, `muxws/conformance_test.py` and
  `muxws/conformance_schema_test.py`; until it happens, write `expect_out`.
- **`bytes-payload-under-binary-codec.json` used to be executed by nothing.** *Resolved.* It declares
  `"requires_codec": "msgpack"`, and for a while both runners of record were configured with `json`
  and skipped it while no third runner was configured with msgpack, so the fixture was a **claim
  about WSM-CDC-008 that nothing checked** and the two "the skip set is pinned" tests turned that
  into a green line in the report. Both runners now replay the whole sequence corpus a second time
  under msgpack, in both role assignments, which is what WSM-CDC-007 asks for; `$bytes` is defined
  above and implemented in both; and each runner asserts by name that no fixture requires a codec no
  pass is configured with, so this cannot silently recur. The *cross-language* half of WSM-CDC-007 is
  covered too, and no longer by the hand-written scenario alone: `interop/runner.py` and
  `interop/runner.ts` carry a corpus conductor (`corpus-accept` / `corpus-dial`) that replays these
  same files with a Python peer at one end of a real socket and a TypeScript peer at the other, in
  both role assignments and both codecs. One process conducts, because a sequence fixture is a single
  ordered script that drives *both* peers; the other executes the steps it is sent over a control
  channel that is deliberately not the WebSocket under test. Two readings of the schema differ there
  and are documented in the drivers: `inject` names the peer a message is delivered *to*, so across
  two processes its remote is what sends it, and `settle` is read as cross-process quiescence rather
  than as turns of one event loop, which no second process can observe.
- **The WSM-TST-002 example uses raw stream ids.** The rule in the same paragraph says fixtures MUST
  refer to streams by `stream_ref` and never by a raw id; the example immediately below it writes
  `{"type": "open", "stream": 1, "end": true}` and `{"type": "open", "stream": 2}`, and omits the
  `peer` key entirely. Those raw ids are correct only in one of the two role assignments, which is
  the failure `stream_ref` exists to prevent. The rule wins over the illustration: write
  `stream_ref`, and name the `peer`.
