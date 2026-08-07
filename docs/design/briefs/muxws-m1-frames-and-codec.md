# muxws M1 - Frames, codec seam, errors and the fragmentation splitter

## 1. Goal

At the end of M1 both languages can turn a logical frame into a WebSocket message and back, agree on
the JSON wire form, know every reset code and every exception class, and can split an over-cap payload
into fragments and reassemble it - all as pure functions, with **no socket, no peer and no stream
anywhere in this milestone**. The shared `conformance/frames/` corpus is written here and replayed by
both test suites; `conformance/invalid/` is written here and executed in M2.

## 2. Prerequisites

**M0** left behind: the repository layout, `pyproject.toml` (ruff with `line-length = 120`,
`quote-style = "double"`, per-file `S101` ignore for `*_test.py`, pytest `asyncio_mode = "auto"`,
`python_files = ["*_test.py"]`), `package.json` with `"sideEffects": false` and the `.`/`./node`/
`./msgpack` exports map, eslint + prettier via `eslint-config-velis`, `muxws/__init__.py` carrying
`__version__`, `ts/index.ts` re-exporting `VERSION`, and empty
`conformance/{frames,sequences,invalid}/` directories.

## 3. Files to create or modify

```
muxws/frames.py                  muxws/frames_test.py
muxws/errors.py                  muxws/errors_test.py
muxws/codecs/__init__.py         muxws/codecs/registry_test.py     # Codec protocol + registry
muxws/codecs/json_.py            muxws/codecs/json_test.py         # trailing _ avoids shadowing stdlib json
muxws/fragment.py                muxws/fragment_test.py
muxws/conformance_schema_test.py
muxws/__init__.py                # + Frame, ResetCode, MAX_FRAME_BYTES, errors, register_codec re-exports
ts/frames.ts                     ts/frames.spec.ts
ts/errors.ts                     ts/errors.spec.ts
ts/codec.ts                      ts/codec.spec.ts                  # Codec, registry, JsonCodec
ts/fragment.ts                   ts/fragment.spec.ts
ts/index.ts                      # + the same re-exports
conformance/frames/v1-frames.json
conformance/invalid/*.json       # eight files, listed in §7
```

## 4. Normative rules in force

### 4.1 Envelope (§3.1)

One frame per WebSocket message. Field names are spelled out, never abbreviated, and stay snake_case
in both languages.

| Field | Type | Required | Default | Present on | Meaning |
|---|---|---|---|---|---|
| `type` | string | yes | - | every frame | Frame type. |
| `stream` | int | on stream-level frames | - | `open`, `data`, `reset` | Stream id. Omitted (or `0`) on connection-level frames. |
| `payload` | any codec value | no | absent | `open`, `data`, `reset` | The application value. On `reset`, the optional structured error object. Absent means "no payload", distinct from `null` only if the application chooses to care. Mutually exclusive with `fragment`. |
| `fragment` | string (text codec) / bytes (binary codec) | no | absent | `open`, `data` | A slice of the codec-encoded logical payload. Mutually exclusive with `payload`. |
| `more` | bool | no | `false` | frames with `fragment` | `true` on every fragment but the last. |
| `headers` | object | no | absent | `open` | Application metadata; string keys, codec-encodable values. Never interpreted by muxws. |
| `end` | bool | no | `false` | `open`, `data` | Last frame this peer will send on this stream. |
| `trailers` | object | no | absent | frames with `end: true` | Post-body metadata. |
| `code` | int | yes | - | `reset`, `goaway` | Reset code (§4.4 below). |
| `reason` | string | no | absent | `reset`, `goaway` | Human-readable, for logs. MUST NOT be parsed. |
| `nonce` | string | yes | - | `ping`, `pong` | Opaque, echoed verbatim. Sender-chosen. |
| `last_stream` | int | yes | - | `goaway` | Highest id from the *other* peer this peer has processed and will still complete. |

- **WSM-FRM-001** A receiver MUST ignore unknown envelope fields.
- **WSM-FRM-002** A receiver MUST ignore unknown *frame types*, logging once, and MUST NOT treat them
  as any kind of error.
- **WSM-FRM-004** `payload` and `fragment` MUST NOT both appear on one frame.
- **WSM-FRM-005** A frame missing `type`, or a message the configured codec refuses to decode, is a
  **connection-level** protocol error.
- **WSM-FRM-006** muxws MUST NOT define any message vocabulary inside `payload`: no `kind` field, no
  reserved key, no discriminator of any sort.
- **WSM-FRM-010** `open` opens a stream. Sender: either peer, using its own parity. Fields: `stream`
  (required), `headers`, `payload`/`fragment`+`more`, `end`. `end: true` on `open` is the unary
  request shape.
- **WSM-FRM-011** `data` carries a payload chunk on an existing stream. Fields: `stream`,
  `payload`/`fragment`+`more`, `end`, `trailers`. A peer MUST NOT send `data` on a stream where its
  own side is already half-closed.
- **WSM-FRM-012** End of stream MUST be a flag, never its own frame type. A peer with nothing left to
  say sends `{"type": "data", "stream": N, "end": true}` with no payload. There MUST NOT be an `end`
  frame type.
- **WSM-FRM-013** Trailers MUST ride on the frame carrying `end: true`. There MUST NOT be a trailers
  frame type.
- **WSM-FRM-014** `reset` terminates a stream immediately in both directions. Sender: either peer, in
  any state except `closed`. Fields: `stream`, `code`, `reason`, optional `payload` for a structured
  error object.
- **WSM-FRM-015** `ping`, `pong` and `goaway` are connection-level and MUST omit `stream`
  (or set it to `0`).
- **WSM-FRM-003** *Retired.* It required a peer not to send an extension frame type unless the remote
  had advertised that extension in `settings.extensions`. There is no `settings` frame and no
  extension advertisement (WSM-CON-031); a v1 peer sends only the frame types in §3.2, and a frame
  type the remote must *act* on requires a new generation (WSM-CON-009). The id is not reused.

v1 frame set: `open`, `data`, `reset`, `ping`, `pong`, `goaway`. `window_update` is **reserved and
unimplemented in v1** and MUST NOT be sent. **There is no `settings` frame** (WSM-CON-031): every
limit is either a protocol constant (`MAX_FRAME_BYTES`) or a local receiver-side defence, and no
limit appears on the wire in any form. A `Frame` model with a `settings` field, or a codec that can
encode one, is a defect in this milestone.

### 4.2 Codec seam (§2.1)

- **WSM-CDC-001** A codec MUST expose `name: str` (the wire-visible name), `binary: bool`,
  `encode(frame) -> str | bytes`, and `decode(message) -> Frame`.
- **WSM-CDC-002** `binary` MUST be declared, not inferred from a value's type; the peer uses it to
  select the socket's text or binary send method and the expected inbound message type. A peer MUST
  NOT sniff incoming messages to decide which codec branch to take.
- **WSM-CDC-003** A peer MUST use exactly one codec for the life of its connection.
- **WSM-CDC-004** The library MUST ship and MUST itself register a `json` codec, and JSON MUST be the
  default. JSON is the interoperability baseline: both language ports MUST produce a JSON wire form
  that the other port decodes to an equal logical frame.
- **WSM-CDC-005** Conformance for the JSON codec MUST be asserted as `decode(json_wire) == frame` and
  `decode(encode(frame)) == frame`, comparing parsed objects, never byte-identical output. Exactly one
  separate test MAY pin a canonical key order (`type`, then `stream`, then the remaining keys
  alphabetically) for the benefit of log diffing; no other test may depend on key order or whitespace.
- **WSM-CDC-006** Any codec other than `json` MUST be asserted by round-trip over the same logical
  frame corpus (`decode(encode(frame)) == frame`) and MUST NOT have wire bytes pinned in a fixture.
- **WSM-CDC-008** Under a binary codec, raw bytes (`bytes` / `ArrayBuffer`) are a first-class payload
  type. Under JSON they are not, and muxws MUST NOT base64-encode bytes on the application's behalf.
- **WSM-CDC-013** Registration MUST be explicit: `register_codec(name, codec)` /
  `registerCodec(name, codec)`. There MUST NOT be dynamic imports, lazy auto-registration, entry-point
  scanning, or any probing of whether a module happens to be installed.
- **WSM-CDC-014** A codec module MUST NOT register itself at import time (a side-effecting import can
  never be tree-shaken out).

### 4.3 Sizes and fragmentation (§4)

- **WSM-FRG-001** Every size limit in this specification MUST be measured as the byte length of the
  **fully encoded WebSocket message** - the complete codec output for the frame, envelope included,
  exactly as it goes on the wire. Not the pre-encoding payload, not the `fragment` field alone.
- **WSM-FRG-002** Under a text codec the TypeScript port MUST measure with
  `new TextEncoder().encode(text).length` (or an equivalent incremental byte count) and the Python
  port with `len(text.encode("utf-8"))`. A JavaScript string's `.length` MUST NOT be used (it counts
  UTF-16 code units and disagrees with Python on every non-BMP character).
- **WSM-FRG-003** Under a binary codec both ports MUST take the length of the produced buffer.
- **WSM-FRG-004** `MAX_FRAME_BYTES` is a **protocol constant of 65536** (64 KiB): the largest encoded
  message a sender may emit. It MUST NOT be negotiated, announced, or read from configuration. A
  receiver MUST accept any message up to the constant and MAY accept larger ones; a sender MUST always
  fragment at the constant regardless of what the remote appears willing to accept.
- **WSM-FRG-005** An implementation MAY expose the cap as a construction argument **for tests only**
  (the conformance runner uses it, WSM-TST-002). It MUST NOT be documented as deployment
  configuration and MUST NOT appear on the wire. A cap too small to hold the envelope plus one
  indivisible unit MUST raise a configuration error at peer construction (WSM-FRG-034).
- **WSM-FRG-010** A sender MUST fragment any logical payload whose encoded frame would exceed
  `MAX_FRAME_BYTES` (WSM-FRG-004). Fragmentation is mandatory, not an optimisation.
- **WSM-FRG-011** The sender MUST encode the logical payload with the connection's codec, slice that
  encoded form, and put each slice into a frame that the codec then encodes again.
- **WSM-FRG-012** Slices MUST be cut at a boundary the codec can represent: Unicode codepoint
  boundaries of the encoded text for a text codec, byte boundaries for a binary one. Under JSON in
  TypeScript this additionally means never splitting a surrogate pair. A splitter that would land
  mid-sequence MUST move the boundary backwards.
- **WSM-FRG-013** The sender MUST budget for the envelope and for re-encoding expansion: slice to
  `cap - reservation` bytes of encoded payload, where `reservation = min(512, cap // 2)`.
- **WSM-FRG-014** The sender MUST then **verify and re-split**: encode the frame, and if the encoded
  message still exceeds the cap, re-split that slice and try again. The reservation is a per-codec
  hint; the loop is the guarantee.
- **WSM-FRG-015** The splitter MUST be a pure function of `(payload, cap, codec)` and MUST be tested
  as one, independently of any socket.
- **WSM-FRG-016** Both ports MUST produce the **same fragment boundaries** for the same payload, cap
  and codec.
- **WSM-FRG-020** `end: true` MUST appear only on the final fragment of a payload.
- **WSM-FRG-021** `headers` MUST NOT be fragmented. An `open` whose headers alone push the frame over
  the cap MUST be rejected by the receiver with `reset(PAYLOAD_TOO_LARGE)`.
- **WSM-FRG-030** The receiver MUST concatenate `fragment` values and hand the result to the codec for
  decoding when a fragment arrives without `more: true`.
- **WSM-FRG-034** A test-override cap (WSM-FRG-005) too small to hold the envelope plus one
  indivisible unit MUST raise a configuration error when the peer is constructed, not be discovered
  later as an infinite split loop. *(M1 owns the splitter's half of this: `split_frame` raises on such
  a cap rather than looping. The peer-construction check is M5a's, where the cap reaches a peer.)*

### 4.4 Reset codes and exceptions (§8.1, §8.2)

| Code | Name | Raised when | Required reaction |
|---|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, orderly shutdown; on `reset`, "done and no longer interested". | None. Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. | Stop producing; do not retry. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. `reason` carries a message; an optional `payload` carries a structured error object. | Surface to the caller. Retry is the application's call. |
| 3 | `PROTOCOL_ERROR` | The peer violated this specification. | Fix the implementation. Never retried automatically. |
| 4 | `REFUSED` | Not accepted and definitively not processed. Used for no registered handler, for post-`goaway` opens, and for an `open` beyond the receiver's own concurrency limit (WSM-STM-036). | Retry: elsewhere if another connection is available, otherwise after a delay - the receiver may be saturated. |
| 5 | - | **Retired.** Was `STREAM_LIMIT`, for rejection against an announced `max_concurrent_streams` (WSM-STM-022). The number MUST NOT be reused and MUST NOT appear on the wire. | - |
| 6 | `TIMEOUT` | A deadline expired locally; the reset informs the remote so it can stop working. | Stop producing. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message exceeded what the receiver accepts (WSM-FRG-031), or a payload exceeded the receiver's `max_payload_bytes` (WSM-FRG-032). | Do not retry unchanged; fragment or shrink. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. | Surface and log. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies, on every stream live at that instant. **MUST NEVER appear on the wire.** | Do not retry on this peer now; rebuild from `on_reconnect`. |

`ResetCode` therefore has **nine** members, not ten: `5` is a hole in the numbering and MUST NOT be
defined as a name in either language.

```
MuxwsError
├── ProtocolError            # this peer or the remote violated the spec
├── ConnectionClosed         # socket died; carries .code, .reason, .was_clean
├── ConnectionGoingAway      # open() after goaway - raised synchronously out of open()
├── StreamAlreadyConsumed    # await and iterate, or two iterations, on one stream
├── StreamClosed             # send()/end()/reply() on a stream that closed normally
├── CodecError               # configuration; carries .configured and .available
│   ├── CodecNotRegistered   # configured name never registered - raised at startup
│   └── CodecMismatch        # acceptor's codec differs; the handshake was rejected
└── StreamReset              # carries .code (ResetCode), .reason, .stream_id
    ├── RemoteError          # code == APPLICATION_ERROR; carries .payload
    ├── StreamTimeout        # code == TIMEOUT
    ├── StreamRefused        # code == REFUSED; not processed - retry, elsewhere or later
    └── ConnectionLost       # code == CONNECTION_CLOSED; synthesised locally, never from the wire
```

- **WSM-ERR-001** *Retired.* It required `StreamRefused` and `StreamLimit` to be sibling classes. With
  the announced quota gone there is no `StreamLimit` (WSM-STM-022, WSM-API-004) and `StreamRefused`
  covers every refusal. The id is not reused.
- **WSM-ERR-009** `send()`, `end()` and `reply()` on a stream that is no longer open MUST raise, and
  the type MUST distinguish the three cases: `StreamClosed` when the stream closed **normally** (both
  ends ended), that stream's own `StreamReset` subclass when it was reset, and `ConnectionLost` when
  the socket died (WSM-RCN-041). `StreamClosed` MUST NOT be a `StreamReset` subclass and MUST NOT be
  a `ProtocolError`: a normal close racing a last `send()` is an expected outcome, not a failure and
  not a caller bug. Test: `stream_test.py::test_send_after_normal_close_raises_stream_closed`.
  *(M1 defines the class and its place in the tree; the raising behaviour is M2's, which reproduces
  this rule.)*
- **WSM-ERR-002** `ConnectionLost` MUST be a `StreamReset` subclass; `ConnectionClosed` MUST NOT be.
- **WSM-ERR-004** TypeScript MUST mirror this hierarchy with classes of the same names, delivered as
  promise rejections and as `throw` inside `for await`. Both languages MUST set a `name` / `__class__`
  discriminator so cross-language tests can assert on error identity.
- **WSM-ERR-005** `CodecNotRegistered` and `CodecMismatch` MUST sit outside `StreamReset`: neither is
  a stream failure and neither is retryable.
- **WSM-ERR-007** muxws MUST NOT map exceptions to status codes of any kind.
- **WSM-INV-016** muxws MUST NOT define a message vocabulary (WSM-FRM-006, above) - or two independent
  consumers sharing one socket must both nest their own vocabulary inside an imposed one, and every
  change to either needs a muxws release.
- **WSM-INV-003** Every size limit MUST be counted in bytes of the **codec's own output**
  (WSM-FRG-001, above) - or a sender budgeting against JSON text while msgpack bytes go on the wire
  produces over-cap frames on exactly the deployments that chose the compact codec.

### 4.5 Conformance corpus (§16)

- **WSM-TST-001** `conformance/frames/*.json` MUST be a list of `{"name", "frame", "json_wire"}`
  triples - a logical frame plus the JSON rendering pinned alongside it - read verbatim by both
  `pytest` and `vitest`.
- **WSM-TST-003** `conformance/invalid/*.json` MUST cover, each asserting which frame goes out and
  whether the connection survives: wrong parity; an `open` id not greater than that peer's highest
  previous open; `data` after `end`; an over-cap encoded message; a message the configured codec
  refuses to decode; a fragment sequence interrupted by a non-fragment frame; a stream-level frame
  above the high-water mark (connection dies); a `data` frame for an already-closed id (connection
  survives, nothing goes out).

## 5. Decisions this brief takes

The specification leaves these to the implementer. They are decided here; implement them as written.

- **D1 - "absent" is a sentinel, not `None`.** `payload` distinguishes *absent* from `null`, so
  `Frame.payload` defaults to a module-level singleton `ABSENT` (TypeScript: an exported `ABSENT`
  symbol). `to_mapping` omits the key when it is `ABSENT` and emits `"payload": null` when it is
  `None`. Every other optional field uses `None`/`undefined` and is omitted when unset.
- **D2 - unknown fields are dropped at decode, not preserved.** WSM-FRM-001 says ignore; a decoder that
  round-tripped them would make `decode(encode(frame)) == frame` pass on garbage.
- **D3 - `Frame` is frozen and compares by value.** Python `@dataclass(frozen=True, slots=True)`;
  TypeScript a readonly interface plus a `framesEqual(a, b)` helper used by the specs.
- **D4 - unknown frame *types* survive decoding** as a `Frame` carrying that `type` string; it is the
  peer (M2) that ignores them. A codec MUST NOT raise on an unrecognised `type`.

## 6. Implementation notes and skeletons

### `muxws/frames.py`

```python
ABSENT: Any = _Absent()          # single module-level sentinel; falsy, repr "ABSENT"


@dataclass(frozen=True, slots=True)
class Frame:
    """One logical protocol unit. Exactly one frame per WebSocket message."""

    type: str
    stream: int | None = None
    payload: Any = ABSENT
    fragment: str | bytes | None = None
    more: bool = False
    headers: dict[str, Any] | None = None
    end: bool = False
    trailers: dict[str, Any] | None = None
    code: int | None = None
    reason: str | None = None
    nonce: str | None = None
    last_stream: int | None = None


def to_mapping(frame: Frame) -> dict[str, Any]:
    """Envelope with defaults omitted, keys ordered type, stream, then alphabetically (WSM-CDC-005)."""


def from_mapping(mapping: Mapping[str, Any]) -> Frame:
    """Drop unknown keys (WSM-FRM-001). Raise ProtocolError on a missing `type` (WSM-FRM-005) or on
    payload and fragment together (WSM-FRM-004)."""
```

### `muxws/codecs/__init__.py`

```python
class Codec(Protocol):
    name: str
    binary: bool

    def encode(self, frame: Frame) -> str | bytes: ...
    def decode(self, message: str | bytes) -> Frame: ...


def register_codec(name: str, codec: Codec) -> None: ...
def get_codec(name: str) -> Codec:
    """Raise CodecNotRegistered naming MUXWS_CODEC, the value found, and the registered set."""
def registered_codecs() -> list[str]: ...
```

`muxws/__init__.py` calls `register_codec("json", JsonCodec())` - the **library** registers JSON
(WSM-CDC-004); `muxws/codecs/json_.py` itself must not (WSM-CDC-014). `ts/index.ts` does the same for
`ts/codec.ts`'s `JsonCodec`, and `ts/msgpack.ts` stays a stub that registers nothing.

### `muxws/fragment.py`

```python
MAX_FRAME_BYTES: Final[int] = 65_536      # protocol constant, WSM-FRG-004 - re-exported from muxws/


def encoded_length(message: str | bytes) -> int:
    """UTF-8 byte length for text, buffer length for bytes (WSM-FRG-002/003)."""


def split_frame(frame: Frame, cap: int, codec: Codec) -> list[Frame]:
    """Pure. Return [frame] when it already fits, else the fragment frames replacing it.

    `cap` defaults to MAX_FRAME_BYTES; a caller passes a smaller one only from a test or the
    conformance runner (WSM-FRG-005). It is never a value read off the wire.

    Encode frame.payload with codec, slice the encoded form to cap - min(512, cap // 2) bytes at
    codepoint boundaries, build each fragment frame, encode it, and re-split any slice whose encoded
    message still exceeds cap (WSM-FRG-013/014). `more` is true on all but the last; `end` and
    `trailers` ride only on the last; `headers` ride only on the first and are never split
    (WSM-FRG-020/021). Raise ProtocolError if the envelope alone does not fit under cap
    (WSM-FRG-034) - an infinite split loop is the failure this prevents.
    """


class Assembler:
    """Receive side. feed() returns ABSENT while `more` is true, else the decoded payload."""

    def feed(self, frame: Frame, codec: Codec) -> Any: ...
    @property
    def in_progress(self) -> bool: ...
```

`ts/fragment.ts` mirrors all three as `encodedLength`, `splitFrame`, `Assembler`. Slice with a
codepoint cursor (`Array.from(text)` or `for (const ch of text)`), never `String.prototype.slice` on
UTF-16 indices, or a surrogate pair splits and the two ports disagree on boundaries (WSM-FRG-016).

### Lint rules that bite in this milestone

1. `UP` requires `str | bytes`, never `Optional[...]` or `Union[...]`.
2. `PT` - use `pytest.raises(ProtocolError)` and `@pytest.mark.parametrize` with tuple argnames; a
   corpus loop is better written as a parametrize over the fixture than as a `for` inside one test
   (a failure then names the case).
3. `S101` is ignored only in `*_test.py`, so any fixture-loading helper must live in a `*_test.py`
   file or raise `AssertionError` explicitly rather than `assert`.
4. `ARG` - unused parameters on test doubles (a fake codec's `encode`) must be `_`-prefixed.
5. TypeScript: kebab-case file names, single-quoted strings, `for...in` forbidden - walk the envelope
   with `Object.entries`.
6. The Python module is `codecs/json_.py`, not `json.py`: a top-level `json.py` inside the package
   would shadow the stdlib module the codec itself imports.

## 7. Tests to write

### Python

| # | Test | Asserts |
|---|---|---|
| 1 | `frames_test.py::test_conformance_wire_decodes_to_frame` | For every triple in `conformance/frames/v1-frames.json`: `codec.decode(json_wire) == frame` (WSM-CDC-005, WSM-TST-001). |
| 2 | `frames_test.py::test_conformance_round_trips` | `decode(encode(frame)) == frame` for every triple, comparing parsed objects, never bytes. |
| 3 | `frames_test.py::test_canonical_key_order` | The single permitted key-order test: `type`, `stream`, then alphabetical (WSM-CDC-005). |
| 4 | `frames_test.py::test_unknown_fields_are_dropped` | A wire object carrying `"colour": "red"` decodes equal to the same frame without it (WSM-FRM-001, D2). |
| 5 | `frames_test.py::test_unknown_frame_type_decodes` | `{"type": "window_update"}` decodes to a `Frame` and does not raise (WSM-FRM-002, D4). |
| 6 | `frames_test.py::test_missing_type_and_payload_with_fragment_raise` | Both raise `ProtocolError` (WSM-FRM-004/005). |
| 7 | `frames_test.py::test_absent_payload_is_not_null` | `Frame("data", stream=1)` emits no `payload` key; `payload=None` emits `"payload": null` (D1). |
| 8 | `codecs/registry_test.py::test_json_is_registered_by_the_library` | Importing `muxws` registers `"json"`; importing `muxws.codecs.json_` alone registers nothing (WSM-CDC-004/014). |
| 9 | `codecs/registry_test.py::test_unregistered_name_raises_codec_not_registered` | `get_codec("msgpack")` raises `CodecNotRegistered` whose message names `MUXWS_CODEC`, the value, and the registered set (WSM-CDC-013). |
| 10 | `codecs/json_test.py::test_binary_is_declared_false` | `JsonCodec.binary is False`, `JsonCodec.name == "json"` (WSM-CDC-001/002). |
| 11 | `codecs/json_test.py::test_bytes_payload_is_rejected_not_base64ed` | Encoding a `bytes` payload raises rather than silently base64-encoding it (WSM-CDC-008). |
| 12 | `fragment_test.py::test_slice_point_sweep_never_exceeds_cap` | Sweep caps 64..4096 over the corpus payloads; **every** produced encoded message is `<= cap` in bytes of the codec's output (WSM-FRG-001/003). |
| 13 | `fragment_test.py::test_slice_point_inside_multibyte_codepoint_moves_back` | A payload of 3- and 4-byte codepoints splits only at codepoint boundaries; every fragment is valid UTF-8 (WSM-FRG-012). |
| 14 | `fragment_test.py::test_control_character_payload_is_resplit_not_emitted_over_cap` | A control-character payload whose JSON escaping triples its encoded size still yields only under-cap messages - the verify-and-re-split loop (WSM-FRG-014). |
| 15 | `fragment_test.py::test_end_and_trailers_only_on_the_last_fragment` | `more` true on all but the last; `end`/`trailers` only on the last; `headers` only on the first and never split (WSM-FRG-020/021). |
| 16 | `fragment_test.py::test_assembler_round_trips_every_corpus_payload` | `Assembler` fed `split_frame`'s output returns the original payload, and `in_progress` is true until the final fragment (WSM-FRG-030). |
| 17 | `fragment_test.py::test_cap_below_envelope_floor_raises` | A cap too small for envelope plus one unit raises instead of looping forever (WSM-FRG-034). |
| 18 | `fragment_test.py::test_split_is_pure` | Two calls with the same `(frame, cap, codec)` return equal results and mutate neither argument (WSM-FRG-015). |
| 19 | `errors_test.py::test_hierarchy` | `issubclass(ConnectionLost, StreamReset)`; `not issubclass(ConnectionClosed, StreamReset)`; `StreamClosed` is neither a `StreamReset` nor a `ProtocolError`; `CodecError` sits outside `StreamReset` (WSM-ERR-002/005/009). |
| 20 | `errors_test.py::test_reset_codes_are_the_pinned_integers` | `ResetCode` values match the table above exactly, and **`5` is not defined**: `ResetCode` has nine members and no name maps to the retired number. |
| 20a | `frames_test.py::test_no_settings_frame_exists` | `Frame` has no `settings` or `ack` field, and a wire object carrying `"settings": {...}` decodes to a frame that drops both keys as unknown (WSM-CON-031, WSM-FRM-001). |
| 21 | `conformance_schema_test.py::test_invalid_fixtures_parse` | Each `conformance/invalid/*.json` parses and carries `name`, `inbound`, `expect_out`, `connection_survives`. |

### TypeScript

`frames.spec.ts`, `codec.spec.ts`, `fragment.spec.ts` and `errors.spec.ts` mirror tests 1-20a one for
one, reading the **same** `conformance/frames/v1-frames.json`. Two are TypeScript-only:

| # | Test | Asserts |
|---|---|---|
| 22 | `fragment.spec.ts` - "measures bytes, not UTF-16 units" | An astral-plane payload produces the same boundaries as the Python port; an implementation measuring with `.length` fails this (WSM-FRG-002/016). |
| 23 | `fragment.spec.ts` - "never splits a surrogate pair" | Every fragment is well-formed: `[...fragment]` never yields a lone surrogate (WSM-FRG-012). |

### Fixtures written here

`conformance/frames/v1-frames.json` MUST cover at least: unary `open(end)`; `open` with headers; `data`
with `end` and trailers; a fragmented `open` (two fragments); `reset` with a structured error payload;
`ping` and `pong`; `goaway`; a frame with `payload: null`; a frame with no payload at all; a payload
containing non-BMP characters. It MUST NOT contain a `settings` frame - there is no such frame type
(WSM-CON-031).

`conformance/invalid/` gets the eight files WSM-TST-003 enumerates, all in one schema -
`{"name", "description", "inbound": [...], "expect_out": [...], "connection_survives": bool}`:
`open-wrong-parity.json`, `open-id-not-monotonic.json`, `data-after-end.json`,
`frame-over-max-frame-bytes.json`, `undecodable-message.json`,
`fragment-interrupted-by-non-fragment.json`, `data-above-high-water-mark.json` (connection dies),
`data-for-closed-id.json` (connection survives, nothing goes out). M1 writes and schema-validates
them; **M2 executes them**.

## 8. Done when

- [ ] `ruff check .` and `ruff format --check .` pass.
- [ ] `pytest --cov=muxws` passes; `frames.py`, `fragment.py`, `errors.py` and `codecs/` reach 100 %
      line coverage - they are pure functions, so there is no excuse.
- [ ] `npm run lint`, `npm test` and `npm run build` pass.
- [ ] Both suites read `conformance/frames/v1-frames.json` unchanged and both pass on it.
- [ ] A cap sweep of 64..4096 over the corpus produces no encoded message above the cap in either port,
      and both ports produce identical fragment boundaries for every corpus payload.
- [ ] `python -c "import muxws; print(muxws.registered_codecs())"` prints `['json']`.
- [ ] `ts/msgpack.ts` still registers nothing at import time.

## 9. Out of scope

No `Peer`, no `Stream`, no state machine, no id allocation, no sockets, no
`conf.py` / `MUXWS_CODEC` reading (M3), no subprotocol assertion (M3), no msgpack codec (M6). No
`conformance/sequences/` fixture is written here: M4 adds the first one
(`goaway-drains-then-closes.json`), M5a adds `small-frame-overtakes-a-fragmented-payload.json`, and
M6 completes the corpus. The fragmentation splitter is written and tested here but **not wired into
any send path** - the one-unsent-fragment rule and the round-robin writer are M5a. Receive-side
enforcement (the frame-size check, `max_payload_bytes`, the concurrency limit) is M5a; M1 supplies
only the measurement function and the constant they will use.
