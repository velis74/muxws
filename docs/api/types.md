---
outline: deep
---

# Types

The shared vocabulary: the frame model that every codec encodes, the constants that bound it, and the
callable types the peer takes as hooks.

Field names are spelled out, never abbreviated, and stay **snake_case in both languages** —
`last_stream` is `last_stream` in TypeScript too, because it is a wire name and the wire has one
spelling.

## `Frame`

One logical protocol unit, and one WebSocket message. Python's is a frozen slotted dataclass compared
by value; TypeScript's is a readonly interface compared with `framesEqual`.

Twelve fields, of which most frames carry three or four. A field still at its default is **omitted
from the envelope entirely**, which is why the JSON form of a `ping` is
`{"type":"ping","nonce":"771c698c25d5c0d1"}` — 42 bytes, two keys, and nothing for the ten fields it
does not use.

### The envelope fields

| Field | Type | Default | What it carries |
|---|---|---|---|
| `type` | `str` / `string` | required | The frame type. A v1 peer sends `open`, `data`, `reset`, `ping`, `pong` or `goaway`. Always emitted, even though it has no default to compare against. |
| `stream` | `int \| None` / `number \| null` | `None` / `null` | Which stream this frame belongs to. Absent on `ping`, `pong` and `goaway`, which are connection-level. Odd ids are the dialer's, even ids the acceptor's. |
| `payload` | `Any` / `unknown \| Absent` | `ABSENT` | The application value. The **one** field whose default is not null: `ABSENT` omits the key entirely, while `None` / `null` emits `"payload": null`. That distinction is why the sentinel exists — "no payload" and "a payload that is null" are different frames. |
| `fragment` | `str \| bytes \| None` / `string \| ArrayBuffer \| null` | `None` / `null` | One slice of an encoded payload. A frame carries `payload` **or** `fragment`, never both. |
| `more` | `bool` / `boolean` | `False` / `false` | True on every fragment but the last. The receiver concatenates until it sees a fragment without it. |
| `headers` | `dict[str, Any] \| None` / `Record<string, unknown> \| null` | `None` / `null` | Per-stream metadata, sent once with the `open`. They ride the **first** fragment and are never split across fragments. |
| `end` | `bool` / `boolean` | `False` / `false` | This side is done sending. A **flag on a frame, never a frame of its own** — there is no "end" frame type. It rides the last fragment. |
| `trailers` | `dict[str, Any] \| None` / `Record<string, unknown> \| null` | `None` / `null` | Metadata that could only be known after the payloads — a row count, a checksum. Rides the `end` frame, and only the last fragment of it. |
| `code` | `int \| None` / `number \| null` | `None` / `null` | A `ResetCode` value, on `reset` and `goaway`. Carried as an integer, so a code this generation does not define survives the trip. |
| `reason` | `str \| None` / `string \| null` | `None` / `null` | Human-readable text accompanying `code`. Never parsed for meaning. |
| `nonce` | `str \| None` / `string \| null` | `None` / `null` | Correlates a `pong` with the `ping` that provoked it. Echoed back verbatim. |
| `last_stream` | `int \| None` / `number \| null` | `None` / `null` | On `goaway`: the highest stream id, of **the remote's** parity, that this peer has dispatched. Everything above it was never processed and is safe to retry elsewhere. |

Envelope keys are emitted in a fixed order — `type`, then `stream`, then the rest alphabetically —
so that two implementations produce byte-identical output for the same frame.

Unknown keys arriving on the wire are **dropped**, not preserved. An unrecognised `type` survives as
an ordinary `Frame`; it is the peer, not the codec, that ignores it. Both are what let a later
generation add a field or a frame type without breaking a 1.0 peer.

### Signature

```python
@dataclass(frozen=True, slots=True)
class Frame:
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
```

```ts
export interface Frame {
  readonly type: string;
  readonly stream?: number | null;
  readonly payload?: unknown | Absent;
  readonly fragment?: string | ArrayBuffer | null;
  readonly more?: boolean;
  readonly headers?: Record<string, unknown> | null;
  readonly end?: boolean;
  readonly trailers?: Record<string, unknown> | null;
  readonly code?: number | null;
  readonly reason?: string | null;
  readonly nonce?: string | null;
  readonly last_stream?: number | null;
}
```

### Parameters

Every constructor parameter is an envelope field; see the table above for what each carries.

| Name | Type | Default | What it does |
|---|---|---|---|
| `type` | `str` / `string` | required | The only field with no default. |
| `stream` | `int \| None` / `number \| null` | `None` / `null` | See the field table. |
| `payload` | `Any` / `unknown \| Absent` | `ABSENT` | See the field table. In TypeScript, omitting the property and setting it to `ABSENT` mean the same thing. |
| `fragment` | `str \| bytes \| None` / `string \| ArrayBuffer \| null` | `None` / `null` | See the field table. |
| `more` | `bool` / `boolean` | `False` / `false` | See the field table. |
| `headers` | `dict[str, Any] \| None` / `Record<string, unknown> \| null` | `None` / `null` | See the field table. |
| `end` | `bool` / `boolean` | `False` / `false` | See the field table. |
| `trailers` | `dict[str, Any] \| None` / `Record<string, unknown> \| null` | `None` / `null` | See the field table. |
| `code` | `int \| None` / `number \| null` | `None` / `null` | See the field table. |
| `reason` | `str \| None` / `string \| null` | `None` / `null` | See the field table. |
| `nonce` | `str \| None` / `string \| null` | `None` / `null` | See the field table. |
| `last_stream` | `int \| None` / `number \| null` | `None` / `null` | See the field table. Snake_case in both languages. |

All twelve are positional-or-keyword in Python, in the order above.

### Return

Python: a frozen `Frame` instance; assigning to a field raises `FrozenInstanceError`. TypeScript:
`Frame` is an interface, so an object literal *is* a frame.

### Raises

Raises: nothing. Python's dataclass raises `TypeError` only for the usual reasons — a missing `type`,
or an argument that is not a field.

### Example

```python
import dataclasses

from muxws import ABSENT, Frame, JsonCodec

codec = JsonCodec()

opening = Frame("open", stream=1, payload={"path": "/reports"}, headers={"trace": "abc"})
print(codec.encode(opening))

closing = Frame("data", stream=1, payload={"rows": 3}, end=True, trailers={"checksum": "9f"})
print(codec.encode(closing))

print(codec.encode(Frame("ping", nonce="n-1")))
print(codec.encode(Frame("goaway", code=0, reason="restart", last_stream=4)))

# ABSENT omits the key; None emits it.
print(codec.encode(Frame("data", stream=1, payload=ABSENT, end=True)))
print(codec.encode(Frame("data", stream=1, payload=None, end=True)))

try:
    setattr(opening, "type", "data")  # noqa: B010 - the point of the example is that this fails
except dataclasses.FrozenInstanceError:
    print("frames are frozen; dataclasses.replace() builds a new one instead")
print(dataclasses.replace(opening, type="data").type)
```

```ts
import { ABSENT, type Frame, JsonCodec } from 'muxws';

const codec = new JsonCodec();

const opening: Frame = { type: 'open', stream: 1, payload: { path: '/reports' }, headers: { trace: 'abc' } };
console.log(codec.encode(opening));

console.log(codec.encode({ type: 'data', stream: 1, payload: { rows: 3 }, end: true, trailers: { checksum: '9f' } }));
console.log(codec.encode({ type: 'ping', nonce: 'n-1' }));
console.log(codec.encode({ type: 'goaway', code: 0, reason: 'restart', last_stream: 4 }));

// ABSENT omits the key; null emits it.
console.log(codec.encode({ type: 'data', stream: 1, payload: ABSENT, end: true }));
console.log(codec.encode({ type: 'data', stream: 1, payload: null, end: true }));
```

## `ABSENT` and `Absent`

The sentinel that distinguishes "no payload" from an explicit `null`. It is the default of
`Frame.payload`, and it is what `Assembler.feed` returns while more fragments are expected.

Python's is a single module-level instance of a private class. It is **falsy**, so `if frame.payload:`
behaves the way a reader expects, and its `repr` is `ABSENT` rather than a memory address.
TypeScript's is a `unique symbol`: it cannot be forged, cannot be serialized by accident, and prints
as `Symbol(ABSENT)` in a failing assertion. `Absent` is its type.

Compare with identity — `is ABSENT` / `=== ABSENT` — never with `==`.

### Signature

```python
ABSENT: Any = _Absent()
```

```ts
export const ABSENT: unique symbol;
export type Absent = typeof ABSENT;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A value, not a callable. There is exactly one of it per process. |

### Return

Nothing — it is a constant.

### Raises

Raises: nothing.

### Example

```python
import copy

from muxws import ABSENT, Frame, JsonCodec

print(repr(ABSENT), "| falsy:", not ABSENT)
print("one instance:", copy.deepcopy(ABSENT) is ABSENT)

codec = JsonCodec()
print("omitted:", codec.encode(Frame("data", stream=1, payload=ABSENT, end=True)))
print("explicit null:", codec.encode(Frame("data", stream=1, payload=None, end=True)))
print("round trip keeps them apart:", codec.decode(codec.encode(Frame("ping"))).payload is ABSENT)
```

```ts
import { ABSENT, type Absent, JsonCodec } from 'muxws';

console.log(String(ABSENT));

const codec = new JsonCodec();
console.log('omitted:', codec.encode({ type: 'data', stream: 1, payload: ABSENT, end: true }));
console.log('explicit null:', codec.encode({ type: 'data', stream: 1, payload: null, end: true }));
console.log('round trip keeps them apart:', codec.decode(codec.encode({ type: 'ping' })).payload === ABSENT);

const nothing: Absent = ABSENT;
console.log('the type has one inhabitant:', nothing === ABSENT);
```

## `V1_FRAME_TYPES`

The frame types a v1 peer **sends**. A receiver tolerates anything not in this set: an unknown type
is ignored, not an error, which is what lets a later generation add one without breaking a 1.0 peer.

`window_update` is reserved and unimplemented, and there is no `settings` frame — no limit is ever
negotiated on this wire.

TypeScript exports it from the package root; in Python, import it from `muxws.frames`.

### Signature

```python
V1_FRAME_TYPES: Final[frozenset[str]] = frozenset({"open", "data", "reset", "ping", "pong", "goaway"})
```

```ts
export const V1_FRAME_TYPES: ReadonlySet<string>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A constant set, not a callable. |

### Return

Nothing — it is a constant. Python's is a `frozenset`; TypeScript's is a `ReadonlySet` (readonly to
the type checker; the underlying `Set` is not frozen at runtime).

### Raises

Raises: nothing.

### Example

```python
from muxws.frames import V1_FRAME_TYPES

print(sorted(V1_FRAME_TYPES))
print("window_update is reserved:", "window_update" in V1_FRAME_TYPES)
print("there is no settings frame:", "settings" in V1_FRAME_TYPES)
```

```ts
import { V1_FRAME_TYPES } from 'muxws';

console.log([...V1_FRAME_TYPES].sort());
console.log('window_update is reserved:', V1_FRAME_TYPES.has('window_update'));
console.log('there is no settings frame:', V1_FRAME_TYPES.has('settings'));
```

## `to_mapping` / `toMapping`

Renders a frame as an envelope — a plain dict / object, ready for a codec to serialize. Every field
still at its default is omitted; keys come out `type`, then `stream`, then the rest alphabetically.

A codec calls this. You call it when you want to see what a frame *is* without picking a codec.

### Signature

```python
def to_mapping(frame: Frame) -> dict[str, Any]: ...
```

```ts
export function toMapping(frame: Frame): Record<string, unknown>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `frame` | `Frame` | required | The frame to render. It is not mutated. |

### Return

A new `dict[str, Any]` / `Record<string, unknown>` holding only the fields that differ from their
defaults, plus `type` always. `payload` appears when it is anything other than `ABSENT`, including
`None` / `null`.

### Raises

Raises: nothing.

### Example

```python
from muxws import ABSENT, Frame, to_mapping

print(to_mapping(Frame("ping", nonce="n-1")))
print(to_mapping(Frame("open", stream=3, payload={"q": 1}, headers={"trace": "abc"}, end=True)))
print(to_mapping(Frame("data", stream=3, payload=ABSENT, end=True)))
print(to_mapping(Frame("data", stream=3, payload=None, end=True)))
print("key order:", list(to_mapping(Frame("reset", stream=3, code=1, reason="stop"))))
```

```ts
import { ABSENT, toMapping } from 'muxws';

console.log(toMapping({ type: 'ping', nonce: 'n-1' }));
console.log(toMapping({ type: 'open', stream: 3, payload: { q: 1 }, headers: { trace: 'abc' }, end: true }));
console.log(toMapping({ type: 'data', stream: 3, payload: ABSENT, end: true }));
console.log(toMapping({ type: 'data', stream: 3, payload: null, end: true }));
console.log('key order:', Object.keys(toMapping({ type: 'reset', stream: 3, code: 1, reason: 'stop' })));
```

## `from_mapping` / `fromMapping`

Builds a `Frame` from a decoded envelope. The inverse of `to_mapping` / `toMapping`, with two
deliberate asymmetries: unknown keys are dropped rather than preserved, and an unrecognised `type`
survives as an ordinary frame.

Dropping unknown keys is what keeps `decode(encode(frame)) == frame` from passing on garbage.

### Signature

```python
def from_mapping(mapping: Mapping[str, Any]) -> Frame: ...
```

```ts
export function fromMapping(mapping: Record<string, unknown>): Frame;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `mapping` | `Mapping[str, Any]` / `Record<string, unknown>` | required | A decoded envelope. Keys that are not envelope fields are ignored. When `payload` is absent from the mapping, the frame's `payload` becomes `ABSENT`. |

### Return

A `Frame`.

### Raises

`ProtocolError` when the mapping has no `type` key, and `ProtocolError` when it carries both
`payload` and `fragment` — a frame is one or the other.

### Example

```python
from muxws import ABSENT, from_mapping, ProtocolError

frame = from_mapping({"type": "data", "stream": 3, "payload": {"n": 1}, "end": True, "future_field": "ignored"})
print(frame.type, frame.stream, frame.payload, frame.end)
print("unknown keys are dropped:", not hasattr(frame, "future_field"))
print("unknown types survive:", from_mapping({"type": "window_update", "stream": 3}).type)
print("no payload key means ABSENT:", from_mapping({"type": "ping"}).payload is ABSENT)

for bad in ({"stream": 1}, {"type": "data", "payload": 1, "fragment": "a"}):
    try:
        from_mapping(bad)
    except ProtocolError as exc:
        print("rejected:", exc)
```

```ts
import { ABSENT, ProtocolError, fromMapping } from 'muxws';

const frame = fromMapping({ type: 'data', stream: 3, payload: { n: 1 }, end: true, future_field: 'ignored' });
console.log(frame.type, frame.stream, frame.payload, frame.end);
console.log('unknown keys are dropped:', !('future_field' in frame));
console.log('unknown types survive:', fromMapping({ type: 'window_update', stream: 3 }).type);
console.log('no payload key means ABSENT:', fromMapping({ type: 'ping' }).payload === ABSENT);

[{ stream: 1 }, { type: 'data', payload: 1, fragment: 'a' }].forEach((bad) => {
  try {
    fromMapping(bad);
  } catch (error) {
    if (error instanceof ProtocolError) console.log('rejected:', error.message);
  }
});
```

## `framesEqual`

TypeScript only. Structural equality over the twelve envelope fields, with defaults normalised first,
so a frame written `{ type: 'ping' }` compares equal to one written
`{ type: 'ping', stream: null, more: false }`.

Python needs no equivalent: `Frame` is a frozen dataclass and `==` already does this.

`ArrayBuffer`s are compared byte by byte, which is what makes a round-tripped bytes payload compare
equal to the one that went in.

### Signature

```ts
export function framesEqual(a: Frame, b: Frame): boolean;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `a` | `Frame` | required | The left frame. |
| `b` | `Frame` | required | The right frame. |

### Return

`true` when every one of the twelve fields is equal after defaults are normalised, otherwise `false`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, framesEqual } from 'muxws';

console.log(framesEqual({ type: 'ping' }, { type: 'ping', stream: null, more: false, end: false }));
console.log(framesEqual({ type: 'ping' }, { type: 'pong' }));

const codec = new JsonCodec();
const original = { type: 'data' as const, stream: 3, payload: { rows: [1, 2, 3] }, end: true };
console.log('round trip:', framesEqual(original, codec.decode(codec.encode(original))));
```

## `MAX_FRAME_BYTES`

The largest encoded message a sender may emit: **65 536 bytes, exactly 64 KiB**.

It is a **protocol constant, not a setting**. It is never negotiated, never announced and never read
from configuration, and it is deliberately not configurable. A larger frame is not better: the cap
bounds how long one stream can monopolise a socket that has a single global message order, so raising
it would optimise the wrong direction — a 1 MB frame would stall every other stream behind it.

A receiver accepts anything up to it and may accept more; a sender always fragments at it regardless
of what the remote appears willing to accept.

The peer's constructor takes a `max_frame_bytes` / `maxFrameBytes` that defaults to this value. That
parameter exists for the conformance runner, which lowers it to exercise fragmentation without
megabyte fixtures. It is not a tuning knob.

### Signature

```python
MAX_FRAME_BYTES: Final[int] = 65_536
```

```ts
export const MAX_FRAME_BYTES = 65_536;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A constant, not a callable. |

### Return

Nothing — it is a constant `int` / `number`.

### Raises

Raises: nothing.

### Example

```python
from muxws import encoded_length, Frame, JsonCodec, MAX_FRAME_BYTES, split_frame

print(MAX_FRAME_BYTES, "bytes =", MAX_FRAME_BYTES // 1024, "KiB")

codec = JsonCodec()
big = Frame("data", stream=1, payload={"blob": "x" * 200_000}, end=True)
fragments = split_frame(big, MAX_FRAME_BYTES, codec)
print("fragments:", len(fragments))
print("largest:", max(encoded_length(codec.encode(part)) for part in fragments), "<=", MAX_FRAME_BYTES)
```

```ts
import { type Frame, JsonCodec, MAX_FRAME_BYTES, encodedLength, splitFrame } from 'muxws';

console.log(MAX_FRAME_BYTES, 'bytes =', MAX_FRAME_BYTES / 1024, 'KiB');

const codec = new JsonCodec();
const big: Frame = { type: 'data', stream: 1, payload: { blob: 'x'.repeat(200_000) }, end: true };
const fragments = splitFrame(big, MAX_FRAME_BYTES, codec);
console.log('fragments:', fragments.length);
const largest = Math.max(...fragments.map((part) => encodedLength(codec.encode(part))));
console.log('largest:', largest, '<=', MAX_FRAME_BYTES);
```

## `__version__` / `VERSION`

The version of the library you are holding, as a string. Python spells it `muxws.__version__`;
TypeScript spells it `VERSION`, and exports the same string from all three entry points — `muxws`,
`muxws/node` and `muxws/msgpack`.

The two packages ship one version stream: the number here and the `version` field of `package.json`
are asserted equal by the test suite, so a Python `0.1.0` and an npm `0.1.0` are the same release.

It is **not** the protocol version, and nothing about it reaches the wire. The only version on the
wire is the `v1` inside the `muxws.v1.<codec>` subprotocol, which changes only for a breaking wire
change - so the wire is already at its first generation while the package is still below 1.0. A 1.7.0
peer and a 0.1.0 peer speak `muxws.v1.json` to each other without either one knowing what the other's
package version is.

### Signature

```python
__version__ = "0.1.0"
```

```ts
export const VERSION = '0.1.0';
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A constant, not a callable. |

### Return

Nothing — it is a constant `str` / `string`.

### Raises

Raises: nothing.

### Example

```python
import muxws

from muxws.subprotocol import PREFIX

print(muxws.__version__)
print("three components:", len(muxws.__version__.split(".")) == 3)
# The package version is not the wire version: that one lives in the subprotocol and stays v1.
print("the wire says:", f"{PREFIX}json")
```

```ts
import { PREFIX, VERSION } from 'muxws';
import { VERSION as NODE_VERSION } from 'muxws/node';

console.log(VERSION);
console.log('one version stream:', VERSION === NODE_VERSION);
// The package version is not the wire version: that one lives in the subprotocol and stays v1.
console.log('the wire says:', `${PREFIX}json`);
```

## `encoded_length` / `encodedLength`

The byte length of an encoded message: UTF-8 bytes for text, buffer length for bytes.

This is the measurement every size limit in muxws is counted in — **bytes of encoded output**, not
characters. A JavaScript string's `.length` counts UTF-16 code units and disagrees with this on every
non-BMP character, which is why the measurement is spelled out rather than left to each language's
idea of "length".

### Signature

```python
def encoded_length(message: str | bytes) -> int: ...
```

```ts
export function encodedLength(message: string | ArrayBuffer): number;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str \| bytes` / `string \| ArrayBuffer` | required | An encoded message. A `str` / `string` is measured as UTF-8; bytes are measured as they are. |

### Return

`int` / `number`: the byte count.

### Raises

Raises: nothing.

### Example

```python
from muxws import encoded_length

print(encoded_length("abc"), encoded_length(b"abc"))
print("a non-BMP character:", len("\U0001f600"), "code point,", encoded_length("\U0001f600"), "bytes")
print("accented text:", len("café"), "code points,", encoded_length("café"), "bytes")
```

```ts
import { encodedLength } from 'muxws';

console.log(encodedLength('abc'), encodedLength(new Uint8Array([1, 2, 3]).buffer));
console.log('a non-BMP character:', '\u{1f600}'.length, 'code units,', encodedLength('\u{1f600}'), 'bytes');
console.log('accented text:', 'café'.length, 'code units,', encodedLength('café'), 'bytes');
```

## `split_frame` / `splitFrame`

Every fragment of a frame at once. Returns the frame unchanged, as a one-element list, when it
already fits under the cap.

Fragmentation is automatic and mandatory — the writer does this for you on every send, one slice at a
time so that a 1 MB export does not stall a 200-byte progress update behind it. `split_frame` is the
same computation exposed as a pure function, for tests, for the conformance corpus, and for anyone
who wants to see the boundaries.

It is a pure function of `(frame, cap, codec)`: it reads nothing else and mutates neither argument.
The sender encodes the logical payload with the codec, slices *that* encoded form, and puts each
slice into a frame the codec then encodes again — so both language ports cut in exactly the same
places. Text is cut on whole Unicode codepoints, never inside a multi-byte sequence.

`headers` ride the first fragment; `end` and `trailers` ride the last; `more` is true on every
fragment but the last.

### Signature

```python
def split_frame(frame: Frame, cap: int = MAX_FRAME_BYTES, codec: Codec | None = None) -> list[Frame]: ...
```

```ts
export function splitFrame(frame: Frame, cap: number = MAX_FRAME_BYTES, codec?: Codec): Frame[];
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `frame` | `Frame` | required | The frame to split. Not mutated. |
| `cap` | `int` / `number` | `MAX_FRAME_BYTES` (65 536) | The largest encoded frame to produce, in bytes. A smaller value comes only from a test or the conformance runner; it is never a value read off the wire. |
| `codec` | `Codec \| None` / `Codec \| undefined` | `None` / `undefined` | The codec whose output the boundaries are measured over. **Required in practice** despite the default: omitting it raises, because fragment boundaries are undefined without one. |

### Return

`list[Frame]` / `Frame[]`: either `[frame]` when it already fits, or two or more fragment frames
carrying `fragment` instead of `payload`. The last one always has `more` false, even when it carries
no bytes at all.

### Raises

`ProtocolError` when `codec` is omitted; when `cap` is below 2; when the frame exceeds the cap but
carries no payload to fragment (headers are never fragmented); when the cap cannot hold the envelope
plus one indivisible unit of payload; and when the closing fragment does not fit even carrying no
payload, because `trailers` ride it whole.

### Example

```python
from muxws import ABSENT, Frame, JsonCodec, ProtocolError, split_frame

codec = JsonCodec()

small = Frame("data", stream=1, payload={"n": 1}, end=True)
print("already fits:", len(split_frame(small, 1024, codec)))

big = Frame("data", stream=1, payload={"blob": "y" * 3000}, headers={"trace": "abc"}, end=True, trailers={"rows": 1})
parts = split_frame(big, 1024, codec)
print("fragments:", len(parts))
print("first carries headers:", parts[0].headers, "| more:", parts[0].more)
print("last carries trailers:", parts[-1].trailers, "| end:", parts[-1].end, "| more:", parts[-1].more)
print("no fragment carries a payload:", all(part.payload is ABSENT for part in parts))

try:
    split_frame(big, 1024)
except ProtocolError as exc:
    print("no codec:", exc)
```

```ts
import { type Frame, JsonCodec, ProtocolError, splitFrame } from 'muxws';

const codec = new JsonCodec();

const small: Frame = { type: 'data', stream: 1, payload: { n: 1 }, end: true };
console.log('already fits:', splitFrame(small, 1024, codec).length);

const big: Frame = {
  type: 'data',
  stream: 1,
  payload: { blob: 'y'.repeat(3000) },
  headers: { trace: 'abc' },
  end: true,
  trailers: { rows: 1 },
};
const parts = splitFrame(big, 1024, codec);
console.log('fragments:', parts.length);
console.log('first carries headers:', parts[0].headers, '| more:', parts[0].more);
const last = parts[parts.length - 1];
console.log('last carries trailers:', last.trailers, '| end:', last.end, '| more:', last.more);

try {
  splitFrame(big, 1024);
} catch (error) {
  if (error instanceof ProtocolError) console.log('no codec:', error.message);
}
```

## `Assembler`

The receive side of fragmentation: concatenates `fragment` values and decodes once the last one
lands. One assembler per stream, held by the stream itself — you rarely construct one, but the type
is public because a third implementation needs it and because `byte_length` / `byteLength` is what a
receiver's payload limit is checked against.

That check happens **as fragments arrive**, not after reassembly. A receiver that assembled a payload
in order to measure it has already spent what the limit was protecting.

### Signature

```python
class Assembler:
    def __init__(self) -> None: ...

    @property
    def in_progress(self) -> bool: ...

    @property
    def byte_length(self) -> int: ...

    def reset(self) -> None: ...

    def feed(self, frame: Frame, codec: Codec) -> Any: ...
```

```ts
export class Assembler {
  get inProgress(): boolean;
  get byteLength(): number;
  reset(): void;
  feed(frame: Frame, codec: Codec): unknown;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(constructor)* | — | — | Takes no arguments. |
| `in_progress` / `inProgress` | `bool` / `boolean` | `False` / `false` | True between the first fragment and the one that arrives without `more`. |
| `byte_length` / `byteLength` | `int` / `number` | `0` | Bytes accumulated so far, measured the way `encoded_length` measures them. |
| `reset()` | `() -> None` / `() => void` | — | Drops the partial buffer and releases the bytes with it. Called when the stream is reset. |
| `feed(frame, codec)` | `frame: Frame`, `codec: Codec` | both required | Absorbs one fragment frame. `codec` is used only on the closing fragment, to decode the concatenation. |

### Return

`feed()` returns `ABSENT` while more fragments are expected, and the decoded payload on the frame
that closes the sequence. `reset()` returns `None` / `void`.

### Raises

`ProtocolError` from `feed()` when the frame carries no `fragment`, and whatever the codec's
`decode_payload` / `decodePayload` raises — `ProtocolError` for both shipped codecs — when the
reassembled bytes do not decode.

### Example

```python
from muxws import ABSENT, Assembler, Frame, JsonCodec, split_frame

codec = JsonCodec()
parts = split_frame(Frame("data", stream=1, payload={"blob": "z" * 3000}, end=True), 1024, codec)

assembler = Assembler()
print("before:", assembler.in_progress, assembler.byte_length)
for part in parts:
    result = assembler.feed(part, codec)
    print("fed a fragment ->", "ABSENT" if result is ABSENT else type(result).__name__, "|", assembler.byte_length)
print("payload length:", len(result["blob"]), "| after:", assembler.in_progress, assembler.byte_length)
```

```ts
import { ABSENT, Assembler, type Frame, JsonCodec, splitFrame } from 'muxws';

const codec = new JsonCodec();
const source: Frame = { type: 'data', stream: 1, payload: { blob: 'z'.repeat(3000) }, end: true };
const parts = splitFrame(source, 1024, codec);

const assembler = new Assembler();
console.log('before:', assembler.inProgress, assembler.byteLength);
let result: unknown = ABSENT;
parts.forEach((part) => {
  result = assembler.feed(part, codec);
  console.log('fed a fragment ->', result === ABSENT ? 'ABSENT' : typeof result, '|', assembler.byteLength);
});
console.log('payload length:', (result as { blob: string }).blob.length);
console.log('after:', assembler.inProgress, assembler.byteLength);
```

## `CloseReason`

Why a socket ended. One type per language, used for every socket loss, and handed to every `on_close`
/ `onClose` handler. Four fields, the same four in both languages.

Python's is a frozen slotted dataclass; TypeScript's is an interface.

### Signature

```python
@dataclass(frozen=True, slots=True)
class CloseReason:
    code: int
    reason: str
    was_clean: bool
    will_retry: bool = False
```

```ts
export interface CloseReason {
  readonly code: number;
  readonly reason: string;
  readonly wasClean: boolean;
  readonly willRetry: boolean;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `int` / `number` | required | The WebSocket close code. `1000` for a clean close, `1006` when the socket died without a close frame, `1008` for a policy violation such as a subprotocol mismatch. |
| `reason` | `str` / `string` | required | The close frame's reason text, or `""` when there was none. |
| `was_clean` / `wasClean` | `bool` / `boolean` | required | Whether a close handshake actually completed. |
| `will_retry` / `willRetry` | `bool` / `boolean` | `False` in Python; **required** in TypeScript | Whether the reconnect helper intends to dial again. False when the attempt budget is exhausted, when `close()` was called deliberately, and on any peer with no reconnect helper at all. A handler that tears down application state should look at this: a close with `will_retry` true is an interruption, not an ending. |

The default on `will_retry` is the one shape difference between the two: Python's dataclass gives it
one, TypeScript's interface has no defaults to give.

### Return

Python: a frozen `CloseReason`. TypeScript: an interface, so an object literal is one.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import CloseReason, JsonCodec, Peer
from muxws.transports.memory import memory_pair

print(CloseReason(code=1000, reason="bye", was_clean=True))
print(CloseReason(1006, "", False).will_retry)


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @dialer.on_close
    def closed(reason: CloseReason) -> None:
        print("closed:", reason.code, repr(reason.reason), reason.was_clean, reason.will_retry)

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    await asyncio.sleep(0.05)
    await left.drop()
    await asyncio.sleep(0.05)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { type CloseReason, JsonCodec, Peer, memoryPair } from 'muxws';

const literal: CloseReason = { code: 1000, reason: 'bye', wasClean: true, willRetry: false };
console.log(literal);

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });

  dialer.onClose((reason: CloseReason) => {
    console.log('closed:', reason.code, JSON.stringify(reason.reason), reason.wasClean, reason.willRetry);
  });

  void dialer.serve();
  void acceptor.serve();
  await new Promise((resolve) => setTimeout(resolve, 50));
  left.drop();
  await new Promise((resolve) => setTimeout(resolve, 50));
}

void main();
```

## `StreamHandler`

The type of the one incoming-stream handler per peer — what `peer.on_stream(...)` /
`peer.onStream(...)` takes, and what `connect(on_stream=...)` and `serve(handler=...)` take.

It is called once per stream the **remote** opens, with the opening payload and the `Stream` itself.
It may be a coroutine / return a promise, and usually is. Its return value is discarded: a unary
handler answers by calling `stream.reply(...)`, not by returning.

Symmetry is the point. An acceptor's handler receives a client's request; a dialer's handler receives
a server push. Same type, same signature, same semantics.

A handler that raises produces `reset(APPLICATION_ERROR)` on that stream, with the reason text and
the `error_serializer`'s payload — never `REFUSED`, which is reserved for streams that were
definitively not processed. Only one handler is registered per peer; registering a second replaces
the first and logs a warning.

### Signature

```python
StreamHandler = Callable[[Any, Stream], Any]
```

```ts
export type StreamHandler = (payload: any, stream: Stream) => void | Promise<void>;
```

### Parameters

The parameters of the callable itself:

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` / `any` | none — positional | The opening payload, already decoded and reassembled. `None` / `null` when the remote opened with none. |
| `stream` | `Stream` | none — positional | The stream. Reply on it, iterate it, send on it, or reset it. Its `headers` are the opening frame's. |

### Return

Python's alias returns `Any` and the value is ignored; a coroutine function is the normal case.
TypeScript's returns `void | Promise<void>`. Returning a value is not how a handler answers — that is
what `stream.reply(...)` is for.

### Raises

The type raises nothing. An implementation that raises causes `reset(APPLICATION_ERROR)` on its
stream; the connection survives.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, Stream, StreamHandler
from muxws.transports.memory import memory_pair


async def handle(payload, stream: Stream) -> None:
    """One handler covering two call shapes, which is the usual arrangement."""
    if (payload or {}).get("action") == "stream":
        for index in range(3):
            await stream.send({"chunk": index})
        await stream.end({"chunk": "done"})
    else:
        await stream.reply({"echo": payload})


async def main() -> None:
    registered: StreamHandler = handle
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)
    acceptor.on_stream(registered)

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    print(await dialer.request({"n": 1}, timeout=2.0))
    async for chunk in dialer.open({"action": "stream"}):
        print(chunk)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, type Stream, type StreamHandler, memoryPair } from 'muxws';

/** One handler covering two call shapes, which is the usual arrangement. */
const handle: StreamHandler = async (payload: { action?: string } | null, stream: Stream) => {
  if (payload?.action === 'stream') {
    for (const index of [0, 1, 2]) await stream.send({ chunk: index });
    await stream.end({ payload: { chunk: 'done' } });
  } else {
    await stream.reply({ echo: payload });
  }
};

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(handle);

  void dialer.serve();
  void acceptor.serve();
  console.log(await dialer.request({ n: 1 }, { timeoutMs: 2000 }));
  for await (const chunk of dialer.open({ action: 'stream' })) console.log(chunk);

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `ErrorSerializer`

The type of the hook that turns a handler's failure into the `payload` of the
`reset(APPLICATION_ERROR)` frame reporting it. Passed as `error_serializer=` / `errorSerializer:` to
`connect()` and `accept()`, and defaulting to `default_error_serializer` /
`defaultErrorSerializer`.

It is chosen **per peer**, which is the whole reason it is a parameter rather than a global: one
process can redact on its browser-facing connection and not on its internal one.

::: warning
Whatever this returns goes on the wire. The default puts the exception's message text there, and
exception messages routinely contain file paths, SQL and identifiers the remote has no business
seeing. A public-facing deployment should pass a redacting implementation.
:::

Its return value must be representable by the configured codec — under JSON that means no `bytes`.

### Signature

```python
ErrorSerializer = Callable[[BaseException], Any]
```

```ts
export type ErrorSerializer = (error: unknown) => unknown;
```

### Parameters

The parameters of the callable itself:

| Name | Type | Default | What it does |
|---|---|---|---|
| `exc` / `error` | `BaseException` / `unknown` | none — positional | The exception the handler raised. Python types it `BaseException` rather than `Exception`; TypeScript types it `unknown` because JavaScript permits throwing anything. |

### Return

`Any` / `unknown`: the value that becomes the reset frame's `payload`, and that arrives at the remote
as `RemoteError.payload`. Return `None` / `null` to send no structured payload at all — the `reason`
text still goes.

### Raises

The type raises nothing. An implementation that raises is a bug in the implementation; keep it total.

### Example

```python
import asyncio

from muxws import ErrorSerializer, JsonCodec, Peer, RemoteError
from muxws.transports.memory import memory_pair


def redacting(exc: BaseException) -> dict[str, str]:
    """Class name only. The message never leaves the process."""
    return {"type": type(exc).__name__}


async def main() -> None:
    serializer: ErrorSerializer = redacting
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False, error_serializer=serializer)

    @acceptor.on_stream
    async def handle(payload, stream):
        raise ValueError("SELECT * FROM accounts WHERE id = 7")

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    try:
        await dialer.request({"n": 1}, timeout=2.0)
    except RemoteError as exc:
        print("payload:", exc.payload, "| reason:", exc)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { type ErrorSerializer, JsonCodec, Peer, RemoteError, memoryPair } from 'muxws';

/** Class name only. The message never leaves the process. */
const redacting: ErrorSerializer = (error: unknown) => ({
  type: error instanceof Error ? error.name : typeof error,
});

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false, errorSerializer: redacting });

  acceptor.onStream(() => {
    throw new Error('SELECT * FROM accounts WHERE id = 7');
  });
  void dialer.serve();
  void acceptor.serve();

  try {
    await dialer.request({ n: 1 }, { timeoutMs: 2000 });
  } catch (error) {
    if (error instanceof RemoteError) console.log('payload:', error.payload, '| reason:', error.message);
  }

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `logger` and `LogLevel` (TypeScript)

TypeScript's stand-in for the logging module Python gets from its standard library.

There is no Python counterpart and there should not be: Python names a logger
(`logging.getLogger("muxws.frames")`) and the application configures it. The browser entry point may
not depend on anything (WSM-PKG-003), so the TypeScript port carries a four-method shim over
`console` instead, and `logger.level` is the only way to turn frame logging on.

It starts at `'warn'`, which is what an unconfigured Python logger does — `logger.info` and
`logger.debug` print nothing until an application asks for them. A port that spammed every frame to
the console by default would not be mirroring Python, it would be shouting.

Frame lines are emitted at `'debug'` and never carry payload **contents** at any level (WSM-OBS-002).

### Signature

```ts
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

export const logger: {
  level: LogLevel;
  isEnabledFor(level: LogLevel): boolean;
  debug(message: string, ...rest: unknown[]): void;
  info(message: string, ...rest: unknown[]): void;
  warn(message: string, ...rest: unknown[]): void;
  error(message: string, ...rest: unknown[]): void;
};
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `level` | `LogLevel` | `'warn'` | The threshold. Assign to it to change what prints; `'silent'` stops everything. |
| `message` | `string` | required | The line, on each of the four emitters. |
| `...rest` | `unknown[]` | `[]` | Extra values passed through to the matching `console` method. |

### Return

`logger.isEnabledFor` returns a `boolean`; the four emitters return `void`. `logger` itself is a
mutable object, not a factory — there is one, and assigning `level` reconfigures it process-wide.

### Raises

Raises: nothing. An unknown `level` string is a type error at compile time and is treated as the
highest threshold at runtime, so the failure is silence rather than an exception.

### Example

```ts
import { logger } from 'muxws';

console.log('default level:', logger.level);
console.log('debug enabled by default:', logger.isEnabledFor('debug'));

logger.level = 'debug';
console.log('debug enabled now:', logger.isEnabledFor('debug'));

logger.level = 'silent';
console.log('silent stops even errors:', logger.isEnabledFor('error'));

logger.level = 'warn';
```

## `FrameDirection` (TypeScript)

Which way a frame was travelling, as handed to `peer.onFrame` (WSM-OBS-003).

`'tx'` is reported **before** the codec encodes, `'rx'` **after** it decodes — so a handler always
sees a logical `Frame`, never bytes, in both directions. Python's `on_frame` takes the same two
strings and needs no type alias for them.

### Signature

```ts
export type FrameDirection = 'tx' | 'rx';
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A string-literal union, not a callable. |

### Return

Nothing — it is a type. It exists at compile time only and is absent from the emitted JavaScript.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, type FrameDirection, MemorySocket, Peer, memoryPair } from 'muxws';

const [dialerSide, acceptorSide] = memoryPair();
const seen: FrameDirection[] = [];

const acceptor = new Peer(acceptorSide, { codec: new JsonCodec(), isDialer: false });
acceptor.onStream(() => undefined);
void acceptor.serve().catch(() => undefined);

const dialer = new Peer(dialerSide, { codec: new JsonCodec(), isDialer: true });
dialer.onFrame((direction: FrameDirection) => seen.push(direction));
void dialer.serve().catch(() => undefined);

dialer.open({ hello: true }, { end: true });
await new Promise((resolve) => setTimeout(resolve, 50));

console.log('directions observed:', [...new Set(seen)].sort());
await dialer.close();
await acceptor.close();
```

## `PREFIX`

The subprotocol prefix, `muxws.v1.`. A dialer offers `` `${PREFIX}${codecName}` `` as its **first**
subprotocol entry (WSM-CDC-020), and the acceptor accepts only that exact value.

The `v1` in it is the **only version anywhere on the wire** (WSM-CON-009): it pins the
breaking-change generation, not the package's semver. muxws 2.0.0 could still speak `muxws.v1.`, and
a peer offering `muxws.v2.json` is rejected at the handshake by a v1 acceptor (WSM-CDC-025).

Exported because a consumer building a subprotocol list by hand needs the constant. The machinery
that builds and reads the offer is not exported — that is the library's own business.

TypeScript exports it from the package root; in Python, import it from `muxws.subprotocol`.

### Signature

```python
PREFIX: Final[str] = "muxws.v1."
```

```ts
export const PREFIX: string;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A constant string, not a callable. |

### Return

Nothing — it is a constant.

### Raises

Raises: nothing.

### Example

```python
from muxws.subprotocol import PREFIX

print(PREFIX)
print("what a json dialer offers first:", f"{PREFIX}json")
print("the only version on the wire is the generation:", PREFIX.split(".")[1])
```

```ts
import { PREFIX } from 'muxws';

console.log(PREFIX);
console.log('what a json dialer offers first:', `${PREFIX}json`);
console.log('the only version on the wire is the generation:', PREFIX.split('.')[1]);
```

## See also

- [Codec](./codec.md) — what turns a `Frame` into bytes, and `Codec.encode_payload`, whose output
  `split_frame` slices.
- [Errors](./errors.md) — `default_error_serializer`, the `ErrorSerializer` you get if you pass none.
- [Peer](./peer.md) — `on_stream`, `on_close` and `on_frame`, the three hooks these types describe.
- [Stream](./stream.md) — what a `StreamHandler` is handed.
