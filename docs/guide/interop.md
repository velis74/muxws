# Interop

muxws is defined by its wire protocol; the Python and TypeScript libraries are two implementations of
it. This page is for anyone who communicates with a muxws peer without using one of these reference
implementations: a further implementation, a proxy, a test harness, or a service in another language.

## The JSON wire form is frozen

The thirteen envelope fields, the six frame types, the defaults, the reset codes and the fragmentation
rules do not change under the subprotocol name `muxws.v1.json`. A peer implemented against this wire
form interoperates with every release of both reference implementations that offers `muxws.v1.json`.

There are six frame types:

`open` · `data` · `reset` · `ping` · `pong` · `goaway`

The envelope has thirteen fields:

| Field | Type | Default |
|---|---|---|
| `type` | string | required |
| `stream` | integer | absent |
| `payload` | anything | absent — and absent is not `null`, see below |
| `fragment` | string or bytes | absent |
| `more` | boolean | `false` |
| `headers` | object | absent — on `open`, and on the first `data` a peer sends |
| `end` | boolean | `false` |
| `trailers` | object | absent |
| `code` | integer | absent |
| `reason` | string | absent |
| `nonce` | string | absent |
| `last_stream` | integer | absent |
| `oneway` | boolean | `false` — on `open`: the receiver sends no frame on the stream |

Field names are not abbreviated and are `snake_case` in **every** language, including the TypeScript
implementation, whose own API is camelCase.

Five properties of the encoding apply to every implementation:

**A field with its default value is omitted.** Both reference implementations emit only fields with
non-default values, and the pinned wire strings in the corpus are written that way. A receiver applies
the defaults either way, so emitting default values explicitly is legal, but the encoded frame then no
longer parses to the same object as the pinned wire string.

**The canonical key order is `type`, then `stream`, then the remaining keys alphabetically.** The
reference implementations emit this order so that logged frames can be compared line by line. The
conformance corpus compares parsed objects, never bytes, so a receiver must accept any key order.

**An absent `payload` key and an explicit `"payload": null` are different frames**, and the corpus
contains both. A decoder must distinguish them (the reference implementations use an `ABSENT`
sentinel), whether or not the application uses the difference.

**`payload` and `fragment` must never both appear on one frame.** A frame carrying both is a
connection-level protocol error. So is an envelope field whose value has the wrong type, such as a
string `stream` or a non-boolean `end`; the accepted types are listed under
[`from_mapping`](/api/types#from-mapping-frommapping).

**`oneway` needs no support from the receiver.** A receiver ignores envelope fields it does not know
(WSM-FRM-001), so one that does not implement `oneway` answers the stream as an ordinary
`open(end)`. The opener closed the stream when it sent the `open` and ignores that answer
(WSM-STM-041).

Two kinds of information are never sent:

- **Limits.** There is no `settings` frame, no capability list, and no `max_payload_bytes`,
  `max_concurrent_streams` or `max_frame_bytes` field. The frame cap is a protocol constant; the other
  two are local limits of each receiver, and a sender learns of them only from the reset it receives
  when it exceeds one. See [Sizes and fragmentation](/guide/sizes-and-fragmentation).
- **Payload vocabulary.** muxws reserves no key inside `payload`: no `kind`, no discriminator. The
  payload belongs entirely to the application.

## The subprotocol prefix is the only version

```text
muxws.v1.json
muxws.v1.msgpack
```

No frame carries a version field, and there is no version handshake, capability exchange or
`protocol_version`. The `v1` in the subprotocol name is the only version identifier, and it has two
functions.

**It identifies the breaking-change generation.** A change that requires the remote to *act* on
something new (a new frame type it must respond to, a field it must honour, a different meaning for an
existing field) is a new generation: `muxws.v2.<codec>`. A v1 acceptor refuses that subprotocol at the
handshake with HTTP 400, before a socket exists. Peers of different generations do not connect.

**Additive revisions need no announcement.** A receiver must ignore what it does not recognise:

- **Unknown frame types** are ignored and are never an error. The connection continues. Each type is
  logged once per peer, at info level, for at most 32 distinct types.
- **Unknown envelope fields** are ignored. A decoder must *drop* them rather than preserve them.

An additive extension, such as a new field on `data` or a new frame type that receivers may ignore, is
deployed by sending it. Peers that support it act on it; other peers ignore it. Nothing is negotiated.

A peer has no mechanism to announce what it supports. `window_update` is reserved as a frame name and
is not implemented; a v1 peer must not send it.

**A reset code the receiver does not define** resets that stream and keeps the connection. See
[Errors](/guide/errors).

## Writing a further implementation

The corpus under `conformance/` is the conformance contract. It is plain JSON, read verbatim by both
reference test suites; nothing in it is generated at test time and nothing in it is language-specific.
A rule that only one implementation tests can diverge between implementations, and a wire
disagreement between implementations shows up only when peers in different languages connect, where
it looks like data corruption.

**`conformance/README.md` is the schema.** It specifies the fixture format precisely enough that two
independently written runners read every file the same way. Read it before the fixtures.

### Contents

| Path | What it holds |
|---|---|
| `conformance/frames/v1-frames.json` | Frame **triples**: a name, a logical frame, and the JSON wire string it must encode to. The round-trip corpus. |
| `conformance/frames/v1-fragment-boundaries.json` | Fragment **boundary cases**: payloads and caps, with the exact cut points. |
| `conformance/sequences/*.json` | Scripted **exchanges** between two peers: the calls made, and the frames and results expected, in order. |
| `conformance/invalid/*.json` | **Misbehaviour** cases: an invalid message in, the peer's required response out, and whether the connection survives. |

The two reference runners are `muxws/conformance_test.py` and `ts/conformance.spec.ts`. They check each
other: the Python file reads the TypeScript file's fixture count and the TypeScript file reads the
Python file's frozen digest, so a change to one suite alone fails the other. They are the reference
examples for a new runner.

### Rules for every fixture

These are constraints on the corpus, and every fixture added by a further implementation must satisfy
them:

- **No floats.** The two reference implementations render floats differently: Python writes `1.0`,
  `-0.0`, `1e+16`, `1e-07` where JavaScript writes `1`, `0`, `10000000000000000`, `1e-7`. Fragment
  boundaries are cut over the encoded form, so the two implementations cut a payload containing a
  float at different places. Integers are safe only within ±(2^53 − 1). `NaN` and `Infinity` are not
  JSON, and the codec rejects them.
- **No bytes.** JSON has no byte type, and muxws does not base64-encode bytes on the application's
  behalf. Bytes are a payload type only under a binary codec, and no msgpack byte string may be pinned
  in a fixture: msgpack libraries make different valid choices of integer width and map format, so a
  pinned byte string would fail a legal encoder. msgpack fixtures assert only the round trip.
- **No limit and no version appears in any frame**, including inside a `description` or a `reason`
  string. A schema test searches the corpus text for those names.
- **Every reset code is one this generation defines, and none is 9**: `CONNECTION_CLOSED` is
  generated locally when a socket closes and must never be sent.
- **Every duration is in milliseconds, and its key ends in `_ms`.** The corpus is read by one language
  whose durations are milliseconds and one whose durations are seconds; the runner converts.
- **A fixture that is one object per file has a `name` equal to its file basename**, asserted by every
  runner, so a renamed copy of a file is detected as a duplicate case.

### Implementation order

1. **The envelope.** Encode and decode the thirteen fields, omitting defaults and keeping an absent
   `payload` distinct from a `null` `payload`. `frames/v1-frames.json` tests this.
2. **Fragmentation.** Slice the *encoded* form of the payload at boundaries the codec can represent;
   for text, at whole codepoints, so a surrogate pair is never split. Reserve space for the envelope,
   then encode each fragment frame, check that it fits the cap, and re-split it if it does not. The
   reservation is an estimate; the check guarantees the cap. `frames/v1-fragment-boundaries.json`
   detects off-by-one errors in the cut points.
3. **The state machine and the id rules.** Dialer ids are odd, acceptor ids are even, and each peer's
   own opens are strictly increasing. A frame for an id below the high-water mark is silently ignored;
   a frame for an id above it is a connection error. `sequences/` tests this.
4. **Misbehaviour.** `invalid/` specifies which failures reset one stream and which end the connection,
   in the `connection_survives` column.

## See also

- [`api/types`](/api/types) — `Frame` and every envelope field, `MAX_FRAME_BYTES`
- [`api/codec`](/api/codec) — the `Codec` interface a further wire format implements
- [`api/errors`](/api/errors) — `ResetCode` and all nine members
- [`api/accept`](/api/accept) — `select_subprotocol`, which enforces the generation at the handshake
