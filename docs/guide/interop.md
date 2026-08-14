# Interop

muxws is a protocol before it is two libraries. This page is for anyone who has to talk to a muxws
peer without using one of the reference implementations — a third port, a proxy, a test harness, or a
service in a language neither reference covers.

## The JSON wire form is frozen at 1.0

What "frozen" means here: the twelve envelope fields, the six frame types, the defaults, the reset
codes and the fragmentation rules will not change under the name `muxws.v1.json`. A peer written
against the 1.0 wire will keep interoperating with every 1.x release of both reference ports.

The frame set is six, and no others:

`open` · `data` · `reset` · `ping` · `pong` · `goaway`

The envelope is twelve fields, and no others:

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

Field names are spelled out rather than abbreviated, and stay `snake_case` in **every** language —
including the TypeScript port, whose own API is camelCase. The wire is one thing; the bindings are
another.

Four properties of the encoding that a third implementation has to match:

**A field at its default is omitted.** Both reference ports emit only the fields carrying a
non-default value, which is why the pinned wire in the corpus is short. A receiver applies the
defaults either way, so emitting them explicitly is legal — but you will not match the pinned bytes.

**Key order is `type`, then `stream`, then the rest alphabetically.** This is what makes a
byte-for-byte fixture possible at all.

**An absent `payload` key and an explicit `"payload": null` are different frames**, and the corpus
carries both. A decoder has to keep them apart — the reference ports use an `ABSENT` sentinel — even
though whether an application cares about the difference is its own business.

**`payload` and `fragment` must never both appear on one frame.** A frame carrying both is a
connection-level protocol error.

There are two things that are conspicuously *not* on the wire, and their absence is load-bearing:

- **No limit, in any form.** No `settings` frame, no capability list, no `max_payload_bytes`, no
  `max_concurrent_streams`, no `max_frame_bytes`. The frame cap is a protocol constant; the other two
  are each one receiver's private defence, and a sender learns of them only from the reset it
  provokes. See [Sizes and fragmentation](/guide/sizes-and-fragmentation).
- **No vocabulary inside `payload`.** No `kind`, no reserved key, no discriminator of any sort. The
  payload is the application's, entirely.

## The subprotocol prefix is the only version

```
muxws.v1.json
muxws.v1.msgpack
```

That is it. There is no version field in any frame, no version handshake, no capability exchange and
no `protocol_version` anywhere. The `v1` in the subprotocol name is the whole of the versioning story,
and it does two jobs.

**It pins the breaking-change generation.** A change that requires the remote to *act* on something
new — a new frame type it must respond to, a field it must honour, a different meaning for an existing
one — is a new generation: `muxws.v2.<codec>`. A v1 acceptor refuses that offer at the handshake, with
HTTP 400, before a socket exists. Two peers of different generations do not connect and cannot
half-connect.

**Additive revisions are announced nowhere, because they need no announcement.** A receiver is already
required to ignore what it does not recognise, on both axes:

- **Unknown frame types** are ignored and logged, and are never an error of any kind. The connection
  continues.
- **Unknown envelope fields** are ignored. A decoder must *drop* them rather than preserve them —
  round-tripping an unknown field would make `decode(encode(frame)) == frame` pass on garbage.

So an additive extension — a new field on `data`, a new frame type that is safe to miss — is deployed
by simply sending it. Peers that understand it act on it; peers that do not carry on. There is nothing
to negotiate, and therefore nothing that can be negotiated wrongly.

The corollary is the rule that keeps this honest: **tolerate, never advertise.** There is no mechanism
for a peer to announce what it supports, deliberately, because the moment such a mechanism exists it
becomes something both ends must agree on and keep agreeing on forever. `window_update` is reserved by
name and unimplemented; a v1 peer must not send it.

One name is retired rather than reserved: **reset code 5**. It was `STREAM_LIMIT`, for rejection
against an announced concurrency limit; there is no announced limit, so there is nothing to reject
against. The number must never be sent, and must never be reused. A peer receiving it treats it as it
treats any unknown code — reset that stream, keep the connection. See [Errors](/guide/errors).

## Writing a third implementation

The corpus under `conformance/` is the contract. It is plain JSON, read verbatim by both reference
suites — nothing in it is generated at test time and nothing in it is language-specific. A rule only
one port checks is a rule that drifts, and a wire disagreement between ports is invisible until two
peers of different languages meet, at which point it looks like corruption rather than like a bug.

**`conformance/README.md` is the schema.** It is written so that two runners implemented
independently from it cannot end up reading the same file differently. Read it before the fixtures.

### What is in there

| Path | What it holds |
|---|---|
| `conformance/frames/v1-frames.json` | Frame **triples**: a name, a logical frame, and the exact JSON bytes it must encode to. The round-trip corpus. |
| `conformance/frames/v1-fragment-boundaries.json` | Fragment **boundary cases**: payloads and caps, with where the cuts must fall. |
| `conformance/sequences/*.json` | Scripted **exchanges** between two peers: what goes in, what must come out, in order. |
| `conformance/invalid/*.json` | **Misbehaviour** cases: a bad frame in, and what the peer must do about it — including whether the connection survives. |

The two runners of record are `muxws/conformance_test.py` and `ts/conformance.spec.ts`. They are
twins, and deliberately cross-checked: the Python file reads the TypeScript one's fixture counts and
the TypeScript file reads the Python one's frozen digest, so neither suite can drift alone. If you are
building a third runner, those two are the worked examples.

### Rules that hold for every fixture

These are constraints on the corpus rather than house style, and a third implementation has to
satisfy every one of them before it adds a case of its own:

- **No floats.** The two reference ports render them differently — Python writes `1.0`, `-0.0`,
  `1e+16`, `1e-07` where JavaScript writes `1`, `0`, `10000000000000000`, `1e-7`. Fragment boundaries
  are cut over the encoded form, so a payload carrying a float is cut in different places by the two
  ports. Integers are safe only within ±(2^53 − 1). `NaN` and `Infinity` are not JSON, and the codec
  refuses them rather than emitting tokens no other parser accepts.
- **No bytes.** JSON has no byte type, and muxws does not base64-encode bytes on the application's
  behalf. Bytes are a payload type only under a binary codec, and no msgpack byte string may be pinned
  in a fixture at all: two msgpack libraries make different but equally valid choices about integer
  width and map format, so pinned bytes would fail a legal encoder. Round-trip is the only assertion
  there.
- **No limit and no version appears in any frame** — not even inside a `description` or a `reason`
  string. A schema test greps the corpus text for those words.
- **Reset code 5 never appears**, and neither does code 9: `CONNECTION_CLOSED` is synthesised locally
  when a socket dies and must never be put on the wire.
- **Every duration is milliseconds, and its key ends in `_ms`.** The corpus is read by a language whose
  durations are milliseconds and by one whose durations are seconds; the runner converts, and the
  fixture does not carry both.
- **A one-object fixture's `name` equals its file basename**, asserted by every runner, so a renamed
  file cannot silently become a second copy of a case already covered.

### The order to build in

1. **The envelope.** Encode and decode the twelve fields, omitting defaults, ordering keys, and
   keeping absent-`payload` apart from `null`-`payload`. `frames/v1-frames.json` passes or it does
   not, byte for byte.
2. **Fragmentation.** Slice over the *encoded* form of the payload, on boundaries the codec can
   represent — for text, whole codepoints, so a surrogate pair is never split. Budget for the
   envelope, then verify the result fits and re-split if it does not; the budget is a hint, the check
   is the guarantee. `frames/v1-fragment-boundaries.json` is where an off-by-one shows up.
3. **The state machine and the id rules.** Dialer ids are odd, acceptor ids are even, each peer's own
   opens are strictly increasing. A frame for an id below your high-water mark is silently ignored;
   one above it kills the connection. `sequences/` exercises this.
4. **Misbehaviour.** `invalid/` is the set that says what must *not* happen: which failures reset one
   stream and which end the connection. The column that carries the meaning is whether the connection
   survives.

### If the schema and a runner disagree

One of them is a defect. Say which; do not quietly pick.

## See also

- [`api/types`](/api/types) — `Frame` and every envelope field, `MAX_FRAME_BYTES`
- [`api/codec`](/api/codec) — the `Codec` port a third wire format plugs into
- [`api/errors`](/api/errors) — `ResetCode` and all nine members
- [`api/accept`](/api/accept) — `select_subprotocol`, which enforces the generation at the handshake
