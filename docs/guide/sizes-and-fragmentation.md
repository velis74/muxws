# Sizes and fragmentation

Every size in muxws is a **byte count of the fully encoded WebSocket message**, envelope included, as
sent on the socket. It is not the size of the payload before encoding, not the number of characters
in a string, and not the size of the `fragment` field alone.

## Bytes of encoded output, not characters

Both ports measure sizes only with `muxws.encoded_length` / `encodedLength`: the UTF-8 byte length for
text, the buffer length for bytes.

```python
from muxws import encoded_length

encoded_length("hello")  # 5
encoded_length(b"hello")  # 5
```

The *length* of a string differs between the two languages, and neither is the byte count.

| | `"🎉"` (U+1F389) |
|---|---|
| Python `len(s)` | 1 — codepoints |
| JavaScript `s.length` | 2 — UTF-16 code units |
| `encoded_length(s)` / `encodedLength(s)` | **4** — UTF-8 bytes |

The three values differ for every character outside the Basic Multilingual Plane. The same string in
a frame, with the JSON codec:

```python
from muxws import Frame, JsonCodec, MAX_FRAME_BYTES, encoded_length, split_frame

codec = JsonCodec()
party = "🎉" * 20_000

print(len(party))                                    # 20000  - Python codepoints
print(encoded_length(party))                         # 80000  - UTF-8 bytes
print(encoded_length(codec.encode_payload(party)))   # 80002  - plus the two JSON quotes
frame = Frame("data", stream=1, payload=party)
print(encoded_length(codec.encode(frame)))           # 80039  - plus the envelope
print(len(split_frame(frame, MAX_FRAME_BYTES, codec)))  # 2     - so it is fragmented
print(MAX_FRAME_BYTES)                               # 65536
```

In the browser, the same value has `party.length === 40000`: 20,000 codepoints, 40,000 UTF-16 code
units, 80,000 UTF-8 bytes, and 80,039 bytes on the wire. Only the last number is the same on both
ends of the connection, so limits are expressed in it.

As a result:

- A payload within a limit by character count can be up to four times over the same limit in bytes.
- Fragment boundaries are cut over the **encoded** form, on a boundary the codec can represent: for
  text, whole Unicode codepoints, so a surrogate pair is never split. Both ports cut at the same
  positions because both run the same search over the same encoded bytes.

## `MAX_FRAME_BYTES` is 64 KiB, and it is not configurable

```python
from muxws import MAX_FRAME_BYTES

MAX_FRAME_BYTES  # 65536
```

It is a protocol constant. It is not negotiated, not announced, not read from configuration, and not
derived from any value the remote sends. A sender always fragments at it; a receiver accepts any frame
up to it, and may accept larger frames.

**The constant bounds interleaving latency.** One muxws connection is one WebSocket, and a WebSocket
sends messages in a single order. A frame being written occupies the socket until it is complete. The
limit is therefore not a memory limit but a *bound on how long one stream can occupy the socket before
another stream's frame is sent*. At 64 KiB, a stream sending 1 MiB releases the socket sixteen times,
and a 200-byte progress update on another stream waits for at most one 64 KiB write. With a 1 MiB
limit, the same update would wait for the whole 1 MiB.

A larger frame size reduces envelope overhead slightly and increases the latency of other streams.
There is no option to raise the constant; changing it is a protocol change, not a configuration
change.

::: warning The `max_frame_bytes` argument is for testing, not configuration
`connect()`, `accept()` and `Peer()` take a `max_frame_bytes` argument (`maxFrameBytes` in
TypeScript), defaulting to `MAX_FRAME_BYTES`. It lets the conformance runner *lower* the limit to test
fragmentation without megabyte fixtures. Do not use it for tuning: raising it produces frames a
conforming remote may reject, and a value too small for one envelope plus one indivisible unit of
payload raises `ProtocolError` at construction.
:::

## Fragmentation is automatic and mandatory

The application does not call the splitter. The writer replaces any frame whose encoded form exceeds
the limit with a sequence of fragment frames before sending it.

The sequence:

- `headers` are carried on the **first** fragment only, and are never split.
- `end` and `trailers` are carried on the **last** fragment only, and are never split.
- `more: true` is set on every fragment except the last. The sequence always ends with a fragment
  carrying `more: false`, even when that fragment carries no payload bytes.
- The receiver concatenates the `fragment` values and passes the result to the codec when the last
  fragment arrives. The payload is decoded only then.

Two rules allow frames of different streams to interleave:

**A stream holds at most one unsent fragment.** Fragment *n+1* is sliced only after fragment *n* has
been passed to the socket. Slicing in advance would fix the order of queued frames before the writer
selects them.

**The writer selects across streams in round-robin order, not in arrival order.** Each lane is served
once before any lane is served twice. Connection-level frames (`ping`, `pong`, `goaway`) have no stream
and are served in the same rotation, at the same priority as stream lanes.

As a result, a 1 MB export does not delay a 200-byte progress update by more than one fragment: the
export sends one fragment per turn, and the update sends its whole frame on its turn.

## `PAYLOAD_TOO_LARGE`, from both sides

Reset code `7`. Two checks produce it:

**A single encoded message over the frame limit.** The receiver measures the *whole encoded message*
read from the socket, not the `fragment` field inside it. If a stream frame exceeds the limit, that
stream is reset with `PAYLOAD_TOO_LARGE`; if a connection-level frame exceeds it, the connection fails,
because there is no stream to reset. An over-size `open` for a new id has its id checked like any
other `open` (WSM-SID-005) and counts as the remote's highest open before the reset goes out, so a
later `data` or `reset` on that id is ignored. An over-size `data` or `reset` for an id that is not
live is not reset: at or below the high-water mark it is ignored (WSM-STM-002), above it the
connection fails (WSM-STM-003). A conforming muxws sender fragments at the same constant and does not
trigger this check.

**A reassembled payload over `max_payload_bytes`.** This is the receiver's memory limit, and it is
checked **as fragments arrive**: for each fragment, the accumulated byte count plus the incoming
fragment is compared against the limit, and the stream is reset when a fragment exceeds it. The
partial buffer is discarded in the same step, so the receiver never holds more than the limit.

Each side observes:

- **The receiver**: the stream is reset locally; a handler awaiting or iterating it receives a
  `StreamReset` with `ResetCode.PAYLOAD_TOO_LARGE`. The rest of the connection is not affected.
- **The sender**: a `reset` frame arrives for that stream, and the pending `send`/`await`/`async for`
  raises `StreamReset` with the same code and the receiver's reason text. The connection stays open.
  The same payload fails the same way on retry; reduce it or split it in the application.

## `max_payload_bytes` belongs to the receiver

```python
# fragment
peer = await connect(url, max_payload_bytes=67_108_864)  # the default: 64 MiB
```

```ts
// fragment
const peer = await connect(url, { maxPayloadBytes: 67_108_864 }); // the default: 64 MiB
```

The default is 64 MiB, and the limit is **local**. It is not announced, not negotiated, and not
carried in any frame. A sender learns it only from the reset. If senders could read the limit, it
would become part of the protocol that both ends must agree on.

**Raise it on the receiving peer.** The sender has no setting for it. If a browser uploads 200 MB
documents to a server, set it on the server's `accept()`:

```python
# fragment
peer = await accept(websocket, max_payload_bytes=209_715_200)
```

If the server also streams 200 MB exports to the browser, set it on the browser's `connect()` as
well. The two limits are independent.

`max_concurrent_streams` (default `100`) is local in the same way: it bounds how many streams the
**remote** may have open on this peer at once, it is not announced, and an `open` over the limit is
answered with `REFUSED` without running the handler.

## See also

- [`api/types`](/api/types) — `MAX_FRAME_BYTES`, `Frame` and every envelope field
- [`api/connect`](/api/connect) — `max_payload_bytes`, `max_concurrent_streams`, `max_frame_bytes`
- [`api/accept`](/api/accept) — the same three on the acceptor
- [`api/errors`](/api/errors) — `ResetCode.PAYLOAD_TOO_LARGE`, `StreamReset`, `ProtocolError`
- [`api/codec`](/api/codec) — `encode_payload` / `decode_payload`, over which boundaries are cut
