# Architecture

muxws consists of five components and one transport interface. Each component is a separate module in
both languages, and the Python and TypeScript implementations have the same module structure.

## The five components

### 1. The codec

`muxws/codecs/__init__.py` &middot; `ts/codec.ts`

A `Codec` turns a logical frame into a WebSocket message and back. It has four members and two
declared attributes:

| Member | Python | TypeScript |
|---|---|---|
| Frame in, message out | `encode(frame)` | `encode(frame)` |
| Message in, frame out | `decode(message)` | `decode(message)` |
| Payload in, encoded payload out | `encode_payload(payload)` | `encodePayload(payload)` |
| Encoded payload in, payload out | `decode_payload(data)` | `decodePayload(data)` |
| Registry name | `name` | `name` |
| Text or binary | `binary` | `binary` |

The payload-level pair exists because fragmentation is defined over the encoded form of the payload
alone, and neither slicing nor reassembly can be expressed through `encode`/`decode`.

`binary` is **declared by the codec**, not inferred from messages. The peer reads it to choose between
the socket's text send and binary send, and to know which message type to expect. No code inspects a
message to determine its type.

Codecs are registered explicitly with `register_codec` / `registerCodec` during bootstrap, before
connecting; only `json` is registered by default. There is no entry-point scan, no lazy registration,
no self-registration of a codec module on import, and no detection of installed modules. A codec name
that was never registered fails at startup; there is no fallback to JSON. See [Codecs](/guide/codecs).

### 2. The frame layer

`muxws/frames.py`, `muxws/fragment.py` &middot; `ts/frames.ts`, `ts/fragment.ts`

`Frame` is the logical protocol unit: one frame per WebSocket message, immutable and compared by
value, with spelled-out snake_case field names in both languages. Rendering a frame to an envelope
omits every field that has its default value, so an ordinary `data` frame is encoded with three keys
rather than all twelve fields of the model.

Two properties of the model affect the wire format:

- **`ABSENT` is not `None`.** `payload=ABSENT` emits no `payload` key; `payload=None` emits
  `"payload": null`. A frame without a payload and a frame with an explicit null payload are
  different frames.
- **Unknown keys and types are tolerated, not preserved.** Decoding drops envelope keys the model
  does not define. A frame with an unrecognised `type` is decoded as an ordinary frame, and the peer,
  not the codec, ignores it.

`fragment.py` / `fragment.ts` hold the splitter and the assembler as pure functions of
`(frame, cap, codec)`. They do not access a socket or any state. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation).

### 3. The stream state machine

`muxws/stream.py` &middot; `ts/stream.ts`

A `Stream` consists of a five-state machine, a consumer claim and a receive queue. It records whether
it may still send, whether the remote has ended, what closed it, and which of its two consumption
modes (await or iteration) has claimed it. It has no access to the socket: it passes every frame it
sends to its peer's writer.

See [Streams & cancellation](/guide/streams-and-cancellation) for the states, and
[Call shapes](/guide/call-shapes) for the two shapes.

### 4. The writer

`muxws/writer.py` &middot; `ts/writer.ts`

The writer performs the multiplexing, in a single object. It holds one queue (lane) per stream, plus
one lane for connection-level frames, which belong to no stream. It selects the next frame to send by
**round-robin across lanes**, not by arrival order.

This gives two properties:

- A stream holds **at most one** prepared, unsent fragment. Fragment *n+1* is sliced only after
  fragment *n* has been passed to the socket. Slicing ahead would fix the order of frames in the queue
  before the writer selects them.
- Fragments of one payload are contiguous **within their stream** and interleave with frames of other
  streams.

A single FIFO queue of frames anywhere on this path would break both properties: the order would be
fixed at enqueue time, and later stages could not interleave streams.

Encoding happens inside the writer, so a payload the codec rejects (for example `bytes` under JSON)
fails only that stream, with an internal-error reset, and the connection stays open.

### 5. The reconnect helper

`muxws/reconnect.py` &middot; `ts/reconnect.ts`

**Dialer only.** An acceptor cannot dial, so it has no reconnect helper. The helper's only persistent
state is an attempt counter. The backoff delay (seconds in Python, milliseconds in TypeScript), the
jitter and the decision to give up are computed from it, so the schedule is a pure function that can
be tested without a clock.

The helper manages the backoff schedule, the heartbeat, and the hello that is replayed verbatim on
every connection the peer makes. It does not manage streams; no stream survives a reconnect. See
[Reconnect](/guide/reconnect).

## `Peer` combines the components

`muxws/peer.py` &middot; `ts/peer.ts`

`Peer` is one end of one WebSocket. It owns the writer, the read loop, the live-stream map, the
per-connection id allocator, the ping registry and the goaway state. It is the same class on both
ends of a connection; see [Rationale](/guide/rationale#symmetry-one-peer-type-per-language).

## A frame's path

Outbound, from an application value to bytes on the wire:

```text
  application value            await stream.send({"rows": 128})
        │
        ▼
  Frame                        {type:"data", stream:7, payload:{...}}
        │
        ▼
  writer lane for stream 7     enqueued; one lane per stream, plus lane 0 for
        │                      ping / pong / goaway
        ▼
  round-robin                  every lane asked once before any lane is asked twice
        │
        ▼
  fragmentation                codec.encode_payload -> slice on a codepoint boundary
        │                      -> at most ONE prepared fragment held per lane
        ▼
  codec.encode(frame)          the fragment frame, envelope and all, as one message
        │
        ▼
  SocketAdapter                send_text(...) or send_bytes(...), chosen from codec.binary
        │
        ▼
  wire                         one WebSocket message, at most 65536 bytes
```

Inbound, from bytes on the wire to an application value:

```text
  wire                         one WebSocket message
        │
        ▼
  SocketAdapter.receive()      str or bytes, whichever the codec declared
        │
        ▼
  codec.decode(message)        -> Frame; unknown keys dropped, unknown types kept
        │
        ▼
  frame size check             measured on the WHOLE encoded message, not the fragment field
        │
        ▼
  dispatch                     open / data / reset -> a stream; ping / pong / goaway -> the peer
        │
        ▼
  assembler                    accumulates fragments; payload-size limit is checked as each
        │                      fragment arrives, never after reassembly
        ▼
  codec.decode_payload(joined) once the last fragment arrives
        │
        ▼
  application value            yielded by `async for`, or resolved by `await stream`
```

The two paths share only the codec and the adapter. The writer has no access to the socket, and the
socket adapter has no knowledge of streams.

## `SocketAdapter`: the transport interface

All components above use a four-method interface:

```python
# fragment
class SocketAdapter(Protocol):
    async def send_text(self, text: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def receive(self) -> str | bytes: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...
```

```typescript
// fragment
export interface SocketAdapter {
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}
```

All transport-specific code is in implementations of this interface. Starlette, `websockets`, the
browser `WebSocket`, the Node `ws` package and the in-memory test pair are five implementations of
these four methods, and the peer treats them identically. The reconnect helper redials through a
closure that returns a new adapter, so it does not depend on the WebSocket library either.

Text and binary sends are separate methods; there is no polymorphic `send`. The peer selects one from
`codec.binary`.

See [Transports](/guide/transports) for the shipped implementations, and
[`SocketAdapter`](/api/transports) for writing your own.

## The normative rules

This guide describes behaviour and its reasons. The normative rules, numbered and testable, against
which every implementation is checked, are in
[SPEC.md](https://github.com/velis74/muxws/blob/main/SPEC.md) at the repository root. This is the only
page of the site that links to it, so that the site contains no second copy of the normative text.

## See also

[`Codec`](/api/codec) &middot; [`Frame`, `MAX_FRAME_BYTES`](/api/types) &middot;
[`Stream`](/api/stream) &middot; [`Peer`](/api/peer) &middot; [`SocketAdapter`](/api/transports)
&middot; [`Reconnect`](/api/reconnect)
