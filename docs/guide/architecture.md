# Architecture

muxws is five moving parts and one port. Each part is a separate module in both languages, and the
two ports are structural mirrors of each other - if you have read one, you can find your way around
the other.

## The five moving parts

### 1. The codec seam

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

`binary` is **declared, not inferred**. The peer reads it to choose between the socket's text send
and its binary send, and to know which message type to expect back. Nothing anywhere sniffs a
message to decide which branch to take.

Codecs are registered explicitly and eagerly - no entry-point scan, no lazy auto-registration, no
probing whether a module happens to be installed - and a name that was never registered fails at
startup rather than falling back to JSON. See [Codecs](/guide/codecs).

### 2. The frame layer

`muxws/frames.py`, `muxws/fragment.py` &middot; `ts/frames.ts`, `ts/fragment.ts`

`Frame` is the logical protocol unit: one frame per WebSocket message, frozen and compared by value,
with spelled-out snake_case field names in both languages. Rendering a frame to an envelope omits
every field still at its default, which is why an ordinary `data` frame goes on the wire as three
keys rather than as the twelve the model defines.

Two details of the model are load-bearing:

- **`ABSENT` is not `None`.** `payload=ABSENT` emits no `payload` key at all; `payload=None` emits
  `"payload": null`. A frame that carries no payload and a frame that carries an explicit null are
  different frames.
- **Unknown things are tolerated, not preserved.** Decoding drops envelope keys the model does not
  know, and an unrecognised frame `type` survives decoding as an ordinary frame - it is the peer,
  not the codec, that ignores it.

`fragment.py` / `fragment.ts` hold the splitter and the assembler as pure functions of
`(frame, cap, codec)`. They touch no socket and read no state. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation).

### 3. The stream state machine

`muxws/stream.py` &middot; `ts/stream.ts`

A `Stream` is five states, a claim, and a queue. It knows whether it may still send, whether the
remote has ended, what it was closed by, and which of its two consuming shapes has taken it. It does
not know what a socket is: everything it sends goes to its peer's writer as a frame.

See [Streams & cancellation](/guide/streams-and-cancellation) for the states, and
[Call shapes](/guide/call-shapes) for the two shapes.

### 4. The writer

`muxws/writer.py` &middot; `ts/writer.ts`

The writer is where multiplexing actually happens, and it is one object on purpose. It holds one
queue per stream - plus one lane for connection-level frames, which have no stream of their own -
and chooses the next frame to put on the wire by **round-robin across lanes**, never by arrival
order.

Two properties fall out of that, and both matter:

- A stream holds **at most one** prepared, unsent fragment. Fragment *n+1* is sliced only once
  fragment *n* has reached the socket. Slicing ahead is not wrong on the wire; it is wrong in the
  queue, because it commits an order the writer has not been asked to commit to yet.
- Fragments of one payload stay contiguous **on their own stream**, and interleave freely with every
  other stream.

A single FIFO of frames anywhere in this path would break both at once: the ordering decision would
then be made at enqueue time, and no amount of cleverness further down could recover the
interleaving.

The encode happens inside the writer, so a payload the codec refuses - `bytes` under JSON, say -
fails exactly that one stream with an internal-error reset and leaves the connection working.

### 5. The reconnect helper

`muxws/reconnect.py` &middot; `ts/reconnect.ts`

**Dialer only.** An acceptor cannot dial, so it does not have one. The helper's entire persistent
state is an attempt counter; the backoff delay - seconds in Python, milliseconds in TypeScript - the
jitter and the decision to give up are all computed from it, which makes the schedule a pure
function testable without a clock.

It owns three things: the backoff schedule, the heartbeat, and the hello replayed verbatim on every
connection the peer ever makes. It owns nothing about streams - no stream survives a reconnect. See
[Reconnect](/guide/reconnect).

## `Peer` holds them together

`muxws/peer.py` &middot; `ts/peer.ts`

`Peer` is one end of one WebSocket. It owns the writer, the read loop, the live-stream map, the
per-connection id allocator, the ping registry and the goaway state. It is the same class on both
ends of a connection; see the symmetry claim in [Rationale](/guide/rationale).

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

Inbound is the mirror image, read bottom to top:

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
  codec.decode_payload(joined) once the last fragment lands
        │
        ▼
  application value            yielded by `async for`, or resolved by `await stream`
```

The two paths meet only at the codec and the adapter. Nothing in the writer knows what a socket is,
and nothing in the socket adapter knows what a stream is.

## `SocketAdapter`: the only transport-specific code

Everything above sits on a four-method port:

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

This is the **only** place transport-specific code lives. Starlette, `websockets`, the browser
`WebSocket`, the Node `ws` package and the in-memory test pair are five implementations of these
four methods, and the peer cannot tell them apart. It is also why the reconnect helper can dial
again without knowing what a WebSocket is: it holds a closure that produces an adapter.

Text and binary sends are separate methods, never one polymorphic `send`. The peer picks between
them from `codec.binary`, which the codec declares.

See [Transports](/guide/transports) for the shipped implementations, and
[`SocketAdapter`](/api/transports) for writing your own.

## The normative rules

This guide explains behaviour and the reasoning behind it. The rules themselves - numbered, testable,
and the thing a third implementation is checked against - live in
[SPEC.md](https://github.com/velis74/muxws/blob/main/SPEC.md) at the repository root. That link is
the only one on this site, deliberately: a second copy of normative text is a copy that drifts.

## See also

[`Codec`](/api/codec) &middot; [`Frame`, `MAX_FRAME_BYTES`](/api/types) &middot;
[`Stream`](/api/stream) &middot; [`Peer`](/api/peer) &middot; [`SocketAdapter`](/api/transports)
&middot; [`Reconnect`](/api/reconnect)
