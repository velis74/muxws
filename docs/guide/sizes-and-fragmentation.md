# Sizes and fragmentation

Every size muxws talks about is a **byte count of the fully encoded WebSocket message** — the
envelope included, exactly as it goes on the socket. Not the payload before encoding, not the number
of characters in a string, and not the `fragment` field on its own.

That sentence is the whole page. Everything below is a consequence of it.

## Bytes of encoded output, not characters

`muxws.encoded_length` / `encodedLength` is the measurement both ports use, and it is the only one:
UTF-8 byte length for text, buffer length for bytes.

```python
from muxws import encoded_length

encoded_length("hello")  # 5
encoded_length(b"hello")  # 5
```

A string's *length* is a different number in each language, and neither of them is the byte count.

| | `"🎉"` (U+1F389) |
|---|---|
| Python `len(s)` | 1 — codepoints |
| JavaScript `s.length` | 2 — UTF-16 code units |
| `encoded_length(s)` / `encodedLength(s)` | **4** — UTF-8 bytes |

The three disagree on every non-BMP character, which is exactly the class of character an application
never tests with. Worked through to a real frame, with the JSON codec:

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

The same value in the browser reports `party.length === 40000`. Twenty thousand characters, forty
thousand code units, eighty thousand bytes, eighty thousand and thirty-nine bytes on the wire. A limit
expressed in any unit but the last one is a limit that means something different on each end of the
connection.

Two practical consequences:

- A payload that "fits" by character count may be four times over a byte limit.
- Fragment boundaries are cut over the **encoded** form, on a boundary the codec can represent — for
  text, whole Unicode codepoints, so a surrogate pair is never split. Both ports cut in the same
  places because both run the same search over the same encoded bytes.

## `MAX_FRAME_BYTES` is 64 KiB, and it is not configurable

```python
from muxws import MAX_FRAME_BYTES

MAX_FRAME_BYTES  # 65536
```

It is a protocol constant. It is never negotiated, never announced, never read from configuration and
never derived from anything the remote said. A sender always fragments at it, regardless of what the
remote looks willing to accept; a receiver accepts anything up to it, and may accept more.

**The reason a bigger frame is not an improvement.** One muxws connection is one WebSocket, and a
WebSocket has a single global message order. Whatever frame is being written holds the socket until
it is finished. The cap is therefore not a memory limit — it is a *bound on how long one stream can
monopolise the wire before another stream gets a turn*. At 64 KiB, a stream shipping a megabyte gives
up the socket sixteen times on the way, and a 200-byte progress update on another stream waits at
most one 64 KiB write. Raise the cap to a megabyte and that same update waits for the whole megabyte.

So a larger frame is worse, not better, and making the number negotiable would let two peers agree to
optimise in the wrong direction — trading the interleaving that is the entire point of multiplexing
for a marginally smaller envelope overhead. There is no option to raise it, and adding one would be a
protocol change rather than a configuration change.

::: warning The `max_frame_bytes` argument is a test seam, not a setting
`connect()`, `accept()` and `Peer()` do take a `max_frame_bytes` argument (`maxFrameBytes` in
TypeScript), defaulting to `MAX_FRAME_BYTES`. It exists so the conformance runner can *lower* the cap
and exercise fragmentation without megabyte fixtures. It is not a tuning knob: raising it produces
frames a conforming remote is entitled to reject, and lowering it below what one envelope plus one
indivisible unit of payload needs raises `ProtocolError` at construction rather than looping forever
in the splitter.
:::

## Fragmentation is automatic and mandatory

You never call the splitter. Any frame whose encoded form exceeds the cap is replaced by a sequence of
fragment frames, and that happens inside the writer, on the way to the socket.

What the sequence looks like:

- `headers` ride the **first** fragment only, and are never split.
- `end` and `trailers` ride the **last** fragment only, and are never split.
- `more: true` is set on every fragment but the last. The sequence always terminates with a fragment
  carrying `more: false`, even when that final fragment carries no payload bytes at all.
- The receiver concatenates the `fragment` values and hands the joined result back to the codec once
  the closing fragment lands. Only then does a payload exist.

Two rules make interleaving possible rather than theoretical:

**A stream holds at most one unsent fragment.** Fragment *n+1* is sliced only once fragment *n* has
been handed to the socket. Slicing ahead is not wrong on the wire; it is wrong in the queue, because
it commits an order the writer has not been asked to commit to yet.

**The writer chooses across streams by round-robin, never by arrival order.** Every lane is asked once
before any lane is asked twice. Connection-level frames — `ping`, `pong`, `goaway` — have no stream of
their own and take their turn in the same rotation, so they neither jump it nor wait on it.

Together: a 1 MB export does not stall a 200-byte progress update. The export gets one fragment per
turn, the update gets its whole frame on its turn, and neither one had to know the other existed.

## `PAYLOAD_TOO_LARGE`, from both sides

Reset code `7`. Two different checks produce it, and they mean slightly different things.

**A single encoded message over the frame cap.** The receiver measures the *whole encoded message* it
just took off the socket, not the `fragment` field inside it. Over the cap on a stream frame, that
stream is reset with `PAYLOAD_TOO_LARGE`; over the cap on a connection-level frame, which has no
stream to blame, the connection fails. In practice a correct muxws sender never provokes this, because
it fragments at the same constant — it is the check that catches a peer that does not.

**A reassembled payload over `max_payload_bytes`.** This is the receiver's own memory limit, and it is
checked **as fragments arrive**: the accumulated byte count plus the incoming fragment is compared
against the limit on every fragment, and the stream is reset the moment the crossing fragment lands.
The partial buffer is dropped in the same step.

Checking after reassembly would be pointless. A receiver that assembles 200 MB in order to discover it
is larger than 64 MiB has already spent everything the limit was protecting.

What each side sees:

- **The receiver** — the stream is reset locally; a handler awaiting or iterating it gets a
  `StreamReset` carrying `ResetCode.PAYLOAD_TOO_LARGE`. Nothing else on the connection is affected.
- **The sender** — a `reset` frame arrives for that stream, and the pending `send`/`await`/`async for`
  raises `StreamReset` with the same code and the receiver's reason text. The connection stays up.
  Retrying the identical payload will fail identically; the reaction is to shrink it or to split it in
  the application, not to retry.

## `max_payload_bytes` belongs to the receiver

```python
# fragment
peer = await connect(url, max_payload_bytes=67_108_864)  # the default: 64 MiB
```

```ts
// fragment
const peer = await connect(url, { maxPayloadBytes: 67_108_864 }); // the default: 64 MiB
```

64 MiB, and it is **local**. It is never announced, never negotiated, and carried in no frame. A
sender has no way to learn it except by provoking the reset — which is deliberate: a limit the sender
can read is a limit the sender starts pre-validating against, and then it is a protocol feature that
both ends must agree on forever.

**To raise it, raise it on the peer that will be receiving.** The sender has no say and no setting.
If a browser uploads 200 MB documents to a server, the number belongs on the server's `accept()`:

```python
# fragment
peer = await accept(websocket, max_payload_bytes=209_715_200)
```

and if the server streams 200 MB exports back, the number belongs on the browser's `connect()` as
well. The two are independent; setting one does not set the other.

The mirror-image knob is `max_concurrent_streams` (default `100`), which is local in exactly the same
way: it bounds how many streams the **remote** may have open here at once, it is announced nowhere,
and an opener over the limit is answered `REFUSED` without its handler ever running.

## See also

- [`api/types`](/api/types) — `MAX_FRAME_BYTES`, `Frame` and every envelope field
- [`api/connect`](/api/connect) — `max_payload_bytes`, `max_concurrent_streams`, `max_frame_bytes`
- [`api/accept`](/api/accept) — the same three on the acceptor
- [`api/errors`](/api/errors) — `ResetCode.PAYLOAD_TOO_LARGE`, `StreamReset`, `ProtocolError`
- [`api/codec`](/api/codec) — `encode_payload` / `decode_payload`, over which boundaries are cut
