# Codecs

A codec is the one seam between a muxws frame and the bytes on the socket. muxws ships two — `json`,
registered for you, and `msgpack`, behind an optional dependency — and the seam is a small enough
interface that a third is a few dozen lines.

There is exactly **one codec per connection**, chosen before the socket is touched, and both ends must
agree on it.

## Selecting a codec

The codec name is read from the **environment**, not from a call argument, because both ends of a
connection must agree on it and an argument is decided per call site rather than per deployment.

| | Variable | Default |
|---|---|---|
| Python | `MUXWS_CODEC` | `json` |
| TypeScript / browser | `VITE_MUXWS_CODEC` | `json` |

```bash
MUXWS_CODEC=msgpack python -m myapp
```

```bash
VITE_MUXWS_CODEC=msgpack npm run build
```

The `VITE_` prefix is what makes Vite replace the expression statically at build time; outside a Vite
build — under Node, under vitest, in a CommonJS consumer — there may be no such environment at all,
and the default applies rather than an error.

An application that would rather decide in code sets the singleton during bootstrap, before it
connects:

```python
from muxws.conf import settings

settings.codec = "msgpack"
```

```ts
import { settings } from 'muxws';

settings.codec = 'msgpack';
```

The value is read at connection time, not at import time, so setting it during bootstrap works even
for a module that imported muxws early. `settings.reload()` re-reads the environment; it exists for
tests, and an application sets `codec` directly.

Finally, a single connection can be given a codec instance outright, which wins over everything above:

```python
# fragment
peer = await connect(url, codec=MsgpackCodec())
```

This is for a process that genuinely speaks different codecs on different connections. It is not the
normal way to configure one.

## Both ends must agree, and the subprotocol says so

The WebSocket subprotocol is `muxws.v1.<codec>` — `muxws.v1.json`, `muxws.v1.msgpack`. The dialer
offers it **first** in its subprotocol list (an application may append its own entries after it, and
the acceptor ignores every one of them), and the acceptor either selects that exact value or **refuses
the upgrade with HTTP 400**.

This is an **assertion, not a negotiation**. There is no fallback encoding, no list of acceptable
alternatives, no per-connection multi-codec support and no runtime codec branching anywhere in the
peer. Two peers configured differently do not meet in the middle; they do not connect at all.

Refusing at the handshake, rather than completing it and closing afterwards, is what makes the failure
readable: a dialer gets `CodecMismatch` naming both environment variables, because it does not yet
know which end is wrong.

```
CodecMismatch: the acceptor refused the muxws handshake for codec 'msgpack'. Both ends must be
configured for the same codec: VITE_MUXWS_CODEC in the browser, MUXWS_CODEC on the server. muxws
asserts the codec at the handshake and never falls back. …
```

The acceptor logs its half — what was offered against what it is configured for — because a browser
cannot read a rejection body and the diagnostic therefore cannot come from the server.

The `v1` in the middle is the protocol generation, and it is the only version on the wire. A peer of a
different generation is refused here in the same way, and for the same reason. See
[Interop](/guide/interop).

## An unregistered name fails loudly, and never falls back

```
# fragment - a Python session transcript
>>> settings.codec = "msgpakc"
>>> await connect(url)
CodecNotRegistered: codec 'msgpakc' is not registered (MUXWS_CODEC='msgpakc'); registered codecs are
['json']. Call register_codec('msgpakc', ...) during bootstrap, before connecting.
```

Two things about this failure are deliberate.

**It happens before any socket is touched.** The codec is resolved first, so a misconfigured
deployment fails as a named error at the call that starts a connection — not as a puzzling decode
error on the tenth frame, once traffic has been flowing for a while.

**There is no fallback to JSON.** Ever. A deployment that believes it is running msgpack, is not, and
finds out from neither end is the failure this rule exists to prevent: both ends silently fell back,
everything works, and the performance work that motivated the change did nothing.

`registered_codecs()` / `registeredCodecs()` returns the sorted list of names, which is what the error
message above prints.

## Registering msgpack

Registration is **explicit and eager**. There is no dynamic import, no lazy auto-registration, no
entry-point scan, and no probing of whether a module happens to be installed. A codec module never
registers itself at import time — a side-effecting import can never be tree-shaken out, whatever a
package claims about side effects.

So: install the extra, and call `register_codec` during bootstrap.

::: code-group

```bash [Python]
pip install "muxws[msgpack]"
```

```bash [TypeScript]
npm install @msgpack/msgpack
```

:::

::: code-group

```python [Python]
from muxws import register_codec
from muxws.codecs.msgpack_ import MsgpackCodec

register_codec("msgpack", MsgpackCodec())
```

```ts [TypeScript]
import { registerCodec } from 'muxws';
import { MsgpackCodec } from 'muxws/msgpack';

registerCodec('msgpack', new MsgpackCodec());
```

:::

Then set `MUXWS_CODEC=msgpack` / `VITE_MUXWS_CODEC=msgpack` on **both** ends. Registering without
selecting connects over JSON; selecting without registering raises `CodecNotRegistered` at connect
time.

The `muxws/msgpack` subpath is the whole of the selection in the browser: the main entry point never
imports it, so a bundle that does not ask for msgpack never carries it, and `@msgpack/msgpack` stays
an optional peer dependency.

## Bytes, and what JSON will not do for you

**Bytes are a first-class payload type under a binary codec.** Under msgpack you send `bytes` (Python)
or an `ArrayBuffer` (TypeScript) as a payload, or nested anywhere inside one, and get the same thing
back.

```python
# fragment
await peer.request({"name": "logo.png", "body": open("logo.png", "rb").read()})
```

**Under JSON they are not, and muxws will not base64-encode them on your behalf.** The JSON codec
raises `TypeError` instead:

```
TypeError: the json codec cannot carry bytes: bytes are a payload type only under a binary codec,
and muxws does not base64-encode them for you … Either encode them in the application or configure
the msgpack codec.
```

The refusal is the point. Silently base64-encoding would make the same application code mean different
things on the two codecs — the remote would receive a `str` under JSON and `bytes` under msgpack — and
the sender would have no way to know which. Worse is what the languages do left to themselves:
`JSON.stringify` turns an `ArrayBuffer` into `{}`, which is not an encoding error but silent data loss.

If you need bytes over JSON, encode them in the application, where the decision and the decoding are
both visible:

```python
# fragment
import base64

await peer.request({"name": "logo.png", "body": base64.b64encode(blob).decode("ascii")})
```

Two smaller refusals from the JSON codec, for the same reason: `NaN` and `Infinity` are not JSON and
are rejected rather than emitted as bare tokens no other parser accepts, and a circular structure is
reported as one.

## Writing your own

The port is four methods and two attributes.

::: code-group

```python [Python]
# fragment - this is the port muxws already ships; you implement it, not redeclare it
from typing import Any, Protocol, runtime_checkable

from muxws import Frame


@runtime_checkable
class Codec(Protocol):
    name: str
    binary: bool

    def encode(self, frame: Frame) -> str | bytes: ...

    def decode(self, message: str | bytes) -> Frame: ...

    def encode_payload(self, payload: Any) -> str | bytes: ...

    def decode_payload(self, data: str | bytes) -> Any: ...
```

```ts [TypeScript]
// fragment - this is the port muxws already ships; you implement it, not redeclare it
export interface Codec {
  readonly name: string;
  readonly binary: boolean;
  encode(frame: Frame): string | ArrayBuffer;
  decode(message: string | ArrayBuffer): Frame;
  encodePayload(payload: unknown): string | ArrayBuffer;
  decodePayload(data: string | ArrayBuffer): unknown;
}
```

:::

What each member owes you:

**`name`** goes straight into the subprotocol as `muxws.v1.<name>`. Both ends must register the same
codec under the same name.

**`binary`** is **declared, not inferred**. The peer reads it to choose the socket's text or binary
send method, and to know what kind of inbound message to expect. A peer never sniffs a message to
decide which branch to take, so a codec whose `binary` disagrees with what `encode` actually returns
breaks the connection rather than merely being untidy.

**`encode` / `decode`** move a whole frame — envelope and all — to and from one WebSocket message.
`decode(encode(frame))` must reproduce the frame, which includes keeping an absent `payload` key and an
explicit `payload: null` apart, and dropping envelope fields the codec does not know rather than
preserving them.

**`encode_payload` / `decode_payload`** move a *payload* on its own. They exist because fragmentation
is defined over the encoded form of a logical payload: the sender encodes the payload with these,
slices *that* encoded form, and puts each slice into a frame that `encode` then encodes again. The
receiver concatenates the slices and hands the joined result to `decode_payload`. A codec that only
implemented the frame-level pair could not be fragmented over.

Then register it:

```python
# fragment
register_codec("cbor", CborCodec())
```

Two things to get right, both of which only fail against the *other* implementation and never against
your own tests:

- **Slice boundaries must be representable.** For a text codec, the splitter cuts on whole Unicode
  codepoints, so a surrogate pair is never split. Anything your `decode_payload` cannot be handed half
  of must not be splittable in the middle.
- **Both ports must encode the same value identically** if peers of both languages will use your codec
  — fragment boundaries are cut over the encoded form, so two encoders that differ cut in different
  places. (The two shipped JSON codecs agree for strings, containers, booleans, null and integers up to
  2^53; they render floats differently, which is why the shared conformance corpus contains none.)

## See also

- [`api/codec`](/api/codec) — the `Codec` protocol, `JsonCodec`, `MsgpackCodec`, `register_codec`, `muxws.conf.settings`
- [`api/connect`](/api/connect) and [`api/accept`](/api/accept) — the `codec` parameter
- [`api/errors`](/api/errors) — `CodecError`, `CodecNotRegistered`, `CodecMismatch`
- [`api/types`](/api/types) — `Frame` and every envelope field
