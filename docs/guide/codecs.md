# Codecs

A codec converts a muxws frame to the bytes of one WebSocket message and back. muxws ships two codecs:
`json`, which is registered by default, and `msgpack`, which requires an optional dependency. The
codec interface has four methods and two attributes, so a further codec is a few dozen lines.

Each connection uses **one codec**, chosen before the socket is opened. Both ends must use the same
codec.

## Selecting a codec

The codec name is read from the **environment**, not from a call argument: both ends of a connection
must agree on it, and an environment variable is set per deployment, while an argument is set per call
site.

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

Vite replaces `import.meta.env.VITE_MUXWS_CODEC` statically at build time; the `VITE_` prefix is
required for that. Outside a Vite build (under Node, under vitest, in a CommonJS consumer) the
environment may be absent, and the default `json` applies; no error is raised.

To choose the codec in code instead, set the settings singleton during bootstrap, before connecting:

```python
from muxws.conf import settings

settings.codec = "msgpack"
```

```ts
import { settings } from 'muxws';

settings.codec = 'msgpack';
```

The value is read at connection time, not at import time, so setting it during bootstrap takes effect
even for a module that imported muxws earlier. `settings.reload()` re-reads the environment; it is
intended for tests, and an application sets `codec` directly.

A single connection can also be given a codec instance, which takes precedence over both of the above:

```python
# fragment
peer = await connect(url, codec=MsgpackCodec())
```

This is for a process that uses different codecs on different connections. For a single-codec
deployment, use the environment variable or `settings.codec`.

## Both ends must agree, and the subprotocol states the codec

The WebSocket subprotocol is `muxws.v1.<codec>`, for example `muxws.v1.json` or `muxws.v1.msgpack`.
The dialer offers it **first** in its subprotocol list. An application may append its own entries after
it; the acceptor ignores them. The acceptor either selects that exact value or **refuses the upgrade
with HTTP 400**.

The subprotocol is an **assertion, not a negotiation**. The peer has no fallback encoding, no list of
acceptable alternatives, no per-connection multi-codec support and no runtime codec branching. Two
peers configured with different codecs do not connect.

The refusal happens at the handshake, before the connection is established. The dialer raises
`CodecMismatch`, and the message names both environment variables, because the dialer cannot tell
which end is misconfigured.

```text
CodecMismatch: the acceptor refused the muxws handshake for codec 'msgpack'. Both ends must be
configured for the same codec: VITE_MUXWS_CODEC in the browser, MUXWS_CODEC on the server. muxws
asserts the codec at the handshake and never falls back. …
```

The acceptor logs its side of the diagnostic: the offered subprotocol and its own configured codec. A
browser cannot read the body of a rejected upgrade, so the dialer's error cannot include the server's
explanation.

The `v1` is the protocol generation and the only version identifier on the wire. A peer of a different
generation is refused at the handshake in the same way. See [Interop](/guide/interop).

## An unregistered name raises an error and never falls back

```text
# fragment - a Python session transcript
>>> settings.codec = "msgpakc"
>>> await connect(url)
CodecNotRegistered: codec 'msgpakc' is not registered (MUXWS_CODEC='msgpakc'); registered codecs are
['json']. Call register_codec('msgpakc', ...) during bootstrap, before connecting.
```

**The error is raised before any socket is opened.** The codec is resolved first, so a misconfigured
deployment fails with a named error at the call that starts the connection, not with a decode error
once frames are flowing.

**muxws never falls back to JSON.** With a fallback, a deployment configured for msgpack on both ends
but missing the registration would run over JSON on both ends without any error.

`registered_codecs()` / `registeredCodecs()` returns the sorted list of registered names; the error
message above prints that list.

## Registering msgpack

Registration is **explicit**. muxws uses no dynamic import, no lazy auto-registration, no entry-point
scan, and no check of whether a module is installed. A codec module does not register itself at import
time, because a module with import-time side effects cannot be removed by tree-shaking.

Install the extra and call `register_codec` during bootstrap:

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

In the browser, msgpack is available only through the `muxws/msgpack` subpath. The main entry point
does not import it, so a bundle that does not import `muxws/msgpack` does not contain it, and
`@msgpack/msgpack` is an optional peer dependency.

## Bytes

**Under a binary codec, bytes are a payload type.** Under msgpack, a payload that is or contains
`bytes` (Python) or an `ArrayBuffer` (TypeScript), at any nesting depth, is decoded to the same type.

```python
# fragment
await peer.request({"name": "logo.png", "body": open("logo.png", "rb").read()})
```

**Under JSON, bytes are not a payload type.** To send a PDF, an image or other binary data, **use
msgpack**; it requires one environment variable on each side and the registration above. The rest of
this section applies when msgpack is not an option.

The JSON codec raises `TypeError` for bytes and does not encode them:

```text
TypeError: the json codec cannot carry bytes: bytes are a payload type only under a binary codec,
and muxws does not base64-encode them for you … Either encode them in the application or configure
the msgpack codec.
```

If the JSON codec base64-encoded bytes, they would decode as a `str`, because a decoded JSON document
does not distinguish a base64 string from any other string. The same `send(b"...")` would then deliver
a `str` under JSON and `bytes` under msgpack, and the receiver could not tell which it received.

A sentinel such as `{"$bytes": "..."}` on the wire, unwrapped on decode, would preserve the type.
muxws does not use one because **muxws defines no vocabulary inside `payload`** (WSM-FRM-006): no
`kind`, no reserved key, no discriminator. A reserved key would require every consumer sharing the
socket to nest its own vocabulary inside muxws's, and a payload that contained that key for its own
purposes would be decoded incorrectly.

`conformance/` uses `{"$bytes": [...]}` in its fixtures. That convention applies only to the fixture
files, which are read by a test runner that implements it; it does not apply to payloads.

Without the codec's check, `JSON.stringify` encodes an `ArrayBuffer` as `{}`, which loses the data
without an error.

To send bytes over JSON, encode them in the application, so that the encoding and the decoding are
both in application code:

```python
# fragment
import base64

await peer.request({"name": "logo.png", "body": base64.b64encode(blob).decode("ascii")})
```

The JSON codec also rejects `NaN` and `Infinity`, which are not valid JSON, rather than emitting them as
bare tokens, and it rejects a circular structure with an error.

## Dates and other types without a JSON representation

`datetime` fails on **both** shipped codecs:

```python
await peer.request({"when": datetime.now()})
# TypeError: the json codec cannot encode datetime
# TypeError: can not serialize 'datetime.datetime' object   (msgpack)
```

msgpack has a timestamp type, and `msgpack-python` writes it when constructed with `datetime=True`.
`MsgpackCodec` does not enable it, because the TypeScript port would have to produce the same mapping,
and the default timestamp handling of the two libraries differs (WSM-CDC-006). Both ports of a codec
must produce the same encoding.

For dates, and for every other type muxws does not encode, **choose the representation in the
application** and use it on both ends. ISO 8601 strings and epoch milliseconds both work. If one end
sends one representation and the other end expects the other, muxws cannot detect the error.

```python
# fragment
await peer.request({"when": moment.isoformat()})            # "2026-08-10T12:00:00+00:00"
await peer.request({"when": int(moment.timestamp() * 1000)})  # 1775822400000
```

To have the transport encode such types, write a codec that handles them and register it under its own
name. The subprotocol then names that codec, and a peer configured for plain `json` refuses the
handshake instead of decoding a date as a plain value. See below.

## Writing your own

The codec interface has four methods and two attributes.

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

Requirements for each member:

**`name`** is used in the subprotocol as `muxws.v1.<name>`. Both ends must register the same codec
under the same name.

**`binary`** is **declared, not inferred**. The peer reads it to choose the socket's text or binary
send method and the type of inbound message it expects. The peer does not inspect a message to decide
how to handle it, so a codec whose `binary` does not match the type `encode` returns breaks the
connection.

**`encode` / `decode`** convert a whole frame, envelope included, to and from one WebSocket message.
`decode(encode(frame))` must reproduce the frame: an absent `payload` key and an explicit
`payload: null` must stay distinct, and envelope fields the codec does not know must be dropped.

**`encode_payload` / `decode_payload`** convert a payload on its own. Fragmentation is defined over the
encoded form of a logical payload: the sender encodes the payload with `encode_payload`, slices the
result, and puts each slice into a frame that `encode` encodes. The receiver concatenates the slices
and passes the result to `decode_payload`. A codec without this pair cannot be used with fragmentation.

Then register it:

```python
# fragment
register_codec("cbor", CborCodec())
```

Two requirements are only tested by a peer running the other implementation, not by tests against
your own:

- **Slice boundaries must be representable.** For a text codec, the splitter cuts on whole Unicode
  codepoints, so a surrogate pair is never split. If `decode_payload` cannot accept a partial value of
  some kind, that value must not be split.
- **Both ports must encode the same value identically** if peers in both languages use the codec.
  Fragment boundaries are cut over the encoded form, so two encoders with different output cut at
  different positions. (The two shipped JSON codecs produce the same output for strings, containers,
  booleans, null and integers up to 2^53; they format floats differently, so the shared conformance
  corpus contains no floats.)

## See also

- [`api/codec`](/api/codec) — the `Codec` protocol, `JsonCodec`, `MsgpackCodec`, `register_codec`, `muxws.conf.settings`
- [`api/connect`](/api/connect) and [`api/accept`](/api/accept) — the `codec` parameter
- [`api/errors`](/api/errors) — `CodecError`, `CodecNotRegistered`, `CodecMismatch`
- [`api/types`](/api/types) — `Frame` and every envelope field
