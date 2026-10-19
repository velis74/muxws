---
outline: deep
---

# Codec

A codec converts a logical `Frame` to a WebSocket message and back. It also encodes and decodes a
*payload* on its own, because fragmentation operates on the encoded form of a payload, not on the
frame that carries it.

- **Both ends must use the same codec.** The WebSocket subprotocol is `muxws.v1.<codec.name>`
  (`muxws.v1.json`, `muxws.v1.msgpack`). The codec is checked during the handshake, and a mismatch is
  refused with HTTP 400.
- **Registration is explicit and eager.** muxws performs no dynamic import, no entry-point scan and
  no check for installed modules. Looking up an unregistered name raises `CodecNotRegistered`; there
  is no fallback to JSON.

The package registers `json`; the application registers every other codec.

## `Codec`

The codec interface: a `runtime_checkable` `Protocol` in Python, an `interface` in TypeScript. Any
object with these six members is a codec; there is no base class.

`binary` is a declared attribute. The peer reads it to choose between the socket's text and
binary send methods and to determine which inbound message type to expect. The peer does not inspect
a message's type to choose a decoder, so a text message on a msgpack connection is a protocol
error.

### Signature

```python
@runtime_checkable
class Codec(Protocol):
    name: str
    binary: bool

    def encode(self, frame: Frame) -> str | bytes: ...

    def decode(self, message: str | bytes) -> Frame: ...

    def encode_payload(self, payload: Any) -> str | bytes: ...

    def decode_payload(self, data: str | bytes) -> Any: ...
```

```ts
export interface Codec {
  readonly name: string;
  readonly binary: boolean;
  encode(frame: Frame): string | ArrayBuffer;
  decode(message: string | ArrayBuffer): Frame;
  encodePayload(payload: unknown): string | ArrayBuffer;
  decodePayload(data: string | ArrayBuffer): unknown;
}
```

### Parameters

The members an implementation must provide:

| Name | Type | Default | What it does |
|---|---|---|---|
| `name` | `str` / `string` | none — required | The codec's wire name. `connect()` and `accept()` offer and verify the subprotocol `muxws.v1.<name>`. Normally equal to the key it is registered under. |
| `binary` | `bool` / `boolean` | none — required | Declares whether encoded output travels as a binary WebSocket message. Read by the peer to pick `send_bytes` / `sendBytes` over `send_text` / `sendText`. |
| `encode(frame)` | `(Frame) -> str \| bytes` / `(Frame) => string \| ArrayBuffer` | none — required | Renders one frame as one WebSocket message. Fields still at their default are omitted; keys are ordered `type`, `stream`, then the rest alphabetically. |
| `decode(message)` | `(str \| bytes) -> Frame` / `(string \| ArrayBuffer) => Frame` | none — required | Parses one WebSocket message back into a `Frame`. Unknown envelope keys are dropped; an unrecognised `type` survives as an ordinary frame for the peer to ignore. |
| `encode_payload(payload)` / `encodePayload(payload)` | `(Any) -> str \| bytes` / `(unknown) => string \| ArrayBuffer` | none — required | Encodes a bare payload. Its output must equal the bytes that `encode` writes in a frame's payload position. The sender slices this output into fragments. |
| `decode_payload(data)` / `decodePayload(data)` | `(str \| bytes) -> Any` / `(string \| ArrayBuffer) => unknown` | none — required | Decodes a reassembled payload — the concatenation of every fragment — back into a value. |

### Return

`Codec` is a type, not a callable: it returns nothing. `isinstance(obj, Codec)` in Python returns
`True` when the four methods and two attributes are present; TypeScript checks it at compile time
only.

### Raises

Raises: nothing by itself. An implementation's `decode` / `decode_payload` should raise
`ProtocolError` for input it cannot parse, and its `encode` should raise `TypeError` for a value it
cannot represent — that is what `JsonCodec` and `MsgpackCodec` do, and what the peer's error handling
expects.

### Example

```python
from typing import Any

from muxws import Codec, Frame, JsonCodec

json_codec = JsonCodec()
print(isinstance(json_codec, Codec), json_codec.name, json_codec.binary)


class ShoutyJsonCodec:
    """A Codec that delegates to JsonCodec but registers under its own name."""

    name = "shouty"
    binary = False

    def __init__(self) -> None:
        self._inner = JsonCodec()

    def encode(self, frame: Frame) -> str:
        return self._inner.encode(frame)

    def decode(self, message: str | bytes) -> Frame:
        return self._inner.decode(message)

    def encode_payload(self, payload: Any) -> str:
        return self._inner.encode_payload(payload)

    def decode_payload(self, data: str | bytes) -> Any:
        return self._inner.decode_payload(data)


shouty: Codec = ShoutyJsonCodec()
print(isinstance(shouty, Codec), shouty.name, shouty.binary)
```

```ts
import { type Codec, type Frame, JsonCodec } from 'muxws';

const jsonCodec = new JsonCodec();
console.log(jsonCodec.name, jsonCodec.binary);

/** A Codec that delegates to JsonCodec but registers under its own name. */
class ShoutyJsonCodec implements Codec {
  readonly name = 'shouty';
  readonly binary = false;

  private readonly inner = new JsonCodec();

  encode(frame: Frame): string | ArrayBuffer {
    return this.inner.encode(frame);
  }

  decode(message: string | ArrayBuffer): Frame {
    return this.inner.decode(message);
  }

  encodePayload(payload: unknown): string | ArrayBuffer {
    return this.inner.encodePayload(payload);
  }

  decodePayload(data: string | ArrayBuffer): unknown {
    return this.inner.decodePayload(data);
  }
}

const shouty: Codec = new ShoutyJsonCodec();
console.log(shouty.name, shouty.binary);
```

## `JsonCodec`

The JSON codec, required for interoperability, and the only codec the library registers. It is
registered under the name `json` by the package entry point (`muxws/__init__.py` / `ts/index.ts`),
not by `codec.py` / `codec.ts`, so that importing the codec module has no side effect and can be
tree-shaken.

`bytes` are **not** a payload type under JSON. `JsonCodec` raises `TypeError` for them and does not
base64-encode them. Encode them in the application, or use a binary codec.

The two ports emit the same bytes for strings, containers, booleans, null and integers up to 2⁵³.
They differ on floats and on larger integers: Python writes `1.0`, `-0.0`, `1e+16`, `1e-07` where
JavaScript writes `1`, `0`, `10000000000000000`, `1e-7`, and JavaScript rounds an integer beyond 2⁵³
to the nearest double. Interoperability does not depend on byte equality, because the sender chooses
fragment boundaries and the receiver only concatenates. The shared conformance corpus contains no
floats for this reason.

### Signature

```python
class JsonCodec:
    name = "json"
    binary = False

    def encode(self, frame: Frame) -> str: ...

    def decode(self, message: str | bytes) -> Frame: ...

    def encode_payload(self, payload: Any) -> str: ...

    def decode_payload(self, data: str | bytes) -> Any: ...
```

```ts
export class JsonCodec implements Codec {
  readonly name = 'json';
  readonly binary = false;

  encode(frame: Frame): string;
  decode(message: string | ArrayBuffer): Frame;
  encodePayload(payload: unknown): string;
  decodePayload(data: string | ArrayBuffer): unknown;
}
```

### Parameters

The constructor takes none in either language.

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | `JsonCodec()` / `new JsonCodec()` takes no arguments. The encoder settings are fixed: no whitespace between tokens, no ASCII escaping of non-ASCII characters. |

### Return

A codec instance whose `name` is `"json"` and whose `binary` is `False` / `false`.

### Raises

The constructor raises nothing. `encode` and `encode_payload` / `encodePayload` raise `TypeError`
for a value JSON cannot represent: bytes; in Python any other type the standard `json` module does
not encode; in TypeScript also `Map`, `Set`, `bigint`, functions, symbols, non-finite numbers and
circular structures. In Python a non-finite float (`nan`, `inf`) or a circular structure raises
`ValueError`. `decode` and `decode_payload` / `decodePayload` raise `ProtocolError` for text that is
not JSON, and `decode` also raises it when the message decodes to something that is not an object.

### Example

```python
from muxws import Frame, JsonCodec

codec = JsonCodec()
message = codec.encode(Frame("open", stream=1, payload={"user": 7}, headers={"trace": "abc"}))
print(message)
print(codec.decode(message))
print(codec.encode_payload(["a", 1, None]), codec.decode_payload('["a",1,null]'))

try:
    codec.encode_payload(b"raw bytes")
except TypeError as exc:
    print("refused:", str(exc).splitlines()[0][:60])
```

```ts
import { JsonCodec } from 'muxws';

const codec = new JsonCodec();
const message = codec.encode({ type: 'open', stream: 1, payload: { user: 7 }, headers: { trace: 'abc' } });
console.log(message);
console.log(codec.decode(message));
console.log(codec.encodePayload(['a', 1, null]), codec.decodePayload('["a",1,null]'));

try {
  codec.encodePayload(new ArrayBuffer(4));
} catch (error) {
  console.log('refused:', (error as Error).message.slice(0, 60));
}
```

## `MsgpackCodec`

The binary codec. It is an **optional extra** in both languages and has its own import path, so
neither muxws nor a bundle imports the msgpack library unless the application imports this codec:

```bash
pip install "muxws[msgpack]"
npm install @msgpack/msgpack
```

```python
from muxws.codecs.msgpack_ import MsgpackCodec
```

```ts
import { MsgpackCodec } from 'muxws/msgpack';
```

::: warning It does not register itself
Importing `MsgpackCodec` registers nothing. **The application calls
`register_codec("msgpack", MsgpackCodec())` / `registerCodec('msgpack', new MsgpackCodec())` during
bootstrap, before it connects.**

In TypeScript, a module with an import-time side effect cannot be tree-shaken, regardless of
`"sideEffects": false`, so registration at import would put `@msgpack/msgpack` in every bundle that
references the subpath. In Python, registration at import time would make the set of available codecs
depend on which modules were imported, and in which order.
:::

The module file is `msgpack_.py`, with a trailing underscore, because a `msgpack.py` inside the
package would shadow the third-party module it imports. The public import path is
`muxws.codecs.msgpack_`.

msgpack output is not pinned as bytes in any fixture: Python's `msgpack` and `@msgpack/msgpack`
choose integer widths and map formats differently, and both encodings are valid. The tests of both
ports assert round-trip only.

### Signature

```python
class MsgpackCodec:
    name = "msgpack"
    binary = True

    def encode(self, frame: Frame) -> bytes: ...

    def decode(self, message: str | bytes) -> Frame: ...

    def encode_payload(self, payload: Any) -> bytes: ...

    def decode_payload(self, data: str | bytes) -> Any: ...
```

```ts
export class MsgpackCodec implements Codec {
  readonly name = 'msgpack';
  readonly binary = true;

  encode(frame: Frame): ArrayBuffer;
  decode(message: string | ArrayBuffer): Frame;
  encodePayload(payload: unknown): ArrayBuffer;
  decodePayload(data: string | ArrayBuffer): unknown;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | `MsgpackCodec()` / `new MsgpackCodec()` takes no arguments. The pack and unpack options are fixed, so that both ports accept the same set of payloads (including maps with integer keys). |

### Return

A codec instance whose `name` is `"msgpack"` and whose `binary` is `True` / `true`.

### Raises

The constructor raises nothing. `decode` and `decode_payload` / `decodePayload` raise `ProtocolError`
for input msgpack cannot parse, **and** for a `str` / `string`: a text WebSocket message cannot carry
msgpack, and a text message on a binary connection is a protocol violation by the remote. `decode` also raises `ProtocolError`
when the message decodes to something that is not a map. `encode` and `encode_payload` /
`encodePayload` propagate whatever the underlying msgpack library raises for a value it cannot
represent.

### Example

```python
from muxws import Frame, register_codec, registered_codecs
from muxws.codecs.msgpack_ import MsgpackCodec

# Explicit, eager, at bootstrap. Importing the module above registered nothing.
register_codec("msgpack", MsgpackCodec())
print(registered_codecs())

codec = MsgpackCodec()
print(codec.name, codec.binary)

message = codec.encode(Frame("data", stream=1, payload={"blob": b"\x00\x01\x02"}))
print(type(message).__name__, len(message))
print(codec.decode(message).payload)
```

```ts
import { registerCodec, registeredCodecs } from 'muxws';
import { MsgpackCodec } from 'muxws/msgpack';

// Explicit, eager, at bootstrap. Importing the subpath above registered nothing.
registerCodec('msgpack', new MsgpackCodec());
console.log(registeredCodecs());

const codec = new MsgpackCodec();
console.log(codec.name, codec.binary);

const message = codec.encode({ type: 'data', stream: 1, payload: { blob: new Uint8Array([0, 1, 2]).buffer } });
console.log(message.constructor.name, message.byteLength);
console.log(new Uint8Array((codec.decode(message).payload as { blob: ArrayBuffer }).blob));
```

## `register_codec` / `registerCodec`

Puts a codec in the process-wide registry under a name. Call it during bootstrap, before anything
connects. Registering the same name again replaces the previous entry without an error or a
warning.

### Signature

```python
def register_codec(name: str, codec: Codec) -> None: ...
```

```ts
export function registerCodec(name: string, codec: Codec): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `name` | `str` / `string` | required | The key to register under: the value `settings.codec` (`MUXWS_CODEC` / `VITE_MUXWS_CODEC`) is looked up by. The subprotocol is built from `codec.name`, not from this key; register a codec under its own `name` so that the configured value and the negotiated subprotocol match. |
| `codec` | `Codec` | required | The codec instance. An instance, not a class: the registry stores exactly the object you pass. |

### Return

`None` / `void`.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, register_codec, registered_codecs

print("before:", registered_codecs())
register_codec("json-alias", JsonCodec())
print("after: ", registered_codecs())
```

```ts
import { JsonCodec, registerCodec, registeredCodecs } from 'muxws';

console.log('before:', registeredCodecs());
registerCodec('json-alias', new JsonCodec());
console.log('after: ', registeredCodecs());
```

## `get_codec` / `getCodec`

Looks a codec up by name. `connect()` and `accept()` call it before opening a socket; on an unknown
name the error message names the environment variable, the configured value and the registered
names.

### Signature

```python
def get_codec(name: str) -> Codec: ...
```

```ts
export function getCodec(name: string): Codec;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `name` | `str` / `string` | required | The registered name to look up. |

### Return

The `Codec` instance registered under `name`.

### Raises

`CodecNotRegistered` when nothing is registered under `name`. Its `configured` attribute holds the
name asked for and `available` holds the registered set. There is **no** fallback to `json`.

### Example

```python
from muxws import CodecNotRegistered, get_codec

print(get_codec("json").name)
try:
    get_codec("protobuf")
except CodecNotRegistered as exc:
    print(exc.configured, "->", exc.available)
```

```ts
import { CodecNotRegistered, getCodec } from 'muxws';

console.log(getCodec('json').name);
try {
  getCodec('protobuf');
} catch (error) {
  if (error instanceof CodecNotRegistered) console.log(error.configured, '->', error.available);
}
```

## `registered_codecs` / `registeredCodecs`

Every registered name, sorted.

### Signature

```python
def registered_codecs() -> list[str]: ...
```

```ts
export function registeredCodecs(): string[];
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | Takes no arguments. |

### Return

A sorted `list[str]` / `string[]` of registered names. A fresh copy each call — mutating it does not
change the registry.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, register_codec, registered_codecs

register_codec("zzz-last", JsonCodec())
names = registered_codecs()
print(names)
names.clear()
print("registry unchanged:", registered_codecs())
```

```ts
import { JsonCodec, registerCodec, registeredCodecs } from 'muxws';

registerCodec('zzz-last', new JsonCodec());
const names = registeredCodecs();
console.log(names);
names.length = 0;
console.log('registry unchanged:', registeredCodecs());
```

## `clearCodecs`

TypeScript only. Forgets every registration, including `json`. It is intended for tests; the library
never calls it. After it, `connect()` raises `CodecNotRegistered` until a codec is registered again.

Python has no equivalent.

### Signature

```ts
export function clearCodecs(): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | Takes no arguments. |

### Return

`void`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, clearCodecs, registerCodec, registeredCodecs } from 'muxws';

console.log('before:', registeredCodecs());
clearCodecs();
console.log('cleared:', registeredCodecs());
registerCodec('json', new JsonCodec());
console.log('restored:', registeredCodecs());
```

## `resolve_codec`

Python only. The rule `connect()` and `accept()` both follow: an explicit `codec=` argument wins;
otherwise the configured name is looked up. It never falls back to `json`.

TypeScript has no exported equivalent — its `connect()` and `accept()` spell the same rule inline as
`options.codec ?? getCodec(settings.codec)`.

### Signature

```python
def resolve_codec(override: Codec | None = None) -> Codec: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `override` | `Codec \| None` | `None` | An explicit codec. When given, it is returned unchanged and `settings.codec` is not consulted. Positional or keyword. |

### Return

The `Codec` to use for this connection.

### Raises

`CodecNotRegistered` when `override` is `None` and `settings.codec` names nothing registered.

### Example

```python
from muxws import JsonCodec, resolve_codec, settings

print(resolve_codec().name, "(from settings.codec =", settings.codec + ")")

explicit = JsonCodec()
print(resolve_codec(explicit) is explicit)
```

## `Settings` and `settings`

The deployment's codec choice. It is read from the environment rather than passed as a call
argument, because both ends of a connection must use the same codec and the choice is made per
deployment, not per call site.

- Python reads `MUXWS_CODEC`, defaulting to `json`.
- TypeScript reads `import.meta.env.VITE_MUXWS_CODEC`, defaulting to `json`. The `VITE_` prefix is
  what makes Vite replace the expression statically at build time. Where there is no such environment
  — Node, vitest, a CommonJS consumer — every step is guarded and the default applies.

`settings` is a singleton and is **writable**: an application may set `settings.codec` during
bootstrap. The environment is read once, when the module is first imported. `connect()` and
`accept()` read the attribute on every call, so an assignment during bootstrap applies to every later
connection. A change to the environment variable after import has no effect until
`settings.reload()`.

Any string is accepted as the configured name, including the empty string. A name with no registered
codec raises `CodecNotRegistered` at `connect()` / `accept()`; there is no fallback.

### Signature

```python
class Settings:
    def __init__(self) -> None: ...

    def reload(self) -> None: ...

    def __repr__(self) -> str: ...


settings = Settings()
```

```ts
export class Settings {
  codec: string;

  constructor();
  reload(): void;
  toString(): string;
}

export const settings: Settings;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(constructor)* | — | — | `Settings()` / `new Settings()` takes no arguments; it reads the environment once. |
| `codec` (attribute) | `str` / `string` | `os.environ["MUXWS_CODEC"]` / `import.meta.env.VITE_MUXWS_CODEC`, else `"json"` | The configured codec name. Assign to it during bootstrap to override the environment. |
| `reload()` | `() -> None` / `() => void` | — | Re-reads the environment, discarding any assignment. Intended for tests; an application sets `codec` directly. |

### Return

`Settings()` returns the settings object. `reload()` returns `None` / `void`. `repr(settings)` /
`String(settings)` returns `Settings(codec='json')`.

### Raises

Raises: nothing. A codec name that is not registered fails later, at `get_codec()` / `getCodec()`.

### Example

```python
import os

from muxws import settings

print(repr(settings), "| default:", settings.codec)

os.environ["MUXWS_CODEC"] = "msgpack"
settings.reload()
print("after reload:", settings.codec)

settings.codec = "json"
print("set directly:", settings.codec)
```

```ts
import { Settings, settings } from 'muxws';

console.log(String(settings), '| default:', settings.codec);

settings.codec = 'msgpack';
console.log('set directly:', settings.codec);

settings.reload();
console.log('after reload:', settings.codec);

console.log(String(new Settings()));
```

## See also

- [Errors](./errors.md) — `CodecError`, `CodecNotRegistered` and `CodecMismatch`.
- [Types](./types.md) — `Frame`, the value a codec encodes, and every envelope field.
- [Transports](./transports.md) — where `codec.binary` decides which send method the peer calls.
- [Connect](./connect.md) and [Accept](./accept.md) — the `codec=` / `codec:` override.
