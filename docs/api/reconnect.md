---
outline: deep
---

# Reconnect

The reconnect helper is **dialer-only**. An acceptor cannot dial, so it has none: over there a
reconnection is simply a new connection with a new `Peer`.

Four things about it, before the tables:

- **`connect()` raises when the first attempt fails**, whatever is configured here. Reconnection
  covers connections that were established and then lost. A peer that retried its first dial forever
  would turn a typo in the URL into silence.
- **The attempt counter resets only on an *established* connection** - the socket open with the
  subprotocol accepted, *and* the hello acknowledged. Resetting it when the socket opened would turn
  exponential backoff into a fixed-interval hammer against a server that accepts sockets while its
  backend is down.
- **A reconnect restores a live socket and an accepted identity, and nothing else.** No stream
  survives, nothing is replayed, the stream id space starts empty, and `tags` start empty on the
  acceptor's side, because over there this is a whole new `Peer`.
- **Durations are seconds as floats in Python and milliseconds as integers in TypeScript.**
  `Reconnect(initial_delay=0.25)` and `new Reconnect({ initialDelayMs: 250 })` are the same schedule.

The delay before retry number *n* is:

```text
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

Jitter is applied to **every** computed delay, including the capped ones: without it, N peers whose
sockets died at the same instant retry at the same instant, and a server coming back up is knocked
over by the reconnection rather than by the load.

## `Reconnect` (Python)

The backoff options, and the whole of what an application configures.

### Signature

```python
@dataclass(frozen=True, slots=True)
class Reconnect:
    initial_delay: float = 0.25
    factor: float = 2.0
    max_delay: float = 30.0
    jitter: float = 0.3
    #: `None` means unlimited. Exhausting it fires `on_close` once with `will_retry` false and never
    #: dials again (WSM-RCN-044).
    max_attempts: int | None = None
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `initial_delay` | `float` — seconds | `0.25` | The unjittered delay before the first retry, in seconds. Every later delay is this multiplied by `factor` for each failed attempt. |
| `factor` | `float` | `2.0` | The multiplier per failed attempt. `1.0` gives a fixed interval; below 1 is a schedule that shrinks and is rejected. |
| `max_delay` | `float` — seconds | `30.0` | The ceiling on the unjittered delay, in seconds. Jitter is still applied after the cap, so capped retries stay dispersed. |
| `jitter` | `float` | `0.3` | The dispersion, as a fraction between 0 and 1: the delay is multiplied by `1 + uniform(-jitter, +jitter)`. `0` disables it - and means every peer that lost its socket at the same instant retries at the same instant. |
| `max_attempts` | `int \| None` | `None` | How many consecutive failed attempts before the helper gives up. `None` is unlimited. Exhausting it fires `on_close` exactly once with `will_retry` false and never dials again; `0` gives up without dialling at all. |

### Return

An immutable `Reconnect` instance. It is frozen, so a schedule cannot be edited under a running
helper; build a new one instead.

### Raises

- `ValueError` — `initial_delay` is not greater than zero, `factor` is below 1, or `jitter` is
  outside the range 0 to 1. Validated at construction, where the mistake was made, rather than a week
  later when a shrinking schedule reads as a flaky server.

`max_delay` and `max_attempts` are **not** validated: a `max_delay` below `initial_delay` simply caps
every delay at `max_delay`, and a negative `max_attempts` behaves as 0.

### Example

```python
import asyncio

from muxws import Reconnect, unjittered_delay


async def main() -> None:
    schedule = Reconnect(initial_delay=0.25, factor=2.0, max_delay=30.0, jitter=0.3)
    print([unjittered_delay(attempt, schedule) for attempt in range(9)], "seconds")

    capped = Reconnect(initial_delay=1.0, factor=10.0, max_delay=5.0)
    print([unjittered_delay(attempt, capped) for attempt in range(4)], "seconds")

    try:
        Reconnect(factor=0.5)
    except ValueError as exc:
        print("rejected:", exc)


asyncio.run(main())
```

## `Reconnect` (TypeScript)

A class rather than a bare interface, so the defaults live in one place and the three impossible
configurations are rejected where they are written.

### Signature

```ts
export class Reconnect {
  readonly initialDelayMs: number;
  readonly factor: number;
  readonly maxDelayMs: number;
  readonly jitter: number;
  readonly maxAttempts: number;

  constructor(options: ReconnectOptions = {});
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `options` | `ReconnectOptions` | `{}` | The five fields below, all optional. See [`ReconnectOptions`](#reconnectoptions-typescript). |
| `initialDelayMs` | `number` — milliseconds | `250` | The unjittered delay before the first retry, in milliseconds. |
| `factor` | `number` | `2` | The multiplier per failed attempt. |
| `maxDelayMs` | `number` — milliseconds | `30_000` | The ceiling on the unjittered delay, in milliseconds. Jitter is applied after the cap. |
| `jitter` | `number` | `0.3` | The dispersion as a fraction between 0 and 1. |
| `maxAttempts` | `number` | `Infinity` | How many consecutive failed attempts before giving up. `Infinity` is unlimited - Python spells the same thing `None`. |

### Return

A `Reconnect` instance whose five fields are `readonly`.

### Raises

- `Error` — `initialDelayMs` is not greater than zero, `factor` is below 1, or `jitter` is outside 0
  to 1. A plain `Error` and deliberately not a muxws error class: this is a programming mistake, not
  a protocol or connection failure.

### Example

```ts
import { Reconnect, unjitteredDelay } from 'muxws';

const schedule = new Reconnect({ initialDelayMs: 250, factor: 2, maxDelayMs: 30_000, jitter: 0.3 });
console.log([0, 1, 2, 3, 4, 5, 6, 7, 8].map((attempt) => unjitteredDelay(attempt, schedule)), 'milliseconds');

const capped = new Reconnect({ initialDelayMs: 1000, factor: 10, maxDelayMs: 5000 });
console.log([0, 1, 2, 3].map((attempt) => unjitteredDelay(attempt, capped)), 'milliseconds');

try {
  // eslint-disable-next-line no-new
  new Reconnect({ factor: 0.5 });
} catch (error) {
  console.log('rejected:', (error as Error).message);
}
console.log('unlimited by default:', new Reconnect().maxAttempts);
```

## `ReconnectOptions` (TypeScript)

The shape a caller may pass to `new Reconnect()`. Every field has a default, so `new Reconnect()` is
the whole API.

### Signature

```ts
export interface ReconnectOptions {
  initialDelayMs?: number;
  factor?: number;
  maxDelayMs?: number;
  jitter?: number;
  maxAttempts?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `initialDelayMs` | `number` — milliseconds | `250` | The unjittered delay before the first retry, in milliseconds. |
| `factor` | `number` | `2` | The multiplier per failed attempt. |
| `maxDelayMs` | `number` — milliseconds | `30_000` | The ceiling on the unjittered delay, in milliseconds. |
| `jitter` | `number` | `0.3` | The dispersion, a fraction between 0 and 1. |
| `maxAttempts` | `number` | `Infinity` | Consecutive failed attempts before giving up. |

### Return

None — `ReconnectOptions` is an interface, not a call.

### Raises

Raises: nothing. The values are validated by `Reconnect`'s constructor.

### Example

```ts
import { Reconnect, type ReconnectOptions } from 'muxws';

const patient: ReconnectOptions = { initialDelayMs: 500, maxDelayMs: 60_000, jitter: 0.5 };
const impatient: ReconnectOptions = { initialDelayMs: 50, maxAttempts: 3 };

console.log(new Reconnect(patient).maxDelayMs, 'milliseconds');
console.log(new Reconnect(impatient).maxAttempts, 'attempts before giving up');
console.log(new Reconnect({}).initialDelayMs, 'milliseconds by default');
```

## `Hello` (Python)

The opening payload replayed on every connection this peer ever makes.

`connect(hello=..., hello_headers=...)` builds one for you; it is exported so an application can
construct and inspect one. The point of the type is that the payload is **captured once, by value**:
it is deep-copied in at construction and deep-copied out again for every replay, so neither your own
later mutation nor anything the codec does to one send can change what the next connection sends.

The hello is an ordinary `open(payload, headers=..., end=True)`. Nothing marks it on the wire, and
the acceptor's ordinary `on_stream` handler sees it exactly as it sees any other stream. The
acknowledgement is that handler **returning**, which ends the stream implicitly - no application code
is required to send one.

A credential must not be carried here: authentication is a handshake concern.

### Signature

```python
@dataclass(frozen=True, slots=True)
class Hello:
    payload: Any = None
    headers: dict[str, Any] | None = None
    timeout: float = 10.0
    #: The capture. Never read by an application, never re-read from `payload`: the copy taken here
    #: is the only thing that ever reaches the wire.
    _captured_payload: Any = field(init=False, repr=False, compare=False, default=None)
    _captured_headers: dict[str, Any] | None = field(init=False, repr=False, compare=False, default=None)
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | `None` | The hello payload, deep-copied at construction. |
| `headers` | `dict[str, Any] \| None` | `None` | Headers for the hello's `open` frame, deep-copied at construction. |
| `timeout` | `float` — seconds | `10.0` | How long, in seconds, the hello may go unacknowledged before the attempt is failed. On the first connection that failure is what `connect()` raises; on a later one it is a failed attempt, and the counter climbs. |
| `_captured_payload` | `Any` | `None` | Not passed by a caller: `init=False`. The deep copy taken at construction, and the only thing that ever reaches the wire. |
| `_captured_headers` | `dict[str, Any] \| None` | `None` | The same for the headers. |

Three read-only properties come with it: `configured` (true when a payload or headers were given -
a peer given no hello sends none and is established as soon as its socket is), `payload_for_wire` and
`headers_for_wire`, each returning a **fresh** copy per read.

### Return

An immutable `Hello`.

### Raises

- Whatever `copy.deepcopy` raises for a value that cannot be copied - a `TypeError` for a payload
  holding something like a socket or a lock. It surfaces here, at construction, rather than as an
  unreplayable hello three hours into a reconnect storm.

### Example

```python
import asyncio

from muxws import Hello


async def main() -> None:
    identity = {"client": "docs", "rooms": ["general"]}
    hello = Hello(payload=identity, headers={"v": 1}, timeout=10.0)

    print(hello.configured, hello.timeout, "seconds")

    identity["rooms"].append("secret")
    print("captured by value:", hello.payload_for_wire)
    print("a fresh copy per replay:", hello.payload_for_wire is not hello.payload_for_wire)
    print("headers:", hello.headers_for_wire)
    print("no hello configured:", Hello().configured)


asyncio.run(main())
```

## `Hello` (TypeScript)

The same contract, with `structuredClone` where Python uses `copy.deepcopy`.

### Signature

```ts
export class Hello {
  readonly timeoutMs: number;

  constructor(options: HelloOptions = {});

  get configured(): boolean;
  payloadForWire(): unknown;
  headersForWire(): Record<string, unknown> | undefined;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `options` | `HelloOptions` | `{}` | `payload`, `headers` and `timeoutMs`. See [`HelloOptions`](#hellooptions-typescript). |

### Return

A `Hello`. `configured` is true when a payload or headers were given; `payloadForWire()` and
`headersForWire()` each return a **fresh** deep copy per call, so the object one connection handed to
the codec can never be the object the next connection sends.

### Raises

- `DataCloneError` — `structuredClone` refuses the payload or the headers: a function, a class
  instance with methods, a `Symbol`. It throws at construction, which is where the application can
  still see it.

### Example

```ts
import { Hello } from 'muxws';

const identity = { client: 'docs', rooms: ['general'] };
const hello = new Hello({ payload: identity, headers: { v: 1 }, timeoutMs: 10_000 });

console.log(hello.configured, hello.timeoutMs, 'milliseconds');

identity.rooms.push('secret');
console.log('captured by value:', hello.payloadForWire());
console.log('a fresh copy per replay:', hello.payloadForWire() !== hello.payloadForWire());
console.log('headers:', hello.headersForWire());
console.log('no hello configured:', new Hello().configured);

try {
  // eslint-disable-next-line no-new
  new Hello({ payload: () => undefined });
} catch (error) {
  console.log('rejected at construction:', (error as Error).name);
}
```

## `HelloOptions` (TypeScript)

### Signature

```ts
export interface HelloOptions {
  payload?: unknown;
  headers?: Record<string, unknown> | null;
  timeoutMs?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | `null` | The hello payload, deep-copied at construction. |
| `headers` | `Record<string, unknown> \| null` | `null` | Headers for the hello's `open` frame, deep-copied at construction. |
| `timeoutMs` | `number` — milliseconds | `DEFAULT_HELLO_TIMEOUT_MS` (10000) | How long, in milliseconds, the hello may go unacknowledged before the attempt fails. Python's `timeout=10.0` seconds. |

### Return

None — `HelloOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { Hello, type HelloOptions } from 'muxws';

const options: HelloOptions = { payload: { client: 'docs' }, headers: { v: 1 }, timeoutMs: 10_000 };
const none: HelloOptions = {};

console.log(new Hello(options).configured, options.timeoutMs, 'milliseconds');
console.log(new Hello(none).configured, new Hello(none).timeoutMs, 'milliseconds by default');
```

## `backoff_delay()` (Python)

The delay before retry number `attempts`, jittered. The helper calls it; it is exported so an
operator can answer "when will it try again" without reading the source.

### Signature

```python
def backoff_delay(attempts: int, options: Reconnect, draw: RandomDraw = _uniform) -> float:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `attempts` | `int` | required | How many consecutive attempts have already failed. `0` is the first retry after a loss. |
| `options` | `Reconnect` | required | The schedule to compute from. |
| `draw` | `RandomDraw` | `_uniform` | A callable returning a float in the range -1 to 1, the jitter draw. Injected so the schedule can be tested as the pure function it is rather than sampled and hoped about. The default uses `random`, deliberately and not `secrets`: jitter disperses a thundering herd, it does not resist an adversary. |

### Return

`float` — the delay in **seconds**, never negative: the jittered value is clamped at zero.

### Raises

- `ValueError` — `attempts` is negative.

### Example

```python
import asyncio

from muxws import backoff_delay, Reconnect


async def main() -> None:
    schedule = Reconnect(initial_delay=1.0, factor=2.0, max_delay=30.0, jitter=0.5)

    print("no jitter drawn:", [backoff_delay(n, schedule, draw=lambda: 0.0) for n in range(4)], "seconds")
    print("jitter at its lowest:", backoff_delay(0, schedule, draw=lambda: -1.0), "seconds")
    print("jitter at its highest:", backoff_delay(0, schedule, draw=lambda: 1.0), "seconds")
    print("the cap is jittered too:", backoff_delay(20, schedule, draw=lambda: -1.0), "seconds")

    try:
        backoff_delay(-1, schedule)
    except ValueError as exc:
        print("rejected:", exc)


asyncio.run(main())
```

## `backoffDelay()` (TypeScript)

### Signature

```ts
export function backoffDelay(attempts: number, options: Reconnect, draw: RandomDraw = uniform): number;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `attempts` | `number` | required | How many consecutive attempts have already failed. |
| `options` | `Reconnect` | required | The schedule to compute from. |
| `draw` | `RandomDraw` | `uniform` | A callable returning a number in the range -1 to 1. The default is built on `Math.random`, deliberately and not on `crypto.getRandomValues`. |

### Return

`number` — the delay in **milliseconds**, clamped at zero.

### Raises

- `Error` — `attempts` is negative.

### Example

```ts
import { backoffDelay, Reconnect } from 'muxws';

const schedule = new Reconnect({ initialDelayMs: 1000, factor: 2, maxDelayMs: 30_000, jitter: 0.5 });

console.log('no jitter drawn:', [0, 1, 2, 3].map((n) => backoffDelay(n, schedule, () => 0)), 'milliseconds');
console.log('jitter at its lowest:', backoffDelay(0, schedule, () => -1), 'milliseconds');
console.log('jitter at its highest:', backoffDelay(0, schedule, () => 1), 'milliseconds');
console.log('the cap is jittered too:', backoffDelay(20, schedule, () => -1), 'milliseconds');

try {
  backoffDelay(-1, schedule);
} catch (error) {
  console.log('rejected:', (error as Error).message);
}
```

## `unjittered_delay()` (Python)

The schedule before jitter and after the cap: the half that is exactly predictable.

### Signature

```python
def unjittered_delay(attempts: int, options: Reconnect) -> float:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `attempts` | `int` | required | How many consecutive attempts have already failed. |
| `options` | `Reconnect` | required | The schedule to compute from. |

### Return

`float` — `min(initial_delay * factor ** attempts, max_delay)`, in **seconds**.

### Raises

Raises: nothing. Unlike `backoff_delay`, it does not check `attempts`: a negative value simply produces a
delay smaller than `initial_delay`.

### Example

```python
import asyncio

from muxws import Reconnect, unjittered_delay


async def main() -> None:
    schedule = Reconnect(initial_delay=0.25, factor=2.0, max_delay=30.0)
    print([unjittered_delay(attempt, schedule) for attempt in range(10)], "seconds")
    print("the cap holds:", unjittered_delay(50, schedule), "seconds")


asyncio.run(main())
```

## `unjitteredDelay()` (TypeScript)

### Signature

```ts
export function unjitteredDelay(attempts: number, options: Reconnect): number;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `attempts` | `number` | required | How many consecutive attempts have already failed. |
| `options` | `Reconnect` | required | The schedule to compute from. |

### Return

`number` — `min(initialDelayMs * factor ** attempts, maxDelayMs)`, in **milliseconds**.

### Raises

Raises: nothing.

### Example

```ts
import { Reconnect, unjitteredDelay } from 'muxws';

const schedule = new Reconnect({ initialDelayMs: 250, factor: 2, maxDelayMs: 30_000 });
console.log([0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((attempt) => unjitteredDelay(attempt, schedule)), 'milliseconds');
console.log('the cap holds:', unjitteredDelay(50, schedule), 'milliseconds');
```

## `should_retry()` (Python)

Whether another attempt is allowed. The helper asks this, and only this, before every dial - never a
mutable field on the peer, so the cap is enforced by the schedule's own arithmetic.

### Signature

```python
def should_retry(attempts: int, options: Reconnect) -> bool:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `attempts` | `int` | required | How many consecutive attempts have already failed. |
| `options` | `Reconnect` | required | The schedule holding `max_attempts`. |

### Return

`bool` — `True` while `max_attempts` is `None` or `attempts` is below it. When it turns `False` the
helper fires `on_close` once with `will_retry` false and never dials again.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Reconnect, should_retry


async def main() -> None:
    limited = Reconnect(max_attempts=3)
    print([should_retry(attempt, limited) for attempt in range(5)])

    print("unlimited by default:", should_retry(1_000_000, Reconnect()))
    print("max_attempts=0 never dials:", should_retry(0, Reconnect(max_attempts=0)))


asyncio.run(main())
```

## `shouldRetry()` (TypeScript)

### Signature

```ts
export function shouldRetry(attempts: number, options: Reconnect): boolean;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `attempts` | `number` | required | How many consecutive attempts have already failed. |
| `options` | `Reconnect` | required | The schedule holding `maxAttempts`. |

### Return

`boolean` — `attempts < options.maxAttempts`, which is always true for the default `Infinity`.

### Raises

Raises: nothing.

### Example

```ts
import { Reconnect, shouldRetry } from 'muxws';

const limited = new Reconnect({ maxAttempts: 3 });
console.log([0, 1, 2, 3, 4].map((attempt) => shouldRetry(attempt, limited)));

console.log('unlimited by default:', shouldRetry(1_000_000, new Reconnect()));
console.log('maxAttempts 0 never dials:', shouldRetry(0, new Reconnect({ maxAttempts: 0 })));
```

## See also

- [`connect()`](./connect.md) — where a `Reconnect` and a hello are handed over, and why a failed
  first attempt raises.
- [`Peer`](./peer.md) — `on_reconnect`, `on_close` and `peer.id`, which changes on every new socket.
- [Errors](./errors.md) — `StreamTimeout` and `StreamRefused`, the two ways a hello fails.
