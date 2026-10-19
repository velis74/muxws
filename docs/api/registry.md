---
outline: deep
---

# Registry

`PeerRegistry` returns the connected peers whose tags match given values. A server uses it to find
the peers to push to, for example every peer for user 42 or every peer subscribed to document 7.

**`peer.tags` is an ordinary dict / plain object.** muxws does not read, validate or observe it, and
writing a tag updates no index. On the acceptor, a reconnect produces a new `Peer` whose `tags` are
empty. A dialer's `Peer` survives a reconnect and keeps its `tags`.

**The registry is updated only by `register` and `deregister`.** A tag write after `register` is not
reflected in the index until the next `register(peer)`. Call `register(peer)` again after each write
to a key that is used for lookup.

A peer is removed from the index automatically when its connection closes.

The registry is **per-process**. muxws ships no cross-process backplane. A multi-process deployment
that needs "every peer for user 42 across all processes" implements it in the application, typically
as a pub/sub channel that every process subscribes to; each process looks up its own `PeerRegistry`
and pushes to the sockets it holds.

## `PeerRegistry`

### Signature

```python
class PeerRegistry:
    def __init__(self) -> None: ...
```

```ts
export class PeerRegistry {
  get size(): number;
  get indexSize(): number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | The constructor takes no arguments. Construct one per process, or one per logical grouping if you want separate indexes. |

### Return

An empty registry.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
print(len(registry), registry.index_size)

_, socket = memory_pair()
peer = Peer(socket, codec=JsonCodec(), is_dialer=False)
peer.tags["user"] = 42
registry.register(peer)
print(len(registry), registry.index_size, [found.id for found in registry.peers_for(user=42)])
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

const registry = new PeerRegistry();
console.log(registry.size, registry.indexSize);

const [, socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
peer.tags.user = 42;
registry.register(peer);
console.log(registry.size, registry.indexSize, registry.peersFor({ user: 42 }).map((found) => found.id));
```

## `PeerRegistry.register`

Indexes a peer under **every key its `tags` holds at the time of the call**.

Re-registering **replaces** the peer's previous entries: after the call the peer is found under its
new values and no longer under the old ones.

A tag value that cannot serve as a lookup key is skipped without an error: a dict or a list in
Python (unhashable), an object, an array, `null` or `undefined` in TypeScript (objects and arrays
would be keyed by identity). The peer is not findable by that key.

Removal on close is automatic. The close hook is installed the first time a peer is registered and
not on later calls, so re-registering a peer on every write adds no further close handlers. The hook
stays installed across a reconnect and removes the peer on every socket loss; a peer registered again
on its next connection keeps the one hook.

### Signature

```python
def register(self, peer: Peer) -> None: ...
```

```ts
register(peer: Peer): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `peer` | `Peer` | required | The peer to index. Its `tags` are read once, at this instant. |

### Return

`None` / `void`.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
_, socket = memory_pair()
peer = Peer(socket, codec=JsonCodec(), is_dialer=False)

peer.tags["room"] = "lobby"
peer.tags["roles"] = ["admin"]  # a list cannot be a lookup key; skipped without an error
registry.register(peer)
print("lobby:", len(registry.peers_for(room="lobby")), "| roles indexed:", registry.index_size)

peer.tags["room"] = "kitchen"
registry.register(peer)  # re-index after writing a looked-up key
print("lobby:", len(registry.peers_for(room="lobby")), "kitchen:", len(registry.peers_for(room="kitchen")))
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

const registry = new PeerRegistry();
const [, socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });

peer.tags.room = 'lobby';
peer.tags.roles = ['admin']; // an array cannot be a lookup key; skipped without an error
registry.register(peer);
console.log('lobby:', registry.peersFor({ room: 'lobby' }).length, '| indexed keys:', registry.indexSize);

peer.tags.room = 'kitchen';
registry.register(peer); // re-index after writing a looked-up key
console.log('lobby:', registry.peersFor({ room: 'lobby' }).length);
console.log('kitchen:', registry.peersFor({ room: 'kitchen' }).length);
```

## `PeerRegistry.deregister`

Forgets a peer entirely. Idempotent: deregistering a peer that was never registered, or one that was
already removed when its connection closed, does nothing.

### Signature

```python
def deregister(self, peer: Peer) -> None: ...
```

```ts
deregister(peer: Peer): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `peer` | `Peer` | required | The peer to forget. Its index entries are removed and empty buckets are dropped with them. |

### Return

`None` / `void`.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
_, socket = memory_pair()
peer = Peer(socket, codec=JsonCodec(), is_dialer=False)
peer.tags["user"] = 42
registry.register(peer)

registry.deregister(peer)
print(len(registry), registry.index_size, registry.peers_for(user=42))
registry.deregister(peer)  # idempotent
print("still empty:", len(registry))
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

const registry = new PeerRegistry();
const [, socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
peer.tags.user = 42;
registry.register(peer);

registry.deregister(peer);
console.log(registry.size, registry.indexSize, registry.peersFor({ user: 42 }));
registry.deregister(peer); // idempotent
console.log('still empty:', registry.size);
```

## `PeerRegistry.registered`

`register` on entry and `deregister` on exit of an explicit scope, independent of the connection's
lifetime. The deregister runs even when the body raises.

In Python it is a context manager, used with `with`. In TypeScript it takes the body as a callback
and is `async`; the package targets ES2020, which has no `Symbol.dispose` for a `using` declaration.

### Signature

```python
@contextmanager
def registered(self, peer: Peer) -> Iterator[Peer]: ...
```

```ts
async registered<T>(peer: Peer, body: (peer: Peer) => T | Promise<T>): Promise<T>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `peer` | `Peer` | required | The peer to register for the duration of the scope. |
| `body` (TypeScript only) | `(peer: Peer) => T \| Promise<T>` | required | The scope. Awaited if it returns a promise; the deregister runs in a `finally`, so it happens whether the body returns or throws. |

### Return

Python: a context manager yielding the same `peer`. TypeScript: a `Promise<T>` resolving to whatever
`body` returned.

### Raises

Raises: nothing of its own. Whatever the body raises propagates unchanged, after the deregister has
run.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
_, socket = memory_pair()
peer = Peer(socket, codec=JsonCodec(), is_dialer=False)
peer.tags["job"] = "import"

with registry.registered(peer) as scoped:
    print("inside: ", len(registry), [found.id for found in registry.peers_for(job="import")] == [scoped.id])
print("outside:", len(registry))

try:
    with registry.registered(peer):
        raise RuntimeError("the job failed")
except RuntimeError:
    print("after a failure:", len(registry))
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const registry = new PeerRegistry();
  const [, socket] = memoryPair();
  const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
  peer.tags.job = 'import';

  const found = await registry.registered(peer, (scoped) => registry.peersFor({ job: 'import' })[0] === scoped);
  console.log('inside: ', found);
  console.log('outside:', registry.size);

  try {
    await registry.registered(peer, () => {
      throw new Error('the job failed');
    });
  } catch {
    console.log('after a failure:', registry.size);
  }
}

void main();
```

## `PeerRegistry.peers_for` / `peersFor`

Every live peer whose `tags` match **all** the given keys. With no keys at all it returns every
registered peer.

The result is a **list / array sorted by `peer.id`**, not a set, so iteration order is the same
on every run.

The result is a snapshot. A peer in it may already be closing when the application sends to it; a
send to such a peer raises `ConnectionLost`.

### Signature

```python
def peers_for(self, **tags: Any) -> list[Peer]: ...
```

```ts
peersFor(tags: Record<string, unknown> = {}): Peer[];
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `**tags` (Python) | `Any` | none | Keyword arguments, one per key to match. All must match. A value that could never have been indexed matches nothing. A tag key that is not a valid Python identifier is still reachable — unpack a dict: `peers_for(**{"device-id": "a1"})`. |
| `tags` (TypeScript) | `Record<string, unknown>` | `{}` | One object, one property per key to match. All must match. A value that could never have been indexed matches nothing. |

### Return

`list[Peer]` / `Peer[]`, sorted by `peer.id`. Empty when nothing matches.

### Raises

Raises: nothing. An unhashable Python value or a non-primitive TypeScript value yields an empty
result, matching `register`, which skips such values.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
codec = JsonCodec()
peers = []
for user, room in ((42, "lobby"), (42, "kitchen"), (7, "lobby")):
    _, socket = memory_pair()
    peer = Peer(socket, codec=codec, is_dialer=False)
    peer.tags.update(user=user, room=room)
    registry.register(peer)
    peers.append(peer)

print("user 42:      ", len(registry.peers_for(user=42)))
print("user 42+lobby:", len(registry.peers_for(user=42, room="lobby")))
print("everything:   ", len(registry.peers_for()))
print("no match:     ", registry.peers_for(user=99))
print("stable order: ", [found.id for found in registry.peers_for()] == sorted(peer.id for peer in peers))
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

const registry = new PeerRegistry();
const codec = new JsonCodec();
const peers = ([[42, 'lobby'], [42, 'kitchen'], [7, 'lobby']] as [number, string][]).map(([user, room]) => {
  const [, socket] = memoryPair();
  const peer = new Peer(socket, { codec, isDialer: false });
  peer.tags.user = user;
  peer.tags.room = room;
  registry.register(peer);
  return peer;
});

console.log('user 42:      ', registry.peersFor({ user: 42 }).length);
console.log('user 42+lobby:', registry.peersFor({ user: 42, room: 'lobby' }).length);
console.log('everything:   ', registry.peersFor().length);
console.log('no match:     ', registry.peersFor({ user: 99 }));
const ids = registry.peersFor().map((found) => found.id);
console.log('stable order: ', ids.join() === peers.map((peer) => peer.id).sort().join());
```

## `len(registry)` / `PeerRegistry.size`

How many peers are registered. Python spells it `len(registry)`; TypeScript spells it
`registry.size`.

### Signature

```python
def __len__(self) -> int: ...
```

```ts
get size(): number;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | Takes no arguments. |

### Return

`int` / `number`: the count of distinct registered peers, regardless of how many tags each carries.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
_, socket = memory_pair()
peer = Peer(socket, codec=JsonCodec(), is_dialer=False)
peer.tags.update(user=42, room="lobby", device="phone")
registry.register(peer)
print(len(registry), "peer with", len(peer.tags), "tags")
registry.register(peer)
print("re-registering does not double it:", len(registry))
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

const registry = new PeerRegistry();
const [, socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
peer.tags.user = 42;
peer.tags.room = 'lobby';
peer.tags.device = 'phone';
registry.register(peer);
console.log(registry.size, 'peer with', Object.keys(peer.tags).length, 'tags');
registry.register(peer);
console.log('re-registering does not double it:', registry.size);
```

## `PeerRegistry.index_size` / `indexSize`

How many `(key, value)` buckets the index holds. It is a diagnostic. `register` replaces a peer's
entries and drops buckets that become empty, so `index_size` equals the number of distinct
`(key, value)` pairs held by registered peers and does not grow when a peer is re-registered under
new values.

### Signature

```python
@property
def index_size(self) -> int: ...
```

```ts
get indexSize(): number;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A read-only property; takes no arguments. |

### Return

`int` / `number`: the number of distinct `(key, value)` pairs currently indexed. Values that could
not serve as lookup keys are not counted, because they were never indexed.

### Raises

Raises: nothing.

### Example

```python
from muxws import JsonCodec, Peer, PeerRegistry
from muxws.transports.memory import memory_pair

registry = PeerRegistry()
_, socket = memory_pair()
peer = Peer(socket, codec=JsonCodec(), is_dialer=False)
peer.tags.update(user=42, room="lobby")
registry.register(peer)
print("two indexable tags:", registry.index_size)

for room in ("kitchen", "garden", "attic"):
    peer.tags["room"] = room
    registry.register(peer)
print("still two after three rewrites:", registry.index_size)
```

```ts
import { JsonCodec, Peer, PeerRegistry, memoryPair } from 'muxws';

const registry = new PeerRegistry();
const [, socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
peer.tags.user = 42;
peer.tags.room = 'lobby';
registry.register(peer);
console.log('two indexable tags:', registry.indexSize);

['kitchen', 'garden', 'attic'].forEach((room) => {
  peer.tags.room = room;
  registry.register(peer);
});
console.log('still two after three rewrites:', registry.indexSize);
```

## `TagValue`

TypeScript only. What may serve as a lookup value: the five primitive `typeof` results. Anything else
— an object, an array, `null`, `undefined` — is skipped by `register` and matches nothing in
`peersFor`.

Python has no equivalent type: any value that `hash()` accepts is indexable, including a tuple.

### Signature

```ts
export type TagValue = string | number | boolean | bigint | symbol;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | A type alias, not a callable. |

### Return

Nothing — it is a type. It exists so an application can type its own tag helpers against the same
set the registry indexes.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, Peer, PeerRegistry, type TagValue, memoryPair } from 'muxws';

const registry = new PeerRegistry();
const [, socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });

/** Accepts only values the registry indexes. */
function tag(target: Peer, key: string, value: TagValue): void {
  target.tags[key] = value;
  registry.register(target);
}

tag(peer, 'user', 42);
tag(peer, 'beta', true);
console.log(registry.indexSize, registry.peersFor({ user: 42, beta: true }).length);
```

## See also

- [Peer](./peer.md) — `peer.tags` and `peer.id`, the two things the registry reads.
- [Errors](./errors.md) — `ConnectionLost`, what a send to a peer from a stale snapshot raises.
- [Guide: registry](../guide/registry.md) — the usage rule and its rationale.
