# Tags and the peer registry

Server push requires selecting the connections that receive a message. muxws provides two parts for
this: a set of application-defined labels on each peer (`peer.tags`), and an index over those labels
(`PeerRegistry`).

**The registry indexes tags; it does not observe them.** Writing a tag is a plain assignment with no
side effects, and the index changes only when the application calls `register` or `deregister`, or
when a registered peer closes.

## `peer.tags`

A plain `dict` in Python, a plain object in TypeScript. muxws does not read or write it (except that
`PeerRegistry.register` reads it), and defines no keys in it.

```python
# fragment
peer.tags["user"] = 42
peer.tags["tenant"] = "acme"
peer.tags["topics"] = {"orders", "shipments"}   # any value type is allowed
```

```ts
// fragment
peer.tags.user = 42;
peer.tags.tenant = 'acme';
```

It has the semantics of a plain container: no change notification, no validation, no ordering
guarantee, no persistence.

**Tags end with their connection.** On the acceptor side, where tags are normally used, a lost socket
ends that `Peer` object, and a reconnecting client is a **new `Peer` with empty `tags`**. The acceptor
does not know that the new socket belongs to a client it served before, so the old tags are not
carried over. State set by a previous connection (for example, a muted topic) therefore does not apply
to the next one.

**The acceptor must index each connection again.** Do this in the hello handler; see
[Reconnect](/guide/reconnect#the-hello).

(A dialer's `Peer` object remains across a reconnect, and the library neither clears nor reads its
`tags`.)

## `PeerRegistry`

```python
from muxws import PeerRegistry

registry = PeerRegistry()
```

The registry has three main members, plus `deregister`.

### `register(peer)`

Indexes the peer under **every key its `tags` holds at the time of the call**.

```python
# fragment
peer.tags["user"] = 42
peer.tags["tenant"] = "acme"
registry.register(peer)          # findable by user=42 and by tenant="acme"
```

Registering again **replaces** all of the peer's previous entries: after `register`, the peer is
found under its current values and no longer under its previous ones.

```python
# fragment
peer.tags["tenant"] = "globex"
registry.register(peer)          # findable by tenant="globex"; no longer by tenant="acme"
```

A value that cannot be a lookup key is skipped without an error: an unhashable value in Python, or
any value other than a string, number, boolean, bigint or symbol in TypeScript. The peer cannot be
found by that key. A dict-valued tag is therefore allowed; it is not indexed.

**Removal on close is automatic.** `register` installs one close hook per peer, so a socket loss
removes the peer from the index without an explicit `deregister`. The hook is installed once, however
many times the peer is registered.

### `peers_for(**tags)` / `peersFor(tags)`

Every live peer whose `tags` match **all** the given keys.

```python
# fragment
for peer in registry.peers_for(tenant="acme", role="admin"):
    await peer.notify({"kind": "maintenance"})
```

```ts
// fragment
for (const peer of registry.peersFor({ tenant: 'acme', role: 'admin' })) {
  await peer.notify({ kind: 'maintenance' });
}
```

It returns a **list in a stable order** (sorted by `peer.id`), not a set, so iteration order is the
same between runs. Called with no arguments, it returns every registered peer.

The result is a **snapshot**. A peer in it may be closing by the time it is used, so a send may raise
`ConnectionLost`; this is an expected race.

### `registered(peer)`

`register` with a `deregister` at the end of a scope, so the registration scope is explicit in the
code.

```python
# fragment
async def endpoint(websocket):
    peer = await accept(websocket)
    peer.tags["tenant"] = tenant_of(websocket)
    with registry.registered(peer):
        await peer.serve()
```

TypeScript has no `with`, and the package's compile target (ES2020) does not support `using`, so the
scope is a callback. `deregister` runs also when the callback throws.

```ts
// fragment
const peer = await accept(socket);
peer.tags.tenant = tenantOf(request);
await registry.registered(peer, async () => {
  await peer.serve();
});
```

`deregister(peer)` is public and idempotent, for use without `registered`.

## Reaching the peer from a handler

`on_stream` receives `(payload, stream)`, and neither language has a public reference from a `Stream`
to its `Peer`. Tagging, registering and pushing need the peer, so **define the handler as a closure
over the peer when the connection is accepted**:

```python
# fragment
async def endpoint(websocket):
    peer = await muxws.accept(websocket)

    async def handle(payload, stream):
        # `peer` is in scope because this closure is defined per connection; a handler has no other
        # reference to its peer.
        if payload.get("action") == "subscribe":
            peer.tags.update(payload["tags"])
            registry.register(peer)

    peer.on_stream(handle)
    await peer.serve()
```

Define one handler per connection, not one per process. A module-level handler has no access to the
peer and therefore to `peer.tags`.

## The usage rule

A write to `peer.tags` does not update the index. A key used for lookup must therefore be set before
`register` and not changed afterwards, and a key that changes must not be used for lookup.

```python
# fragment
# Keys used for lookup: written before register() and not changed.
peer.tags["tenant"] = "acme"
peer.tags["user"] = 42
registry.register(peer)

# Keys that change: never used for lookup.
peer.tags["last_seen"] = time.time()
peer.tags["frames"] = peer.tags.get("frames", 0) + 1
```

**A key that is used for lookup and also changes needs `register(peer)` after every write to it.**

```python
# fragment
peer.tags["room"] = "lobby"
registry.register(peer)
...
peer.tags["room"] = "game-7"
registry.register(peer)      # required, or peers_for(room="game-7") misses this peer
```

Each call iterates over the peer's tags once. It can be called any number of times: the close hook is
not duplicated, and the old entries are replaced.

If `register` is not called after such a write, no error is raised and nothing is logged: the index
keeps the previous value, and the peer is missing from lookups by the new value.

## One process

`PeerRegistry` is **per process**: an in-memory index over the peers whose sockets this process
holds. Cross-process fan-out depends on deployment choices (the broker, the delivery guarantee, the
handling of a message published while a client is reconnecting, whether a client is pinned to one
process), and muxws ships no cross-process backplane.

A multi-process deployment publishes the event on its message bus, has every process subscribe, and in
each subscriber uses the **local** `PeerRegistry` to find the matching peers that process holds and
push to them. `peers_for(...)` returns only the matching peers connected to the current process.

## See also

- [`api/registry`](/api/registry) — `PeerRegistry.register`, `.registered`, `.peers_for`
- [`api/peer`](/api/peer) — `peer.tags`, `peer.id`, `peer.notify`, `peer.open`, `peer.on_close`
- [`api/accept`](/api/accept) — where an acceptor gets the peer it tags
- [`api/errors`](/api/errors) — `ConnectionLost`, which a snapshot can hand you
