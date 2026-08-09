# Tags and the peer registry

Server push needs an answer to one question: *which connections should get this?* muxws answers it
with two pieces — a bag of application-chosen labels on each peer, and an index over those labels.

The design rests on one distinction: **the registry indexes, it does not watch.** Nothing observes
your tags. Writing one is a plain assignment that costs nothing and triggers nothing, and the index
moves only when you say so.

## `peer.tags`

An ordinary `dict` in Python, an ordinary object in TypeScript. muxws never reads it, never writes it,
and defines no key of its own in it.

```python
# fragment
peer.tags["user"] = 42
peer.tags["tenant"] = "acme"
peer.tags["topics"] = {"orders", "shipments"}   # fine — muxws does not care what you store
```

```ts
// fragment
peer.tags.user = 42;
peer.tags.tenant = 'acme';
```

Because it is a plain container, it has plain container semantics: no change notification, no
validation, no ordering guarantee, no persistence.

**It dies with its connection.** On the acceptor side — which is where tags are almost always used — a
lost socket is the end of that `Peer` object, and a reconnecting client is a **brand-new `Peer` with
an empty `tags`**. The acceptor never learns that the socket it just accepted belongs to the client
that was here a moment ago, so there is nowhere for the old tags to come from. That is not a
limitation to work around; it is what keeps a tab that muted something and then died from leaving a
successor muted that never asked to be.

The consequence for your code: **whatever the acceptor indexes, it must index again on every
connection.** The hello handler is where that happens — see [Reconnect](/guide/reconnect#the-hello).

(A dialer's `Peer` object does survive a reconnect, and the library neither clears nor reads its
`tags`. A dialer rarely has any.)

## `PeerRegistry`

```python
from muxws import PeerRegistry

registry = PeerRegistry()
```

Three members do the work.

### `register(peer)`

Indexes the peer under **every key its `tags` holds at this moment**. The registry has no notion of
which keys matter, so it takes all of them.

```python
# fragment
peer.tags["user"] = 42
peer.tags["tenant"] = "acme"
registry.register(peer)          # findable by user=42 and by tenant="acme"
```

Re-registering **replaces** a peer's previous entries wholesale rather than adding to them: after
`register`, the peer is found under its new values and no longer under its old ones.

```python
# fragment
peer.tags["tenant"] = "globex"
registry.register(peer)          # findable by tenant="globex"; no longer by tenant="acme"
```

A value that cannot serve as a lookup key is passed over rather than rejected — an unhashable value in
Python, anything other than a string, number, boolean, bigint or symbol in TypeScript. The peer is
simply not findable by that key. Raising instead would turn an ordinary tag write into an error, and a
dict-valued tag would become illegal for no benefit.

**Removal on close is automatic.** `register` installs one close hook per peer, so a socket loss
prunes the index by itself. You never have to remember to deregister, and the hook is installed once
however many times you re-register.

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

It returns a **list in a stable order** (sorted by `peer.id`), never a set: callers iterate it, and an
order that varied between runs for no visible reason is a debugging cost with no upside. Called with
no arguments it returns every registered peer.

Treat the result as a **snapshot**. A peer in it may already be closing by the time you reach it, so a
send may raise `ConnectionLost`; that is an ordinary race, not a registry bug.

### `registered(peer)`

`register` plus an explicit deregister, for a consumer that wants the scope visible in the code.

```python
# fragment
async def endpoint(websocket):
    peer = await accept(websocket)
    peer.tags["tenant"] = tenant_of(websocket)
    with registry.registered(peer):
        await peer.serve()
```

TypeScript has no `with`, and the package's compile target has no `using` either, so the scope is a
callback. The deregister still runs when the body throws.

```ts
// fragment
const peer = await accept(socket);
peer.tags.tenant = tenantOf(request);
await registry.registered(peer, async () => {
  await peer.serve();
});
```

`deregister(peer)` is public and idempotent if you want the two halves separately.

## Reaching the peer from a handler

`on_stream` is handed `(payload, stream)` and there is no public route from a `Stream` back to its
`Peer` in either language. That is deliberate — a handler's business is its stream — but tagging,
registering and pushing all need the peer, so the pattern is to **bind the handler to the peer when
you accept the connection**:

```python
# fragment
async def endpoint(websocket):
    peer = await muxws.accept(websocket)

    async def handle(payload, stream):
        # `peer` is in scope because this closure was built for this connection, and it is the only
        # way the handler can reach it.
        if payload.get("action") == "subscribe":
            peer.tags.update(payload["tags"])
            registry.register(peer)

    peer.on_stream(handle)
    await peer.serve()
```

One handler per connection rather than one per process, which is what makes `peer.tags` usable at all.
If you find yourself wanting a module-level handler, you will find yourself wanting a way back to the
peer, and there is not one.

## The usage rule

**Look up on keys you do not mutate, and mutate keys you do not look up.**

That is the whole contract, and it falls straight out of "the registry indexes, it does not watch". A
write to `peer.tags` does not move the index. So a key you *look up on* must be settled before you
call `register` and left alone afterwards, and a key you *mutate freely* is one nobody searches by.

```python
# fragment
# Look up on these: written once, then never touched again.
peer.tags["tenant"] = "acme"
peer.tags["user"] = 42
registry.register(peer)

# Mutate these freely: nothing ever calls peers_for() on them.
peer.tags["last_seen"] = time.time()
peer.tags["frames"] = peer.tags.get("frames", 0) + 1
```

**If you need both on one key, call `register(peer)` after every write to it.**

```python
# fragment
peer.tags["room"] = "lobby"
registry.register(peer)
...
peer.tags["room"] = "game-7"
registry.register(peer)      # required, or peers_for(room="game-7") misses this peer
```

This costs one pass over the peer's tags and is safe to call as often as you like — the close hook is
not duplicated, and the old entries are replaced rather than accumulated.

The failure mode of forgetting is quiet, which is why the rule is stated as a rule: nothing raises,
nothing logs, and the peer is simply absent from a broadcast it should have received. The index still
holds the value it was told about, which is now a value nobody has.

## One process

`PeerRegistry` is **per process**. It is an in-memory index over the peers this process is holding
sockets for, and muxws ships no cross-process backplane — no Redis adapter, no pub/sub bridge, no
sticky-routing helper.

That is a deliberate boundary rather than an omission. A multi-process deployment already has to
decide where its fan-out lives, and that decision belongs to the deployment: which broker, what
delivery guarantee, what happens to a message published while a client is between reconnections, and
whether a client's connection is pinned to one process at all. Any answer muxws picked would be wrong
for most deployments and impossible to remove from a frozen surface.

What such a deployment does itself: publish the event on whatever bus it already runs, have every
process subscribe, and in each subscriber use its **local** `PeerRegistry` to find the peers *it*
holds and push to those. Each process fans out to its own sockets and to nobody else's; the bus
handles the crossing. The registry's job stops at the process boundary, and its answer to
`peers_for(...)` is always "the matching peers connected *here*".

## See also

- [`api/registry`](/api/registry) — `PeerRegistry.register`, `.registered`, `.peers_for`
- [`api/peer`](/api/peer) — `peer.tags`, `peer.id`, `peer.notify`, `peer.open`, `peer.on_close`
- [`api/accept`](/api/accept) — where an acceptor gets the peer it tags
- [`api/errors`](/api/errors) — `ConnectionLost`, which a snapshot can hand you
