/**
 * `peer.tags` and `PeerRegistry`: finding a peer by keys the application chose (§9.5).
 *
 * A direct port of `muxws/registry.py`, member for member. The registry indexes; it does not watch.
 * That distinction is the whole design, and WSM-REG-013 and WSM-REG-014 exist to make a plain object
 * provably the right choice for `tags`: writing a tag is free and costs no bookkeeping, and the index
 * only moves when someone says so.
 *
 * The documented usage rule follows from that: **look up on keys you do not mutate, and mutate keys
 * you do not look up.** A consumer needing both on one key calls `register(peer)` after each write.
 */

import type { Peer } from './peer';

/**
 * What may serve as a lookup value. Python asks `hash()` and catches `TypeError`; JavaScript has no
 * such question to ask, so the five primitive `typeof` results are the enumeration of it.
 *
 * An object or an array is passed over rather than rejected (WSM-REG-011): `Map` keys them by
 * identity, so indexing one would make the peer findable only by a caller already holding the very
 * same reference - a lookup nobody can perform, bought at the price of an entry that never matches.
 */
export type TagValue = string | number | boolean | bigint | symbol;

const INDEXABLE: ReadonlySet<string> = new Set(['string', 'number', 'boolean', 'bigint', 'symbol']);

function isIndexable(value: unknown): value is TagValue {
  return INDEXABLE.has(typeof value);
}

/** Per-process. muxws does not ship a cross-process backplane (WSM-REG-018). */
export class PeerRegistry {
  /**
   * Tag name -> tag value -> the peers registered under it.
   *
   * Two levels rather than one string key: `('a', 1)` and `('a1', undefined)` concatenate to the same
   * characters, and a registry that confused them would answer `peersFor({ a: 1 })` with a peer that
   * never carried that tag. A value that is not indexable never reaches here at all, so a
   * object-valued tag costs nothing whatever.
   */
  private readonly index = new Map<string, Map<TagValue, Set<Peer>>>();

  /**
   * The reverse map, so `register` can replace a peer's entries **wholesale** in one pass
   * (WSM-REG-012) rather than scanning the index for it.
   */
  private readonly entries = new Map<Peer, [string, TagValue][]>();

  /**
   * Peers whose close hook is already installed.
   *
   * WSM-REG-017 tells a consumer that both looks up and mutates one key to call `register(peer)`
   * after **every** write, so a hook appended per call would leave a long-lived peer carrying one
   * handler per write, all of them doing the same already-idempotent deregistration - bookkeeping
   * WSM-REG-001 says a rewrite must not cost. `Peer` has no way to remove a handler, so the only
   * place to be careful is here. Weak, so a registry never keeps a peer alive that nothing else
   * holds.
   */
  private readonly hooked = new WeakSet<Peer>();

  /** How many peers are registered. Python spells it `len(registry)`. */
  get size(): number {
    return this.entries.size;
  }

  /** How many (key, value) buckets exist. WSM-REG-014 asserts this does not grow. */
  get indexSize(): number {
    let total = 0;
    this.index.forEach((byValue) => {
      total += byValue.size;
    });
    return total;
  }

  /**
   * Index `peer` under **every key its `tags` holds at this moment** (WSM-REG-010).
   *
   * It has no notion of which keys matter, and re-registering replaces the previous entries rather
   * than adding to them: found under the new values, no longer under the old ones.
   *
   * Removal on close is automatic (WSM-REG-016) - a consumer never has to prune the index.
   */
  register(peer: Peer): void {
    this.deregister(peer);

    const entries: [string, TagValue][] = [];
    // `Object.entries`, never `for...in`: the latter walks the prototype chain and is banned by the
    // shared eslint config for exactly that reason.
    Object.entries(peer.tags).forEach(([key, value]) => {
      if (!isIndexable(value)) return;
      entries.push([key, value]);
      let byValue = this.index.get(key);
      if (byValue === undefined) {
        byValue = new Map<TagValue, Set<Peer>>();
        this.index.set(key, byValue);
      }
      let holders = byValue.get(value);
      if (holders === undefined) {
        holders = new Set<Peer>();
        byValue.set(value, holders);
      }
      holders.add(peer);
    });

    this.entries.set(peer, entries);
    if (this.hooked.has(peer)) return;
    this.hooked.add(peer);
    peer.onClose(() => {
      // Forgets the hook as well as the entries, so a `Peer` that survives a reconnect
      // (WSM-RCN-032) and is registered again on its next connection gets a fresh hook rather than
      // spending the rest of its life without one.
      this.hooked.delete(peer);
      this.deregister(peer);
    });
  }

  /** Forget `peer` entirely. Idempotent. */
  deregister(peer: Peer): void {
    const entries = this.entries.get(peer);
    if (entries === undefined) return;
    this.entries.delete(peer);
    entries.forEach(([key, value]) => {
      const byValue = this.index.get(key);
      const holders = byValue?.get(value);
      if (byValue === undefined || holders === undefined) return;
      holders.delete(peer);
      if (holders.size === 0) byValue.delete(value);
      if (byValue.size === 0) this.index.delete(key);
    });
  }

  /**
   * `register` plus an explicit deregister, for a consumer that wants the scope visible.
   *
   * Python has `with registry.registered(peer):`. TypeScript's `using` needs a `Symbol.dispose` this
   * package's ES2020 target does not carry, so the scope is a callback: the shape survives, and the
   * deregister still runs when the body throws.
   */
  async registered<T>(peer: Peer, body: (peer: Peer) => T | Promise<T>): Promise<T> {
    this.register(peer);
    try {
      return await body(peer);
    } finally {
      this.deregister(peer);
    }
  }

  /**
   * Every live peer whose `tags` match **all** the given keys.
   *
   * An **array** in a stable order, never a `Set` (WSM-REG-015): callers iterate it, and a `Set`'s
   * order is insertion order, which varies between runs for no reason anyone can see. Treat it as a
   * snapshot - a peer in it may already be closing.
   */
  peersFor(tags: Record<string, unknown> = {}): Peer[] {
    const wanted = Object.entries(tags);
    if (wanted.length === 0) return byId([...this.entries.keys()]);

    let found: Set<Peer> | null = null;
    for (const [key, value] of wanted) {
      // A value that could never have been indexed matches nothing, which is the same answer
      // WSM-REG-011 gives on the write side rather than a different one (Python's `except TypeError`).
      const holders = isIndexable(value) ? this.index.get(key)?.get(value) : undefined;
      if (holders === undefined || holders.size === 0) return [];
      found = found === null ? new Set(holders) : intersect(found, holders);
      if (found.size === 0) return [];
    }
    return byId([...(found ?? [])]);
  }
}

function intersect(found: Set<Peer>, holders: Set<Peer>): Set<Peer> {
  const both = new Set<Peer>();
  found.forEach((peer) => {
    if (holders.has(peer)) both.add(peer);
  });
  return both;
}

/** `peer.id` is `<prefix>-<counter>` (WSM-API-009); sorting on it is what makes the order stable. */
function byId(peers: Peer[]): Peer[] {
  return peers.sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
}
