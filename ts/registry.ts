/**
 * `peer.tags` and `PeerRegistry`: finding a peer by keys the application chose (§9.5).
 *
 * Mirrors `muxws/registry.py` member for member. The registry indexes `peer.tags` when `register` is
 * called and does not observe later changes. `tags` is a plain object (WSM-REG-013, WSM-REG-014):
 * writing a tag performs no bookkeeping, and the index changes only on `register`, `deregister` or
 * close.
 *
 * Usage rule: look up on keys that are not mutated, and mutate keys that are not looked up. For a key
 * that is both, call `register(peer)` after each write.
 */

import type { Peer } from './peer';

/**
 * The types usable as lookup values: the five primitive `typeof` results listed here. Python decides
 * with `hash()` and catches `TypeError`; JavaScript has no hashability check.
 *
 * Object and array values are skipped, not rejected (WSM-REG-011). `Map` compares them by identity,
 * so an indexed object would match only a lookup with the same reference.
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
   * Two levels, not one concatenated string key, because concatenation is ambiguous (`'a' + 1` and
   * `'a1' + ''` are equal). Non-indexable values are not stored.
   */
  private readonly index = new Map<string, Map<TagValue, Set<Peer>>>();

  /**
   * The reverse map, so `register` replaces all of a peer's entries in one pass (WSM-REG-012)
   * without scanning the index.
   */
  private readonly entries = new Map<Peer, [string, TagValue][]>();

  /**
   * Peers whose close hook is already installed.
   *
   * WSM-REG-017 has a consumer call `register(peer)` after every write to a key it also looks up.
   * Installing a close hook on every call would add one handler per write, and `Peer` cannot remove
   * a handler; WSM-REG-001 requires a rewrite to add no bookkeeping. A `WeakSet`, so the registry does
   * not keep an otherwise unreferenced peer alive.
   */
  private readonly hooked = new WeakSet<Peer>();

  /** The number of registered peers. Python: `len(registry)`. */
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
   * Index `peer` under every key in its current `tags` (WSM-REG-010).
   *
   * Re-registering replaces the previous entries: the peer is found under the new values and no
   * longer under the old ones.
   *
   * The peer is removed automatically when it closes (WSM-REG-016).
   */
  register(peer: Peer): void {
    this.deregister(peer);

    const entries: [string, TagValue][] = [];
    // `Object.entries`, not `for...in`, which includes inherited properties and is disallowed by the
    // shared eslint config.
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
    // `Peer` calls its close handlers on every connection loss and keeps them across a reconnect
    // (WSM-RCN-032, WSM-RCN-040), so the handler stays installed and deregisters the peer on each loss.
    peer.onClose(() => this.deregister(peer));
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
   * Python uses `with registry.registered(peer):`. TypeScript's `using` requires `Symbol.dispose`,
   * which the package's ES2020 target lacks, so the scope is a callback. `deregister` runs also when
   * `body` throws.
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
   * Returns an array sorted by `peer.id`, not a `Set` (WSM-REG-015); a `Set` iterates in insertion
   * order, which varies between runs. The result is a snapshot: a peer in it may already be closing.
   */
  peersFor(tags: Record<string, unknown> = {}): Peer[] {
    const wanted = Object.entries(tags);
    if (wanted.length === 0) return byId([...this.entries.keys()]);

    let found: Set<Peer> | null = null;
    for (const [key, value] of wanted) {
      // A non-indexable value matches nothing, consistent with `register` skipping such values
      // (WSM-REG-011; Python's `except TypeError`).
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

/** Sort by `peer.id` (`<prefix>-<counter>`, WSM-API-009), compared as strings. */
function byId(peers: Peer[]): Peer[] {
  return peers.sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
}
