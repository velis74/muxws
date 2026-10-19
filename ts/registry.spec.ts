/**
 * `peer.tags` and `PeerRegistry` (§9.5).
 *
 * Mirrors `muxws/registry_test.py` test for test. The registry indexes `tags` when `register` is
 * called; it does not observe later writes (WSM-REG-013/014), so `tags` is a plain object. The
 * assertions below cover both what the registry does and what it does not do.
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { vi } from 'vitest';

import { JsonCodec } from './codec';
import { Peer, type StreamHandler } from './peer';
import { ConnectionLoop, Hello, Reconnect } from './reconnect';
import { PeerRegistry } from './registry';
import { memoryPair, type MemorySocket } from './transports/memory';

// --------------------------------------------------------------------------- the harness

/** A dialer and an acceptor over one in-memory pair, neither serving until a test says so. */
interface Pair {
  readonly dialer: Peer;
  readonly acceptor: Peer;
  readonly dialerSocket: MemorySocket;
}

const built: Peer[] = [];

function makePair(): Pair {
  const codec = new JsonCodec();
  const [left, right] = memoryPair();
  const pair = {
    dialer: new Peer(left, { codec, isDialer: true }),
    acceptor: new Peer(right, { codec, isDialer: false }),
    dialerSocket: left,
  };
  built.push(pair.dialer, pair.acceptor);
  return pair;
}

afterEach(() => {
  built.splice(0, built.length);
});

/** One macrotask turn, which also drains the microtask queue - Python's `asyncio.sleep(0)`. */
function settle(rounds = 8): Promise<void> {
  return new Promise<void>((resolve) => {
    let left = rounds;
    const tick = (): void => {
      left -= 1;
      if (left <= 0) resolve();
      else setTimeout(tick, 0);
    };
    setTimeout(tick, 0);
  });
}

async function until(predicate: () => boolean, timeoutMs = 3000, what = 'the condition'): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`${what} never became true within ${timeoutMs}ms`);
    await settle(2);
  }
}

/**
 * A dialable acceptor without a real socket; a reduced version of the `reconnect.spec.ts` harness.
 *
 * WSM-RCN-033 concerns the state after a reconnect, so the test drives a real reconnect. Two peers
 * constructed separately would only test that the constructor sets `tags = {}`.
 */
class FakeServer {
  readonly acceptors: Peer[] = [];

  private readonly acceptorSockets: MemorySocket[] = [];

  handler: StreamHandler = () => undefined;

  dial = async (): Promise<MemorySocket> => {
    const [dialerSide, acceptorSide] = memoryPair();
    const acceptor = new Peer(acceptorSide, { codec: new JsonCodec(), isDialer: false });
    acceptor.onStream((payload, stream) => this.handler(payload, stream));
    this.acceptors.push(acceptor);
    this.acceptorSockets.push(acceptorSide);
    void acceptor.serve().catch(() => undefined);
    return dialerSide;
  };

  /** Closes the socket from the server side. */
  drop(): void {
    this.acceptorSockets[this.acceptorSockets.length - 1]?.drop();
  }
}

let registry: PeerRegistry;

beforeEach(() => {
  registry = new PeerRegistry();
});

// --------------------------------------------------------------------------- tags

describe('peer.tags', () => {
  it('is an ordinary object with ordinary object semantics - WSM-REG-001/002/003', () => {
    const peer = makePair().dialer;

    expect(peer.tags).toEqual({});
    peer.tags.session = 'abc';
    peer.tags.session = 'def';
    // Last write wins, with no copy and no snapshot in the read path: `tags` is a plain object, and
    // WSM-REG-003 forbids a proxy.
    expect(peer.tags.session).toBe('def');
    peer.tags.anything = { nested: true };
    expect(Object.getPrototypeOf(peer.tags)).toBe(Object.prototype);
  });

  it('starts empty on the peer a reconnect produces on the acceptor side - WSM-RCN-033/WSM-INV-014', async () => {
    // Uses the real reconnect driver, because the rule concerns the state after a reconnect. The
    // acceptor does not know that the new socket belongs to the previous client, so the old peer's
    // tags could reach the new peer only if an implementation copied them. Copying would, for
    // example, carry a mute set by a tab that has since disconnected to its successor.
    const server = new FakeServer();
    server.handler = (payload) => {
      const acceptor = server.acceptors[server.acceptors.length - 1];
      acceptor.tags.tab = (payload as { tab: string }).tab;
      registry.register(acceptor);
    };

    const peer = new Peer(await server.dial(), { codec: new JsonCodec(), isDialer: true });
    const reconnects: number[] = [];
    peer.onReconnect((attempt) => reconnects.push(attempt));
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
      hello: new Hello({ payload: { tab: 'abc' } }),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    await loop.establish();
    loop.start();

    try {
      // A tag the application wrote on this connection; no hello replays it.
      const first = server.acceptors[0];
      first.tags.muted = true;
      registry.register(first);
      expect(registry.peersFor({ muted: true })).toEqual([first]);

      server.drop();
      await until(() => reconnects.length === 1, 3000, 'the reconnection');
      const successor = server.acceptors[server.acceptors.length - 1];

      expect(successor, 'the acceptor side of a reconnect is a new peer object').not.toBe(first);
      expect(successor.tags, 'carrying only what this connection itself set').toEqual({ tab: 'abc' });

      // The closed connection's index entries are removed as well.
      await until(() => registry.peersFor({ tab: 'abc' }).length === 1, 3000, 'the successor in the index');
      expect(registry.peersFor({ tab: 'abc' })).toEqual([successor]);
      expect(
        registry.peersFor({ muted: true }),
        'a tag set on the previous connection must not appear on the new peer',
      ).toEqual([]);
    } finally {
      await loop.stop();
    }
  });
});

// --------------------------------------------------------------------------- register

describe('register', () => {
  it('indexes every key present in tags at call time - WSM-REG-010', () => {
    const peer = makePair().dialer;
    Object.assign(peer.tags, { session: 'abc', tenant: 7, role: 'admin' });
    registry.register(peer);

    // The registry indexes every key, so all three lookups find the peer.
    expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);
    expect(registry.peersFor({ tenant: 7 })).toEqual([peer]);
    expect(registry.peersFor({ role: 'admin' })).toEqual([peer]);
  });

  it('passes over a tag value that cannot serve as a lookup key - WSM-REG-011', () => {
    const peer = makePair().dialer;
    Object.assign(peer.tags, { session: 'abc', profile: { deep: true }, seen: [1, 2] });

    // Must not throw: the peer is not findable by that key, and the other tags are still indexed.
    registry.register(peer);

    expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);
    expect(registry.indexSize, 'an object-valued tag must not take an index entry').toBe(1);
  });

  it('replaces a peer entries wholesale when tags changed - WSM-REG-012', () => {
    const peer = makePair().dialer;
    peer.tags.session = 'old';
    registry.register(peer);
    expect(registry.peersFor({ session: 'old' })).toEqual([peer]);

    peer.tags.session = 'new';
    registry.register(peer);
    expect(registry.peersFor({ session: 'new' })).toEqual([peer]);
    expect(registry.peersFor({ session: 'old' }), 'the old entry must be gone, not merely shadowed').toEqual([]);
    expect(registry.indexSize).toBe(1);
  });

  it('does not find a tag written after register until it is registered again - WSM-REG-013', () => {
    const peer = makePair().dialer;
    peer.tags.session = 'abc';
    registry.register(peer);

    peer.tags.session = 'changed';
    expect(peer.tags.session, 'a direct read sees the newest value at once').toBe('changed');
    expect(registry.peersFor({ session: 'changed' }), 'but the index has not been told').toEqual([]);
    expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);

    registry.register(peer);
    expect(registry.peersFor({ session: 'changed' })).toEqual([peer]);
  });

  it('does no index work when an unindexed key is overwritten - WSM-REG-014', () => {
    const peer = makePair().dialer;
    peer.tags.session = 'abc';
    registry.register(peer);
    const sizeBefore = registry.indexSize;

    for (let generation = 0; generation < 100; generation += 1) {
      peer.tags.watch = { generation };
      expect(peer.tags.watch).toEqual({ generation });
      expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);
    }

    expect(registry.indexSize, 'overwriting an unindexed key must not change the index').toBe(sizeBefore);
  });

  it('keys the index by name and value, so two tags cannot collide into one bucket', () => {
    // A string-concatenated key would put ('a', 1) and ('a1', undefined) in the same bucket, and
    // `peersFor({ a: 1 })` would return a peer without that tag. The two-level `Map` makes such a
    // collision impossible.
    const one = makePair().dialer;
    const other = makePair().dialer;
    one.tags.a = 1;
    other.tags.a1 = '';
    registry.register(one);
    registry.register(other);

    expect(registry.peersFor({ a: 1 })).toEqual([one]);
    expect(registry.peersFor({ a1: '' })).toEqual([other]);
  });
});

// --------------------------------------------------------------------------- peersFor

describe('peersFor', () => {
  it('returns an array in a stable order - WSM-REG-015', () => {
    const peers: Peer[] = [];
    for (let index = 0; index < 5; index += 1) {
      const peer = makePair().dialer;
      peer.tags.room = 'lobby';
      peers.push(peer);
    }
    // Registered newest-first, so insertion order is the reverse of id order. A registry that
    // returned its `Set` insertion order would pass a test that registered peers in id order and
    // fail this one.
    [...peers].reverse().forEach((peer) => registry.register(peer));

    const first = registry.peersFor({ room: 'lobby' });
    expect(Array.isArray(first), 'an array, never a Set, so two runs agree').toBe(true);
    expect(first).toHaveLength(5);
    expect(first.map((peer) => peer.id)).toEqual([...first.map((peer) => peer.id)].sort());
    expect(registry.peersFor({ room: 'lobby' }), 'the order must not vary between calls').toEqual(first);
  });

  it('matches all the given keys, so a partial match is not a match', () => {
    const both = makePair().dialer;
    Object.assign(both.tags, { room: 'lobby', role: 'admin' });
    const one = makePair().dialer;
    Object.assign(one.tags, { room: 'lobby', role: 'guest' });
    registry.register(both);
    registry.register(one);

    expect(registry.peersFor({ room: 'lobby', role: 'admin' })).toEqual([both]);
    expect(registry.peersFor({ room: 'lobby' })).toHaveLength(2);
    // A key no peer has, and two keys that some peers have but no peer has together: both lookups
    // must return an empty array.
    expect(registry.peersFor({ room: 'lobby', role: 'nobody' })).toEqual([]);
    const elsewhere = makePair().dialer;
    Object.assign(elsewhere.tags, { room: 'attic', role: 'auditor' });
    registry.register(elsewhere);
    expect(registry.peersFor({ room: 'attic', role: 'admin' })).toEqual([]);
  });

  it('returns every registered peer when asked for no keys at all', () => {
    const first = makePair().dialer;
    const second = makePair().dialer;
    first.tags.room = 'lobby';
    registry.register(first);
    registry.register(second);
    expect(registry.peersFor()).toEqual([first, second]);
    expect(registry.size).toBe(2);
  });

  it('finds nothing rather than throwing when the lookup value could never be a key', () => {
    const peer = makePair().dialer;
    peer.tags.session = 'abc';
    registry.register(peer);
    expect(registry.peersFor({ session: ['not', 'a', 'key'] })).toEqual([]);
  });
});

// --------------------------------------------------------------------------- lifetime

describe('the registry lifetime', () => {
  it('removes a peer from the index automatically when its socket closes - WSM-REG-016', async () => {
    const pair = makePair();
    pair.dialer.tags.session = 'abc';
    registry.register(pair.dialer);
    const serving = pair.dialer.serve();
    void serving.catch(() => undefined);

    expect(registry.peersFor({ session: 'abc' })).toEqual([pair.dialer]);

    // Removed through the peer's close hook; the consumer does not remove it.
    pair.dialerSocket.drop();
    await settle();
    await serving.catch(() => undefined);

    expect(registry.peersFor({ session: 'abc' }), 'a closed peer must stop being findable').toEqual([]);
    expect(registry.size).toBe(0);
  });

  it('scopes a registration to a body and deregisters even when it throws', async () => {
    const peer = makePair().dialer;
    peer.tags.session = 'abc';

    // Python spells this `with registry.registered(peer):`; TypeScript's `using` needs a
    // `Symbol.dispose` this package's ES2020 target does not carry, so the scope is a callback.
    await registry.registered(peer, () => {
      expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);
    });
    expect(registry.peersFor({ session: 'abc' })).toEqual([]);

    await expect(
      registry.registered(peer, () => {
        throw new Error('the body failed');
      }),
    ).rejects.toThrow('the body failed');
    expect(registry.peersFor({ session: 'abc' }), 'a throwing body must still deregister').toEqual([]);
  });

  it('adds no close handler to the peer when it is re-registered - WSM-REG-001/012/017', () => {
    // WSM-REG-017 tells a consumer that both looks up and mutates one key to call `register(peer)`
    // after every write. A close handler added per call would leave a long-lived peer with one
    // handler per write, each performing the same idempotent deregistration; `Peer` has no way to
    // remove a handler.
    const peer = makePair().dialer;
    const hooked = vi.spyOn(peer, 'onClose');

    for (let generation = 0; generation < 50; generation += 1) {
      peer.tags.session = `s${generation}`;
      registry.register(peer);
    }

    expect(hooked, 'one close hook per peer after repeated register() calls').toHaveBeenCalledTimes(1);
    expect(registry.peersFor({ session: 's49' })).toEqual([peer]);
    expect(registry.peersFor({ session: 's0' })).toEqual([]);
    expect(registry.indexSize).toBe(1);
  });

  it('keeps one close handler on a peer re-registered after each reconnect - WSM-REG-001/016/RCN-032', () => {
    // Each socket loss deregisters the peer; the handler stays installed for the next loss.
    const peer = makePair().dialer;
    const hooked = vi.spyOn(peer, 'onClose');

    for (let generation = 0; generation < 20; generation += 1) {
      peer.tags.session = `s${generation}`;
      registry.register(peer);
      expect(registry.peersFor({ session: `s${generation}` })).toEqual([peer]);
      peer.notifyClose({ code: 1006, reason: '', wasClean: false, willRetry: true });
      expect(registry.peersFor({ session: `s${generation}` }), 'every socket loss deregisters the peer').toEqual([]);
    }

    expect(hooked, 'one close hook per peer across reconnects and re-registrations').toHaveBeenCalledTimes(1);
  });

  it('deregisters idempotently', () => {
    const peer = makePair().dialer;
    peer.tags.session = 'abc';
    registry.register(peer);
    registry.deregister(peer);
    registry.deregister(peer);
    expect(registry.size).toBe(0);
    expect(registry.indexSize).toBe(0);
  });

  it('ships no cross-process backplane - WSM-REG-018', () => {
    // The registry is per-process. Asserted on the imports, not on the source text, so a comment
    // mentioning Redis does not fail the test and an actual dependency does.
    const source = readFileSync(join(process.cwd(), 'ts', 'registry.ts'), 'utf8');
    const imported = [...source.matchAll(/^import\s.*?from\s+'([^']+)';$/gm)].map((match) => match[1]);
    expect(imported).toEqual(['./peer']);
  });
});
