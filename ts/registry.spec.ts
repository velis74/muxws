/**
 * `peer.tags` and `PeerRegistry` (§9.5).
 *
 * A mirror of `muxws/registry_test.py`, test for test. The registry **indexes**; it does not watch,
 * and WSM-REG-013/014 exist to make a plain object provably the right choice for `tags` - so the
 * assertions below are as much about what the registry declines to do as about what it does.
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
 * A dialable acceptor with no socket anywhere - `reconnect.spec.ts`'s rig, cut down to what a
 * registry test needs.
 *
 * WSM-RCN-033 is a claim about what a **reconnect** leaves behind, so the only honest way to assert
 * it is to drive a real one: two peers built side by side would prove nothing that `tags = {}` in the
 * constructor does not already prove.
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

  /** Socket death, from the server's side. */
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
    // WSM-REG-003 is what forbids the proxy an implementation is tempted to reach for.
    expect(peer.tags.session).toBe('def');
    peer.tags.anything = { nested: true };
    expect(Object.getPrototypeOf(peer.tags)).toBe(Object.prototype);
  });

  it('starts empty on the peer a reconnect produces on the acceptor side - WSM-RCN-033/WSM-INV-014', async () => {
    // Driven through the real reconnect driver rather than by building two peers by hand: the claim
    // is about what a *reconnect* leaves behind. The acceptor never learns that the socket it just
    // accepted belongs to the client that was here a moment ago, so there is nowhere for the old
    // peer's tags to come from unless an implementation deliberately carries them - and a tab that
    // silenced something and then died would keep a successor silent that never asked to be.
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
      // Something the *application* wrote on this connection, which no hello ever replays.
      const first = server.acceptors[0];
      first.tags.muted = true;
      registry.register(first);
      expect(registry.peersFor({ muted: true })).toEqual([first]);

      server.drop();
      await until(() => reconnects.length === 1, 3000, 'the reconnection');
      const successor = server.acceptors[server.acceptors.length - 1];

      expect(successor, 'the acceptor side of a reconnect is a new peer object').not.toBe(first);
      expect(successor.tags, 'carrying only what this connection itself set').toEqual({ tab: 'abc' });

      // And the dead connection's index entries went with it, so nothing finds it either.
      await until(() => registry.peersFor({ tab: 'abc' }).length === 1, 3000, 'the successor in the index');
      expect(registry.peersFor({ tab: 'abc' })).toEqual([successor]);
      expect(registry.peersFor({ muted: true }), 'a silenced tab must not silence its successor').toEqual([]);
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

    // It has no notion of which keys matter, so all three answer.
    expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);
    expect(registry.peersFor({ tenant: 7 })).toEqual([peer]);
    expect(registry.peersFor({ role: 'admin' })).toEqual([peer]);
  });

  it('passes over a tag value that cannot serve as a lookup key - WSM-REG-011', () => {
    const peer = makePair().dialer;
    Object.assign(peer.tags, { session: 'abc', profile: { deep: true }, seen: [1, 2] });

    // Must not throw: the peer is simply not findable by that key, which is strictly better than a
    // registration that fails because one unrelated tag happened to hold an object.
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

  it('makes overwriting a never-indexed key free - WSM-REG-014', () => {
    const peer = makePair().dialer;
    peer.tags.session = 'abc';
    registry.register(peer);
    const sizeBefore = registry.indexSize;

    for (let generation = 0; generation < 100; generation += 1) {
      peer.tags.watch = { generation };
      expect(peer.tags.watch).toEqual({ generation });
      expect(registry.peersFor({ session: 'abc' })).toEqual([peer]);
    }

    expect(registry.indexSize, 'a rewrite must cost the registry nothing').toBe(sizeBefore);
  });

  it('keys the index by name and value, so two tags cannot collide into one bucket', () => {
    // A string-concatenated key would put ('a', 1) and ('a1', undefined) in the same bucket, and the
    // registry would answer `peersFor({ a: 1 })` with a peer that never carried that tag. The
    // two-level `Map` is what makes that unrepresentable rather than merely unlikely.
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
    // Registered youngest-first, so insertion order is the reverse of id order. A registry that
    // returned whatever order its `Set` happened to hold would pass a test that registered them in
    // order and fail this one - which is the difference between "stable" and "stable by accident".
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
    // A key nobody carries, and a pair of keys both of which somebody carries but nobody carries
    // together: two different ways of not matching, and both must answer with nothing.
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

    // Via the peer's own close hook - a consumer never has to prune the index.
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

  it('costs the peer no bookkeeping when it is re-registered - WSM-REG-001/012/017', () => {
    // WSM-REG-017 tells a consumer that both looks up and mutates one key to call `register(peer)`
    // after every write, so this is the documented pattern rather than a pathological one. A hook
    // appended per call would leave a long-lived peer carrying one close handler per write, all of
    // them doing the same already-idempotent deregistration, and nothing anywhere would look wrong -
    // `Peer` has no way to remove a handler once it is on.
    const peer = makePair().dialer;
    const hooked = vi.spyOn(peer, 'onClose');

    for (let generation = 0; generation < 50; generation += 1) {
      peer.tags.session = `s${generation}`;
      registry.register(peer);
    }

    expect(hooked, 'one hook per peer, however often it is re-registered').toHaveBeenCalledTimes(1);
    expect(registry.peersFor({ session: 's49' })).toEqual([peer]);
    expect(registry.peersFor({ session: 's0' })).toEqual([]);
    expect(registry.indexSize).toBe(1);
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
    // Per-process, and nothing in this module reaches further. Asserted on the imports rather than on
    // the prose, so a comment mentioning Redis cannot fail it and an actual dependency cannot pass.
    const source = readFileSync(join(process.cwd(), 'ts', 'registry.ts'), 'utf8');
    const imported = [...source.matchAll(/^import\s.*?from\s+'([^']+)';$/gm)].map((match) => match[1]);
    expect(imported).toEqual(['./peer']);
  });
});
