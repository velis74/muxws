# Gaps

Every place a brief is silent, ambiguous, self-contradictory, or contradicted by the specification,
plus every deliberate deviation from a brief's literal text. One entry per problem, newest last.

## muxws-m0-scaffolding.md — WSM-PKG-001, WSM-PKG-003

**What I needed:** the distribution name for both packages.

**What the brief says:** `dynamicforms-wsmux` on PyPI and `@dynamicforms/wsmux` on npm, with the
Python import package `wsmux/`.

**What I assumed:** neither. The author decided the library is not a DynamicForms component - it has
nothing to do with forms - and must stand on its own name. `wsmux` was unavailable on PyPI (taken by
an unrelated workspace-launcher CLI whose wheel ships a colliding `wsmux/` module), so the name is
**`muxws`**, one word, everywhere: PyPI `muxws`, npm `muxws`, Python package `muxws/`, environment
variables `MUXWS_CODEC` / `VITE_MUXWS_CODEC`, loggers `muxws.frames` / `muxws.codec`, root exception
`MuxwsError`, and the wire subprotocol `muxws.v1.<codec>`. The whole of `docs/design/` was renamed
mechanically to match; the `WSM-*` rule ids were deliberately left untouched, because the briefs make
them the stable contract between milestones.

## muxws-m0-scaffolding.md — no rule id

**What I needed:** to know whether `docs/design/` is part of the published documentation site.

**What the brief says:** nothing. Its `docs/.vitepress/config.ts` was written for a `docs/` workspace
that contained only `index.md`, `guide/` and `api/`, because in the original design repository the
briefs lived somewhere else entirely.

**What I assumed:** it is not. The author confirmed the design material is a record of how muxws was
designed, not documentation of how to use it. `srcExclude: ['design/**']` keeps it out of the build.
This also disposes of a problem M7 would otherwise have hit: `docs/design/README.md` still carries 16
links to backchannel briefs that do not exist in this repository, and M7 requires the VitePress build
to treat dead links as fatal.

## muxws-m0-scaffolding.md — no rule id

**What I needed:** a CI job for the Node side that survives a fresh checkout.

**What the brief says:** two things that cannot both hold. Its `.gitignore` (§5) ignores
`/package-lock.json`, and its CI description (§5) runs `npm ci` - which fails outright when no
lockfile is present, which on a fresh clone is always.

**What I assumed:** nothing, in the end - the author decided. The `.gitignore` half stands and CI
runs `npm install`. The reason is not that a lockfile is unwanted in principle but that resolving
afresh is *what CI is for* at this stage: it tests the newest dependency tree against the pinned one
sitting on the development machine, so upstream breakage surfaces on the day it happens instead of
whenever someone next regenerates the lock. A committed lockfile is a deployment posture, not a
development one. Worth revisiting at 1.0, when reproducibility starts to matter more than early
warning.

## muxws-m1-frames-and-codec.md — WSM-CDC-001, WSM-FRG-011, WSM-FRG-030

**What I needed:** a way to encode a *logical payload* on its own, and to decode a reassembled one.

**What the brief says:** the `Codec` port exposes exactly four members - `name`, `binary`,
`encode(frame)` and `decode(message)`. But WSM-FRG-011 defines fragmentation over "the encoded form
of the logical payload", and WSM-FRG-030 has the receiver "hand the result to the codec for
decoding". Neither operation can be expressed through the frame-level pair: encoding a whole frame
to get at its payload's bytes would include the envelope, and there is no frame to hand a bare
reassembled buffer to.

**What I assumed:** that the port needs two more members, `encode_payload` / `decode_payload`
(`encodePayload` / `decodePayload`). WSM-CDC-001 says a codec MUST *expose* those four, not that it
may expose only those, so this extends the port rather than contradicting it. Both are documented on
the protocol in both languages. If the intent was that fragmentation should slice the encoded
*frame* instead, that is a different protocol - the `fragment` field would then carry envelope bytes
- and WSM-FRG-011 says otherwise.

## muxws-m1-frames-and-codec.md — WSM-CDC-014, test 8

**What I needed:** a test that a codec module does not register itself at import time.

**What the brief says:** "Importing `muxws` registers `json`; importing `muxws.codecs.json_` alone
registers nothing."

**What I assumed:** that the second half cannot be written in Python as described, and that the rule
is still worth testing by other means. Importing `muxws.codecs.json_` necessarily executes
`muxws/__init__.py` first - that is how Python packages work - and that file registers JSON
deliberately, because WSM-CDC-004 requires the library to ship it registered. The two halves are
only separable by looking at *where the call is written*, so the test parses the codec module and
asserts it contains no module-scope `register_codec` call. This is also how M6's own done-when
checks it (`grep -rn "register_codec" muxws/codecs`). The TypeScript half of the test is
unaffected: there, importing `./msgpack` really does register nothing, and the spec asserts it.

## muxws-m1-frames-and-codec.md — WSM-FRG-015 vs §6

**What I needed:** the splitter's signature.

**What the brief says:** two things. WSM-FRG-015 calls it "a pure function of `(payload, cap,
codec)`", while the skeleton in §6 declares `split_frame(frame, cap, codec)`.

**What I assumed:** the skeleton. A payload alone is not enough to build the output: `headers` ride
the first fragment, `end` and `trailers` the last, and `stream` and `type` every one of them
(WSM-FRG-020/021). The function stays pure in the sense the rule cares about - same arguments, same
result, no argument mutated - which is asserted by `test_split_is_pure`.

## muxws-m1-frames-and-codec.md — WSM-FRG-016

**What I needed:** a test that actually holds both ports to the same fragment boundaries.

**What the brief says:** "Both ports MUST produce the same fragment boundaries. Test: shared
fixtures in `conformance/frames/`." The frames corpus there is a list of `{name, frame, json_wire}`
triples (WSM-TST-001), which pins wire *renderings* and says nothing about where a payload gets cut.

**What I assumed:** that a second fixture kind is needed, and wrote
`conformance/frames/v1-fragment-boundaries.json` - `{name, cap, payload, fragments}` records listing
the exact slices, generated once from the Python port and asserted by both suites. Without it,
WSM-FRG-016 is untested until the cross-language matrix in M6, where a boundary drift surfaces as
apparent payload corruption rather than as a named failure. The schema test knows both fixture kinds
and validates each against its own shape.

## muxws-m1-frames-and-codec.md — no rule id

**What I needed:** ruff to accept the specified exception names.

**What the brief says:** the M0 ruff configuration, verbatim, with `N` (pep8-naming) enabled.

**What I assumed:** that the names win. `N818` requires every exception class to end in `Error`, and
seven of the specified ones do not - `ConnectionClosed`, `ConnectionGoingAway`,
`StreamAlreadyConsumed`, `StreamClosed`, `CodecNotRegistered`, `CodecMismatch`, `StreamReset`.
WSM-ERR-004 fixes these names in both languages precisely so cross-language tests can assert on
error identity, so the rule is inapplicable to that one module and is disabled there by a per-file
ignore carrying this reason.

## muxws-m1-frames-and-codec.md — WSM-FRG-016, WSM-CDC-004

**What I needed:** both ports to emit the same bytes for the same payload, since fragment boundaries
are cut over exactly that form.

**What the brief says:** WSM-FRG-016, a MUST: "Both ports MUST produce the **same fragment
boundaries** for the same payload, cap and codec."

**What I assumed:** that the rule holds for every value the shared corpus may contain, and no
further. It cannot hold in general, and no amount of codec configuration makes it. Python's
`json.dumps` and JavaScript's `JSON.stringify` agree on strings, containers, booleans, `null` and
integers up to 2^53, and disagree on every float and on larger integers:

| value | Python | JavaScript |
|---|---|---|
| `1.0` | `1.0` | `1` |
| `-0.0` | `-0.0` | `0` |
| `100.0` | `100.0` | `100` |
| `1e16` | `1e+16` | `10000000000000000` |
| `1e-7` | `1e-07` | `1e-7` |
| `1e-6` | `1e-06` | `0.000001` |
| `12345678901234567890` | exact | `12345678901234567000` |

JavaScript has one number type and one canonical rendering of it, fixed by ECMAScript; the
divergence is inherited from the language, not chosen by this codec.

**Why this is accepted rather than fixed:** it costs nothing on the wire. Boundaries are chosen by
the *sender*; a receiver concatenates whatever fragments arrive and never learns where the cuts
were, so a Python sender and a TypeScript receiver interoperate on a float payload exactly as they
do on any other. What the rule really buys is a corpus that replays identically in both languages,
and that is preserved by keeping floats out of `conformance/frames/v1-fragment-boundaries.json` and
out of the M6 sequence fixtures. Both ports carry a test pinning the divergence
(`test_number_forms_that_the_two_ports_spell_differently` and its TypeScript twin) so it stays known
and cannot silently widen, and both codec modules now scope their byte-identity claim to the types
where it is true - they previously asserted it unconditionally, which was simply wrong.

**If strict conformance is wanted instead:** the Python codec would have to emit ECMAScript-shaped
numbers - strip `.0` from integral floats, normalise `-0.0`, and switch to exponent form only
outside JavaScript's 1e21 / 1e-7 thresholds with unpadded exponents. That means a custom
`JSONEncoder` with the C accelerator disabled, which is both slower and more fragile than the
divergence it removes. It is a decision for the author, not for the implementer, and it is recorded
here rather than taken.

## muxws-m1-frames-and-codec.md — WSM-CDC-005

**What I needed:** to assert that both ports emit compact, un-escaped JSON.

**What the brief says:** "Exactly one separate test MAY pin a canonical key order ... no other test
may depend on key order or whitespace." `test_canonical_key_order` is that one test.

**What I assumed:** that one whitespace dependency has to stay. `test_encoding_is_compact_and_not_ascii_escaped`
asserts no spaces and no `\u` escaping, and it is load-bearing for WSM-FRG-016: if either port
started emitting spaces, fragment boundaries would diverge everywhere rather than only on floats.
The redundant byte-exact comparisons that had crept into the D1 test in both ports were removed -
the `to_mapping` and `decode` assertions beside them already covered what that test is for.

## muxws-m2-peer-core.md — no rule id

**What I needed:** a type for `on_close` to hand its handler.

**What the brief says:** M2's `Peer` skeleton declares `on_close(handler: Callable[[CloseReason], None])`,
but `CloseReason` is created in M5a's `observability.py` and gains its `will_retry` field in M5b.

**What I assumed:** that the type has to exist as soon as the callback does, so
`muxws/observability.py` is created here with `CloseReason` alone. M5a fills the module out with the
one-line logger and `on_frame` dispatch as its brief says; `will_retry` is already present and always
`False`, because until the reconnect helper lands there is nothing that retries.

## muxws-m2-peer-core.md — WSM-TST-003, brief §7 test 30

**What I needed:** to run all eight `conformance/invalid/` fixtures against a live peer.

**What the brief says:** "Parametrized over all eight ... The over-cap and undecodable cases use a
codec double, since size enforcement itself lands in M5a."

**What I assumed:** seven of the eight run for real. The undecodable case needs no double - a codec
that refuses to decode is exactly what `JsonCodec` already does, and the connection dies as the
fixture declares. The over-cap case cannot be made honest with a double: the fixture asserts
`reset(PAYLOAD_TOO_LARGE)`, which is receive-side size enforcement, and that is M5a's by the brief's
own §9. Faking it with a codec double would assert that the double works, not that the peer does. It
is marked `xfail(strict=True)` instead, so the moment M5a implements the cap the test fails as an
unexpected pass and the marker has to be removed - a skip would have rotted silently.

## muxws-m3-transports.md — WSM-API-002, WSM-API-015

**What I needed:** a helper that builds a stream and hands it back.

**What the brief says:** `Stream` is awaitable (WSM-API-002) and reaches that by implementing
`PromiseLike` rather than subclassing `Promise` (WSM-API-015). Neither rule mentions the
consequence.

**What I assumed:** nothing - I found it out. `await` unwraps thenables *recursively*, so a
`Promise<Stream>` never resolves to the stream: awaiting it awaits the stream and yields its first
payload, or throws the reset that closed it. **A `Stream` can never be the resolution value of a
promise anywhere in TypeScript.** Any `async function openIt(): Promise<Stream>` is silently wrong,
and it fails in a way that reads as a state-machine bug rather than a language rule - in the test
suite it surfaced as nine state-table cells failing with `StreamReset: NO_ERROR` from a helper. The
fix is to box it: `Promise<{ stream: Stream }>`. Python has no such trap, because a
`__await__`-bearing object is only awaited when it is awaited. M7's `call-shapes.md` must say this
out loud, because every user will write that signature eventually.

## muxws-m3-transports.md — WSM-ERR-014

**What I needed:** the TypeScript half of "a consumer that walks away resets the stream".

**What the brief says:** WSM-ERR-014 requires local cancellation propagating out of
`await stream.result()` or an `async for` to send `reset(CANCELLED)` and re-raise. It is written in
Python's terms, where `CancelledError` is delivered *into* the awaiting frame.

**What I assumed:** that it carries across only where JavaScript can observe the walking away, and
that saying so is better than pretending. Three cases:
- **`for await` that breaks, returns or throws** - carries across. The async generator is finalised
  and its `finally` sends the reset. This is the case the rule names explicitly.
- **`result({ signal })`** - carries across, via an `AbortSignal` the caller supplies. Aborting
  resets the stream with CANCELLED and throws, which is the analogue of re-raising.
- **A bare `await stream` the caller drops** - does **not** carry across, and cannot. Nothing in
  JavaScript observes an abandoned promise; there is no `CancelledError` to deliver and no hook to
  hang one on. A consumer that needs the reset passes a signal.

The remote is therefore told in two of the three shapes rather than three. `stream.signal` and
`stream.closed` are the TypeScript observation points either way (WSM-API-023, WSM-ERR-013).

## muxws-m3-transports.md — WSM-ERR-006, interop

**What I needed:** an interop assertion on the structured error a `reset(APPLICATION_ERROR)` carries.

**What the brief says:** WSM-ERR-006 fixes the default serializer as
`{"type": type(exc).__name__, "message": str(exc)}`.

**What I assumed:** that `type` is **not portable across languages** and an interop check must not
assert equality on it. The same failing handler produces `{"type": "ValueError"}` from a Python
acceptor and `{"type": "Error"}` from a TypeScript one, because the field is by definition the
remote's own exception class name. `message` is portable; `type` identifies which language answered.
An application that switches on `type` works within one language and breaks the moment the other end
is reimplemented - worth a sentence in M7's `errors.md`, since the default serializer invites exactly
that.

## muxws-m4-connection-lifecycle.md — WSM-CON-024, TypeScript port

**What I needed:** the TypeScript shape of the drain window.

**What the brief says:** "Drain is a deadline, not a poll loop: `await asyncio.wait_for(
all_draining_streams_closed, drain)` then close regardless." The Python implementation of `_drain`
does the opposite - it spins `while self._streams and loop.time() < deadline: await
asyncio.sleep(0)`, a busy wait that burns the whole ten seconds of the default when any stream is
still live.

**What I assumed:** the brief, not the Python code. `Peer.drain` races `Promise.all` over the live
streams' `closed` promises against one `setTimeout`, which is observably identical - streams at or
below the cut-off finish, whatever is still live at the deadline takes the socket-death path - and
does not occupy the event loop for the duration. The consequence for the tests is that the two
suites' drain values differ: `test_goaway_carries_code_reason_and_last_stream` leaves Python's
default 10 s drain in place and sits it out, while its TypeScript mirror passes `drainMs: 20`,
because the frame under test is on the wire before the drain begins and vitest's per-test deadline is
5 s. No assertion is weakened by that.

## muxws-m4-connection-lifecycle.md — no rule id

**What I needed:** to know whether `GoawayState.draining` carries anything.

**What the brief says:** nothing. `muxws/lifecycle.py` gives the dataclass a
`draining: asyncio.Event` field, and no code in `muxws/` ever sets, waits on or reads it.

**What I assumed:** that it is vestigial and that mirroring it would be mirroring dead weight.
`ts/lifecycle.ts`'s `GoawayState` carries the six fields that are actually consulted - `sent`,
`received`, `remoteLastStream`, `sentCode`, `receivedCode`, `receivedReason` - plus `isGoingAway` and
`survivesDrain`. If a later milestone gives `draining` a job, it comes back on both sides at once.

## muxws-m4-connection-lifecycle.md — WSM-SID-007 (closed)

**What I needed:** what a peer does when the id space runs out.

**What the brief says:** "the exhausting peer MUST send `goaway` with `last_stream` set to the
highest id it has processed, MUST stop opening new streams, MUST let in-flight streams drain, and
MUST then close." Four obligations.

**What I did first, and why it was wrong:** only the second. `open()` raised `ConnectionGoingAway`
once the allocator passed 2^31-1 and nothing else happened - no `goaway`, no drain, no close. The
test asserted the raise and stopped there, so the gap was invisible. An audit of the TypeScript port
found it and correctly declined to fix it unilaterally, because the obvious repair looks like it
conflicts with WSM-API-001: `open()` returns a `Stream` without suspending, so it cannot await a
drain.

**How it is resolved:** the conflict is only apparent. Taking the *last* id now starts the shutdown
rather than the next call reporting it - `asyncio.create_task` / an unawaited promise schedules the
goaway-drain-close sequence without suspending the caller, so `open()` still returns a `Stream`
synchronously and the stream just allocated gets its drain window like any other. Both ports
implement it, and the test in each fails when the trigger is removed. Nothing is deferred here any
more; this entry is kept because the shape of the mistake - satisfying one clause of a four-clause
MUST and writing a test that agrees with you - is worth remembering.


## muxws-m4-connection-lifecycle.md — WSM-CON-012

**What I needed:** the type of `peer.ping()`'s return value in TypeScript.

**What the brief says:** it returns "seconds (Python) / milliseconds (TypeScript)", and its
configuration table says durations are "milliseconds as integers in TypeScript".

**What I assumed:** that "integers" governs the *options* the caller supplies (`timeoutMs`,
`drainMs`) and not the measurement handed back. `ping()` returns a fractional number of milliseconds,
measured with `performance.now()`, because an in-memory round trip completes in well under one
millisecond and an integer result would report every such ping as `0` - which is the one value that
cannot be distinguished from a broken clock. `performance.now()` rather than `Date.now()` also makes
the measurement monotonic, so a wall-clock adjustment mid-flight cannot produce a negative RTT.

## muxws-m4-connection-lifecycle.md — §7 test 9, tooling

**What I needed:** somewhere to put the TypeScript-only test that native WebSocket ping frames are
never used (WSM-CON-011).

**What the brief says:** `ts/peer.spec.ts::native websocket ping frames are never used`.

**What I assumed:** that the file it lives in is not load-bearing and that keeping M4's tests
together is worth more. It is in `ts/lifecycle.spec.ts` beside the rest of the ping material, as the
one test in that file with no Python counterpart, labelled as such. Separately: the shared eslint
configuration's `globals` list knows `console` and `setTimeout` but not `crypto` or `performance`, so
both are reached through `globalThis` - a `no-undef` disable comment would suppress more than the one
name it is about.

## muxws-m4-connection-lifecycle.md / muxws-m6-conformance.md — WSM-TST-002

**What I needed:** the schema of a `conformance/sequences/` fixture, in enough detail to write the
first one and the two runners that replay it.

**What the brief says:** two things that cannot both hold. WSM-TST-002 is a MUST: "Fixtures MUST
refer to streams by `stream_ref` (an ordinal the runner resolves), **never by a raw id** - a raw id
bakes in one side's parity." The worked example reproduced immediately below it then writes
`{"expect_frame": {"type": "open", "stream": 1, "end": true}}` and `{"type": "data", "stream": 1}` -
raw ids, and the dialer's parity at that. Beyond those seven lines the schema is undefined: there is
no list of step kinds, no statement of which peer an `expect_frame` is about, and no way at all to
express `goaway.last_stream`, whose whole point (WSM-CON-020) is that it carries the *other* peer's
parity and so can never be written as a literal.

**What I assumed:** the MUST wins and the example is illustrative. `conformance/sequences/goaway-
drains-then-closes.json` carries no raw stream id anywhere, and the two runners resolve four things:

- `stream_ref: n` -> the id the n-th stream *the script opens* actually got. It appears both as a
  call argument (`{"call": "reply", "stream_ref": 1}`) and inside an `expect_frame`, where it
  replaces the example's raw `stream` key.
- `last_stream_ref: n` -> the same resolution applied to `goaway.last_stream`. Without it the one
  field this fixture exists to pin could only be asserted as a number, which is precisely the parity
  the rule forbids baking in.
- `drain_ms` -> milliseconds in the corpus, as in TypeScript; the Python runner divides by 1000,
  because Python's durations are seconds (WSM-CON-012). A corpus carrying both units would be two
  corpora.
- `peer` on an `expect_frame` / `expect_no_frame` step -> which side's wire is searched. The example
  omits it and leaves the direction to be inferred from the preceding step; two peers write two
  independent `sent` lists with no shared clock between them, so the runner would have to invent a
  merge order that the fixture never stated. Naming the sender is one word and removes the guess.

The step kinds both runners implement are `settle`, `call` (`open`, `reply`, `close`, `await_close`),
`expect_frame`, `expect_no_frame`, `expect_result`, `expect_error` and `expect_closed` - the calls
this one fixture needs and no more. An unrecognised call fails loudly rather than being skipped, so
M6 extends the pair deliberately rather than discovering that one runner quietly ignored a step the
other executed. `expect_frame` is a subset match in both, as WSM-TST-002's implementation note
requires; `expect_no_frame` searches the whole of a peer's wire rather than the unmatched tail,
because "nothing goes out for them" (WSM-CON-023) is a claim about all of it.

**Still open, and deliberately not done here:** M6 §7 test 3 replays every sequence fixture *twice*,
with the roles swapped. The schema above is what makes that possible and the fixture is written to
survive it, but the swap itself is M6's test and is not run by these runners. Neither is a schema
test over `conformance/sequences/` in `conformance_schema_test.py`; M6 owns `conformance/README.md`,
which is where that schema is supposed to be pinned first.

## muxws-m5a-fragmentation-and-writer.md — WSM-FRG-019, the TypeScript writer

**What I needed:** a TypeScript spelling for four things `muxws/writer.py` gets from Python and
`writer_test.py` asserts against directly.

**What the brief says:** §6 describes the writer structurally (`dict[int, StreamQueue]`, a rotating
cursor, at most one prepared frame) and requires that "a `list` used as a FIFO of frames anywhere in
the send path is an automatic failure". It says nothing about how the port names any of it.

**What I assumed:** the structure is normative and the spelling is not, so `ts/writer.ts` mirrors
`writer.py` member for member with four mechanical substitutions:

- `__len__` on `Writer` and `StreamQueue` becomes a `depth` getter (`len(writer)` has no operator to
  overload). `depth_of` / `prepared_depth_of` keep their names as `depthOf` / `preparedDepthOf`.
- `_rotate()` and `_queues` become `private`. Python's underscore is a convention a test can reach
  through; TypeScript's `private` is erased at runtime, so `writer.spec.ts` reaches through one
  narrowly-typed cast (`internals(writer)`) rather than widening the public surface past Python's.
  Anything the peer needs is public: `nextFrame`, `enqueue`, `advance`, `discard`, `discardAll`,
  `stop`.
- `asyncio.Event` becomes a five-line latching `Gate`. Latching matters and is the reason it is not a
  bare promise: an `enqueue` landing between two turns of the write loop must not be missed.
- `LaneEncodingError` extends `Error`, not `MuxwsError`, because `muxws/writer.py` extends
  `Exception` and not `MuxwsError`. Keeping the inheritance identical keeps `instanceof` and
  `isinstance` answering the same question in both ports; the alternative would make a
  cross-language error-identity test pass in one language only.

**The one place this is genuinely weaker than Python.** `writer_test.py` proves the absence of a
send-path FIFO by counting occurrences of the type annotation `deque[Frame]` in the module source.
The TypeScript equivalent counts `Frame[]`, which `writer.spec.ts` does. It is a weaker witness: a
TypeScript array can be introduced as `const waiting = []` with its element type inferred and never
written down, and such an array would evade the count. The check is still worth having — it is the
only witness a rule about what must *not* exist can have — but it is a tripwire, not a proof, and a
reviewer should read the file rather than trust it alone.

## muxws-m5a-fragmentation-and-writer.md — WSM-OBS-001/003, WSM-RCN-045, the TypeScript caps suite

**What I needed:** a TypeScript home for `muxws/observability.py`, and a TypeScript spelling for the
`conftest.py` fixtures `muxws/caps_test.py` is written against.

**What the brief says:** §3 lists `ts/observability.ts` as a file to create and §7 names fourteen
tests, several of them **(spec)**-marked and therefore required to match "character for character".
It says nothing about what a TypeScript observability module contains, and nothing about `Lone` -
which is a `conftest.py` fixture, not a milestone artefact, and so appears in no brief at all.

**What I assumed:** four things.

- **`ts/observability.ts` takes the level-filtered `console` shim as well as the frame line.**
  Python's `observability.py` holds `CloseReason` and `log_frame` and gets its logger from the
  standard library; TypeScript has no logging module it may depend on (WSM-PKG-003), so the shim that
  stood in for one has always lived in `ts/peer.ts` - with a comment saying M5a's observability module
  would take ownership of it. It now does, together with `CloseReason`, which mirrors Python's module
  layout exactly. Both are re-exported from `ts/peer.ts` so no existing import site moved.
- **`withinPayloadCap` is synchronous where Python's `_within_payload_cap` is a coroutine.** Python
  awaits only because `_reset_stream` is `async`; the TypeScript `resetStream` is not, and there is
  nothing else in the method to suspend on. Making it `async` would have made `continueOpen` and
  `onData` async for no reason and put a suspension point between the cap decision and the reset.
- **`Lone` is a class in `ts/caps.spec.ts`, not a shared fixture.** vitest has no `conftest.py`, and
  the only other spec that injects raw frames (`ts/peer.spec.ts`) already carries its own `Pair` with
  an `inject` on it rather than importing one. A second harness module for one consumer would be
  indirection, not reuse; when M5b needs `Lone` it should move to a shared spec helper then.
- **Spec-marked test names are prose `it(...)` descriptions carrying the rule id, not the Python
  function name.** This is the convention every TypeScript spec in the repository already follows
  (`ts/fragment.spec.ts` mirrors `test_slice_point_sweep_never_exceeds_cap` as "never exceeds the cap
  - ..."), and a `snake_case` string inside `it()` would be unreadable in vitest's reporter for no
  gain; the brief's "character for character" requirement is about the Python names, which do match.
  Where the prose had to differ from a literal translation - vitest's `printWidth: 120` does not fit
  `refuses an open beyond the receiver limit and the opener raises nothing locally` on one line - the
  Python name is written out in a comment directly above the test, so the two suites can still be
  diffed test for test.

## muxws-m5a-fragmentation-and-writer.md — WSM-FRG-019, a gap in my own verification

**What I needed:** to know that M5a was actually finished in TypeScript before saying so.

**What the brief says:** the writer selects by round-robin, and a FIFO send queue MUST NOT be used.

**What went wrong:** `ts/writer.ts` landed with thirteen passing tests, `npx vitest run` was green,
and I committed M5a as complete in both languages. It was not. `ts/peer.ts` still sent through a
private `AsyncQueue`, so the rotation - the entire point of the milestone - was not on the
TypeScript send path at all. Every test passed because they all tested the writer *in isolation*,
and nothing asserted that the peer used it.

**What I assumed, and the correction:** that a green suite plus a component's own tests means the
component is wired in. It does not. `ts/writer.spec.ts` now carries an end-to-end case - two streams
through a real `Peer` pair, one fragmenting a 40 kB payload and one sending 200 bytes - and the small
frame must reach the wire within four frames. Replacing the rotation with a genuine FIFO fails that
test and two others; the earlier isolated tests alone did not notice the peer bypassing the writer
entirely. The same shape of blind spot applies to any port: test the seam, not only the part.

## muxws-m5b-reconnect-and-registry.md — WSM-RCN-011, the close code a locally-declared-dead socket carries

**What I needed:** the WebSocket close code to send when *this* peer decides the socket is dead —
after a swallowed pong (WSM-RCN-011), or after a hello that was never acknowledged (WSM-RCN-026).

**What the brief says:** "the socket MUST be declared dead, closed locally". It names no code, and
neither does the spec.

**What I assumed:** 1000. 1006 is reserved for "closed abnormally" and a peer may never put it on the
wire; `websockets` rejects it outright (`Close(1006, ...).check()` raises `ProtocolError`). The first
implementation chose 1006 in Python, the rejection was swallowed by the surrounding `except`, and the
socket therefore stayed **open** — so `serve()` never returned, the supervisor never saw the loss, and
WSM-RCN-011's re-dial never happened on a real transport. Nothing caught it because every driver test
dialled the in-memory rig, whose `close()` discards the code, and the one test that used a real server
had the heartbeat disabled. The rule needs the code stated, and a locally-declared death needs a named
seam (`Peer._close_socket_locally` / `closeSocketLocally`) rather than a raw `socket.close` at each
call site.

## muxws-m5b-reconnect-and-registry.md — WSM-RCN-006, which exception a failed hello raises

**What I needed:** the exception class `connect()` raises when the hello times out or is reset.

**What the brief says:** `connect()` MUST raise "with the underlying error". It does not say what the
underlying error of a hello failure is, and the two ports independently chose differently —
`ConnectionClosed` in Python, `StreamTimeout` / `StreamReset` in TypeScript.

**What I assumed:** the underlying error, unwrapped: `StreamTimeout` for the deadline and the raw
`StreamReset` for a reset hello. A timeout wrapped in a connection close is not the underlying error,
and WSM-ERR-002 already makes both of those stream-shaped failures — which is what a hello is.

## muxws-m5b-reconnect-and-registry.md — WSM-RCN-043, `is_open` during the hello window

**What I needed:** whether a connection whose socket is open but whose hello has not been acknowledged
counts as open.

**What the brief says:** WSM-RCN-043 — `is_open` MUST be false "for the whole window between a socket
loss and the next **established** connection" — and WSM-RCN-004 defines established as socket **and**
hello ack. WSM-RCN-023 separately requires the hello to precede every application frame. But the brief
also lists `is_open` among the things a socket-open sets, and its §6 note says "for a peer with no
hello, established really is socket-open", which reads as if socket-open were the flag.

**What I assumed:** the rules win over the note. `is_open` is socket-open **and** established, so
during the hello window `open()` raises `ConnectionLost` and `notify()`/`request()` reject — which is
also the only way WSM-RCN-023 can be enforced rather than hoped for. A peer with no hello is
established at socket-open, exactly as the note says, so nothing changes for it. This needed a private
allocate-and-enqueue path for the driver's own hello, since it goes through `open()` itself; that path
keeps WSM-SID-006's indivisibility.

**What it cost, found afterwards:** `Stream.reset()` in both ports read `peer.is_open` meaning *"is
there a wire I can put this frame on"*, which is a different question from *"may the application start
something here"* — and narrowing `is_open` silently changed the answer to the first one. A stream the
acceptor pushed inside the hello window and this side then reset was failed locally with **no `reset`
on the wire**, leaving the remote holding a stream this peer had already closed (WSM-STM-021). Both
ports now have an explicit `_has_a_socket` / `hasASocket` for the wire question, and `is_open` answers
only the application's. The lesson is narrower than the rule: a predicate that two call sites read for
two different questions cannot be narrowed for one of them without checking the other.

## muxws-m5b-reconnect-and-registry.md — WSM-RCN-040/044, `max_attempts = 0` and double reporting

**What I needed:** what `max_attempts=0` means, and how many `will_retry=False` closes a peer may fire.

**What the brief says:** WSM-RCN-040 — `on_close` fires on every socket loss, exactly once per loss,
with `will_retry` false only when the cap is exhausted or `close()` was deliberate. WSM-RCN-044 —
exhaustion fires `on_close` once with `will_retry` false. Nothing reconciles the two when the loss
that exhausts the cap is itself the last one.

**What I assumed:** at most one `will_retry=False` close per peer, ever; whichever path reaches it
first wins and the other is suppressed. `max_attempts=0` therefore means the first loss reports
`will_retry=False` and no dial follows — not a silent return, which is what one port did.

## muxws-m5b-reconnect-and-registry.md — an application callback must not be able to kill the driver

**What I needed:** what happens when an `on_reconnect` or `on_close` handler raises.

**What the brief says:** nothing. It specifies when the callbacks fire and what they guarantee, not
what their failure costs.

**What I assumed:** the conservative option — every application callback is isolated, logged, and the
next handler still runs. A throwing `on_reconnect` handler unwound into the supervisor and stopped it
permanently: the peer then looked alive and never dialled again, with no error anywhere, which is the
same silent-spinner failure WSM-INV-011 exists to prevent one level up.

## muxws-m3-transports.md — `connect()` was never implemented in TypeScript

**What I needed:** the TypeScript dialer entry point, which is where the reconnect helper lives.

**What the brief says:** m3 §4.5 declares `export function connect(url: string, options?:
ConnectOptions): Promise<Peer>` and says `ConnectOptions` accepts `hello`, `helloHeaders`,
`reconnect`, `pingIntervalMs`, `pingTimeoutMs`, `helloTimeoutMs`, `codec`, `onStream`, `onClose`,
`onReconnect` — "accepted and stored in M3 but not acted on".

**What went wrong:** M3 shipped `BrowserSocket.connect` and `accept`/`serve`, and no `connect()` at
all. Nothing noticed, because M3's own tests exercised the socket adapter rather than the factory, and
the Python port — which does have `connect()` — kept the milestone looking symmetric. M5b had to build
it before it could put a reconnect helper in it.

**What I assumed:** m3 §4.5's declaration verbatim, including all three callbacks. Python's `connect()`
gained the same three, which m3 §4.5's Python declaration does not list — that declaration does not
list `max_payload_bytes` either, so it was never the exhaustive signature. Without `on_stream` at
`connect()` there is no way to register a handler before the first hello, and an acceptor that pushes a
stream on the hello is answered `reset(REFUSED, "no on_stream handler")` in one port and served in the
other.

## muxws-m5b-reconnect-and-registry.md — WSM-RCN-003/011, how far the injected clock reaches

**What I needed:** to test the heartbeat's `ping_interval + ping_timeout` detection bound "against an
injected clock", as WSM-RCN-011's named test requires.

**What the brief says:** the schedule MUST be a pure function of `(attempts, options, draw)` tested
against an injected clock and random source (WSM-RCN-003), and the heartbeat test must not wait on
anything resembling a TCP timeout (WSM-RCN-011).

**What I assumed:** the injection reaches as far as the code this library owns. The backoff schedule is
genuinely pure and is tested with an injected draw, and the heartbeat's *idle* arithmetic runs off an
injected clock and an injected sleep. The pong deadline itself is `asyncio.wait_for`, which no
injection reaches without reimplementing it, so the end-to-end bound is asserted against the real clock
with interval and timeout in the tens of milliseconds. That waits on nothing TCP-shaped, which is what
the rule is protecting; a fake clock wrapped around `wait_for` would test the wrapper.

## muxws-m3-transports.md — WSM-CDC-022, a rule the test rig could not see

**What I needed:** to know whether the acceptor really answers HTTP 400 when the offered codec does
not match.

**What the brief says:** D1, verbatim — "Under `websockets`, `select_subprotocol` returns `None` and
the library answers the upgrade with 400."

**What is actually true:** returning `None` means *"no subprotocol selected"* and `websockets`
completes the handshake with **HTTP 101**. Only an `InvalidHandshake` subclass out of that hook
produces a 400; anything else renders 500. `ws` behaves the same way — `handleProtocols` returning
`false` answers 101 with no subprotocol, and cannot refuse at all. Both ports therefore violated
WSM-CDC-022 from M3 until M6, and the docstring in `muxws/transports/websockets_.py` asserted the
behaviour that was missing.

**Why it survived three milestones:** every test dialled with the **same language's** dialer, which
recovers through WSM-CDC-028's post-handshake check and raises `CodecMismatch` anyway. The outcome was
right in the only configuration ever tested. A **cross-language** dial is where it shows: `ws`'s client
rejects with `Server sent no subprotocol`, a message containing no `400`, so the dialer's `/\b400\b/`
heuristic missed and a bare error propagated — taking WSM-CDC-024 down with it.

**The lesson, which is the same one WSM-RCN-011 taught with close code 1006:** a witness for a rule
about an HTTP status has to be an HTTP request. No peer-level test can see a status code, and the
in-memory and same-language rigs structurally could not fail. When a rule is about something the test
transport discards, the rule is untested no matter how many tests mention it.

**What I assumed:** `NegotiationError` in Python (verified: status 400) and a second exported hook in
TypeScript, `refuseMismatchedUpgrade`, wrapping `shouldHandle` (verified: status 400; `verifyClient`
also works but is deprecated in `ws` 8). WSM-CDC-027 names `select_subprotocol` for Python and nothing
for `ws`; one hook cannot both select and refuse, so the second export is forced and the rule should
say so.

## muxws-m6-conformance.md — WSM-TST-001, why `json_wire` must be authored by hand

**What I needed:** to know whether the "authored by hand, not generated from our own encoder" rule
buys anything, or is ceremony.

**What the brief says:** it does not say this at all. The rule was written into
`conformance/README.md` while pinning the schema, and then tested.

**What it buys, concretely:** a mutation that makes `from_mapping` clamp every reset code outside
{0,1,2,3,4} to `PROTOCOL_ERROR` survives both `decode(json_wire) == frame` and
`decode(encode(frame)) == frame` — the fixture's `frame` is itself built through the mutated
`from_mapping`, so both sides of each comparison are clamped together and agree. Only the pinned wire
disagrees. Had `json_wire` been generated as `encode(from_mapping(frame))`, a peer silently rewriting
TIMEOUT, PAYLOAD_TOO_LARGE and INTERNAL_ERROR as PROTOCOL_ERROR would have passed the entire corpus
green. The hand-authored wire is the only information in the corpus that the code does not already
contain.

## muxws-m6-conformance.md — the corpus was destroyed mid-milestone, by the same command as in M5a

**What went wrong:** an auditor's mutation-probe script reverted files with `git checkout -- <path>`.
That is safe for files nobody has edited, and destructive for a tracked file with uncommitted changes —
which `conformance/frames/v1-frames.json` was. The M6 extension from 19 triples to 44 was lost and was
unrecoverable: no editor backup, no dangling git blob.

**What made it worse:** `test_json_wire_is_frozen` then failed, correctly, because the corpus had
changed. I read that as "the digest is stale after the extension" and re-pinned it — to the **damaged**
corpus. A correct red signal became a false green, and the suite passed because I had made it pass.

**What I assumed, and the correction:** that a failing freeze test means the digest needs updating. It
means the corpus changed, and the first question is *which way*. The digest was re-pinned only after the
25 triples were rebuilt and each one was shown to catch the mutation it existed for. `git checkout` and
`git restore` are now forbidden in every agent instruction in this project; a mutation probe copies to
a scratch directory and copies back.

## muxws-m6-conformance.md — WSM-CDC-007, what "running the sequence corpus" can mean cross-language

**What I needed:** to know what a live cross-language corpus run *is*, given that a sequence fixture
scripts **both** peers and a cross-language run puts each peer in a different process and language.

**What the brief says:** every codec that ships MUST have a live cross-language pair in CI, "running
the sequence corpus". It does not say how a two-sided script is driven from two processes.

**What I assumed:** a conductor that drives both processes through the corpus, emitting the fixture
names it exercised and asserting that count against the number the in-process runners pin — so a
regression to "it ran nothing" fails the job rather than reporting success. The four legs run 12, 12,
13 and 13 fixtures (the JSON legs skip the bytes fixture with a recorded reason). Proven to have teeth:
making a raising handler answer `INTERNAL_ERROR` instead of `APPLICATION_ERROR` in either port fails
the leg driven by the other.

## muxws-m6-conformance.md — a hollow assertion and a hollow comparison

**What I needed:** confidence that the conformance tests can fail.

**Two that could not, both found by audit rather than by the suite:**

`muxws/conformance_schema_test.py` asserted `parsed == frame or parsed["type"] == frame["type"]`. The
right disjunct is true for every well-formed triple, so the comparison beside it could never fail the
test. It read as proof of round-tripping and proved only that the wire parses. Narrowed to what it
honestly checks — shape — with the semantic check left where the codec is.

`ts/conformance.spec.ts` compared payloads with vitest's `toEqual`, which reports two `ArrayBuffer`s of
equal length as **equal regardless of contents**. The bytes fixture would have shipped unable to detect
a peer delivering the right number of entirely wrong bytes. Both now use a structural comparison with an
`ArrayBuffer` branch.

**The pattern:** in a conformance milestone the dangerous failure is not a test that fails, it is a
test that cannot. A corpus that passes against anything is worse than no corpus, because it is read as
proof.

## Decisions taken alone, and what became of them

I first wrote this section as eleven open questions for the author. That was the wrong instinct, and
the author said so: these are internal implementation choices, not decisions a consumer of the library
has any stake in, and surfacing them as dilemmas made ordinary engineering judgement look like
paralysis. The standing instruction is now: **where I know the right answer, take it; where a decision
changes what a user of the library must know, write the consequence into the documentation rather than
into a question.**

Applying that filter to the eleven below leaves **exactly one** that a consumer can be bitten by, and
it is item 2 — a Node acceptor that does not install `refuseMismatchedUpgrade` still completes the
handshake where the rule requires a refusal. That one belongs on `docs/guide/transports.md` next to the
`ws` snippet, stated as a consequence rather than as a rule id. The rest are recorded here for the
record and resolved by me.

Items 3-9 are decided and implemented as described. Items 1, 10 and 11 are cost, not doubt: they are
work someone has to do, and the entry says how much.

**1. `SPEC.md` cites 39 rule ids that no test resolves to.** Out of 216. Appendix B names exactly those
39 and no more, so the document is honest about it — but a rule with no witness that could fail is a
wish, not a rule. The choice is: write the missing 39 witnesses, or retire the ids that turn out to
describe nothing checkable. My instinct is that some of them are genuinely untestable in-process
(browser behaviour, cross-process deployment shape) and should say so in the rule text itself rather
than sit in an appendix.

**2. `refuseMismatchedUpgrade` is public API that no brief names.** WSM-CDC-027 names
`select_subprotocol` for Python and nothing for `ws`. One hook cannot both select and refuse, so the
second export was forced by the platform. It is **opt-in**: every existing
`new WebSocketServer({ handleProtocols })` in the wild still answers 101 and still violates
WSM-CDC-022. Either the rule should name it, or `accept()` should refuse to serve a socket whose
server never installed it.

**3. `serve()` behaves differently in the two ports on a refused handshake** — Python swallows
`CodecMismatch` and returns, TypeScript rejects. Unreachable now that the 400 is in place, so no test
can see it. One API, two behaviours, is still a defect in a library whose whole premise is one protocol
from one repository.

**4. `peer.ping()` guards on the raw socket flag, not on `is_open`.** Both ports agree, so there is no
divergence — but a public call succeeds on a peer that reports `is_open is False` during the hello
window. Deliberate (the heartbeat must be able to ping) or an oversight, depending on whether a ping
counts as "starting something".

**5. `_give_up()` in the cap-exhausted branch is unreachable-with-effect in both ports.** Proven by
mutation: deleting it leaves every test green, because at `max_attempts=0` the loss itself already
latched the `will_retry=False` report. Kept for structural parity between the ports. A future reader
will believe it is load-bearing.

**6. `_final_close_reported` is never reset, including by `_adopt_socket`.** Literal reading of "at most
one `will_retry=False` close per peer, ever". For a helper-driven peer the two rules cannot conflict;
for a bare `Peer` handed a new socket after such a close, the second loss is silent. Only tests do that
today.

**7. `interop/*.ts` is neither linted nor type-checked** — `tsconfig.json` includes `ts/**/*` only and
`lint:ci` runs `eslint ts`. `runner.ts` is now ~1700 lines of load-bearing WSM-CDC-007 machinery whose
type errors would surface only as runtime failures in CI.

**8. The interop interleaving assertion overstates what it proves.** Removing round-robin entirely does
not fail it — the assertion is satisfied by the handler's sleeps, not by the writer's rotation.
WSM-FRG-019's real witnesses (`writer_test.py` and the `small-frame-overtakes-a-fragmented-payload`
fixture) do catch it, so the rule is covered; the comment claims more than the code delivers. The sharp
version is timing-dependent, and a flaky cross-language assertion would be worse than a weak one.

**9. `ci.yml`'s `interop` job duplicates two of `cross-language.yml`'s scenario jobs.** Harmless, but it
is the same work twice on every push and the two now have to be kept in step.

**10. Docstring coverage — measured, and mostly closed.** *Resolved.* The original count (80 of 201)
was over-broad: it included `__init__`, `__repr__` and other dunders, which carry no docstring by
convention. Narrowed to non-dunder members reachable from the public surface it was **31**, and those
turned out to cluster almost entirely into the two documented extension points — the `Codec` protocol
and the `SocketAdapter` protocol. Both now document every member, including the two things a reader
implementing one of them most needs and could not have known: that `receive` **raises**
`ConnectionClosed` and that this is the peer's only signal that the socket died, and that `close`
must never be given code 1006.

The **19** left are implementations of those two protocols — `JsonCodec.encode`, `MemorySocket.receive`
and so on. They are deliberately left bare: the contract lives on the protocol, and repeating it on
five implementations produces five copies to drift apart. The single implementation that does more
than forward, `StarletteSocket.close`, says so and says why.

**11. The rule count — I had the number wrong, and the correction changes the conclusion.**
*Resolved, by measuring.*

I said repeatedly that the design material carries "1028 rules" and that this is roughly twice what
the protocol needs. **1039 is the number of *occurrences* of a rule id across `docs/design/`, not the
number of rules.** There are **216 distinct ids**, and the repetition is deliberate and documented:
`docs/design/CLAUDE.md` says a brief reproduces the normative text it needs so an implementer never
has to hold three documents open at once. Counting mentions and calling them rules was my error, and
the "twice what it needs" conclusion was built on it.

216 rules for a protocol with six frame types, a five-state stream machine, mandatory fragmentation
with a round-robin writer, a reconnect helper with a hello, a codec seam and a peer registry is not
obviously bloated. The distribution is not lopsided either — `RCN` 27, `CDC` 25, `FRG` 23, `API` 22,
`STM` 22, `INV` 18, and a long tail — which is roughly what the subsystems' complexity would predict.

What the measurement does support is a narrower claim, and it is the one worth keeping: **189 of the
216 are cited by at least one test** (a crude grep that does not resolve combined-id shorthand, so the
true figure is better; `SPEC.md`'s own appendix, which does resolve it, says 19 uncited). The useful
question was never "are there too many rules" but "which rules have a witness that can fail" — and
that question is now answered per id in Appendix B, which is a better artefact than a smaller
specification would have been.

## muxws-m8-demo.md — WSM-INV-004 was not true on a fast socket, and only the demo could find it

**What I needed:** a demo panel showing a 1 MB export fragmenting while ticks keep arriving, with the
tick-latency readout staying flat. The brief calls it the headline and says plainly that without the
round-robin writer "the headline demonstration is a lie".

**What the brief and the spec say:** WSM-INV-004 — at most one unsent fragment per stream and
round-robin writer selection, "or a 1 MB payload adds a full second of latency to a 200-byte progress
update on another stream". M5a built the writer and proved it: thirteen tests, a conformance fixture,
and a mutation to a FIFO that fails three of them.

**What was actually true:** the guarantee did not hold. Nothing in `Peer._write_loop` is guaranteed to
suspend — `next_frame()` returns without awaiting when there is work, and a socket whose buffer has
room completes its send without yielding — so the loop drained every fragment of a megabyte in one
uninterrupted run of the task. No other task ran, nothing else could enqueue, and the round-robin had
exactly one lane to rotate between. **The writer was correct and was simply never asked.** Measured on
a memory transport: seven fragments, zero frames of any other stream between the first and the last.
Measured through the demo's real Vite proxy to real uvicorn: eighteen fragments, zero ticks between.

**Why every test missed it:** the writer's own tests drive the writer directly, so the rotation is
observed with the queues already full. The one end-to-end test in `ts/writer.spec.ts` and the demo's
own headline test both interleaved because *their* transport happened to yield — the demo's fixture
even wrapped both ends in a `PacedSocket` that slept in proportion to frame size, with an honest
docstring saying the real transport did not behave that way. The guarantee held only on links slow
enough that backpressure supplied the missing suspension, and every test transport and localhost are
the fastest links there are. This is the same shape as the 1006 close code in M5b and the missing
HTTP 400 in M6: **the rig could not see the thing the rule is about.**

**What I did:** one event-loop turn per frame in both ports — `await asyncio.sleep(0)` in Python, a
`MessageChannel`-based macrotask in TypeScript, because a microtask does not let a timer-driven
producer run and `setTimeout(0)` is clamped to 4 ms by browsers under nesting. Both ports now carry a
test over a *plain* transport that fails without the yield. The demo's `PacedSocket` is gone with the
defect it was compensating for.

**The lesson, which is the whole argument for building the demo:** M8 was the first consumer of muxws
that was not a test, and it invalidated the project's central performance claim within hours. A
library's own suite tests it against the transports the suite owns. Something has to run it against a
real one.

## muxws-m8-demo.md — the splitter renders 20x the payload it is fragmenting

**What I needed:** a 1 MB export that does not stall the sender.

**What is true:** `iter_fragments` asked the codec "does the whole remainder fit as the closing
fragment?" on every pass, rendering the entire remaining payload each time. Measured: 395 encodes and
**42 MB rendered** for a 1.2 MB payload. That cost is synchronous, so it blocks the event loop — the
same latency WSM-INV-004 exists to prevent, arriving by another road.

**What I did:** the probe now skips when the remainder alone exceeds the cap, because an encoded frame
carries the remainder *plus* an envelope *plus* escaping and can never be shorter than it — so the
answer is already known. 42 MB down to 25 MB, and boundary-preserving by construction, which the
frozen corpus confirms in both ports.

**What I deliberately did not do:** the rest is the binary search in `_largest_fitting_count`, which
runs on nearly every fragment because the reservation `min(512, cap // 2)` is far short of what
JSON-inside-JSON escaping costs — 64 KiB of JSON text carries thousands of quotes, each becoming two
bytes. A better reservation would fix it, and **must not be applied**: the reservation decides the
boundary whenever its first guess fits, so changing it cuts in different places. Fragment boundaries
are frozen (WSM-FRG-016, `conformance/frames/`), which makes this a generation concern rather than an
optimisation. `fragment_test.py::test_how_much_encoding_one_megabyte_costs` records the cost as a
ceiling so it cannot quietly get worse.

## muxws-m8-demo.md — `close()` spun at 100% for its whole drain window

**What is true:** `Peer._drain` polled with `await asyncio.sleep(0)`, which is not a pause. Every
stream open at close time belongs to the application, so against a peer that *pushes* — the normal
case for the registry pattern this demo exists to show — `close()` burned ten seconds of CPU flat out
and starved the very tasks that would have ended those streams. Now a 5 ms pause, which costs a close
at most 5 ms of extra latency. TypeScript's `drain` was already event-driven and needed no change;
the two ports had quietly diverged on this and only the Python side was wrong.

## muxws-m8-demo.md — the published artefacts had never been installed

**What I needed:** to know that a consumer who follows the documented install can run the documented
quick start.

**What every check did instead:** ran from the source tree. The Python suite imports `muxws` from the
repository; the documentation examples resolve `muxws` and `muxws/node` through a tsconfig path
mapping to `ts/index.ts`; and the two audits that inspected the wheel looked at its **top level**,
which was correctly `{muxws, dist-info}`.

**What was actually shipped:** inside `muxws/` were every `*_test.py`, `conftest.py`, and `__pycache__`
full of bytecode compiled on this machine. The exclude list sat under
`[tool.hatch.build.targets.sdist]` and not under the wheel target, and `python -m build` hid it
completely by building the wheel *from* the sdist. The only route that could see it was installing the
wheel and looking inside the installed package.

**What I did:** the exclude on the wheel target, plus
`packaging_test.py::test_the_published_wheel_ships_the_library_and_nothing_else`, asserted against the
archive rather than an install because `pip` compiles to `__pycache__` at install time and that is its
business. Then both artefacts verified from clean installs off the source tree, and the documented
quick start run as a reader would run it — a fresh directory, the documented `pip install` and
`npm install` lines, the three documented files — with the output compared to the block printed in the
guide. It matches character for character in both languages.

**The lesson, which is the fourth instance of one shape:** 1006 was hidden by the memory transport,
HTTP 400 by same-language dialling, WSM-INV-004 by a socket that never made the writer wait, and this
by running from source. Every time, the rig could not see the thing — never the code. The question
worth asking at the end of any milestone is not "do the tests pass" but **"what has never been
exercised at all?"**

## muxws-m3-transports.md — WSM-CDC-027 named one hook where two were needed

**What the rule said:** the library MUST expose `select_subprotocol`, a callable installable in the
transport's handshake hook.

**Why that was not enough:** it is true of `websockets` and false of `ws`. `handleProtocols` selects
and *cannot refuse* — whatever it returns, `ws` answers 101 — so a second hook is structurally
required. The rule named only the Python shape, so both ports shipped an acceptor that violated
WSM-CDC-022 for three milestones, and the second hook, when it finally had to exist, was public API no
brief named.

**What I did:** the rule now requires one callable per job where one cannot do both, requires both to
be documented as required rather than optional, and carries the failure its own absence caused. The id
is unchanged.

## A gap in my own mutation method, found while closing the last one

**What I was doing:** proving a new test can fail, by editing a library file, running the test, and
copying the file back from a scratch copy.

**What went wrong:** after one restore the suite kept failing against a file `git diff` reported as
identical to HEAD. The source *was* restored; Python was running the **cached bytecode** compiled from
the mutated version. I spent a cycle reading the failure as a real defect before checking
`__pycache__`.

**What it means for everything before it:** a mutation whose restore left stale bytecode would leave
the *next* run red, which is loud and gets noticed. The dangerous direction is the other one — a
mutation that appeared not to be caught because the test ran against pre-mutation bytecode, reported
as "this test has no teeth" when it does. I re-ran the one mutation in this round that had looked like
a survivor with the cache cleared, and it was a genuine survivor for a different reason (below); but
earlier rounds in this project used the same method, so any single "mutation survived" finding from
them is worth one re-check before being believed.

**The method now:** clear `__pycache__` after every restore, and never read a post-restore failure as
a finding without checking `git diff` first.

## `sequences_property_test.py` — what the enumerated tests structurally could not reach

Every other test in this suite enumerates: `stream_test.py` walks the forty-five state-table cells one
at a time, `conformance/sequences/` scripts thirteen exchanges someone thought of. Both are necessary
and neither can find the ordering nobody wrote down — which, on this project's record, is where the
defects were.

Two things the property test taught while being written, both about invariants rather than about the
library:

**An invariant asserted against a sequence that cannot produce a violation is not an invariant.** The
first version asserted that reset code 9 never reaches the wire, and a mutation removing that guard
left it green. Socket death synthesises `CONNECTION_CLOSED` *locally* and never enqueues a frame, so
the only path by which code 9 can reach the wire is an application asking for it explicitly — and the
sequence never did. The sequence now occasionally asks, and the mutation fails four seeds.

**Catching the documented failures narrowed the test to what it had already filtered for.** The first
version caught `StreamReset`, `ConnectionLost` and `ProtocolError`, and then asserted that nothing
non-muxws escaped — an assertion the `except` clause had made true by construction. It now records
whatever comes out and judges afterwards, which is the same test with teeth.
