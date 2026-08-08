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
