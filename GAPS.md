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
