# Implementing from these briefs

This directory contains the milestone briefs for **muxws**, a WebSocket transport with HTTP/3-like
semantics. It ships a Python package and a TypeScript package from this one repository, and this is
where it is implemented.

`README.md` lists every brief in dependency order and says what each one delivers. Start there.

## The three documents, and which one to open

| Document | Answers | When you open it |
|---|---|---|
| `briefs/muxws-m<N>-*.md` (a brief) | What do I build now, and how do I know I'm done? | Always. This is your work order. |
| `muxws-spec.md` | What is true of the finished system? | When two briefs disagree, or a rule's exact wording matters. It is the adjudicator. |
| `muxws-websocket-transport.md` (the design doc) | *Why* is this rule what it is? | Only when you are tempted to change a rule. It exists to change your mind, not to be implemented from. |
| `README.md` | Which brief is next? | Between milestones. |

The briefs deliberately duplicate the spec. That is not an oversight — a brief reproduces the
normative text it needs so you never have to hold three documents open at once. If a brief tells you
to do something and the spec appears to say otherwise, the spec wins and you have found a defect:
record it (see *Gaps*, below) rather than silently picking one.

## Rules of engagement

**One brief is one unit of work.** Read it end to end before writing any code. Do not start the next
milestone because it seems adjacent — every brief has an *Out of scope* section, and it is a real
boundary. Work that belongs to a later milestone will conflict with that milestone's brief.

**Never weaken a MUST.** These rules are load-bearing; several exist because a plausible-looking
simplification breaks something non-obvious, and the brief states the failure it prevents. If a MUST
looks wrong, impossible, or untestable as written, stop and ask. Do not downgrade it to a SHOULD, do
not implement "the spirit of it", and do not leave a TODO.

**Tests are the acceptance criteria.** Each brief names the tests to write and what each asserts.
They are the definition of done, not documentation of work already finished. Write them, run them,
and do not report a milestone complete on a partial pass.

**Do not invent.** If the brief is silent on something you need, that is a gap in the brief, not an
invitation. Record it and pick the most conservative option, stating your assumption in the gap
entry.

**Do not refactor earlier milestones to taste.** Previous milestones passed their own acceptance
criteria. If you believe earlier code is wrong, say so rather than rewriting it — a change there may
invalidate tests a later brief depends on.

**Do not add dependencies** beyond those the brief names. muxws has a deliberate policy of a core
with *zero* required runtime dependencies and everything else behind an optional extra, and it must
install and pass its full test suite with none of those extras present.

## How to work a milestone

1. Read the whole brief. Then re-read *Normative rules in force* and *Done when* — those two sections
   bracket the work.
2. Confirm the prerequisites the brief lists actually exist in the repository. If they do not, you are
   on the wrong milestone.
3. Create or modify only the files the brief lists.
4. Write the implementation.
5. Write every test named in *Tests to write*.
6. Run the *Done when* checklist. Every command in it must pass — lint, type-check, tests, coverage.
7. Only then move on.

## Conventions that will bite you

These are enforced by the linters configured in the M0 scaffolding milestone, and several of them
differ between the two languages in the same repository:

- **Quotes are opposite by language.** Python uses double quotes (ruff `Q`); TypeScript uses single
  quotes (prettier `singleQuote: true`). Both at 120 columns.
- **TypeScript filenames are kebab-case** (`stream-state.ts`, never `streamState.ts`) — enforced by
  `unicorn/filename-case`.
- **Python imports** put a blank line between `import x` and `from x import y` within a group
  (isort `lines-between-types = 1`), and use `X | None` rather than `Optional[X]` (ruff `UP`).
- **Unused parameters are underscore-prefixed** in Python (`_request`, `_cls`) — ruff `ARG`.
- **`for...in` is forbidden** in TypeScript (`no-restricted-syntax`); use `Object.keys/values/entries`.
- **`assert` is banned outside test files** (ruff `S101`), so test modules must be named `*_test.py`
  for the per-file ignore to apply.
- **`random` trips `S311`.** The reconnect jitter needs it; use `random` with `# noqa: S311` and a
  comment that jitter is not security-sensitive. Do not reach for `secrets`.
- **FastAPI `Depends()` in a default argument trips `B008`.** Use `# noqa: B008`.
- **Tests live next to the code they test**: `frames.py` / `frames_test.py`, `codec.ts` /
  `codec.spec.ts`. There is no `tests/` directory.

## Gaps

Keep a `GAPS.md` at the root of the implementation repository. Append an entry whenever a brief is
silent, ambiguous, self-contradictory, or contradicted by the spec. One entry per problem:

```
## <brief filename> — <rule id if any>
What I needed: ...
What the brief says: ...
What I assumed: ...
```

This file is a deliverable. It is how the specification gets fixed, and an empty `GAPS.md` after a
long milestone is more suspicious than a full one.

## When to stop and ask

- A MUST cannot be satisfied, or its named test cannot be written as described.
- Two briefs contradict each other on something you cannot resolve from the spec.
- A milestone's *Done when* cannot pass without doing work the brief places out of scope.
- You are about to change a public API signature that a later brief will depend on.

Ask before implementing around any of these. The cost of a question is minutes; the cost of a
plausible wrong turn is a milestone.
