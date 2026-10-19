# Implementing from these briefs

This directory contains the milestone briefs for **muxws**, a WebSocket transport with HTTP/3-like
semantics. It ships a Python package and a TypeScript package from this one repository, and this is
where it is implemented.

`README.md` lists every brief in dependency order and says what each one delivers. Start there.

## The documents

| Document | Contents | When to open it |
|---|---|---|
| `briefs/muxws-m<N>-*.md` (a brief) | The work for one milestone and its acceptance criteria. | Always. It is the work order. |
| `muxws-spec.md` | The normative rules of the finished system. | When two briefs disagree, or a rule's exact wording matters. The spec takes precedence. |
| `muxws-websocket-transport.md` (the design doc) | The rationale for each rule. | Before proposing a change to a rule. Do not implement from it. |
| `README.md` | The order of the briefs. | Between milestones. |

A brief reproduces the normative text it needs, so it can be implemented without the spec open. If
a brief and the spec disagree, the spec takes precedence: record the disagreement as a gap (see
*Gaps*, below) instead of choosing one.

## Rules

**One brief is one unit of work.** Read it end to end before writing any code. Work listed in its
*Out of scope* section belongs to a later milestone; do not start it.

**Do not weaken a MUST.** Several MUSTs exist because a simpler implementation breaks something, and
the brief states the failure each one prevents. If a MUST looks wrong, impossible, or untestable as
written, stop and ask. Do not downgrade it to a SHOULD, implement an approximation of it, or leave a
TODO.

**Tests are the acceptance criteria.** Each brief names the tests to write and what each asserts.
Write them and run them; a milestone with failing tests is not complete.

**Do not invent.** If the brief does not cover something you need, record it as a gap, choose the
most conservative option, and state the assumption in the gap entry.

**Do not refactor code from earlier milestones.** If you believe earlier code is wrong, report it
instead of rewriting it; later briefs depend on its tests.

**Do not add dependencies** beyond those the brief names. The core has no required runtime
dependencies; everything else is an optional extra, and the full test suite must install and pass
with none of the extras present.

## How to work a milestone

1. Read the whole brief, then re-read *Normative rules in force* and *Done when*.
2. Confirm that the prerequisites the brief lists exist in the repository. If they do not, stop: the
   brief's prerequisites are not met.
3. Create or modify only the files the brief lists.
4. Write the implementation.
5. Write every test named in *Tests to write*.
6. Run the *Done when* checklist. Every command in it must pass: lint, type-check, tests, coverage.
7. Then move on.

## Lint and layout conventions

These are enforced by the linters configured in the M0 scaffolding milestone. Several differ between
the two languages:

- **Quotes differ by language.** Python uses double quotes (ruff `Q`); TypeScript uses single quotes
  (prettier `singleQuote: true`). Both at 120 columns.
- **TypeScript filenames are kebab-case** (`stream-state.ts`, not `streamState.ts`), enforced by
  `unicorn/filename-case`.
- **Python imports** put a blank line between `import x` and `from x import y` within a group
  (isort `lines-between-types = 1`), and use `X | None` rather than `Optional[X]` (ruff `UP`).
- **Unused parameters are underscore-prefixed** in Python (`_request`, `_cls`), ruff `ARG`.
- **`for...in` is forbidden** in TypeScript (`no-restricted-syntax`); use `Object.keys/values/entries`.
- **`assert` is banned outside test files** (ruff `S101`), so test modules must be named `*_test.py`
  for the per-file ignore to apply.
- **`random` triggers `S311`.** The reconnect jitter uses `random` with `# noqa: S311` and a comment
  that jitter is not security-sensitive. Do not use `secrets` for it.
- **FastAPI `Depends()` in a default argument triggers `B008`.** Use `# noqa: B008`.
- **Tests live next to the code they test**: `frames.py` / `frames_test.py`, `codec.ts` /
  `codec.spec.ts`. There is no `tests/` directory.

## Gaps

`GAPS.md` at the repository root lists open gaps: a brief that is silent, ambiguous,
self-contradictory or contradicted by the spec, a rule without a complete witness, and known
deviations of the implementation from the spec. Each entry names the rule id, if any, and states the
gap as a present fact together with the option the code implements. An entry is deleted when the gap
is closed. `GAPS.md` records no history of decisions.

## When to stop and ask

- A MUST cannot be satisfied, or its named test cannot be written as described.
- Two briefs contradict each other on something the spec does not resolve.
- A milestone's *Done when* cannot pass without work the brief places out of scope.
- You are about to change a public API signature that a later brief depends on.

Ask before implementing around any of these.
