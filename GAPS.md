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

**What I assumed:** that the `npm ci` half is the deliberate one, so `package-lock.json` is
committed and line 8 is gone from `.gitignore`. The author's other published packages commit their
lockfile, which settles what the house style is, and `npm ci` is the stronger choice anyway: CI then
builds the exact tree that was tested locally rather than whatever resolves on the day.
