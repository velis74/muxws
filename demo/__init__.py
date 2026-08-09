"""The muxws demo: a market board in which every claim the protocol makes has a witness on screen.

It is a **consumer** of the shipped packages and is a prerequisite for nothing. Nothing under
`muxws/` imports anything from here, `[tool.hatch.build.targets.wheel]` packages only `muxws`, and
the npm package ships only `dist/*` - so the demo cannot reach either published artefact.
"""
