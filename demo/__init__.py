"""The muxws demo: a market board that shows the protocol's features in a browser.

It uses the published packages and nothing depends on it. Nothing under `muxws/` imports from here,
`[tool.hatch.build.targets.wheel]` packages only `muxws`, and the npm package ships only `dist/*`, so
the demo is in neither published artefact.
"""
