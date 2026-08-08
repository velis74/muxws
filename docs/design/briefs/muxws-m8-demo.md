---
title: muxws M8 - the demo application
sidebar: false
search: false
outline: deep
---

# muxws M8 - a demo that makes the protocol visible

## 1. Goal

At the end of M8 the repository ships a runnable demo: a FastAPI backend and a Vue 3 + Vuetify
frontend, started with one command, in which **every distinguishing claim muxws makes is something a
reader can watch happen**. It is a market board - a live price grid, a detail panel, and a
diagnostics strip - chosen not for the subject matter but because a market board is the smallest
honest application that genuinely needs all four call shapes at once.

This milestone was not in the specification's §17 list. It exists because the documentation site
(M7) can describe the round-robin writer, and only a running screen can show that a 1 MB export does
not stall a 200-byte tick.

## 2. Prerequisites

M0-M7 are done and the wire is frozen. In particular the demo leans on:

- **M2** - `open`/`request`/`notify`, the awaitable-and-iterable `Stream`, `cancel()`.
- **M3** - the Starlette acceptor and the browser dialer; the `muxws.v1.json` subprotocol.
- **M4** - `peer.ping()` for the latency readout, `peer.close()` for an orderly shutdown.
- **M5a** - **the round-robin writer**, without which the headline demonstration is a lie.
- **M5b** - the reconnect helper and `hello` replay, without which the "kill the backend" button
  shows nothing worth watching, and `PeerRegistry`, which is how the backend finds the peers to push
  to.

The demo MUST NOT be a prerequisite for anything. It is a consumer of the shipped packages and is
excluded from the published artefacts.

## 3. Files to create

```
demo.py                              # one entry point: uvicorn + the vite dev server
demo/__init__.py
demo/backend/__init__.py
demo/backend/main.py                 # the FastAPI app and its one WebSocket route
demo/backend/market.py               # the tick generator and the fake order book
demo/backend/handlers.py             # the on_stream handler: every action the frontend can ask for
demo/frontend/package.json           # a workspace, like the reference repository's
demo/frontend/index.html
demo/frontend/vite.config.ts
demo/frontend/tsconfig.json
demo/frontend/src/main.ts
demo/frontend/src/App.vue
demo/frontend/src/muxws.ts           # one connect(), shared; the only place the peer is built
demo/frontend/src/components/BoardGrid.vue
demo/frontend/src/components/SymbolDetail.vue
demo/frontend/src/components/Diagnostics.vue
demo/backend/handlers_test.py        # the handlers, against the in-memory transport
```

Root `package.json` gains `demo/frontend` to `workspaces` and a `demo:dev` script, exactly as
`@dynamicforms/fastapi-viewsets` does. `pyproject.toml` gains a `demo` extra
(`fastapi`, `uvicorn`) and `[tool.coverage.run] omit` gains `demo/*`.

## 4. What each panel demonstrates

The point of the layout is that no panel is decoration: each one exists because a rule needs a
witness.

| Panel | What the user does | What it proves |
|---|---|---|
| **Board grid** | nothing - rows update on their own | Server push. The backend calls `peer.open()` on its own initiative, and the browser receives it through *its* `on_stream` handler. One `Peer` type, one mechanism, no second correlation story (WSM-INV-002). |
| **Symbol detail** | clicks a row | `await peer.request({action: 'quote', symbol})` - the unary shape (WSM-API-006). |
| **History chart** | the detail panel opens | `for await (const point of peer.open({action: 'history'}))` - a streaming response filling in progressively (WSM-API-002). |
| **Switching symbols** | clicks another row mid-load | `cancel()` on the outstanding history stream. The backend handler observes `asyncio.CancelledError` and stops generating (WSM-ERR-012/013). The counter of cancelled-server-side generations is shown, so the reader sees the *backend* stopped, not just the frontend looking away. |
| **Order book** | presses "full depth" | A payload comfortably over `MAX_FRAME_BYTES`, fragmented and reassembled without the application doing anything (WSM-FRG-010). |
| **The stall test** | presses "1 MB export" | **The headline.** A large payload starts fragmenting on one stream while ticks keep arriving on twenty others. The tick-latency sparkline stays flat. This is WSM-INV-004 made visible, and it is the one thing no static document can show. |
| **Diagnostics** | watches | `peer.on_frame` feeding a rolling frame counter, bytes/second, ticks/second, `peer.ping()` RTT, live stream count, and the connection state. |
| **Kill the backend** | presses it | The backend closes every peer. Every open stream raises `ConnectionLost` and the UI says so rather than freezing; the reconnect helper re-dials on a jittered delay, replays its `hello`, and `on_reconnect` fires once. Streams do **not** come back, and the UI must show that it re-subscribed rather than pretending nothing happened (WSM-RCN-030/031). |

## 5. Decisions this brief takes

- **D1 - the backend is the only source of truth about symbols.** The frontend holds no seed data.
  A demo that could render without a connection would prove nothing about the transport.
- **D2 - the tick generator is deterministic given a seed**, so two runs produce the same numbers and
  a screenshot in the documentation stays true. `random` with `# noqa: S311` and a comment: a fake
  price is not security-sensitive.
- **D3 - the frontend subscribes by sending a `hello`**, and the backend registers the peer in a
  `PeerRegistry` under the tags it names. This is the documented usage pattern of M5b, and the demo
  is where it is shown working rather than described.
- **D4 - no state management library, no router.** One `App.vue` holding three components and a
  `reactive` store in `muxws.ts`. Every line the reader has to understand before they see muxws is a
  line taxed against the demo's purpose.
- **D5 - Vuetify is a demo dependency and nothing else.** It appears in `demo/frontend/package.json`
  only. `muxws` itself keeps zero runtime dependencies in Python (WSM-PKG-002) and zero in the
  browser entry point (WSM-PKG-003), and a test asserts that the demo's presence has not changed
  that.

## 6. Tests to write

A demo with no tests rots into a screenshot. These are cheap and they are the ones that matter:

| # | Test | Asserts |
|---|---|---|
| 1 | `demo/backend/handlers_test.py::test_every_action_answers` | Each action the frontend can send (`quote`, `history`, `depth`, `export`) is handled over the in-memory transport and returns the shape the frontend expects. |
| 2 | `handlers_test.py::test_cancelling_history_stops_the_generator` | After `cancel()`, the backend generator stops producing - asserted by a counter it increments, not by the absence of frames. |
| 3 | `handlers_test.py::test_the_export_payload_fragments` | The export exceeds `MAX_FRAME_BYTES` and arrives whole. |
| 4 | `handlers_test.py::test_ticks_keep_flowing_during_an_export` | **The headline, as a test.** With an export in flight on one stream, tick frames on another still interleave - asserted from `on_frame`'s record, so it fails if the round-robin writer is ever replaced by a FIFO. |
| 5 | `version_test.py::test_the_demo_adds_no_runtime_dependency` | `dependencies` in `pyproject.toml` is still empty and the browser bundle still imports nothing optional (WSM-PKG-002/003). |
| 6 | `docs` check | The published wheel and the npm package contain no `demo/` files. |

## 7. Done when

```bash
python demo.py                 # backend on :8000, frontend on :5173
```

- [ ] The board renders live ticks with no user action.
- [ ] Clicking a row loads a quote (unary) and a history (streaming, progressive).
- [ ] Switching symbols mid-load visibly stops the backend generator.
- [ ] "1 MB export" runs while the tick-latency sparkline stays flat - **the reason the demo exists**.
- [ ] "Kill the backend" surfaces `ConnectionLost` in the UI, then reconnects, replays the hello, and
      re-subscribes, with the reconnect attempt count visible.
- [ ] `ruff check .`, `pytest`, `npm run lint`, `npm test` and `npm run build` all still pass.
- [ ] `python -m build` produces a wheel with no `demo/` inside it.

## 8. Out of scope

- Real market data, or any network egress. The generator is local and fake.
- Authentication. The demo dials without a credential; where one would go is a comment pointing at
  WSM-AUT-001.
- Persistence, a database, Celery, or anything backchannel-shaped.
- Mobile layout, theming, i18n.
