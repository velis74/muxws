# TODO

## 0.5.0: local backpressure

The wire stays as it is. WSM-BPR-001 forbids flow control on the wire in v1; a local budget sends
nothing and needs no new generation. WSM-BPR-002 lists the v1 mechanisms exhaustively and has to be
amended first: a proposal for the new wording comes before the implementation.

- **Send side.** A byte budget for the frames a stream has queued in the writer, and one for the
  whole connection. `send()`, `send_headers()`, `end()` and `reply()` wait until the stream and the
  connection are under budget, and suspend on every call. A Python `while True: await
  stream.send(x)` loop never suspends: the write loop does not run, nothing reaches the socket and
  the writer's queue grows without bound.
- **Receive side.** A byte cap on the payloads a stream holds that nobody has read yet. A stream over
  the cap is reset; the code is one of the nine v1 codes and has to be chosen.
- **Both ports**, with defaults, units and the behaviour at the limit documented in the API
  reference and in `docs/guide/sizes-and-fragmentation.md`.

## 0.5.0: latency under load

- A benchmark of the p95/p99 latency of a small request while a large payload is in transit, and of
  peak memory with a slow consumer. `demo.py --bench` measures throughput only.
- The codec encodes a whole payload before the writer slices it, so a large encode blocks the event
  loop for its whole duration. The benchmark measures that cost.
