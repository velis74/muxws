---
layout: home

hero:
  name: muxws
  text: Many streams, one WebSocket
  tagline: A framing, multiplexing, stream-lifecycle and cancellation protocol, in Python and TypeScript.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: API Reference
      link: /api/
    - theme: alt
      text: GitHub
      link: https://github.com/velis74/muxws

features:
  - title: Many streams on one socket
    details: Independently addressed, independently cancellable, bidirectional streams sharing a single connection.
  - title: Symmetric peers
    details: One Peer type per language. Server push is the same mechanism as a client request.
  - title: Automatic fragmentation
    details: Payloads above the 64 KiB frame constant are split and reassembled for you, with a round-robin writer.
  - title: Reconnect with hello replay
    details: Jittered backoff, a heartbeat that bounds detection, and an opening payload replayed on every connection.
  - title: Pluggable codec
    details: JSON by default, msgpack optional, your own if you want one - asserted at the handshake.
  - title: Two languages, one wire
    details: Python and TypeScript ports proven against a shared conformance corpus in CI.
---
