---
layout: home

hero:
  name: muxws
  text: Many streams, one WebSocket
  tagline: HTTP/3 stream semantics over a WebSocket - one wire format, any number of ports.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: API Reference
      link: /api/connect
    - theme: alt
      text: GitHub
      link: https://github.com/velis74/muxws

features:
  - title: HTTP/3 semantics, on a WebSocket
    details: The stream model HTTP/2 and HTTP/3 already settled - many independent streams, either end able to open one, cancellation per stream - without needing QUIC or an HTTP/3 stack.
  - title: Symmetric peers
    details: One Peer type per language. Server push is the same mechanism as a client request.
  - title: Automatic fragmentation
    details: Payloads above the 64 KiB frame constant are split and reassembled for you, with a round-robin writer.
  - title: Reconnect with hello replay
    details: Jittered backoff, a heartbeat that bounds detection, and an opening payload replayed on every connection.
  - title: Pluggable codec
    details: JSON by default, msgpack optional, your own if you want one - asserted at the handshake.
  - title: Ports, not bindings
    details: Python and TypeScript today, each a full implementation of the same frozen wire. A third is a normative spec and 65 shared fixtures away - the corpus exists so nobody has to read either port's source to write one.
---
