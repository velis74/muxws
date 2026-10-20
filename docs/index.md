---
title: 'WebSocket multiplexing for Python and TypeScript'
description: 'HTTP/2-style streams over one WebSocket: either end opens them, each cancels alone, large payloads don''t delay small ones. Python and TypeScript.'
layout: home

hero:
  name: muxws
  text: Multiplexed streams over one WebSocket
  image:
    src: /muxws-icon.svg
    alt: muxws
  tagline: Either end can open a stream. Each stream can be cancelled independently. Large payloads are fragmented, so they do not delay small ones. muxws provides the multiplexing layer that applications otherwise implement themselves.
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
  - title: HTTP/2 and HTTP/3 semantics, on a WebSocket
    details: The stream model of HTTP/2 and HTTP/3 - many independent streams, either end can open one, per-stream cancellation - without a QUIC or HTTP/3 stack.
  - title: Symmetric peers
    details: One Peer type per language. Server push is the same mechanism as a client request.
  - title: Automatic fragmentation
    details: Payloads above the 64 KiB frame cap are split and reassembled automatically, and a round-robin writer interleaves the fragments of concurrent streams.
  - title: Reconnect with hello replay
    details: Jittered backoff, a heartbeat that bounds the time to detect a dead connection, and an opening payload replayed on every connection.
  - title: Pluggable codec
    details: JSON by default, msgpack optional, custom codecs supported. The codec is part of the subprotocol and is checked at the handshake.
  - title: Independent implementations
    details: Python and TypeScript, each a complete implementation of the same frozen wire protocol. A normative specification and a shared conformance corpus define the protocol, so a further implementation does not depend on either existing implementation's source.
---
