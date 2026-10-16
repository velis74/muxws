<!--
  Help dialog: what each panel demonstrates about the transport.

  Each section states what the panel shows, which muxws property it demonstrates, and what the panel
  would show if that property did not hold. `docs/guide/` documents the mechanisms.
-->
<template>
  <v-dialog v-model="open" max-width="820" scrollable>
    <template #activator="{ props: activator }">
      <!-- `vue-cached-icon` fetches the glyph from jsDelivr and caches it, so the demo ships no icon
           font. This is the demo's only request to an external host; all data is generated locally.
           Vuetify's `icon="$help"` is not used: its defaults are mdi CSS classes, `@mdi/font` is not a
           dependency, and the icon would render as an empty square. -->
      <v-btn v-bind="activator" size="small" variant="tonal" aria-label="What am I looking at?">
        <cached-icon name="mdi-help-circle-outline" />
      </v-btn>
    </template>

    <v-card>
      <v-card-title class="d-flex align-center ga-2">
        <span>What am I looking at?</span>
        <v-spacer />
        <v-btn size="small" variant="text" aria-label="Close" @click="open = false">
          <cached-icon name="mdi-close" />
        </v-btn>
      </v-card-title>

      <v-card-text>
        <p class="mb-4">
          Everything on this page uses one WebSocket. Each request and each price feed is an independent stream on it;
          streams start, finish and are cancelled independently of each other. Each panel demonstrates one part of this.
        </p>

        <template v-for="section in sections" :key="section.title">
          <div class="mb-5">
            <div class="text-subtitle-1 mb-1">{{ section.title }}</div>
            <div class="text-body-2 mb-2">{{ section.what }}</div>
            <div class="text-body-2 mb-2"><strong>Demonstrates:</strong> {{ section.proves }}</div>
            <div class="text-body-2 text-medium-emphasis">
              <strong>If it did not hold:</strong> {{ section.broken }}
            </div>
          </div>
          <v-divider class="mb-5" />
        </template>

        <div class="text-subtitle-1 mb-1">The 1 MB export</div>
        <p class="text-body-2 mb-2">
          Press <strong>1 MB export</strong> and watch the tick-lateness sparkline. The backend sends 1 MB on one
          stream, in 64 KiB fragments, while twenty other streams deliver prices. The line stays flat, and the readout
          reports how many tick frames arrived <em>between</em> the export's first and last fragment.
        </p>
        <p class="text-body-2 text-medium-emphasis mb-0">
          If the fragments were sent consecutively, the count would be zero and the line would spike.
        </p>
      </v-card-text>

      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="open = false">Close</v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { CachedIcon } from 'vue-cached-icon';

const open = ref(false);

/**
 * One entry per panel, in on-screen order.
 */
const sections = [
  {
    title: 'The board updates without requests',
    what:
      'The browser does not request these rows. The backend opens twenty streams to the browser, one per symbol, ' +
      'and sends prices on them.',
    proves:
      'The server opens streams with the same open() call a client uses; the browser receives them as Stream ' +
      'objects through its onStream handler. There is no separate push or subscription protocol.',
    broken: 'Rows that appear only after a click, or prices shown while the connection is down.',
  },
  {
    title: 'Clicking a row sends two requests',
    what: 'The quote is one reply to one request. The history is a stream of 120 points that arrive one at a time.',
    proves:
      'The unary and the streaming shape use the same stream primitive: one await, or one for-await loop. Both ' +
      'run over the socket that carries the board prices at the same time.',
    broken: 'The board stops updating while the history loads.',
  },
  {
    title: 'Switching symbols mid-load stops the backend',
    what: 'Click another row before the chart finishes. The counter of generations the backend stopped increases.',
    proves:
      'Cancellation reaches the producer: the backend handler stops generating points, instead of the browser ' +
      'only discarding them.',
    broken:
      'The counter stays at zero while the chart stops: the browser stopped reading and the server kept ' +
      'generating.',
  },
  {
    title: 'Full depth is bigger than a frame',
    what: 'A payload of about 190 KB, larger than the 64 KiB frame limit, received as one payload.',
    proves:
      'The library fragments and reassembles the payload. The application code does not handle the frame size limit.',
    broken: 'A truncated order book, or an error about message size.',
  },
  {
    title: 'The diagnostics strip',
    what:
      'Frames and bytes per second, the live stream count, the round-trip time of a ping, the connection state, ' +
      'and how many times this peer has reconnected.',
    proves:
      'The values come from the library: every frame the peer sends or receives passes through the onFrame hook, ' +
      'which the export panel also uses to count interleaved frames.',
    broken:
      'Counters that keep increasing after the socket closes, or a stream count that does not return to its idle ' +
      'value.',
  },
  {
    title: 'Kill the backend',
    what:
      'The backend closes every connection. Open streams fail, the UI reports it, and the dialer reconnects ' +
      'after a jittered delay.',
    proves:
      'A closed socket fails every open stream with an error instead of leaving it pending. The reconnect ' +
      'replays the hello, and no stream is resumed: the board consists of new streams.',
    broken: 'The page stops updating without an error, or shows the connection as live after the socket closed.',
  },
];
</script>
