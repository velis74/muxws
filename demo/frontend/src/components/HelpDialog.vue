<!--
  What the reader is looking at, and why each panel is on the screen.

  The first person to run this demo said, reasonably: "it works, but I do not know what I am looking
  at." A market board explains itself as a market board and explains nothing about the transport,
  which is the only reason it exists.

  So this is not a tour of the UI. Every section answers three questions in the same order - what you
  see, what it proves, and **what it would look like if muxws were broken** - because the third is the
  only one a running screen can answer better than the documentation site can. `docs/guide/` explains
  the mechanisms; this explains what to watch.
-->
<template>
  <v-dialog v-model="open" max-width="820" scrollable>
    <template #activator="{ props: activator }">
      <!-- `vue-cached-icon` fetches the glyph from jsDelivr and caches it, so the demo carries no
           icon font. Note that this is the demo's only network egress: everything else it shows is
           generated locally and on purpose. Vuetify's own `icon="$help"` is not usable here - its
           defaults are mdi *CSS classes* and `@mdi/font` is not a dependency, so it would have
           rendered an empty square. -->
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
          One WebSocket. Everything below travels over it at the same time, on independent streams that start, finish
          and get cancelled without waiting for one another. That is the whole claim, and each panel is here because
          some part of it needs a witness.
        </p>

        <template v-for="section in sections" :key="section.title">
          <div class="mb-5">
            <div class="text-subtitle-1 mb-1">{{ section.title }}</div>
            <div class="text-body-2 mb-2">{{ section.what }}</div>
            <div class="text-body-2 mb-2"><strong>Proves:</strong> {{ section.proves }}</div>
            <div class="text-body-2 text-medium-emphasis">
              <strong>Broken would look like:</strong> {{ section.broken }}
            </div>
          </div>
          <v-divider class="mb-5" />
        </template>

        <div class="text-subtitle-1 mb-1">The one to actually try</div>
        <p class="text-body-2 mb-2">
          Press <strong>1 MB export</strong> and watch the tick-lateness sparkline. A megabyte leaves on one stream, in
          fragments, while twenty other streams keep delivering prices. The line should stay flat, and the readout
          should say some number of tick frames arrived <em>between</em> the export's first and last fragment.
        </p>
        <p class="text-body-2 text-medium-emphasis mb-0">
          If muxws sent that megabyte as one uninterrupted run, the count would be zero and the line would spike. That
          is not hypothetical: it was true of this library until the demo was built, and building this panel is how it
          was found.
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
 * One entry per panel. Deliberately in the order a reader's eye travels, not in the order the code
 * is arranged.
 */
const sections = [
  {
    title: 'The board updates on its own',
    what:
      'Nobody asked for these rows. The backend opened twenty streams towards the browser and keeps sending ' +
      'prices on them.',
    proves:
      'A server pushes exactly the way a client requests - the same open(), the same Stream, received by the ' +
      'browser through its own handler. There is no second mechanism and no subscription protocol on top.',
    broken:
      'Rows that only appear after you click something, or a page that renders prices it made up locally while ' +
      'disconnected.',
  },
  {
    title: 'Clicking a row asks two different questions',
    what:
      'The quote arrives as one answer to one question. The history arrives as many answers to a different ' +
      'question, filling in progressively.',
    proves:
      'The unary shape and the streaming shape are the same primitive used two ways: one await, or one loop. ' +
      'Both are in flight over the socket that is still carrying every price above.',
    broken:
      'The board freezing while the history loads, or the quote and the history arriving in the wrong order ' +
      'because something serialised them.',
  },
  {
    title: 'Switching symbols mid-load stops the backend',
    what: 'Click another row before the chart finishes. The counter of cancelled server-side generations goes up.',
    proves:
      'Cancellation reaches the producer. The backend handler is actually stopped, not merely ignored - which ' +
      'is the difference between saving the work and paying for it anyway.',
    broken:
      'The counter staying at zero while the chart still stops - that would mean the browser looked away and ' +
      'the server kept generating.',
  },
  {
    title: 'Full depth is bigger than a frame',
    what: 'A payload comfortably larger than the 64 KiB protocol limit, arriving whole.',
    proves:
      'Fragmentation and reassembly are automatic. Nothing in the application code above knows a size limit exists.',
    broken: 'A truncated order book, or an error about message size.',
  },
  {
    title: 'The diagnostics strip',
    what:
      'Frames and bytes per second, the live stream count, the round-trip time of a ping, the connection state, ' +
      'and how many times this peer has reconnected.',
    proves:
      'These are read from the library, not modelled: every frame the peer sends or receives passes through one ' +
      'hook, which is also how the export panel counts what interleaved.',
    broken:
      'Counters that keep climbing after the socket dies, or a stream count that never returns to its resting value.',
  },
  {
    title: 'Kill the backend',
    what:
      'The backend closes every connection. Open streams fail, the UI says so, and the dialer comes back on its ' +
      'own after a jittered delay.',
    proves:
      'A dead socket fails every stream loudly rather than hanging, the reconnect replays the identity that ' +
      'subscribed, and no stream survives - the board is a new subscription, not a resumed one.',
    broken: 'A page that just stops updating with no error, or one that claims to be live while the socket is gone.',
  },
];
</script>

<style>
.cached-icon-wrapper {
  width: 1.5em;
  height: 1.5em;
}
</style>
