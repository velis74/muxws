<!--
  Three call shapes on one click.

  The quote is `peer.request()` - one payload out, exactly one back (WSM-API-006). The chart is
  `for await (... of peer.open(...))` and fills in a point at a time, which is the only reason the
  backend pauses between points at all. Clicking another row cancels the outstanding history, and the
  number under the chart is the count the *backend* increments when its generator is stopped
  (WSM-ERR-012/013) - a frontend that had merely stopped looking could not move it.
-->
<template>
  <v-card>
    <v-card-title class="d-flex align-center ga-2">
      <span>{{ store.selected ?? 'no symbol selected' }}</span>
      <v-spacer />
      <v-tooltip location="bottom" max-width="360">
        <template #activator="{ props: hint }">
          <span v-bind="hint" class="text-caption text-medium-emphasis" style="cursor: help">
            unary &middot; streaming &middot; fragmented &middot; <span class="text-decoration-underline">?</span>
          </span>
        </template>
        Three shapes at once, over the socket still carrying every price on the left: the quote is one answer to one
        question, the history is many answers filling in as they arrive, and full depth is a payload larger than a
        single frame, reassembled for you.
      </v-tooltip>
    </v-card-title>

    <v-card-text v-if="store.selected === null" class="text-medium-emphasis">
      Click a row. The quote arrives as one payload; the chart arrives as a hundred and twenty.
    </v-card-text>

    <template v-else>
      <v-card-text class="pt-0">
        <div class="text-overline text-medium-emphasis">peer.request({ action: 'quote' })</div>
        <div v-if="store.quote === null" class="text-medium-emphasis">waiting for the quote&hellip;</div>
        <div v-else class="d-flex flex-wrap ga-6">
          <div v-for="field in quoteFields" :key="field.label">
            <div class="text-caption text-medium-emphasis">{{ field.label }}</div>
            <div class="text-body-1">{{ field.value }}</div>
          </div>
        </div>
      </v-card-text>

      <v-divider />

      <v-card-text>
        <div class="d-flex align-center ga-2 mb-2">
          <span class="text-overline text-medium-emphasis">for await (peer.open({ action: 'history' }))</span>
          <v-spacer />
          <v-chip size="x-small" variant="tonal" :color="historyColour">{{ store.historyState }}</v-chip>
          <v-chip size="x-small" variant="tonal">{{ store.history.length }} points</v-chip>
        </div>

        <svg class="chart" viewBox="0 0 300 90" preserveAspectRatio="none" role="img" aria-label="price history">
          <polyline v-if="path !== ''" :points="path" fill="none" stroke="currentColor" stroke-width="1" />
        </svg>

        <div class="text-caption text-medium-emphasis mt-2">
          <template v-if="store.historyTrailers">
            trailers on the end frame: {{ JSON.stringify(store.historyTrailers) }}
          </template>
          <template v-else>the point count arrives in the trailers of the end frame</template>
        </div>

        <div class="text-caption mt-2">
          generations the <strong>backend</strong> stopped:
          <strong>{{ store.stats?.history_cancelled ?? '-' }}</strong>
          <span class="text-medium-emphasis">
            (of {{ store.stats?.history_started ?? '-' }} started, {{ store.stats?.history_points_sent ?? '-' }} points
            sent)
          </span>
        </div>
      </v-card-text>

      <v-divider />

      <v-card-text>
        <div class="d-flex align-center ga-2">
          <v-btn size="small" variant="tonal" :disabled="!canAsk" @click="onDepth">full depth</v-btn>
          <span class="text-caption text-medium-emphasis">one payload over MAX_FRAME_BYTES</span>
        </div>
        <div v-if="store.depth" class="text-caption mt-2">
          {{ store.depth.levels }} levels, {{ kb(store.depth.bytes) }} on the wire in
          <strong>{{ store.depth.fragments }}</strong> fragments, reassembled in {{ store.depth.ms }} ms. This component
          never called a reassembler (WSM-FRG-010).
        </div>
      </v-card-text>
    </template>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue';

import { loadDepth, store } from '../muxws';

const canAsk = computed(() => store.connection === 'live' && store.selected !== null);

const historyColour = computed(() => {
  if (store.historyState === 'cancelled') return 'warning';
  if (store.historyState === 'lost') return 'error';
  if (store.historyState === 'done') return 'success';
  return undefined;
});

const quoteFields = computed(() => {
  const quote = store.quote;
  if (quote === null) return [];
  return [
    { label: 'last', value: quote.last.toFixed(2) },
    { label: 'bid', value: quote.bid.toFixed(2) },
    { label: 'ask', value: quote.ask.toFixed(2) },
    { label: 'spread', value: quote.spread.toFixed(2) },
    { label: 'open', value: quote.open.toFixed(2) },
    { label: 'volume', value: quote.volume.toLocaleString() },
  ];
});

/**
 * Scaled to the points that have arrived so far, not to the 120 that are coming: the line has to
 * grow while the stream is still open, or the streaming shape looks exactly like the unary one.
 */
const path = computed(() => {
  const points = store.history;
  if (points.length < 2) return '';
  let low = points[0].px;
  let high = points[0].px;
  points.forEach((point) => {
    if (point.px < low) low = point.px;
    if (point.px > high) high = point.px;
  });
  const span = high - low || 1;
  return points
    .map((point, index) => {
      const x = (index / (points.length - 1)) * 300;
      const y = 88 - ((point.px - low) / span) * 86;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
});

function kb(bytes: number): string {
  return `${(bytes / 1024).toFixed(1)} KiB`;
}

function onDepth(): void {
  void loadDepth();
}
</script>

<style scoped>
.chart {
  width: 100%;
  height: 90px;
  color: rgb(var(--v-theme-primary));
}
</style>
