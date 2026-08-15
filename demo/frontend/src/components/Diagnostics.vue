<!--
  The strip that watches the connection, and the panel the whole demo exists for.

  Everything on the left comes from `peer.on_frame`, `peer.ping()` and `peer.streams` - the seams
  muxws offers an application for exactly this (WSM-OBS-003). The sparkline is the headline: press
  "1 MB export" and watch a megabyte fragment on one stream while twenty others keep their turn. The
  number that proves it is not the picture but `interleaved` - tick frames that landed *between* the
  export's first and last fragment (WSM-INV-004).

  The panel prints what it counted and never a number arranged to look right: `interleaved` is the
  one figure here that a FIFO writer cannot produce, so a demo that massaged it would be witnessing
  nothing at all.
-->
<template>
  <v-card>
    <v-card-text>
      <div class="d-flex flex-wrap ga-8">
        <div v-for="metric in metrics" :key="metric.label">
          <div class="text-caption text-medium-emphasis">{{ metric.label }}</div>
          <div class="text-body-1">{{ metric.value }}</div>
        </div>

        <div class="flex-grow-1" style="min-width: 260px">
          <div class="d-flex align-center ga-2">
            <span class="text-caption text-medium-emphasis">tick lateness</span>
            <span class="text-body-2">{{ store.latencyNowMs }} ms</span>
            <span class="text-caption text-medium-emphasis">worst {{ store.latencyWorstMs }} ms</span>
            <v-btn size="x-small" variant="text" @click="onResetWorst">reset</v-btn>
          </div>
          <svg class="spark" viewBox="0 0 300 40" preserveAspectRatio="none" role="img" aria-label="tick lateness">
            <line x1="0" y1="39" x2="300" y2="39" stroke="currentColor" stroke-width="0.5" opacity="0.3" />
            <polyline v-if="spark !== ''" :points="spark" fill="none" stroke="currentColor" stroke-width="1" />
          </svg>
          <div class="text-caption text-medium-emphasis">
            how much later than {{ Math.round(store.tickInterval * 1000) }} ms a symbol's next row was; full scale
            {{ sparkScale }} ms
          </div>
        </div>
      </div>

      <v-divider class="my-3" />

      <!-- The pacing control, and the reason it is on the screen at all: the board's default rate is
           a sleep in the backend's generator, and without a way to change it a reader reasonably
           reads four-a-second as what muxws can do. Turn it up and watch frames/second, not the
           board - the rows blur long before the socket notices. -->
      <div class="d-flex flex-wrap align-center ga-3 mb-3">
        <span class="text-caption text-medium-emphasis">push symbols every</span>
        <v-btn-toggle
          :model-value="intervalMs"
          density="compact"
          variant="outlined"
          divided
          mandatory
          @update:model-value="onRate"
        >
          <v-btn v-for="choice in intervals" :key="choice" :value="choice" size="small">{{ choice }} ms</v-btn>
        </v-btn-toggle>
      </div>

      <div class="d-flex flex-wrap align-center ga-3">
        <v-btn size="small" color="primary" variant="flat" :loading="store.exportState === 'running'" @click="onExport">
          1 MB export
        </v-btn>
        <span v-if="store.exportReport" class="text-caption">
          {{ kb(store.exportReport.bytes) }} in <strong>{{ store.exportReport.fragments }}</strong> fragments over
          {{ store.exportReport.ms }} ms &middot; <strong>{{ store.exportReport.interleaved }}</strong> tick frames
          interleaved between the first and last fragment &middot; worst tick lateness while it ran
          <strong>{{ store.exportReport.worstLatencyMs }} ms</strong>
        </span>
        <span v-else class="text-caption text-medium-emphasis">
          a megabyte on one stream, while twenty others keep ticking
        </span>

        <v-spacer />

        <v-btn size="small" color="error" variant="tonal" :disabled="store.connection !== 'live'" @click="onKill">
          kill the backend
        </v-btn>
      </div>

      <div v-if="store.lastStreamEnd" class="text-caption text-medium-emphasis mt-2">
        the last socket took {{ store.streamsLost }} stream(s) with it, raising
        <strong>{{ store.lastStreamEnd }}</strong> - nothing was resumed and the board above is a new subscription
        (WSM-RCN-030/031)
      </div>

      <v-divider class="my-3" />

      <div class="log">
        <div v-for="(line, index) in store.events" :key="`${index}-${line}`" class="log-line">{{ line }}</div>
        <div v-if="store.events.length === 0" class="text-medium-emphasis">
          nothing has happened to the connection yet
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue';

import { killBackend, resetLatencyWorst, runExport, setTickInterval, store } from '../muxws';

/** Two orders of magnitude apart on purpose: the point is the range, not fine control. */
const intervals = [250, 50, 10] as const;

const intervalMs = computed(() => Math.round(store.tickInterval * 1000));

const metrics = computed(() => [
  { label: 'state', value: store.connection },
  // Changes on every reconnection, on purpose: two sockets under one name would read as one
  // connection in a log (WSM-API-009).
  { label: 'peer.id', value: store.peerId || '-' },
  { label: 'reconnects', value: store.reconnects },
  { label: 'ping RTT', value: store.rttMs === null ? '-' : `${store.rttMs} ms` },
  { label: 'live streams', value: store.liveStreams },
  { label: 'board streams', value: `${store.boardLive} live / ${store.boardOpened} opened` },
  { label: 'streams ended by a failure', value: store.streamsLost },
  { label: 'frames/s', value: `${store.framesInPerSecond} in / ${store.framesOutPerSecond} out` },
  { label: 'bytes/s', value: `${kb(store.bytesInPerSecond)} in / ${kb(store.bytesOutPerSecond)} out` },
  // Not "dropped": every tick arrived, was counted and was timed. This is the count whose row was
  // superseded before the browser could paint it, and it starts climbing the moment the push rate
  // passes what a display can show.
  { label: 'renders coalesced', value: store.coalesced },
]);

/** Fixed at four tick periods so the line does not silently rescale under a stall it is measuring. */
const sparkScale = computed(() => Math.round(store.tickInterval * 4000));

const spark = computed(() => {
  const samples = store.latency;
  if (samples.length < 2) return '';
  const scale = sparkScale.value;
  return samples
    .map((sample, index) => {
      const x = (index / (samples.length - 1)) * 300;
      const y = 39 - Math.min(1, sample / scale) * 38;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
});

function kb(bytes: number): string {
  return `${(bytes / 1024).toFixed(1)} KiB`;
}

function onRate(value: unknown): void {
  if (typeof value === 'number') void setTickInterval(value);
}

function onExport(): void {
  void runExport();
}

function onKill(): void {
  void killBackend();
}

function onResetWorst(): void {
  resetLatencyWorst();
}
</script>

<style scoped>
.spark {
  width: 100%;
  height: 40px;
  color: rgb(var(--v-theme-primary));
}
.log {
  max-height: 8.5rem;
  overflow-y: auto;
  font-family: ui-monospace, monospace;
  font-size: 0.75rem;
  line-height: 1.35;
}
.log-line {
  white-space: pre-wrap;
}
</style>
