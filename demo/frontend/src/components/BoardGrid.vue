<!--
  The board. Nobody asked for any of it.

  Twenty streams the *backend* opened on its own initiative arrive at this dialer's `onStream`
  handler, and every row below was drawn from what came down them - there is no seed data in this
  application at all (D1, WSM-INV-002). The `ticks` column is the count the backend put in the row,
  so a reader can see that each row is being fed by its own stream rather than by one broadcast.
-->
<template>
  <v-card>
    <v-card-title class="d-flex align-center ga-2">
      <span>Board</span>
      <v-chip size="x-small" variant="tonal">{{ rows.length }} symbols</v-chip>
      <v-chip size="x-small" variant="tonal" :color="store.stale ? 'warning' : undefined">
        {{ store.boardLive }} live streams
      </v-chip>
      <v-spacer />
      <!-- The caption names the mechanism; the tooltip says why it is worth noticing. A reader who
           has never met this library reads "server push" as a feature name rather than as a claim. -->
      <v-tooltip location="bottom" max-width="360">
        <template #activator="{ props: hint }">
          <span v-bind="hint" class="text-caption text-medium-emphasis" style="cursor: help">
            server push &middot; click a row &middot; <span class="text-decoration-underline">?</span>
          </span>
        </template>
        Nobody asked for these rows. The backend opened a stream per symbol towards this browser, using
        the same call a client uses to ask a question - one mechanism, both directions.
      </v-tooltip>
    </v-card-title>
    <v-card-text class="pa-0">
      <div v-if="rows.length === 0" class="pa-6 text-center text-medium-emphasis">
        waiting for the backend to push the board&hellip;
      </div>
      <v-table v-else density="compact" fixed-header height="60vh">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Name</th>
            <th class="text-right">Price</th>
            <th class="text-right">Change</th>
            <th class="text-right">%</th>
            <th class="text-right">Volume</th>
            <th class="text-right">Ticks</th>
          </tr>
        </thead>
        <tbody :class="{ stale: store.stale }">
          <tr
            v-for="row in rows"
            :key="row.symbol"
            class="board-row"
            :class="{ selected: row.symbol === store.selected }"
            @click="onSelect(row.symbol)"
          >
            <td class="font-weight-medium">{{ row.symbol }}</td>
            <td class="text-medium-emphasis">{{ row.name }}</td>
            <td class="text-right">{{ row.price.toFixed(2) }}</td>
            <td class="text-right" :class="moveClass(row.change)">{{ row.change.toFixed(2) }}</td>
            <td class="text-right" :class="moveClass(row.change)">{{ row.change_pct.toFixed(2) }}</td>
            <td class="text-right text-medium-emphasis">{{ row.volume.toLocaleString() }}</td>
            <td class="text-right text-medium-emphasis">{{ row.ticks }}</td>
          </tr>
        </tbody>
      </v-table>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue';

import { select, store, type BoardRow } from '../muxws';

// Sorted by symbol rather than by the arrival order of twenty independent streams, so a row keeps
// its place on the screen while its price moves under it.
const rows = computed<BoardRow[]>(() =>
  Object.values(store.rows).sort((left, right) => left.symbol.localeCompare(right.symbol)),
);

function moveClass(change: number): string {
  if (change > 0) return 'text-success';
  if (change < 0) return 'text-error';
  return 'text-medium-emphasis';
}

function onSelect(symbol: string): void {
  void select(symbol);
}
</script>

<style scoped>
.board-row {
  cursor: pointer;
}
.board-row.selected {
  background: rgba(255, 255, 255, 0.08);
}
/* The rows are still on screen after a socket loss, and they are no longer true. Saying so is the
   whole difference between a demonstration and a frozen screenshot (WSM-RCN-030). */
.stale {
  opacity: 0.45;
}
</style>
