<!--
  The board.

  Twenty streams opened by the backend arrive at this dialer's `onStream` handler, and every row is
  drawn from the data received on them; the frontend holds no symbol data (WSM-INV-002). The `ticks`
  column is the per-symbol count the backend puts in each row; each row has its own stream.
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
      <!-- The caption names the mechanism; the tooltip explains it. -->
      <v-tooltip location="bottom" max-width="360">
        <template #activator="{ props: hint }">
          <span v-bind="hint" class="text-caption text-medium-emphasis" style="cursor: help">
            server push &middot; click a row &middot; <span class="text-decoration-underline">?</span>
          </span>
        </template>
        The browser did not request these rows. The backend opened one stream per symbol to this browser, with the same
        open() call a client uses for a request.
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

// Sorted by symbol, not by the arrival order of the twenty streams, so each row keeps its position.
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
/* After a socket loss the rows remain on screen but are no longer updated; they are dimmed until the
   next connection's first update (WSM-RCN-030). */
.stale {
  opacity: 0.45;
}
</style>
