<template>
  <v-app>
    <v-main>
      <v-container fluid class="pa-4">
        <div class="d-flex align-center mb-4 ga-3">
          <h1 class="text-h5">muxws market board</h1>
          <v-chip :color="stateColour" size="small" variant="flat">{{ store.connection }}</v-chip>
          <span class="text-caption text-medium-emphasis">
            one socket &middot; {{ boardSize }} pushed streams &middot; four call shapes
          </span>
        </div>

        <v-alert v-if="store.connectError" type="error" variant="tonal" density="compact" class="mb-4">
          connect() raised and did not retry (WSM-RCN-006): {{ store.connectError }}
        </v-alert>

        <Diagnostics class="mb-4" />

        <v-row>
          <v-col cols="12" md="7">
            <BoardGrid />
          </v-col>
          <v-col cols="12" md="5">
            <SymbolDetail />
          </v-col>
        </v-row>
      </v-container>
    </v-main>
  </v-app>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';

import BoardGrid from './components/BoardGrid.vue';
import Diagnostics from './components/Diagnostics.vue';
import SymbolDetail from './components/SymbolDetail.vue';
import { start, store } from './muxws';

const boardSize = computed(() => store.boardLive);

const stateColour = computed(() => {
  if (store.connection === 'live') return 'success';
  if (store.connection === 'failed') return 'error';
  return 'warning';
});

// One dial for the whole application, from the one place a `Peer` is built (D4). Nothing below this
// line knows what a WebSocket is.
onMounted(() => {
  void start();
});
</script>
