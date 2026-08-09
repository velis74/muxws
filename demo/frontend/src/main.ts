import { createApp } from 'vue';
import { createVuetify } from 'vuetify';
import * as components from 'vuetify/components';
import * as directives from 'vuetify/directives';

import 'vuetify/dist/vuetify.css';

import App from './App.vue';

// Vuetify is the demo's dependency and nothing else's (D5). muxws keeps zero runtime dependencies in
// the browser entry point (WSM-PKG-003), and `muxws/packaging_test.py` asserts that this file's
// presence has not changed that.
const vuetify = createVuetify({ components, directives, theme: { defaultTheme: 'dark' } });

createApp(App).use(vuetify).mount('#app');
