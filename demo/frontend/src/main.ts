import { createApp } from 'vue';
import { createVuetify } from 'vuetify';
import * as components from 'vuetify/components';
import * as directives from 'vuetify/directives';

import 'vuetify/dist/vuetify.css';

import App from './App.vue';

// Vuetify is a dependency of the demo frontend only, declared in `demo/frontend/package.json`. muxws
// has no runtime dependencies in the browser entry point (WSM-PKG-003); `muxws/packaging_test.py`
// asserts that the demo's packages are not dependencies of the root package.
const vuetify = createVuetify({ components, directives, theme: { defaultTheme: 'dark' } });

createApp(App).use(vuetify).mount('#app');
