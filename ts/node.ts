// Subpath export muxws/node. M3 adds accept()/serve() over `ws`.
// Nothing reachable from ts/index.ts may import this file (WSM-API-022).
export { VERSION } from './version';
