import packageJson from '../package.json';

import { VERSION } from './version';

describe('packaging', () => {
  it('keeps VERSION and package.json in step - WSM-PKG-001', () => {
    expect(VERSION).toBe(packageJson.version);
  });

  it('declares sideEffects false - WSM-CDC-015', () => {
    expect(packageJson.sideEffects).toBe(false);
  });

  it('exposes node and msgpack as subpaths - WSM-API-022', () => {
    expect(Object.keys(packageJson.exports)).toEqual(['.', './node', './msgpack']);
    Object.values(packageJson.exports).forEach((entry) => {
      expect(Object.keys(entry).sort()).toEqual(['import', 'require']);
    });
  });
});
