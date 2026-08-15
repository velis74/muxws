#!/usr/bin/env node
/**
 * The documentation completeness checker.
 *
 * Run it with `npm run docs:check`. It exits non-zero, naming every offence, when any of the five
 * checks below fails. It reads the source of truth rather than a snapshot of it, so a symbol added
 * to `ts/index.ts` without a page entry fails here on the next run rather than in a reader's editor.
 *
 *   api-coverage           every symbol exported from `ts/index.ts`, `ts/node.ts` (`muxws/node`)
 *                          and `ts/msgpack.ts` (`muxws/msgpack`) has an entry in `docs/api/`, and
 *                          every TypeScript symbol an entry claims still exists.
 *   entry-shape            every entry carries all five required sections as headings. A missing
 *                          "Raises" is a failure even when the answer is "nothing".
 *   no-unitless-durations  no parameter-table row naming a timeout, an interval or a delay states
 *                          its number without `seconds` or `milliseconds`.
 *   no-dead-links          `ignoreDeadLinks` in the VitePress config is limited to
 *                          `[/^http:\/\/localhost/]`, so the build fails on a broken internal link -
 *                          plus a resolution of every internal link and heading anchor here, because
 *                          VitePress checks the path and not the fragment.
 *   spec-is-linked-once    `SPEC.md` is linked from `guide/architecture.md` and from nowhere else.
 *
 * The Python half of coverage is `muxws/docs_test.py`: this file enumerates the TypeScript surface
 * only, because `__all__` and `inspect.signature` are reachable from pytest and not from node.
 */

import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const DOCS = dirname(fileURLToPath(import.meta.url));
const ROOT = dirname(DOCS);

/** The three entry points `package.json` publishes, in the order `exports` lists them. */
const ENTRY_POINTS = [
  { specifier: 'muxws', file: join(ROOT, 'ts', 'index.ts') },
  { specifier: 'muxws/node', file: join(ROOT, 'ts', 'node.ts') },
  { specifier: 'muxws/msgpack', file: join(ROOT, 'ts', 'msgpack.ts') },
];

/** The five sections every API entry must carry, spelled exactly as the brief spells them. */
const REQUIRED_SECTIONS = ['Signature', 'Parameters', 'Return', 'Raises', 'Example'];

/**
 * Backticked spans in an entry heading that are not symbol names. `len(registry)` is how Python
 * spells `PeerRegistry.__len__`, and `muxws` / `muxws/node` name the entry point an overloaded
 * symbol was reached through - neither is a name anything exports.
 */
const PROSE_HEADING_SPANS = new Set(['len(registry)', ...ENTRY_POINTS.map((entry) => entry.specifier)]);

// ---------------------------------------------------------------------------------------------
// Markdown reading
// ---------------------------------------------------------------------------------------------

/** Every published Markdown page. `design/` is `srcExclude`d by the VitePress config, so it is not the site. */
function sitePages() {
  const pages = [];
  const walk = (dir) => {
    for (const name of readdirSync(dir).sort()) {
      const path = join(dir, name);
      if (statSync(path).isDirectory()) {
        if (name === 'design' || name === 'node_modules' || name === '.vitepress' || name === 'examples') continue;
        walk(path);
      } else if (name.endsWith('.md')) {
        pages.push(path);
      }
    }
  };
  walk(DOCS);
  return pages;
}

/**
 * Blank out fenced code blocks, keeping line numbering intact. Every scan below is about the prose:
 * a `## heading` or a `| table |` inside a fence is sample text, not site structure.
 */
function withoutFences(text) {
  const lines = text.split('\n');
  let fence = null;
  return lines
    .map((line) => {
      const opener = /^\s*(`{3,}|~{3,})/.exec(line);
      if (fence === null && opener) {
        fence = opener[1][0].repeat(3);
        return '';
      }
      if (fence !== null) {
        if (new RegExp(`^\\s*${fence}`).test(line)) fence = null;
        return '';
      }
      return line;
    })
    .join('\n');
}

/** The YAML frontmatter is configuration, not prose; drop it before scanning for links or headings. */
function withoutFrontmatter(text) {
  if (!text.startsWith('---\n')) return text;
  const end = text.indexOf('\n---', 4);
  if (end === -1) return text;
  const consumed = text.slice(0, end + 4).split('\n').length - 1;
  return '\n'.repeat(consumed) + text.slice(end + 4);
}

function headings(text) {
  const found = [];
  const lines = withoutFences(text).split('\n');
  lines.forEach((line, index) => {
    const match = /^(#{1,6})\s+(.*?)\s*$/.exec(line);
    if (match) found.push({ level: match[1].length, text: match[2], line: index + 1 });
  });
  return found;
}

/**
 * VitePress's heading anchors, which are `@mdit-vue/shared`'s `slugify` applied to the raw heading
 * text. Reproduced rather than imported because VitePress bundles that package rather than
 * installing it; `validateSlugifier` below proves the reproduction against a built site when one is
 * present.
 */
function slugify(text) {
  return text
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[\u0000-\u001f]/g, '')
    .replace(/[\s~`!@#$%^&*()\-_+=[\]{}|\\;:"'“”‘’<>,.?/]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/^(\d)/, '_$1')
    .toLowerCase();
}

// ---------------------------------------------------------------------------------------------
// The documented surface
// ---------------------------------------------------------------------------------------------

/**
 * The names a `##` heading claims to document. A heading may name one symbol per language
 * (`` `register_codec` / `registerCodec` ``) or one symbol qualified by its receiver
 * (`` `peer.open()` ``), so a heading yields a set rather than a name.
 */
function headingSymbols(headingText) {
  const spans = [...headingText.matchAll(/`([^`]+)`/g)].map((m) => m[1]);
  const entries = [];
  for (const raw of spans) {
    if (PROSE_HEADING_SPANS.has(raw)) continue;
    let span = raw.trim().replace(/^new\s+/, '');
    // A module specifier - `muxws/node` - qualifies the entry, it is not a symbol.
    if (span.includes('/')) continue;
    const bracket = /^([A-Za-z_$][\w$]*)\[(Symbol\.[\w$]+)\]\(\)$/.exec(span);
    if (bracket) {
      entries.push({ receiver: bracket[1], member: bracket[2], full: span });
      continue;
    }
    span = span.replace(/\(\)$/, '');
    // Anything left holding a bracket is a call with arguments, i.e. prose.
    if (/[()[\]<>]/.test(span)) continue;
    const dot = span.lastIndexOf('.');
    if (dot === -1) entries.push({ receiver: null, member: span, full: span });
    else entries.push({ receiver: span.slice(0, dot), member: span.slice(dot + 1), full: span });
  }
  return entries;
}

/** Which language an entry heading is about, when it says so. */
function headingLanguage(headingText) {
  if (/\(Python\b/.test(headingText)) return 'python';
  if (/\(TypeScript\b/.test(headingText)) return 'typescript';
  return 'unspecified';
}

/** Every `##` entry on a page, with the `###` sections it carries. */
function entriesOf(path) {
  const text = readFileSync(path, 'utf8');
  const all = headings(withoutFrontmatter(text));
  const entries = [];
  for (let i = 0; i < all.length; i += 1) {
    const heading = all[i];
    if (heading.level !== 2) continue;
    // An entry is a `##` heading that names a symbol in backticks; `## See also` is not one.
    const symbols = headingSymbols(heading.text);
    if (symbols.length === 0) continue;
    const sections = [];
    for (let j = i + 1; j < all.length && all[j].level > 2; j += 1) {
      if (all[j].level === 3) sections.push(all[j].text.trim());
    }
    entries.push({ path, heading: heading.text, line: heading.line, symbols, sections, language: headingLanguage(heading.text) });
  }
  return entries;
}

function apiPages() {
  return readdirSync(join(DOCS, 'api'))
    .filter((name) => name.endsWith('.md') && name !== 'index.md')
    .sort()
    .map((name) => join(DOCS, 'api', name));
}

// ---------------------------------------------------------------------------------------------
// The exported surface
// ---------------------------------------------------------------------------------------------

/** Comments can hold the word `export`; strip them before the source is parsed for exports. */
function withoutComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
}

/**
 * Statically declared exports, including the type-only ones. A runtime import cannot see
 * `export type { Codec }` at all, and roughly a third of this library's public surface is
 * interfaces and type aliases, so the static parse is the authority and the runtime import below is
 * its check.
 */
function staticExports(file) {
  const source = withoutComments(readFileSync(file, 'utf8'));
  const names = new Set();
  for (const match of source.matchAll(/export\s+(?:type\s+)?\{([^}]*)\}/g)) {
    for (const item of match[1].split(',')) {
      const clause = item.trim().replace(/^type\s+/, '');
      if (!clause) continue;
      const as = /\bas\s+([A-Za-z_$][\w$]*)\s*$/.exec(clause);
      names.add(as ? as[1] : clause.split(/\s+/)[0]);
    }
  }
  const declaration =
    /export\s+(?:declare\s+)?(?:default\s+)?(?:async\s+)?(?:abstract\s+)?(?:function|class|const|let|var|interface|enum|type)\s+([A-Za-z_$][\w$]*)/g;
  for (const match of source.matchAll(declaration)) names.add(match[1]);
  return names;
}

/**
 * What the module system actually hands a consumer, plus the shape of each class, obtained by
 * importing the TypeScript sources through `tsx` in a child process. `node docs/check-docs.mjs` has
 * no TypeScript loader of its own, and `dist/` is not committed, so the child is how the real
 * objects are reached.
 */
function runtimeSurface() {
  const script = join(tmpdir(), `muxws-docs-exports-${process.pid}.mjs`);
  const body = `
const files = ${JSON.stringify(ENTRY_POINTS.map((e) => e.file))};
const out = {};
for (const file of files) {
  const ns = await import(file);
  const exports = {};
  for (const name of Object.keys(ns)) {
    const value = ns[name];
    const shape = { members: [], statics: [], symbols: [] };
    if (typeof value === 'function' && value.prototype) {
      shape.members = Object.getOwnPropertyNames(value.prototype).filter((n) => n !== 'constructor');
      shape.symbols = Object.getOwnPropertySymbols(value.prototype).map((s) => s.description ?? String(s));
      shape.statics = Object.getOwnPropertyNames(value).filter((n) => !['length', 'name', 'prototype'].includes(n));
    } else if (value && (typeof value === 'object')) {
      shape.members = Object.keys(value);
    }
    exports[name] = shape;
  }
  out[file] = exports;
}
process.stdout.write(JSON.stringify(out));
`;
  writeFileSync(script, body);
  try {
    const stdout = execFileSync(process.execPath, ['--import', 'tsx', script], {
      cwd: ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, NODE_NO_WARNINGS: '1' },
    });
    return JSON.parse(stdout);
  } finally {
    rmSync(script, { force: true });
  }
}

/** Every `ts/**\/*.ts` source file, so a class body can be found by name. */
function tsSources() {
  const files = [];
  const walk = (dir) => {
    for (const name of readdirSync(dir).sort()) {
      const path = join(dir, name);
      if (statSync(path).isDirectory()) walk(path);
      else if (name.endsWith('.ts') && !name.endsWith('.spec.ts')) files.push(path);
    }
  };
  walk(join(ROOT, 'ts'));
  return files;
}

/**
 * The body of `class <name>`, so that a documented member can be looked for where it would be
 * declared. The prototype tells us about methods and accessors; a field initialised in the
 * constructor - `readonly tags: Record<string, unknown> = {}` - exists on instances only, and this
 * is how those are reached without constructing a peer.
 */
function classBodies() {
  const bodies = new Map();
  for (const file of tsSources()) {
    const source = readFileSync(file, 'utf8');
    const lines = source.split('\n');
    for (let i = 0; i < lines.length; i += 1) {
      const match = /^(?:export\s+)?(?:abstract\s+)?class\s+([A-Za-z_$][\w$]*)/.exec(lines[i]);
      if (!match) continue;
      const body = [];
      for (let j = i + 1; j < lines.length && !/^\}/.test(lines[j]); j += 1) body.push(lines[j]);
      bodies.set(match[1], body.join('\n'));
    }
  }
  return bodies;
}

/** Does `member` appear as a declaration inside this class body? */
function declaresMember(body, member) {
  const escaped = member.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const declaration = new RegExp(
    `^\\s*(?:public\\s+|private\\s+|protected\\s+|readonly\\s+|static\\s+|abstract\\s+|declare\\s+|override\\s+|async\\s+|get\\s+|set\\s+|\\*)*${escaped}\\s*[(?:=<]`,
    'm',
  );
  return declaration.test(body);
}

// ---------------------------------------------------------------------------------------------
// Check 1 - api-coverage
// ---------------------------------------------------------------------------------------------

function checkApiCoverage() {
  const failures = [];
  const notes = [];

  const declared = new Map();
  for (const entry of ENTRY_POINTS) {
    for (const name of staticExports(entry.file)) {
      if (!declared.has(name)) declared.set(name, []);
      declared.get(name).push(entry.specifier);
    }
  }

  let runtime;
  try {
    runtime = runtimeSurface();
  } catch (error) {
    failures.push(`could not import the TypeScript entry points through tsx: ${error.message}`);
    return { name: 'api-coverage', failures, notes };
  }

  // The static parse is what the coverage requirement is measured against, so it must not miss a
  // name the module system really exports. If it does, this checker is wrong before the site is.
  const shapes = new Map();
  for (const [file, exportsOfFile] of Object.entries(runtime)) {
    for (const [name, shape] of Object.entries(exportsOfFile)) {
      shapes.set(name, shape);
      if (!declared.has(name)) {
        failures.push(
          `${relative(ROOT, file)} exports \`${name}\` at runtime but this checker's export parser did not ` +
            'see it - fix the parser, not the page',
        );
      }
    }
  }

  const documented = new Map();
  const duplicates = [];
  for (const path of apiPages()) {
    for (const entry of entriesOf(path)) {
      for (const symbol of entry.symbols) {
        const key = symbol.receiver === null ? symbol.member : symbol.full;
        if (!documented.has(key)) documented.set(key, []);
        documented.get(key).push(entry);
      }
    }
  }
  // Two pages carrying an entry for the same symbol is not a coverage failure - the site is still
  // complete - but it is how two copies start to drift, so it is reported.
  for (const [key, entries] of documented) {
    const pages = [...new Set(entries.map((e) => relative(DOCS, e.path)))];
    if (pages.length > 1) duplicates.push(`duplicate entry: \`${key}\` is documented on ${pages.join(' and ')}`);
  }
  notes.push(...duplicates.sort());

  // A class's members are documented as `peer.open()`, which names the class by its receiver.
  const receivers = new Map();
  for (const name of declared.keys()) {
    receivers.set(name.toLowerCase(), name);
    receivers.set(`${name[0].toLowerCase()}${name.slice(1)}`.toLowerCase(), name);
  }
  const documentedLower = new Set([...documented.keys()].map((k) => k.toLowerCase()));
  const documentedReceivers = new Set(
    [...documented.keys()].filter((k) => k.includes('.')).map((k) => k.slice(0, k.lastIndexOf('.')).toLowerCase()),
  );

  // Forward: every exported symbol has an entry.
  for (const [name, specifiers] of [...declared].sort()) {
    const lower = name.toLowerCase();
    if (documentedLower.has(lower)) continue;
    if (documentedReceivers.has(lower)) continue; // documented member-by-member, e.g. `stream.send()`
    failures.push(`\`${name}\` is exported from ${specifiers.join(', ')} but has no entry in docs/api/`);
  }

  // Reverse: every TypeScript symbol an entry claims still exists. Entries tagged `(Python)` or
  // untagged are the Python mirror's business - `muxws/docs_test.py` resolves those against the
  // module itself, which node cannot do.
  const bodies = classBodies();
  for (const path of apiPages()) {
    for (const entry of entriesOf(path)) {
      if (entry.language !== 'typescript') continue;
      for (const symbol of entry.symbols) {
        if (symbol.receiver === null) {
          if (!declared.has(symbol.member)) {
            failures.push(
              `${relative(DOCS, path)}:${entry.line} documents \`${symbol.full}\` as TypeScript, ` +
                'but no entry point exports that name',
            );
          }
          continue;
        }
        const owner = receivers.get(symbol.receiver.toLowerCase());
        if (owner === undefined) {
          failures.push(
            `${relative(DOCS, path)}:${entry.line} documents \`${symbol.full}\`, but nothing named ` +
              `\`${symbol.receiver}\` is exported`,
          );
          continue;
        }
        const shape = shapes.get(owner) ?? { members: [], statics: [], symbols: [] };
        const onPrototype =
          shape.members.includes(symbol.member) ||
          shape.statics.includes(symbol.member) ||
          shape.symbols.includes(symbol.member);
        const inBody = declaresMember(bodies.get(owner) ?? '', symbol.member.replace(/^Symbol\./, ''));
        if (!onPrototype && !inBody) {
          failures.push(
            `${relative(DOCS, path)}:${entry.line} documents \`${symbol.full}\`, but \`${owner}\` has no ` +
              `member \`${symbol.member}\``,
          );
        }
      }
    }
  }

  notes.unshift(
    `${declared.size} exported symbols across ${ENTRY_POINTS.length} entry points; ` +
      `${documented.size} documented names across ${apiPages().length} pages`,
  );
  return { name: 'api-coverage', failures, notes };
}

// ---------------------------------------------------------------------------------------------
// Check 2 - entry-shape
// ---------------------------------------------------------------------------------------------

function checkEntryShape() {
  const failures = [];
  let entryCount = 0;
  for (const path of apiPages()) {
    for (const entry of entriesOf(path)) {
      entryCount += 1;
      const missing = REQUIRED_SECTIONS.filter((section) => !entry.sections.includes(section));
      if (missing.length > 0) {
        failures.push(
          `${relative(DOCS, path)}:${entry.line} entry "${entry.heading}" is missing ${missing.map((s) => `### ${s}`).join(', ')}`,
        );
      }
      // An entry may carry more than the five - `api/types.md`'s `Frame` adds an envelope-field
      // table - but never fewer.
    }
  }
  return { name: 'entry-shape', failures, notes: [`${entryCount} entries checked`] };
}

// ---------------------------------------------------------------------------------------------
// Check 8 - no-unitless-durations
// ---------------------------------------------------------------------------------------------

const DURATION_NAME = /timeout|interval|delay|deadline/i;
const UNIT = /\bseconds?\b|\bmilliseconds?\b/i;

function tablesIn(text) {
  const lines = withoutFences(withoutFrontmatter(text)).split('\n');
  const tables = [];
  let current = null;
  lines.forEach((line, index) => {
    if (/^\s*\|/.test(line)) {
      const cells = line
        .trim()
        .replace(/^\|/, '')
        .replace(/\|$/, '')
        .split(/(?<!\\)\|/)
        .map((c) => c.trim());
      if (current === null) current = { header: cells, separator: null, rows: [], line: index + 1 };
      else if (current.separator === null && cells.every((c) => /^:?-{2,}:?$/.test(c))) current.separator = cells;
      else if (current.separator !== null) current.rows.push({ cells, line: index + 1 });
      else current = { header: cells, separator: null, rows: [], line: index + 1 };
    } else if (current !== null) {
      if (current.separator !== null) tables.push(current);
      current = null;
    }
  });
  if (current !== null && current.separator !== null) tables.push(current);
  return tables;
}

function checkNoUnitlessDurations() {
  const failures = [];
  let scanned = 0;
  for (const path of sitePages()) {
    const text = readFileSync(path, 'utf8');
    for (const table of tablesIn(text)) {
      // A parameter table is one that states types or defaults. The reset-code table's "required
      // reaction" column says "otherwise after a delay" and has a TIMEOUT row; it is prose about
      // behaviour, not a parameter list, and check 8 is scoped to parameter tables for that reason.
      const typeColumn = table.header.findIndex((c) => /^type$/i.test(c));
      const isParameterTable = typeColumn !== -1 || table.header.some((c) => /^default$/i.test(c));
      if (!isParameterTable) continue;
      scanned += 1;
      for (const row of table.rows) {
        const name = row.cells[0] ?? '';
        if (!DURATION_NAME.test(name)) continue;
        // The type column is where the unit belongs; a table that states defaults without types may
        // put it anywhere in the row.
        const where = typeColumn === -1 ? row.cells.join(' | ') : (row.cells[typeColumn] ?? '');
        if (!UNIT.test(where)) {
          failures.push(
            `${relative(DOCS, path)}:${row.line} parameter \`${name.replace(/`/g, '')}\` names a duration but ` +
              `${typeColumn === -1 ? 'the row' : 'its type column'} does not say seconds or milliseconds: ` +
              `${where.slice(0, 100)}`,
          );
        }
      }
    }
  }
  return { name: 'no-unitless-durations', failures, notes: [`${scanned} parameter tables scanned`] };
}

// ---------------------------------------------------------------------------------------------
// Check 9 - no-dead-links
// ---------------------------------------------------------------------------------------------

/**
 * Prove the reproduced slugifier against a built site, when `docs:build` has left one behind.
 * Only pages whose HTML is newer than their Markdown are compared: a stale page would report a
 * disagreement that is really just an edit the last build did not see.
 */
function validateSlugifier() {
  const dist = join(DOCS, '.vitepress', 'dist');
  if (!existsSync(dist)) return null;
  const mismatches = [];
  let compared = 0;
  for (const path of sitePages()) {
    const html = join(dist, relative(DOCS, path).replace(/\.md$/, '.html'));
    if (!existsSync(html)) continue;
    if (statSync(html).mtimeMs < statSync(path).mtimeMs) continue;
    compared += 1;
    const rendered = readFileSync(html, 'utf8');
    for (const heading of headings(withoutFrontmatter(readFileSync(path, 'utf8')))) {
      if (heading.level < 2) continue;
      const id = slugify(heading.text);
      if (!rendered.includes(`id="${id}"`)) mismatches.push(`${relative(DOCS, path)}:${heading.line} -> #${id}`);
    }
  }
  return compared === 0 ? null : mismatches;
}

function checkNoDeadLinks() {
  const failures = [];
  const notes = [];

  const config = readFileSync(join(DOCS, '.vitepress', 'config.ts'), 'utf8');
  const declared = /ignoreDeadLinks\s*:\s*([^\n]*)/.exec(config);
  const expected = '[/^http:\\/\\/localhost/]';
  if (declared === null) {
    failures.push('docs/.vitepress/config.ts does not set ignoreDeadLinks; the build would not fail on a dead link');
  } else if (declared[1].replace(/,\s*$/, '').trim() !== expected) {
    failures.push(
      `docs/.vitepress/config.ts sets ignoreDeadLinks to ${declared[1].replace(/,\s*$/, '').trim()}; ` +
        `it must be exactly ${expected} ` +
        'so that a broken internal link is a build failure',
    );
  }

  // The anchors VitePress does not check. Everything the site can be linked to, by page and by id.
  const anchors = new Map();
  for (const path of sitePages()) {
    const route = `/${relative(DOCS, path).replace(/\.md$/, '')}`.replace(/\/index$/, '/');
    const ids = new Set(headings(withoutFrontmatter(readFileSync(path, 'utf8'))).map((h) => slugify(h.text)));
    anchors.set(route, ids);
    if (route.endsWith('/')) anchors.set(route.replace(/\/$/, '/index'), ids);
  }

  const mismatches = validateSlugifier();
  if (mismatches === null) notes.push('no current build under docs/.vitepress/dist; the slugifier was not cross-checked');
  else if (mismatches.length > 0) {
    failures.push(
      `this checker's heading slugifier disagrees with the built site on ${mismatches.length} heading(s), ` +
        `starting at ${mismatches[0]} - the anchor results below cannot be trusted until it is fixed`,
    );
  } else notes.push('heading slugifier agrees with the built site');

  let linkCount = 0;
  for (const path of sitePages()) {
    const text = withoutFences(withoutFrontmatter(readFileSync(path, 'utf8')));
    const route = `/${relative(DOCS, path).replace(/\.md$/, '')}`.replace(/\/index$/, '/');
    for (const match of text.matchAll(/\[[^\]]*\]\(([^)\s]+)\)/g)) {
      const target = match[1];
      if (/^[a-z][a-z0-9+.-]*:/i.test(target)) continue; // external scheme
      linkCount += 1;
      const line = text.slice(0, match.index).split('\n').length;
      const [rawPath, fragment] = target.split('#');
      // A link may be written the way the file is named (`./peer.md`) or the way it is served
      // (`/api/peer`); both reach the same page and both must resolve here.
      const pathPart = rawPath.replace(/\.md$/, '').replace(/\/index$/, '/');
      const page = pathPart === '' ? route : pathPart.startsWith('/') ? pathPart : resolve(dirname(route), pathPart);
      const ids = anchors.get(page) ?? anchors.get(page.replace(/\/$/, '')) ?? anchors.get(`${page}/`);
      if (ids === undefined) {
        failures.push(`${relative(DOCS, path)}:${line} links to ${target}, which is not a page of this site`);
        continue;
      }
      if (fragment !== undefined && fragment !== '' && !ids.has(fragment)) {
        failures.push(
          `${relative(DOCS, path)}:${line} links to ${target}, but that page has no heading with the ` +
            `anchor #${fragment}`,
        );
      }
    }
  }
  notes.push(`${linkCount} internal links resolved`);
  return { name: 'no-dead-links', failures, notes };
}

// ---------------------------------------------------------------------------------------------
// Check 10 - spec-is-linked-once
// ---------------------------------------------------------------------------------------------

function checkSpecIsLinkedOnce() {
  const failures = [];
  const owner = join(DOCS, 'guide', 'architecture.md');
  const sightings = [];
  for (const path of sitePages()) {
    const text = withoutFences(withoutFrontmatter(readFileSync(path, 'utf8')));
    // A link, not a mention: `[SPEC.md](…/SPEC.md)` is one link whose text happens to repeat the
    // file name. What must not multiply is the number of places the site points a reader at.
    for (const match of text.matchAll(/\[[^\]]*\]\(([^)\s]*SPEC\.md[^)\s]*)\)/g)) {
      sightings.push({ path, line: text.slice(0, match.index).split('\n').length, target: match[1] });
    }
  }
  // Off the architecture page, any mention at all is the offence: a page that names SPEC.md is a
  // page that has started paraphrasing it, whether or not it links.
  for (const path of sitePages()) {
    if (path === owner) continue;
    const text = withoutFences(withoutFrontmatter(readFileSync(path, 'utf8')));
    for (const match of text.matchAll(/SPEC\.md/g)) {
      const line = text.slice(0, match.index).split('\n').length;
      failures.push(
        `${relative(DOCS, path)}:${line} mentions SPEC.md; the site points at it once, from ` +
          'guide/architecture.md, so that it never becomes a second copy of the normative rules',
      );
    }
  }
  const own = sightings.filter((s) => s.path === owner);
  if (own.length === 0) failures.push('guide/architecture.md does not link SPEC.md');
  if (own.length > 1) {
    failures.push(
      `guide/architecture.md links SPEC.md ${own.length} times (lines ${own.map((s) => s.line).join(', ')}); ` +
        'exactly one link is the rule',
    );
  }
  return { name: 'spec-is-linked-once', failures, notes: [`${sightings.length} link(s) to SPEC.md on the site`] };
}

// ---------------------------------------------------------------------------------------------

const results = [
  checkApiCoverage(),
  checkEntryShape(),
  checkNoUnitlessDurations(),
  checkNoDeadLinks(),
  checkSpecIsLinkedOnce(),
];

let failed = 0;
for (const result of results) {
  const ok = result.failures.length === 0;
  if (!ok) failed += 1;
  process.stdout.write(`${ok ? 'PASS' : 'FAIL'}  ${result.name}\n`);
  for (const note of result.notes) process.stdout.write(`      note: ${note}\n`);
  for (const failure of result.failures) process.stdout.write(`      ${failure}\n`);
}
process.stdout.write(
  failed === 0 ? `\nAll ${results.length} documentation checks passed.\n` : `\n${failed} of ${results.length} documentation checks failed.\n`,
);
process.exit(failed === 0 ? 0 : 1);
