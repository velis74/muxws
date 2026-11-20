/**
 * The browser demo end to end: `demo.py --browser` serves the page, Chromium opens it, and every
 * button, toggle, tooltip and clickable row is used once, with the visible result of each asserted.
 */
import { createConnection } from 'node:net';

import { expect, type Locator, type Page, test } from '@playwright/test';

/** The backend's address; the Vite proxy forwards `/ws` to it (`demo/frontend/vite.config.ts`). */
const BACKEND_HOST = '127.0.0.1';
const BACKEND_PORT = 8020;

/** Resolves once something accepts TCP connections on the backend's port. */
async function backendListening(timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const accepted = await new Promise<boolean>((resolve) => {
      const socket = createConnection({ host: BACKEND_HOST, port: BACKEND_PORT });
      socket.once('connect', () => {
        socket.destroy();
        resolve(true);
      });
      socket.once('error', () => resolve(false));
    });
    if (accepted) return;
    await new Promise((resolve) => {
      setTimeout(resolve, 250);
    });
  }
  throw new Error(`nothing listens on ${BACKEND_HOST}:${BACKEND_PORT} after ${timeoutMs} ms`);
}

/** The value under a caption-and-value pair, as in the diagnostics strip and the quote. */
function metric(page: Page, label: string): Locator {
  return page.locator(`xpath=//div[div[normalize-space()="${label}"]]/div[2]`);
}

async function numberIn(locator: Locator): Promise<number> {
  const match = /-?\d+/u.exec(await locator.innerText());
  return match === null ? Number.NaN : Number(match[0]);
}

test('uses every control of the browser demo once and asserts the visible result of each', async ({ page }) => {
  // The page dials once and does not retry a failed first connection (WSM-RCN-006), so it is opened
  // only after the backend listens; `webServer.url` covers the dev server only.
  await backendListening();
  await page.goto('/');

  const rows = page.locator('tr.board-row');
  const lateness = page.getByText('tick lateness', { exact: true }).locator('xpath=following-sibling::span[1]');
  const worst = page.getByText(/^worst \d+ ms$/u);

  await test.step('connects and receives the pushed board', async () => {
    await expect(metric(page, 'state')).toHaveText('live');
    await expect(rows).toHaveCount(20);
    await expect(metric(page, 'board streams')).toHaveText('20 live / 20 opened');
    await expect(page.getByText(/ {2}connected as /u)).toBeVisible();
  });

  await test.step('tooltips explain the board and the detail panel', async () => {
    await page.getByText(/server push · click a row/u).hover();
    await expect(page.getByText(/The browser did not request these rows/u)).toBeVisible();
    await page.getByText(/unary · streaming · fragmented/u).hover();
    await expect(page.getByText(/Three call shapes over the socket/u)).toBeVisible();
    await page.mouse.move(0, 0);
  });

  await test.step('the help dialog opens and closes with both close buttons', async () => {
    const help = page.getByRole('button', { name: 'What am I looking at?' });
    const dialog = page
      .locator('.v-overlay__content')
      .filter({ hasText: 'Everything on this page uses one WebSocket' });

    await help.click();
    await expect(dialog).toBeVisible();
    await dialog.locator('.v-card-title').getByRole('button', { name: 'Close' }).click();
    await expect(dialog).toBeHidden();

    await help.click();
    await expect(dialog).toBeVisible();
    await dialog.locator('.v-card-actions').getByRole('button', { name: 'Close' }).click();
    await expect(dialog).toBeHidden();
  });

  await test.step('a row click shows the quote and streams the history; a second click cancels it', async () => {
    const first = (await rows.nth(0).locator('td').first().innerText()).trim();
    const second = (await rows.nth(1).locator('td').first().innerText()).trim();
    const stopped = page
      .locator('.text-caption', { hasText: 'generations the backend stopped' })
      .locator('strong')
      .nth(1);

    await expect(page.getByText('Click a row. The quote arrives as one payload')).toBeVisible();
    await rows.nth(0).click();
    await expect(page.locator('.v-card-title > span', { hasText: new RegExp(`^${first}$`, 'u') })).toBeVisible();
    await expect(metric(page, 'bid')).toHaveText(/^\d+\.\d{2}$/u);
    await expect(page.getByText(/^loading$/u)).toBeVisible();
    await expect(stopped).toHaveText(/^\d+$/u);
    const stoppedBefore = await numberIn(stopped);

    await rows.nth(1).click();
    await expect(page.locator('.v-card-title > span', { hasText: new RegExp(`^${second}$`, 'u') })).toBeVisible();
    await expect.poll(async () => numberIn(stopped)).toBeGreaterThan(stoppedBefore);

    await expect(page.getByText(/^120 points$/u)).toBeVisible();
    await expect(page.getByText(/^done$/u)).toBeVisible();
    await expect(page.getByText(/trailers on the end frame:/u)).toBeVisible();
    await expect(page.locator('svg[aria-label="price history"] polyline')).toHaveCount(1);
  });

  await test.step('full depth arrives as one reassembled payload', async () => {
    await page.getByRole('button', { name: 'full depth' }).click();
    await expect(page.getByText(/^\d+ levels, [\d.]+ KiB on the wire in \d+ fragments/u)).toBeVisible();
  });

  await test.step('the tick interval toggle changes the backend rate', async () => {
    for (const interval of [50, 10, 250]) {
      const button = page.getByRole('button', { name: `${interval} ms`, exact: true });
      await button.click();
      await expect(button).toHaveClass(/v-btn--active/u);
      await expect(page.getByText(`how much later than ${interval} ms`)).toBeVisible();
      await expect(page.getByText(`the backend now pushes every ${interval} ms per symbol`)).toBeVisible();
    }
  });

  await test.step('the 1 MB export reports its fragments', async () => {
    await page.getByRole('button', { name: '1 MB export' }).click();
    await expect(page.getByText(/ in \d+ fragments over \d+ ms/u)).toBeVisible();
    await expect(page.getByText(/ {2}export of \d+ rows: \d+ fragments/u)).toBeVisible();
  });

  await test.step('reset clears the worst tick lateness', async () => {
    // A busy main thread delays every tick by the time it is blocked.
    await page.evaluate(() => {
      const end = performance.now() + 1500;
      let spins = 0;
      while (performance.now() < end) spins += 1;
      return spins;
    });
    await expect.poll(async () => numberIn(worst)).toBeGreaterThanOrEqual(1000);
    // The ticks queued while the thread was blocked are received before the reset.
    await expect.poll(async () => numberIn(lateness)).toBeLessThan(200);
    await page.getByRole('button', { name: 'reset', exact: true }).click();
    await expect.poll(async () => numberIn(worst)).toBeLessThan(1000);
  });

  await test.step('killing the backend fails the streams and the page reconnects', async () => {
    const peerBefore = await metric(page, 'peer.id').innerText();

    await page.getByRole('button', { name: 'kill the backend' }).click();
    await expect(page.getByText(/ {2}the backend is closing \d+ peer\(s\)/u)).toBeVisible();
    await expect(page.getByText(/stream\(s\) failed on socket loss with/u)).toBeVisible();
    await expect(metric(page, 'reconnects')).toHaveText('1');
    await expect(metric(page, 'state')).toHaveText('live');
    await expect(metric(page, 'peer.id')).not.toHaveText(peerBefore);
    await expect(page.getByText(/ {2}reconnected on attempt 1:/u)).toBeVisible();
    // Only the live count: with the Node backend the new board streams can arrive before
    // `onReconnected` resets the opened count.
    await expect(metric(page, 'board streams')).toHaveText(/^20 live \//u);
    await expect(page.locator('tbody.stale')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'kill the backend' })).toBeEnabled();
  });
});
