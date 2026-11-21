import { existsSync } from 'node:fs';

/**
 * Playwright ships without its browser binaries; they are downloaded by
 * `npx playwright install chromium`. Tests that drive a real browser (live
 * preview, PDF rendering) are genuine integration tests, so they are skipped —
 * never faked — on a host where that download has not happened.
 *
 * Skipping must never happen silently where the browser is expected: set
 * REQUIRE_CHROMIUM=1 (CI does) and a missing binary fails the run instead of
 * quietly dropping nine tests.
 */
function detect(): { ok: boolean; detail: string } {
  let executablePath = '';
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { chromium } = require('playwright');
    executablePath = chromium.executablePath();
  } catch (err) {
    return { ok: false, detail: `playwright could not resolve a browser path: ${(err as Error).message}` };
  }
  if (!executablePath) return { ok: false, detail: 'playwright returned an empty executable path' };
  if (!existsSync(executablePath)) return { ok: false, detail: `no browser binary at ${executablePath}` };
  return { ok: true, detail: executablePath };
}

const result = detect();

export const chromiumAvailable = result.ok;

export const chromiumSkipReason =
  'Chromium is not installed for Playwright — run "npx playwright install chromium" to enable this test.';

const required = ['1', 'true', 'yes', 'on'].includes((process.env.REQUIRE_CHROMIUM ?? '').toLowerCase());

if (!chromiumAvailable) {
  if (required) {
    // CI asked for the real browser suites: do not let them disappear.
    throw new Error(
      `REQUIRE_CHROMIUM is set but Chromium is unusable (${result.detail}). ` +
        'Run "npx playwright install --with-deps chromium" before the test step.',
    );
  }
  // One clear line in the output instead of six confusing failures.
  console.warn(`[tests] ${chromiumSkipReason} (${result.detail})`);
} else if (process.env.VITEST_CHROMIUM_DEBUG) {
  console.info(`[tests] using Chromium at ${result.detail}`);
}
