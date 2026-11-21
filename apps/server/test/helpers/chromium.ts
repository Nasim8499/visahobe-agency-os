import { existsSync } from 'node:fs';

/**
 * Playwright ships without its browser binaries; they are downloaded by
 * `npx playwright install chromium`. Tests that drive a real browser (live
 * preview, PDF rendering) are genuine integration tests, so they are skipped —
 * never faked — on a host where that download has not happened.
 */
function detect(): boolean {
  try {
    // Imported lazily: on a host without the package this must not throw.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { chromium } = require('playwright');
    const path = chromium.executablePath();
    return Boolean(path) && existsSync(path);
  } catch {
    return false;
  }
}

export const chromiumAvailable = detect();

export const chromiumSkipReason =
  'Chromium is not installed for Playwright — run "npx playwright install chromium" to enable this test.';

if (!chromiumAvailable) {
  // One clear line in the output instead of six confusing failures.
  console.warn(`[tests] ${chromiumSkipReason}`);
}
