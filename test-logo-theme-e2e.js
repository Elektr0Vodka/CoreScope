#!/usr/bin/env node
/* Cornmeister logo E2E.
 *
 * Verifies the default navbar and home hero use the Cornmeister radio-node
 * SVG mark from the Dutch Meshcore Toolbox, paired with the required
 * CORNMEISTER.NL / Dutch mesh analyzer text. This intentionally does not
 * assert the old CORE/SCOPE wordmark.
 */
'use strict';

const { chromium } = require('playwright');

const BASE = process.env.BASE_URL || 'http://localhost:13581';

function fail(msg) {
  console.error(`test-logo-theme-e2e.js: FAIL - ${msg}`);
  process.exit(1);
}

async function main() {
  const requireChromium = process.env.CHROMIUM_REQUIRE === '1';
  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      executablePath: process.env.CHROMIUM_PATH || undefined,
      args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'],
    });
  } catch (err) {
    if (requireChromium) fail(`Chromium required but unavailable: ${err.message}`);
    console.log(`test-logo-theme-e2e.js: SKIP (${err.message.split('\n')[0]})`);
    process.exit(0);
  }

  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  page.setDefaultTimeout(10000);
  await page.addInitScript(() => {
    try { localStorage.setItem('meshcore-user-level', 'experienced'); } catch (_) {}
  });

  await page.goto(BASE + '/#/', { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('.nav-brand svg.brand-logo.cornmeister-logo');

  const nav = await page.evaluate(() => {
    const root = document.querySelector('.nav-brand');
    const svg = root && root.querySelector('svg.brand-logo.cornmeister-logo');
    return {
      title: root && root.querySelector('.brand-title')?.textContent.trim(),
      subtitle: root && root.querySelector('.brand-subtitle')?.textContent.trim(),
      circles: svg ? svg.querySelectorAll('circle').length : 0,
      paths: svg ? svg.querySelectorAll('path').length : 0,
      gradients: svg ? svg.querySelectorAll('linearGradient stop[stop-color="#3b82f6"], linearGradient stop[stop-color="#1d4ed8"]').length : 0,
      oldWordmark: !!(root && [...root.querySelectorAll('svg text')].some(t => /CORE|SCOPE/.test(t.textContent || ''))),
    };
  });

  if (nav.title !== 'CORNMEISTER.NL') fail(`navbar title was ${JSON.stringify(nav.title)}`);
  if (nav.subtitle !== 'Dutch mesh analyzer') fail(`navbar subtitle was ${JSON.stringify(nav.subtitle)}`);
  if (nav.circles < 1 || nav.paths < 6) fail(`navbar Cornmeister mark shape missing: ${JSON.stringify(nav)}`);
  if (nav.gradients < 2) fail(`navbar Cornmeister gradient stops missing: ${JSON.stringify(nav)}`);
  if (nav.oldWordmark) fail('navbar still contains old CORE/SCOPE SVG wordmark');

  await page.evaluate(() => { window.location.hash = '#/home'; });
  await page.waitForFunction(() => location.hash === '#/home');
  await page.waitForSelector('.home-hero svg.home-hero-logo.cornmeister-logo');

  const hero = await page.evaluate(() => {
    const root = document.querySelector('.home-hero');
    const svg = root && root.querySelector('svg.home-hero-logo.cornmeister-logo');
    return {
      title: root && root.querySelector('.home-hero-brand-name')?.textContent.trim(),
      subtitle: root && root.querySelector('.home-hero-brand-subtitle')?.textContent.trim(),
      circles: svg ? svg.querySelectorAll('circle').length : 0,
      paths: svg ? svg.querySelectorAll('path').length : 0,
      gradients: svg ? svg.querySelectorAll('linearGradient stop[stop-color="#3b82f6"], linearGradient stop[stop-color="#1d4ed8"]').length : 0,
      oldWordmark: !!(root && [...root.querySelectorAll('svg text')].some(t => /CORE|SCOPE/.test(t.textContent || ''))),
    };
  });

  if (hero.title !== 'CORNMEISTER.NL') fail(`hero title was ${JSON.stringify(hero.title)}`);
  if (hero.subtitle !== 'Dutch mesh analyzer') fail(`hero subtitle was ${JSON.stringify(hero.subtitle)}`);
  if (hero.circles < 1 || hero.paths < 6) fail(`hero Cornmeister mark shape missing: ${JSON.stringify(hero)}`);
  if (hero.gradients < 2) fail(`hero Cornmeister gradient stops missing: ${JSON.stringify(hero)}`);
  if (hero.oldWordmark) fail('hero still contains old CORE/SCOPE SVG wordmark');

  await browser.close();
  console.log('test-logo-theme-e2e.js: PASS');
}

main().catch(async (err) => {
  console.error(`test-logo-theme-e2e.js: FAIL - ${err.message}`);
  process.exit(1);
});
