#!/usr/bin/env node
'use strict';

const assert = require('assert');
const fs = require('fs');
const http = require('http');
const path = require('path');

let chromium;
try {
  ({ chromium } = require('playwright'));
} catch (err) {
  console.log(`test-home-config-refresh.js: SKIP (Playwright unavailable: ${err.message.split('\n')[0]})`);
  process.exit(0);
}

const root = process.cwd();
const mime = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

function stripExternalAssets(html) {
  return html
    .replace(/<link rel="stylesheet" href="https:\/\/unpkg[\s\S]*?crossorigin="anonymous">/g, '')
    .replace(/<script src="https:\/\/unpkg[\s\S]*?<\/script>/g, '')
    .replace(/<script src="https:\/\/cdn\.jsdelivr[\s\S]*?<\/script>/g, '');
}

function serveStatic(req, res) {
  const url = new URL(req.url, 'http://localhost');
  const target = path.join(root, 'public', url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname));
  fs.readFile(target, (err, buf) => {
    if (err) {
      res.writeHead(404);
      res.end('not found');
      return;
    }
    let body = buf;
    if (path.basename(target) === 'index.html') {
      body = Buffer.from(stripExternalAssets(buf.toString()).replaceAll('__BUST__', 'test'));
    }
    res.writeHead(200, { 'content-type': mime[path.extname(target)] || 'application/octet-stream' });
    res.end(body);
  });
}

function listen(server) {
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server.address().port)));
}

async function main() {
  const cfg = {
    branding: { siteName: 'Configured Site' },
    home: {
      heroTitle: 'Configured Home Hero',
      heroSubtitle: 'Configured subtitle',
      steps: [],
      checklist: [],
      footerLinks: [],
    },
    timestamps: {},
  };

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname === '/api/config/theme') {
      setTimeout(() => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(cfg));
      }, 150);
      return;
    }
    if (url.pathname === '/api/config/cache' || url.pathname === '/api/stats') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
      return;
    }
    serveStatic(req, res);
  });

  const port = await listen(server);
  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      executablePath: process.env.CHROMIUM_PATH || undefined,
      args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'],
    });
  } catch (err) {
    server.close();
    if (process.env.CHROMIUM_REQUIRE === '1') throw err;
    console.log(`test-home-config-refresh.js: SKIP (Chromium unavailable: ${err.message.split('\n')[0]})`);
    return;
  }

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(7000);
    await page.addInitScript(() => {
      localStorage.setItem('meshcore-user-level', 'new');
      localStorage.removeItem('cs-theme-overrides');
    });
    await page.goto(`http://127.0.0.1:${port}/#/home`, { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => {
      const h1 = document.querySelector('.home-hero h1');
      return h1 && h1.textContent.includes('Configured Home Hero');
    });
    const h1 = await page.$eval('.home-hero h1', el => el.textContent.trim());
    assert.strictEqual(h1, 'Configured Home Hero');
    console.log('test-home-config-refresh.js: OK - home refresh re-renders after config arrives');
  } finally {
    await browser.close().catch(() => {});
    server.close();
  }
}

main().catch((err) => {
  console.error(`test-home-config-refresh.js: FAIL - ${err.message}`);
  process.exit(1);
});
