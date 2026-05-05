/* test-roles-foldin-nav.js — issue #1085 nav regression */
'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');

const indexHtml = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(__dirname, 'public', 'app.js'), 'utf8');
const analyticsJs = fs.readFileSync(path.join(__dirname, 'public', 'analytics.js'), 'utf8');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log(`  ✓ ${name}`); }
  catch (e) { failed++; console.log(`  ✗ ${name}\n    ${e.message}`); }
}

console.log('── Roles fold-in nav regression ──');

test('top nav does not expose the old roles route', () => {
  assert.ok(!/data-route=["']roles["']/.test(indexHtml), 'top nav must not contain data-route="roles"');
  assert.ok(!/href=["']#\/roles["']/.test(indexHtml), 'top nav must not link directly to #/roles');
});

test('old roles URL still redirects to analytics roles tab', () => {
  assert.ok(appJs.includes("#/analytics?tab=roles"), 'app router should preserve #/roles redirect');
});

test('analytics keeps the folded-in Roles tab', () => {
  assert.ok(/data-tab=["']roles["']/.test(analyticsJs), 'analytics must keep data-tab="roles"');
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
