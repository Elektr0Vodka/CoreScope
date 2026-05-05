/* test-home-announcement.js — homepage MQTT migration announcement */
'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');

const homeJs = fs.readFileSync(path.join(__dirname, 'public', 'home.js'), 'utf8');
const homeCss = fs.readFileSync(path.join(__dirname, 'public', 'home.css'), 'utf8');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log(`  ✓ ${name}`); }
  catch (e) { failed++; console.log(`  ✗ ${name}\n    ${e.message}`); }
}

console.log('── Home announcement modal ──');

test('renders requested Dutch migration copy and target links', () => {
  assert.ok(homeJs.includes('Heb jij jouw observers al omgezet naar de dutchmeshcore.nl servers?'), 'Dutch observer migration copy missing');
  assert.ok(homeJs.includes('De Cornmeister MQTT komt binnenkort te vervallen.'), 'Dutch MQTT retirement copy missing');
  assert.ok(homeJs.includes('https://dutch-meshcore.github.io/Dutch-Meshcore-Toolbox/#/'), 'Toolbox link missing');
  assert.ok(homeJs.includes('https://discord.gg/HfJVk9J29K'), 'Discord link missing');
});

test('provides English copy and language toggle controls', () => {
  assert.ok(homeJs.includes('Have you already moved your observers to the dutchmeshcore.nl servers?'), 'English migration copy missing');
  assert.ok(homeJs.includes('data-announcement-lang="nl"'), 'NL toggle missing');
  assert.ok(homeJs.includes('data-announcement-lang="en"'), 'EN toggle missing');
  assert.ok(homeJs.includes("localStorage.setItem(ANNOUNCEMENT_LANG_KEY, lang)"), 'language persistence missing');
});

test('supports persisted collapse state', () => {
  assert.ok(homeJs.includes('ANNOUNCEMENT_COLLAPSED_KEY'), 'collapse storage key missing');
  assert.ok(homeJs.includes('aria-expanded="${collapsed ? \'false\' : \'true\'}"'), 'initial aria-expanded state missing');
  assert.ok(homeJs.includes("body.hidden = collapsed"), 'body collapse wiring missing');
  assert.ok(homeCss.includes('.home-announcement-modal[data-collapsed="true"]'), 'collapsed CSS state missing');
});

test('announcement uses scoped homepage classes', () => {
  assert.ok(homeJs.includes('home-announcement-modal'), 'modal markup class missing');
  assert.ok(homeCss.includes('.home-announcement-modal'), 'modal CSS class missing');
  assert.ok(homeCss.includes('.home-announcement-modal[data-lang="nl"]'), 'language CSS state missing');
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
