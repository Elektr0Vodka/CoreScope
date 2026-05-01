const assert = require('assert');
const fs = require('fs');
const path = require('path');

const indexHtml = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(__dirname, 'public', 'app.js'), 'utf8');

function countMatches(haystack, needle) {
  return (haystack.match(new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
}

assert.strictEqual(countMatches(indexHtml, '>Tools'), 1, 'navbar should expose one Tools menu label');
assert.ok(indexHtml.includes('href="#/mc-keygen"'), 'Tools menu should include MC-Keygen');
assert.ok(indexHtml.includes('href="#/tools/path-inspector"'), 'Tools menu should include Path Inspector');
assert.ok(indexHtml.includes('href="#/tools/trace/"'), 'Tools menu should include Trace Viewer');
assert.ok(!indexHtml.includes('<a href="#/tools" class="nav-link" data-route="tools">Tools</a>'), 'Tools landing should not be a separate top-level nav link');
assert.ok(appJs.includes("basePage === 'mc-keygen'"), 'Tools menu active state should include MC-Keygen');

console.log('All navbar tools tests passed!');
