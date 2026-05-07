#!/usr/bin/env node
/* Regression guard for #1151: Heard By observer rows should build a filtered
 * stats array and join it, instead of hardcoding separators around nullable
 * SNR/RSSI fields.
 */
'use strict';

const assert = require('assert');
const fs = require('fs');

const src = fs.readFileSync('public/nodes.js', 'utf8');

assert.ok(src.includes('const stats = [`${o.packetCount} pkts`];'), 'observer stats array is missing');
assert.ok(src.includes("if (o.avgSnr != null) stats.push('SNR '"), 'SNR should be conditionally pushed');
assert.ok(src.includes("if (o.avgRssi != null) stats.push('RSSI '"), 'RSSI should be conditionally pushed');
assert.ok(src.includes('${stats.join(\' · \')}'), 'observer stats should be joined with one separator');
assert.ok(!src.includes('${o.packetCount} pkts · ${o.avgSnr'), 'old orphan-separator template is still present');

console.log('issue 1151 orphan separator test passed');
