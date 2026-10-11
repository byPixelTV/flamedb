// Checks documented SDK contracts against the exported configuration/timing
// field names. Runtime concurrency semantics are tested by each SDK's suite.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.join(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const specs = [
  ['go', ['pkg/go/flamedb/client.go', 'pkg/go/flamedb/pool.go'],
    ['ReadConnections', 'WriteConnections', 'OnCommandTiming', 'QueueWait', 'RequestTime', 'Success']],
  ['kotlin', ['pkg/kotlin/src/main/kotlin/dev/bypixel/flamedb/FlameDB.kt', 'pkg/kotlin/src/main/kotlin/dev/bypixel/flamedb/CommandPool.kt'],
    ['readConnections', 'writeConnections', 'onCommandTiming', 'queueWaitNanos', 'requestNanos', 'success']],
  ['typescript', ['pkg/typescript/src/index.ts'],
    ['readConnections', 'writeConnections', 'onCommandTiming', 'queueWaitMs', 'requestMs', 'success']],
];
for (const [sdk, sources, names] of specs) {
  const doc = read(`pkg/${sdk}/README.md`);
  const source = sources.map(read).join('\n');
  for (const name of names) {
    assert.ok(doc.includes(name), `${sdk}: ${name} missing from documentation`);
    const declaration = sdk === 'go' ? new RegExp(`^\\s*${name}\\s+(?:int|bool|string|time\\.Duration|func\\()`, 'm')
      : new RegExp(`\\b${name}\\??\\s*[:(]`);
    assert.ok(declaration.test(source), `${sdk}: ${name} is not a declared field`);
  }
  for (const contract of ['two read connections and one write connection', 'total deadline',
      'pool wait', 'lazy connect/auth', 'no ordering guarantee across connections',
      'not retried', 'no timing history']) {
    assert.ok(doc.includes(contract), `${sdk}: missing contract ${contract}`);
  }
  assert.ok(!doc.includes('mutex over a single persistent TCP connection'), `${sdk}: stale transport description`);
}
console.log('SDK documentation contracts verified');
