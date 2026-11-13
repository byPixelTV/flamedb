// Fail explicitly on missing integration prerequisites; fixture-only or skipped
// tests cannot satisfy this check. Uses the repository's prepared real binaries.
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname,'../../..');
const env = {
  ...process.env,
  FLAMEDB_INTEGRATION:'1',
  FLAMEDB_REFERENCE_BINARY:process.env.FLAMEDB_REFERENCE_BINARY || path.join(root,'.compat','flamedb-reference'+(process.platform==='win32'?'.exe':'')),
  FLAMEDB_PEBBLE_BINARY:process.env.FLAMEDB_PEBBLE_BINARY || path.join(root,'.compat','flamedb-pebble'+(process.platform==='win32'?'.exe':'')),
};
for (const key of ['FLAMEDB_REFERENCE_BINARY','FLAMEDB_PEBBLE_BINARY']) {
  if (!fs.existsSync(env[key])) throw new Error(`Missing ${key}: ${env[key]}`);
}
const requiredTests = ['TestFrozenLegacySDKsAgainstPebble','TestFrozenRawEventSDKBaselineLimitation','TestFrozenMissingMetricResultShapesAgainstPebble','TestAdditiveRankingSDKsAgainstPebble'];
const result = spawnSync('go',['test','./internal/compat','-run',`(${requiredTests.join('|')})`,'-count=1','-v','-timeout=8m'],{cwd:root,env,encoding:'utf8',maxBuffer:8*1024*1024,timeout:540000});
const output = (result.stdout || '')+(result.stderr || '');
if (Buffer.byteLength(output)>8*1024*1024) throw new Error('SDK integration output exceeds 8 MiB');
process.stdout.write(output);
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status || 1);
if (/testing: warning: no tests to run/.test(output)) throw new Error('SDK integration selected no tests');
for (const name of requiredTests) {
  if (!new RegExp(`^--- PASS: ${name} \\(`,'m').test(output)) throw new Error(`Missing executed PASS for ${name}`);
}
console.log('REAL_FROZEN_AND_RANKING_SDKS_VERIFIED');
