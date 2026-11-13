const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { spawnSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const out = path.join(root, '.compat/pebble-only');
const reference = 'a7bb892b881e59d207f79ae7258ceecdd4c63ac7';
const baseline = 'd02dc17e2f7701b2f43473f630ae8c1966a9a73a';
const exe = process.platform === 'win32' ? '.exe' : '';
fs.mkdirSync(out, { recursive: true });

function run(cmd, args, { cwd = root, env = process.env, file } = {}) {
  const r = spawnSync(cmd, args, { cwd, env, encoding: 'utf8', timeout: 900000, maxBuffer: 32 * 1024 * 1024, windowsHide: true });
  const transcript = (r.stdout || '') + '\n' + (r.stderr || '');
  if (file) fs.writeFileSync(path.join(out, file), transcript);
  if (r.error || r.status !== 0) throw r.error || Error(`${cmd} ${args.join(' ')} failed (${r.status})\n${transcript.slice(-7000)}`);
  return r.stdout;
}

function archive(ref, dest) {
  const zip = path.join(out, `${ref}.zip`);
  fs.mkdirSync(dest, { recursive: true });
  run('git', ['archive', '--format=zip', `--output=${zip}`, ref]);
  run('tar', ['-xf', zip, '-C', dest]);
}

function architecture() {
  const retired = /(^|\/)(nextgen[^/]*|[^/]*\.nextgen\.[^/]+|Dockerfile\.nextgen|import-postgres|redis-sentinel[^/]*)(\/|$)|^internal\/(analytics|ha)\//i;
  assert.ok(retired.test('internal/analytics/engine.go'));
  assert.ok(retired.test('compose.nextgen.yml'));
  const files = run('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard']).split('\0').filter(p => p && fs.existsSync(path.join(root, p)));
  assert.deepEqual(files.filter(p => retired.test(p)), []);
  const forbiddenDependency = /(?:github\.com\/(?:jackc\/pgx|redis\/go-redis)|\/internal\/(?:analytics|ha)(?:\s|$))/m;
  assert.match('github.com/jackc/pgx/v5', forbiddenDependency);
  const deps = run('go', ['list', '-deps', './cmd/flamedb'], { file: 'dependencies.txt' });
  assert.match(deps, /^github\.com\/cockroachdb\/pebble\/v2$/m);
  assert.doesNotMatch(deps, forbiddenDependency);
  assert.doesNotMatch(fs.readFileSync(path.join(root, 'go.mod'), 'utf8'), forbiddenDependency);
  const binary = path.join(out, 'flamedb-pebble' + exe);
  run('go', ['build', '-o', binary, './cmd/flamedb']);
  const fixture = fs.mkdtempSync(path.join(out, 'rejected-config-'));
  const config = path.join(fixture, 'config.yml'), data = path.join(fixture, 'data');
  for (const engine of ['postgres', 'timescaledb', 'unknown', '']) {
    fs.writeFileSync(config, `auth:\n  enabled: false\nserver:\n  data_path: ${JSON.stringify(data)}\nstorage:\n  engine: ${JSON.stringify(engine)}\n`);
    const r = spawnSync(binary, [config], { cwd: fixture, env: { ...process.env, FLAMEDB_POSTGRES_DSN: engine ? '' : 'obsolete-fixture' }, encoding: 'utf8', timeout: 5000, windowsHide: true });
    assert.ifError(r.error);
    assert.equal(r.status, 1);
    assert.match(r.stderr, engine ? /only pebble is supported/ : /no longer selects a backend/);
    assert.ok(!fs.existsSync(data), 'An unsupported configuration opened storage');
  }
  console.log('PEBBLE_ONLY_ARCHITECTURE_VERIFIED');
}

function compatibility() {
  const frozen = path.join(root, '.compat/reference');
  archive(reference, frozen);
  const env = { ...process.env, FLAMEDB_REFERENCE_BINARY: path.join(out, 'flamedb-reference' + exe), FLAMEDB_PEBBLE_BINARY: path.join(out, 'flamedb-pebble' + exe) };
  run('go', ['build', '-o', env.FLAMEDB_REFERENCE_BINARY, './cmd/flamedb'], { cwd: frozen });
  run('go', ['build', '-o', env.FLAMEDB_PEBBLE_BINARY, './cmd/flamedb']);
  const result = run(process.execPath, ['internal/compat/check-release.cjs'], { env, file: 'compatibility.txt' });
  assert.match(result, /^REAL_RELEASE_RACE_AND_VET_VERIFIED$/m);
  run('go', ['test', '-race', './...'], { cwd: path.join(root, 'pkg/go'), file: 'sdk-go.txt' });
  run('go', ['vet', './...'], { cwd: path.join(root, 'pkg/go'), file: 'sdk-go-vet.txt' });
  const ts = path.join(root, 'pkg/typescript');
  run(process.platform === 'win32' ? 'cmd.exe' : 'npm', process.platform === 'win32' ? ['/d', '/c', 'npm.cmd', 'test'] : ['test'], { cwd: ts, file: 'sdk-typescript.txt' });
  run('bun', ['test'], { cwd: ts, file: 'sdk-bun.txt' });
  run(process.execPath, ['internal/compat/sdk/bun-wire.cjs'], { file: 'sdk-bun-wire.txt' });
  const kotlin = path.join(root, 'pkg/kotlin');
  run(process.platform === 'win32' ? 'cmd.exe' : './gradlew', [...(process.platform === 'win32' ? ['/d', '/c', 'gradlew.bat'] : []), 'test', '--rerun-tasks', '--console=plain'], { cwd: kotlin, file: 'sdk-kotlin.txt' });
  run('go', ['test', './...'], { cwd: path.join(root, 'grafana'), file: 'grafana-go.txt' });
  const integration = run(process.execPath, ['scripts/integration.mjs'], { cwd: path.join(root, 'grafana'), file: 'grafana-integration.txt' });
  assert.match(integration, /Native grouping beyond raw scan limit passed/);
  console.log('PEBBLE_ONLY_COMPATIBILITY_VERIFIED');
}

function performance() {
  const source = path.join(out, 'baseline-source');
  archive(baseline, source);
  const relative = 'internal/aggregates/legacy_row_bench_test.go';
  fs.copyFileSync(path.join(root, relative), path.join(source, relative));
  const values = { baseline: {}, candidate: {} };
  const env = { ...process.env, GOMAXPROCS: '4' };
  const args = ['test', './internal/aggregates', '-run', '^$', '-bench', '^BenchmarkLegacyLeaderboardLookup$', '-benchmem', '-benchtime=250ms', '-count=1'];
  for (const [round, order] of [['1', ['baseline', 'candidate']], ['2', ['candidate', 'baseline']], ['3', ['baseline', 'candidate']]]) {
    for (const side of order) {
      const stdout = run('go', args, { cwd: side === 'baseline' ? source : root, env, file: `bench-${round}-${side}.txt` });
      const measurements = [...stdout.matchAll(/^BenchmarkLegacyLeaderboardLookup\/(\S+)-4[ \t]+\d+[ \t]+([\d.]+) ns\/op[ \t]+(\d+) B\/op[ \t]+(\d+) allocs\/op\r?$/gm)];
      assert.equal(measurements.length, 3, stdout);
      for (const [, name, ns, bytes, allocs] of measurements) (values[side][name] ??= []).push({ ns: +ns, bytes: +bytes, allocs: +allocs });
    }
  }
  const median = (side, name, key) => values[side][name].map(v => v[key]).sort((a, b) => a - b)[1];
  const summary = {};
  for (const name of ['missing-single', 'missing-batch128', 'indexed']) {
    summary[name] = Object.fromEntries(['baseline', 'candidate'].map(side => [side, Object.fromEntries(['ns', 'bytes', 'allocs'].map(key => [key, median(side, name, key)]))]));
  }
  fs.writeFileSync(path.join(out, 'performance.json'), JSON.stringify({ baseline, rows: 32768, GOMAXPROCS: 4, values, summary }, null, 2));
  for (const name of ['missing-single', 'missing-batch128']) {
    assert.ok(summary[name].candidate.ns < summary[name].baseline.ns * 0.8, `${name} did not improve by 20%`);
    assert.ok(summary[name].candidate.allocs < summary[name].baseline.allocs * 0.1, `${name} allocations did not drop`);
  }
  assert.ok(summary.indexed.candidate.ns < summary.indexed.baseline.ns * 1.25, 'Indexed lookup regressed');
  console.log(JSON.stringify(summary));
  console.log('PEBBLE_ONLY_PERFORMANCE_VERIFIED');
}

function journal() {
  const inventory = JSON.parse(fs.readFileSync(path.join(root, '.compat/production-journal/inventory.json'), 'utf8'));
  const input = process.env.FLAMEDB_JOURNAL_SOURCE || inventory.source;
  const baseImage = 'ghcr.io/bypixeltv/flamedb@sha256:a15d050621c098ff0b3d4ffe1a10e171e6b59acbfa6a2f3e46d15629cdf57762';
  const runId = randomUUID().slice(0, 8);
  const context = path.join(out, `journal-image-${runId}`);
  const source = path.join(out, 'baseline-source');
  archive(baseline, source);
  fs.mkdirSync(context, { recursive: true });
  const buildEnv = { ...process.env, CGO_ENABLED: '0', GOOS: 'linux', GOARCH: 'amd64' };
  fs.writeFileSync(path.join(context, 'Dockerfile'), `FROM ${baseImage}\nCOPY --chmod=755 app /root/app\n`);
  const images = {}, evidence = {};
  for (const side of ['baseline', 'candidate']) {
    run('go', ['build', '-trimpath', '-o', path.join(context, 'app'), './cmd/flamedb'], { cwd: side === 'baseline' ? source : root, env: buildEnv, file: `journal-${side}-build.txt` });
    images[side] = `flamedb:pebble-only-${side}-${runId}`;
    run('docker', ['build', '-t', images[side], context], { file: `journal-${side}-image.txt` });
  }
  for (const side of ['baseline', 'candidate']) {
    const name = `pebble-only-${side}-${runId}`;
    const env = { ...process.env, FLAMEDB_JOURNAL_CASE: name, FLAMEDB_JOURNAL_IMAGE: images[side] };
    const stdout = run(process.execPath, ['deploy/check-production-journal.cjs', 'replay', input], { env, file: `journal-${side}.txt` });
    assert.match(stdout, /^PRODUCTION_JOURNAL_REPLAY_VERIFIED$/m);
    evidence[side] = JSON.parse(fs.readFileSync(path.join(root, '.compat/production-journal', `${name}-result.json`), 'utf8'));
  }
  const before = evidence.baseline, after = evidence.candidate;
  for (const key of ['sourceFingerprint', 'testSource', 'cpus', 'memoryMiB', 'compression', 'replicationFactor', 'readConnections', 'writeConnections']) assert.equal(after[key], before[key], key);
  for (const run of [before, after]) {
    assert.equal(run.result.events, inventory.records);
    assert.equal(run.result.batches, inventory.batches.length);
    assert.equal(run.result.uncertain, 0);
    assert.equal(run.result.rejected, 0);
    assert.equal(run.result.timeout_ms, 5000);
  }
  fs.writeFileSync(path.join(out, 'journal.json'), JSON.stringify({ baseline, evidence }, null, 2));
  console.log(JSON.stringify({ baseline: before.result, candidate: after.result }));
  console.log('PEBBLE_ONLY_JOURNAL_VERIFIED');
}

const checks = { architecture, compatibility, performance, journal };
assert.ok(Object.hasOwn(checks, process.argv[2]), 'Choose architecture, compatibility, performance or journal');
checks[process.argv[2]]();
