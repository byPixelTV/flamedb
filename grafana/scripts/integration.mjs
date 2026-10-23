import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createConnection, createServer } from 'node:net';
import { createInterface } from 'node:readline';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, cpSync, writeFileSync, openSync, closeSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.chdir(root);
const runtime = path.join(root, '.runtime'); mkdirSync(runtime, { recursive: true });
const home = process.env.GRAFANA_HOME || readdirSync(runtime, { withFileTypes: true }).filter(x => x.isDirectory()).map(x => path.join(runtime, x.name)).find(x => existsSync(path.join(x, 'bin', process.platform === 'win32' ? 'grafana.exe' : 'grafana')));
assert.ok(home, 'Install a portable Grafana distribution under grafana/.runtime or set GRAFANA_HOME');
const work = mkdtempSync(path.join(runtime, 'test-'));
const port = () => new Promise((resolve, reject) => { const s = createServer(); s.on('error', reject); s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); }); });
const dbPort = await port(), grafanaPort = await port();
const base = `http://127.0.0.1:${grafanaPort}`;
const children = [];
const pause = ms => new Promise(r => setTimeout(r, ms));
function launch(executable, args, cwd, log) {
  const fd = openSync(path.join(work, log), 'a');
  const child = spawn(executable, args, { cwd, windowsHide: true, stdio: ['ignore', fd, fd], env: { ...process.env, GF_PLUGINS_PREINSTALL_DISABLED: 'true' } });
  closeSync(fd); children.push(child); child.on('error', e => console.error(e)); return child;
}
async function until(fn, description) { let error; for (let i = 0; i < 120; i++) { try { return await fn(); } catch (e) { error = e; await pause(500); } } throw new Error(`${description}: ${error}`); }
async function connectDB(key) {
  const socket = createConnection({ host: '127.0.0.1', port: dbPort });
  socket.setTimeout(10000, () => socket.destroy(new Error('TCP test timed out')));
  await new Promise((resolve, reject) => { socket.once('connect', resolve); socket.once('error', reject); });
  const lines = createInterface({ input: socket, crlfDelay: Infinity })[Symbol.asyncIterator]();
  const next = async () => { const line = await lines.next(); assert.equal(line.done, false); return JSON.parse(line.value); };
  assert.equal((await next()).auth, 'required'); socket.write(`AUTH ${key}\n`); assert.equal((await next()).auth, 'ok');
  return { query: async q => { socket.write(q + '\n'); return next(); }, close: () => socket.destroy() };
}
const api = async (url, body, method = body ? 'POST' : 'GET') => {
  const res = await fetch(base + url, { method, headers: { 'Content-Type': 'application/json', Authorization: 'Basic ' + Buffer.from('admin:flamedb-local-test-password').toString('base64') }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(15000) });
  const value = await res.json(); assert.ok(res.ok, `${method} ${url}: ${JSON.stringify(value)}`); return value;
};
let keep = false;
try {
  const exe = path.join(work, process.platform === 'win32' ? 'flamedb-test.exe' : 'flamedb-test');
  const build = spawnSync('go', ['build', '-o', exe, './cmd/flamedb'], { cwd: path.dirname(root), stdio: 'inherit', windowsHide: true });
  assert.equal(build.status, 0, 'FlameDB build failed');
  const config = path.join(work, 'flamedb.yml');
  writeFileSync(config, `auth:\n  enabled: true\n  keys:\n    - name: seed\n      key: grafana-test-writer\n      permissions: [read, write]\n    - name: grafana\n      key: grafana-test-reader\n      permissions: [read]\nstorage:\n  compression: snappy\nserver:\n  host: 127.0.0.1\n  port: ${dbPort}\n  node_id: grafana-test\n  data_path: ${JSON.stringify(path.join(work, 'flamedb-data'))}\ncluster:\n  replication_factor: 1\n  seeds: []\n`);
  const db = launch(exe, [config], work, 'flamedb.log');
  const writer = await until(() => connectDB('grafana-test-writer'), 'FlameDB startup');
  const now = Date.now();
  for (let sample = 0; sample < 60; sample++) {
    const ts = BigInt(now - (59 - sample) * 10000) * 1000000n;
    const commands = ['WRITE_BATCH'];
    for (let region = 1; region <= 3; region++) {
      const values = { tps: region === 3 ? 17 + Math.sin(sample / 6) : 19.5 + Math.sin(sample / 6) * .4, mspt: 2 + region + Math.cos(sample / 5), players: 20 + region * 4 + Math.round(Math.sin(sample / 9) * 5), cpu_load: 20 + region * 5 + Math.sin(sample / 4) * 8, memory_used: (2 + region * .25 + sample / 200) * 1024 ** 3, chunks: 200 + region * 100 };
      for (const [metric, value] of Object.entries(values)) { commands.push(`WRITE ${metric} ${value} server="survival-1" world="world" region="${region}" ts=${ts + BigInt(region) * 1000n}`); }
    }
    commands.push('END');
    const result = await writer.query(commands.join('\n')); assert.equal(result.failed, 0, JSON.stringify(result));
  }
  writer.close();
  const reader = await connectDB('grafana-test-reader'); assert.ok((await reader.query('WRITE forbidden 1')).error, 'read-only key accepted writes'); reader.close();
  const plugins = path.join(work, 'plugins'); mkdirSync(plugins, { recursive: true });
  cpSync(path.join(root, 'dist'), path.join(plugins, 'flamedb-flamedb-datasource'), { recursive: true });
  for (const directory of ['grafana-data', 'logs']) { mkdirSync(path.join(work, directory), { recursive: true }); }
  const ini = path.join(work, 'grafana.ini');
  const norm = p => p.replaceAll('\\', '/');
  writeFileSync(ini, `[server]\nhttp_addr = 127.0.0.1\nhttp_port = ${grafanaPort}\n[paths]\ndata = ${norm(path.join(work, 'grafana-data'))}\nlogs = ${norm(path.join(work, 'logs'))}\nplugins = ${norm(plugins)}\nprovisioning = ${norm(path.join(work, 'provisioning'))}\n[plugins]\nallow_loading_unsigned_plugins = flamedb-flamedb-datasource\npreinstall_disabled = true\n[security]\nadmin_password = flamedb-local-test-password\n[auth.anonymous]\nenabled = true\norg_role = Admin\n[analytics]\nreporting_enabled = false\ncheck_for_updates = false\ncheck_for_plugin_updates = false\n[log]\nmode = console\n`);
  for (const name of ['dashboards', 'datasources', 'plugins', 'alerting']) { mkdirSync(path.join(work, 'provisioning', name), { recursive: true }); }
  const grafana = launch(path.join(home, 'bin', process.platform === 'win32' ? 'grafana.exe' : 'grafana'), ['server', '--homepath', home, '--config', ini], home, 'grafana.log');
  await until(async () => { const r = await api('/api/health'); assert.equal(r.database, 'ok'); }, 'Grafana startup');
  await api('/api/datasources', { name: 'FlameDB', uid: 'flamedb', type: 'flamedb-flamedb-datasource', access: 'proxy', isDefault: true, jsonData: { host: '127.0.0.1', port: dbPort, maxScanEvents: 50000 }, secureJsonData: { apiKey: 'grafana-test-reader' } });
  assert.equal((await api('/api/datasources/uid/flamedb/health')).status, 'OK');
  const metrics = await api('/api/datasources/uid/flamedb/resources/metrics'); assert.ok(metrics.includes('tps') && metrics.includes('players'));
  const tags = await api('/api/datasources/uid/flamedb/resources/tags?metric=tps'); assert.deepEqual(tags.region, ['1', '2', '3']);
  const query = async extra => (await api('/api/ds/query', { from: String(now - 600000), to: String(now + 1000), queries: [{ refId: 'A', datasource: { uid: 'flamedb', type: 'flamedb-flamedb-datasource' }, metrics: ['tps'], filters: [], mode: 'timeseries', aggregation: 'AVG', interval: '10s', intervalMs: 10000, maxDataPoints: 1000, groupBy: [], ...extra }] })).results.A;
  const native = await query({}); assert.ok(!native.error && native.frames[0].data.values[0].length > 0, JSON.stringify(native));
  const grouped = await query({ groupBy: ['region'] }); assert.equal(grouped.frames.length, 3); assert.ok(grouped.frames.every(f => f.schema.fields[1].labels.region));
  const filtered = await query({ filters: [{ key: 'region', value: '2' }], groupBy: ['region'] }); assert.equal(filtered.frames.length, 1); assert.equal(filtered.frames[0].schema.fields[1].labels.region, '2');
  const table = await query({ mode: 'table', metrics: ['tps', 'players'], groupBy: ['region'] }); assert.equal(table.frames[0].data.values[0].length, 3); assert.deepEqual(table.frames[0].schema.fields.map(f => f.name), ['region', 'tps', 'tps time', 'players', 'players time']);
  const latest = await query({ mode: 'latest', groupBy: ['region'] }); assert.equal(latest.frames.length, 3); assert.ok(latest.frames.every(f => f.data.values[0].length === 1));
  const dashboard = JSON.parse(readFileSync(path.join(root, 'dashboards', 'overview.json'), 'utf8'));
  const saved = await api('/api/dashboards/db', { dashboard, overwrite: true });
  const url = base + saved.url + `?from=${now - 600000}&to=${now + 1000}`;
  writeFileSync(path.join(runtime, 'session.json'), JSON.stringify({ url, base, dbPort, work, grafanaPID: grafana.pid, flameDBPID: db.pid }, null, 2));
  console.log('Grafana integration passed'); console.log(`Demo dashboard: ${url}`);
  if (process.env.KEEP_GRAFANA_TEST === '1') { keep = true; for (const child of children) { child.unref(); } }
} catch (error) { console.error(`Logs: ${work}`); throw error; }
finally { if (!keep) { await Promise.all(children.reverse().map(child => new Promise(resolve => { if (child.exitCode !== null || child.signalCode !== null) { resolve(); return; } child.once('close', resolve); child.kill(); }))); } }





