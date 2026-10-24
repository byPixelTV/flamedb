import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createConnection } from 'node:net';
import { createInterface } from 'node:readline';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '../..');
const preview = process.env.BETTERATTACK_PREVIEW === '1';
process.env.KEEP_GRAFANA_TEST = '1';
await import('../../scripts/integration.mjs');
const session = JSON.parse(readFileSync(path.join(root, '.runtime/session.json'), 'utf8'));
const auth = 'Basic ' + Buffer.from('admin:flamedb-local-test-password').toString('base64');
const api = async (url, body) => {
  const response = await fetch(session.base + url, { method: body ? 'POST' : 'GET', headers: { Authorization: auth, 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(20000) });
  const result = await response.json(); assert.ok(response.ok, `${url}: ${JSON.stringify(result)}`); return result;
};
let socket;
try {
  socket = createConnection({ host: '127.0.0.1', port: session.dbPort });
  socket.setTimeout(10000, () => socket.destroy(new Error('Seed TCP timed out')));
  await new Promise((resolve, reject) => { socket.once('connect', resolve); socket.once('error', reject); });
  const lines = createInterface({ input: socket, crlfDelay: Infinity })[Symbol.asyncIterator]();
  const next = async () => { const line = await lines.next(); assert.equal(line.done, false); return JSON.parse(line.value); };
  assert.equal((await next()).auth, 'required'); socket.write('AUTH grafana-test-writer\n'); assert.equal((await next()).auth, 'ok');
  const supplied = JSON.parse(readFileSync(path.join(here, 'supplied-metrics.json'), 'utf8'));
  const allMetrics = [...supplied, 'system:tick:count', 'system:tick:duration_seconds', 'system:tick:duration_bucket'];
  const now = Date.now(); let sequence = 0n; let count = 0;
  for (let i = 0; i < 12; i++) {
    const batch = ['WRITE_BATCH'];
    const emit = (metric, value, tags = {}) => {
      const ts = BigInt(now - (12 - i) * 10000) * 1000000n + sequence++;
      const attributes = Object.entries(tags).map(([k, v]) => `${k}=${JSON.stringify(String(v))}`).join(' ');
      batch.push(`WRITE ${metric} ${value} ${attributes} ts=${ts}`); count++;
    };
    for (const metric of allMetrics.filter(m => !m.startsWith('system:'))) {
      emit(metric, metric === 'playtime:sessions' ? 300 : metric.includes('damage') ? 2.5 : 1, { group: 'craftattack', server: 'folia-1' });
    }
    for (const server of ['folia-1', 'paper-1']) {
      const folia = server.startsWith('folia');
      const base = { group: 'craftattack', server, folia: String(folia) };
      for (const metric of allMetrics.filter(m => m.startsWith('system:'))) {
        if (metric === 'system:global-playercount') { if (folia) { emit(metric, 45 + i, {}); } continue; }
        if (metric.startsWith('system:jvm:memory:')) {
          for (const area of ['heap', 'nonheap']) {
            if (area === 'nonheap' && (metric.endsWith(':max') || metric.endsWith(':init'))) { continue; }
            const size = metric.endsWith(':max') ? 8 : metric.endsWith(':committed') ? 4 : metric.endsWith(':init') ? 1 : 2.1 + i / 100;
            emit(metric, (area === 'heap' ? size : .3) * 1024 ** 3, { ...base, area });
          } continue;
        }
        if (metric.startsWith('system:gc:')) {
          const bucket = metric.endsWith('_bucket');
          const collectors = ['G1 Young Generation', 'G1 Old Generation'];
          if (!bucket && !metric.startsWith('system:gc:notification:')) { collectors.push(''); }
          for (const gc of collectors) {
            const tags = gc ? { ...base, gc } : base;
            if (bucket) {
              const bounds = metric.includes('duration') ? ['0.005', '0.01', '0.025', '0.05', '0.1', '0.25', '0.5', '1.0', '2.0', '5.0', '+Inf'] : ['25000000', '50000000', '100000000', '250000000', '500000000', '1000000000', '2000000000', '3000000000', '5000000000', '+Inf'];
              bounds.forEach((le, index) => emit(metric, Math.floor((i + 1) * (index + 1) / bounds.length), { ...tags, le }));
            } else { emit(metric, metric.endsWith('freed_bytes') ? (i + 1) * 1e6 : metric.endsWith('_seconds') ? (i + 1) * .01 : i + 1, tags); }
          } continue;
        }
        if (metric.startsWith('system:region:')) {
          if (!folia) { continue; }
          for (const region of ['1', '2']) {
            const val = metric.endsWith(':tps') ? region === '1' ? 19.8 : 17.2 : metric.endsWith(':mspt') ? 3 + Number(region) * 4 : metric.endsWith(':util') ? .2 + Number(region) * .1 : metric.endsWith(':online_players') ? 5 + Number(region) : 200 + Number(region) * 30;
            emit(metric, val, { ...base, world: region === '1' ? 'world' : 'world_nether', region });
          } continue;
        }
        if (metric.startsWith('system:world:')) {
          for (const world of ['world', 'world_nether', 'world_the_end']) { emit(metric, metric.endsWith(':players') ? 3 : metric.endsWith(':chunks') ? 300 : 500, { ...base, world }); } continue;
        }
        if (metric.startsWith('system:regions:world')) { if (folia) { emit(metric, 1, { ...base, world: metric.slice('system:regions:'.length) }); } continue; }
        if (metric === 'system:regions' || metric === 'system:regions:util') { if (folia) { emit(metric, metric === 'system:regions' ? 2 : 70, base); } continue; }
        if (metric.startsWith('system:tick:')) {
          if (folia) { continue; }
          if (metric.endsWith('_bucket')) { ['0.005','0.01','0.025','0.05','0.1','0.25','0.5','1.0','+Inf'].forEach((le, n) => emit(metric, (i + 1) * (n + 1) * 10, { ...base, le })); }
          else { emit(metric, metric.endsWith(':count') ? (i + 1) * 200 : (i + 1) * 2, base); } continue;
        }
        let value = 10 + i;
        if (metric.startsWith('system:events:')) { value = i * 2; }
        else if (metric === 'system:tps') { value = folia ? 17.2 : 19.8; }
        else if (metric === 'system:mspt') { value = folia ? 11 : 3; }
        else if (metric === 'system:util' || metric === 'system:cpu:system') { value = 25 + i; }
        else if (metric === 'system:cpu:seconds') { value = 1000 + i; }
        else if (metric === 'system:cpu:cores') { value = 8; }
        else if (metric === 'system:players:max') { value = 200; }
        else if (metric === 'system:process:start_time_seconds') { value = Math.floor(now / 1000) - 3600; }
        else if (metric === 'system:process:uptime_seconds') { value = 3600 + i * 10; }
        else if (metric.startsWith('system:ram:')) { value = metric.endsWith(':usage') ? 35 + i : (metric.endsWith(':max') ? 8 : metric.endsWith(':allocated') ? 4 : 2.1 + i / 100) * 1024 ** 3; }
        emit(metric, value, base);
      }
    }
    batch.push('END'); socket.write(batch.join('\n') + '\n'); const result = await next(); assert.equal(result.failed, 0, JSON.stringify(result));
  }
  socket.destroy(); socket = null;
  await api('/api/datasources', { name: 'FlameDB · BetterAttack test', uid: 'cg24sd0kvre9sd', type: 'flamedb-flamedb-datasource', access: 'proxy', jsonData: { host: '127.0.0.1', port: session.dbPort, maxScanEvents: 50000 }, secureJsonData: { apiKey: 'grafana-test-reader' } });
  const dashboard = JSON.parse(readFileSync(path.join(here, 'betterattack-dashboard.json'), 'utf8'));
  const resourceURL = '/apis/dashboard.grafana.app/v2/namespaces/default/dashboards';
  const created = await api(resourceURL, dashboard); assert.equal(created.metadata.name, 'adzlbfq');
  const persisted = await api(resourceURL + '/adzlbfq');
  assert.equal(Object.keys(persisted.spec.elements).length, Object.keys(dashboard.spec.elements).length);
  assert.equal(persisted.spec.layout.spec.tabs.length, 7);
  // Exercise all actual backend query models used by the exported dashboard.
  for (const [name, element] of Object.entries(dashboard.spec.elements)) {
    const queries = element.spec.data.spec.queries.map(p => ({ ...p.spec.query.spec, refId: p.spec.refId, datasource: { uid: 'cg24sd0kvre9sd', type: 'flamedb-flamedb-datasource' }, intervalMs: 10000, maxDataPoints: 1000 }));
    const response = await api('/api/ds/query', { from: String(now - 3600000), to: String(now + 1000), queries });
    for (const q of queries) {
      const result = response.results[q.refId]; assert.ok(!result.error, `${name}: ${result.error}`); const undefinedNonHeap = q.filters.some(f => f.key === 'area' && f.value === 'nonheap') && q.metrics.every(m => ['system:jvm:memory:max', 'system:jvm:memory:init'].includes(m));
      if (undefinedNonHeap) { assert.equal(result.frames?.length ?? 0, 0); continue; }
      assert.ok(result.frames?.length, `${name} ${q.metrics} empty`);
      if (q.metrics[0] === 'system:events:join') { assert.equal(result.frames[0].data.values[1].at(-1), 22); }
      if (q.metrics[0] === 'playtime:sessions') { assert.equal(result.frames[0].data.values[1].reduce((sum, v) => sum + (v ?? 0), 0), 12); }
    }
  }
  const url = session.base + '/d/adzlbfq?from=' + (now - 150000) + '&to=' + (now + 1000);
  const result = { ...session, url, seedEvents: count, panels: Object.keys(dashboard.spec.elements).length };
  writeFileSync(path.join(here, 'preview-session.json'), JSON.stringify(result, null, 2));
  console.log(`Validated ${result.panels} panels against ${count} synthetic collector samples; ${url}`);
  console.log('BetterAttack Grafana integration passed');
  if (preview) {
    const timer = setInterval(() => {}, 60000);
    const stop = () => { clearInterval(timer); for (const pid of [session.grafanaPID, session.flameDBPID]) { try { process.kill(pid); } catch {} } process.exit(); };
    process.on('SIGINT', stop); process.on('SIGTERM', stop);
  }
} finally {
  socket?.destroy();
  if (!preview) { for (const pid of [session.grafanaPID, session.flameDBPID]) { try { process.kill(pid); } catch {} } }
}
