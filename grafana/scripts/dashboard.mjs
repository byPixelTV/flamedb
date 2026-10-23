import { writeFileSync } from 'node:fs';
const datasource = { type: 'flamedb-flamedb-datasource', uid: 'flamedb' };
const target = (metric, extra = {}) => ({ refId: 'A', datasource, metrics: [metric], filters: [], mode: 'timeseries', aggregation: 'AVG', interval: '10s', groupBy: ['server'], legend: '{{metric}} · {{server}}', ...extra });
const stats = [
  ['Players', 'players', 'short', '#339bd1'], ['TPS', 'tps', 'short', '#51b39a'],
  ['MSPT', 'mspt', 'ms', '#51b39a'], ['CPU load', 'cpu_load', 'percent', '#51b39a'],
  ['Memory used', 'memory_used', 'bytes', '#51b39a'], ['Chunks', 'chunks', 'short', '#339bd1'],
].map(([title, metric, unit, color], i) => ({
  id: i + 1, title, type: 'stat', datasource, description: 'Mean of the selected regions per time bucket. This panel does not show a sum of gauges across regions.',
  gridPos: { x: (i % 2) * 4, y: Math.floor(i / 2) * 4, w: 4, h: 4 },
  targets: [target(metric)],
  fieldConfig: { defaults: { unit, decimals: metric === 'players' || metric === 'chunks' ? 0 : 2, color: { mode: 'fixed', fixedColor: color }, thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] } }, overrides: [] },
  options: { colorMode: 'background', graphMode: 'area', justifyMode: 'auto', orientation: 'auto', textMode: 'auto', reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false } },
}));
const table = {
  id: 7, title: 'Regions · latest observations', type: 'table', datasource,
  gridPos: { x: 8, y: 0, w: 16, h: 12 },
  targets: [target('tps', { metrics: ['tps', 'mspt', 'players', 'chunks'], mode: 'table', groupBy: ['server', 'world', 'region'] })],
  options: { showHeader: true, cellHeight: 'sm' },
  fieldConfig: { defaults: { custom: { align: 'auto' } }, overrides: [
    { matcher: { id: 'byName', options: 'tps' }, properties: [{ id: 'decimals', value: 2 }, { id: 'custom.cellOptions', value: { type: 'color-background' } }, { id: 'thresholds', value: { mode: 'absolute', steps: [{ color: 'red', value: null }, { color: 'orange', value: 17 }, { color: 'green', value: 19 }] } }] },
    { matcher: { id: 'byRegexp', options: '.* time$' }, properties: [{ id: 'custom.hidden', value: true }] },
  ] },
};
const graph = { id: 8, title: 'TPS by server', type: 'timeseries', datasource, gridPos: { x: 0, y: 12, w: 24, h: 8 }, targets: [target('tps')], options: { legend: { displayMode: 'list', placement: 'bottom' }, tooltip: { mode: 'multi' } }, fieldConfig: { defaults: { min: 0, max: 20, decimals: 2 }, overrides: [] } };
writeFileSync('dashboards/overview.json', JSON.stringify({ uid: 'flamedb-overview', title: 'FlameDB · Minecraft overview', tags: ['flamedb', 'minecraft'], schemaVersion: 41, version: 1, editable: true, timezone: 'browser', time: { from: 'now-15m', to: 'now' }, refresh: '10s', panels: [...stats, table, graph] }, null, 2) + '\n');

