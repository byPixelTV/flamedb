import { readFileSync } from 'node:fs';
process.env.KEEP_GRAFANA_TEST = '1';
await import('./integration.mjs');
const session = JSON.parse(readFileSync(new URL('../.runtime/session.json', import.meta.url), 'utf8'));
console.log('Synthetic demo services are running on loopback. Press Ctrl+C to stop both services.');
const timer = setInterval(() => {}, 60000);
const stop = () => {
  clearInterval(timer);
  for (const pid of [session.grafanaPID, session.flameDBPID]) { try { process.kill(pid); } catch {} }
  process.exit(0);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
