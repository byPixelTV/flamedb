import { spawnSync } from 'node:child_process';
const result = spawnSync('go', ['test', './pkg/...', '-count=1'], { stdio: 'inherit', windowsHide: true });
if (result.status !== 0) { process.exit(result.status ?? 1); }
console.log('FlameDB backend tests passed');

