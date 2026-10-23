import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
const platforms = [['windows', 'amd64', '.exe'], ['linux', 'amd64', ''], ['linux', 'arm64', '']];
mkdirSync('dist', { recursive: true });
for (const [os, arch, suffix] of platforms) {
  const result = spawnSync('go', ['build', '-trimpath', '-o', `dist/gpx_flamedb_${os}_${arch}${suffix}`, './pkg'], {
    stdio: 'inherit', env: { ...process.env, CGO_ENABLED: '0', GOOS: os, GOARCH: arch },
  });
  if (result.error) {
    console.error(`Cannot start Go for ${os}/${arch}: ${result.error.message}`);
    console.error('Install Go and ensure the go executable is available in PATH.');
    process.exit(1);
  }
  if (result.status !== 0) {
    console.error(`Go build failed for ${os}/${arch} (exit: ${result.status}, signal: ${result.signal ?? 'none'}).`);
    process.exit(result.status ?? 1);
  }
  if (!existsSync(`dist/gpx_flamedb_${os}_${arch}${suffix}`)) {
    throw new Error(`Backend artifact missing for ${os}/${arch}`);
  }
}
if (!existsSync('dist/module.js') || !existsSync('dist/plugin.json')) { throw new Error('Frontend artifacts missing'); }
console.log('FlameDB plugin build passed');

