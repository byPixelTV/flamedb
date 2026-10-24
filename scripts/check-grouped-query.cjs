const {spawnSync} = require('node:child_process');
const r = spawnSync('go', ['test', './internal/query', './internal/storage', './internal/server', './internal/analytics', '-count=1'], {stdio:'inherit', windowsHide:true});
if(r.status!==0) process.exit(r.status ?? 1);
console.log('Server grouped-query tests passed');
