// Test-owned fixtures only; no production data or journal access.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/pebble-local-delay');
fs.mkdirSync(dir,{recursive:true});
function run(args,file) {
 const r=spawnSync('go',args,{cwd:root,encoding:'utf8',windowsHide:true,timeout:300000,maxBuffer:8*1024*1024});
 fs.writeFileSync(path.join(dir,file),(r.stdout||'')+'\n'+(r.stderr||''));
 if(r.error||r.status!==0) throw r.error||Error(`go ${args.join(' ')} failed\n${r.stdout}\n${r.stderr}`);
 return r.stdout;
}
const packages=['aggregates','query','storage','server','cluster'].map(x=>'./internal/'+x);
const out=run(['test','-race','-count=1','-timeout=4m',...packages],'race.txt');
for(const pkg of packages)assert.match(out,new RegExp('ok\\s+github.com/byPixelTV/flamedb/'+pkg.slice(2)+'\\s'));
run(['vet',...packages],'vet.txt');
console.log('PEBBLE_LOCAL_DELAY_VERIFIED');
