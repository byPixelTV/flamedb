const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const assert=require('node:assert/strict');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/benchmarks-minecraft');
const read=name=>JSON.parse(fs.readFileSync(path.join(dir,name),'utf8').replace(/^\uFEFF/,''));
const env=read('environment.json');
assert.equal(env.metrics,250);assert.equal(env.entities,1500);assert.equal(env.automatic_timestamps,true);
assert.equal(env.client_os,'linux');assert.ok(env.duration_seconds>=30);
for(const file of [...env.source,...env.configs]){
 assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file.path))).digest('hex').toUpperCase(),file.sha256,`Source/configuration changed: ${file.path}`);
}
const apis=env.containers.filter(c=>c.name.includes('-api'));
assert.equal(apis.length,3);assert.equal(new Set(apis.map(c=>c.image)).size,1);
const current=spawnSync('docker',['inspect',...apis.map(c=>c.name)],{encoding:'utf8'});
assert.equal(current.status,0,current.stderr);
for(const c of JSON.parse(current.stdout)){assert.equal(c.Image,apis.find(a=>a.name===c.Name.slice(1)).image);assert.equal(c.State.Health.Status,'healthy');}
assert.deepEqual(env.matrix.map(x=>x.name).sort(),['board-250','board-1000','mixed'].sort());
function valid(r,item){
 assert.equal(r.correctness_pass,true);assert.equal(r.all_entity_scores_verified,true);
 assert.equal(r.errors,0);assert.equal(r.events_ambiguous,0);assert.equal(r.automatic_timestamps,true);
 for(const key of ['metrics','entities','workers'])assert.equal(r[key],env[key]);
 assert.equal(r.batch_size,item.batch);assert.equal(r.mode,item.mode);assert.equal(r.leaderboard_on_write,true);
 assert.equal(r.events_submitted,r.events_confirmed);assert.equal(r.events_confirmed,r.readback_events);
 assert.equal(r.requests_submitted,r.requests_confirmed);assert.equal(r.requests_confirmed,r.reads_confirmed+r.events_confirmed/r.batch_size);
 assert.equal(r.durability_contract,'postgres-sync');assert.equal(r.events_durable_by_configured_contract,r.events_confirmed);
 assert.ok(r.elapsed_seconds_including_drain>=env.duration_seconds && r.events_confirmed>0);
 assert.ok(Math.abs(r.events_confirmed/r.elapsed_seconds_including_drain-r.confirmed_events_per_second)<1e-6);
 assert.ok(Math.abs(r.requests_confirmed/r.elapsed_seconds_including_drain-r.confirmed_requests_per_second)<1e-6);
 for(const v of Object.values(r.latency_ms_bucket_upper_bound))assert.ok(Number.isFinite(v)&&v>0);
}
for(const item of env.matrix){
 const r=read(item.name+'.json');valid(r,item);
 assert.throws(()=>valid({...r,all_entity_scores_verified:false},item));
 assert.throws(()=>valid({...r,events_confirmed:r.events_confirmed+1},item));
 assert.throws(()=>valid({...r,confirmed_requests_per_second:r.confirmed_events_per_second},item));
 console.log(`${item.name}: ${r.confirmed_events_per_second.toFixed(1)} events/s; p99 <= ${r.latency_ms_bucket_upper_bound.p99.toFixed(1)}ms`);
}
console.log('MINECRAFT_PROFILE_DURABLE_EVENTS_VERIFIED');
