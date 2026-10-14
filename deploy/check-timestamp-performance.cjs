// Match actual, durable response/event counts with an identical Linux client.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const assert=require('node:assert/strict');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const beforeDir=path.join(root,'.compat/benchmarks-timestamps-before');
const afterDir=path.join(root,'.compat/benchmarks-timestamps-after');
const read=(dir,name)=>JSON.parse(fs.readFileSync(path.join(dir,name),'utf8').replace(/^\uFEFF/,''));
function valid(r,env){
 assert.equal(r.correctness_pass,true);assert.equal(r.errors,0);assert.equal(r.events_ambiguous,0);
 assert.equal(r.automatic_timestamps,true);assert.equal(r.pipeline,1);
 assert.equal(r.requests_submitted,r.requests_confirmed);assert.equal(r.events_submitted,r.events_confirmed);
 assert.equal(r.events_confirmed,r.readback_events);assert.equal(r.durability_contract,'postgres-sync');
 assert.equal(r.events_durable_by_configured_contract,r.events_confirmed);
 assert.equal(r.requests_confirmed,r.reads_confirmed+r.events_confirmed/r.batch_size);
 for(const key of ['workers','metrics','entities']) assert.equal(r[key],env[key]);
 assert.ok(r.elapsed_seconds_including_drain>=env.duration_seconds && r.requests_confirmed>0);
 assert.ok(Math.abs(r.requests_confirmed/r.elapsed_seconds_including_drain-r.confirmed_requests_per_second)<1e-6);
 assert.ok(Math.abs(r.events_confirmed/r.elapsed_seconds_including_drain-r.confirmed_events_per_second)<1e-6);
 if(r.leaderboard_on_write) assert.equal(r.all_entity_scores_verified,true);
 return r.events_confirmed/r.elapsed_seconds_including_drain;
}
const before=read(beforeDir,'environment.json'),after=read(afterDir,'environment.json');
for(const key of ['client_os','network','addresses','workers','duration_seconds','metrics','entities','binary_sha256','automatic_timestamps','docker_environment']) assert.equal(after[key],before[key],key);
assert.equal(after.client_os,'linux');assert.equal(after.entities,1500);assert.equal(after.automatic_timestamps,true);
assert.ok(after.duration_seconds>=30);assert.deepEqual(after.matrix,before.matrix);
for(const source of after.source){
 assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,source.path))).digest('hex').toUpperCase(),source.sha256,`Source changed: ${source.path}`);
}
for(const source of after.configs){
 assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,source.path))).digest('hex').toUpperCase(),source.sha256,`Configuration changed: ${source.path}`);
}
for(const key of ['fsync','full_page_writes','synchronous_commit']) assert.equal(after.postgres_settings[key].setting,'on',key);
assert.equal(after.postgres_settings.max_wal_size.setting,'4096');
assert.equal(after.postgres_settings.checkpoint_timeout.setting,'120');
assert.equal(after.postgres_settings.wal_compression.setting,'lz4');
for(const setting of Object.values(after.postgres_settings)) assert.equal(setting.pending_restart,false);
assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,'.compat/loadtest-timestamps-linux'))).digest('hex').toUpperCase(),after.binary_sha256);
const apis=after.containers.filter(c=>c.name.includes('-api'));
assert.equal(apis.length,3);assert.equal(new Set(apis.map(c=>c.image)).size,1);
assert.notEqual(apis[0].image,before.containers.find(c=>c.name.includes('-api')).image);
const actual=spawnSync('docker',['inspect',...apis.map(c=>c.name)],{encoding:'utf8',cwd:root});
assert.equal(actual.status,0,actual.stderr);
for(const c of JSON.parse(actual.stdout)){
 assert.equal(c.Image,apis.find(a=>a.name===c.Name.slice(1)).image);
 assert.equal(c.State.Running,true);assert.equal(c.State.Health.Status,'healthy');
}
for(const item of after.matrix){
 const b=read(beforeDir,item.name+'.json'),a=read(afterDir,item.name+'.json');
 assert.equal(a.batch_size,item.batch);assert.equal(a.mode,item.mode);assert.equal(a.leaderboard_on_write,item.board==='true');
 assert.equal(a.batch_size,b.batch_size);assert.equal(a.mode,b.mode);assert.equal(a.leaderboard_on_write,b.leaderboard_on_write);
 const oldRate=valid(b,before),newRate=valid(a,after);
 const factor=['mixed','plain-1000'].includes(item.name)?1.2:0.9;
 assert.ok(newRate>=oldRate*factor,`${item.name}: ${newRate.toFixed(1)} vs ${oldRate.toFixed(1)} requires ${factor}x`);
 assert.throws(()=>valid({...a,automatic_timestamps:false},after));
 assert.throws(()=>valid({...a,correctness_pass:false},after));
 assert.throws(()=>valid({...a,events_confirmed:a.events_confirmed+1},after));
 if(a.batch_size>1) assert.throws(()=>valid({...a,confirmed_requests_per_second:a.confirmed_events_per_second},after));
 console.log(`${item.name}: ${oldRate.toFixed(1)} -> ${newRate.toFixed(1)} confirmed events/s (${(newRate/oldRate).toFixed(2)}x)`);
}
console.log('AUTOMATIC_TIMESTAMP_THROUGHPUT_VERIFIED');
