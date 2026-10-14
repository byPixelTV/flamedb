// Verify actual observations against the preceding runtime and current source.
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const {spawnSync}=require('node:child_process');
const assert=require('node:assert/strict');
const root=path.resolve(__dirname,'..');
const baseline=path.resolve(root,process.argv[2]||'.compat/benchmarks-linux-metric-batching');
const candidate=path.resolve(root,process.argv[3]||'.compat/benchmarks-linux-coalescing');
const factor=Number(process.argv[4]||2);
assert.ok(Number.isFinite(factor)&&factor>1);
const read=(dir,name)=>JSON.parse(fs.readFileSync(path.join(dir,name),'utf8').replace(/^\uFEFF/,''));
function valid(r){
 assert.equal(r.correctness_pass,true);
 assert.equal(r.errors,0);assert.equal(r.events_ambiguous,0);
 assert.equal(r.requests_submitted,r.requests_confirmed);
 assert.equal(r.events_submitted,r.events_confirmed);
 if(r.mode==='write' || r.mode==='mixed') assert.equal(r.events_confirmed,r.readback_events);
 else assert.equal(r.readback_events,r.fixture_events_per_metric*r.metrics);
	assert.equal(r.durability_contract,'postgres-sync');
	assert.equal(r.events_durable_by_configured_contract,r.events_confirmed);
	assert.equal(r.requests_confirmed,r.reads_confirmed+r.events_confirmed/r.batch_size);
 assert.equal(r.workers,32);assert.equal(r.metrics,16);assert.equal(r.entities,1000);
 assert.ok(r.elapsed_seconds_including_drain>=15);
 assert.ok(r.requests_confirmed>0);
 const rate=r.requests_confirmed/r.elapsed_seconds_including_drain;
 assert.ok(Math.abs(rate-r.confirmed_requests_per_second)<1e-6);
 if(r.leaderboard_on_write || r.mode==='rank' || r.mode==='top') assert.equal(r.all_entity_scores_verified,true);
 return rate;
}
function run(){
 const oldEnv=read(baseline,'environment.json'),env=read(candidate,'environment.json');
 assert.equal(env.client_os,'linux');assert.equal(env.duration_seconds,15);
 assert.equal(env.workers,oldEnv.workers);assert.equal(env.addresses,oldEnv.addresses);
 assert.equal(env.binary_sha256,oldEnv.binary_sha256);
 for(const source of env.source){
  const hash=crypto.createHash('sha256').update(fs.readFileSync(path.join(root,source.path))).digest('hex').toUpperCase();
  assert.equal(hash,source.sha256,`Source changed after measurement: ${source.path}`);
 }
 const apis=env.containers.filter(c=>c.name.includes('-api'));
 assert.equal(apis.length,3);assert.equal(new Set(apis.map(c=>c.image)).size,1);
 assert.notEqual(apis[0].image,oldEnv.containers.find(c=>c.name.includes('-api')).image);
 const inspection=spawnSync('docker',['inspect',...apis.map(c=>c.name)],{encoding:'utf8',cwd:root});
 assert.equal(inspection.status,0,inspection.stderr);
 for(const actual of JSON.parse(inspection.stdout)) {
  const recorded=apis.find(c=>c.name===actual.Name.slice(1));
  // Recreating an API from the identical measured image is allowed. Exact
  // process/container identities remain recorded in each workload capture.
  assert.equal(actual.Image,recorded.image);assert.equal(actual.State.Running,true);
  assert.equal(actual.State.Health.Status,'healthy');
 }
 for(const item of env.matrix) {const report=read(candidate,item.name+'.json');valid(report);assert.equal(report.batch_size,item.batch);}
 const before=read(baseline,'mixed.json'),after=read(candidate,'mixed.json');
 const oldRate=valid(before),newRate=valid(after);
 assert.equal(after.batch_size,before.batch_size);assert.equal(after.mode,before.mode);
	const improves=(a,b)=>assert.ok(b>=a*factor,`Mixed throughput ${b.toFixed(2)} did not exceed ${factor} times ${a.toFixed(2)}`);
	improves(oldRate,newRate);
	assert.throws(()=>improves(oldRate,oldRate));
 // Ensure the same oracle rejects an unverified/failed workload and a rate
 // substituted from event counts instead of confirmed TCP command counts.
 assert.throws(()=>valid({...after,correctness_pass:false}));
 assert.throws(()=>valid({...after,confirmed_requests_per_second:after.confirmed_events_per_second}));
 console.log(`Mixed confirmed requests/s ${oldRate.toFixed(2)} -> ${newRate.toFixed(2)} (${(newRate/oldRate).toFixed(2)}x); events/s ${after.confirmed_events_per_second.toFixed(2)}; target 200000 remains separate`);
 console.log('COALESCING_THROUGHPUT_IMPROVEMENT_VERIFIED');
}
run();
