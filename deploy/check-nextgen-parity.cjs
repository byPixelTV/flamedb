// Success requires current-source evidence; partial measurements cannot certify
// performance parity. Integration cases own and remove separate SQL schemas.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/nextgen-parity/correctness');
const action=process.argv[2];
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
function sources(){
 const files=['go.mod','go.sum','deploy/check-nextgen-parity.cjs'];
 function walk(p){for(const entry of fs.readdirSync(path.join(root,p),{withFileTypes:true})){const name=p+'/'+entry.name;if(entry.isDirectory())walk(name);else if(name.endsWith('.go')||name.endsWith('.sql'))files.push(name);}}
 walk('internal');walk('cmd');return files.sort().map(p=>({path:p,sha256:hash(path.join(root,p))}));
}
const required=[
 'TestAggregateBatchExactFreshnessAndIndependentErrors',
 'TestIndexedHistoricalAggregates','TestIndexedAggregateGenericPlan','TestIndexedAggregateReusesPlanWithoutSessionLeak',
 'TestPostgresAutomaticDirectCopy','TestPostgresNativeBoardBatch','TestPostgresSemantics',
 'TestPostgresLosslessIDsAndAtomicOverflow','TestPostgresLargeProtocolIdentities',
 'TestPostgresSharedAutomaticTimestampAllocator','TestPostgresAutomaticBatchIndependentMetrics',
 'TestPostgresBatchedAutomaticTimestamps','TestPostgresSchemaUpgradeAndBoundedRebuild',
 'TestPostgresConcurrentAndCacheRecovery','TestPostgresIngestSnapshotAfterLock',
];
function testEvidence(text,compatibility=false){
 const events=text.split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));
 assert.equal(events.filter(e=>e.Action==='fail').length,0,'Go test failure');
 for(const name of required)assert.ok(events.some(e=>e.Action==='pass'&&e.Test===name),'Required integration test did not pass: '+name);
 if(compatibility)for(const name of ['TestFrozenReferenceDifferentialTCP','TestSharedNodesConcurrentDurabilityAndRecovery'])assert.ok(events.some(e=>e.Action==='pass'&&e.Test===name),'Compatibility case did not pass: '+name);
 assert.ok(events.some(e=>e.Action==='pass'&&!e.Test),'No successful package result');
 return events.filter(e=>e.Action==='pass'&&e.Test&&!e.Test.includes('/')).length;
}
function run(command,args,file,env){
 const started=Date.now(),r=spawnSync(command,args,{cwd:root,env,encoding:'utf8',windowsHide:true,timeout:1200000,maxBuffer:64*1024*1024});
 fs.writeFileSync(path.join(dir,file),(r.stdout||'')+(r.stderr||''));
 assert.ifError(r.error);assert.equal(r.status,0,`${command} failed; see ${path.join(dir,file)}`);
 return {command,args,file,sha256:hash(path.join(dir,file)),milliseconds:Date.now()-started};
}
function verifyCorrectness(){
 const env=JSON.parse(fs.readFileSync(path.join(dir,'environment.json')));
 assert.deepEqual(env.source,sources(),'Source changed since correctness checks');
 assert.equal(env.race,true);assert.deepEqual(env.modes,['postgres','timescaledb']);
 for(const check of env.checks)assert.equal(hash(path.join(dir,check.file)),check.sha256);
 for(const mode of env.modes)testEvidence(fs.readFileSync(path.join(dir,mode+'.jsonl'),'utf8'),mode==='postgres');
 assert.equal(hash(env.referenceBinary),env.referenceSha256);
 assert.equal(hash(env.nextgenBinary),env.nextgenSha256);
 assert.ok(env.checks.some(c=>c.command==='go'&&c.args[0]==='vet'));
 console.log('NEXTGEN_PARITY_CORRECTNESS_VERIFIED');
}
function correctness(){
 fs.mkdirSync(dir,{recursive:true});
 const env={...process.env,FLAMEDB_INTEGRATION:'1',FLAMEDB_TEST_POSTGRES:process.env.FLAMEDB_TEST_POSTGRES||'postgres://flamedb:flamedb@127.0.0.1:55432/flamedb?sslmode=disable',FLAMEDB_TEST_REDIS:process.env.FLAMEDB_TEST_REDIS||'127.0.0.1:56379'};
 env.FLAMEDB_REFERENCE_BINARY=path.resolve(root,process.env.FLAMEDB_REFERENCE_BINARY||'.compat/flamedb-reference.exe');
 env.FLAMEDB_NEXTGEN_BINARY=path.join(dir,process.platform==='win32'?'flamedb-nextgen.exe':'flamedb-nextgen');
 assert.ok(fs.existsSync(env.FLAMEDB_REFERENCE_BINARY),'Provide the frozen reference binary');
 const before=sources(),checks=[];
 checks.push(run('go',['build','-o',env.FLAMEDB_NEXTGEN_BINARY,'./cmd/flamedb'],'build.txt',env));
 checks.push(run('go',['vet','./...'],'vet.txt',env));
 for(const [mode,timescale,packages] of [['postgres','0',['./...']],['timescaledb','1',['./internal/analytics']]]){
  console.log('CHECK '+mode+' integration with race detector');
  checks.push(run('go',['test','-json','-race','-count=1','-timeout=15m',...packages],mode+'.jsonl',{...env,FLAMEDB_TEST_TIMESCALE:timescale}));
  console.log(mode+': '+testEvidence(fs.readFileSync(path.join(dir,mode+'.jsonl'),'utf8'),mode==='postgres')+' top-level tests passed');
 }
 assert.deepEqual(sources(),before,'Source changed during checks');
 fs.writeFileSync(path.join(dir,'environment.json'),JSON.stringify({source:before,race:true,modes:['postgres','timescaledb'],checks,referenceBinary:env.FLAMEDB_REFERENCE_BINARY,referenceSha256:hash(env.FLAMEDB_REFERENCE_BINARY),nextgenBinary:env.FLAMEDB_NEXTGEN_BINARY,nextgenSha256:hash(env.FLAMEDB_NEXTGEN_BINARY),finishedUtc:new Date().toISOString()},null,2));
 verifyCorrectness();
}
const cases=['single','batch-one-sender','batch-parallel','player-sum-20','player-sum-100','top','mixed-player'];
function verifyPerformance(){
 const runs=(process.env.FLAMEDB_PARITY_RUNS||'.compat/nextgen-parity/final-player,.compat/nextgen-parity/final-player-region').split(',');
 assert.equal(runs.length,2,'Need both one-tag and two-tag comparisons');
 const filters=new Set();let allocation;
 for(const relative of runs){
  const folder=path.resolve(root,relative);assert.ok(folder.startsWith(path.join(root,'.compat')+path.sep));
  const env=JSON.parse(fs.readFileSync(path.join(folder,'environment.json')));
  assert.equal(env.profileAPI,false,'CPU profiles cannot certify unprofiled throughput');
  assert.ok(['player','player-region'].includes(env.playerFilter));assert.ok(!filters.has(env.playerFilter));filters.add(env.playerFilter);
  assert.deepEqual(env.matrix.map(c=>c.name),cases,'A partial matrix cannot certify parity');
  assert.equal(env.cpuBudget,4);assert.equal(env.memoryBudgetMiB,4096);
  assert.equal(env.allocation.nextgen.api.cpus+env.allocation.nextgen.postgres.cpus+env.allocation.nextgen.redis.cpus,4);
  if(allocation)assert.deepEqual(env.allocation,allocation,'Use one resource split for both filters');else allocation=env.allocation;
  // The comparison verifier checks rates against independent ACK counters,
  // all 7500 player targets, input history, source and loader fingerprints.
  const check=spawnSync(process.execPath,['deploy/compare-backends.cjs','verify'],{cwd:root,encoding:'utf8',env:{...process.env,FLAMEDB_COMPARISON_DIR:relative,FLAMEDB_PLAYER_FILTER:env.playerFilter,FLAMEDB_COMPARISON_CASES:cases.join(',')}});
  assert.ifError(check.error);assert.equal(check.status,0,check.stdout+check.stderr);
  const rows=JSON.parse(fs.readFileSync(path.join(folder,'results.json')));
  for(const name of cases){
   const fields=name==='mixed-player'?['confirmed_events_per_second','confirmed_reads_per_second']:[name.startsWith('single')||name.startsWith('batch-')?'confirmed_events_per_second':'confirmed_reads_per_second'];
   for(const field of fields){
    const mean=backend=>{const selected=rows.filter(r=>r.backend===backend&&r.case===name);assert.equal(selected.length,2);return selected.reduce((n,r)=>n+r.report[field],0)/2;};
    const pebble=mean('pebble'),nextgen=mean('nextgen');
    console.log(`${env.playerFilter}/${name}/${field}: Pebble=${pebble.toFixed(1)} NextGen=${nextgen.toFixed(1)}`);
    assert.ok(nextgen>=pebble,`Performance shortfall in ${env.playerFilter}/${name}/${field}`);
   }
  }
 }
 console.log('NEXTGEN_PEBBLE_PERFORMANCE_PARITY_VERIFIED');
}
try{
 if(action==='correctness')correctness();
 else if(action==='verify-correctness')verifyCorrectness();
 else if(action==='performance')verifyPerformance();
 else throw Error('Choose correctness, verify-correctness or performance');
}catch(error){console.error(error);process.exitCode=1;}
