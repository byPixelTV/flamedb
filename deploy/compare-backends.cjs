// Current backend comparison. Own labeled resources only; never replays writes.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {spawn,spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.resolve(root,process.env.FLAMEDB_COMPARISON_DIR||'.compat/backend-comparison'),action=process.argv[2];
assert.ok(dir.startsWith(path.join(root,'.compat')+path.sep));
fs.mkdirSync(dir,{recursive:true});
const allCases=[
 {name:'single',mode:'write',batch:1,workers:32,metrics:250,fixture:0},
 {name:'batch-one-sender',mode:'write',batch:128,workers:1,metrics:250,fixture:0},
 {name:'batch-parallel',mode:'write',batch:128,workers:32,metrics:250,fixture:0},
 {name:'player-sum-20',mode:'player-sum',batch:1,workers:32,metrics:5,fixture:30000},
 {name:'player-sum-100',mode:'player-sum',batch:1,workers:32,metrics:5,fixture:150000},
 {name:'top',mode:'top',batch:1,workers:32,metrics:5,fixture:30000},
 {name:'mixed-player',mode:'player-mixed',batch:128,workers:32,metrics:5,fixture:30000},
];
const selected=process.env.FLAMEDB_COMPARISON_CASES?.split(',');
const matrix=selected?allCases.filter(x=>selected.includes(x.name)):allCases;
if(selected)assert.equal(matrix.length,selected.length);
const playerFilter=process.env.FLAMEDB_PLAYER_FILTER||'player-region';
assert.ok(['player','player-region'].includes(playerFilter));
const firstBackend=process.env.FLAMEDB_COMPARISON_FIRST||'pebble';
assert.ok(['pebble','nextgen'].includes(firstBackend));
const profileAPI=process.env.FLAMEDB_PROFILE_API==='1';
// Keep the total CPU budget fixed while testing whether the API or PostgreSQL
// is the limiting service. The chosen split applies to every case in the run.
const apiCPUs=Number(process.env.FLAMEDB_COMPARISON_API_CPUS||1);
const redisCPUs=Number(process.env.FLAMEDB_COMPARISON_REDIS_CPUS||0.5);
const postgresCPUs=4-apiCPUs-redisCPUs;
for(const n of [apiCPUs,redisCPUs,postgresCPUs])assert.ok(Number.isFinite(n)&&n>=0.25&&n<=3.5,'invalid four-CPU allocation');
function command(args){
 const r=spawnSync('docker',args,{cwd:root,encoding:'utf8',timeout:180000,maxBuffer:8*1024*1024});
 if(r.error||r.status!==0)throw r.error||new Error(`docker ${args[0]}: ${r.stderr}`);
 return r.stdout.trim();
}
const json=args=>JSON.parse(command(args));
const sha=p=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,p))).digest('hex');
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function sources(){const files=['go.mod','go.sum','Dockerfile'];function walk(dir){for(const e of fs.readdirSync(path.join(root,dir),{withFileTypes:true})){const p=dir+'/'+e.name;if(e.isDirectory())walk(p);else if((p.endsWith('.go')&&!p.endsWith('_test.go'))||p.endsWith('.sql'))files.push(p);}}walk('cmd');walk('internal');return files.sort().map(p=>({path:p,sha256:sha(p)}));}
function validate(env,results){
 assert.equal(env.rounds,2);assert.equal(env.durationSeconds,20);assert.equal(env.entities,1500);assert.equal(env.timestamps,'explicit');
 assert.deepEqual(env.matrix,matrix);assert.equal(env.storageDriver,'local-named-volumes');assert.equal(env.cpuBudget,4);assert.equal(env.memoryBudgetMiB,4096);
 assert.equal(results.length,4*matrix.length);
 const seen=new Set();
 for(const row of results){
  const item=matrix.find(i=>i.name===row.case);assert.ok(item);assert.ok(['pebble','nextgen'].includes(row.backend));assert.ok([1,2].includes(row.round));
  const key=`${row.round}/${row.backend}/${row.case}`;assert.ok(!seen.has(key));seen.add(key);
  assert.equal(row.image,env.image);const r=row.report;
  for(const [k,v] of Object.entries({mode:item.mode,batch_size:item.batch,workers:item.workers,metrics:item.metrics,entities:1500,pipeline:1,leaderboard_on_write:true,automatic_timestamps:false}))assert.equal(r[k],v,`${key} ${k}`);
  assert.equal(r.fixture_events_per_metric,item.fixture);assert.equal(r.timestamp_start,1706745600000000000);assert.ok(r.elapsed_seconds_including_drain>=20);
  assert.equal(r.player_filter||'player-region',playerFilter);
  assert.equal(r.correctness_pass,true);assert.equal(r.all_entity_scores_verified,true);assert.equal(r.errors,0);assert.equal(r.events_ambiguous,0);
  assert.equal(r.events_submitted,r.events_confirmed);assert.equal(r.requests_submitted,r.requests_confirmed);
  assert.equal(r.requests_confirmed,r.reads_confirmed+r.events_confirmed/item.batch);
  assert.equal(r.readback_events,r.events_confirmed+item.fixture*item.metrics);assert.equal(r.readback_mismatches,null);
  assert.ok(r.requests_confirmed>0);
  for(const [k,count] of [['confirmed_requests_per_second',r.requests_confirmed],['confirmed_events_per_second',r.events_confirmed],['confirmed_reads_per_second',r.reads_confirmed]])assert.ok(Math.abs(r[k]-count/r.elapsed_seconds_including_drain)<1e-6,`${key} ${k}`);
  for(const v of Object.values(r.latency_ms_bucket_upper_bound))assert.ok(Number.isFinite(v)&&v>0);
  if(item.mode.startsWith('player-')){assert.equal(r.all_player_sums_verified,true);assert.equal(r.player_query_targets,7500);assert.ok(r.player_query_targets_observed>0&&r.player_query_targets_observed<=7500);}
  if(item.mode==='player-sum')assert.equal(r.stable_read_responses_verified,true);
  assert.ok(row.resources.length>=2);assert.ok(row.resources.every(s=>s.containers.length===(row.backend==='pebble'?1:3)));
 }
}
async function run(){
 const run=`${Date.now()}-${process.pid}`,network='fdbcmp-'+run;
 const image=json(['image','inspect',process.env.FLAMEDB_COMPARISON_IMAGE||'flamedb:pebble-batch-fix'])[0].Id;
 // Containerd can drop an old manifest-list identity when a build retags the
 // development image. Keep a unique local tag for every recorded experiment.
 const pinnedImage='flamedb-comparison:'+run;command(['tag',image,pinnedImage]);
 const pgImage='timescale/timescaledb:latest-pg17@sha256:b346edcdb51a1fd6020e3965e0bd1c9f3406fa6d5fbce1e28f4852587ef934e2';
 const redisImage='redis:8-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0';
 const prior=command(['ps','-q']).split(/\s+/).filter(Boolean);
 const loader=path.relative(root,path.join(dir,'loadtest-linux')).replaceAll('\\','/');
 const env={run,rounds:2,durationSeconds:20,entities:1500,timestamps:'explicit',matrix,image,pgImage,redisImage,cpuBudget:4,memoryBudgetMiB:4096,storageDriver:'local-named-volumes',allocation:{pebble:{cpus:4,memoryMiB:4096},nextgen:{api:{cpus:apiCPUs,memoryMiB:1024},postgres:{cpus:postgresCPUs,memoryMiB:2816},redis:{cpus:redisCPUs,memoryMiB:256}}},source:sources(),loader,loaderSha256:sha(loader),docker:json(['info','--format','{{json .}}']),priorContainers:prior,startedUtc:new Date().toISOString()};
 const results=action==='resume'?JSON.parse(fs.readFileSync(path.join(dir,'results.json'))):[],created=[];
 if(action==='resume'){
  const previous=JSON.parse(fs.readFileSync(path.join(dir,'environment.json')));
  for(const source of previous.source)assert.equal(sha(source.path),source.sha256,`cannot resume changed source: ${source.path}`);
  assert.equal(env.image,previous.image);assert.equal(env.loaderSha256,previous.loaderSha256);
  assert.deepEqual(env.allocation,previous.allocation,'cannot resume with changed CPU allocation');
  env.resumedFrom=previous.run;env.previousEnvironment=previous;
 }
 env.playerFilter=playerFilter;env.firstBackend=firstBackend;
 env.pinnedImage=pinnedImage;
 env.profileAPI=profileAPI;
 fs.writeFileSync(path.join(dir,'environment.json'),JSON.stringify(env,null,2));
 function owned(type,id){const info=json([type==='volume'?'volume':'container','inspect',id])[0];const labels=type==='volume'?info.Labels:info.Config.Labels;assert.equal(labels['flamedb.compare'],run);return info;}
 function volume(role){const name=`${network}-${role}`;command(['volume','create','--label',`flamedb.compare=${run}`,name]);created.push({type:'volume',id:name});assert.equal(owned('volume',name).Driver,'local');return name;}
 function container(role,args){const id=command(['run','-d','--name',`${network}-${role}`,'--label',`flamedb.compare=${run}`,'--network',network,...args]);created.push({type:'container',id});owned('container',id);return id;}
 function remove(resource){owned(resource.type,resource.id);command(resource.type==='volume'?['volume','rm',resource.id]:['rm','-f',resource.id]);created.splice(created.indexOf(resource),1);}
 function diagnose(id){const info=owned('container',id),logs=spawnSync('docker',['logs',id],{encoding:'utf8',timeout:10000});const text=JSON.stringify(info.State,null,2)+'\n'+(logs.stdout||'')+(logs.stderr||'');fs.writeFileSync(path.join(dir,'fixture-failure-'+id.slice(0,12)+'.txt'),text);return text.slice(-4000);}
 async function health(id){for(let i=0;i<120;i++){const info=owned('container',id);if(info.State.Health?.Status==='healthy')return;if(!info.State.Running)throw new Error(diagnose(id));await wait(500);}throw new Error('fixture health timeout '+diagnose(id));}
 async function measure(backend,round,item,ids,endpoint){
  const name=`${round}-${backend}-${item.name}`,resources=[];
  console.log(`START ${name}: ${item.workers} connections, ${item.batch} events/batch, fixture ${item.fixture*item.metrics} events`);
  const started=performance.now(),reportFile=path.join(dir,name+'.json'),errorFile=path.join(dir,name+'.stderr.txt');
  const args=['run','--rm','--name',`${network}-load`,'--label',`flamedb.compare=${run}`,'--network',network,'--cpus','2','--memory','512m','--mount',`type=bind,source=${dir},target=/bench`,image,'/bench/loadtest-linux','--addrs',endpoint,'--key','compare-key','--mode',item.mode,'--batch-size',String(item.batch),'--workers',String(item.workers),'--metrics',String(item.metrics),'--entities','1500','--pipeline','1','--duration','20s','--timeout','5s','--prefix',`compare.${round}.${item.name}`,'--fixture-events',String(item.fixture||1000),'--start-timestamp','1706745600000000000','--leaderboard=true','--automatic-timestamps=false','--durability-contract',backend==='nextgen'?'postgres-sync':'unspecified'];
  args.push('--player-filter',playerFilter);
  const child=spawn('docker',args,{cwd:root,windowsHide:true}),out=[],err=[];
  child.stdout.on('data',b=>out.push(b));child.stderr.on('data',b=>err.push(b));
  let finished=false;const done=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',code=>{finished=true;resolve(code);});});
  let profileError;
  const profile=(async()=>{if(!profileAPI||backend!=='nextgen')return;while(!finished){await wait(500);if(finished)return;const metrics=command(['exec',ids[0],'wget','-q','-O','-','http://127.0.0.1:9090/metrics']);const n=Number(metrics.match(/flamedb_inflight_requests (\d+)/)?.[1]||0);if(n<16)continue;const capture=spawn('docker',['exec',ids[0],'wget','-q','-O','/tmp/comparison-cpu.pprof','http://127.0.0.1:6060/debug/pprof/profile?seconds=10'],{windowsHide:true});await new Promise((resolve,reject)=>{capture.once('error',reject);capture.once('close',code=>code===0?resolve():reject(Error('profile failed '+code)))});owned('container',ids[0]);command(['cp',ids[0]+':/tmp/comparison-cpu.pprof',path.join(dir,name+'.cpu.pprof')]);command(['cp',ids[0]+':/root/app',path.join(dir,'server-linux')]);return;}})().catch(error=>{profileError=error;});
  let sampleError;
  function sample(){try{const lines=command(['stats','--no-stream','--format','{{json .}}',...ids]).split('\n').filter(Boolean);resources.push({elapsedSeconds:(performance.now()-started)/1000,containers:lines.map(JSON.parse)});}catch(e){sampleError=e;}}
  sample();const timer=setInterval(sample,3000);
  const timeout=setTimeout(()=>{if(!finished){try{const info=json(['inspect',`${network}-load`])[0];assert.equal(info.Config.Labels['flamedb.compare'],run);command(['rm','-f',info.Id]);}catch(e){sampleError=e;}}},900000);
  let status;try{status=await done;await profile;}finally{clearInterval(timer);clearTimeout(timeout);sample();}
  fs.writeFileSync(reportFile,Buffer.concat(out));fs.writeFileSync(errorFile,Buffer.concat(err));
  if(status!==0){for(const id of ids){owned('container',id);const logs=spawnSync('docker',['logs',id],{encoding:'utf8',maxBuffer:8*1024*1024});fs.writeFileSync(path.join(dir,name+'-failure-'+id.slice(0,12)+'.log'),JSON.stringify(json(['inspect',id])[0].State)+'\n'+(logs.stdout||'')+(logs.stderr||''));}}
  if(sampleError)throw sampleError;if(profileError)throw profileError;assert.equal(status,0,Buffer.concat(err).toString());
  const report=JSON.parse(Buffer.concat(out).toString());
  results.push({backend,round,case:item.name,image,report,resources,totalSecondsIncludingSetupAndReadback:(performance.now()-started)/1000});
  fs.writeFileSync(path.join(dir,'results.json'),JSON.stringify(results,null,2));
  console.log(`DONE ${name}: ${report.confirmed_events_per_second.toFixed(1)} events/s; ${report.confirmed_reads_per_second.toFixed(1)} reads/s; p99 <= ${report.latency_ms_bucket_upper_bound.p99.toFixed(2)}ms`);
 }
 try {
  command(['network','create','--label',`flamedb.compare=${run}`,network]);
  const order=firstBackend==='pebble'?['pebble','nextgen']:['nextgen','pebble'];
  for(const round of [1,2])for(const backend of (round===1?order:[...order].reverse()))for(const [index,item] of matrix.entries()){
   if(results.some(r=>r.round===round&&r.backend===backend&&r.case===item.name))continue;
   const mark=created.length,role=`r${round}-${backend==='pebble'?'p':'n'}-c${index}`,config=path.join(dir,role+'.yml');
   const common=`auth:\n  internal_key: compare-internal\n  keys:\n    - name: test\n      key: compare-key\n      permissions: [read, write]\nserver:\n  host: 0.0.0.0\n  port: 7777\n  node_id: ${role}\n  data_path: /data\n  metrics_address: 0.0.0.0:9090\n  request_timeout: 30s\n  max_connections: 1024\n  max_request_bytes: 8388608\n  max_response_bytes: 67108864\n  max_batch_entries: 10000\n`;
   let ids=[],args=[],endpoint=`${network}-${role}-api:7777`;
   if(backend==='nextgen'){
    const pgVol=volume(role+'-pg'),cacheVol=volume(role+'-redis');
    const pg=container(role+'-pg',['--cpus',String(postgresCPUs),'--memory','2816m','-e','POSTGRES_USER=flamedb','-e','POSTGRES_PASSWORD=flamedb','-e','POSTGRES_DB=flamedb','--mount',`type=volume,source=${pgVol},target=/var/lib/postgresql/data`,'--health-cmd','pg_isready -h 127.0.0.1 -U flamedb -d flamedb','--health-interval','1s',pgImage,'postgres','-c','fsync=on','-c','full_page_writes=on','-c','synchronous_commit=on','-c','shared_buffers=256MB','-c','max_wal_size=4GB','-c','checkpoint_timeout=2min','-c','wal_compression=lz4']);
    const redis=container(role+'-redis',['--cpus',String(redisCPUs),'--memory','256m','--mount',`type=volume,source=${cacheVol},target=/data`,redisImage,'redis-server','--appendonly','yes','--appendfsync','everysec']);await health(pg);
    fs.writeFileSync(config,common+`storage:\n  engine: timescaledb\n  postgres:\n    dsn: postgres://flamedb:flamedb@${network}-${role}-pg:5432/flamedb?sslmode=disable\n    max_connections: 8\n  redis:\n    mode: standalone\n    address: ${network}-${role}-redis:6379\n    pool_size: 32\n  max_query_events: 1000000\ncluster:\n  enabled: true\n  replication_factor: 1\nperformance:\n  batch_size: 1000\n  max_inflight_requests: 512\n`);
    if(profileAPI)fs.appendFileSync(config,'  profile_address: 0.0.0.0:6060\n');
    const api=container(role+'-api',['--cpus',String(apiCPUs),'--memory','1024m','--mount',`type=bind,source=${config},target=/etc/compare.yml,readonly`,'--health-cmd','wget -q -O /dev/null http://127.0.0.1:9090/readyz','--health-interval','1s',image,'./app','/etc/compare.yml']);await health(api);ids=[api,pg,redis];
    env[role+'-postgres-settings']=command(['exec',pg,'psql','-U','flamedb','-d','flamedb','-At','-c',"SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit','max_wal_size','checkpoint_timeout','shared_buffers','wal_compression')"]);
   }else{
    const data=volume(role+'-data');fs.writeFileSync(config,common+`storage:\n  engine: pebble\n  compression: snappy\ncluster:\n  enabled: false\n  replication_factor: 1\n`);
    const api=container(role+'-api',['--cpus','4','--memory','4096m','--mount',`type=volume,source=${data},target=/data`,'--mount',`type=bind,source=${config},target=/etc/compare.yml,readonly`,image,'./app','/etc/compare.yml']);ids=[api];await wait(1000);
   }
   await measure(backend,round,item,ids,endpoint);
   for(const id of ids){const r=spawnSync('docker',['logs',id],{encoding:'utf8',maxBuffer:8*1024*1024});fs.writeFileSync(path.join(dir,role+'-'+id.slice(0,12)+'.log'),(r.stdout||'')+(r.stderr||''));}
   for(const resource of [...created.slice(mark)].reverse())remove(resource);
  }
  for(const id of prior)assert.equal(json(['inspect',id])[0].State.Running,true,'existing service stopped');
  env.priorServicesPreserved=true;env.finishedUtc=new Date().toISOString();
  fs.writeFileSync(path.join(dir,'environment.json'),JSON.stringify(env,null,2));validate(env,results);
  console.log('CURRENT_BACKENDS_EQUAL_WORKLOADS_VERIFIED');
 }finally{
  for(const resource of [...created].reverse())remove(resource);
  const info=json(['network','inspect',network])[0];assert.equal(info.Labels['flamedb.compare'],run);command(['network','rm',network]);
 }
}
if(action==='run'||action==='resume')run().catch(e=>{console.error(e);process.exitCode=1;});
else if(action==='verify'||action==='controls'||action==='report'){
 const env=JSON.parse(fs.readFileSync(path.join(dir,'environment.json'))),rows=JSON.parse(fs.readFileSync(path.join(dir,'results.json')));
 validate(env,rows);assert.equal(env.priorServicesPreserved,true);assert.equal(sha(env.loader||'.compat/backend-comparison/loadtest-linux'),env.loaderSha256);
 for(const source of env.source)assert.equal(sha(source.path),source.sha256,`source changed: ${source.path}`);
 if(action==='controls'){
  const bad=JSON.parse(JSON.stringify(rows));bad[0].report.events_confirmed++;assert.throws(()=>validate(env,bad));
  const badHistory=JSON.parse(JSON.stringify(rows));badHistory[0].report.fixture_events_per_metric++;assert.throws(()=>validate(env,badHistory));
  const badRate=JSON.parse(JSON.stringify(rows));const sample=badRate.find(r=>r.report.batch_size>1)||badRate[0];sample.report.confirmed_requests_per_second=sample.report.batch_size>1?sample.report.confirmed_events_per_second:sample.report.confirmed_requests_per_second+1;assert.throws(()=>validate(env,badRate));
  assert.throws(()=>validate(env,rows.slice(1)));console.log('BACKEND_COMPARISON_NEGATIVE_CONTROLS_VERIFIED');
 }else if(action==='report'){
  assert.equal(dir,path.join(root,'.compat/backend-comparison'),'Preserve the original comparison report; use a separate report for optimization experiments');
  assert.equal(env.profileAPI||false,false,'Profiling runs cannot replace the comparison report');
  const avg=(backend,name,field)=>{const values=rows.filter(r=>r.backend===backend&&r.case===name).map(r=>r.report[field]);assert.equal(values.length,2);return (values[0]+values[1])/2;};
  const format=n=>n.toLocaleString('en-US',{maximumFractionDigits:1});
  const labels={single:'Single leaderboard writes, 32 connections','batch-one-sender':'128-event batches, one sender','batch-parallel':'128-event batches, 32 connections','player-sum-20':'Player SUM, 20 events/player/metric','player-sum-100':'Player SUM, 100 events/player/metric',top:'Top 10 leaderboard, 1500 entities','mixed-player':'80/20 batch writes / player SUM'};
  let text='# Current Pebble versus Next Gen: local comparison\n\nMeasured on 2026-10-14 on the local i3-12100F Docker Desktop environment. Values are the mean of two 20-second submission runs, including response drain. Each run has fresh labeled Linux Docker volumes; both server modes use the same compiled image. This compares the repaired Pebble backend with the current PostgreSQL/TimescaleDB + Redis implementation.\n\n';
  text+='| Workload | Unit | Pebble | Next Gen |\n| --- | --- | ---: | ---: |\n';
  for(const item of matrix){
   const field=item.mode==='write'?'confirmed_events_per_second':'confirmed_reads_per_second';
   const value=b=>item.mode==='player-mixed'?`${format(avg(b,item.name,'confirmed_events_per_second'))} events/s + ${format(avg(b,item.name,'confirmed_reads_per_second'))} reads/s`:format(avg(b,item.name,field));
   text+=`| ${labels[item.name]} | ${item.mode==='write'?'events/s':item.mode==='player-mixed'?'events/s + reads/s':'queries/s'} | ${value('pebble')} | ${value('nextgen')} |\n`;
  }
  text+='\nA batch is one protocol request containing 128 events. Divide the batch event rate by 128 for write requests/s. Mixed total requests/s equals events/s divided by 128 plus read queries/s. Single writes have one event per request. Only successful server responses count; journal admission is not measured.\n\n';
  text+='## Parameters and correctness\n\nBoth backends receive a total budget of 4 vCPUs and 4 GiB. Pebble receives that budget in one process. Next Gen splits it between API (1 vCPU, 1 GiB), PostgreSQL (2.5 vCPUs, 2.75 GiB), and Redis (0.5 vCPU, 0.25 GiB). The separate Linux client has 2 vCPUs and 512 MiB. This allocation affects results and is not a claim that it is optimal for every workload. The Docker VM exposes 8 CPUs and about 15.54 GiB. Existing test services remain running; no production service was replaced.\n\nWrite-only runs use 250 metrics and an ID range of 1500 players, starting without score entries. They include creation of new metric/entity scores. Read and mixed runs use five metrics with every one of the 1500 players prefilled. The two immutable SUM histories contain 150000 and 750000 events respectively. Queries use `WHERE player=... AND region="eu"`. Mixed tests start with 150000 events and use a read after every four write batches. All writes update the default leaderboard and retain original raw events/tags. Explicit timestamps and pipeline=1 are identical in both modes; the response timeout remains five seconds.\n\nThe dataset uses synthetic player IDs, value=1 and region=eu throughout. The usual EraMC player-only SUM filter is not directly measured by this two-filter scenario. No manual ANALYZE is run after fixture loading; PostgreSQL planner statistics and automatic maintenance on fresh databases can influence these short measurements. Do not substitute the results for a mature five-day production dataset.\n\n';
  const events=rows.reduce((n,r)=>n+r.report.events_confirmed,0),reads=rows.reduce((n,r)=>n+r.report.reads_confirmed,0);
  text+=`All ${rows.length} measured runs have zero reported errors, zero ambiguous events and exact independent count/sum/entity-score readback. Total measured confirmed writes: ${events.toLocaleString('en-US')} events; reads: ${reads.toLocaleString('en-US')}. Player SUM runs additionally check every one of the 7500 metric/player results against input and successful-ACK counters after response drain. Immutable timed responses match independently constructed SUM fingerprints; mixed reads have evolving values and receive full independent verification after drain.\n\n`;
  text+='Timed target coverage is stored as `player_query_targets_observed` in every player workload report. Some slower SUM and mixed runs do not cycle through all 7500 targets within 20 seconds; every target still receives independent verification after drain.\n\n';
  text+='## Latency and observed memory\n\nQuantiles are upper bounds from the loader\'s fixed histogram, not exact percentile samples. Memory is sampled Docker CLI memory usage (Linux cache adjustment), summed across backend components; it includes setup/readback and is not RSS or an end-to-end Minecraft memory budget.\n\n| Workload | Pebble p99 range (ms) | Next Gen p99 range (ms) |\n| --- | ---: | ---: |\n';
  for(const item of matrix){const range=b=>{const values=rows.filter(r=>r.backend===b&&r.case===item.name).map(r=>r.report.latency_ms_bucket_upper_bound.p99);return `${format(Math.min(...values))}–${format(Math.max(...values))}`;};text+=`| ${labels[item.name]} | ${range('pebble')} | ${range('nextgen')} |\n`;}
  const memory=value=>{const match=value.split(' / ')[0].match(/^([\d.]+)(B|KiB|MiB|GiB)$/);assert.ok(match);return +match[1]*({B:1/1048576,KiB:1/1024,MiB:1,GiB:1024}[match[2]]);};
  text+='\n| Equal immutable dataset | Pebble observed peak range (MiB) | Next Gen observed peak range (MiB) |\n| --- | ---: | ---: |\n';
  for(const name of ['player-sum-20','player-sum-100']){const range=b=>{const values=rows.filter(r=>r.backend===b&&r.case===name).map(r=>Math.max(...r.resources.map(s=>s.containers.reduce((sum,c)=>sum+memory(c.MemUsage),0))));return `${format(Math.min(...values))}–${format(Math.max(...values))}`;};text+=`| ${labels[name]} | ${range('pebble')} | ${range('nextgen')} |\n`;}
  text+='\n## Architecture and limits\n\nPebble keeps raw events, tag indexes and ordered score keys in the Go server\'s embedded database. Next Gen uses durable PostgreSQL transactions and a rebuildable Redis ranking projection, so separate API instances can share authoritative data. Both retain the old TCP/SDK commands and buffered Kotlin transport. Dedicated RANK/SCORE/AROUND/count commands are implemented by the Next Gen engine; the current legacy executor does not implement those new ranking commands. This comparison measures their common TOP/GET/WRITE surfaces.\n\nNext Gen requires operating PostgreSQL and Redis as well as the API. Its deployment still needs furnished production promotion/fencing and HA qualification; additional API replicas alone are not database HA. The benchmark uses one authoritative copy for both backends. Pebble acknowledgements use synchronous WAL commits; PostgreSQL has fsync, full_page_writes and synchronous_commit enabled. Redis is not the sole durable score source. No expiry, aggregation shortcut or unknown-write replay is used.\n\nTwo initial Next Gen test-fixture starts failed before a workload began. A retry diagnosed an API start before PostgreSQL listened on TCP: a Unix-socket `pg_isready` check could pass for the temporary init server. The fixture now checks `pg_isready -h 127.0.0.1` before starting the API. Diagnostic logs and successfully completed earlier samples are retained; resumed samples use the unchanged client/image/workload/resource limits. This startup correction does not change database query settings.\n\nThese are short closed-loop server measurements, not a sustained open-loop test at 30000–375000 offered events/s. They exclude SDK disk-journal overhead, Minecraft TPS/MSPT, multiple database replicas, million-entity boards and production Ryzen measurements. The 1500 player IDs are not 1500 Minecraft clients. No universal performance winner or production capacity guarantee follows from this matrix.\n\nArtifacts: `.compat/backend-comparison/environment.json`, `results.json`, per-run output/errors, backend logs and fixture failure diagnostics. Reproduce with `go build` for the Linux `cmd/loadtest` binary and `node deploy/compare-backends.cjs run`; validate with `verify` and `controls`, then regenerate this document with `report`. Comparison volumes/containers are removed only after ownership-label checks; original services stay running.\n';
  fs.writeFileSync(path.join(root,'docs/backend-comparison.md'),text);console.log('BACKEND_COMPARISON_REPORT_WRITTEN');
 }else console.log('CURRENT_BACKENDS_EQUAL_WORKLOADS_VERIFIED');
}else throw new Error('Choose run, verify or controls');
