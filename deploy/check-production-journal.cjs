// Real journal inspection/reproduction. The source directory is read-only.
// Unknown records are replayed ONLY into a fresh, labeled local test database.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const assert=require('node:assert/strict'),{crc32}=require('node:zlib'),{spawnSync}=require('node:child_process');
const {TextDecoder}=require('node:util');
const net=require('node:net');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/production-journal');
const inventoryPath=path.join(dir,'inventory.json');
const source=process.argv[3]||process.env.FLAMEDB_JOURNAL_SOURCE||(fs.existsSync(inventoryPath)?JSON.parse(fs.readFileSync(inventoryPath)).source:undefined);
assert.ok(source,'Pass the journal directory to inspect, or set FLAMEDB_JOURNAL_SOURCE');
const mode=process.argv[2];fs.mkdirSync(dir,{recursive:true});
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
function inspect(){
 const files=fs.readdirSync(source,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name));
 const fingerprint=[],groups={},metrics=new Map(),metricEntities=new Map(),boards=new Map(),entities=new Set(),tags=new Map(),tagValues=new Map(),entityShapes=new Map(),ids=new Set();
 let bytes=0,records=0,duplicates=0,quorum=0,minTs,maxTs,minimum=Infinity,maximum=-Infinity;
 const batches=[],utf8=new TextDecoder('utf-8',{fatal:true});
 for(const file of files){
  assert.ok(file.isFile()&&!file.isSymbolicLink(),'Source must contain ordinary files only');
  const data=fs.readFileSync(path.join(source,file.name)),ext=path.extname(file.name);
  fingerprint.push({name:file.name,bytes:data.length,sha256:hash(data)});bytes+=data.length;
  groups[ext]??={files:0,bytes:0,records:0};groups[ext].files++;groups[ext].bytes+=data.length;
  if(ext==='.lock'){assert.equal(data.length,0);continue;}
  assert.equal(ext,'.uncertain','This qualification must not silently ignore another journal state');
  assert.ok(data.length<=512*1024+64*1024+8,'Oversized source batch');
  let offset=0,count=0;
  while(offset<data.length){
   assert.ok(data.length-offset>=8,'Truncated frame header');
   const size=data.readInt32BE(offset),checksum=data.readUInt32BE(offset+4);offset+=8;
   assert.ok(size>0&&size<=64*1024&&size<=data.length-offset,'Invalid frame size');
   const payload=data.subarray(offset,offset+size);offset+=size;
   assert.equal(crc32(payload),checksum,'Journal CRC mismatch');
   const text=utf8.decode(payload);
   // Never round nanosecond timestamps through a JavaScript Number.
   const entry=JSON.parse(text,(key,value,context)=>key==='timestampNs'?context.source:value);
   for(const key of ['id','metric','value','leaderboardEntity','tags','timestampNs','quorum','leaderboardEntities'])assert.ok(Object.hasOwn(entry,key),'Missing '+key);
   assert.ok(typeof entry.id==='string'&&typeof entry.metric==='string'&&Number.isFinite(entry.value));
   assert.ok(entry.leaderboardEntity===null||typeof entry.leaderboardEntity==='string');
   assert.equal(typeof entry.quorum,'boolean');
   assert.match(entry.timestampNs,/^\d+$/);const ts=BigInt(entry.timestampNs);assert.ok(ts<=9223372036854775807n);
   minTs=minTs===undefined||ts<minTs?ts:minTs;maxTs=maxTs===undefined||ts>maxTs?ts:maxTs;
   assert.ok(entry.tags&&typeof entry.tags==='object'&&!Array.isArray(entry.tags));
   assert.ok(entry.leaderboardEntities&&typeof entry.leaderboardEntities==='object'&&!Array.isArray(entry.leaderboardEntities));
   for(const [key,value] of Object.entries(entry.tags)){assert.equal(typeof value,'string');tags.set(key,(tags.get(key)||0)+1);if(!tagValues.has(key))tagValues.set(key,new Set());tagValues.get(key).add(value);}
   metrics.set(entry.metric,(metrics.get(entry.metric)||0)+1);
   if(!metricEntities.has(entry.metric))metricEntities.set(entry.metric,new Set());
   if(entry.leaderboardEntity!==null)metricEntities.get(entry.metric).add(entry.leaderboardEntity);
   const destinations=new Map(Object.entries(entry.leaderboardEntities));
   if(entry.leaderboardEntity!==null)destinations.set('',entry.leaderboardEntity);
   for(const [board,entity] of destinations){assert.equal(typeof entity,'string');boards.set(board,(boards.get(board)||0)+1);entities.add(entity);const shape=entity.replace(/[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}/gi,'<uuid>').replace(/\d{6,}/g,'<number>');entityShapes.set(shape,(entityShapes.get(shape)||0)+1);}
   if(ids.has(entry.id))duplicates++;ids.add(entry.id);
   minimum=Math.min(minimum,entry.value);maximum=Math.max(maximum,entry.value);if(entry.quorum)quorum++;
   count++;records++;groups[ext].records++;assert.ok(count<=512,'Too many entries per batch');
  }
  batches.push({name:file.name,records:count,bytes:data.length});
 }
 assert.ok(records>0);assert.equal(duplicates,0,'Repeated record IDs require explicit reconciliation before selecting a replay sequence');
 const report={source,files:files.length,bytes,groups,records,batches,duplicateIds:duplicates,quorum,distinctMetrics:metrics.size,distinctBoardNames:boards.size,distinctEntities:entities.size,
  valueRange:{minimum,maximum},firstTimestamp:minTs.toString(),lastTimestamp:maxTs.toString(),
  metrics:[...metrics].map(([metric,records])=>({metric,hash:hash(metric).slice(0,16),records,entities:metricEntities.get(metric).size})).sort((a,b)=>b.records-a.records),
  boardEventCounts:[...boards].map(([board,records])=>({board:board||'(default)',records})),tagKeyCounts:Object.fromEntries(tags),tagValueCounts:Object.fromEntries([...tagValues].map(([k,v])=>[k,v.size])),entityShapes:Object.fromEntries(entityShapes),fingerprint};
 const saved=path.join(dir,'inventory.json');
 if(mode==='inspect')fs.writeFileSync(saved,JSON.stringify(report,null,2));
 else assert.deepEqual(report.fingerprint,JSON.parse(fs.readFileSync(saved)).fingerprint,'Source journal changed since inspection');
 return report;
}
function run(command,args,cwd=root,env=process.env,file){
 const r=spawnSync(command,args,{cwd,env,encoding:'utf8',windowsHide:true,timeout:600000,maxBuffer:16*1024*1024});
 if(file)fs.writeFileSync(path.join(dir,file),(r.stdout||'')+'\n'+(r.stderr||''));
 if(r.error||r.status!==0)throw r.error||Error(command+' failed '+r.status+'\n'+(r.stdout||'').slice(-3000)+'\n'+(r.stderr||'').slice(-3000));
 return r.stdout.trim();
}
async function replay(){
 const before=inspect(),runId=Date.now()+'-'+process.pid,name='flamedb-real-journal-'+runId;
 const caseName=process.env.FLAMEDB_JOURNAL_CASE||'dev47';assert.match(caseName,/^[a-z0-9-]+$/);
 const imageRef=process.env.FLAMEDB_JOURNAL_IMAGE||'ghcr.io/bypixeltv/flamedb@sha256:4f302e1e98f2ca9d2d7e148d4135fc7f96398c682df6d725b28aa5248d3a4962';
 const docker=args=>run('docker',args),json=args=>JSON.parse(docker(args));
 const image=json(['image','inspect',imageRef])[0].Id;
 const output=path.join(dir,caseName+'-'+runId),input=path.join(output,'input'),journal=path.join(output,'journal'),config=path.join(output,'server.yml');
 fs.mkdirSync(input,{recursive:true});
 for(const file of before.fingerprint.filter(f=>f.name.endsWith('.uncertain'))){
  const target=path.join(input,file.name);fs.copyFileSync(path.join(source,file.name),target);assert.equal(hash(fs.readFileSync(target)),file.sha256);
 }
 fs.writeFileSync(config,`auth:\n  keys:\n    - name: test\n      key: production-journal-check\n      permissions: [read, write]\nstorage:\n  engine: pebble\n  compression: zstd\nserver:\n  host: 0.0.0.0\n  port: 7777\n  node_id: node-1\n  data_path: /data\ncluster:\n  seeds: []\n  replication_factor: 1\n`);
 const volume=name+'-data';docker(['volume','create','--label','flamedb.real-journal='+runId,volume]);
 let id;
 function owned(){const actual=json(['inspect',id])[0];assert.equal(actual.Config.Labels['flamedb.real-journal'],runId);assert.equal(actual.Image,image);return actual;}
 const port=()=>docker(['port',id,'7777/tcp']).split(':').at(-1);
 async function ready(){
  for(let attempt=0;attempt<60;attempt++){
   assert.ok(owned().State.Running,'Fixture server stopped');
   try{await new Promise((resolve,reject)=>{
    const socket=net.createConnection({host:'127.0.0.1',port:Number(port())});let buffer='',authenticated=false;
    const done=error=>{clearTimeout(timer);socket.destroy();error?reject(error):resolve();};
    const timer=setTimeout(()=>done(Error('AUTH readiness timeout')),1000);socket.once('error',done);
    socket.on('data',chunk=>{buffer+=chunk.toString();if(buffer.length>2048)return done(Error('Oversized AUTH'));
     while(buffer.includes('\n')){const end=buffer.indexOf('\n'),line=buffer.slice(0,end);buffer=buffer.slice(end+1);let response;try{response=JSON.parse(line);}catch(error){return done(error);}
      if(!authenticated&&response.auth==='required'){authenticated=true;socket.write('AUTH production-journal-check\n');}
      else if(authenticated&&response.auth==='ok')return done();else return done(Error('Unexpected AUTH'));
     }
    });
   });return;}catch(error){await new Promise(resolve=>setTimeout(resolve,500));}
  }throw Error('Fixture AUTH never became ready');
 }
 function gradle(method,env,file){
  const args=['productionJournalTest','--tests','dev.bypixel.flamedb.ProductionJournalTest.'+method,'--rerun-tasks','--console=plain'];
  const cmd=process.platform==='win32'?'cmd.exe':'./gradlew';
  const out=run(cmd,process.platform==='win32'?['/d','/c','gradlew.bat',...args]:args,path.join(root,'pkg/kotlin'),env,file);
  assert.match(out,/BUILD SUCCESSFUL/);return out;
 }
 let result;
 try{
  id=docker(['run','-d','--name',name,'--label','flamedb.real-journal='+runId,'--cpus','4','--memory','4g','-p','127.0.0.1::7777','--mount',`type=volume,source=${volume},target=/data`,'--mount',`type=bind,source=${config},target=/etc/test.yml,readonly`,image,'./app','/etc/test.yml']);
  await ready();
  let env={...process.env,FLAMEDB_JOURNAL_INPUT:input,FLAMEDB_JOURNAL_COPY:journal,FLAMEDB_JOURNAL_PORT:port()};
  const first=gradle('replayCopiedUnknownRecords',env,caseName+'-sdk-replay.txt');
  assert.match(first,new RegExp('PRODUCTION_JOURNAL_COPY_REPLAY_VERIFIED events='+before.records+' batches='+before.batches.length));
  const latency=first.match(/PRODUCTION_JOURNAL_LATENCY ([^\r\n]+)/);assert.ok(latency);
  result=Object.fromEntries(latency[1].split(' ').map(pair=>{const [key,value]=pair.split('=');return [key,Number(value)];}));
  owned();docker(['kill','--signal','KILL',id]);docker(['start',id]);await ready();env.FLAMEDB_JOURNAL_PORT=port();
  const second=gradle('acknowledgedCopySurvivesKill',env,caseName+'-sdk-after-kill.txt');
  assert.match(second,new RegExp('PRODUCTION_JOURNAL_KILL_READBACK_VERIFIED events='+before.records+' replayed=0'));
  fs.writeFileSync(path.join(dir,caseName+'-result.json'),JSON.stringify({runId,image,imageRef,output,cpus:4,memoryMiB:4096,compression:'zstd',replicationFactor:1,readConnections:2,writeConnections:1,result,sourceFingerprint:hash(JSON.stringify(before.fingerprint)),testSource:hash(fs.readFileSync(path.join(root,'pkg/kotlin/src/test/kotlin/dev/bypixel/flamedb/ProductionJournalTest.kt'))),pebbleSource:{batch:hash(fs.readFileSync(path.join(root,'internal/query/pebble_batch.go'))),lookup:fs.existsSync(path.join(root,'internal/aggregates/batch_lookup.go'))?hash(fs.readFileSync(path.join(root,'internal/aggregates/batch_lookup.go'))):null}},null,2));
 }finally{
  if(id){owned();const logs=spawnSync('docker',['logs',id],{encoding:'utf8',windowsHide:true});fs.writeFileSync(path.join(dir,caseName+'-server.log'),(logs.stdout||'')+(logs.stderr||''));docker(['rm','-f',id]);}
  assert.equal(json(['volume','inspect',volume])[0].Labels['flamedb.real-journal'],runId);docker(['volume','rm',volume]);
  assert.deepEqual(inspect().fingerprint,before.fingerprint,'Original source journal was modified');
 }
 console.log(JSON.stringify({case:caseName,result}));
 console.log('PRODUCTION_JOURNAL_REPLAY_VERIFIED');
}
Promise.resolve().then(async()=>{
 if(mode==='inspect'){const r=inspect();console.log(JSON.stringify({files:r.files,bytes:r.bytes,groups:r.groups,records:r.records,distinctMetrics:r.distinctMetrics,distinctBoardNames:r.distinctBoardNames,distinctEntities:r.distinctEntities,metrics:r.metrics,boardEventCounts:r.boardEventCounts,tagKeyCounts:r.tagKeyCounts,tagValueCounts:r.tagValueCounts,entityShapes:r.entityShapes,valueRange:r.valueRange}));console.log('PRODUCTION_JOURNAL_INSPECTED');}
 else if(mode==='replay')await replay();
 else if(mode==='candidate'){
  process.env.FLAMEDB_JOURNAL_CASE='candidate';process.env.FLAMEDB_JOURNAL_IMAGE??='flamedb:production-journal-fix';await replay();
  const baseline=JSON.parse(fs.readFileSync(path.join(dir,'dev47-result.json'))),candidate=JSON.parse(fs.readFileSync(path.join(dir,'candidate-result.json')));
  for(const key of ['sourceFingerprint','testSource','cpus','memoryMiB','compression','replicationFactor','readConnections','writeConnections'])assert.equal(candidate[key],baseline[key],'Unmatched comparison '+key);
  assert.equal(candidate.result.events,baseline.result.events);assert.equal(candidate.result.batches,baseline.result.batches);
  assert.ok(candidate.result.p99_ms<baseline.result.p99_ms,'No p99 improvement');
  assert.ok(candidate.result.elapsed_s<baseline.result.elapsed_s,'No throughput improvement');
  console.log('PRODUCTION_JOURNAL_CANDIDATE_VERIFIED');
 }else throw Error('Choose inspect, replay or candidate');
}).catch(error=>{console.error(error);process.exitCode=1;});
