// Local, test-owned fixtures only. Never deletes/replays a production journal.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {spawnSync}=require('node:child_process');
const net=require('node:net');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/pebble-journal-rootcause'),mode=process.argv[2];
fs.mkdirSync(dir,{recursive:true});
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
function run(command,args,cwd=root,env=process.env,file){
 const r=spawnSync(command,args,{cwd,env,encoding:'utf8',windowsHide:true,timeout:900000,maxBuffer:32*1024*1024});
 if(file)fs.writeFileSync(path.join(dir,file),(r.stdout||'')+'\n'+(r.stderr||''));
 if(r.error||r.status!==0)throw r.error||Error(`${command} failed: ${r.status}\n${(r.stdout||'').slice(-2000)}\n${(r.stderr||'').slice(-4000)}`);
 return r.stdout.trim();
}
const docker=args=>run('docker',args),json=args=>JSON.parse(docker(args));
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function correctness(){
 const out=run('go',['test','-race','-count=1','-timeout=5m','./internal/query','./internal/aggregates','./internal/storage','./internal/server','./internal/cluster'],root,process.env,'race.txt');
 for(const pkg of ['query','aggregates','storage','server','cluster'])assert.match(out,new RegExp('ok\\s+github.com/byPixelTV/flamedb/internal/'+pkg+'\\s'));
 run('go',['vet','./internal/query','./internal/aggregates','./internal/storage','./internal/server','./internal/cluster'],root,process.env,'vet.txt');
 const app=process.env.FLAMEDB_ERAMC_SOURCE||'C:/Dev/Java/EraMC';
 const config=fs.readFileSync(path.join(app,'platform/bukkit/Moonlight/src/main/resources/config.yml'),'utf8');
 assert.match(config,/max-journal-bytes:\s*4294967296/);
 for(const source of ['Moonlight.kt','metrics/MoonlightFlameDB.kt'])assert.match(fs.readFileSync(path.join(app,'platform/bukkit/Moonlight/src/main/kotlin/net/eramc/moonlight',source),'utf8'),/maxJournalBytes[^\n]*4L \* 1024 \* 1024 \* 1024/);
 run(process.execPath,['deploy/check-sdk-batching-adoption.cjs','application'],root,{...process.env,FLAMEDB_ERAMC_SOURCE:app},'moonlight.txt');
 console.log('PEBBLE_JOURNAL_CORRECTNESS_VERIFIED');
}
function benchmark(){
 const baseline=path.join(dir,'baseline');
 const old=run('git',['show','0bfdd68:internal/query/pebble_batch.go']);
 assert.equal(fs.readFileSync(path.join(baseline,'internal/query/pebble_batch.go'),'utf8').replaceAll('\r\n','\n').trim(),old.replaceAll('\r\n','\n').trim(),'Baseline must be the deployed dev.45 source');
 assert.equal(hash(path.join(baseline,'internal/query/pebble_five_boards_bench_test.go')),hash(path.join(root,'internal/query/pebble_five_boards_bench_test.go')));
 const env={...process.env,GOMAXPROCS:'4'};
 const records=[];
 for(const [name,cwd] of [['dev45',baseline],['candidate',root]]){
  console.log('BENCH '+name);
  const out=run('go',['test','./internal/query','-run','^$','-bench','^BenchmarkPebbleFiveBoards128$','-benchtime=125x','-count=3'],cwd,env,name+'-bench.txt');
  let activeCase;
  for(const line of out.split('\n')){
   const header=line.match(/^BenchmarkPebbleFiveBoards128\/(repeated|distinct)-4/);if(header)activeCase=header[1];
   // Pebble startup logs can split Go's benchmark name and measurements
   // across lines; keep the name rather than dropping real observations.
   const match=line.match(/(?:^|\s)125\s+(\d+) ns\/op/);if(!match)continue;assert.ok(activeCase);
   const wal=line.match(/([\d.]+) WAL-bytes\/batch/),rate=line.match(/([\d.]+) events\/s/);assert.ok(wal&&rate);
   records.push({backend:name,case:activeCase,ns:+match[1],wal:+wal[1],eventsPerSecond:+rate[1]});
  }
  assert.equal((out.match(/FIVE_BOARDS_READBACK_VERIFIED events=16000 boards=5/g)||[]).length,6);
 }
 const median=xs=>xs.sort((a,b)=>a-b)[1];
 const metric=(backend,c,field)=>{const selected=records.filter(r=>r.backend===backend&&r.case===c);assert.equal(selected.length,3);return median(selected.map(r=>r[field]));};
 for(const c of ['repeated','distinct'])console.log(`${c}: ns dev45=${metric('dev45',c,'ns')} candidate=${metric('candidate',c,'ns')}; WAL dev45=${metric('dev45',c,'wal')} candidate=${metric('candidate',c,'wal')}`);
 fs.writeFileSync(path.join(dir,'benchmark.json'),JSON.stringify({records,source:{batch:hash(path.join(root,'internal/query/pebble_batch.go')),executor:hash(path.join(root,'internal/query/executor.go'))},fixedIterations:125,boards:5,compression:'zstd',GOMAXPROCS:4},null,2));
 assert.ok(metric('candidate','repeated','wal')<metric('dev45','repeated','wal')*0.5,'No meaningful score-write reduction');
 assert.ok(metric('candidate','repeated','ns')<metric('dev45','repeated','ns'),'Repeated-score latency did not improve');
 assert.ok(metric('candidate','distinct','ns')<metric('dev45','distinct','ns')*1.2,'Distinct-metric regression exceeded 20%');
 console.log('PEBBLE_SCORE_COALESCING_VERIFIED');
}
async function sdk(){
 const runId=Date.now()+'-'+process.pid,name='flamedb-pebble-sdk-'+runId,config=path.join(dir,name+'.yml'),volume=name+'-data';
 const image=json(['image','inspect','flamedb:pebble-journal-fix'])[0].Id;
 fs.writeFileSync(config,`auth:\n  keys:\n    - name: test\n      key: pebble-sdk-check\n      permissions: [read, write]\nstorage:\n  engine: pebble\n  compression: zstd\nserver:\n  host: 0.0.0.0\n  port: 7777\n  node_id: node-1\n  data_path: /data\n  metrics_address: 0.0.0.0:9090\ncluster:\n  seeds: []\n  replication_factor: 1\n  replication_queue_size: 262144\n  fanout_queue_size: 262144\n`);
 docker(['volume','create','--label','flamedb.pebble-sdk='+runId,volume]);
 let id;
 function own(){const actual=json(['inspect',id])[0];assert.equal(actual.Config.Labels['flamedb.pebble-sdk'],runId);assert.equal(actual.Image,image);return actual;}
 const port=()=>docker(['port',id,'7777/tcp']).split(':').at(-1);
 async function ready(){
  // Legacy Pebble has no shared-backend HTTP /readyz. Probe its actual TCP
  // AUTH exchange rather than waiting for an unavailable HTTP endpoint.
  for(let i=0;i<60;i++){
   if(!own().State.Running)throw Error('Server stopped');
   try{await new Promise((resolve,reject)=>{
    const socket=net.createConnection({host:'127.0.0.1',port:Number(port())});let data='',authenticated=false;
    const timer=setTimeout(()=>{socket.destroy();reject(Error('AUTH readiness timeout'));},1000);
    socket.once('error',e=>{clearTimeout(timer);socket.destroy();reject(e);});
    socket.on('data',chunk=>{data+=chunk.toString();if(data.length>1024){clearTimeout(timer);socket.destroy();reject(Error('Oversized AUTH'));return;}
     while(data.includes('\n')){const end=data.indexOf('\n'),line=data.slice(0,end);data=data.slice(end+1);let response;try{response=JSON.parse(line);}catch(error){clearTimeout(timer);socket.destroy();reject(error);return;}
      if(!authenticated&&response.auth==='required'){authenticated=true;socket.write('AUTH pebble-sdk-check\n');}
      else if(authenticated&&response.auth==='ok'){clearTimeout(timer);socket.destroy();resolve();return;}
      else{clearTimeout(timer);socket.destroy();reject(Error('Unexpected AUTH response'));return;}
     }
    });
   });return;}catch(error){await sleep(500);}
  }throw Error('Fixture did not become ready over TCP');
 }
 function gradle(test,file){
  const args=['realPebbleTest','--tests','dev.bypixel.flamedb.RealPebbleBufferedTest.'+test,'--rerun-tasks','--console=plain'];
  const cmd=process.platform==='win32'?'cmd.exe':'./gradlew';
  const out=run(cmd,process.platform==='win32'?['/d','/c','gradlew.bat',...args]:args,path.join(root,'pkg/kotlin'),env,file);
  assert.match(out,/BUILD SUCCESSFUL/);return out;
 }
 let env;
 try{
  id=docker(['run','-d','--name',name,'--label','flamedb.pebble-sdk='+runId,'--cpus','4','--memory','4g','-p','127.0.0.1::7777','--mount',`type=volume,source=${volume},target=/data`,'--mount',`type=bind,source=${config},target=/etc/test.yml,readonly`,image,'./app','/etc/test.yml']);
  await ready();
  env={...process.env,FLAMEDB_PEBBLE_PORT:port(),FLAMEDB_PEBBLE_PREFIX:'checked.'+runId,FLAMEDB_PEBBLE_JOURNAL:path.join(dir,name+'-journal')};
  const first=gradle('pendingRecoveryAndConcurrentReads','sdk-before-kill.txt');assert.match(first,/PEBBLE_BUFFERED_RECOVERY_VERIFIED events=30000 offline=7500/);assert.match(first,/PEBBLE_BUFFERED_BATCH_LATENCY/);
  own();docker(['kill','--signal','KILL',id]);docker(['start',id]);await ready();env.FLAMEDB_PEBBLE_PORT=port();
  const second=gradle('acknowledgedStateSurvivesKill','sdk-after-kill.txt');assert.match(second,/PEBBLE_BUFFERED_KILL_READBACK_VERIFIED events=30000 boards=5 replayed=0/);
  fs.writeFileSync(path.join(dir,'sdk-environment.json'),JSON.stringify({runId,image,compression:'zstd',replicationFactor:1,cpus:4,memoryMiB:4096,events:30000,players:1500,metrics:5,boards:5,offlineEvents:7500,timeoutMs:5000,journal:env.FLAMEDB_PEBBLE_JOURNAL,source:{batch:hash(path.join(root,'internal/query/pebble_batch.go')),sdk:hash(path.join(root,'pkg/kotlin/src/test/kotlin/dev/bypixel/flamedb/RealPebbleBufferedTest.kt'))}},null,2));
  console.log('PEBBLE_BUFFERED_SDK_VERIFIED');
 }finally{
  if(id){own();const r=spawnSync('docker',['logs',id],{encoding:'utf8'});fs.writeFileSync(path.join(dir,'sdk-server.log'),(r.stdout||'')+(r.stderr||''));docker(['rm','-f',id]);}
  assert.equal(json(['volume','inspect',volume])[0].Labels['flamedb.pebble-sdk'],runId);docker(['volume','rm',volume]);
 }
}
Promise.resolve().then(()=>{if(mode==='correctness')return correctness();if(mode==='benchmark')return benchmark();if(mode==='sdk')return sdk();throw Error('Choose correctness, benchmark or sdk');}).catch(e=>{console.error(e);process.exitCode=1;});
