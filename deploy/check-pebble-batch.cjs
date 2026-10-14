const fs=require('node:fs'),path=require('node:path'),net=require('node:net'),readline=require('node:readline');
const assert=require('node:assert/strict'),crypto=require('node:crypto'),{spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/pebble-batch-timeout');
fs.mkdirSync(dir,{recursive:true});
const mode=process.argv[2];
const median=values=>[...values].sort((a,b)=>a-b)[Math.floor(values.length/2)];
function measurements(name){
 const text=fs.readFileSync(path.join(dir,name),'utf8');
 assert.match(text,/PASS/);
 const rows=[...text.matchAll(/125\s+(\d+) ns\/op\s+([\d.]+) WAL-bytes\/batch\s+([\d.]+) commands\/s\s+([\d.]+) events\/s\s+([\d.]+) fsyncs\/batch/g)];
 assert.equal(rows.length,3);
 assert.equal((text.match(/events=16000 metrics=250 entity-scores=16000/g)||[]).length,3);
 return rows.map(r=>({nanoseconds:+r[1],walBytes:+r[2],commands:+r[3],events:+r[4],syncs:+r[5]}));
}
function docker(...args){
 const r=spawnSync('docker',args,{cwd:root,encoding:'utf8',timeout:120000,maxBuffer:4*1024*1024});
 if(r.error||r.status!==0)throw r.error||new Error(`docker ${args[0]} failed: ${r.stderr}`);
 return r.stdout.trim();
}
async function connect(port){
 const socket=net.createConnection({host:'127.0.0.1',port});
 socket.setNoDelay(true);socket.on('error',()=>{});
 await new Promise((resolve,reject)=>{socket.once('connect',resolve);socket.once('error',reject);});
 const rl=readline.createInterface({input:socket,crlfDelay:Infinity}),iterator=rl[Symbol.asyncIterator]();
 async function read(){
  let timer;
  try {
   const result=await Promise.race([iterator.next(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('response exceeded unchanged 5000ms deadline')),5000);})]);
   assert.equal(result.done,false);const r=JSON.parse(result.value);assert.equal(r.error,undefined);return r;
  } finally {clearTimeout(timer);}
 }
 const client={query:async command=>{socket.write(command+'\n');return read();},close:()=>{rl.close();socket.destroy();}};
 try {assert.equal((await read()).auth,'required');assert.equal((await client.query('AUTH batch-check-key')).auth,'ok');return client;}
 catch(error){client.close();throw error;}
}
async function ready(port){
 const end=Date.now()+30000;let error;
 while(Date.now()<end){try{return await connect(port);}catch(e){error=e;await new Promise(resolve=>setTimeout(resolve,100));}}
 throw error;
}
async function tcp(){
 const run=`${Date.now()}-${process.pid}`,name=`flamedb-pebble-batch-check-${run}`;
 const data=path.join(dir,'data-'+run),config=path.join(dir,'docker-config-'+run+'.yml');
 fs.mkdirSync(data);
 fs.writeFileSync(config,`auth:\n  internal_key: batch-check-internal\n  keys:\n    - name: local-test\n      key: batch-check-key\n      permissions: [read, write]\nserver:\n  host: 0.0.0.0\n  port: 7777\n  node_id: batch-check\n  data_path: /data\nstorage:\n  engine: pebble\n  compression: none\ncluster:\n  enabled: false\n  replication_factor: 1\n`);
 const image=JSON.parse(docker('image','inspect','flamedb:pebble-batch-fix'))[0].Id;
 let id,db;
 function owned(){const inspect=JSON.parse(docker('inspect',id))[0];assert.equal(inspect.Config.Labels['flamedb.batch-check'],run);assert.equal(inspect.Image,image);}
 const counts=new Map(),scores=new Map(),history=new Map(),latencies=[];
 const result={image,events:16000,batchSize:128,metrics:250,entityRange:1500,boardsPerEvent:2,timeoutMs:5000};
 async function verify(){
  for(const [metric,want] of counts){
   const r=await db.query(`GET ${metric} SUM`);assert.equal(r.aggregate.count,want);assert.equal(r.aggregate.value,want);
   const events=(await db.query(`GET ${metric} LIMIT 2000`)).events;assert.equal(events.length,want);
   for(const event of events){assert.equal(event.value,1);assert.equal(event.tags.player,history.get(`${metric}:${event.timestamp}`));assert.equal(event.tags.region,'eu');}
   for(const board of ['', 'team']){
    const r=await db.query(`LEADERBOARD ${metric}${board?' BOARD '+board:''} LIMIT 2000`);
    const wantScores=scores.get(metric);assert.equal(r.leaderboard.length,wantScores.size);
    for(const entry of r.leaderboard)assert.equal(entry.value,wantScores.get(entry.entity_id));
   }
  }
 }
 try {
  id=docker('run','-d','--name',name,'--label',`flamedb.batch-check=${run}`,'-p','127.0.0.1::7777','--mount',`type=bind,source=${data},target=/data`,'--mount',`type=bind,source=${config},target=/etc/batch-check.yml,readonly`,'flamedb:pebble-batch-fix','./app','/etc/batch-check.yml');
  owned();const port=+docker('port',id,'7777/tcp').split(':').pop();result.initialHostPort=port;db=await ready(port);
  const begin=performance.now();
  for(let batch=0;batch<125;batch++){
   const lines=['WRITE_BATCH'];
   for(let i=0;i<128;i++){
    const seq=batch*128+i,metric=`batch:metric:${seq%250}`,entity=`player:${(seq*17+Math.floor(seq/250))%1500}`,ts=seq+1;
    lines.push(`WRITE ${metric} 1 lb="${entity}" lb.team="${entity}" player="${entity}" region="eu" ts=${ts}`);
    counts.set(metric,(counts.get(metric)||0)+1);history.set(`${metric}:${ts}`,entity);
    if(!scores.has(metric))scores.set(metric,new Map());const entries=scores.get(metric);entries.set(entity,(entries.get(entity)||0)+1);
   }
   lines.push('END');const start=performance.now(),r=await db.query(lines.join('\n'));
   assert.equal(r.ok,true);assert.equal(r.accepted,128);assert.equal(r.failed,0);latencies.push(performance.now()-start);
  }
  result.elapsedSeconds=(performance.now()-begin)/1000;result.confirmedEventsPerSecond=result.events/result.elapsedSeconds;
  result.confirmedCommandsPerSecond=125/result.elapsedSeconds;latencies.sort((a,b)=>a-b);
  result.latencyMs={p50:latencies[62],p99:latencies[123],max:latencies[124]};
  await verify();result.initialReadback=true;db.close();db=undefined;
  owned();docker('kill','--signal','KILL',id);docker('start',id);
  // An automatically assigned published port may change when Docker starts
  // the stopped container; resolve its actual mapping again after restart.
  result.restartedHostPort=+docker('port',id,'7777/tcp').split(':').pop();db=await ready(result.restartedHostPort);
  await verify();result.postKillReadback=true;result.errors=0;result.ambiguous=0;result.replayedEvents=0;
  result.fixtureDiskKiB=+docker('exec',id,'du','-sk','/data').split(/\s+/)[0];
  fs.writeFileSync(path.join(dir,'tcp-docker.json'),JSON.stringify(result,null,2));
  console.log(`Actual Linux TCP: ${result.confirmedEventsPerSecond.toFixed(1)} events/s; p99 ${result.latencyMs.p99.toFixed(2)}ms; 16000 events and both boards survived SIGKILL without replay`);
  console.log('PEBBLE_DOCKER_TCP_DURABILITY_VERIFIED');
 } finally {
  if(db)db.close();
  if(id){
   owned();
   const logs=spawnSync('docker',['logs',id],{encoding:'utf8',timeout:10000});
   fs.writeFileSync(path.join(dir,'docker-server.log'),(logs.stdout||'')+(logs.stderr||''));
   docker('rm','-f',id);
  }
 }
}
if(mode==='measurements'){
 const before=measurements('before.txt'),after=measurements('after.txt');
 assert.ok(before.every(r=>r.syncs>=127));assert.ok(after.every(r=>r.syncs<2));
 assert.ok(median(after.map(r=>r.nanoseconds))<median(before.map(r=>r.nanoseconds)));
 const result={before,after,speedup:median(before.map(r=>r.nanoseconds))/median(after.map(r=>r.nanoseconds)),eventsPerRun:16000,metrics:250,independentReadback:true};
 fs.writeFileSync(path.join(dir,'comparison.json'),JSON.stringify(result,null,2));
 console.log(`Median durable batch speedup: ${result.speedup.toFixed(2)}x`);console.log('PEBBLE_DURABLE_BATCH_COMPARISON_VERIFIED');
}else if(mode==='tcp'){tcp().catch(error=>{console.error(error);process.exitCode=1;});}
else throw new Error('Choose measurements or tcp');
