// Diagnose only on fresh, labeled fixtures. Never use existing database volumes.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/nextgen-parity/plans');fs.mkdirSync(dir,{recursive:true});
const action=process.argv[2],file=path.join(dir,'results.json');
function docker(args){const r=spawnSync('docker',args,{encoding:'utf8',cwd:root,timeout:180000,maxBuffer:16*1024*1024});if(r.error||r.status!==0)throw r.error||Error(r.stderr);return r.stdout.trim();}
const json=args=>JSON.parse(docker(args));
function verify(rows){
 for(const count of [150000,750000])for(const filter of ['player','player-region'])for(const stage of ['fresh','analyzed','vacuumed']) {
  const row=rows.find(r=>r.count===count&&r.filter===filter&&r.stage===stage);assert.ok(row);assert.ok(row.plan['Execution Time']>0);assert.equal(row.result[0],count/7500);assert.equal(row.result[1],count/7500);
 }
 for(const count of [150000,750000])for(const filter of ['player','player-region']){
  const row=rows.find(r=>r.count===count&&r.filter===filter&&r.variant==='generated-generic');assert.ok(row,'Missing optimized generic plan');
  assert.deepEqual(row.result,[count/7500,count/7500]);
  const plan=JSON.stringify(row.plan);assert.match(plan,/flame_events_tag_candidates/);assert.doesNotMatch(plan,/flame_events_ts_idx/);
 }
}
if(action==='verify'){verify(JSON.parse(fs.readFileSync(file)));console.log('PLAYER_SQL_PLANS_VERIFIED');}
else if(action==='run') (async()=>{
 const run='fdbplan-'+Date.now(),created=[],rows=[],prior=docker(['ps','-q']).split(/\s+/).filter(Boolean);
 function own(type,id){const r=json([type,'inspect',id])[0];assert.equal((type==='container'?r.Config.Labels:r.Labels)['flamedb.plans'],run);return r;}
 function start(role,args){const id=docker(['run','-d','--name',run+'-'+role,'--label','flamedb.plans='+run,'--network',run,...args]);created.push(['container',id]);own('container',id);return id;}
 function sql(pg,text){return docker(['exec',pg,'psql','-U','flamedb','-d','flamedb','-At','-v','ON_ERROR_STOP=1','-c',text]);}
 async function healthy(id){for(let n=0;n<120;n++){const s=own('container',id).State;if(s.Health?.Status==='healthy')return;if(!s.Running){const r=spawnSync('docker',['logs',id],{encoding:'utf8'});fs.writeFileSync(path.join(dir,'failure-'+id.slice(0,12)+'.txt'),(r.stdout||'')+(r.stderr||''));throw Error((r.stdout||'')+(r.stderr||''));}await new Promise(r=>setTimeout(r,500));}throw Error('health timeout');}
 try {
  docker(['network','create','--label','flamedb.plans='+run,run]);
  const vol=docker(['volume','create','--label','flamedb.plans='+run,run+'-pg']);created.push(['volume',vol]);own('volume',vol);
  const pg=start('pg',['--cpus','2.5','--memory','2816m','-e','POSTGRES_USER=flamedb','-e','POSTGRES_PASSWORD=flamedb','-e','POSTGRES_DB=flamedb','--mount',`type=volume,source=${vol},target=/var/lib/postgresql/data`,'--health-cmd','pg_isready -h 127.0.0.1 -U flamedb -d flamedb','--health-interval','1s','timescale/timescaledb:latest-pg17@sha256:b346edcdb51a1fd6020e3965e0bd1c9f3406fa6d5fbce1e28f4852587ef934e2','postgres','-c','shared_buffers=256MB','-c','synchronous_commit=on']);await healthy(pg);
  start('redis',['--cpus','0.5','--memory','256m','redis:8-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0']);
  const cfg=path.join(dir,'config.yml');fs.writeFileSync(cfg,`auth:\n  keys:\n    - name: test\n      key: probe-key\n      permissions: [read, write]\nserver:\n  port: 7777\n  node_id: probe\n  metrics_address: 0.0.0.0:9090\nstorage:\n  engine: timescaledb\n  postgres:\n    dsn: postgres://flamedb:flamedb@${run}-pg:5432/flamedb?sslmode=disable\n    max_connections: 8\n  redis:\n    address: ${run}-redis:6379\ncluster:\n  replication_factor: 1\n`);
  const api=start('api',['--cpus','1','--memory','1024m','--mount',`type=bind,source=${cfg},target=/etc/probe.yml,readonly`,'--health-cmd','wget -q -O /dev/null http://127.0.0.1:9090/readyz','--health-interval','1s','flamedb:pebble-batch-fix','./app','/etc/probe.yml']);await healthy(api);
  const metadata={image:json(['image','inspect','flamedb:pebble-batch-fix'])[0].Id,run,seed:'generate_series exact unit values, five metrics, 1500 players',startedUtc:new Date().toISOString()};
  for(const count of [150000,750000]) {
   sql(pg,`TRUNCATE flame_events; INSERT INTO flame_events(metric,ts,value,tags) SELECT 'probe.'||(i/${count/5}),1704067200000000000+i,1,jsonb_build_object('player','player'||(i%1500),'region','eu') FROM generate_series(0,${count-1}) g(i);`);
   for(const stage of ['fresh','analyzed','vacuumed']) {
    if(stage==='analyzed')sql(pg,'ANALYZE flame_events');
    if(stage==='vacuumed')sql(pg,'VACUUM (ANALYZE) flame_events');
    for(const filter of ['player','player-region']) {
     const tags=filter==='player'?'\'{"player":"player0"}\'':'\'{"player":"player0","region":"eu"}\'';
     const query=`SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash('probe.0') AND metric='probe.0' AND ts>=0 AND ts<9223372036854775807 AND tags @> ${tags}::jsonb`;
     const result=sql(pg,query).split('|').map(Number);
     for(const variant of ['literal','generic','small-identity']) {
      const statement=variant==='generic'?`SET plan_cache_mode=force_generic_plan; PREPARE q(text,bigint,bigint,jsonb) AS SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts>=$2 AND ts<$3 AND tags @> $4; EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE q('probe.0',0,9223372036854775807,${tags}::jsonb)`:`EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) ${query}${variant==='small-identity'?' AND octet_length(metric)<=2000':''}`;
      const raw=sql(pg,statement),plan=JSON.parse(raw.slice(raw.indexOf('[')))[0];rows.push({count,filter,stage,variant,result,plan});
      const nodes=[];function walk(p){nodes.push([p['Node Type'],p['Index Name'],p['Actual Rows'],p['Rows Removed by Filter'],p['Shared Hit Blocks']]);for(const c of p.Plans||[])walk(c);}walk(plan.Plan);
      console.log(JSON.stringify({count,filter,stage,variant,ms:plan['Execution Time'],nodes}));
     }
    }
   }
   sql(pg,`CREATE OR REPLACE FUNCTION flame_tag_token(metric text,tag text,value text) RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT md5(jsonb_build_array(metric,tag,value)::text) $$;
CREATE OR REPLACE FUNCTION flame_event_tag_tokens(metric text,tags jsonb) RETURNS text[] LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT coalesce(array_agg(md5(jsonb_build_array(metric,t.key,t.value)::text)),ARRAY[]::text[]) FROM jsonb_each_text(tags) t $$;
CREATE INDEX IF NOT EXISTS flame_events_tag_tokens ON flame_events USING gin(flame_event_tag_tokens(metric,tags)); ANALYZE flame_events;`);
   for(const filter of ['player','player-region']) {
    const tags=filter==='player'?'\'{"player":"player0"}\'':'\'{"player":"player0","region":"eu"}\'';
    for(const variant of ['tokens','tokens-materialized']) {
     const candidates=`SELECT metric,ts,value,tags FROM flame_events WHERE ts>=0 AND ts<9223372036854775807 AND flame_event_tag_tokens(metric,tags) @> ARRAY[flame_tag_token('probe.0','player','player0')]`;
     const query=variant==='tokens'?`SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM flame_events WHERE metric='probe.0' AND ts>=0 AND ts<9223372036854775807 AND flame_event_tag_tokens(metric,tags) @> ARRAY[flame_tag_token('probe.0','player','player0')] AND tags @> ${tags}::jsonb`:`WITH candidates AS MATERIALIZED (${candidates}) SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM candidates WHERE metric='probe.0' AND tags @> ${tags}::jsonb`;
     const result=sql(pg,query).split('|').map(Number),raw=sql(pg,`EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) ${query}`),plan=JSON.parse(raw.slice(raw.indexOf('[')))[0];
     rows.push({count,filter,stage:'indexed',variant,result,plan});console.log(JSON.stringify({count,filter,stage:'indexed',variant,ms:plan['Execution Time'],plan:plan.Plan}));
    }
   }
   sql(pg,fs.readFileSync(path.join(root,'internal/analytics/event_filter.sql'),'utf8'));
   for(const filter of ['player','player-region']){
    const tags=filter==='player'?'\'{"player":"player0"}\'':'\'{"player":"player0","region":"eu"}\'';
    const query=`WITH candidates AS MATERIALIZED (SELECT metric,ts,value,tags FROM flame_events WHERE tag_tokens @> ARRAY[md5(jsonb_build_array($1::text,$5::text,$6::text)::text)]) SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM candidates WHERE metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb`;
    const args=`('probe.0',0,9223372036854775807,${tags}::jsonb,'player','player0')`;
    const result=sql(pg,`SET plan_cache_mode=force_generic_plan; PREPARE optimized(text,bigint,bigint,jsonb,text,text) AS ${query}; EXECUTE optimized${args}`).split('\n').at(-1).split('|').map(Number);
    const raw=sql(pg,`SET plan_cache_mode=force_generic_plan; PREPARE optimized(text,bigint,bigint,jsonb,text,text) AS ${query}; EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE optimized${args}`),plan=JSON.parse(raw.slice(raw.indexOf('[')))[0];
    rows.push({count,filter,stage:'optimized',variant:'generated-generic',result,plan});
    console.log(JSON.stringify({count,filter,stage:'optimized',variant:'generated-generic',ms:plan['Execution Time'],plan:plan.Plan}));
   }
  }
  verify(rows);metadata.priorServicesPreserved=prior.every(id=>json(['container','inspect',id])[0].State.Running);assert.ok(metadata.priorServicesPreserved);fs.writeFileSync(file,JSON.stringify(rows,null,2));fs.writeFileSync(path.join(dir,'environment.json'),JSON.stringify(metadata,null,2));console.log('PLAYER_SQL_PLANS_VERIFIED');
 }finally {
  for(const [type,id]of created.reverse()){own(type,id);docker(type==='container'?['rm','-f',id]:['volume','rm',id]);}
  const n=json(['network','inspect',run])[0];assert.equal(n.Labels['flamedb.plans'],run);docker(['network','rm',run]);
 }
})().catch(e=>{console.error(e);process.exitCode=1;});else throw Error('Choose run or verify');
