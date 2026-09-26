const { test } = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { once } = require('node:events');
const { FlameDB } = require('../dist');
async function fixture(t, respond, timeout=1000) {
 const sockets=new Set();
 const server=net.createServer(socket=>{
  sockets.add(socket);socket.on('close',()=>sockets.delete(socket));socket.setEncoding('utf8');socket.write('{"auth":"required"}\n');let buffer='';
  socket.on('data',chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(line.startsWith('AUTH '))socket.write('{"auth":"ok"}\n');else{const out=respond(line);if(out)socket.write(JSON.stringify(out)+'\n');}}});
 });
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const db=new FlameDB({host:'127.0.0.1',port:server.address().port,apiKey:'test',timeout});
 t.after(()=>{db.disconnect();for(const s of sockets)s.destroy();server.close();});return db;
}
test('concurrent lazy handshake, Unicode and response mappings',async t=>{
 const db=await fixture(t,line=>line.startsWith('STATS')?{stats:{metric:'m',tag_stats:[{tag_key:'p',cardinality:2}]}}:{leaderboard:[{entity_id:'Hello 🌍🔥',value:42}]});
 const results=await Promise.all(Array.from({length:20},()=>db.leaderboard('m')));
 for(const rows of results)assert.deepEqual(rows,[{entity_id:'Hello 🌍🔥',score:42}]);
 assert.equal((await db.stats('m',['p'])).tag_stats[0].cardinality,2);
 assert.deepEqual(await db.groupLeaderboard('m',[{name:'g',members:['p']}]),[{group:'Hello 🌍🔥',score:42}]);
});
test('timeout rejects whole pipeline and prevents response reuse',async t=>{
 const db=await fixture(t,()=>null,30);
 const results=await Promise.allSettled([db.get('m'),db.get('n')]);assert.ok(results.every(r=>r.status==='rejected'));
 await assert.rejects(db.get('m'));
});
test('quote values and reject metric injection',async t=>{
 let received='';const db=await fixture(t,line=>{received=line;return {};});
 await db.write('m',1,{tags:{p:'a"\nb'}});assert.ok(received.includes('p="a\\"\\nb"'));
 await assert.rejects(db.write('m 1 lb=',1));
});

test('preserves nanosecond precision in exact bigint companion fields',async t=>{
 const server=net.createServer(socket=>{socket.setEncoding('utf8');socket.write('{"auth":"required"}\n');socket.on('data',data=>{if(data.startsWith('AUTH'))socket.write('{"auth":"ok"}\n');else socket.write('{"events":[{"timestamp":1790000000123456789,"metric":"m","value":1}],"series":[{"ts":1790000000123456789,"count":1,"value":1}]}\n');});});
 server.listen(0,'127.0.0.1');await once(server,'listening');const db=new FlameDB({host:'127.0.0.1',port:server.address().port,apiKey:'test'});t.after(()=>{db.disconnect();server.close();});
 const result=await db.get('m');assert.equal(result.events[0].timestampNs,1790000000123456789n);assert.equal(result.series[0].tsNs,1790000000123456789n);
});

test('accepts namespaced metrics and tag keys', async t => {
 let received = '';
 const db = await fixture(t, line => { received = line; return {}; });
 await db.write('smp:', 1, {tags: {'player:id': 'p:1'}});
 assert.ok(received.startsWith('WRITE smp: 1'));
 assert.ok(received.includes('player:id="p:1"'));
});

for (const mode of ['disconnect', 'timeout']) {
 test(`reconnects after ${mode} without replaying writes`, async t => {
  const sockets = new Set(); const commands = []; let connections = 0;
  const server = net.createServer(socket => {
   const generation = ++connections;
   sockets.add(socket); socket.on('close', () => sockets.delete(socket));
   socket.on('error', () => {}); socket.setEncoding('utf8');
   socket.write('{"auth":"required"}\n'); let buffer = '';
   socket.on('data', chunk => {
    buffer += chunk; let end;
    while ((end = buffer.indexOf('\n')) >= 0) {
     const line = buffer.slice(0,end); buffer = buffer.slice(end+1);
     if (line.startsWith('AUTH ')) { socket.write('{"auth":"ok"}\n'); continue; }
     commands.push(line);
     if (generation === 1) { if (mode === 'disconnect') socket.destroy(); }
     else socket.write('{}\n');
    }
   });
  });
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  const db = new FlameDB({host:'127.0.0.1',port:server.address().port,apiKey:'test',timeout:150});
  t.after(() => { db.disconnect(); for (const socket of sockets) socket.destroy(); server.close(); });
  const failed = await Promise.allSettled([db.write('first',1), db.write('second',1)]);
  assert.ok(failed.every(result => result.status === 'rejected'));
  await Promise.all(Array.from({length:8}, (_,i) => db.write(`next${i}`,1)));
  assert.equal(connections,2);
  assert.equal(commands.filter(line => line.startsWith('WRITE first ')).length,1);
  assert.ok(commands.filter(line => line.startsWith('WRITE second ')).length <= 1);
  for (let i=0;i<8;i++) assert.equal(commands.filter(line => line.startsWith(`WRITE next${i} `)).length,1);
  db.disconnect(); await db.connect(); await db.write('afterDisconnect',1);
  assert.equal(connections,3);
 });
}

test('lists all metrics and filters by UUID', async t => {
 const db=await fixture(t,line=>{
  if(line==='METRICS')return {metric_keys:[]};
  assert.equal(line,'METRICS WHERE uuid="player-id"');
  return {metric_keys:['smp:kills']};
 });
 assert.deepEqual(await db.listMetrics(),[]);
 assert.deepEqual(await db.listMetrics({uuid:'player-id'}),['smp:kills']);
});

test('deletes by arbitrary tags and rejects empty filters', async t => {
 let received;
 const db=await fixture(t,line=>{received=line;return {delete:{dry_run:false,events:2,leaderboard_entries:1}};});
 await db.delete('kills',{where:{player:'a"b'},leaderboardEntity:'a'});
 assert.equal(received,`DELETE kills WHERE player=${JSON.stringify('a"b')} lb="a"`);
 await assert.rejects(db.delete('kills',{where:{}}));
 await assert.rejects(db.delete('kills',{where:{'bad key':'a'}}));
});

test('deleteByTags previews and reports partial failures', async t => {
 const db=await fixture(t,line=>{
  if(line==='METRICS')return {metric_keys:['a','b','c']};
  assert.ok(line.startsWith('PREVIEW_DELETE '));
  assert.ok(line.includes('WHERE player="id"'));
  if(line.startsWith('PREVIEW_DELETE b '))return {error:'unavailable'};
  return {delete:{dry_run:true,events:3,leaderboard_entries:1}};
 });
 const report=await db.deleteByTags({player:'id'},{dryRun:true,leaderboardEntity:'id'});
 assert.equal(report.events,6);assert.equal(report.leaderboard_entries,2);
 assert.equal(report.metrics.length,3);assert.ok(report.metrics[1].error);
 await assert.rejects(db.deleteByTags({}));
});
