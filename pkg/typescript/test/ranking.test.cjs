const { test } = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { once } = require('node:events');
const { FlameDB } = require('../dist');

async function fixture(t, respond, timings = []) {
  const sockets = new Set(); const commands = [];
  const server = net.createServer(socket => {
    sockets.add(socket); socket.on('close', () => sockets.delete(socket)); socket.on('error', () => {});
    socket.setEncoding('utf8'); socket.write('{"auth":"required"}\n'); let buffer = '';
    socket.on('data', chunk => {
      buffer += chunk; let end;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0,end); buffer = buffer.slice(end+1);
        if (line.startsWith('AUTH ')) socket.write('{"auth":"ok"}\n');
        else { commands.push(line); socket.write(JSON.stringify(respond(line))+'\n'); }
      }
    });
  });
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  const db = new FlameDB({host:'127.0.0.1', port:server.address().port, apiKey:'test', onCommandTiming:t => timings.push(t)});
  t.after(() => {db.disconnect(); for (const socket of sockets) socket.destroy(); server.close();});
  return {db,commands};
}

test('ranking wire options, null versus zero, around global ranks and read pools', async t => {
  const timings = [];
  const {db,commands} = await fixture(t,line => ({ranking: line.includes('"missing"')
    ? {rank:null,score:null,count:3,around:[]}
    : {rank:2,score:0,count:3,around:[{entity_id:'a',value:10,rank:1},{entity_id:'🌍 "quoted"',value:0,rank:2},{entity_id:'z',value:-4,rank:3}]}}),timings);
  const options = {board:'reason',limit:7,offset:2};
  assert.equal(await db.getRank('kills','🌍 "quoted"',options),2);
  assert.equal(await db.getScore('kills','zero'),0);
  assert.equal(await db.getRank('kills','missing'),null);
  assert.equal(await db.getScore('kills','missing'),null);
  const rows = await db.getAroundEntity('kills','zero',1,{from:new Date('2026-10-10T01:02:03.456+01:00'),to:new Date('2026-10-10T02:02:03.456+01:00'),entityTag:'player:id'});
  assert.deepEqual(rows,[{entity_id:'a',score:10,rank:1},{entity_id:'🌍 "quoted"',score:0,rank:2},{entity_id:'z',score:-4,rank:3}]);
  assert.equal(await db.getLeaderboardCount('kills',options),3);
  assert.deepEqual(await db.getAroundEntity('kills','missing',0),[]);
  assert.deepEqual(commands,[
    'RANK kills "🌍 \\"quoted\\"" BOARD reason LIMIT 7 OFFSET 2',
    'SCORE kills "zero"', 'RANK kills "missing"', 'SCORE kills "missing"',
    'AROUND kills "zero" 1 FROM 2026-10-10T00:02:03.456Z TO 2026-10-10T01:02:03.456Z ENTITY player:id',
    'LEADERBOARD_COUNT kills BOARD reason LIMIT 7 OFFSET 2','AROUND kills "missing" 0']);
  assert.ok(timings.every(t => t.read && t.success));
});

test('ranking rejects old-server, malformed, incomplete and unsafe responses', async t => {
  let response = {};
  const {db} = await fixture(t,() => response);
  for (const r of [undefined,null,{}, {rank:null,score:null,count:null},{rank:0,score:0,count:1},{rank:2,score:0,count:1},{rank:null,score:0,count:1},{rank:null,score:null,count:-1},{rank:null,score:null,count:9007199254740992}]) {
    response = {ranking:r}; await assert.rejects(db.getRank('m','a'),/ranking result/);
  }
  response = {ranking:{rank:1,score:1,count:1}};
  await assert.rejects(db.getAroundEntity('m','a',1),/around/);
  response.ranking.around = [{entity_id:'a',value:1,rank:0}];
  await assert.rejects(db.getAroundEntity('m','a',1),/around/);
  response = {error:'permission denied'};
  await assert.rejects(db.getScore('m','a'),/permission denied/);
});

test('ranking validates options before sending a command', async t => {
  const {db,commands} = await fixture(t,() => ({}));
  for (const options of [{from:new Date()},{board:'reason',from:new Date(),entityTag:'p'},{board:'bad:board'},{limit:-1},{offset:0.5},{to:new Date(NaN)}])
    await assert.rejects(db.getRank('m','a',options));
  await assert.rejects(db.getRank('m 1','a')); await assert.rejects(db.getScore('m',''));
  await assert.rejects(db.getAroundEntity('m','a',-1)); await assert.rejects(db.getAroundEntity('m','a',0.5));
  assert.deepEqual(commands,[]);
});
