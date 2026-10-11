const { test } = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { once } = require('node:events');
const { FlameDB } = require('../dist');

function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}

async function fixture(t, respond, config = {}) {
  const sockets = new Set(), commands = [], timers = new Set();
  let connections = 0;
  const later = (fn, ms) => {
    const timer = setTimeout(() => { timers.delete(timer); fn(); }, ms);
    timers.add(timer);
  };
  const server = net.createServer(socket => {
    connections++;
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    socket.setEncoding('utf8');
    socket.write('{"auth":"required"}\n');
    let buffer = '', batch;
    socket.on('data', chunk => {
      buffer += chunk;
      let end;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        if (line.startsWith('AUTH ')) { socket.write('{"auth":"ok"}\n'); continue; }
        if (line === 'WRITE_BATCH') { batch = [line]; continue; }
        if (batch) { batch.push(line); if (line !== 'END') continue; }
        const frame = batch ? batch.join('|') : line;
        batch = undefined;
        commands.push(frame);
        respond(socket, frame, later);
      }
    });
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const db = new FlameDB({host:'127.0.0.1', port:server.address().port, apiKey:'test', timeout:2000, ...config});
  t.after(() => {
    db.disconnect();
    for (const timer of timers) clearTimeout(timer);
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  return { db, commands, sockets, connections: () => connections };
}

test('reserved reads bypass a blocked batch write and retain response ownership', async t => {
  const started = deferred(), timings = [];
  let blockedSocket;
  const f = await fixture(t, (socket, line) => {
    if (line.startsWith('WRITE_BATCH')) { blockedSocket = socket; started.resolve(); return; }
    socket.write(JSON.stringify({leaderboard:[{entity_id:line.split(' ')[1],value:42}]})+'\n');
  }, {onCommandTiming: timing => timings.push(timing)});
  let completed = false;
  const write = f.db.writeBatch([{metric:'blocked',value:1}]).then(v => {completed=true; return v;});
  await started.promise;
  const results = await Promise.all(Array.from({length:40}, (_,i) => f.db.leaderboard(`clan:${i}`)));
  results.forEach((rows,i) => assert.deepEqual(rows,[{entity_id:`clan:${i}`,score:42}]));
  assert.equal(completed,false);
  assert.equal(f.connections(),3);
  assert.equal(timings.length,40);
  assert.ok(timings.every(t => t.read && t.success && t.queueWaitMs>=0 && t.requestMs>=0));
  blockedSocket.write('{"ok":true,"accepted":1,"failed":0}\n');
  assert.equal((await write).accepted,1);
});

test('total deadline includes time waiting for pipeline capacity', async t => {
  const firstStarted = deferred(), timings = [];
  const f = await fixture(t,(socket,line,later) => {
    if (line.includes('first')) { firstStarted.resolve(); later(() => socket.write('{}\n'),100); }
  },{pipelineSize:1,timeout:300,onCommandTiming:t => timings.push(t)});
  const first = f.db.write('first',1);
  await firstStarted.promise;
  await assert.rejects(f.db.write('second',1),/timeout|deadline/);
  await first;
  const failed = timings.find(t => !t.success);
  assert.ok(failed.queueWaitMs>=50,JSON.stringify(failed));
  assert.ok(failed.queueWaitMs+failed.requestMs<380,JSON.stringify(failed));
  assert.ok(failed.requestMs<280,JSON.stringify(failed));
});

test('disconnect wakes queued calls without sending them and permits a fresh pool', async t => {
  const started=deferred(), timings=[];
  const f=await fixture(t,(socket,line) => {
    if (line.includes('blocked')) started.resolve();
    else socket.write('{}\n');
  },{pipelineSize:1,onCommandTiming:t => timings.push(t)});
  const active=f.db.write('blocked',1);
  const activeRejected=assert.rejects(active,/closed/);
  await started.promise;
  const queued=f.db.write('queued',1);
  const queuedRejected=assert.rejects(queued,/closed/);
  f.db.disconnect();
  // Reconnect before the old pool's rejected calls release their leases.
  await f.db.write('next',1);
  await Promise.all([activeRejected,queuedRejected]);
  assert.equal(f.connections(),2);
  assert.deepEqual(f.commands.map(line=>line.split(' ')[1]),['blocked','next']);
  assert.ok(timings.some(t=>!t.success && t.requestMs===0));
});

test('a late timed-out response cannot satisfy the next request; observers cannot fail commands', async t => {
  const f=await fixture(t,(socket,line,later) => {
    if (line.includes('slow')) later(()=>socket.write('{"leaderboard":[{"entity_id":"wrong","value":99}]}\n'),150);
    else socket.write('{"leaderboard":[{"entity_id":"next","value":42}]}\n');
  },{readConnections:1,timeout:60,onCommandTiming:()=>{throw new Error('observer');}});
  await assert.rejects(f.db.leaderboard('slow'),/timeout|deadline/);
  assert.deepEqual(await f.db.leaderboard('next'),[{entity_id:'next',score:42}]);
  assert.equal(f.connections(),2);
  assert.equal(f.commands.filter(line=>line.includes('slow')).length,1);
});

test('per-connection pipelining remains concurrent rather than serializing round trips', async t => {
  const seen=deferred(); let waiting=[];
  const f=await fixture(t,(socket,line) => {
    waiting.push([socket,line]);
    if(waiting.length===8) {
      // The fixture only replies after all eight commands arrived. A mutex
      // around each round trip would never reach this point before timeout.
      for(const [s,l] of waiting) s.write(JSON.stringify({leaderboard:[{entity_id:l.split(' ')[1],value:1}]})+'\n');
      seen.resolve();
    }
  },{readConnections:1,pipelineSize:8,timeout:1000});
  const results=await Promise.all(Array.from({length:8},(_,i)=>f.db.leaderboard(`m${i}`)));
  await seen.promise;
  results.forEach((rows,i)=>assert.equal(rows[0].entity_id,`m${i}`));
  assert.equal(f.connections(),1);
});

test('lazy greeting and auth share one request deadline and never send after expiry', async t => {
  const sockets=new Set(), timers=new Set(), commands=[];
  const later=(fn,ms)=>{const timer=setTimeout(()=>{timers.delete(timer);fn();},ms);timers.add(timer);};
  const server=net.createServer(socket=>{
    sockets.add(socket);socket.on('error',()=>{});socket.on('close',()=>sockets.delete(socket));
    socket.setEncoding('utf8');
    later(()=>socket.write('{"auth":"required"}\n'),100);
    socket.on('data',line=>{
      if(line.startsWith('AUTH '))later(()=>socket.write('{"auth":"ok"}\n'),250);
      else commands.push(line);
    });
  });
  server.listen(0,'127.0.0.1');await once(server,'listening');
  const timings=[];
  const db=new FlameDB({host:'127.0.0.1',port:server.address().port,apiKey:'test',timeout:300,onCommandTiming:t=>timings.push(t)});
  t.after(()=>{db.disconnect();for(const timer of timers)clearTimeout(timer);for(const s of sockets)s.destroy();server.close();});
  await assert.rejects(db.leaderboard('neverSent'),/timeout|deadline/);
  assert.equal(commands.length,0);
  assert.equal(timings.length,1);
  assert.equal(timings[0].success,false);
  assert.ok(timings[0].queueWaitMs+timings[0].requestMs<340,JSON.stringify(timings[0]));
});
