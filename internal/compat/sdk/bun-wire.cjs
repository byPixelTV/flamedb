// Host the fixture independently on Node. Every SDK operation below executes
// in a separate Bun process against the real TCP fixture.
const assert = require('node:assert/strict');
const net = require('node:net');
const { spawn } = require('node:child_process');
const { once } = require('node:events');

async function consumer(addr) {
  assert.ok(process.versions.bun, 'consumer must execute in Bun');
  const { FlameDB } = require('../../../pkg/typescript/dist');
  const timings = [];
  const db = new FlameDB({host:'127.0.0.1',port:Number(addr),apiKey:'test',timeout:500,
    readConnections:1,writeConnections:1,onCommandTiming:t=>timings.push(t)});
  try {
    assert.equal(await db.getRank('kills','zero'),2);
    assert.equal(await db.getScore('kills','zero'),0);
    assert.equal(await db.getRank('kills','missing'),null);
    assert.equal(await db.getScore('kills','missing'),null);
    assert.equal(await db.getLeaderboardCount('kills'),3);
    assert.deepEqual(await db.getAroundEntity('kills','zero',1),[
      {entity_id:'a',score:10,rank:1},{entity_id:'zero',score:0,rank:2},{entity_id:'z',score:-4,rank:3}]);
    const events = await db.get(['precision']);
    assert.equal(events.events[0].timestampNs,1704067200000000001n);
    // A pending write uses the write pool; ranking must complete through reads.
    const pending = db.write('slow',1);
    const rejected = assert.rejects(pending,/timeout|deadline/);
    await new Promise(resolve=>setTimeout(resolve,30));
    assert.equal(await db.getRank('kills','zero'),2);
    await rejected;
    assert.equal(await db.getRank('kills','zero'),2);
    assert.ok(timings.filter(t=>['RANK','SCORE','AROUND','LEADERBOARD_COUNT'].includes(t.command)).every(t=>t.read && t.success));
  } finally { db.disconnect(); }
  console.log('BUN_WIRE_RUNTIME_VERIFIED');
}

async function fixture() {
  const sockets = new Set(); const commands = [];
  const server = net.createServer(socket=>{
    sockets.add(socket); socket.on('close',()=>sockets.delete(socket)); socket.on('error',()=>{});
    socket.setEncoding('utf8'); socket.write('{"auth":"required"}\n'); let buffer='';
    socket.on('data',chunk=>{
      buffer+=chunk; let end;
      while ((end=buffer.indexOf('\n'))>=0) {
        const line=buffer.slice(0,end); buffer=buffer.slice(end+1);
        if (line.startsWith('AUTH ')) {socket.write('{"auth":"ok"}\n');continue;}
        commands.push(line);
        if (line==='WRITE slow 1') continue;
        if (line.startsWith('GET ')) {
          socket.write('{"events":[{"timestamp":1704067200000000001,"metric":"precision","value":1,"tags":{}}]}\n');
          continue;
        }
        socket.write(JSON.stringify({ranking:line.includes('"missing"')
          ? {rank:null,score:null,count:3,around:[]}
          : {rank:2,score:0,count:3,around:[{entity_id:'a',value:10,rank:1},{entity_id:'zero',value:0,rank:2},{entity_id:'z',value:-4,rank:3}]}})+'\n');
      }
    });
  });
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  let output=''; let child;
  try {
    child=spawn('bun',[__filename,'consumer',String(server.address().port)],{stdio:['ignore','pipe','pipe']});
    child.stdout.on('data',chunk=>{output+=chunk;process.stdout.write(chunk);});
    child.stderr.on('data',chunk=>{output+=chunk;process.stderr.write(chunk);});
    const timer=setTimeout(()=>child.kill(),15000);
    let status;
    try { [status]=await once(child,'close'); } finally {clearTimeout(timer);}
    assert.equal(status,0,'Bun consumer failed');
    assert.match(output,/BUN_WIRE_RUNTIME_VERIFIED/);
    assert.equal(commands.filter(line=>line==='WRITE slow 1').length,1,'ambiguous write must never replay');
  } finally { if (child && child.exitCode===null) child.kill();for (const socket of sockets) socket.destroy();server.close(); }
}

(process.argv[2]==='consumer'?consumer(process.argv[3]):fixture()).catch(error=>{console.error(error);process.exitCode=1;});
