const assert = require('node:assert/strict');
const { FlameDB } = require(process.argv[2]);
async function main() {
  const address = new URL(`tcp://${process.argv[3]}`);
  const metric = process.argv[4]+'.ts';
  const db = new FlameDB({host:address.hostname,port:Number(address.port),apiKey:'compat-key',timeout:10000});
  try {
    for (const entity of ['b','a','zero']) await db.write(metric,entity === 'zero' ? 0 : 5,{leaderboardEntity:entity,leaderboardEntities:{team:entity},tags:{player:entity},timestampNs:1704067200000000001n+BigInt(entity.charCodeAt(0))});
    assert.equal(await db.getRank(metric,'b'),2); assert.equal(await db.getScore(metric,'zero'),0);
    assert.equal(await db.getRank(metric,'missing'),null); assert.equal(await db.getScore(metric,'missing'),null);
    assert.deepEqual(await db.getAroundEntity(metric,'b',1),[{entity_id:'a',score:5,rank:1},{entity_id:'b',score:5,rank:2},{entity_id:'zero',score:0,rank:3}]);
    assert.equal(await db.getLeaderboardCount(metric,{board:'team',limit:1,offset:2}),3);
    const window = {from:new Date('2024-01-01'),to:new Date('2024-01-02'),entityTag:'player'};
    assert.equal(await db.getRank(metric,'b',window),2); assert.equal(await db.getLeaderboardCount(metric,window),3);
    assert.deepEqual(await db.getAroundEntity(metric,'missing',0,window),[]);
    console.log('RANKING_TYPESCRIPT_SDK_OK');
  } finally { db.disconnect(); }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
