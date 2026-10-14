// The first argument is JS compiled from the frozen baseline index.ts.
const assert = require('node:assert/strict');
const { FlameDB } = require(process.argv[2]);
async function main() {
  const address = new URL(`tcp://${process.argv[3]}`);
  const prefix = process.argv[4]; const metric = prefix+'.ts';
  const db = new FlameDB({host:address.hostname,port:Number(address.port),apiKey:'compat-key',timeout:10000});
  try {
    await db.connect();
    await db.write(metric,5,{leaderboardEntity:'alice',leaderboardEntities:{reason:'fall'},tags:{suite:prefix,player:'alice'},timestampNs:1704067200000000001n});
    if (process.argv[5] === 'raw-only') {
      const raw = await db.get(metric);
      assert.equal(raw.events[0].timestampNs,1704067200000000001n);
      console.log('FROZEN_RAW_TYPESCRIPT_SDK_OK'); return;
    }
    const br = await db.writeBatch([{metric,value:3,options:{leaderboardEntity:'bob',tags:{suite:prefix,player:'bob'},timestampNs:1704070800000000002n}},{metric,value:-2,options:{leaderboardEntity:'alice',tags:{suite:prefix,player:'alice'},timestampNs:1704153600000000003n}}]);
    assert.equal(br.accepted,2); assert.equal(br.failed,0); assert.equal(br.ok,true);
    if (process.argv[5] !== 'skip-raw') {
      const get = await db.get(metric,{where:{player:'alice'},order:'ASC',limit:10});
      assert.equal(get.events.length,2); assert.equal(get.events[0].timestampNs,1704067200000000001n); assert.equal(get.events[1].value,-2);
      const multi = await db.get([metric,metric+'.absent']); assert.equal(multi.metrics[metric].length,3);
    }
    assert.deepEqual(await db.leaderboard(metric),[{entity_id:'alice',score:3},{entity_id:'bob',score:3}]);
    assert.deepEqual(await db.leaderboard(metric,{board:'reason'}),[{entity_id:'fall',score:5}]);
    assert.deepEqual(await db.groupLeaderboard(metric,[{name:'team',members:['alice','bob']}]),[{group:'team',score:6}]);
    await db.set(metric,0,'alice'); assert.deepEqual(await db.leaderboard(metric,{offset:1}),[{entity_id:'alice',score:0}]);
    const stats = await db.stats(metric,['player']); assert.equal(stats.metric,metric); assert.equal(stats.tag_stats.length,1);
    assert.deepEqual(await db.listMetrics({suite:prefix}),[metric]);
    assert.deepEqual(await db.delete(metric,{where:{player:'alice'},leaderboardEntity:'alice',dryRun:true}),{dry_run:true,events:2,leaderboard_entries:1});
    await db.delete(metric,{leaderboardEntity:'alice'}); assert.deepEqual(await db.leaderboard(metric),[{entity_id:'bob',score:3}]);
    const report = await db.deleteByTags({suite:prefix}); assert.equal(report.events,3); assert.equal(report.metrics.length,1);
    assert.equal((await db.get(metric)).events?.length ?? 0,0);
    assert.deepEqual(await db.leaderboard(metric),[{entity_id:'bob',score:3}]);
    await db.delete(metric,{leaderboardEntity:'bob'});
    console.log('FROZEN_TYPESCRIPT_SDK_OK');
  } finally { db.disconnect(); }
}
main().catch(error => {console.error(error);process.exitCode=1;});
