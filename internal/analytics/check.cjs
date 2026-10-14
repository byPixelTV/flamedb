// Mandatory backend integration gate. Unlike ordinary unit tests, this fails
// when services or required observations are absent and rejects skipped tests.
const {spawnSync}=require('node:child_process');
const path=require('node:path');
for(const name of ['FLAMEDB_TEST_POSTGRES','FLAMEDB_TEST_REDIS']){
 if(!process.env[name]){console.error(`${name} is required`);process.exit(1);}
}
const required=['TestExactRankingEncoding','TestPostgresSemantics','TestPostgresBatchReplicaAndRejection','TestPostgresConcurrentAndCacheRecovery','TestPostgresLargeNativeAnalytics','TestPostgresLosslessIDsAndAtomicOverflow','TestRedisOutageFallbackAndIncompleteRebuild','TestPostgresNativeBoardBatch','TestPostgresResultByteBudget','TestRedisDelayedProjectionCannotOverwriteNewerState','TestRedisSocketDeadline','TestSynchronousQuorumValidation','TestPostgresSingleReplicaQuorum','TestPostgresSharedAutomaticTimestampAllocator','TestPostgresRawHistoryLeavesScoreProjectionValid','TestRedisAttemptBudgetPreservesPostgresFallback','TestPostgresParallelRawBatches','TestRedisRankingAtomicVersionAndRoundTrips','TestPostgresLargeProtocolIdentities','TestPostgresSchemaUpgradeAndBoundedRebuild','TestPostgresIdentityIndexBoundary','TestPostgresIndependentMetricBatches','TestPostgresCoalescedSmallBatches','TestRedisProjectionBoardCommandCost','TestPostgresBatchedAutomaticTimestamps','TestPostgresAutomaticBatchIndependentMetrics','TestPostgresAutomaticDirectCopy','TestRedisBatchedProjectionRoundTrips','TestPostgresIngestSnapshotAfterLock'];
const result=spawnSync(process.env.FLAMEDB_GO||'go',['test','./internal/analytics','-count=1','-json'],{cwd:path.resolve(__dirname,'../..'),env:{...process.env,FLAMEDB_INTEGRATION:'1'},encoding:'utf8',maxBuffer:16*1024*1024});
if(result.error){console.error(result.error);process.exit(1);}
process.stdout.write(result.stdout||'');process.stderr.write(result.stderr||'');
const passed=new Set();let skipped=false;
for(const line of (result.stdout||'').split('\n')){try{const event=JSON.parse(line);if(event.Action==='pass'&&event.Test)passed.add(event.Test);if(event.Action==='skip')skipped=true;}catch{}}
const missing=required.filter(name=>!passed.has(name));
if(result.status!==0||skipped||missing.length){console.error(`Backend gate failed: status=${result.status}, skipped=${skipped}, missing=${missing.join(',')}`);process.exit(1);}
console.log(`Backend gate passed: ${required.length} required tests; Redis mode ${process.env.FLAMEDB_TEST_REDIS_MODE||'standalone'}`);
