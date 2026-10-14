// Mandatory release checks: real observations, rather than a package's cached OK.
const {spawnSync}=require('node:child_process');
const path=require('node:path');
const fs=require('node:fs');
const {randomUUID}=require('node:crypto');
for(const key of ['FLAMEDB_TEST_POSTGRES','FLAMEDB_TEST_REDIS','FLAMEDB_REFERENCE_BINARY','FLAMEDB_NEXTGEN_BINARY']) {
  if(!process.env[key]) throw new Error(`${key} is required`);
}
const required=[
  'TestFrozenReferenceDifferentialTCP','TestSharedNodesConcurrentDurabilityAndRecovery',
  'TestFrozenLegacySDKsAgainstNextgen','TestFrozenRawEventSDKBaselineLimitation',
  'TestFrozenMissingMetricResultShapesAgainstNextgen','TestAdditiveRankingSDKsAgainstNextgen',
  'TestImportPreservesEventsAbsoluteScoresAndSource','TestCorruptSnapshotRowsReject',
  'TestPostgresSemantics','TestPostgresConcurrentAndCacheRecovery','TestPostgresNativeBoardBatch',
  'TestPostgresSharedAutomaticTimestampAllocator','TestPostgresRawHistoryLeavesScoreProjectionValid',
  'TestRedisAttemptBudgetPreservesPostgresFallback','TestListenerReadinessLifecycle','TestPostgresParallelRawBatches',
  'TestRedisRankingAtomicVersionAndRoundTrips','TestRecoveryVerificationRejectsWrongScores','TestFrameSelectsTimestampModeWithoutChangingEvents',
  'TestPostgresLargeProtocolIdentities','TestPostgresSchemaUpgradeAndBoundedRebuild','TestPostgresIdentityIndexBoundary','TestPostgresIndependentMetricBatches','TestPostgresCoalescedSmallBatches','TestRedisProjectionBoardCommandCost','TestPostgresBatchedAutomaticTimestamps','TestPostgresAutomaticBatchIndependentMetrics','TestPostgresAutomaticDirectCopy','TestRedisBatchedProjectionRoundTrips','TestPostgresIngestSnapshotAfterLock'
];
// All packages share local Docker and finite CPU/I/O. Serialize packages so
// unrelated SDK compilers cannot consume a deadline-bound backend fixture's
// budget. Assertions, race detection and production timing limits stay intact.
const result=spawnSync('go',['test','-race','-p','1','./...','-count=1','-json','-timeout=12m'],{
  cwd:path.resolve(__dirname,'../..'),env:{...process.env,FLAMEDB_INTEGRATION:'1',FLAMEDB_TEST_TIMESCALE:'1'},
  encoding:'utf8',maxBuffer:32*1024*1024,timeout:780000
});
if(result.error) throw result.error;
// Preserve complete diagnostics; gate transcripts intentionally retain only
// bounded failure excerpts and must not hide the failing assertion.
const artifactDir=path.resolve(__dirname,'../../.compat');
fs.mkdirSync(artifactDir,{recursive:true});
fs.writeFileSync(path.join(artifactDir,'release-current.jsonl'),result.stdout||'');
fs.writeFileSync(path.join(artifactDir,'release-current.stderr.txt'),result.stderr||'');
const runId=randomUUID();
fs.writeFileSync(path.join(artifactDir,`release-${runId}.jsonl`),result.stdout||'');
fs.writeFileSync(path.join(artifactDir,`release-${runId}.stderr.txt`),result.stderr||'');
// Show deciding assertions before verbose PASS lines so a bounded gate excerpt
// cannot conceal the failure even when multiple release runs follow each other.
const events=(result.stdout||'').split('\n').flatMap(line=>{try{return [JSON.parse(line)];}catch{return [];}});
if(result.status!==0){
 console.error(`Release failed: status=${result.status}; complete diagnostics release-${runId}.jsonl`);
 for(const e of events.filter(e=>e.OutputType==='error'||e.Action==='build-fail')) console.error(e.Output||JSON.stringify(e));
}
const passed=new Set();
for(const line of (result.stdout||'').split('\n')) {
  let e; try {e=JSON.parse(line);}catch{continue;}
  if(e.Action==='pass'&&e.Test) {passed.add(e.Test);console.log(`PASS ${e.Package} ${e.Test}`);}
  if(e.Action==='fail'||e.Action==='skip'||(e.Output&&e.Output.includes('WARNING: DATA RACE'))) console.log(line);
}
process.stderr.write(result.stderr||'');
if(result.status!==0) {process.stdout.write(result.stdout||'');process.exit(result.status||1);}
const missing=required.filter(name=>!passed.has(name));
if(missing.length) throw new Error(`Mandatory release observations absent: ${missing.join(', ')}`);
const allocations=spawnSync('go',['test','./internal/storage','-run','^TestWindowedAllocations$','-count=1','-json'],{cwd:path.resolve(__dirname,'../..'),encoding:'utf8',timeout:120000});
if(allocations.error) throw allocations.error;
process.stdout.write(allocations.stdout||'');process.stderr.write(allocations.stderr||'');
const allocationEvents=(allocations.stdout||'').split('\n').flatMap(line=>{try{return [JSON.parse(line)];}catch{return [];}});
if(allocations.status!==0 || allocationEvents.some(e=>e.Action==='skip') || !allocationEvents.some(e=>e.Action==='pass'&&e.Test==='TestWindowedAllocations')) throw new Error('Mandatory production allocation measurement absent or failed.');
const vet=spawnSync('go',['vet','./...'],{cwd:path.resolve(__dirname,'../..'),encoding:'utf8',timeout:120000});
process.stdout.write(vet.stdout||'');process.stderr.write(vet.stderr||'');
if(vet.error) throw vet.error;
if(vet.status!==0) process.exit(vet.status||1);
console.log('REAL_RELEASE_RACE_AND_VET_VERIFIED');
