const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),action=process.argv[2];
const app=process.env.FLAMEDB_ERAMC_SOURCE;
if(!app)throw new Error('FLAMEDB_ERAMC_SOURCE must name the EraMC checkout');
const relative='platform/bukkit/Moonlight';
const adapter=path.join(app,relative,'src/main/kotlin/net/eramc/moonlight/metrics/MoonlightFlameDB.kt');
const artifacts=path.join(root,'.compat/sdk-batching-adoption');fs.mkdirSync(artifacts,{recursive:true});
function owner(source){
 assert.match(source,/FlameDB\.buffered\(/);
 for(const name of ['record','recordThen','getSum','leaderboard','groupLeaderboard','shutdown','statistics'])assert.match(source,new RegExp('client\\.'+name+'\\('));
 assert.doesNotMatch(source,/class\s+(?:MetricJournal|JournalMetric)|fun\s+sendLoop|CoroutineScope\(|FileOutputStream|\.writeBatch\(/);
 for(const field of ['batchSize','flushMs','retryMs','confirmationMs','maxConfirmations','shutdownMs','maxJournalBytes'])assert.match(source,new RegExp(field+'\\s*=\\s*'+field));
}
function gradle(cwd,args,log){
 const result=process.platform==='win32'
  ?spawnSync('cmd.exe',['/d','/c','gradlew.bat',...args,'--console=plain'],{cwd,encoding:'utf8',timeout:900000,maxBuffer:16*1024*1024})
  :spawnSync('./gradlew',[...args,'--console=plain'],{cwd,encoding:'utf8',timeout:900000,maxBuffer:16*1024*1024});
 const out=(result.stdout||'')+'\n'+(result.stderr||'');fs.writeFileSync(path.join(artifacts,log),out);
 if(result.error||result.status!==0){process.stderr.write(out.slice(-24000));throw result.error||new Error(`Gradle failed: ${result.status}; ${log}`);}
 assert.match(out,/BUILD SUCCESSFUL/);return out;
}
if(action==='ownership'){
 const source=fs.readFileSync(adapter,'utf8');owner(source);
 const old=fs.readFileSync(path.join(artifacts,'moonlight-before.kt'),'utf8');assert.throws(()=>owner(old));
 assert.throws(()=>owner(source.replace('FlameDB.buffered(','FlameDB.connect(')));
 assert.throws(()=>owner(source+'\nprivate fun sendLoop() {}'));
 const factory=fs.readFileSync(path.join(root,'pkg/kotlin/src/main/kotlin/dev/bypixel/flamedb/FlameDB.kt'),'utf8');assert.match(factory,/fun buffered\(/);
 console.log('SDK_BATCHING_OWNERSHIP_VERIFIED');
}else if(action==='sdk'){
 const sdk=path.join(root,'pkg/kotlin');
 const out=gradle(sdk,['test','sdkStressTest','jar','--rerun-tasks'],'sdk-gradle.txt');
 const xml=fs.readFileSync(path.join(sdk,'build/test-results/test/TEST-dev.bypixel.flamedb.BufferedClientTest.xml'),'utf8');
 for(const field of ['failures','errors','skipped'])assert.match(xml,new RegExp(field+'="0"'));
 const required=['minimalFactoryPreservesSnapshotsAndDirectWriteSemantics','unavailableConnectionAuthAndRestart','lostAckIsNeverReplayed','truncatedAckIsNeverReplayed','oversizedAckIsNeverReplayed','timeoutIsNeverReplayed','partialAckIsNeverReplayed','invalidCountersAreNeverReplayed','shutdownAndTruncatedJournalRecoverSafely','confirmationCapacityReleasesAfterDeadline','runningClientRecoversAfterServerRestart','diskBudgetRejectsEarlyAndReleasesAfterAcknowledgement','partialJournalFlushDoesNotWaitForBlockedSocket','exclusiveDirectoryAndCancelledShutdownReleaseOwnership'];
 for(const name of required)assert.match(xml,new RegExp('<testcase[^>]*name="'+name));
 const stress=fs.readFileSync(path.join(sdk,'build/test-results/sdkStressTest/TEST-dev.bypixel.flamedb.BufferedClientStressTest.xml'),'utf8');
 assert.match(stress,/name="sdkManagedBurstPreservesAllEventsWithBoundedHeapAndOneWriter/);for(const field of ['failures','errors','skipped'])assert.match(stress,new RegExp(field+'="0"'));
 assert.match(out,/BUFFERED_SDK_STRESS_VERIFIED events=100000 producers=64/);
 console.log(`SDK buffered cases checked: ${required.length}; actual 100000-event stress passed`);
 console.log('SDK_BUFFERED_RUNTIME_VERIFIED');
}else if(action==='application'){
 owner(fs.readFileSync(adapter,'utf8'));
 const out=gradle(path.resolve(app),[':platform:bukkit:Moonlight:verifyFlameDBIngestion','--rerun-tasks'],'moonlight-gradle.txt');
 for(const marker of ['ERAMC_FLAMEDB_INGESTION_VERIFIED','BURST','RECOVERY','SHUTDOWN','LIVE_RESTART','NEGATIVE_CONTROL','SDK_DELEGATION','LEGACY_JOURNAL'])assert.ok(out.includes(marker),`Missing application observation ${marker}`);
 console.log('MOONLIGHT_SDK_BATCHING_VERIFIED');
}else throw new Error('Select ownership, sdk or application');
