// Verify source and packaged listeners without committing EraMC.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..'),dir=path.join(root,'.compat/production-journal'),app=process.env.FLAMEDB_ERAMC_SOURCE||'C:/Dev/Java/EraMC';
fs.mkdirSync(dir,{recursive:true});
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
function run(cmd,args,cwd=root,file){
 const r=spawnSync(cmd,args,{cwd,encoding:'utf8',windowsHide:true,timeout:600000,maxBuffer:16*1024*1024});
 if(file)fs.writeFileSync(path.join(dir,file),(r.stdout||'')+'\n'+(r.stderr||''));
 if(r.error||r.status!==0)throw r.error||Error(cmd+' failed\n'+(r.stdout||'').slice(-3000)+'\n'+(r.stderr||'').slice(-3000));
 return r.stdout;
}
function gradle(args,cwd,file){const out=run(process.platform==='win32'?'cmd.exe':'./gradlew',process.platform==='win32'?['/d','/c','gradlew.bat',...args]:args,cwd,file);assert.match(out,/BUILD SUCCESSFUL/);}
gradle([':platform:bukkit:modules:CACoreModule:shadowJar',':platform:bukkit:modules:EcoSmpCoreModule:shadowJar','--console=plain'],app,'eramc-mob-killer-build.txt');
const artifacts=[];
for(const [module,pkg] of [['CACoreModule','cacoremodule'],['EcoSmpCoreModule','ecosmpcoremodule']]){
 const source=path.join(app,'platform/bukkit/modules',module,'src/main/kotlin/net/eramc/moonlight',pkg,'metrics/MetricEntityDeathListener.kt'),code=fs.readFileSync(source,'utf8');
 assert.match(code,/leaderboardEntity = killer\.uniqueId\.toString\(\)/);assert.match(code,/"player" to killer\.uniqueId\.toString\(\)/);assert.doesNotMatch(code,/leaderboardEntity = entity\.uniqueId/);
 // Module-local directories can contain stale JARs. Current outputs are here.
 const libs=path.join(app,'build/libs'),jars=fs.readdirSync(libs).filter(n=>n.startsWith(module+'-')&&n.endsWith('.jar')).map(n=>path.join(libs,n)).sort((a,b)=>fs.statSync(b).mtimeMs-fs.statSync(a).mtimeMs);
 assert.ok(jars.length);const jar=jars[0];assert.ok(fs.statSync(jar).mtimeMs>=fs.statSync(source).mtimeMs,'Stale module JAR');
 const classes=run('jar',['tf',jar]).split(/\r?\n/).filter(n=>/MetricEntityDeathListener.*\.class$/.test(n));assert.ok(classes.length);
 const bytecode=classes.map(name=>run('javap',['-c','-p','-classpath',jar,name.slice(0,-6).replaceAll('/','.')])).join('\n');
 assert.equal((bytecode.match(/org\/bukkit\/entity\/Player\.getUniqueId/g)||[]).length,2,'Killer UUID missing from packaged listener');
 assert.equal((bytecode.match(/org\/bukkit\/entity\/LivingEntity\.getUniqueId/g)||[]).length,0,'Mob UUID remains in packaged listener');
 artifacts.push({module,path:jar,sha256:hash(jar),source:hash(source)});
}
gradle(['test','--rerun-tasks','--console=plain'],path.join(root,'pkg/kotlin'),'sdk-normal-test.txt');
fs.writeFileSync(path.join(dir,'application-artifacts.json'),JSON.stringify(artifacts,null,2));
console.log('PRODUCTION_JOURNAL_APPLICATION_VERIFIED');
