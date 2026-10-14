[CmdletBinding()]
param(
  [string]$TestBinary='.compat/analytics-projection-linux.test',
  [string]$OutputDirectory='.compat/projection-modes',
  [string]$Network='flamedb-nextgen-ha',
  [string]$DatabaseNetwork='flamedb-nextgen_default'
)
$ErrorActionPreference='Stop'
$binary=(Resolve-Path -LiteralPath $TestBinary).Path
$outputPath=[IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force|Out-Null
$runId=[guid]::NewGuid().ToString('N').Substring(0,12)
$required=@('TestRedisProjectionBoardCommandCost','TestRedisDelayedProjectionCannotOverwriteNewerState','TestRedisRankingAtomicVersionAndRoundTrips','TestPostgresIndependentMetricBatches','TestRedisBatchedProjectionRoundTrips')
$pattern='^('+($required -join '|')+')$'
$image='redis:8-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0'
function InvokeDocker([string[]]$Arguments) {
  $output=& docker @Arguments 2>&1
  if($LASTEXITCODE -ne 0){throw "Docker failed: $($output -join "`n")"}
  return ($output -join "`n")
}
$fixtures=@()
foreach($name in @('flamedb-nextgen-rc1','flamedb-nextgen-rc2','flamedb-nextgen-rc3','flamedb-nextgen-rc4','flamedb-nextgen-rc5','flamedb-nextgen-rc6','flamedb-nextgen-sentinel')) {
  $info=(InvokeDocker @('inspect',$name)|ConvertFrom-Json)[0]
  if(-not $info.State.Running -or -not $info.NetworkSettings.Networks.$Network){throw "Missing local fixture $name in $Network"}
  $fixtures+=@{name=$name;id=$info.Id;image=$info.Image}
}
$pg=(InvokeDocker @('inspect','flamedb-nextgen-postgres-1')|ConvertFrom-Json)[0]
if(-not $pg.State.Running -or $pg.Config.Labels.'com.docker.compose.project' -ne 'flamedb-nextgen' -or -not $pg.NetworkSettings.Networks.$DatabaseNetwork){throw 'Expected owned development PostgreSQL'}
@{run_id=$runId;test_binary_sha256=(Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash;required_tests=$required;fixtures=$fixtures;postgres_id=$pg.Id;failover_injected=$false}|
  ConvertTo-Json -Depth 6|Set-Content -LiteralPath (Join-Path $outputPath 'environment.json')
foreach($mode in @('cluster','sentinel')) {
  $name="flamedb-nextgen-projection-$runId-$mode"
  $id=$null
  try {
    $addresses=if($mode -eq 'cluster'){(1..6|ForEach-Object{"flamedb-nextgen-rc${_}:6379"}) -join ','}else{'flamedb-nextgen-sentinel:26379'}
    $master=if($mode -eq 'sentinel'){'flame-master'}else{''}
    $id=InvokeDocker @('create','--name',$name,'--label','flamedb.test=projection-mode','--label',"flamedb.run=$runId",'--network',$Network,'--mount',"type=bind,source=$(Split-Path -Parent $binary),target=/test,readonly",'-e','FLAMEDB_TEST_POSTGRES=postgres://flamedb:flamedb@postgres:5432/flamedb?sslmode=disable','-e',"FLAMEDB_TEST_REDIS=$addresses",'-e',"FLAMEDB_TEST_REDIS_MODE=$mode",'-e',"FLAMEDB_TEST_REDIS_MASTER=$master",'-e','FLAMEDB_INTEGRATION=1','-e','FLAMEDB_TEST_TIMESCALE=1',$image,"/test/$(Split-Path -Leaf $binary)",'-test.v','-test.count=1',"-test.run=$pattern",'-test.timeout=2m')
    InvokeDocker @('network','connect',$DatabaseNetwork,$id)|Out-Null
    $text=InvokeDocker @('start','-a',$id)
    $text|Set-Content -LiteralPath (Join-Path $outputPath "$mode.txt")
    $completed=(InvokeDocker @('inspect',$id)|ConvertFrom-Json)[0]
    if($completed.State.ExitCode -ne 0 -or $text -match '--- (FAIL|SKIP):'){throw "Projection mode $mode failed: $text"}
    foreach($test in $required){if($text -notmatch ('--- PASS: '+[regex]::Escape($test)+' \(')){throw "Missing $mode observation $test"}}
    Write-Output "$mode`: $($required.Count) real projection/ranking tests passed; no failover claim"
  } finally {
    if($id){
      $owned=(InvokeDocker @('inspect',$id)|ConvertFrom-Json)[0]
      if($owned.Name -ne "/$name" -or $owned.Config.Labels.'flamedb.run' -ne $runId -or $owned.Config.Labels.'flamedb.test' -ne 'projection-mode'){throw 'Runner ownership changed'}
      InvokeDocker @('rm','-f',$id)|Out-Null
    }
  }
}
Write-Output 'REDIS_PROJECTION_MODES_VERIFIED'
