[CmdletBinding()]
param(
  [string]$PostgresImage = 'timescale/timescaledb:latest-pg17@sha256:b346edcdb51a1fd6020e3965e0bd1c9f3406fa6d5fbce1e28f4852587ef934e2',
  [string]$RedisImage = 'redis:8-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0',
  [ValidateRange(1000,30000)][int]$SentinelTiltPeriodMilliseconds = 30000
)
$ErrorActionPreference = 'Stop'
$rootPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
Set-Location -LiteralPath $rootPath
$suffix = [guid]::NewGuid().ToString('N').Substring(0,10)
$prefix = "flamedb-nextgen-ha-$suffix"
$network = "$prefix-net"
$volume = "$prefix-data"
$artifactPath = Join-Path $rootPath ".compat/ha-$suffix"
New-Item -ItemType Directory -Path $artifactPath -Force | Out-Null
$containers = [Collections.Generic.List[string]]::new()
$oldGoos = $env:GOOS
$oldCgo = $env:CGO_ENABLED
$dockerExecutable = (Get-Command docker.exe -CommandType Application).Source
function InvokeDocker([string[]]$Arguments) {
  $result = & $dockerExecutable @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) { throw "Docker failed: $($Arguments -join ' '): $($result -join "`n")" }
  return ($result -join "`n")
}
function StartContainer([string]$Name, [string[]]$Arguments) {
  $id = InvokeDocker (@('run','-d','--name',$Name,'--label','flamedb.test=nextgen-ha','--label',"flamedb.run=$suffix",'--network',$network) + $Arguments)
  $containers.Add($id.Trim())
  return $Name
}
function WaitPostgres([string]$Name) {
  for ($i=0; $i -lt 100; $i++) {
    & docker exec $Name pg_isready -h 127.0.0.1 -U flamedb -d flamedb *> $null
    if ($LASTEXITCODE -eq 0) { return }
    Start-Sleep -Milliseconds 200
  }
  throw "$Name did not become ready."
}
function AssertTests([string]$Output, [string[]]$Names) {
  if ($Output -match '--- SKIP:') { throw 'HA test skipped.' }
  foreach ($name in $Names) {
    if ($Output -notmatch ('(?m)^--- PASS: ' + [regex]::Escape($name) + ' \(')) { throw "Missing test PASS: $name" }
  }
}
function WaitRedisReplicas([string[]]$Names, [int]$Expected) {
  for ($attempt=0; $attempt -lt 60; $attempt++) {
    $online=0
    foreach ($name in $Names) {
      $replication=InvokeDocker @('exec',$name,'redis-cli','INFO','replication')
      if ($replication -match '(?m)^role:slave\r?$' -and $replication -match '(?m)^master_link_status:up\r?$' -and $replication -match '(?m)^master_sync_in_progress:0\r?$') { $online++ }
    }
    if ($online -eq $Expected) { return }
    Start-Sleep -Milliseconds 200
  }
  throw "Expected $Expected synchronized Redis replicas, found $online."
}
function FenceRedis([string]$Mode, [string[]]$Addresses, [string]$MasterName) {
  $signal = Join-Path $artifactPath "$Mode.ready"
  $fenced = Join-Path $artifactPath "$Mode.fenced"
  $stdout = Join-Path $artifactPath "$Mode.stdout.txt"
  $stderr = Join-Path $artifactPath "$Mode.stderr.txt"
  $runnerName = "$prefix-test-$Mode"
  $arguments = @('run','--rm','--name',$runnerName,'--label','flamedb.test=nextgen-ha','--label',"flamedb.run=$suffix",'--network',$network,
    '--mount',"type=bind,source=$artifactPath,target=/signals",
    '-e','FLAMEDB_REDIS_HA_INTEGRATION=1','-e',"FLAMEDB_TEST_REDIS_MODE=$Mode",'-e',"FLAMEDB_TEST_REDIS=$($Addresses -join ',')",
    '-e',"FLAMEDB_TEST_REDIS_MASTER=$MasterName",'-e','FLAMEDB_TEST_POSTGRES=postgres://flamedb:flamedb@pg2:5432/flamedb?sslmode=disable',
    '-e',"FLAMEDB_HA_SIGNAL=/signals/$Mode.ready",'-e',"FLAMEDB_HA_FENCED=/signals/$Mode.fenced",
    $RedisImage,'/signals/redis-ha.test','-test.v','-test.run=^TestRedisMasterLossAndRecovery$','-test.timeout=90s')
  # All arguments are internally generated paths/names; quote paths containing spaces.
  $quoted = $arguments | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }
  $process = Start-Process -FilePath 'docker' -ArgumentList $quoted -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
  try {
    $deadline = [DateTime]::UtcNow.AddSeconds(40)
    while (-not (Test-Path -LiteralPath $signal)) {
      $process.Refresh()
      if ($process.HasExited -or [DateTime]::UtcNow -gt $deadline) { throw "HA runner failed before master signal: $(Get-Content -LiteralPath $stdout -Raw) $(Get-Content -LiteralPath $stderr -Raw)" }
      Start-Sleep -Milliseconds 200
    }
    $masterAddress = (Get-Content -LiteralPath $signal -Raw).Trim()
    $masterHost = $masterAddress.Split(':')[0]
    $owner = $null
    foreach ($id in $containers) {
      $info = (InvokeDocker @('inspect',$id) | ConvertFrom-Json)[0]
      $ip = $info.NetworkSettings.Networks.$network.IPAddress
      if ($ip -eq $masterHost -or $info.Name.TrimStart('/') -eq $masterHost -or @($info.NetworkSettings.Networks.$network.Aliases) -contains $masterHost) {
        if ($info.Config.Labels.'flamedb.run' -ne $suffix -or $info.Config.Image -ne $RedisImage) { throw 'Redis master ownership guard failed.' }
        $owner = $id; break
      }
    }
    if (-not $owner) { throw "No owned master for $masterAddress" }
    InvokeDocker @('kill','--signal','KILL',$owner) | Out-Null
    'fenced' | Set-Content -LiteralPath $fenced
    if (-not $process.WaitForExit(95000)) { throw 'Redis HA runner timed out.' }
    $process.Refresh()
    $output = (Get-Content -LiteralPath $stdout -Raw) + (Get-Content -LiteralPath $stderr -Raw)
    $output | Write-Output
    if ($process.ExitCode -ne 0) { throw "$Mode HA failed." }
    AssertTests $output @('TestRedisMasterLossAndRecovery')
  } finally {
    $process.Refresh()
    if (-not $process.HasExited) {
      # This exact generated runner belongs to this invocation only.
      & docker rm -f $runnerName *> $null
      Stop-Process -Id $process.Id
    }
  }
}
try {
  $env:GOOS='linux'; $env:CGO_ENABLED='0'
  & go test -c -o (Join-Path $artifactPath 'analytics.test') ./internal/analytics
  if ($LASTEXITCODE -ne 0) { throw 'Linux analytics build failed.' }
  & go test -c -o (Join-Path $artifactPath 'redis-ha.test') ./internal/ha
  if ($LASTEXITCODE -ne 0) { throw 'Linux HA build failed.' }
  $env:GOOS=$oldGoos; $env:CGO_ENABLED=$oldCgo
  InvokeDocker @('network','create','--label','flamedb.test=nextgen-ha','--label',"flamedb.run=$suffix",$network) | Out-Null
  InvokeDocker @('volume','create','--label','flamedb.test=nextgen-ha','--label',"flamedb.run=$suffix",$volume) | Out-Null
  $pg1 = StartContainer "$prefix-pg1" @('--network-alias','pg1','-p','127.0.0.1::5432','-e','POSTGRES_USER=flamedb','-e','POSTGRES_PASSWORD=flamedb','-e','POSTGRES_DB=flamedb',$PostgresImage,'postgres','-c','wal_level=replica','-c','max_wal_senders=8')
  WaitPostgres $pg1
  InvokeDocker @('exec',$pg1,'sh','-c','printf "host replication flamedb all scram-sha-256\n" >> "$PGDATA/pg_hba.conf"') | Out-Null
  InvokeDocker @('exec',$pg1,'psql','-U','flamedb','-d','flamedb','-c','SELECT pg_reload_conf()') | Out-Null
  InvokeDocker @('run','--rm','--label','flamedb.test=nextgen-ha','--label',"flamedb.run=$suffix",'--network',$network,'-e','PGPASSWORD=flamedb','-v',"${volume}:/var/lib/postgresql/data",$PostgresImage,'sh','-c','chown postgres:postgres /var/lib/postgresql/data && chmod 700 /var/lib/postgresql/data && su postgres -c "pg_basebackup -h pg1 -U flamedb -D /var/lib/postgresql/data -R -X stream --checkpoint=fast"') | Out-Null
  $pg2 = StartContainer "$prefix-pg2" @('--network-alias','pg2','-p','127.0.0.1::5432','-v',"${volume}:/var/lib/postgresql/data",$PostgresImage,'postgres','-c','primary_conninfo=host=pg1 user=flamedb password=flamedb application_name=flame_standby')
  WaitPostgres $pg2
  InvokeDocker @('exec',$pg1,'psql','-U','flamedb','-d','flamedb','-c',"ALTER SYSTEM SET synchronous_standby_names='ANY 1 (flame_standby)'") | Out-Null
  InvokeDocker @('exec',$pg1,'psql','-U','flamedb','-d','flamedb','-c','SELECT pg_reload_conf()') | Out-Null
  for ($i=0; $i -lt 100; $i++) {
    $count = InvokeDocker @('exec',$pg1,'psql','-U','flamedb','-d','flamedb','-Atc',"SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND sync_state='quorum'")
    if ($count.Trim() -eq '1') { break }
    Start-Sleep -Milliseconds 200
  }
  if ($count.Trim() -ne '1') { throw 'Physical quorum standby not streaming.' }
  $port1 = ((InvokeDocker @('inspect',$pg1) | ConvertFrom-Json)[0].NetworkSettings.Ports.'5432/tcp')[0].HostPort
  $port2 = ((InvokeDocker @('inspect',$pg2) | ConvertFrom-Json)[0].NetworkSettings.Ports.'5432/tcp')[0].HostPort
  $env:FLAMEDB_HA_INTEGRATION='1'
  $env:FLAMEDB_HA_PRIMARY_CONTAINER=$pg1
  $env:FLAMEDB_HA_REPLICA_DSN="postgres://flamedb:flamedb@127.0.0.1:$port2/flamedb?sslmode=disable"
  $env:FLAMEDB_HA_FAILOVER_DSN="postgres://flamedb:flamedb@127.0.0.1:$port1,127.0.0.1:$port2/flamedb?sslmode=disable&target_session_attrs=read-write&connect_timeout=1"
  $output = & go test ./internal/ha -run '^TestSynchronousPostgresQuorumAndFailover$' -count=1 -v -timeout=90s 2>&1
  $code=$LASTEXITCODE; $output | Write-Output
  $output | Set-Content -LiteralPath (Join-Path $artifactPath 'postgres.txt')
  if ($code -ne 0) { throw 'PostgreSQL HA failed.' }
  AssertTests ($output -join "`n") @('TestSynchronousPostgresQuorumAndFailover')
  $clusterNodes=@()
  for ($i=1; $i -le 6; $i++) {
    $node=StartContainer "$prefix-rc$i" @('--network-alias',"rc$i",$RedisImage,'redis-server','--cluster-enabled','yes','--cluster-node-timeout','1000','--appendonly','yes','--protected-mode','no')
    $clusterNodes += "${node}:6379"
  }
  InvokeDocker (@('exec',"$prefix-rc1",'redis-cli','--cluster','create') + $clusterNodes + @('--cluster-replicas','1','--cluster-yes')) | Out-Null
  WaitRedisReplicas @(1..6 | ForEach-Object { "$prefix-rc$_" }) 3
  $smaster=StartContainer "$prefix-smaster" @('--network-alias','smaster',$RedisImage,'redis-server','--appendonly','yes','--protected-mode','no')
  $sreplica=StartContainer "$prefix-sreplica" @($RedisImage,'redis-server','--replicaof','smaster','6379','--appendonly','yes','--protected-mode','no')
  WaitRedisReplicas @($sreplica) 1
  $masterIp=((InvokeDocker @('inspect',$smaster) | ConvertFrom-Json)[0].NetworkSettings.Networks.$network.IPAddress)
  "port 26379`nsentinel monitor flame-master $masterIp 6379 1`nsentinel down-after-milliseconds flame-master 1000`nsentinel failover-timeout flame-master 10000`nsentinel parallel-syncs flame-master 1" | Set-Content -LiteralPath (Join-Path $artifactPath 'sentinel.conf')
  $sentinel=StartContainer "$prefix-sentinel" @('--mount',"type=bind,source=$artifactPath,target=/signals",$RedisImage,'sh','-c','cp /signals/sentinel.conf /tmp/sentinel.conf && redis-server /tmp/sentinel.conf --sentinel')
  # Optional isolated-fixture accommodation for VM wall-clock jumps. The normal
  # deployment's 30-second TILT safety period remains the default.
  InvokeDocker @('exec',$sentinel,'redis-cli','-p','26379','SENTINEL','DEBUG','TILT-PERIOD',"$SentinelTiltPeriodMilliseconds") | Out-Null
  @{sentinel_tilt_period_ms=$SentinelTiltPeriodMilliseconds;scope='isolated disposable test fixture'} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $artifactPath 'fixture.json')
  $required=@('TestExactRankingEncoding','TestPostgresSemantics','TestPostgresBatchReplicaAndRejection','TestPostgresConcurrentAndCacheRecovery','TestPostgresLargeNativeAnalytics','TestPostgresLosslessIDsAndAtomicOverflow','TestRedisOutageFallbackAndIncompleteRebuild','TestPostgresNativeBoardBatch','TestPostgresResultByteBudget','TestRedisDelayedProjectionCannotOverwriteNewerState','TestRedisSocketDeadline','TestSynchronousQuorumValidation','TestPostgresSingleReplicaQuorum','TestPostgresSharedAutomaticTimestampAllocator','TestPostgresRawHistoryLeavesScoreProjectionValid','TestRedisAttemptBudgetPreservesPostgresFallback','TestPostgresParallelRawBatches','TestRedisRankingAtomicVersionAndRoundTrips','TestPostgresLargeProtocolIdentities','TestPostgresSchemaUpgradeAndBoundedRebuild','TestPostgresIdentityIndexBoundary','TestPostgresIndependentMetricBatches','TestPostgresCoalescedSmallBatches','TestRedisProjectionBoardCommandCost','TestPostgresBatchedAutomaticTimestamps','TestPostgresAutomaticBatchIndependentMetrics','TestPostgresAutomaticDirectCopy','TestRedisBatchedProjectionRoundTrips','TestPostgresIngestSnapshotAfterLock')
  foreach ($mode in @('cluster','sentinel')) {
    $addresses=if ($mode -eq 'cluster') { @('rc1:6379','rc2:6379','rc3:6379') } else { @("${sentinel}:26379") }
    $output=InvokeDocker @('run','--rm','--network',$network,'--mount',"type=bind,source=$artifactPath,target=/signals",'-e','FLAMEDB_INTEGRATION=1','-e','FLAMEDB_TEST_TIMESCALE=1','-e','FLAMEDB_TEST_POSTGRES=postgres://flamedb:flamedb@pg2:5432/flamedb?sslmode=disable','-e',"FLAMEDB_TEST_REDIS_MODE=$mode",'-e',"FLAMEDB_TEST_REDIS=$($addresses -join ',')",'-e','FLAMEDB_TEST_REDIS_MASTER=flame-master',$RedisImage,'/signals/analytics.test','-test.v','-test.timeout=120s')
    $output | Write-Output
    $output | Set-Content -LiteralPath (Join-Path $artifactPath "analytics-$mode.txt")
    AssertTests $output $required
    FenceRedis $mode $addresses 'flame-master'
  }
  Write-Output "HA_ARTIFACTS=$artifactPath"
  Write-Output 'REAL_POSTGRES_CLUSTER_SENTINEL_HA_VERIFIED'
} finally {
  $env:GOOS=$oldGoos; $env:CGO_ENABLED=$oldCgo
  $env:FLAMEDB_HA_INTEGRATION=$null
  foreach ($id in $containers) {
    $info=& docker inspect $id 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -eq 0 -and $info[0].Config.Labels.'flamedb.run' -eq $suffix) {
      $name=$info[0].Name.TrimStart('/')
      & docker logs --tail 80 $id 2>&1 | Set-Content -LiteralPath (Join-Path $artifactPath "$name.log")
      & docker rm -f $id *> $null
    }
  }
  foreach ($resource in @(@('volume',$volume),@('network',$network))) {
    $info=& docker $resource[0] inspect $resource[1] 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -eq 0 -and $info[0].Labels.'flamedb.run' -eq $suffix) { & docker $resource[0] rm $resource[1] *> $null }
  }
}
