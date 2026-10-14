[CmdletBinding()]
param(
  [string]$LoadtestBinary = '.compat/loadtest.exe',
  [string]$Addresses = '127.0.0.1:7777',
  [string]$OutputDirectory = '.compat/benchmarks',
  [string[]]$DatabaseContainers = @('flamedb-nextgen-postgres-1', 'flamedb-nextgen-redis-1', 'flamedb-nextgen-api-1'),
  [string[]]$MetricsUrls = @('http://127.0.0.1:9090/metrics'),
  [int[]]$ApiProcessIds = @(),
  [int]$Workers = 32,
  [string]$Duration = '15s',
  [int]$Metrics = 64,
  [int]$FixtureEvents = 1000
)
$ErrorActionPreference = 'Stop'
if (-not $env:FLAMEDB_API_KEY) { throw 'Set FLAMEDB_API_KEY before benchmarking.' }
$binaryPath = (Resolve-Path -LiteralPath $LoadtestBinary).Path
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$runPrefix = 'matrix' + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$matrix = @(
  @{name='single-write-board';mode='write';batch=1;pipeline=1;board='true'},
  @{name='pipeline-write-board';mode='write';batch=1;pipeline=16;board='true'},
  @{name='batch-write-board';mode='write';batch=100;pipeline=1;board='true'},
  @{name='batch-write-plain';mode='write';batch=100;pipeline=1;board='false'},
  @{name='mixed';mode='mixed';batch=10;pipeline=4;board='true'}
)
foreach ($mode in @('top','rank','window','sum','avg','count','history','tags')) {
  $matrix += @{name=$mode;mode=$mode;batch=1;pipeline=1;board='true'}
}
$sourceFiles = @(Get-ChildItem -LiteralPath 'internal/analytics','internal/server','cmd/loadtest' -File -Recurse | Sort-Object FullName | ForEach-Object {
  @{path=[IO.Path]::GetRelativePath((Get-Location).Path,$_.FullName);sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}
})
$dockerEnvironment = if ($DatabaseContainers.Count -gt 0) { & docker info --format 'cpus={{.NCPU}} memory_bytes={{.MemTotal}} server={{.ServerVersion}}' } else { 'not sampled' }
$containerImages = @($DatabaseContainers | ForEach-Object { @{container=$_;image_id=(& docker inspect --format '{{.Image}}' $_)} })
@{addresses=$Addresses;workers=$Workers;duration=$Duration;metrics=$Metrics;fixture_events=$FixtureEvents;started_utc=[DateTime]::UtcNow.ToString('o');matrix=$matrix;os=[Environment]::OSVersion.ToString();processor_count=[Environment]::ProcessorCount;docker_environment=$dockerEnvironment;loadtest_sha256=(Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash;source_files=$sourceFiles;resource_containers=$DatabaseContainers;container_images=$containerImages} |
  ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $outputPath 'matrix.json')
$failed = @()
foreach ($item in $matrix) {
  $prefix = $runPrefix + '.' + $item.name
  $stdout = Join-Path $outputPath ($item.name + '.json')
  $stderr = Join-Path $outputPath ($item.name + '.stderr.txt')
  $resourcePath = Join-Path $outputPath ($item.name + '.resources.jsonl')
  $arguments = @('--addrs', $Addresses, '--workers', $Workers, '--duration', $Duration,
    '--metrics', $Metrics, '--fixture-events', $FixtureEvents, '--prefix', $prefix,
    '--mode', $item.mode, '--batch-size', $item.batch, '--pipeline', $item.pipeline,
    ('--leaderboard=' + $item.board), '--durability-contract', 'postgres-sync')
  $process = Start-Process -FilePath $binaryPath -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
  do {
    $sample = @{utc=[DateTime]::UtcNow.ToString('o');loadtest_pid=$process.Id;api=@();docker=@();metrics=@()}
    foreach ($taskPid in @($process.Id) + $ApiProcessIds) {
      $taskProcess = Get-Process -Id $taskPid -ErrorAction SilentlyContinue
      if ($taskProcess) { $sample.api += @{pid=$taskPid;cpu_seconds=$taskProcess.CPU;working_set_bytes=$taskProcess.WorkingSet64;private_bytes=$taskProcess.PrivateMemorySize64;threads=$taskProcess.Threads.Count} }
    }
    if ($DatabaseContainers.Count -gt 0) {
      $rows = & docker stats --no-stream --format '{{json .}}' @DatabaseContainers
      $sample.docker = @($rows | ForEach-Object { $_ | ConvertFrom-Json })
      $pg = $DatabaseContainers[0]
      $sample.postgres_activity = & docker exec $pg psql -U flamedb -d flamedb -Atc "SELECT json_build_object('state',state,'wait_event_type',wait_event_type,'sessions',count(*)) FROM pg_stat_activity WHERE datname='flamedb' GROUP BY state,wait_event_type;"
      $sample.postgres_database = & docker exec $pg psql -U flamedb -d flamedb -Atc "SELECT row_to_json(s) FROM (SELECT xact_commit,xact_rollback,blks_read,blks_hit,tup_inserted,tup_updated,tup_deleted,deadlocks,temp_bytes FROM pg_stat_database WHERE datname='flamedb') s;"
    }
    foreach ($url in $MetricsUrls) {
      try { $sample.metrics += @{url=$url;text=(Invoke-WebRequest -Uri $url -TimeoutSec 2).Content} }
      catch { $sample.metrics += @{url=$url;error=$_.Exception.Message} }
    }
    $sample | ConvertTo-Json -Depth 10 -Compress | Add-Content -LiteralPath $resourcePath
    $process.Refresh()
    if (-not $process.HasExited) { Start-Sleep -Milliseconds 250 }
  } while (-not $process.HasExited)
  $process.WaitForExit()
  if ($process.ExitCode -ne 0) { $failed += $item.name }
  Write-Output ($item.name + ': exit=' + $process.ExitCode + ' output=' + $stdout)
}
if ($failed.Count -gt 0) { throw ('Benchmark/readback failures: ' + ($failed -join ', ')) }
