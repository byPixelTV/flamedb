[CmdletBinding()]
param(
  [string]$LoadtestBinary = '.compat/loadtest-linux',
  [string]$OutputDirectory = '.compat/benchmarks-linux-client',
  [string]$Network = 'flamedb-nextgen_default',
  [string]$Addresses = 'api:7777,api-2:7777,api-3:7777',
  [string[]]$Containers = @('flamedb-nextgen-postgres-1','flamedb-nextgen-redis-1','flamedb-nextgen-api-1','flamedb-nextgen-api-2-1','flamedb-nextgen-api-3-1'),
  [string]$RunnerImage = 'redis:8-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0',
  [ValidateRange(1,256)][int]$Workers = 32,
  [ValidateRange(1,100000)][int]$Entities = 1000,
  [ValidateRange(1,10000)][int]$Metrics = 16,
  [switch]$AutomaticTimestamps,
  [string[]]$Cases = @(),
  [ValidateRange(5,3600)][int]$DurationSeconds = 15
)
$ErrorActionPreference='Stop'
if (-not $env:FLAMEDB_API_KEY) { throw 'Set FLAMEDB_API_KEY for the development APIs.' }
$binary=(Resolve-Path -LiteralPath $LoadtestBinary).Path
$binaryDirectory=Split-Path -Parent $binary
$binaryName=Split-Path -Leaf $binary
$outputPath=[IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$runId=[guid]::NewGuid().ToString('N').Substring(0,12)
$dockerExecutable=(Get-Command docker.exe -CommandType Application).Source
$matrix=@(
  @{name='rank';mode='rank';batch=1;board='true'},
  @{name='top';mode='top';batch=1;board='true'},
  @{name='plain-1';mode='write';batch=1;board='false'},
  @{name='board-1';mode='write';batch=1;board='true'},
  @{name='plain-100';mode='write';batch=100;board='false'},
  @{name='plain-1000';mode='write';batch=1000;board='false'},
  @{name='plain-10000';mode='write';batch=10000;board='false'},
  @{name='board-1000';mode='write';batch=1000;board='true'},
  @{name='mixed';mode='mixed';batch=100;board='true'}
)
if ($Cases.Count -gt 0) {
  $matrix+=@(@{name='board-250';mode='write';batch=250;board='true'},@{name='plain-250';mode='write';batch=250;board='false'})
  foreach ($case in $Cases) { if ($case -notin $matrix.name) { throw "Unknown workload: $case" } }
  $matrix = @($matrix | Where-Object { $_.name -in $Cases })
}
$identities=@($Containers | ForEach-Object {
  $info=(& $dockerExecutable inspect $_ | ConvertFrom-Json)[0]
  if ($LASTEXITCODE -ne 0 -or -not $info.State.Running) { throw "Missing running fixture: $_" }
  @{name=$_;id=$info.Id;image=$info.Image}
})
$source=@(Get-ChildItem -LiteralPath 'cmd','internal' -File -Recurse | Where-Object { $_.Extension -in '.go','.sql' } | Sort-Object FullName | ForEach-Object { @{path=[IO.Path]::GetRelativePath((Get-Location).Path,$_.FullName);sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash} })
$postgres=@($identities | Where-Object { $_.name -like '*-postgres-1' })
$postgresSettings=$null
if ($postgres.Count -eq 1) {
  $settings=& $dockerExecutable exec $postgres[0].name psql -U flamedb -d flamedb -At -c "SELECT json_object_agg(name,json_build_object('setting',setting,'unit',unit,'pending_restart',pending_restart)) FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit','max_wal_size','checkpoint_timeout','wal_compression','shared_buffers');"
  if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL configuration observation failed.' }
  $postgresSettings=$settings | ConvertFrom-Json
}
$configs=@('compose.nextgen.yml','deploy/nextgen.yml' | ForEach-Object { @{path=$_;sha256=(Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash} })
@{run_id=$runId;client_os='linux';network=$Network;addresses=$Addresses;workers=$Workers;duration_seconds=$DurationSeconds;metrics=$Metrics;entities=$Entities;automatic_timestamps=$AutomaticTimestamps.IsPresent;matrix=$matrix;containers=$identities;source=$source;configs=$configs;postgres_settings=$postgresSettings;binary_sha256=(Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash;docker_environment=(& $dockerExecutable info --format 'cpus={{.NCPU}} memory_bytes={{.MemTotal}} server={{.ServerVersion}}')} |
  ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $outputPath 'environment.json')
foreach ($item in $matrix) {
  $name="flamedb-nextgen-load-$runId-$($item.name)"
  $stdout=Join-Path $outputPath "$($item.name).json"
  $stderr=Join-Path $outputPath "$($item.name).stderr.txt"
  $arguments=@('run','--rm','--name',$name,'--label','flamedb.test=linux-load', '--label',"flamedb.run=$runId",'--network',$Network,'--mount',"type=bind,source=$binaryDirectory,target=/bench,readonly",'-e','FLAMEDB_API_KEY',$RunnerImage,"/bench/$binaryName",'--addrs',$Addresses,'--mode',$item.mode,'--batch-size',"$($item.batch)",('--leaderboard='+$item.board),'--workers',"$Workers",'--metrics',"$Metrics",'--entities',"$Entities",'--fixture-events','1000','--duration',"${DurationSeconds}s",'--prefix',"linux$runId.$($item.name)",'--durability-contract','postgres-sync',('--automatic-timestamps='+$AutomaticTimestamps.IsPresent.ToString().ToLowerInvariant()))
  $quoted=@($arguments | ForEach-Object { if ($_ -match '\s') { '"'+$_+'"' } else { $_ } })
  $process=Start-Process -FilePath $dockerExecutable -ArgumentList $quoted -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
  try {
    $clock=[Diagnostics.Stopwatch]::StartNew()
    do {
      $rows=& $dockerExecutable stats --no-stream --format '{{json .}}' @Containers
      if ($LASTEXITCODE -ne 0) { throw 'Resource observation failed.' }
      @{elapsed_seconds=$clock.Elapsed.TotalSeconds;containers=@($rows | ForEach-Object { $_ | ConvertFrom-Json })} | ConvertTo-Json -Depth 6 -Compress | Add-Content -LiteralPath (Join-Path $outputPath "$($item.name).resources.jsonl")
      $process.Refresh()
      if (-not $process.HasExited) { Start-Sleep -Milliseconds 250 }
    } while (-not $process.HasExited)
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) { throw "Linux workload $($item.name) failed: $(Get-Content -LiteralPath $stderr -Raw)" }
    $report=Get-Content -LiteralPath $stdout -Raw | ConvertFrom-Json
    if (-not $report.correctness_pass -or $report.errors -ne 0 -or $report.events_ambiguous -ne 0 -or $report.requests_confirmed -eq 0 -or $report.elapsed_seconds_including_drain -lt $DurationSeconds) { throw "Incomplete Linux measurement: $($item.name)" }
    Write-Output "$($item.name): requests/s=$($report.confirmed_requests_per_second) events/s=$($report.confirmed_events_per_second)"
  } finally {
    # A failed/aborted host client must not leave its workload running.
    $info=& $dockerExecutable inspect $name 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -eq 0 -and $info[0].Config.Labels.'flamedb.run' -eq $runId -and $info[0].Config.Labels.'flamedb.test' -eq 'linux-load') { & $dockerExecutable rm -f $info[0].Id *> $null }
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -ErrorAction SilentlyContinue }
  }
}
Write-Output 'LINUX_NETWORK_BATCH_SWEEP_VERIFIED'
