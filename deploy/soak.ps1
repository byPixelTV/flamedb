[CmdletBinding()]
param(
  [string]$LoadtestBinary = '.compat/loadtest-soak.exe',
  [string]$Addresses = '127.0.0.1:27777,127.0.0.1:27778,127.0.0.1:27779',
  [string]$OutputDirectory = '.compat/soak',
  [string]$ComposeProject = 'flamedb-nextgen',
  [string]$PostgresContainer = 'flamedb-nextgen-postgres-1',
  [string]$RedisContainer = 'flamedb-nextgen-redis-1',
  [string[]]$ApiContainers = @('flamedb-nextgen-api-1','flamedb-nextgen-api-2-1','flamedb-nextgen-api-3-1'),
  [string[]]$MetricsUrls = @('http://127.0.0.1:29090/metrics','http://127.0.0.1:29091/metrics','http://127.0.0.1:29092/metrics'),
  [ValidateRange(60,86400)][int]$DurationSeconds = 300,
  [ValidateRange(30,86400)][int]$ReadDurationSeconds = 120,
  [ValidateRange(1,256)][int]$Workers = 32,
  [ValidateRange(1,100000)][int]$Entities = 1000,
  [switch]$AutomaticTimestamps
)
$ErrorActionPreference = 'Stop'
if (-not $env:FLAMEDB_API_KEY) { throw 'Set FLAMEDB_API_KEY for this isolated development Compose deployment.' }
if ($ApiContainers.Count -ne 3 -or $MetricsUrls.Count -ne 3 -or $Addresses.Split(',').Count -ne 3) { throw 'This check requires exactly three API nodes.' }
$binary = (Resolve-Path -LiteralPath $LoadtestBinary).Path
$rootPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
Set-Location -LiteralPath $rootPath
$runId = [guid]::NewGuid().ToString('N').Substring(0,12)
$outputPath = [IO.Path]::GetFullPath((Join-Path $OutputDirectory $runId))
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$dockerExecutable = (Get-Command docker.exe -CommandType Application).Source
function InvokeDocker([string[]]$Arguments) {
  $result = & $dockerExecutable @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) { throw "Docker failed: $($Arguments -join ' '): $($result -join "`n")" }
  return ($result -join "`n")
}
# Save exact IDs and verify the named development deployment before any outage.
# A different project, replacement container or production deployment is rejected.
$ids = @{}
$identities = @()
foreach ($name in @($PostgresContainer,$RedisContainer) + $ApiContainers) {
  $info = (InvokeDocker @('inspect',$name) | ConvertFrom-Json)[0]
  if ($name -notlike 'flamedb-nextgen-*' -or $info.Config.Labels.'com.docker.compose.project' -ne $ComposeProject -or -not $info.State.Running) { throw "Not a running FlameDB development Compose fixture: $name" }
  $ids[$name] = $info.Id
  $identities += @{name=$name;id=$info.Id;image=$info.Image;service=$info.Config.Labels.'com.docker.compose.service'}
}
if (($identities | Where-Object { $_.name -in $ApiContainers } | ForEach-Object { $_.image } | Sort-Object -Unique).Count -ne 1) { throw 'API nodes run different images.' }
function AssertIdentity([string]$Name) {
  $info = (InvokeDocker @('inspect',$ids[$Name]) | ConvertFrom-Json)[0]
  if ($info.Name.TrimStart('/') -ne $Name -or $info.Config.Labels.'com.docker.compose.project' -ne $ComposeProject) { throw "Fixture ownership changed: $Name" }
}
function WaitReady {
  $clock = [Diagnostics.Stopwatch]::StartNew()
  do {
    $ready = 0
    foreach ($url in $MetricsUrls) {
      try { if ((Invoke-WebRequest -Uri ($url -replace '/metrics$','/readyz') -TimeoutSec 2).StatusCode -eq 200) { $ready++ } } catch {}
    }
    if ($ready -eq 3) { return }
    Start-Sleep -Milliseconds 500
  } while ($clock.Elapsed.TotalSeconds -lt 120)
  throw 'Three APIs did not become ready after recovery.'
}
$source = @(Get-ChildItem -LiteralPath 'cmd','internal' -Recurse -File | Where-Object { $_.Extension -in '.go','.sql' } | Sort-Object FullName | ForEach-Object {
  @{path=[IO.Path]::GetRelativePath($rootPath,$_.FullName);sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}
})
$postgresSettings=InvokeDocker @('exec',$ids[$PostgresContainer],'psql','-U','flamedb','-d','flamedb','-At','-c',"SELECT json_object_agg(name,json_build_object('setting',setting,'unit',unit,'pending_restart',pending_restart)) FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit','max_wal_size','checkpoint_timeout','wal_compression','shared_buffers');") | ConvertFrom-Json
foreach ($setting in @('fsync','full_page_writes','synchronous_commit')) { if ($postgresSettings.$setting.setting -ne 'on') { throw "Unsafe durability configuration: $setting" } }
$configs=@('compose.nextgen.yml','deploy/nextgen.yml' | ForEach-Object { @{path=$_;sha256=(Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash} })
@{run_id=$runId;addresses=$Addresses;duration_seconds=$DurationSeconds;read_duration_seconds=$ReadDurationSeconds;workers=$Workers;metrics=16;entities=$Entities;automatic_timestamps=$AutomaticTimestamps.IsPresent;batch_size=100;containers=$identities;loadtest_sha256=(Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash;source=$source;configs=$configs;postgres_settings=$postgresSettings;docker_environment=(InvokeDocker @('info','--format','cpus={{.NCPU}} memory_bytes={{.MemTotal}} server={{.ServerVersion}}'))} |
  ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $outputPath 'environment.json')
$redisStopped = $false
$postgresStopped = $false
$process = $null
$faults = [Collections.Generic.List[object]]::new()
function RunWorkload([string]$Mode,[int]$Seconds,[bool]$Outage) {
  $verification = Join-Path $outputPath "$Mode.verify.json"
  $batchSize = if ($Mode -eq 'mixed') { '100' } else { '1' }
  $arguments = @('--addrs',$Addresses,'--mode',$Mode,'--duration',"${Seconds}s",'--workers',"$Workers",'--metrics','16','--entities',"$Entities",'--fixture-events','1000','--batch-size',$batchSize,'--pipeline','1','--durability-contract','postgres-sync','--prefix',"soak$runId.$Mode",'--verification-file',$verification,('--automatic-timestamps='+$AutomaticTimestamps.IsPresent.ToString().ToLowerInvariant()))
  $quoted = @($arguments | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } })
  $script:process = Start-Process -FilePath $binary -ArgumentList $quoted -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath "$Mode.json") -RedirectStandardError (Join-Path $outputPath "$Mode.stderr.txt")
  $clock = [Diagnostics.Stopwatch]::StartNew()
  $nextSample = 0
  $faultInjected = $false
  do {
    $elapsed = $clock.Elapsed.TotalSeconds
    if ($Outage -and -not $faultInjected -and $elapsed -ge 30) {
      AssertIdentity $RedisContainer
      InvokeDocker @('stop','--time','1',$ids[$RedisContainer]) | Out-Null
      $script:redisStopped = $true; $faultInjected = $true
      $faults.Add(@{fault='redis-stop';workload=$Mode;elapsed_seconds=$clock.Elapsed.TotalSeconds})
    }
    if ($Outage -and $script:redisStopped -and $elapsed -ge 50) {
      InvokeDocker @('start',$ids[$RedisContainer]) | Out-Null
      $script:redisStopped = $false
      $faults.Add(@{fault='redis-start';workload=$Mode;elapsed_seconds=$clock.Elapsed.TotalSeconds})
    }
    if ($elapsed -ge $nextSample) {
      $sample = @{elapsed_seconds=$clock.Elapsed.TotalSeconds;docker=@();metrics=@()}
      $rows = InvokeDocker (@('stats','--no-stream','--format','{{json .}}') + @($PostgresContainer,$RedisContainer) + $ApiContainers)
      $sample.docker = @($rows.Split("`n") | Where-Object { $_ } | ForEach-Object { $_ | ConvertFrom-Json })
      $sample.postgres_activity = InvokeDocker @('exec',$ids[$PostgresContainer],'psql','-U','flamedb','-d','flamedb','-Atc',"SELECT json_build_object('state',state,'wait_event_type',wait_event_type,'sessions',count(*)) FROM pg_stat_activity WHERE datname='flamedb' GROUP BY state,wait_event_type;")
      foreach ($url in $MetricsUrls) {
        try { $sample.metrics += @{url=$url;text=(Invoke-WebRequest -Uri $url -TimeoutSec 2).Content} } catch { $sample.metrics += @{url=$url;error=$_.Exception.Message} }
      }
      $sample | ConvertTo-Json -Depth 10 -Compress | Add-Content -LiteralPath (Join-Path $outputPath "$Mode.resources.jsonl")
      Write-Output "SOAK_PROGRESS mode=$Mode elapsed=$([int]$clock.Elapsed.TotalSeconds)s"
      $nextSample += 10
    }
    $script:process.Refresh()
    if (-not $script:process.HasExited) { Start-Sleep -Milliseconds 250 }
  } while (-not $script:process.HasExited)
  $script:process.WaitForExit()
  if ($script:process.ExitCode -ne 0) { throw "Workload $Mode failed: $(Get-Content -LiteralPath (Join-Path $outputPath "$Mode.stderr.txt") -Raw)" }
  $report = Get-Content -LiteralPath (Join-Path $outputPath "$Mode.json") -Raw | ConvertFrom-Json
  if (-not $report.correctness_pass -or -not $report.all_entity_scores_verified -or $report.errors -ne 0 -or $report.events_ambiguous -ne 0 -or $report.requests_confirmed -le 0 -or $report.reads_confirmed -le 0 -or ($Mode -eq 'mixed' -and $report.events_confirmed -le 0) -or $report.elapsed_seconds_including_drain -lt $Seconds) { throw "Incomplete workload observations for $Mode" }
  if ($Outage -and (-not $faultInjected -or $script:redisStopped)) { throw 'Redis outage/recovery was not observed.' }
  return $report
}
try {
  WaitReady
  $mixed = @(RunWorkload 'mixed' $DurationSeconds $true)[-1]
  # Stop admission after all responses have drained, then crash the exact
  # primary. All recorded confirmations must survive recovery without replay.
  foreach ($name in $ApiContainers) { AssertIdentity $name; InvokeDocker @('stop','--time','35',$ids[$name]) | Out-Null }
  AssertIdentity $PostgresContainer
  InvokeDocker @('kill','--signal','KILL',$ids[$PostgresContainer]) | Out-Null
  $postgresStopped = $true
  $faults.Add(@{fault='postgres-kill-after-drain';events_confirmed=$mixed.events_confirmed})
  InvokeDocker @('start',$ids[$PostgresContainer]) | Out-Null
  $postgresStopped = $false
  foreach ($name in $ApiContainers) { InvokeDocker @('start',$ids[$name]) | Out-Null }
  WaitReady
  $recovery = & $binary --addrs $Addresses --verify-file (Join-Path $outputPath 'mixed.verify.json')
  if ($LASTEXITCODE -ne 0) { throw 'Acknowledged data did not survive the database/API restart.' }
  $recovery | Set-Content -LiteralPath (Join-Path $outputPath 'recovery.json')
  $verified = $recovery | ConvertFrom-Json
  if (-not $verified.recovery_verified -or $verified.readback_events -ne $mixed.events_confirmed) { throw 'Recovery count differs from durable confirmations.' }
  $rank = @(RunWorkload 'rank' $ReadDurationSeconds $false)[-1]
  @{mixed=$mixed;rank=$rank;recovery=$verified;faults=@($faults);sentinel_default_timing_tested=$false;production_capacity_proven=$false} | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $outputPath 'summary.json')
  @{run_path=$outputPath} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path ([IO.Path]::GetFullPath($OutputDirectory)) 'latest.json')
  Write-Output "SOAK_ARTIFACTS=$outputPath"
  Write-Output 'SUSTAINED_MULTI_API_REDIS_RECOVERY_VERIFIED'
} finally {
  $faults | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outputPath 'faults.json')
  if ($process -and -not $process.HasExited) { Stop-Process -Id $process.Id -ErrorAction SilentlyContinue }
  if ($redisStopped) { InvokeDocker @('start',$ids[$RedisContainer]) | Out-Null }
  if ($postgresStopped) { InvokeDocker @('start',$ids[$PostgresContainer]) | Out-Null }
  foreach ($name in $ApiContainers) {
    $info = (InvokeDocker @('inspect',$ids[$name]) | ConvertFrom-Json)[0]
    if (-not $info.State.Running) { InvokeDocker @('start',$ids[$name]) | Out-Null }
  }
}
