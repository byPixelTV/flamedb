[CmdletBinding()]
param(
  [string]$LoadtestBinary = '.compat/loadtest.exe',
  [string]$Address = '127.0.0.1:7777',
  [string]$ProfilerUrl = 'http://127.0.0.1:6060',
  [string]$OutputDirectory = '.compat/profile',
  [int]$Workers = 32,
  [int]$BatchSize = 100,
  [bool]$Leaderboard = $false,
  [string]$Duration = '15s',
  [int]$CpuSeconds = 10
)
$ErrorActionPreference = 'Stop'
if (-not $env:FLAMEDB_API_KEY) { throw 'Set FLAMEDB_API_KEY.' }
$binaryPath = (Resolve-Path -LiteralPath $LoadtestBinary).Path
$outputPath = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$stdout = Join-Path $outputPath 'workload.json'
$stderr = Join-Path $outputPath 'workload.stderr.txt'
$cpu = Join-Path $outputPath 'cpu.pprof'
$heap = Join-Path $outputPath 'heap.pprof'
$arguments = @('--addrs',$Address,'--workers',$Workers,'--duration',$Duration,'--batch-size',$BatchSize,
  '--metrics','64','--mode','write',('--leaderboard=' + $Leaderboard.ToString().ToLowerInvariant()),'--durability-contract','postgres-sync')
@{address=$Address;profiler_url=$ProfilerUrl;workers=$Workers;batch_size=$BatchSize;leaderboard=$Leaderboard;duration=$Duration;cpu_seconds=$CpuSeconds;started_utc=[DateTime]::UtcNow.ToString('o');loadtest_sha256=(Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash;allocation_profile_scope='cumulative since server process start'} |
  ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outputPath 'profile.json')
$process = Start-Process -FilePath $binaryPath -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
try {
  Start-Sleep -Milliseconds 500
  Invoke-WebRequest -Uri ($ProfilerUrl + '/debug/pprof/profile?seconds=' + $CpuSeconds) -OutFile $cpu -TimeoutSec ($CpuSeconds + 10)
  Invoke-WebRequest -Uri ($ProfilerUrl + '/debug/pprof/heap') -OutFile $heap -TimeoutSec 5
  $process.WaitForExit()
  & go tool pprof -top -nodecount=25 $cpu | Set-Content -LiteralPath (Join-Path $outputPath 'cpu-top.txt')
  if ($LASTEXITCODE -ne 0) { throw 'CPU profile inspection failed.' }
  & go tool pprof -top -nodecount=25 $heap | Set-Content -LiteralPath (Join-Path $outputPath 'heap-top.txt')
  if ($LASTEXITCODE -ne 0) { throw 'Heap profile inspection failed.' }
  & go tool pprof -top -sample_index=alloc_space -nodecount=25 $heap | Set-Content -LiteralPath (Join-Path $outputPath 'alloc-top.txt')
  if ($LASTEXITCODE -ne 0) { throw 'Allocation profile inspection failed.' }
  if ($process.ExitCode -ne 0) { throw 'Profile workload/readback failed; inspect output.' }
} finally {
  $process.Refresh()
  if (-not $process.HasExited) { Stop-Process -Id $process.Id }
}
