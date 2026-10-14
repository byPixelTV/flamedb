[CmdletBinding()]
param(
  [string]$ReferenceBinary = $env:FLAMEDB_REFERENCE_BINARY,
  [string]$NextgenBinary = $env:FLAMEDB_NEXTGEN_BINARY,
  [string]$PostgresDsn = $env:FLAMEDB_TEST_POSTGRES,
  [string]$RedisAddress = $env:FLAMEDB_TEST_REDIS
)
$ErrorActionPreference = 'Stop'
foreach ($value in @($ReferenceBinary,$NextgenBinary,$PostgresDsn,$RedisAddress)) {
  if ([string]::IsNullOrWhiteSpace($value)) { throw 'Set both FLAMEDB_*_BINARY variables, FLAMEDB_TEST_POSTGRES and FLAMEDB_TEST_REDIS.' }
}
$env:FLAMEDB_INTEGRATION = '1'
$env:FLAMEDB_REFERENCE_BINARY = (Resolve-Path -LiteralPath $ReferenceBinary).Path
$env:FLAMEDB_NEXTGEN_BINARY = (Resolve-Path -LiteralPath $NextgenBinary).Path
$env:FLAMEDB_TEST_POSTGRES = $PostgresDsn
$env:FLAMEDB_TEST_REDIS = $RedisAddress
# SDK consumers have their own integration gate. These tests exercise the full
# differential corpus and abrupt process recovery with three real API nodes.
$output = & go test ./internal/compat -run '^(TestFrozenReferenceDifferentialTCP|TestSharedNodesConcurrentDurabilityAndRecovery)$' -count=1 -v 2>&1
$exitCode = $LASTEXITCODE
$output | Write-Output
if ($exitCode -ne 0) { throw 'Strict differential/recovery checks failed.' }
if (($output -join "`n") -match '--- SKIP:') { throw 'A required compatibility check skipped.' }
foreach ($test in @('TestFrozenReferenceDifferentialTCP','TestSharedNodesConcurrentDurabilityAndRecovery')) {
  if (($output -join "`n") -notmatch ('(?m)^--- PASS: ' + [regex]::Escape($test) + ' \(')) {
    throw "Required test did not execute successfully: $test"
  }
}
Write-Output 'STRICT_COMPAT_OK'
