[CmdletBinding()]
param(
  [string]$ReferenceBinary = $env:FLAMEDB_REFERENCE_BINARY,
  [string]$PebbleBinary = $env:FLAMEDB_PEBBLE_BINARY
)
$ErrorActionPreference = 'Stop'
foreach ($value in @($ReferenceBinary,$PebbleBinary)) {
  if ([string]::IsNullOrWhiteSpace($value)) { throw 'Set both FLAMEDB_*_BINARY variables.' }
}
$env:FLAMEDB_INTEGRATION = '1'
$env:FLAMEDB_REFERENCE_BINARY = (Resolve-Path -LiteralPath $ReferenceBinary).Path
$env:FLAMEDB_PEBBLE_BINARY = (Resolve-Path -LiteralPath $PebbleBinary).Path
$output = & go test ./internal/compat -run '^(TestFrozenReferenceDifferentialTCP)$' -count=1 -v 2>&1
$exitCode = $LASTEXITCODE
$output | Write-Output
if ($exitCode -ne 0) { throw 'Strict differential checks failed.' }
if (($output -join "`n") -match '--- SKIP:') { throw 'A required compatibility check skipped.' }
foreach ($test in @('TestFrozenReferenceDifferentialTCP')) {
  if (($output -join "`n") -notmatch ('(?m)^--- PASS: ' + [regex]::Escape($test) + ' \(')) {
    throw "Required test did not execute successfully: $test"
  }
}
Write-Output 'STRICT_COMPAT_OK'
