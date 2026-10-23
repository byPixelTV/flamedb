# Download a portable official Grafana release into the ignored local runtime.
$ErrorActionPreference = 'Stop'
$pluginRoot = Split-Path $PSScriptRoot -Parent
$runtimeRoot = Join-Path $pluginRoot '.runtime'
$grafanaRoot = Join-Path $runtimeRoot 'grafana-13.2.4'
if (Test-Path -LiteralPath (Join-Path $grafanaRoot 'bin/grafana.exe')) {
    Write-Host "Grafana already available: $grafanaRoot"
    exit 0
}
New-Item -ItemType Directory -Path $runtimeRoot -Force | Out-Null
$archivePath = Join-Path $runtimeRoot 'grafana.tar.gz'
$releaseUrl = 'https://dl.grafana.com/grafana/release/13.2.4/grafana_13.2.4_38252365904_windows_amd64.tar.gz'
& curl.exe --fail --location --retry 3 $releaseUrl -o $archivePath
if ($LASTEXITCODE -ne 0) { throw 'Grafana download failed' }
$expectedHash = '3e005ff50fe3bb6b8cb428ef127636fac4ee676bf9b6d819c6a2297d21f4bcbd'
if ((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expectedHash) {
    throw 'Grafana SHA256 mismatch'
}
# Resolve the physical directory; Windows tar refuses some directory junctions.
$physicalRuntime = (& node -e 'console.log(require("node:fs").realpathSync(process.argv[1]))' $runtimeRoot).Trim()
& tar.exe -xzf $archivePath -C $physicalRuntime
if ($LASTEXITCODE -ne 0) { throw 'Grafana extraction failed' }
Write-Host "Grafana ready: $grafanaRoot"
