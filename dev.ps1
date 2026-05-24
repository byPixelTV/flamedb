param(
    [int]$Count = 4
)

$configs = @(
    "config.yml",
    "config2.yml",
    "config3.yml",
    "config4.yml"
)

if ($Count -lt 1 -or $Count -gt $configs.Count) {
    Write-Host "Usage: .\start.ps1 [1-$($configs.Count)]"
    exit 1
}

$processes = @()

$configs[0..($Count - 1)] | ForEach-Object {
    $processes += Start-Process ".\flamedb.exe" -ArgumentList $_ -PassThru -NoNewWindow
}

try {
    Wait-Process -Id $processes.Id
} finally {
    $processes | ForEach-Object {
        if (!$_.HasExited) {
            Stop-Process -Id $_.Id -Force
        }
    }
}