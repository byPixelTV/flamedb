$processes = @()

@(
    "config.yml",
    "config2.yml",
    "config3.yml",
    "config4.yml"
) | ForEach-Object {
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