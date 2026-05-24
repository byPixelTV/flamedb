@(
    "config.yml",
    "config2.yml",
    "config3.yml",
    "config4.yml"
) | ForEach-Object {
    Start-Process ".\flamedb.exe" -ArgumentList $_
}