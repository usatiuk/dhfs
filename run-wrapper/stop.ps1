$ErrorActionPreference = 'Stop'

$PIDFILE = Join-Path $PSScriptRoot ".pid"

if (-Not (Test-Path $PIDFILE)) {
    Write-Host "Not running"
    exit 2
}

$ReadPID = Get-Content $PIDFILE

if (-Not (Get-Process -Id $ReadPID -ErrorAction SilentlyContinue)) {
    Write-Host "Not running"
    Remove-Item $PIDFILE -Force
    exit 2
}

Write-Host "Killing $ReadPID"

# TODO: Graceful shutdown

Stop-Process -Id $ReadPID

Remove-Item $PIDFILE -Force
