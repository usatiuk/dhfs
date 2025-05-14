# https://chatgpt.com/c/681762a4-dddc-800a-adad-2797355013f8

$ErrorActionPreference = 'Stop'

$PIDFILE = Join-Path $PSScriptRoot ".pid"
$VERSION_FILE = Join-Path $PSScriptRoot "version"

if (Test-Path $PIDFILE) {
    $ReadPID = Get-Content $PIDFILE
    if (Get-Process -Id $ReadPID -ErrorAction SilentlyContinue) {
        Write-Host "Already running: $ReadPID"
        exit 2
    }
}

$response = Invoke-RestMethod -Uri "https://api.github.com/repos/usatiuk/dhfs/actions/runs?branch=main&status=completed&per_page=1"

$LATEST = $response.workflow_runs[0].id
Write-Host "Latest: $LATEST"

$CUR = (Get-Content $VERSION_FILE -Raw).Trim()
Write-Host "Current: $CUR"

if ([long]$CUR -ge [long]$LATEST) {
    Write-Host "Already latest!"
    exit 1
}

Write-Host "Downloading..."

Set-Location $PSScriptRoot

$zipFile = "Run wrapper.zip"
$tarFile = "run-wrapper.tar.gz"
$dhfsDir = "dhfs"

Remove-Item $zipFile, $tarFile -Force -ErrorAction SilentlyContinue
Remove-Item $dhfsDir -Recurse -Force -ErrorAction SilentlyContinue

Invoke-WebRequest -Uri "https://nightly.link/usatiuk/dhfs/actions/runs/$LATEST/Run%20wrapper.zip" -OutFile $zipFile

Expand-Archive -LiteralPath $zipFile -DestinationPath $PSScriptRoot
Remove-Item $zipFile -Force

tar -xf $tarFile --strip-components=2
Remove-Item $tarFile -Force

Remove-Item "Server", "Webui", "NativeLibs" -Recurse -Force -ErrorAction SilentlyContinue
Move-Item "$dhfsDir\app\*" . -Force
Remove-Item $dhfsDir -Recurse -Force

Write-Host "Update complete!"
