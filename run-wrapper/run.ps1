# https://chatgpt.com/c/681762a4-dddc-800a-adad-2797355013f8

$ErrorActionPreference = 'Stop'

$PIDFILE = Join-Path $PSScriptRoot ".pid"
$EXTRAOPTS = Join-Path $PSScriptRoot "extra-opts"

if (-Not (Test-Path $EXTRAOPTS)) {
    New-Item -ItemType File -Path $EXTRAOPTS | Out-Null
}

if (Test-Path $PIDFILE) {
    $ReadPID = Get-Content $PIDFILE
    if (Get-Process -Id $ReadPID -ErrorAction SilentlyContinue) {
        Write-Host "Already running: $ReadPID"
        exit 2
    }
}

$ExtraOptsParsed = Get-Content $EXTRAOPTS | Where-Object {$_}

Write-Host "Extra options: $($ExtraOptsParsed -join ' ')"

$JAVA_OPTS = @(
    "-Xmx512M"
    "--enable-preview"
    "-Ddhfs.objects.writeback.limit=16777216"
    "-Ddhfs.objects.lru.limit=67108864"
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"
    "--add-exports", "java.base/jdk.internal.access=ALL-UNNAMED"
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
    "-Ddhfs.objects.persistence.files.root=$($PSScriptRoot)\..\data\objects"
    "-Ddhfs.objects.persistence.stuff.root=$($PSScriptRoot)\..\data\stuff"
    "-Ddhfs.objects.persistence.lmdb.size=1000000000"
    "-Ddhfs.fuse.root=Z:\"
    "-Dquarkus.http.host=0.0.0.0"
    '-Dquarkus.log.category.\"com.usatiuk\".level=INFO'
    '-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=INFO'
    "-Ddhfs.webui.root=$($PSScriptRoot)\Webui"
) + $ExtraOptsParsed + @(
    "-jar", "`"$PSScriptRoot\Server\quarkus-run.jar`""
)

$Process = Start-Process -FilePath "java" -ArgumentList $JAVA_OPTS `
    -RedirectStandardOutput "$PSScriptRoot\quarkus.log" `
    -RedirectStandardError "$PSScriptRoot\quarkus.log.err" `
    -NoNewWindow -PassThru

Write-Host "Started $($Process.Id)"
$Process.Id | Out-File -FilePath $PIDFILE
