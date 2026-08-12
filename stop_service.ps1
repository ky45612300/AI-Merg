# Stop API Pool service
# Usage: .\stop_service.ps1

$ErrorActionPreference = "SilentlyContinue"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $Root "api-pool.pid"
$BundledPy = Join-Path $Root ".runtime\python-3.12.5-embed-amd64\python.exe"

function Get-ServiceRecord {
    if (-not (Test-Path $PidFile)) { return $null }
    $raw = (Get-Content -LiteralPath $PidFile -Raw).Trim()
    if ($raw -match '^\d+$') { return [pscustomobject]@{ pid = [int]$raw; port = $null } }
    try { return $raw | ConvertFrom-Json } catch { return $null }
}

function Get-ProjectProcess([int]$Id) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$Id" -ErrorAction SilentlyContinue
    if (-not $proc) { return $null }
    if ($proc.Name -notlike "python*") { return $null }
    if ($proc.CommandLine -notmatch '(^|\s)api_pool_server\.py(\s|$)') { return $null }
    if ($proc.ExecutablePath -ne $BundledPy) { return $null }
    return $proc
}

$record = Get-ServiceRecord
if (-not $record -or -not $record.pid) {
    Write-Host "No running API Pool service record found." -ForegroundColor Yellow
    exit 0
}

$proc = Get-ProjectProcess -Id ([int]$record.pid)
if (-not $proc) {
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
    Write-Host "Removed stale API Pool service record for PID $($record.pid)." -ForegroundColor Yellow
    exit 0
}

Stop-Process -Id $proc.ProcessId -Force
Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
Write-Host "Stopped API Pool service (PID $($proc.ProcessId), port $($record.port))." -ForegroundColor Green
