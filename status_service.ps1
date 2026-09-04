# Show API Pool service status
# Usage: .\status_service.ps1

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
    return $proc
}

$record = Get-ServiceRecord
if (-not $record -or -not $record.pid) {
    Write-Host "[DOWN] No API Pool service record found." -ForegroundColor DarkGray
    exit 0
}

$proc = Get-ProjectProcess -Id ([int]$record.pid)
if (-not $proc) {
    Write-Host "[DOWN] Stale service record for PID $($record.pid)." -ForegroundColor DarkGray
    exit 1
}

$port = if ($record.port) { [int]$record.port } else { 5100 }
$conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
    Where-Object { $_.OwningProcess -eq $proc.ProcessId } |
    Select-Object -First 1

if (-not $conn) {
    Write-Host "[STARTING] API Pool PID $($proc.ProcessId) has not bound port $port yet." -ForegroundColor Yellow
    exit 1
}

Write-Host "[RUNNING] API Pool PID $($proc.ProcessId)" -ForegroundColor Green
Write-Host "[LISTENING] Port $port (PID $($conn.OwningProcess))" -ForegroundColor Green
Write-Host "            Dashboard: http://localhost:$port" -ForegroundColor Cyan
