# Show API Pool service status
# Usage: run  .\status_service.ps1

$ErrorActionPreference = "SilentlyContinue"
$Root    = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $Root "api-pool.pid"
$Port    = if ($env:PORT) { $env:PORT } else { "5100" }

$running = $false

if (Test-Path $PidFile) {
    $spid = (Get-Content $PidFile -Raw).Trim()
    $proc = Get-Process -Id $spid -ErrorAction SilentlyContinue
    if ($proc -and $proc.ProcessName -like "python*") {
        Write-Host "[RUNNING] PID $spid" -ForegroundColor Green
        $running = $true
    }
}

$conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($conns) {
    Write-Host "[LISTENING] Port $Port (PID $($conns[0].OwningProcess))" -ForegroundColor Green
    Write-Host "            Dashboard: http://localhost:$Port" -ForegroundColor Cyan
    $running = $true
} else {
    Write-Host "[DOWN] Port $Port not listening" -ForegroundColor DarkGray
}

if (-not $running) { Write-Host "Service not running." -ForegroundColor Yellow }
