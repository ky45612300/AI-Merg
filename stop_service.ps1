# Stop API Pool service
# Usage: run  .\stop_service.ps1

$ErrorActionPreference = "SilentlyContinue"
$Root    = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $Root "api-pool.pid"
$Port    = if ($env:PORT) { $env:PORT } else { "5200" }

$stopped = $false

# 1. Stop by pid file
if (Test-Path $PidFile) {
    $spid = (Get-Content $PidFile -Raw).Trim()
    if ($spid -match '^\d+$') {
        $proc = Get-Process -Id $spid -ErrorAction SilentlyContinue
        if ($proc -and $proc.ProcessName -like "python*") {
            Stop-Process -Id $spid -Force
            Write-Host "Stopped service (PID $spid)" -ForegroundColor Green
            $stopped = $true
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

# 2. Fallback: by listening port
$conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
foreach ($c in $conns) {
    $p = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
    if ($p -and $p.ProcessName -like "python*") {
        Stop-Process -Id $p.Id -Force
        Write-Host "Stopped process on port $Port (PID $($p.Id))" -ForegroundColor Green
        $stopped = $true
    }
}

if (-not $stopped) { Write-Host "No running API Pool service found." -ForegroundColor Yellow }
