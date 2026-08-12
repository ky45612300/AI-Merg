# Start API Pool service (hidden background)
# Usage: .\start_service.ps1 [-Port 5100]

[CmdletBinding()]
param(
    [ValidateRange(1, 65535)]
    [int]$Port = 5100
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $Root "api-pool.pid"
$LogDir = Join-Path $Root "logs"
$OutLog = Join-Path $LogDir "api-pool.out.log"
$ErrLog = Join-Path $LogDir "api-pool.err.log"
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

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$record = Get-ServiceRecord
if ($record -and $record.pid) {
    $existing = Get-ProjectProcess -Id ([int]$record.pid)
    if ($existing) {
        $existingPort = if ($record.port) { [int]$record.port } else { $Port }
        Write-Host "Already running (PID $($existing.ProcessId), port $existingPort). Open http://localhost:$existingPort" -ForegroundColor Yellow
        exit 0
    }
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
}

$busy = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($busy) {
    Write-Host "Port $Port already used by PID $($busy[0].OwningProcess). Not starting." -ForegroundColor Red
    exit 1
}

if (Test-Path $BundledPy) {
    $py = $BundledPy
} else {
    $cmd = Get-Command python.exe -ErrorAction SilentlyContinue
    if (-not $cmd) { $cmd = Get-Command python3.exe -ErrorAction SilentlyContinue }
    if ($cmd) { $py = $cmd.Source }
}
if (-not $py) { Write-Host "python not found. Bundled runtime is missing and no system python was found." -ForegroundColor Red; exit 1 }

$env:PORT = $Port
$env:PYTHONIOENCODING = "utf-8"
$proc = Start-Process -FilePath $py `
    -ArgumentList "-u", "api_pool_server.py" `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError $ErrLog `
    -PassThru

@{ pid = $proc.Id; port = $Port } | ConvertTo-Json -Compress | Set-Content -LiteralPath $PidFile -Encoding ascii -NoNewline

Start-Sleep -Seconds 2
if (Get-ProjectProcess -Id $proc.Id) {
    Write-Host "[OK] API Pool started (PID $($proc.Id), port $Port)" -ForegroundColor Green
    Write-Host "     Dashboard : http://localhost:$Port" -ForegroundColor Green
    Write-Host "     Base URL  : http://localhost:$Port/v1" -ForegroundColor Green
} else {
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
    Write-Host "[FAIL] Process exited immediately. See $ErrLog" -ForegroundColor Red
    exit 1
}
