# Start API Pool service (hidden background)
# Usage: right-click "Run with PowerShell" or run  .\start_service.ps1

$ErrorActionPreference = "Stop"
$Root    = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $Root "api-pool.pid"
$LogDir  = Join-Path $Root "logs"
$OutLog  = Join-Path $LogDir "api-pool.out.log"
$ErrLog  = Join-Path $LogDir "api-pool.err.log"
$Port    = if ($env:PORT) { $env:PORT } else { "5100" }

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# 1. Already running per pid file
if (Test-Path $PidFile) {
    $oldPid = (Get-Content $PidFile -Raw).Trim()
    if ($oldPid -match '^\d+$') {
        $proc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
        if ($proc -and $proc.ProcessName -like "python*") {
            Write-Host "Already running (PID $oldPid). Open http://localhost:$Port" -ForegroundColor Yellow
            exit 0
        }
    }
}

# 2. Port in use?
$busy = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($busy) {
    Write-Host "Port $Port already used by PID $($busy.OwningProcess). Not starting." -ForegroundColor Red
    exit 1
}

# 3. Pick python. Prefer the bundled runtime so double-click startup does
#    not depend on the user's PATH or the Microsoft Store python shim.
$BundledPy = Join-Path $Root ".runtime\python-3.12.5-embed-amd64\python.exe"
if (Test-Path $BundledPy) {
    $py = $BundledPy
} else {
    $cmd = Get-Command python.exe -ErrorAction SilentlyContinue
    if (-not $cmd) { $cmd = Get-Command python3.exe -ErrorAction SilentlyContinue }
    if ($cmd) { $py = $cmd.Source }
}
if (-not $py) { Write-Host "python not found. Bundled runtime is missing and no system python was found." -ForegroundColor Red; exit 1 }

# 4. Start hidden, redirect logs
$env:PORT = $Port
$env:PYTHONIOENCODING = "utf-8"
$proc = Start-Process -FilePath $py `
    -ArgumentList "-u", "api_pool_server.py" `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError  $ErrLog `
    -PassThru

$proc.Id | Out-File -FilePath $PidFile -Encoding ascii -NoNewline

Start-Sleep -Seconds 2
if (Get-Process -Id $proc.Id -ErrorAction SilentlyContinue) {
    Write-Host "[OK] API Pool started (PID $($proc.Id))" -ForegroundColor Green
    Write-Host "     Dashboard : http://localhost:$Port" -ForegroundColor Green
    Write-Host "     Base URL  : http://localhost:$Port/v1" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Process exited immediately. See $ErrLog" -ForegroundColor Red
    exit 1
}
