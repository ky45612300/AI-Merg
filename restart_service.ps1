# Restart API Pool service
# Usage: .\restart_service.ps1 [-Port 5100]

[CmdletBinding()]
param(
    [ValidateRange(1, 65535)]
    [int]$Port = 5100
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$StopScript = Join-Path $Root "stop_service.ps1"
$StartScript = Join-Path $Root "start_service.ps1"

Write-Host "Restarting API Pool service..." -ForegroundColor Cyan

# Stop if running
& $StopScript
Start-Sleep -Seconds 1

# Start
& $StartScript -Port $Port
