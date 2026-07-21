@echo off
setlocal
set "ROOT=%~dp0"
set "PS=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
set "SCRIPT=%ROOT%start_service.ps1"

if not exist "%PS%" exit /b 1
if not exist "%SCRIPT%" exit /b 1

REM Fully silent: no console, no pause. Service runs in background via start_service.ps1.
start "" "%PS%" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "%SCRIPT%"
exit /b 0
