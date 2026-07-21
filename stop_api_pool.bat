@echo off
setlocal
set "ROOT=%~dp0"
set "PS=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%PS%" (
  echo Windows PowerShell not found: %PS%
  pause
  exit /b 1
)

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%ROOT%stop_service.ps1"
echo.
pause
