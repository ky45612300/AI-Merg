@echo off
setlocal
set "ROOT=%~dp0"
set "PS=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
set "SCRIPT=%ROOT%restart_service.ps1"

if not exist "%PS%" (
  echo Windows PowerShell not found: %PS%
  pause
  exit /b 1
)

if not exist "%SCRIPT%" (
  echo Restart script not found: %SCRIPT%
  pause
  exit /b 1
)

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%"
echo.
pause
