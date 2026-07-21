@echo off
rem Double-click to continue the last Claude session in this project folder
cd /d "%~dp0"
claude --continue
if errorlevel 1 (
    echo.
    echo Could not resume last session. Showing session list...
    claude --resume
)
pause
