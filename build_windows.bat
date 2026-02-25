@echo off
REM Build wrapper for Windows packaging via PyInstaller.
setlocal

cd /d "%~dp0"

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build_windows.ps1" %*
set exit_code=%errorlevel%

if not "%exit_code%"=="0" (
  echo Build failed with exit code %exit_code%.
)

exit /b %exit_code%
