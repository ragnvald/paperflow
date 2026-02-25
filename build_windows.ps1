# Build wrapper for Windows packaging via PyInstaller.
$ErrorActionPreference = "Stop"

param(
    [switch]$OneFile
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venv = Join-Path $root ".venv"
if (-not (Test-Path $venv)) {
    python -m venv $venv
}

$python = Join-Path $venv "Scripts\\python.exe"
$pip = Join-Path $venv "Scripts\\pip.exe"

& $pip install --upgrade pip
& $pip install -r requirements.txt
& $pip install pyinstaller

if (Test-Path "build") {
    Remove-Item -Recurse -Force "build"
}
if (Test-Path "dist") {
    Remove-Item -Recurse -Force "dist"
}

$pyinstallerArgs = @(
    "--noconfirm",
    "--clean",
    "--windowed",
    "--name", "ocr_tracking_dashboard",
    "--collect-all", "ttkbootstrap",
    "--add-data", "secrets\\paperlesstoken.api.template;secrets",
    "ocr_tracking_dashboard.py"
)

if ($OneFile) {
    $pyinstallerArgs += "--onefile"
}

& $python -m PyInstaller @pyinstallerArgs

if ($OneFile) {
    Write-Host "Build complete. Output: dist\\ocr_tracking_dashboard.exe"
} else {
    Write-Host "Build complete. Output: dist\\ocr_tracking_dashboard\\ocr_tracking_dashboard.exe"
}
