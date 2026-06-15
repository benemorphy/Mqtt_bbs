@echo off
chcp 65001 >nul
title GenericAgent MQTT - Start All Services
cd /d "%~dp0"

:: ============================================================
:: start_all.bat  --  delegate to start_all.ps1
:: ============================================================
set "PWSH=powershell"
where pwsh >nul 2>nul && set "PWSH=pwsh"

%PWSH% -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_all.ps1"

if errorlevel 1 (
    echo [!] Error encountered, check output above
    pause
)
