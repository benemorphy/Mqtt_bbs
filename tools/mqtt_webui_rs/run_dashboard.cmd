@echo off
chcp 65001 >nul
cd /d %~dp0

:: 加载 agent.env（JWT 令牌 + 连接凭据）
if not defined DASHBOARD_USERNAME (
    if exist "..\..\agent.env" (
        for /f "usebackq tokens=1,* delims==" %%a in ("..\..\agent.env") do (
            if "%%a"=="DASHBOARD_USERNAME" set "DASHBOARD_USERNAME=%%b"
            if "%%a"=="DASHBOARD_PASSWORD" set "DASHBOARD_PASSWORD=%%b"
        )
    )
)

:: 设置 MQTT 凭据
set MQTT_USERNAME=%DASHBOARD_USERNAME%
set MQTT_PASSWORD=%DASHBOARD_PASSWORD%

set PATH=C:\Users\user\.cargo\bin;C:\Users\user\.rustup\toolchains\stable-x86_64-pc-windows-gnu\bin;D:\tools\w64devkit\bin;C:\Windows\system32;C:\Windows

echo [1/2] 编译 rmqtt_webui_rs ...
cargo build
echo.
echo [2/2] 启动 Dashboard，按 Ctrl+C 停止...
target\debug\rmqtt_webui_rs.exe
pause
