# Mqtt_bbs - 一键启动中间件全部服务
param([switch]$NoDashboard)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root
$ErrorActionPreference = 'SilentlyContinue'

# ═══════════════════════════════════════════
# 加载 agent.env (JWT 令牌 + 连接凭据)
# ═══════════════════════════════════════════
$envFile = Join-Path $root 'agent.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
        }
    }
    Write-Host '[OK] agent.env loaded' -Fore Green
    # Dashboard 凭据
    if ($env:DASHBOARD_USERNAME) { $env:MQTT_USERNAME = $env:DASHBOARD_USERNAME }
    if ($env:DASHBOARD_PASSWORD) { $env:MQTT_PASSWORD = $env:DASHBOARD_PASSWORD }
    Write-Host '[OK] MQTT credentials set' -Fore Green
    # 映射 DB 密码
    if (-not $env:DB_PASSWORD -and $env:mariadb_password) { $env:DB_PASSWORD = $env:mariadb_password }
} else {
    Write-Host '[!] agent.env not found (JWT tokens missing)' -Fore Yellow
}

# ═══════════════════════════════════════════
# 1. MariaDB (:3306)
# ═══════════════════════════════════════════
$svc = Get-Service -Name MariaDB -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -eq 'Running') {
    Write-Host '[OK] MariaDB (3306) 已在运行' -Fore Green
} else {
    try { Start-Service MariaDB -ErrorAction Stop; Write-Host '[OK] MariaDB 已启动' -Fore Green }
    catch { Write-Host '[!] MariaDB 未启动' -Fore Red }
}

# ═══════════════════════════════════════════
# 2. Mosquitto (:1883)
# ═══════════════════════════════════════════
$mq = Get-Process mosquitto -ErrorAction SilentlyContinue
if ($mq) {
    Write-Host "[OK] Mosquitto (1883) PID=$($mq.Id) 已在运行" -Fore Green
} else {
    $mosquittoExe = Join-Path $root 'tools\mosquitto\mosquitto.exe'
    $mosquittoConf = Join-Path $root 'tools\mosquitto\mosquitto.conf'
    if (Test-Path $mosquittoExe) {
        Start-Process $mosquittoExe -ArgumentList "-c $mosquittoConf" -WindowStyle Hidden
        Start-Sleep 3
        Write-Host '[OK] Mosquitto 已启动 (1883)' -Fore Green
    } else {
        # fallback: system mosquitto
        try {
            Start-Process 'D:\tools\mosquitto\mosquitto.exe' -ArgumentList '-c D:\tools\mosquitto\mosquitto.conf' -WindowStyle Hidden
            Start-Sleep 3
            Write-Host '[OK] Mosquitto 已启动 (1883) [system fallback]' -Fore Green
        } catch {
            Write-Host '[!] Mosquitto 未找到，请安装' -Fore Red
        }
    }
}

# ═══════════════════════════════════════════
# 3. simphtml_rs (:8901) — HTML 简化服务
# ═══════════════════════════════════════════
$p = Get-NetTCPConnection -LocalPort 8901 -ErrorAction SilentlyContinue
if ($p) { Write-Host '[OK] simphtml_rs (8901) 已在运行' -Fore Green }
else {
    $exe = Join-Path $root 'tools\simphtml_rs\target\release\simphtml_rs.exe'
    if (-not (Test-Path $exe)) { $exe = Join-Path $root 'tools\simphtml_rs\target\debug\simphtml_rs.exe' }
    if (Test-Path $exe) {
        Start-Process $exe -ArgumentList '--serve --port 8901' -WindowStyle Hidden
        Start-Sleep 2
        Write-Host '[OK] simphtml_rs 已启动 (8901)' -Fore Green
    } else { Write-Host '[!] simphtml_rs 未编译 (cargo build --release in tools/simphtml_rs)' -Fore Yellow }
}

# ═══════════════════════════════════════════
# 4. mqtt_webui_rs (:8900) — MQTT Web 仪表盘
# ═══════════════════════════════════════════
$p = Get-NetTCPConnection -LocalPort 8900 -ErrorAction SilentlyContinue
if ($p) {
    Write-Host '[..] mqtt Web UI (8900) 存在，重启...' -Fore Yellow
    $old = Get-Process -Name "mqtt_webui_rs" -ErrorAction SilentlyContinue
    if ($old) { Stop-Process -Id $old.Id -Force; Start-Sleep 2 }
}
$exe = Join-Path $root 'tools\mqtt_webui_rs\target\release\mqtt_webui_rs.exe'
if (-not (Test-Path $exe)) { $exe = Join-Path $root 'tools\mqtt_webui_rs\target\debug\mqtt_webui_rs.exe' }
if (Test-Path $exe) {
    $env:MQTT_USERNAME = $env:MQTT_USERNAME ?? 'dashboard'
    $env:MQTT_PASSWORD = $env:MQTT_PASSWORD ?? ''
    Start-Process -FilePath $exe -WindowStyle Hidden
    Start-Sleep 3
    try { $null = Get-NetTCPConnection -LocalPort 8900 -ErrorAction Stop; Write-Host '[OK] mqtt Web UI 已启动 (8900)' -Fore Green }
    catch { Write-Host '[!] mqtt Web UI 启动失败' -Fore Red }
} else { Write-Host '[!] mqtt_webui_rs 未编译 (cargo build --release in tools/mqtt_webui_rs)' -Fore Yellow }

# ═══════════════════════════════════════════
# 5. BoardService RS — BBS 核心服务
# ═══════════════════════════════════════════
$bs_exe = Join-Path $root 'tools\board_service_rs\target\release\board_service_rs.exe'
if (-not (Test-Path $bs_exe)) { $bs_exe = Join-Path $root 'tools\board_service_rs\target\debug\board_service_rs.exe' }
if (Test-Path $bs_exe) {
    $pw = [Environment]::GetEnvironmentVariable('DB_PASSWORD','Process')
    $jwt = [Environment]::GetEnvironmentVariable('JWT_SECRET','Process')
    if (-not $jwt) { $jwt = 'bbs-browser-dev-secret-change-in-production' }
    $env:MQTT_USERNAME = 'board-service-rs'
    $env:MQTT_PASSWORD = 'board-service-rs'
    Write-Host "[OK] BoardService JWT_SECRET 已就绪" -Fore Green
    Start-Process $bs_exe -ArgumentList "--db-url ""mysql://root:$pw@127.0.0.1/mqtt_bbs"" --jwt-secret ""$jwt""" -WindowStyle Hidden
    Start-Sleep 3
    try { $null = Get-NetTCPConnection -LocalPort 9100 -ErrorAction SilentlyContinue; Write-Host '[OK] BoardService RS 已启动 (:9100)' -Fore Green }
    catch { Write-Host '[!] BoardService RS 启动可能失败' -Fore Yellow }
} else {
    # fallback: Python BoardService
    Write-Host '[..] BoardService RS 未编译，尝试 Python 版本...' -Fore Yellow
    $py = Join-Path $root '.venv\Scripts\python.exe'
    if (-not (Test-Path $py)) { $py = (Get-Command python -ErrorAction SilentlyContinue).Source }
    if ($py) {
        Start-Process $py -ArgumentList '-m Mqtt_bbs_server.board_service' -WorkingDirectory $root -WindowStyle Hidden
        Start-Sleep 3
        Write-Host '[OK] BoardService (Python) 已启动' -Fore Green
    } else { Write-Host '[!] Python 未找到，BoardService 无法启动' -Fore Red }
}

# ═══════════════════════════════════════════
# 6. rmqtt_auth_rs — MQTT 认证插件
# ═══════════════════════════════════════════
$auth_exe = Join-Path $root 'tools\rmqtt_auth_rs\target\release\rmqtt_auth_rs.exe'
if (-not (Test-Path $auth_exe)) { $auth_exe = Join-Path $root 'tools\rmqtt_auth_rs\target\debug\rmqtt_auth_rs.exe' }
if (Test-Path $auth_exe) {
    $p = Get-Process -Name "rmqtt_auth_rs" -ErrorAction SilentlyContinue
    if ($p) { Write-Host '[OK] rmqtt_auth_rs 已在运行' -Fore Green }
    else {
        Start-Process $auth_exe -WindowStyle Hidden
        Start-Sleep 2
        Write-Host '[OK] rmqtt_auth_rs 已启动' -Fore Green
    }
} else { Write-Host '[!] rmqtt_auth_rs 未编译，跳过' -Fore Yellow }

# ═══════════════════════════════════════════
# 服务状态总览
# ═══════════════════════════════════════════
Write-Host ''
Write-Host '========================================' -Fore Cyan
Write-Host '  Service           Port    Status' -Fore Cyan
Write-Host '  --------           ----    ------' -Fore Cyan
@(
  @{n='Mosquitto';       p=1883;  u='mqtt://127.0.0.1:1883'},
  @{n='MariaDB';         p=3306;  u='mysql://127.0.0.1:3306'},
  @{n='simphtml_rs';    p=8901;  u='http://localhost:8901'},
  @{n='mqtt Web UI';    p=8900;  u='http://localhost:8900'},
  @{n='BoardService';   p='---'; u='MQTT BBS'; chk='board_service_rs'},
  @{n='rmqtt_auth_rs';  p='---'; u='Auth Plugin'; chk='rmqtt_auth_rs'}
) | ForEach-Object {
  $s = if ($_.chk) { try { if (Get-Process -Name $_.chk -ErrorAction Stop) { 'RUN' } else { 'OFF' } } catch { 'OFF' } } elseif ($_.p -eq '---') { '---' } else { try { $t = Get-NetTCPConnection -LocalPort $_.p -ErrorAction Stop; if ($t.State -eq 'Listen') { 'RUN' } else { '???' } } catch { 'OFF' } };
  $c = if ($s -eq 'RUN') { 'Green' } elseif ($s -eq 'OFF') { 'Red' } else { 'Yellow' };
  Write-Host ('  {0,-18} {1,5}  [{2}]' -f $_.n, ('port ' + $_.p), $s) -Fore $c;
};
Write-Host '========================================' -Fore Cyan
Write-Host ''
Write-Host '中间件全部启动完成。BoardService 管理 BBS 协议。' -Fore Cyan
Write-Host '浏览器访问 http://localhost:8900 查看仪表盘。' -Fore Cyan
Write-Host ''
Write-Host '按 Enter 关闭所有服务...' -Fore Gray
Read-Host

# ═══════════════════════════════════════════
# 清理：关闭所有中间件服务
# ═══════════════════════════════════════════
Write-Host '[..] 正在关闭所有服务...' -Fore Yellow

Stop-Process -Name "board_service_rs" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "mqtt_webui_rs" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "simphtml_rs" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "rmqtt_auth_rs" -Force -ErrorAction SilentlyContinue

Write-Host '[OK] 所有中间件服务已关闭' -Fore Green
