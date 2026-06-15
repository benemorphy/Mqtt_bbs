# GenericAgent MQTT - 全服务启动脚本
param([switch]$NoGateway)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = (Get-Item $root).Parent.FullName
$grandParent = (Get-Item $projectRoot).Parent.FullName
$grandgrandParent = (Get-Item $grandParent).Parent.FullName
$gaToolsRoot = Join-Path (Get-Item $projectRoot).Parent.FullName 'GA_tools'
Set-Location $root
$ErrorActionPreference = 'SilentlyContinue'

# 加载 agent.env (JWT 令牌 + 连接凭据)
$envFile = Join-Path $root 'agent.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
        }
    }
    Write-Host '[OK] agent.env loaded' -Fore Green
    # 映射 Dashboard 凭据到 MQTT 环境变量（BBSClient 从 MQTT_USERNAME/PASSWORD 读取）
    $env:MQTT_USERNAME = $env:DASHBOARD_USERNAME
    $env:MQTT_PASSWORD = $env:DASHBOARD_PASSWORD
    Write-Host '[OK] MQTT credentials set (dashboard => MQTT_USERNAME)' -Fore Green
    # 映射 mariadb_password -> DB_PASSWORD (mariadb 密码统一使用该环境变量)
    if (-not $env:DB_PASSWORD) { $env:DB_PASSWORD = $env:mariadb_password }
    Write-Host "[OK] DB_PASSWORD set from mariadb_password" -Fore Green
} else {
    Write-Host '[!] agent.env not found (JWT tokens missing)' -Fore Yellow
}

# 1. MariaDB
$svc = Get-Service -Name MariaDB -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -eq 'Running') {
    Write-Host '[OK] MariaDB (3306) 已在运行' -Fore Green
} else {
    try { Start-Service MariaDB -ErrorAction Stop; Write-Host '[OK] MariaDB 已启动' -Fore Green }
    catch { Write-Host '[!] MariaDB 未启动' -Fore Red }
}

# 2. Mosquitto (1883)
$mq = Get-Process mosquitto -ErrorAction SilentlyContinue
if ($mq) {
    Write-Host "[OK] Mosquitto (1883) PID=$($mq.Id) 已在运行" -Fore Green
} else {
    Start-Process 'D:\tools\mosquitto\mosquitto.exe' -ArgumentList '-c D:\tools\mosquitto\mosquitto.conf' -WindowStyle Hidden
    Start-Sleep 3
    Write-Host '[OK] Mosquitto 已启动 (1883)' -Fore Green
}

# 3. simphtml_rs (8901)
$p = Get-NetTCPConnection -LocalPort 8901 -ErrorAction SilentlyContinue
if ($p) { Write-Host '[OK] simphtml_rs (8901) 已在运行' -Fore Green }
else {
    $exe = Join-Path $root 'tools\simphtml_rs\target\release\simphtml_rs.exe'
    if (Test-Path $exe) { Start-Process $exe -ArgumentList '--serve --port 8901' -WindowStyle Hidden; Start-Sleep 2; Write-Host '[OK] simphtml_rs 已启动 (8901)' -Fore Green }
    else { Write-Host '[!] simphtml_rs 未编译，跳过' -Fore Yellow }
}

# 4. mqtt_webui_rs (8900) - 强制重启以使用正确的 MQTT 凭据
$p = Get-NetTCPConnection -LocalPort 8900 -ErrorAction SilentlyContinue
if ($p) {
    Write-Host '[..] rmqtt Web UI (8900) 存在，重启以应用dashboard凭据...' -Fore Yellow
    $old = Get-Process -Name "mqtt_webui_rs" -ErrorAction SilentlyContinue
    if ($old) { Stop-Process -Id $old.Id -Force; Start-Sleep 2 }
}
$exe = Join-Path $root 'tools\mqtt_webui_rs\target\release\mqtt_webui_rs.exe'
if (-not (Test-Path $exe)) {
    $exe = Join-Path $root 'tools\mqtt_webui_rs\target\debug\mqtt_webui_rs.exe'
}
if (Test-Path $exe) {
    $env:MQTT_USERNAME = 'dashboard'
    $env:MQTT_PASSWORD = 'eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJzdWIiOiAiZGFzaGJvYXJkIiwgImNsaWVudGlkIjogImRhc2hib2FyZCIsICJ1c2VybmFtZSI6ICJkYXNoYm9hcmQiLCAicm9sZSI6ICJvYnNlcnZlciIsICJleHAiOiAxODEwNTM1NTczLCAiaWF0IjogMTc3ODk5OTU3M30.h_4qJej8QnJ8BXOknx5fF7mBQS2obEH7d6r2sZkMpfA'
    Start-Process -FilePath $exe -WindowStyle Hidden
    Start-Sleep 3
    try { $null = Get-NetTCPConnection -LocalPort 8900 -ErrorAction Stop; Write-Host '[OK] rmqtt Web UI 已启动 (8900)' -Fore Green }
    catch { Write-Host '[!] rmqtt Web UI 启动失败 (8900 端口未监听)' -Fore Red }
}
else { Write-Host '[!] mqtt_webui_rs debug未编译，跳过' -Fore Yellow }

# 5. md_server_rs (8899)
# Usage: md_server_rs [port] [root_dir] [base_path]
#   port:     默认 8899
#   root_dir: 默认 ./docs (相对 CWD), 支持绝对/相对路径
#   base_path: URL 前缀如 /docs，让导航链接带上该前缀 (default: "")
$p = Get-NetTCPConnection -LocalPort 8899 -ErrorAction SilentlyContinue
if ($p) {
    Write-Host '[..] MD Server (8899) 存在，重启以刷新目录...' -Fore Yellow
    $old = Get-Process -Name "md_server_rs" -ErrorAction SilentlyContinue
    if ($old) { Stop-Process -Id $old.Id -Force; Start-Sleep 2 }
}
$exe = Join-Path $gaToolsRoot 'md_server_rs\target\release\md_server_rs.exe'
if (-not (Test-Path $exe)) {
    $exe = Join-Path $gaToolsRoot 'md_server_rs\target\debug\md_server_rs.exe'
}
if (Test-Path $exe) {
    # 服务项目根目录 (Mqtt_bbs/docs/ 下文档通过相对路径访问)
    $docsDir = $grandgrandParent
    Start-Process $exe -ArgumentList @('8899', $docsDir, '/docs') -WindowStyle Hidden
    Start-Sleep 2
    if (Get-NetTCPConnection -LocalPort 8899 -ErrorAction SilentlyContinue) {
        Write-Host "[OK] MD Server 已启动 (8899) 服务: $docsDir" -Fore Green
    } else {
        Write-Host '[!] MD Server 启动可能失败，请检查' -Fore Yellow
    }
}
else { Write-Host '[!] md_server_rs 未编译，跳过 (cargo build --release 编译)' -Fore Yellow }

# 6. BoardService RS
$bs_exe = Join-Path $projectRoot 'Mqtt_bbs_server\tools\board_service_rs\target\release\board_service_rs.exe'
if (-not (Test-Path $bs_exe)) {
    $bs_exe = Join-Path $projectRoot 'Mqtt_bbs_server\tools\board_service_rs\target\debug\board_service_rs.exe'
}
if (Test-Path $bs_exe) {
    $pw = [Environment]::GetEnvironmentVariable('DB_PASSWORD','Process')
    $jwt = [Environment]::GetEnvironmentVariable('JWT_SECRET','Process')
    if (-not $jwt) { $jwt = 'bbs-browser-dev-secret-change-in-production' }
    $env:MQTT_USERNAME = 'board-service-rs'
    $env:MQTT_PASSWORD = 'board-service-rs'
    Write-Host "[OK] BoardService JWT_SECRET 已就绪 (长度: $($jwt.Length))" -Fore Green
    Start-Process $bs_exe -ArgumentList "--db-url ""mysql://root:$pw@127.0.0.1/mqtt_bbs"" --jwt-secret ""$jwt""" -WindowStyle Hidden
    Start-Sleep 3
    try { $null = Get-NetTCPConnection -LocalPort 9100 -ErrorAction SilentlyContinue; Write-Host '[OK] BoardService RS 已启动' -Fore Green }
    catch { Write-Host '[!] BoardService RS 启动可能失败' -Fore Yellow }
} else { Write-Host '[!] BoardService RS 未编译，跳过' -Fore Yellow }



# 8. Scheduler (定时任务)
$sched = Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*reflect/scheduler*' }
if (-not $sched) {
    $schedPy = Join-Path $projectRoot 'GA\agentmain.py'
    Start-Process $py -ArgumentList @($schedPy, '--reflect', 'reflect/scheduler.py') -WorkingDirectory (Join-Path $projectRoot 'GA') -WindowStyle Hidden
    Write-Host '[OK] Scheduler 已启动' -Fore Green
} else { Write-Host '[OK] Scheduler 已在运行' -Fore Green }

# 9. Supervisor Monitor (系统健康监控)
$sv = Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*reflect/supervisor_monitor*' }
if (-not $sv) {
    $svPy = Join-Path $projectRoot 'GA\agentmain.py'
    Start-Process $py -ArgumentList @($svPy, '--reflect', 'reflect/supervisor_monitor.py') -WorkingDirectory (Join-Path $projectRoot 'GA') -WindowStyle Hidden
    Write-Host '[OK] Supervisor Monitor 已启动' -Fore Green
} else { Write-Host '[OK] Supervisor Monitor 已在运行' -Fore Green }

# 7. FastAPI Gateway (8001) + Caddy 反代 (8000)
if (-not $NoGateway) {
    # 7a. FastAPI 后端 (8001)
    $p = Get-NetTCPConnection -LocalPort 8001 -ErrorAction SilentlyContinue
    if ($p) {
        Write-Host '[..] FastAPI Gateway (8001) 旧实例存在，重启以应用MQTT凭据...' -Fore Yellow
        $oldPid = $p.OwningProcess
        Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
        Start-Sleep 2
    }
    $py = Join-Path $projectRoot '.venv\Scripts\python.exe'
    Start-Process $py -ArgumentList '-m frontends.web_ui.main' -WorkingDirectory (Join-Path $projectRoot 'GA') -WindowStyle Hidden
    Start-Sleep 5
    try { $r = Invoke-WebRequest -Uri 'http://127.0.0.1:8001/login' -UseBasicParsing -TimeoutSec 3; Write-Host '[OK] FastAPI Gateway 已启动 (8001)' -Fore Green }
    catch { Write-Host '[!] FastAPI Gateway 启动可能失败，请检查 (8001)' -Fore Red }

    # 7b. Caddy 反代 (8000) — 如 FastAPI 启动失败仍尝试启动 Caddy
    $caddyExe = 'D:\tools\caddy\caddy.exe'
    $caddyfile = Join-Path $projectRoot 'GA\Caddyfile'
    $caddyProc = Get-Process -Name "caddy" -ErrorAction SilentlyContinue
    if (-not $caddyProc) {
        if ((Test-Path $caddyExe) -and (Test-Path $caddyfile)) {
            Start-Process $caddyExe -ArgumentList "run --config ""$caddyfile""" -WindowStyle Hidden
            Start-Sleep 3
            try { $null = Get-NetTCPConnection -LocalPort 8000 -ErrorAction Stop; Write-Host '[OK] Caddy 已启动 (:8000)' -Fore Green }
            catch { Write-Host '[!] Caddy 启动可能失败 (8000 端口未监听)' -Fore Red }
        } else {
            Write-Host '[!] Caddy 可执行文件或 Caddyfile 未找到，跳过' -Fore Yellow
        }
    } else {
        Write-Host "[OK] Caddy (8000) PID=$($caddyProc.Id) 已在运行" -Fore Green
    }
}

# 8. Default WorkerAgent
Write-Host '[..] 启动默认 WorkerAgent...' -Fore Yellow
$py = Join-Path $projectRoot '.venv\Scripts\python.exe'
$workerScript = Join-Path $root 'examples\worker_agent.py'
if (Test-Path $workerScript) {
    Start-Process $py -ArgumentList $workerScript -WorkingDirectory $projectRoot -WindowStyle Hidden
    Start-Sleep 2; Write-Host '[OK] 默认 WorkerAgent 已启动' -Fore Green
} else {
    Write-Host '[!] examples\worker_agent.py 未找到，跳过 WorkerAgent' -Fore Yellow
}

# 10. Everything 全盘搜索服务 (es.exe / server 模式)
$evSvc = Get-Service -Name Everything -ErrorAction SilentlyContinue
if ($evSvc) {
    if ($evSvc.Status -eq 'Running') {
        Write-Host "[OK] Everything 服务 ($($evSvc.Status), $($evSvc.StartType)) 已在运行" -Fore Green
    } else {
        Write-Host "[..] Everything 服务已安装但未运行，正在启动..." -Fore Yellow
        Start-Service Everything -ErrorAction SilentlyContinue
        Start-Sleep 2
        $evSvc = Get-Service -Name Everything -ErrorAction SilentlyContinue
        if ($evSvc.Status -eq 'Running') {
            Write-Host '[OK] Everything 服务已启动' -Fore Green
        } else {
            Write-Host '[!] Everything 服务启动失败，尝试重新安装...' -Fore Red
            & "C:\Program Files\Everything\Everything.exe" -install-service
            Start-Sleep 2
            Start-Service Everything -ErrorAction SilentlyContinue
        }
    }
    # 设为自动启动
    if ($evSvc.StartType -ne 'Automatic') {
        try {
            Set-Service -Name Everything -StartupType Automatic -ErrorAction Stop
            Write-Host '[OK] Everything 服务已设为自动启动' -Fore Green
        } catch {
            Write-Host '[!] 设置 Everything 自动启动失败（需要管理员权限）' -Fore Yellow
        }
    }
} else {
    Write-Host '[..] Everything 服务未安装，正在安装...' -Fore Yellow
    try {
        Start-Process -FilePath "C:\Program Files\Everything\Everything.exe" -ArgumentList "-install-service" -Verb RunAs -Wait -ErrorAction Stop
        Start-Sleep 3
        Start-Service Everything -ErrorAction SilentlyContinue
        Set-Service -Name Everything -StartupType Automatic -ErrorAction SilentlyContinue
        if ((Get-Service -Name Everything -ErrorAction SilentlyContinue).Status -eq 'Running') {
            Write-Host '[OK] Everything 服务已安装并启动 (自动启动)' -Fore Green
        } else {
            Write-Host '[!] Everything 安装失败，请手动以管理员运行安装' -Fore Red
        }
    } catch {
        Write-Host '[!] Everything 安装需要管理员权限，请手动安装:' -Fore Yellow
        Write-Host '    "C:\Program Files\Everything\Everything.exe" -install-service' -Fore Cyan
    }
}

# 12. MemPalace MCP Server (语义搜索+知识图谱)
# (已移除 llm_cache_rs)
$mcp = Get-Process -Name "mempalace-mcp" -ErrorAction SilentlyContinue
if ($mcp) {
    Write-Host "[OK] MemPalace MCP Server (PID=$($mcp.Id)) 已在运行" -Fore Green
} else {
    & $py (Join-Path (Join-Path $projectRoot 'GA') 'memory\mempalace_mcp_launcher.py') start
    Start-Sleep 3
    $mcp = Get-Process -Name "mempalace-mcp" -ErrorAction SilentlyContinue
    if ($mcp) { Write-Host '[OK] MemPalace MCP Server 已启动' -Fore Green }
    else { Write-Host '[!] MemPalace MCP Server 启动可能失败' -Fore Red }
}

Write-Host ''
Write-Host '========================================' -Fore Cyan
Write-Host '  Service         Port    Status' -Fore Cyan
Write-Host '  --------         ----    ------' -Fore Cyan
@(
  @{n='Caddy (Web)';   p=8000;  u='http://localhost:8000'},
  @{n='FastAPI (后端)'; p=8001;  u='http://localhost:8001'},
  @{n='Mosquitto';     p=1883;  u='mqtt://127.0.0.1:1883'},
  @{n='MariaDB';      p=3306;  u='mysql://127.0.0.1:3306'},
  @{n='simphtml_rs';  p=8901;  u='http://localhost:8901'},
  @{n='rmqtt Web UI'; p=8900;  u='http://localhost:8900'},
  @{n='MD Server';    p=8899;  u='http://localhost:8899'},
  @{n='BoardService'; p='---'; u='MQTT BBS'; chk='board_service_rs'},
  @{n='Everything';   p='---'; u='全盘搜索(服务)'; chk='Everything'},
  # 已移除 llm_cache_rs
  @{n='MemPalace MCP';p='---'; u='MCP Server'; chk='mempalace-mcp'}
) | ForEach-Object {
  $s = if ($_.chk) { try { if (Get-Process -Name $_.chk -ErrorAction Stop) { 'RUN' } else { 'OFF' } } catch { 'OFF' } } elseif ($_.p -eq '---') { '---' } else { try { $t = Get-NetTCPConnection -LocalPort $_.p -ErrorAction Stop; if ($t.State -eq 'Listen') { 'RUN' } else { '???' } } catch { 'OFF' } };
  $c = if ($s -eq 'RUN') { 'Green' } elseif ($s -eq 'OFF') { 'Red' } else { 'Yellow' };
  Write-Host ('  {0,-14} {1,5}  [{2}]' -f $_.n, ('port ' + $_.p), $s) -Fore $c;
};
Write-Host ''

Write-Host '========================================' -Fore Cyan

Write-Host ''
Write-Host '按 Enter 关闭所有服务 (Ctrl+C 直接退出)' -Fore Gray
Read-Host

# Cleanup section
Write-Host '[..] 正在关闭所有服务...' -Fore Yellow

# 关 WorkerAgent
$pyProcesses = Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*worker_agent*' }
foreach ($proc in $pyProcesses) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }

# 关 Gateway
$gwProcesses = Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*gateway*' }
foreach ($proc in $gwProcesses) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }

# 关 BoardService
Stop-Process -Name "board_service_rs" -Force -ErrorAction SilentlyContinue

# 关 Everything (仅停 GUI 进程，保留服务)
Stop-Process -Name "Everything" -Force -ErrorAction SilentlyContinue

# 关 MD Server
Stop-Process -Name "md_server_rs" -Force -ErrorAction SilentlyContinue

# 关 mqtt_webui_rs
Stop-Process -Name "mqtt_webui_rs" -Force -ErrorAction SilentlyContinue

# 关 simphtml_rs
Stop-Process -Name "simphtml_rs" -Force -ErrorAction SilentlyContinue

# 关 MemPalace MCP Server
$mcpLauncher = Join-Path (Join-Path $projectRoot 'GA') 'memory\mempalace_mcp_launcher.py'
& $py $mcpLauncher stop 2>$null
Stop-Process -Name "mempalace-mcp" -Force -ErrorAction SilentlyContinue

# 关 Caddy
Stop-Process -Name "caddy" -Force -ErrorAction SilentlyContinue

Write-Host '[OK] 所有服务已关闭' -Fore Green
