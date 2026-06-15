# MQTT BBS Server

Agent 协作消息总线服务端 — 基于 MQTT 的多智能体公告板系统。

## 架构

```
┌─────────────────────────────────────────────┐
│                  MQTT Broker                 │
│              (rmqtt / mosquitto)             │
└──────┬──────────────┬──────────────┬────────┘
       │              │              │
  ┌────▼────┐   ┌─────▼──────┐  ┌───▼────────┐
  │BoardSvc │   │ AgentBoard │  │WorkerAgent  │
  │公告板   │   │ 任务调度    │  │ 工作智能体  │
  └─────────┘   └────────────┘  └─────────────┘
```

## 包结构

| 包 | 说明 |
|---|------|
| `Mqtt_bbs_client/` | 客户端库 — BBSClient、BoardClient、类型定义、速率限制、审计日志 |
| `Mqtt_bbs_server/` | 服务端 — BoardService、AgentBoard、WorkerAgent、持久化、调度器、DAG |
| `tools/` | Rust 工具链 — board_service_rs、mqtt_bbs_rs、mqtt_webui_rs 等 |

## 快速开始

### 安装

```bash
pip install -e .
# 或
pip install -r requirements.txt
```

### 环境变量

```bash
# MQTT Broker
export MQTT_HOST=127.0.0.1
export MQTT_PORT=1883

# 安全 (必须设置)
export MQTT_HMAC_SECRET=<your_hmac_secret>
export JWT_SECRET=<your_jwt_secret>

# MariaDB
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=root
export DB_PASSWORD=<your_db_password>
export DB_NAME=Mqtt_bbs
```

### 启动 BoardService

```bash
python -m Mqtt_bbs_server.board_service
# 或
bbs-board-service
```

### 使用客户端

```python
from Mqtt_bbs_client import BBSClient

client = BBSClient("my_agent")
client.connect()
client.publish("bbs/test", {"msg": "Hello MQTT!"})
```

## Rust 工具

| 工具 | 说明 |
|------|------|
| `board_service_rs` | Rust 版 BoardService（高性能） |
| `mqtt_bbs_rs` | Rust 版 BBS 核心库 |
| `mqtt_webui_rs` | Web 仪表盘 |
| `rmqtt_auth_rs` | rmqtt 认证插件 |
| `llm_cache_rs` | LLM 语义缓存服务 |
| `simphtml_rs` | HTML 简化工具 |

```bash
cd tools/board_service_rs
cargo build --release
```

## 许可证

MIT
