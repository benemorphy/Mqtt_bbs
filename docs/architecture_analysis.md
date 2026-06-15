# Mqtt_bbs 架构分析

> 分析日期: 2026-06-15
> 基于: CodeGraph索引 + 源码阅读

---

## 项目定位

纯 MQTT BBS 消息总线基础设施（无LLM、无记忆系统），对标 subagent 文件协议。

## 模块结构

### Python 端 (37文件, 258KB)

```
Mqtt_bbs_client/          ← 客户端库（智能体引入）
├── client.py             BBSClient — MQTT连接封装 (paho-mqtt)
├── board_client.py       BoardClient — 公告板 CRUD 客户端
├── types.py              TaskMessage / TaskOutput / TaskStatus (共享数据类型)
├── plugin.py             Plugin基类 + PluginContext + PluginManager + HMAC签名验证
├── registry.py           RetainCapabilityRegistry — 无状态能力注册表
├── rate_limiter.py       TokenBucket + RateLimiter
├── audit_log.py          AuditEvent + AuditLogger
├── config.py             全环境变量驱动（无硬编码密码）
├── persistence.py        BBSClientWithPersistence (MariaDB持久化)
└── examples/
    ├── master_agent.py   主智能体示例
    ├── worker_agent.py   工作智能体示例
    └── worker_factory.py

Mqtt_bbs/                  ← 项目根
├── Mqtt_bbs_server/         ← 服务端 Python 包
├── board_core.py         BoardService 核心生命周期 (260行)
├── board_handlers.py     MQTT消息处理器 (545行, 安全加固)
├── board_config.py       配置与常量 (TOPIC_BBS="agent/bbs")
├── board_db.py           MariaDB封装 + CapabilityRegistry
├── board_service.py      向后兼容包装层 → board_core
├── bbs.py                AgentBoard(发布者) + WorkerAgent(执行者) (844行)
├── scheduler.py          BBScheduler — 定时任务调度器
├── dag.py                DAGWorkflow — 工作流引擎
├── file_transfer_v2.py   分片文件传输 (MariaDB LONGBLOB)
├── persistence.py        BBSClientWithPersistence
├── persistence_worker.py WorkerAgentWithPersistence
├── mqtt_agent_runner.py  将GenericAgent包装为MQTT WorkerAgent
├── plugin_manager.py     服务端插件管理器
└── audit_log.py          服务端审计日志
```

### Rust 端 (42文件, 188KB) — 5个独立二进制

| 工具 | main入口 | 核心结构体 |
|------|---------|-----------|
| `board_service_rs` | `src/main.rs:37` | AppState, BBSClient, BoardClient, AgentBoard, CapabilityRegistry |
| `mqtt_bbs_rs` | *(库)* | BBSClient, AgentBoard, WorkerAgent, DAGWorkflow, FileTransfer, Scheduler, StateKV |
| `mqtt_webui_rs` | `src/main.rs:57` | AppState, AgentInfo, TaskInfo, BrokerStats |
| `llm_cache_rs` | `src/main.rs:21` | LlmCache, CacheEntry, CacheServer, CacheClient |
| `rmqtt_auth_rs` | `src/main.rs:17` | rmqtt认证插件 |
| `simphtml_rs` | `src/main.rs:14` | SelReq — HTML简化工具 |

## 核心协议：文件协议 → MQTT 映射

```
input.txt         → PUBLISH board/task/{id}/input   [Retain=True]
output.txt        → PUBLISH board/task/{id}/output  [Retain=True]
[ROUND END]       → PUBLISH board/task/{id}/signal  [QoS=2]
temp/{name}/      → topic/ 主题空间隔离
```

## MQTT 主题空间

### 协议层 bbs/ (BoardService管控)
- `bbs/register` / `bbs/register/response` — JWT注册
- `bbs/{board}/post` / `bbs/{board}/query` — 发帖/查询
- `bbs/{board}/response/{corr_id}` — 响应槽

### 应用层 board/ & node/
- `board/task/{id}/input|output|status|signal` — 任务生命周期
- `board/open` — 待认领任务索引
- `node/{agent_id}/capability|status` — 能力注册(Retain)

## 安全机制
- JWT认证 (BoardService签发)
- HMAC-SHA256任务签名 (_calc_hmac / _verify_task)
- 速率限制 (注册5/s, 发帖30/s, 查询20/s)
- 结构化审计日志

## 依赖关系

**被依赖**: Beneh(GenericAgent_mqtt) 通过 `Mqtt_bbs_client` 和 `Mqtt_bbs_server.mqtt_agent_runner` 使用本包

**依赖外部**: paho-mqtt, pymysql, MariaDB
