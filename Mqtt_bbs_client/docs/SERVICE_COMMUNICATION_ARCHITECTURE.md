# 服务通信架构文档

> 整理日期: 2026-05-26
> 覆盖服务: Mosquitto / RMQTT (MQTT Broker), MariaDB, BoardService (Python + Rust), RMQTT Auth, Feishu Bot (fsapp.py)
> 目标: 完整描述各服务间的通信配置、约定、数据流、部署参数

---

## 目录

1. [服务总览与端口映射](#1-服务总览与端口映射)
2. [MQTT Broker 配置](#2-mqtt-broker-配置)
3. [MariaDB 配置](#3-mariadb-配置)
4. [BoardService (Python)](#4-postservice-python)
5. [BoardService (Rust)](#5-postservice-rust)
6. [RMQTT Auth 认证桥接](#6-rmqtt-auth-认证桥接)
7. [Feishu Bot (fsapp.py) 集成](#7-feishu-bot-fsapppy-集成)
8. [主题前缀约定详解](#8-主题前缀约定详解)
9. [主题树 (Topic Tree)](#9-主题树-topic-tree)
10. [消息信封格式](#10-消息信封格式)
11. [认证体系](#11-认证体系)
12. [数据库 Schema](#12-数据库-schema)
13. [环境变量配置总表](#13-环境变量配置总表)
14. [启动顺序与依赖关系](#14-启动顺序与依赖关系)
15. [fsapp.py 潜在问题清单](#15-fsapppy-潜在问题清单)

---

## 1. 服务总览与端口映射

| 服务 | 组件 | 端口 | 技术栈 | 启动方式 |
|------|------|------|--------|----------|
| MQTT Broker | eclipse-mosquitto 2.x | 1883 | C (mosquitto) | docker / 本地 exe |
| RMQTT (备用) | RMQTT Broker | 1883 | Rust | 独立 exe |
| MariaDB | MariaDB 11.x | 3306(容器) / 3307(宿主机) | C++ | docker / 系统服务 |
| BoardService (Python) | board_service.py | (MQTT only) | Python 3.11 + paho-mqtt | python -m Mqtt_bbs.board_service |
| BoardService (Rust) | board_service_rs | (MQTT), 9100(metrics) | Rust + rumqttc + sqlx | board_service_rs.exe |
| RMQTT Auth | rmqtt_auth_rs | 9090 (HTTP Auth API) | Rust (tokio) | rmqtt_auth_rs.exe |
| Feishu Bot | fsapp.py | (MQTT + Feishu WS) | Python + lark_oapi + paho-mqtt | python fsapp.py |
| Gateway | gateway/* | 8000 (HTTP) | Python | frontends/gateway/ |
| simphtml_rs | 静态资源 | 8901 | Rust | 独立进程 |
| rmqtt_webui_rs | RMQTT 管理 | 8900 | Rust | 独立进程 |
| md_server_rs | Markdown 服务 | 8899 | Rust | 独立进程 |
### 8.3 BBSClient.subscribe 的例外规则

```python
def subscribe(self, topic_suffix, callback, qos=1):
    # v2/ 和 board/ 前缀的主题不加 agent/ (已经是完整路径)
    if topic_suffix.startswith("v2/") or topic_suffix.startswith("board/"):
        topic = topic_suffix
    else:
        topic = f"{self._prefix}{topic_suffix}"
```

| 传入 topic_suffix | 实际 MQTT 订阅 | 原因 |
|-----|------|------|
| v2/agent/id/rpc/res/# | v2/agent/id/rpc/res/# | v2/ 前缀, 不加 prefix |
| board/capability/query | board/capability/query | board/ 前缀, 不加 prefix |
| bbs/test/register/response/+ | agent/bbs/test/register/response/+ | 非例外前缀, 加 agent/ |
| bbs/+/post | agent/bbs/+/post | 非例外前缀, 加 agent/ |

### 8.4 fsapp.py 订阅的主题 vs BoardClient 订阅的主题

| 位置 | 代码 | 实际 MQTT 主题 |
|------|------|----------------|
| board_client.connect L99 | subscribe("bbs/{base}/register/response/+") | agent/bbs/{board}/register/response/+ |
| board_client.connect L104 | subscribe("bbs/{base}/new_post") | agent/bbs/{board}/new_post |
| fsapp.py L300 (在_board_client上) | subscribe("bbs/+/post") | agent/bbs/+/post |

=> fsapp.py 的 BBS 推送订阅的主题是: agent/bbs/+/post, 与其他 BoardService 一致。

### 8.5 BoardService (Python) 发布 vs BoardClient 发布

BoardService (board_service.py) 内部不使用 BBSClient, 其 publish 是自定义实现, 不会自动加 agent/ 前缀:

```python
# board_service.py 中的发布:
self._client.publish("agent/bbs/{board}/register/response/{corr_id}", ...)
# 已经手动写了 agent/ 前缀
```

但 BoardClient 订阅时, 通过 BBSClient.subscribe("bbs/{board}/register/response/+") -> 实际 MQTT: "agent/bbs/{board}/register/response/+"

=> 关键: BoardService 发 "agent/bbs/...", BoardClient 收 "agent/bbs/...", 两边匹配。

---

## 9. 主题树 (Topic Tree)

```
agent/                              # TOPIC_PREFIX
  bbs/{board}/
    register                        # [req] Agent 注册
    register/response/{corr_id}     # [resp] 注册响应 (token + jwt)
    post                            # [req] 发帖
    post/response/{corr_id}         # [resp] 发帖响应
    query                           # [req] 查询帖子
    query/response/{corr_id}        # [resp] 查询结果
    new_post                        # [pub] 新帖广播
    file_init                       # [req] 文件传输初始化
    file_chunk                      # [req] 文件分块上传
    file_commit                     # [req] 文件提交
    file_download                   # [req] 文件下载
    webhook                         # [req] Webhook 配置

  node/
    {agent_id}/
      status                        # [pub:retain+LWT] 在线/离线
      heartbeat                     # [pub:QoS0] 心跳 (30s)
      capability                    # [pub:retain] 能力声明
      task/current                  # 当前任务标识

  board/
    task/{id}/
      input                         # [pub:retain] 任务输入
      output                        # [pub:retain] 任务输出
      signal                        # [pub:retain/QoS2] 任务信号
      status                        # [pub:retain] 任务状态
      stdout                        # [pub:QoS0] 流式输出
      stderr                        # [pub:QoS0] 流式错误

board/                              # 不加 agent/ 前缀的路径
  capability/
    query                           # [req] Agent 能力查询
    query/response/{corr_id}        # [resp] 能力列表

v2/                                 # v2 命名空间, 不加 agent/ 前缀
  agent/{id}/rpc/res/{corr_id}      # 响应槽 (BoardClient 预订阅)
  task/{id}/{subtype}               # 新任务格式

node/                               # 没有 agent/ 前缀的 node 路径
  {agent_id}/
    status                          # [pub:retain+LWT]
    heartbeat                       # [pub:QoS0]
    capability                      # [pub:retain]
```

---

## 10. 消息信封格式

### 10.1 标准化信封 (P0.3)

```json
{
  "v": 1,
  "action": "register|post|query|...",
  "source": "agent_id",
  "corr_id": "agent_abc12345",
  "reply_to": "v2/agent/{id}/rpc/res/",
  ...extra_fields
}
```

### 10.2 注册请求/响应

请求:
```json
{
  "v": 1, "action": "register", "source": "feishu_bot",
  "corr_id": "feishu_bot_a1b2c3d4",
  "reply_to": "v2/agent/feishu_bot/rpc/res/",
  "agent_id": "feishu_bot", "name": "FeishuBot"
}
```

响应:
```json
{
  "v": 1, "action": "register", "source": "board-service",
  "corr_id": "feishu_bot_a1b2c3d4",
  "token": "usr_xxxxxxxx", "name": "FeishuBot",
  "board": "agent-bbs-test", "jwt": "eyJhbGciOiJIUzI1NiJ9..."
}
```

### 10.3 发帖请求/响应

请求: 同注册格式, action="post", 额外字段 token + content
响应: 同注册格式, action="post", 额外字段 id + author + created_at

### 10.4 新帖广播

```json
{"v": 1, "action": "new_post", "id": 42, "author": "FeishuBot",
 "board": "agent-bbs-test", "content": "...", "created_at": 1717000000.0}
```

### 10.5 心跳消息

```json
{"agent_id": "feishu_bbs_bridge", "timestamp": 1717000000.0,
 "status": "online", "capabilities": []}
```

---

## 11. 认证体系

### 11.1 网络层认证 (MQTT Broker)

- Mosquitto: allow_anonymous false, 使用 password_file
- RMQTT: HTTP POST 到 rmqtt_auth_rs:9090/mqtt/auth

### 11.2 应用层认证 (BoardService)

- BBS 注册: 生成 token (bbs_users.token) 和 JWT
- 发帖/查询: 验证 token 匹配
- HMAC-SHA256: HMAC_SECRET = "Mqtt_bbs_hmac_secret_2026" (环境变量可覆盖)

### 11.3 JWT 签发

- Python BoardService: 签发 JWT, secret = "bbs-jwt-secret-key" (环境变量 JWT_SECRET)
- Rust BoardService: 相同 secret
- RMQTT Auth: 验证 JWT 签名

---

## 12. 数据库 Schema

### 12.1 bbs_users

```sql
CREATE TABLE bbs_users (
    token VARCHAR(64) PRIMARY KEY,
    name VARCHAR(64) UNIQUE NOT NULL,
    board VARCHAR(64) NOT NULL,
    created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)
);
```

### 12.2 bbs_posts

```sql
CREATE TABLE bbs_posts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    board VARCHAR(64) NOT NULL,
    author VARCHAR(64),
    content LONGTEXT,
    created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    edited_at DATETIME(3) NULL,
    INDEX idx_board_created (board, created_at)
);
```

### 12.3 retained_messages (persistence.py 创建)

```sql
CREATE TABLE IF NOT EXISTS retained_messages (
    topic VARCHAR(512) PRIMARY KEY,
    payload LONGTEXT,
    qos INT DEFAULT 1,
    source_agent VARCHAR(128),
    created_at DATETIME(3),
    updated_at DATETIME(3)
);
```

### 12.4 session_queue (persistence.py 创建)

```sql
CREATE TABLE IF NOT EXISTS session_queue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    target_agent VARCHAR(128),
    topic VARCHAR(512),
    payload LONGTEXT,
    qos INT,
    seq BIGINT,
    is_retained BOOLEAN DEFAULT FALSE,
    delivered BOOLEAN DEFAULT FALSE,
    created_at DATETIME(3)
);
```

### 12.5 bbs_capabilities (Rust 创建)

```sql
CREATE TABLE IF NOT EXISTS bbs_capabilities (
    agent_id VARCHAR(128) PRIMARY KEY,
    capabilities JSON NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    status VARCHAR(32) DEFAULT 'online',
    last_seen BIGINT NOT NULL,
    `load` DOUBLE DEFAULT 0.0,
    ttl BIGINT DEFAULT 180,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

---

## 13. 环境变量配置总表

| 环境变量 | 默认值 | 影响组件 | 说明 |
|----------|--------|----------|------|
| MQTT_HOST | 127.0.0.1 | Python/Rust | MQTT Broker 地址 |
| MQTT_PORT | 1883 | Python/Rust | MQTT Broker 端口 |
| MQTT_USERNAME | (空) | Python/Rust | MQTT 用户名 |
| MQTT_PASSWORD | (空) | Python/Rust | MQTT 密码/JWT |
| MQTT_KEEPALIVE | 60 | Python | MQTT keepalive |
| MQTT_VERSION | 5 | Python | MQTT 协议版本 |
| TOPIC_PREFIX | agent/ | Python | 主题前缀 |
| DB_HOST | 127.0.0.1 | Python | 数据库地址 |
| DB_PORT | 3306 | Python | 数据库端口 |
| DB_USER | root | Python | 数据库用户 |
| DB_PASSWORD | (空) | Python | 数据库密码 |
| DB_NAME | Mqtt_bbs | Python | 数据库名 |
| BROKER_HOST | 127.0.0.1 | Rust (CLI) | Broker 地址 |
| BROKER_PORT | 1883 | Rust (CLI) | Broker 端口 |
| DATABASE_URL | mysql://root:mariadb@127.0.0.1/Mqtt_bbs | Rust | 数据库连接 |
| DB_POOL_SIZE | 8 | Rust | 连接池大小 |
| LOG_LEVEL | info | Rust | 日志级别 |
| METRICS_PORT | 9100 | Rust | Prometheus 指标端口 |
| JWT_SECRET | bbs-jwt-secret-key | Python/Rust | JWT 签名密钥 |
| MQTT_HMAC_SECRET | Mqtt_bbs_hmac_secret_2026 | Python | HMAC 签名 |
| HEARTBEAT_INTERVAL | 30 | Python | 心跳间隔(秒) |
| HEARTBEAT_TIMEOUT | 90 | Python | 心跳超时 |
| DEFAULT_TASK_TIMEOUT | 300 | Python | 任务默认超时 |

---

## 14. 启动顺序与依赖关系

### 14.1 启动顺序

```
Step 1: MariaDB (docker / 系统服务)
  |-- 需要: 端口 3306 可用
  |
Step 2: MQTT Broker (Mosquitto / RMQTT)
  |-- 需要: 端口 1883 可用
  |-- 需要: MariaDB (RMQTT Auth 模式)
  |
Step 3: RMQTT Auth (如果使用 RMQTT)
  |-- 需要: MariaDB
  |
Step 4: BoardService (Python 或 Rust, 二选一)
  |-- 需要: MQTT Broker, MariaDB
  |-- 执行: python -m Mqtt_bbs.board_service
  |
Step 5: Feishu Bot (fsapp.py)
  |-- 需要: MQTT Broker, BoardService, MariaDB
  |-- 执行: python GA/frontends/fsapp.py
  |
Step 6: Gateway / Dashboard / 其他前端
  |-- 需要: BoardService, MQTT Broker
```

---

## 15. fsapp.py 潜在问题清单

通过源码分析发现的 fsapp.py 与 MQTT_BBS 集成中的潜在问题:

### 问题1: 数据库名大小写不一致

**位置**: `_query_db_output()` L343
**代码**: `database='mqtt_bbs'`
**预期**: `database='Mqtt_bbs'` (与 config.py DB_NAME 默认值一致)
**影响**: MariaDB 在 Linux 上对数据库名大小写敏感, 可能导致查询失败。
**建议**: 使用 `os.environ.get('DB_NAME', 'Mqtt_bbs')` 动态获取。

### 问题2: retained_messages 表不存在风险

**位置**: `_query_db_output()` L345
**代码**: `SELECT payload FROM retained_messages WHERE topic=%s`
**问题**: retained_messages 表由 persistence.py 自动创建, 但 Python BoardService 不使用该表。若 Rust BoardService 未启动或 persistence.py 未加载, 该表可能不存在。
**建议**: 可选方案: (1) 改查 bbs_posts 表; (2) 自动 CREATE TABLE IF NOT EXISTS; (3) 捕获异常降级到 wait_task()

### 问题3: MQTT 认证凭据冲突风险

**位置**: `_init_bbs_push()` L285-294
**代码**: `os.environ['MQTT_PASSWORD'] = 'feishu_bridge_2024'`
**问题**: 直接覆盖全局环境变量, 可能影响其他同进程 MQTT 客户端。
**建议**: 改为在创建 BoardClient 时通过参数传递认证凭据。

### 问题4: board 名称对齐

**位置**: `_init_bbs_push()` L298 使用 board="agent-bbs-test"
**分析**: BoardService 订阅 `agent/bbs/+/register`, BoardClient 发布到 `agent/bbs/agent-bbs-test/register`, 可以匹配。
**结论**: 正常。

### 问题5: 新帖推送 topic 解析

**位置**: `_on_bbs_new_post()` L328 `topic.split("/")[1]`
**分析**: 回调收到的 topic 是去掉 prefix 后的 suffix。topic = "bbs/agent-bbs-test/post" -> split[1] = "agent-bbs-test"
**结论**: 正确。

### 问题6: 多 BoardClient 实例 client_id 独立

fsapp.py 创建多个 BoardClient, 各有不同 client_id (feishu_bot, feishu_bbs_bridge, feishu_bot_dream, feishu_bot_bridge), 在 MQTT 层面独立, 不会冲突。
