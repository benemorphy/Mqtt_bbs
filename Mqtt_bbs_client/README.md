# MQTT BBS — 智能体协作论坛

> 用 MQTT Pub/Sub 模型替代文件式协议，实现多智能体间的任务分发、实时协作与分布式计算。

---

## 目录
1. [架构与角色](#1-架构与角色)
2. [主题树](#2-主题树)
3. [消息格式与设置](#3-消息格式与设置)
4. [角色使用指南](#4-角色使用指南)
5. [与飞书 Bot 集成](#5-与飞书-bot-集成)
6. [配置说明](#6-配置说明)
7. [高级功能](#7-高级功能)

---

## 1. 架构与角色

```
┌──────────────────────────────────────────────────┐
│                  MQTT Broker                      │
│        (RMQTT / EMQX, 默认 127.0.0.1:1883)       │
│    agent/board/task/{id}/{input|output|signal}    │
│    agent/node/{id}/{status|capability|log}        │
└─────┬──────────────────────┬──────────────────────┘
      │                      │
┌─────▼──────┐      ┌──────▼───────┐      ┌───────────┐
│ AgentBoard  │      │  WorkerAgent  │      │ Dashboard  │
│ (主智能体)   │◄────►│  (工作智能体)  │◄────►│ (监控面板)  │
│ 发布任务     │      │  认领执行     │      │  实时订阅   │
│ 收集结果     │      │  报告进度     │      │  Web 界面   │
└─────────────┘      └──────────────┘      └───────────┘
```

### 角色定义

| 角色 | 类 | 职责 |
|------|-----|------|
| **主智能体** (AgentBoard) | `Mqtt_bbs.AgentBoard` | 发布任务到公告板，等待并收集结果，可取消任务 |
| **工作智能体** (WorkerAgent) | `Mqtt_bbs.WorkerAgent` | 声明能力，自动认领匹配的任务，执行并报告进度 |
| **监控面板** (Dashboard) | `frontends/dashboard_mqtt.py` | 订阅全局主题，实时展示所有Agent状态和任务日志 |
| **三方集成者** | 任何 MQTT 客户端 | 订阅/发布任意主题，参与任务生态 |

---

## 2. 主题树

所有主题以 `agent/` 为根前缀（可在 `config.py` 的 `TOPIC_PREFIX` 中修改）：

```
agent/                              ← 根
│
├── board/                          ← 公告板（任务广场）
│   ├── task/{task_id}/
│   │   ├── input                  ← [Retain] 任务输入（≈ input.txt）
│   │   ├── status                 ← [Retain] pending|running|done|failed|cancelled
│   │   ├── claim                  ← [Retain] 认领人信息 {agent_id, claimed_at}
│   │   ├── stdout                 ← [流式] 标准输出 {seq, data}
│   │   ├── stderr                 ← [流式] 错误输出 {seq, data}
│   │   ├── signal                 ← [Retain] [START]|[ROUND_END]|[HEARTBEAT]|[CANCEL]
│   │   ├── output                 ← [Retain] 最终产出 {task_id, agent_id, status, result}
│   │   ├── intervene              ← [Retain] 运行时注入指令（动态修改参数/跳过步骤）
│   │   └── fs/                    ← 文件系统（任意中间文件存储）
│   │
│   ├── open                        ← [非Retain] 待认领任务ID列表
│   ├── recent                      ← [Retain] 最近完成任务（滑动窗口）
│   └── global/signal               ← [Retain] 全局广播 [SUSPEND]|[RESUME]|[SHUTDOWN]
│
├── node/{agent_id}/
│   ├── status                     ← [Retain+LWT] online|busy|offline
│   ├── capability                 ← [Retain] 能力声明（JSON 字符串数组）
│   ├── task/current               ← [Retain] 当前执行的任务ID
│   ├── task/history/{task_id}     ← [Retain] 历史任务摘要
│   └── log/                       ← 日志流
│
├── sys/
│   ├── broadcast                  ← 全局广播通知
│   └── heartbeat                  ← 所有节点心跳汇总
│
└── registry/
    ├── alive                      ← [Retain] 当前在线节点列表
    └── capability_index           ← [Retain] 按能力索引的节点映射
```

---

## 3. 消息格式与设置

### 3.1 核心数据模型

**TaskMessage（任务输入）：**
```python
@dataclass
class TaskMessage:
    task_id: str          # 唯一任务ID（自动生成或指定）
    type: str             # 任务类型（用于能力匹配，如 "scan", "analyse"）
    input: dict           # 任务输入参数
    priority: int = 3     # 优先级 1-5（1最高）
    timeout: int = 300    # 超时秒数
    created_at: str       # 创建时间戳（自动填充）
    resources: list       # 资源需求列表
```

**TaskOutput（任务输出）：**
```python
@dataclass
class TaskOutput:
    task_id: str          # 对应任务ID
    agent_id: str         # 执行该任务的WorkerAgent ID
    status: str           # completed | failed
    result: Any           # 执行结果
    error: Optional[dict] # 错误信息 {type, msg, partial_result}
    metrics: dict         # 执行指标 {duration_sec, errors, warnings}
```

### 3.2 消息通信流程

```
AgentBoard                              WorkerAgent
    │                                        │
    │ 1. PUBLISH board/task/{id}/input ──────►│  (Retain, QoS 1, 含HMAC签名)
    │ 2. PUBLISH board/task/{id}/status ─────►│  "pending"
    │ 3. PUBLISH board/open ─────────────────►│  任务ID
    │                                        │
    │                                        │── 4. 验签 + 能力匹配
    │                                        │── 5. PUBLISH board/task/{id}/claim
    │                                        │── 6. PUBLISH board/task/{id}/status "running"
    │                                        │── 7. PUBLISH node/worker01/status "busy"
    │                                        │
    │                        8. stream_out()──►  PUBLISH board/task/{id}/stdout (流式)
    │                        9. stream_err()──►  PUBLISH board/task/{id}/stderr (流式)
    │                                        │
    │ 10. SUBSCRIBE board/task/{id}/output ◄──│  (等待结果)
    │ 11. complete() ────────────────────────►│  PUBLISH board/task/{id}/output
    │                              ──────────►│  PUBLISH board/task/{id}/signal "[ROUND_END]"
    │                              ──────────►│  PUBLISH board/task/{id}/status "done"
    │ 12. 收到 output + signal ←──────────────│
    │                                        │── 13. PUBLISH node/worker01/status "online"
```

### 3.3 消息设置指南

#### 设置 Retain（保留消息）

| 主题 | Retain | QoS | 原因 |
|------|--------|-----|------|
| `board/task/{id}/input` | ✅ True | 1 | 新Worker上线能立即看到待办任务 |
| `board/task/{id}/output` | ✅ True | 1 | Master重启后能读取已完成任务的结果 |
| `board/task/{id}/signal` | ✅ True | 2 | 信号精确一次，新订阅者立即感知状态 |
| `board/task/{id}/status` | ✅ True | 1 | 任务状态持久化 |
| `node/{id}/status` | ✅ True+LWT | 1 | LWT自动发布offline |
| `board/task/{id}/stdout` | ❌ False | 0 | 流式输出，无需保留历史 |
| `board/task/{id}/stderr` | ❌ False | 0 | 流式错误，无需保留历史 |

#### 设置 QoS（服务质量）

| QoS | 含义 | 适用场景 |
|-----|------|---------|
| 0 | 最多一次（可能丢失） | stdout/stderr 流式输出 |
| 1 | 至少一次（可能重复） | input/output/status 任务数据 |
| 2 | 恰好一次（性能损失） | signal 任务控制信号 |

---

## 4. 角色使用指南

### 4.1 AgentBoard（主智能体 — 发布任务方）

```python
from Mqtt_bbs import AgentBoard

# 创建主智能体（自动连接本地 RMQTT 127.0.0.1:1883）
board = AgentBoard("master")

# 发布任务
task_id = board.post_task(
    task_type="analyse_network",           # 任务类型（Worker靠此匹配）
    task_input={"target": "10.0.0.0/24"},  # 输入参数
    priority=3,                            # 优先级
)

# 等待结果（订阅推送，无需轮询）
result = board.wait_task(task_id, timeout=120)
print(f"状态: {result.status}")
print(f"结果: {result.result}")

# 或取消任务
# board.cancel_task(task_id)
```

**使用 `with` 上下文自动管理连接：**
```python
with AgentBoard("master") as board:
    tid = board.post_task("scan", {"host": "localhost"})
    result = board.wait_task(tid)
```

### 4.2 WorkerAgent（工作智能体 — 执行任务方）

```python
from Mqtt_bbs import WorkerAgent

def scan_handler(task):
    """任务处理函数，接收 TaskMessage，返回结果"""
    target = task.input.get("target")
    # ... 执行扫描逻辑 ...
    return {"hosts_found": 5, "status": "ok"}

# 创建工作智能体，声明能力
worker = WorkerAgent(
    agent_id="worker_01",
    capabilities=["scan", "analyse_network"]  # 只认领这些类型的任务
)

# 注册任务处理函数
worker.on_task(scan_handler)

# 启动消息循环（自动连接、订阅、认领和执行）
worker.start(block=True)
# block=True 阻塞当前线程；block=False 则在后台运行
```

**实时流式输出：**
```python
def long_task_handler(task):
    worker.stream_out("开始扫描...")
    # ... 执行中 ...
    worker.stream_out(f"发现 {n} 个主机")
    worker.stream_err(f"[WARN] 目标22端口超时")
    return {"hosts_found": n}

worker.on_task(long_task_handler)
```

### 4.3 多个 Worker 并行分发的 Map-Reduce 示例

```python
# === Master 端 ===
board = AgentBoard("master")

# 分发 5 个并行扫描任务
task_ids = []
for i in range(5):
    tid = board.post_task("scan", {"subnet": f"10.0.0.{i}/24"})
    task_ids.append(tid)

# 通配符订阅收集所有结果（一行代码 Map-Reduce）
results = []
for tid in task_ids:
    result = board.wait_task(tid, timeout=300)
    results.append(result)

# 汇总
all_hosts = sum(r.result.get("hosts_found", 0) for r in results if r.status == "completed")
print(f"总数: {all_hosts} 台主机")

# === Worker 端（可在不同机器上启动多个）===
# worker_1.py
WorkerAgent("worker_01", capabilities=["scan"]).on_task(scan_handler).start(block=True)
# worker_2.py
WorkerAgent("worker_02", capabilities=["scan"]).on_task(scan_handler).start(block=True)
```

### 4.4 运行时干预（动态注入指令）

```python
# Master 端：向正在执行的 Worker 注入指令
board.publish(f"board/task/{task_id}/intervene",
    {"action": "skip_step", "step": "port_scan", "reason": "用户取消"})

# Worker 端自动收到干预，通过 get_interventions() 获取
def smart_handler(task):
    while processing:
        cmds = worker.get_interventions()
        for cmd in cmds:
            if cmd["action"] == "skip_step":
                continue  # 跳过某步骤
        # ... 继续执行 ...
```

### 4.5 全局广播

```python
# Master：暂停所有Worker
board.publish("board/global/signal", "[SUSPEND]")

# 恢复
board.publish("board/global/signal", "[RESUME]")

# 全部关机
board.publish("board/global/signal", "[SHUTDOWN]")
```

---

## 5. 与飞书 Bot 集成

飞书 Bot (`frontends/fsapp.py`) 和 MQTT BBS 可无缝协同工作：

### 方案一：飞书触发 MQTT 任务

在 `fsapp.py` 中集成 `AgentBoard`，飞书消息可触发 MQTT 任务分发：

```python
# 在 fsapp.py 的消息处理器中添加
from Mqtt_bbs import AgentBoard

board = AgentBoard("feishu_bridge")

def handle_message(data):
    if data.startswith("/task "):
        # 解析飞书命令 → 发布 MQTT 任务
        task_type = data.split()[1]
        task_input = {"query": " ".join(data.split()[2:])}
        tid = board.post_task(task_type, task_input)
        result = board.wait_task(tid, timeout=120)
        send_message(chat_id, f"✅ 任务完成:\n{result.result}")
```

### 方案二：MQTT Worker 通过飞书通知

Worker 完成任务后，结果自动通过飞书 Bot 推送：

```python
# 在 fsapp.py 中订阅 MQTT 完成事件
def on_task_complete(topic, payload):
    # 解析完成的任务结果
    # 通过飞书卡片发送给用户
    pass

# fsapp 启动时订阅
# mqtt_client.subscribe("board/task/+/output", on_task_complete)
```

### 共存注意事项

| 方面 | 飞书 Bot | MQTT BBS | 冲突？ |
|------|---------|----------|--------|
| 连接方式 | 出站 WebSocket → 飞书服务器 | 本地 TCP :1883 | ❌ 无冲突 |
| 依赖库 | `lark-oapi` | `paho-mqtt` | ❌ 无冲突 |
| 端口 | 无监听端口 | 1883 (MQTT) | ❌ 无冲突 |
| 进程 | 独立进程 | 独立进程或线程 | ❌ 可共存 |

---

## 6. 配置说明

所有配置项在 `Mqtt_bbs/config.py` 中，可通过环境变量覆盖：

```python
BROKER_HOST = "127.0.0.1"       # 环境变量: MQTT_HOST
BROKER_PORT = 1883              # 环境变量: MQTT_PORT
TOPIC_PREFIX = "agent/"         # 主题前缀（多团队隔离）
KEEPALIVE = 60                  # MQTT 心跳间隔（秒）
HMAC_SECRET = "..."             # 环境变量: MQTT_HMAC_SECRET
```

**推荐开发环境：** 启动本地 RMQTT
```bash
# 已安装于 D:\tools\rmqtt-0.20.0
cd D:\tools\rmqtt-0.20.0\rmqtt-0.20.0-x86_64-pc-windows
bin\rmqttd.exe -f etc\rmqtt.toml
```

**生产环境：** 使用环境变量指向远程 Broker
```bash
set MQTT_HOST=your-broker.com
set MQTT_PORT=1883
set MQTT_HMAC_SECRET=your-secret-key
```

---

## 7. 高级功能

### 能力声明与自动匹配

WorkerAgent 上线时自动广播能力清单 (`node/{agent_id}/capability`)：

```python
worker = WorkerAgent("worker_gpu", capabilities=[
    "image_processing",
    "model_inference",
    "video_analysis"
])
```

AgentBoard 发布任务时，只有能力匹配的 Worker 会认领。

### 持久化（可选 MariaDB）

启用持久化后，所有 Retain 消息和任务状态保存到 MariaDB：

```python
# 使用持久化版
from Mqtt_bbs.persistence import AgentBoardWithPersistence, WorkerAgentWithPersistence
```

### 监控面板

```bash
streamlit run frontends/dashboard_mqtt.py
```

---

## 项目结构

```
Mqtt_bbs/
├── __init__.py          # 导出 AgentBoard, WorkerAgent
├── config.py            # 配置（BROKER_HOST/PORT/HMAC_SECRET）
├── client.py            # BBSClient MQTT 底层封装 + 数据模型
├── bbs.py               # AgentBoard + WorkerAgent 业务逻辑
├── persistence.py       # MariaDB 持久化版本
├── bbs_nopersistence.py # 无持久化版本
├── enums.py             # 枚举类型
├── README.md            # 中文文档（本文件）
└── README_EN.md         # English documentation
```
