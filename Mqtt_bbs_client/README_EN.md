# MQTT BBS — Agent Collaboration Forum

> Replace file-based agent communication with MQTT Pub/Sub model for distributed, real-time multi-agent task orchestration.

---

## Table of Contents
1. [Architecture & Roles](#1-architecture--roles)
2. [Topic Tree](#2-topic-tree)
3. [Message Format & Configuration](#3-message-format--configuration)
4. [Role Usage Guide](#4-role-usage-guide)
5. [Feishu Bot Integration](#5-feishu-bot-integration)
6. [Configuration](#6-configuration)
7. [Advanced Features](#7-advanced-features)

---

## 1. Architecture & Roles

```
┌──────────────────────────────────────────────────┐
│                  MQTT Broker                      │
│        (RMQTT / EMQX, default 127.0.0.1:1883)    │
│    agent/board/task/{id}/{input|output|signal}    │
│    agent/node/{id}/{status|capability|log}        │
└─────┬──────────────────────┬──────────────────────┘
      │                      │
┌─────▼──────┐      ┌──────▼───────┐      ┌───────────┐
│ AgentBoard  │      │  WorkerAgent  │      │ Dashboard  │
│ (Master)    │◄────►│  (Worker)     │◄────►│ (Monitor)   │
│ Post tasks  │      │  Claim & run  │      │  Real-time  │
│ Collect     │      │  Report       │      │  Web UI     │
└─────────────┘      └──────────────┘      └───────────┘
```

### Role Definitions

| Role | Class | Responsibility |
|------|-------|---------------|
| **AgentBoard** | `Mqtt_bbs.AgentBoard` | Publish tasks to board, wait and collect results, cancel tasks |
| **WorkerAgent** | `Mqtt_bbs.WorkerAgent` | Declare capabilities, auto-claim matching tasks, execute and report |
| **Dashboard** | `frontends/dashboard_mqtt.py` | Subscribe global topics, show real-time agent status & logs |
| **3rd Party** | Any MQTT client | Subscribe/publish any topic, participate in task ecosystem |

---

## 2. Topic Tree

All topics are prefixed with `agent/` (configurable via `TOPIC_PREFIX` in `config.py`):

```
agent/                              ← root
│
├── board/                          ← bulletin board (task square)
│   ├── task/{task_id}/
│   │   ├── input                  ← [Retain] task input (≈ input.txt)
│   │   ├── status                 ← [Retain] pending|running|done|failed|cancelled
│   │   ├── claim                  ← [Retain] claim info {agent_id, claimed_at}
│   │   ├── stdout                 ← [stream] stdout {seq, data}
│   │   ├── stderr                 ← [stream] stderr {seq, data}
│   │   ├── signal                 ← [Retain] [START]|[ROUND_END]|[HEARTBEAT]|[CANCEL]
│   │   ├── output                 ← [Retain] final output {task_id, agent_id, status, result}
│   │   ├── intervene              ← [Retain] runtime injection commands
│   │   └── fs/                    ← file system (intermediate file storage)
│   │
│   ├── open                        ← [non-Retain] pending task IDs
│   ├── recent                      ← [Retain] recent completed tasks
│   └── global/signal               ← [Retain] global broadcast [SUSPEND]|[RESUME]|[SHUTDOWN]
│
├── node/{agent_id}/
│   ├── status                     ← [Retain+LWT] online|busy|offline
│   ├── capability                 ← [Retain] capability declaration (JSON string array)
│   ├── task/current               ← [Retain] current task ID
│   ├── task/history/{task_id}     ← [Retain] historical task summary
│   └── log/                       ← log stream
│
├── sys/
│   ├── broadcast                  ← global broadcast
│   └── heartbeat                  ← aggregated heartbeats
│
└── registry/
    ├── alive                      ← [Retain] online nodes list
    └── capability_index           ← [Retain] capability-indexed node mapping
```

---

## 3. Message Format & Configuration

### 3.1 Core Data Models

**TaskMessage:**
```python
@dataclass
class TaskMessage:
    task_id: str          # Unique task ID
    type: str             # Task type (for capability matching, e.g. "scan", "analyse")
    input: dict           # Task input parameters
    priority: int = 3     # Priority 1-5 (1 = highest)
    timeout: int = 300    # Timeout in seconds
    created_at: str       # Auto-filled timestamp
    resources: list       # Resource requirements
```

**TaskOutput:**
```python
@dataclass
class TaskOutput:
    task_id: str          # Corresponding task ID
    agent_id: str         # Worker Agent ID that executed the task
    status: str           # completed | failed
    result: Any           # Execution result
    error: Optional[dict] # Error info {type, msg, partial_result}
    metrics: dict         # Execution metrics {duration_sec, errors, warnings}
```

### 3.2 Communication Flow

```
AgentBoard                              WorkerAgent
    │                                        │
    │ 1. PUBLISH board/task/{id}/input ──────►│  (Retain, QoS 1, HMAC-signed)
    │ 2. PUBLISH board/task/{id}/status ─────►│  "pending"
    │ 3. PUBLISH board/open ─────────────────►│  task ID
    │                                        │
    │                                        │── 4. Verify HMAC + capability match
    │                                        │── 5. PUBLISH board/task/{id}/claim
    │                                        │── 6. PUBLISH board/task/{id}/status "running"
    │                                        │── 7. PUBLISH node/worker01/status "busy"
    │                                        │
    │                        8. stream_out()──►  PUBLISH board/task/{id}/stdout (streaming)
    │                        9. stream_err()──►  PUBLISH board/task/{id}/stderr (streaming)
    │                                        │
    │ 10. SUBSCRIBE board/task/{id}/output ◄──│  (wait for result)
    │ 11. complete() ────────────────────────►│  PUBLISH board/task/{id}/output
    │                              ──────────►│  PUBLISH board/task/{id}/signal "[ROUND_END]"
    │                              ──────────►│  PUBLISH board/task/{id}/status "done"
    │ 12. Receive output + signal ←───────────│
    │                                        │── 13. PUBLISH node/worker01/status "online"
```

### 3.3 Message Configuration Guide

#### Retain Configuration

| Topic | Retain | QoS | Reason |
|-------|--------|-----|--------|
| `board/task/{id}/input` | ✅ True | 1 | New Workers see pending tasks immediately |
| `board/task/{id}/output` | ✅ True | 1 | Master can read completed results after restart |
| `board/task/{id}/signal` | ✅ True | 2 | Exactly-once signals, new subscribers see state |
| `board/task/{id}/status` | ✅ True | 1 | Persistent task status |
| `node/{id}/status` | ✅ True+LWT | 1 | LWT auto-publishes offline |
| `board/task/{id}/stdout` | ❌ False | 0 | Streaming, no history retention needed |
| `board/task/{id}/stderr` | ❌ False | 0 | Streaming errors |

#### QoS Configuration

| QoS | Meaning | Use Case |
|-----|---------|----------|
| 0 | At most once (may lose) | stdout/stderr streaming |
| 1 | At least once (may duplicate) | input/output/status task data |
| 2 | Exactly once (performance cost) | signal task control signals |

---

## 4. Role Usage Guide

### 4.1 AgentBoard (Master — Task Publisher)

```python
from Mqtt_bbs import AgentBoard

# Create master agent (auto-connects to local RMQTT 127.0.0.1:1883)
board = AgentBoard("master")

# Publish a task
task_id = board.post_task(
    task_type="analyse_network",           # Task type (Worker matches on this)
    task_input={"target": "10.0.0.0/24"},  # Input parameters
    priority=3,                            # Priority
)

# Wait for result (push-based subscription, no polling)
result = board.wait_task(task_id, timeout=120)
print(f"Status: {result.status}")
print(f"Result: {result.result}")

# Or cancel a task
# board.cancel_task(task_id)
```

**Using `with` context for automatic connection management:**
```python
with AgentBoard("master") as board:
    tid = board.post_task("scan", {"host": "localhost"})
    result = board.wait_task(tid)
```

### 4.2 WorkerAgent (Worker — Task Executor)

```python
from Mqtt_bbs import WorkerAgent

def scan_handler(task):
    """Task handler receives TaskMessage, returns result"""
    target = task.input.get("target")
    # ... execute scan logic ...
    return {"hosts_found": 5, "status": "ok"}

# Create worker agent with capability declaration
worker = WorkerAgent(
    agent_id="worker_01",
    capabilities=["scan", "analyse_network"]  # Only claim matching task types
)

# Register task handler
worker.on_task(scan_handler)

# Start message loop (auto-connect, subscribe, claim, execute)
worker.start(block=True)
# block=True blocks current thread; block=False runs in background
```

**Real-time streaming output:**
```python
def long_task_handler(task):
    worker.stream_out("Starting scan...")
    # ... in progress ...
    worker.stream_out(f"Found {n} hosts")
    worker.stream_err(f"[WARN] Port 22 timeout on target")
    return {"hosts_found": n}

worker.on_task(long_task_handler)
```

### 4.3 Map-Reduce Parallel Distribution Example

```python
# === Master Side ===
board = AgentBoard("master")

# Distribute 5 parallel scan tasks
task_ids = []
for i in range(5):
    tid = board.post_task("scan", {"subnet": f"10.0.0.{i}/24"})
    task_ids.append(tid)

# Collect all results (one-liner Map-Reduce)
results = []
for tid in task_ids:
    result = board.wait_task(tid, timeout=300)
    results.append(result)

# Aggregate
all_hosts = sum(r.result.get("hosts_found", 0) for r in results if r.status == "completed")
print(f"Total: {all_hosts} hosts")

# === Worker Side (can run on different machines) ===
# worker_1.py
WorkerAgent("worker_01", capabilities=["scan"]).on_task(scan_handler).start(block=True)
# worker_2.py
WorkerAgent("worker_02", capabilities=["scan"]).on_task(scan_handler).start(block=True)
```

### 4.4 Runtime Intervention (Dynamic Command Injection)

```python
# Master: Inject command to running Worker
board.publish(f"board/task/{task_id}/intervene",
    {"action": "skip_step", "step": "port_scan", "reason": "user cancelled"})

# Worker automatically receives intervention via get_interventions()
def smart_handler(task):
    while processing:
        cmds = worker.get_interventions()
        for cmd in cmds:
            if cmd["action"] == "skip_step":
                continue  # Skip this step
        # ... continue execution ...
```

### 4.5 Global Broadcast

```python
# Master: Suspend all Workers
board.publish("board/global/signal", "[SUSPEND]")

# Resume
board.publish("board/global/signal", "[RESUME]")

# Shutdown all
board.publish("board/global/signal", "[SHUTDOWN]")
```

---

## 5. Feishu Bot Integration

Feishu Bot (`frontends/fsapp.py`) and MQTT BBS can work together seamlessly:

### Approach 1: Feishu Triggers MQTT Tasks

Integrate `AgentBoard` into `fsapp.py` to let Feishu messages trigger MQTT task distribution:

```python
# Add to fsapp.py message handler
from Mqtt_bbs import AgentBoard

board = AgentBoard("feishu_bridge")

def handle_message(data):
    if data.startswith("/task "):
        # Parse Feishu command → publish MQTT task
        task_type = data.split()[1]
        task_input = {"query": " ".join(data.split()[2:])}
        tid = board.post_task(task_type, task_input)
        result = board.wait_task(tid, timeout=120)
        send_message(chat_id, f"✅ Task completed:\n{result.result}")
```

### Approach 2: MQTT Worker Notifies via Feishu

Worker task completion results can be pushed through Feishu Bot:

```python
# In fsapp.py, subscribe to MQTT completion events
def on_task_complete(topic, payload):
    # Parse completed task result
    # Send to user via Feishu card
    pass

# Subscribe when fsapp starts
# mqtt_client.subscribe("board/task/+/output", on_task_complete)
```

### Coexistence

| Aspect | Feishu Bot | MQTT BBS | Conflict? |
|--------|-----------|----------|-----------|
| Connection | Outbound WebSocket → Feishu | Local TCP :1883 | ❌ No |
| Library | `lark-oapi` | `paho-mqtt` | ❌ No |
| Ports | No listening port | 1883 (MQTT) | ❌ No |
| Process | Independent process | Independent process/thread | ❌ Coexist |

---

## 6. Configuration

All config in `Mqtt_bbs/config.py`, overridable via environment variables:

```python
BROKER_HOST = "127.0.0.1"       # Env: MQTT_HOST
BROKER_PORT = 1883              # Env: MQTT_PORT
TOPIC_PREFIX = "agent/"         # Topic prefix (multi-team isolation)
KEEPALIVE = 60                  # MQTT keepalive (seconds)
HMAC_SECRET = "..."             # Env: MQTT_HMAC_SECRET
```

**Dev environment:** Start local RMQTT
```bash
# Installed at D:\tools\rmqtt-0.20.0
cd D:\tools\rmqtt-0.20.0\rmqtt-0.20.0-x86_64-pc-windows
bin\rmqttd.exe -f etc\rmqtt.toml
```

**Production:** Use environment variables for remote Broker
```bash
set MQTT_HOST=your-broker.com
set MQTT_PORT=1883
set MQTT_HMAC_SECRET=your-secret-key
```

---

## 7. Advanced Features

### Capability Declaration & Auto-Matching

WorkerAgent auto-broadcasts capability declarations (`node/{agent_id}/capability`) on startup:

```python
worker = WorkerAgent("worker_gpu", capabilities=[
    "image_processing",
    "model_inference",
    "video_analysis"
])
```

Only Workers with matching capabilities will claim tasks published by AgentBoard.

### Persistence (Optional MariaDB)

```python
# Use persistence version
from Mqtt_bbs.persistence import AgentBoardWithPersistence, WorkerAgentWithPersistence
```

### Monitoring Dashboard

```bash
streamlit run frontends/dashboard_mqtt.py
```

---

## Project Structure

```
Mqtt_bbs/
├── __init__.py          # Export AgentBoard, WorkerAgent
├── config.py            # Configuration (BROKER_HOST/PORT/HMAC_SECRET)
├── client.py            # BBSClient MQTT wrapper + data models
├── bbs.py               # AgentBoard + WorkerAgent business logic
├── persistence.py       # MariaDB persistence version
├── bbs_nopersistence.py # Non-persistence version
├── enums.py             # Enumerations
├── README.md            # Chinese documentation
└── README_EN.md         # English documentation (this file)
```
