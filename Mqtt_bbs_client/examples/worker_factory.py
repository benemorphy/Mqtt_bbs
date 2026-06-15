"""
Worker Agent 工厂 — 通过环境变量配置

用法:
    set WORKER_ID=scanner_01
    set WORKER_CAPS=scan_network,port_scan
    python -m Mqtt_bbs.examples.worker_factory
"""

import json, os, sys, time, logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

from Mqtt_bbs_server import WorkerAgentWithPersistence as WorkerAgent

WORKER_ID = os.environ.get("WORKER_ID", "worker_factory")
WORKER_CAPS = os.environ.get("WORKER_CAPS", "analyse_log")
MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
if "," in WORKER_CAPS:
    WORKER_CAPS = [c.strip() for c in WORKER_CAPS.split(",")]
else:
    WORKER_CAPS = [WORKER_CAPS]

agent = WorkerAgent(WORKER_ID, capabilities=WORKER_CAPS, host=MQTT_HOST, port=MQTT_PORT)


def handler(task):
    """通用任务处理器"""
    agent.stream_out(f"[{WORKER_ID}] 收到任务: type={task.type}, id={task.task_id}")
    agent.stream_out(f"[{WORKER_ID}] 输入: {json.dumps(task.input, ensure_ascii=False)[:200]}")

    # 模拟执行进度
    steps = max(1, min(task.timeout // 60, 5))
    for i in range(steps):
        time.sleep(1)
        agent.stream_out(f"[{WORKER_ID}] 进度 {i+1}/{steps}")

    if hasattr(task, 'input') and isinstance(task.input, dict) and task.input.get("simulate_error"):
        agent.stream_err(f"[{WORKER_ID}] 模拟告警: 资源不足")
        return {"status": "completed", "warnings": ["resource_low"], "agent": WORKER_ID}

    return {
        "status": "completed",
        "agent": WORKER_ID,
        "capabilities": WORKER_CAPS,
        "result": f"{task.type} 由 {WORKER_ID} 完成（能力: {', '.join(WORKER_CAPS)}）",
    }


agent.on_task(handler)
print(f"[{WORKER_ID}] 🟢 就绪, 能力: {WORKER_CAPS}")
agent.start(block=True)
