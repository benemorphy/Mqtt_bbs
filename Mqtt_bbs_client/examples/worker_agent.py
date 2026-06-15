"""
示例：工作智能体 — 认领任务、执行、流式反馈、完成

用法:
    python -m Mqtt_bbs.examples.worker_agent    # 自动认领 'analyse_log' 任务
    python -m Mqtt_bbs.examples.worker_agent --task scan

对标:
    # 当前 subagent
    agentmain.py --task {name}  →  读 input.txt  →  写 output.txt  →  [ROUND END]
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

from Mqtt_bbs_server import WorkerAgentWithPersistence as WorkerAgent


def analyse_log_handler(task):
    """模拟日志分析任务"""
    agent.stream_out(f"收到任务: {task.type}")
    agent.stream_out(f"目标: {task.input.get('path', 'N/A')}")

    # 模拟执行过程
    for i in range(5):
        time.sleep(1)
        agent.stream_out(f"分析中... {i*20}%")

    # 模拟一个告警
    agent.stream_err(f"发现2个超时连接")

    return {
        "total_lines": 1024,
        "errors": 3,
        "warnings": 12,
        "top_ips": ["10.0.0.1", "10.0.0.2"],
    }


def main():
    global agent
    agent = WorkerAgent(
        "worker_demo",
        capabilities=["analyse_log", "scan"],
    )

    agent.on_task(analyse_log_handler)
    agent.start(block=True)


if __name__ == "__main__":
    main()
