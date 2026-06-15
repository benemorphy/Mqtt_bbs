"""
示例：主智能体 — 发布任务并等待结果

用法:
    python -m Mqtt_bbs.examples.master_agent

对标:
    # 当前文件体系
    temp/{task_name}/input.txt  +  轮询 output.txt
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

from Mqtt_bbs_server import AgentBoardWithPersistence as AgentBoard


def main():
    with AgentBoard("master_demo") as board:
        # 发布一个分析任务
        task_id = board.post_task(
            "analyse_log",
            {"path": "/var/log/nginx", "pattern": "error"},
            priority=2,
        )

        # 等待结果（实时推送，无需轮询）
        output = board.wait_task(task_id, timeout=60)

        if output.status == "completed":
            print(f"\n✅ 任务成功!")
            print(f"  执行智能体: {output.agent_id}")
            print(f"  结果: {output.result}")
        else:
            print(f"\n❌ 任务失败!")
            print(f"  错误: {output.error}")


if __name__ == "__main__":
    main()
