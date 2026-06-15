"""MQTT Agent Runner — 将 GeneraticAgent 包装为 MQTT WorkerAgent。

提取自 agentmain.py __main__，将 MQTT 耦合代码隔离到独立模块。

用法:
    python -m Mqtt_bbs.mqtt_agent_runner [--broker_host HOST] [--broker_port PORT]
    # 或通过 agentmain.py __main__ 自动调用 (向后兼容)
    
    # 编程方式:
    from Mqtt_bbs_server.mqtt_agent_runner import start_mqtt_agent
    start_mqtt_agent(args_as_namespace)
"""

import os, sys, threading

# 确保能找到上级模块
_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent not in sys.path:
    sys.path.insert(0, _parent)


def start_mqtt_agent(args):
    """创建 GeneraticAgent + MQTT WorkerAgent，启动消息循环。
    
    Args:
        args: 命名空间对象，需包含:
            - broker_host (str, 默认 "localhost")
            - broker_port (int, 默认 1883)
            - llm_no (int, 默认 0)
            - verbose (bool, 默认 False)
    
    Raises:
        ImportError: paho-mqtt 未安装时抛出
    """
    from agentmain import GeneraticAgent
    from Mqtt_bbs_server import WorkerAgentWithPersistence as WorkerAgent
    import logging as _l; _l.basicConfig(level=_l.WARNING)

    agent = GeneraticAgent()
    agent.next_llm(getattr(args, 'llm_no', 0))
    agent.verbose = getattr(args, 'verbose', False)
    threading.Thread(target=agent.run, daemon=True).start()

    # MQTT WorkerAgent 模式
    agent.peer_hint = False
    _mqtt_agent_id = f"agent_{os.urandom(4).hex()}"
    worker = WorkerAgent(
        _mqtt_agent_id,
        host=getattr(args, 'broker_host', 'localhost'),
        port=getattr(args, 'broker_port', 1883),
    )

    def _mqtt_handler(msg):
        dq = agent.put_task(msg.input, source='mqtt')
        while 'done' not in (item := dq.get(timeout=300)):
            if 'next' in item:
                worker.stream_out(msg.task_id, item.get('next', ''))
        result = item['done']
        worker.complete(msg.task_id, result=result)

    worker.on_task(_mqtt_handler)
    print(f"[MQTT] WorkerAgent 已启动 (host={worker._client.host}:{worker._client.port})")
    worker.start(block=True)


def main():
    """CLI entry point for running as: python -m Mqtt_bbs.mqtt_agent_runner"""
    import argparse
    parser = argparse.ArgumentParser(description='MQTT Agent Runner')
    parser.add_argument('--broker_host', default='localhost', help='MQTT broker host')
    parser.add_argument('--broker_port', type=int, default=1883, help='MQTT broker port')
    parser.add_argument('--llm_no', type=int, default=0, help='LLM model index')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    args = parser.parse_args()

    try:
        start_mqtt_agent(args)
    except ImportError as e:
        print(f"[MQTT] 初始化失败: {e} (need pip install paho-mqtt)")
        sys.exit(1)


if __name__ == '__main__':
    main()
