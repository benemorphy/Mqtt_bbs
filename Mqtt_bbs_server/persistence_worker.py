"""
MariaDB 持久化 Worker — 常驻运行，保存所有 MQTT BBS 消息到 MariaDB

启动:
    python Mqtt_bbs/persistence_worker.py
"""
import json, logging, time, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

from Mqtt_bbs_server.persistence import BBSClientWithPersistence

def main():
    worker = BBSClientWithPersistence("persist_writer")
    worker.connect()
    worker.wait_connected(5)
    
    # 订阅全量主题，记录所有 MQTT 消息到 session_queue
    def _on_any(topic, payload):
        try:
            # 兼容 bytes/dict/str 三种 payload 类型
            if isinstance(payload, bytes):
                payload_str = payload.decode('utf-8', errors='replace')
            else:
                payload_str = json.dumps(payload, ensure_ascii=False) if not isinstance(payload, str) else payload
            worker._db.execute(
                "INSERT INTO session_queue (target_agent, topic, payload, qos, is_retained) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("persist_writer", topic, payload_str, 1, 0)
            )
        except Exception as e:
            log.warning(f"DB写入失败: {topic} -> {e}")
    
    worker.subscribe("#", _on_any)
    log.info("[SIGNAL] 持久化 Worker: 已订阅全量主题 #，开始记录所有 MQTT 消息到 session_queue")
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        worker.stop()

if __name__ == "__main__":
    main()
