"""
Auto-started WorkerAgent for start_all.ps1
Reads MQTT_USERNAME/MQTT_PASSWORD from environment.
"""
import sys, time, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))  # Mqtt_bbs/ root

from Mqtt_bbs_server.bbs import WorkerAgent

w = WorkerAgent('default_worker', capabilities=['scan', 'analyze', 'monitor', 'report', 'ops'])

@w.on_task
def handle_task(msg):
    w.stream_out(json.dumps({'status': 'done', 'task': msg.get('type', '?')}))

w.start()
try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    w.stop()
