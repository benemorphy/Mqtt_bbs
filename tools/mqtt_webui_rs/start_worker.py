
import sys, time, os, json
sys.path.insert(0, r"D:\open_claw_agent\GenericAgent_mqtt\Mqtt_bbs")
os.environ["MQTT_USERNAME"] = "dashboard"
os.environ["MQTT_PASSWORD"] = "eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJzdWIiOiAiZGFzaGJvYXJkIiwgImNsaWVudGlkIjogImRhc2hib2FyZCIsICJ1c2VybmFtZSI6ICJkYXNoYm9hcmQiLCAicm9sZSI6ICJvYnNlcnZlciIsICJleHAiOiAxODEwNTM1NTczLCAiaWF0IjogMTc3ODk5OTU3M30.h_4qJej8QnJ8BXOknx5fF7mBQS2obEH7d6r2sZkMpfA"
from Mqtt_bbs_server.bbs import WorkerAgent
w = WorkerAgent("default_worker", capabilities=["scan","analyze","monitor","report","ops"])
@w.on_task
def h(msg):
    w.stream_out(json.dumps({"status":"done","task":msg.get("type","?")}))
w.start()
try:
    while True: time.sleep(10)
except:
    w.stop()
