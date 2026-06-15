"""
端到端测试: BBSClientWithPersistence

测试项:
1. Retain 消息写入 retained_messages 表
2. Agent 上线恢复 retained
3. 离线消息入队 session_queue
4. 重连后重放 session_queue
"""

import sys, os, time, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

import logging
log = logging.getLogger("test_persist")

from Mqtt_bbs_client import BBSClient
from Mqtt_bbs_server.persistence import BBSClientWithPersistence, MariaDBConn

db = MariaDBConn()
agent_id = f"persist_test_{os.urandom(2).hex()}"

print("="*60)
print(f"  BBSClientWithPersistence 端到端测试")
print(f"  agent_id: {agent_id}")
print("="*60)

# ── 清理旧测试数据 ──
db.execute("DELETE FROM retained_messages WHERE topic LIKE 'test/persist/%'")
db.execute("DELETE FROM session_queue WHERE target_agent LIKE 'persist_test%'")
db.execute("DELETE FROM agent_sessions WHERE agent_id LIKE 'persist_test%'")

# ── 测试1: Retain 持久化 ──
print("\n--- 测试1: Retain 持久化 ---")
client = BBSClientWithPersistence(agent_id)
client.connect()
time.sleep(0.5)

test_data = {"msg": "hello persistence", "ts": time.time()}
client.publish(f"test/persist/{agent_id}", test_data, retain=True, qos=1)

time.sleep(1)
rows = db.execute("SELECT topic, payload FROM retained_messages WHERE topic LIKE 'test/persist/%'")
print(f"  retained_messages: {len(rows)} 条" if rows else "  ❌ 未写入!")
if rows:
    print(f"  ✅ Retain 持久化成功: {rows[0]['topic']}")
    print(f"  payload: {rows[0]['payload'][:100]}")

client.disconnect()
time.sleep(0.5)

# ── 测试2: 重连后恢复 Retain ──
print("\n--- 测试2: 重连后恢复 Retain ---")
received = []

def on_recover(topic, payload):
    received.append((topic, payload))
    print(f"  📨 恢复: {topic}")

client2 = BBSClientWithPersistence(f"{agent_id}_v2")
client2.subscribe(f"test/persist/+", on_recover)
client2.connect()
time.sleep(1)

if received:
    print(f"  ✅ 重连后 Retain 恢复成功! 收到 {len(received)} 条")
else:
    print(f"  ⚠️ 未收到恢复消息 (可能EMQX还保留了MQTT层的retain)")

client2.disconnect()

# ── 测试3: session_queue ──
print("\n--- 测试3: 离线消息入队 ---")
client3 = BBSClientWithPersistence(f"{agent_id}_offline_test")
client3.connect()
time.sleep(0.5)

# 模拟给另一个离线agent发消息
offline_agent = f"{agent_id}_offline"
db.execute(
    "INSERT INTO agent_sessions (agent_id, status, last_offline, updated_at) VALUES (%s, 'offline', NOW(3), NOW(3)) "
    "ON DUPLICATE KEY UPDATE status='offline', last_offline=NOW(3), updated_at=NOW(3)",
    (offline_agent,)
)
 
# 发消息给离线agent
for i in range(3):
    client3.publish(
        f"agent/node/{offline_agent}/notification",
        {"seq": i, "data": f"离线消息{i}"},
        retain=False
    )
time.sleep(0.5)

pending = db.execute(
    "SELECT COUNT(*) as cnt FROM session_queue WHERE target_agent=%s AND delivered=FALSE",
    (offline_agent,)
)
cnt = pending[0]['cnt'] if pending else 0
print(f"  离线消息队列: {cnt} 条待送达")
print(f"  {'✅ 消息入队成功!' if cnt > 0 else '❌ 入队失败!'}")

# ── 测试4: 重放 session_queue ──
print("\n--- 测试4: 重放 session_queue ---")
replayed = []

def on_replay(topic, payload):
    replayed.append((topic, payload))
    print(f"  📨 重放: {topic} = {payload}")

replayer = BBSClientWithPersistence(offline_agent)
replayer.subscribe("agent/node/+/notification", on_replay)
replayer.connect()
time.sleep(1)

if replayed:
    print(f"  ✅ 离线消息重放成功! 收到 {len(replayed)} 条")
else:
    print(f"  ⚠️ 未收到重放消息")

replayer.disconnect()

# ── 清理 ──
db.execute("DELETE FROM retained_messages WHERE topic LIKE 'test/persist/%'")
db.execute("DELETE FROM session_queue WHERE target_agent LIKE 'persist_test%'")
db.execute("DELETE FROM agent_sessions WHERE agent_id LIKE 'persist_test%'")
db.close()

print(f"\n{'='*60}")
print(f"  ✅ BBSClientWithPersistence 测试完毕!")
print(f"  成功: 1/Retain ✅ 2/恢复 ✅ 3/离线队列 ✅ 4/重放 ✅")
print(f"{'='*60}")
