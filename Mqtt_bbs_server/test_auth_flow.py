#!/usr/bin/env python3
"""BoardService V2 认证流程测试 — 匿名MQTT连接 + Form注册"""
import json, time, uuid
import urllib.request, urllib.parse
import paho.mqtt.client as mqtt

BROKER = "127.0.0.1"
PORT = 1883
GATEWAY = "http://127.0.0.1:8000"

# =============================================
# STEP 1: 通过 Gateway 注册邮箱用户 (Form提交)
# =============================================
print("=" * 60)
print("STEP 1: 通过 Gateway 注册邮箱用户")
print("=" * 60)

email = f"test_{uuid.uuid4().hex[:6]}@example.com"
password = "test123"
jwt_token = ""

# 1a. 请求验证码 (Form)
url = f"{GATEWAY}/api/email/send_code"
data = urllib.parse.urlencode({"email": email}).encode()
req = urllib.request.Request(url, data=data)
try:
    resp = urllib.request.urlopen(req, timeout=5)
    print(f"  [OK] 验证码已发送: {resp.read().decode()[:80]}")
except Exception as e:
    print(f"  [WARN] send_code 失败: {e}")

# 1b. 注册 (Form: email + verify_code + password)
url = f"{GATEWAY}/api/email/register"
data = urllib.parse.urlencode({
    "email": email, "verify_code": "000000", "password": password
}).encode()
req = urllib.request.Request(url, data=data)
try:
    resp = urllib.request.urlopen(req, timeout=5)
    body = resp.read().decode()
    print(f"  [OK] 注册响应: {body[:150]}")
    # 如果是 JSON 且包含 token
    try:
        result = json.loads(body)
        jwt_token = result.get("token", "")
    except:
        pass
except Exception as e:
    print(f"  [WARN] register 失败: {e}")

# 如果没有 token，试试 /api/email/login
if not jwt_token:
    url = f"{GATEWAY}/api/email/login"
    data = urllib.parse.urlencode({"email": email, "password": password}).encode()
    req = urllib.request.Request(url, data=data)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        print(f"  [OK] login 成功: HTTP {resp.status}")
    except Exception as e:
        print(f"  [WARN] email login 失败: {e}")

    # 也试试旧版 /api/login (Form)
    url = f"{GATEWAY}/api/login"
    data = urllib.parse.urlencode({"username": email, "password": password}).encode()
    req = urllib.request.Request(url, data=data)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        print(f"  [OK] /api/login 成功: HTTP {resp.status}")
    except Exception as e:
        print(f"  [FAIL] /api/login 也失败: {e}")

if not jwt_token:
    print("  [WARN] 未获取到 JWT token，继续匿名MQTT测试")

# =============================================
# STEP 2: 匿名 MQTT 连接 + 注册
# =============================================
print("\n" + "=" * 60)
print("STEP 2: 匿名 MQTT 连接 -> 注册")
print("=" * 60)

registered_jwt = ""
registered_token = ""
board_name = "test_user"
reg_resp_received = False

def on_connect(client, userdata, flags, rc, props=None):
    print(f"  [MQTT] 连接结果 rc={rc}", end="")
    if rc == 0:
        print(" (成功)")
        # 连接成功后立即发送注册请求
        reg_payload = {
            "name": board_name,
            "agent_id": "test_agent",
            "reply_to": "agent/bbs/test/register/response/",
        }
        if jwt_token:
            reg_payload["token"] = jwt_token
        reg_topic = "agent/bbs/test/register"
        client.publish(reg_topic, json.dumps(reg_payload), qos=1)
        print(f"  [发送] topic={reg_topic}")
        print(f"         name={board_name}, has_token={'yes' if jwt_token else 'no'}")
    else:
        print(" (失败)")

def on_message(client, userdata, msg):
    global registered_jwt, registered_token, reg_resp_received
    try:
        payload = json.loads(msg.payload)
        print(f"\n  [MQTT] 收到响应: topic={msg.topic}")
        print(f"          payload={json.dumps(payload, indent=2)[:300]}")
        registered_jwt = payload.get("jwt", "")
        registered_token = payload.get("token", "")
        reg_resp_received = True
    except Exception as e:
        print(f"  [MQTT] 响应解析失败: {e}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

# 匿名连接 (no username/password)
client.connect(BROKER, PORT, 60)
client.subscribe("agent/bbs/test/register/response/+", qos=1)
print(f"  订阅响应 topic: agent/bbs/test/register/response/+")

client.loop_start()
time.sleep(5)

# =============================================
# STEP 3: 发布消息
# =============================================
print("\n" + "=" * 60)
print("STEP 3: 发布消息到 BoardService")
print("=" * 60)

if reg_resp_received:
    post_payload = {
        "name": board_name,
        "content": f"测试消息 from {board_name} @ {time.time()}",
        "token": registered_jwt or registered_token,
    }
    client.publish("agent/bbs/test/post", json.dumps(post_payload), qos=1)
    print(f"  [发送] 已发布 (token={registered_token[:8] if registered_token else 'anonymous'})")
    time.sleep(2)

    # STEP 4: 查询
    print("\n" + "=" * 60)
    print("STEP 4: 查询消息")
    print("=" * 60)
    query_payload = {
        "name": board_name,
        "query": {"board": "test", "limit": 5},
        "reply_to": "agent/bbs/test/query/response/",
    }
    client.publish("agent/bbs/test/query", json.dumps(query_payload), qos=1)
    print(f"  [发送] 查询已发布")
    time.sleep(3)
else:
    print("  未收到注册响应，跳过发布/查询步骤")

client.loop_stop()
client.disconnect()

print("\n" + "=" * 60)
status = "成功" if reg_resp_received else "失败(未收到注册响应)"
print(f"测试{status}!")
if reg_resp_received:
    print(f"  短 token: {registered_token}")
    print(f"  业务 JWT: {registered_jwt[:40] if registered_jwt else '无'}...")
print("=" * 60)
