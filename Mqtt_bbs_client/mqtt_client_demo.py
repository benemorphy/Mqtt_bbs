"""
mqtt_client_demo.py — MQTT 远端客户端增强示例

展示通过 MQTT 进行注册、心跳、发布/订阅的完整生命周期。
基于 BoardService 的 JWT 认证体系。

使用方式:
  set BROKER_PASS=your_password
  python mqtt_client_demo.py

环境变量:
  BROKER_HOST  MQTT broker 地址 (默认 localhost)
  BROKER_PORT  MQTT broker 端口 (默认 1883)
  BROKER_USER  MQTT 用户名 (默认 client)
  BROKER_PASS  MQTT 密码 (必需)
  BOARD_NAME   Board 名称 (默认 client)
  BOARD_KEY    Board 标识键 (默认 example)

依赖:
  pip install paho-mqtt
"""

import json
import os
import sys
import threading
import time
import paho.mqtt.client as mqtt


def main():
    # 配置: 全部来自环境变量, 无硬编码敏感值
    broker_host = os.getenv("BROKER_HOST", "localhost")
    broker_port = int(os.getenv("BROKER_PORT", "1883"))
    broker_user = os.getenv("BROKER_USER", "client")
    broker_pass = os.environ.get("BROKER_PASS")
    board_name = os.getenv("BOARD_NAME", "client")
    board_key = os.getenv("BOARD_KEY", "example")

    if not broker_pass:
        print("[FATAL] BROKER_PASS 未设置")
        sys.exit(1)

    jwt_token = None
    registered = threading.Event()

    # ---- MQTT 回调 ----
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc != 0:
            print(f"[ERR] 连接失败 rc={rc}")
            return
        print("[OK] 已连接到 MQTT Broker")
        # 注册响应 topic (通配符捕获带 corr_id 后缀的回复)
        resp_topic = f"agent/bbs/{board_key}/register/response/#"
        client.subscribe(resp_topic)
        corr_id = str(int(time.time() * 1000))
        reg_payload = json.dumps({
            "v": 1, "action": "register",
            "source": board_name, "corr_id": corr_id, "name": board_name,
        })
        client.publish(f"agent/bbs/{board_key}/register", reg_payload)
        print(f"[注册] 请求已发送 (corr_id={corr_id})")

    def on_message(client, userdata, msg):
        nonlocal jwt_token
        try:
            data = json.loads(msg.payload)
        except json.JSONDecodeError:
            return
        if "/register/response" in msg.topic and data.get("jwt"):
            jwt_token = data["jwt"]
            registered.set()
            print(f"[OK] 注册成功, JWT={jwt_token[:80]}...")
            # 上线通知
            notice = json.dumps({
                "v": 1, "action": "post", "source": board_name,
                "token": jwt_token,
                "content": {"text": f"{board_name} 上线"},
            })
            client.publish(f"agent/board/{board_key}/post", notice)
            print("[发布] 上线通知已发送")

    # ---- 客户端 ----
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(broker_user, broker_pass)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(broker_host, broker_port, 60)
    except Exception as e:
        print(f"[ERR] 连接失败: {e}")
        sys.exit(1)

    client.loop_start()

    if not registered.wait(timeout=10):
        print("[ERR] 注册超时, 请确认 BoardService 在运行")
        client.loop_stop()
        sys.exit(1)

    # ---- 心跳线程 (30 秒间隔) ----
    def heartbeat_loop():
        while True:
            time.sleep(30)
            if jwt_token:
                hb = json.dumps({
                    "v": 1, "action": "heartbeat",
                    "source": board_name, "token": jwt_token,
                    "timestamp": time.time(),
                })
                client.publish(f"agent/board/{board_key}/heartbeat", hb)

    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # ---- 交互式终端 ----
    print("\n远端 GA 交互终端")
    print("  /post <msg>              发布消息到 Board")
    print("  /subscribe <topic>       订阅自定义 topic")
    print("  /publish <topic> <msg>   发布到指定 topic")
    print("  /status                  查看状态")
    print("  /quit                    退出")

    while True:
        try:
            line = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        if line == "/quit":
            break
        if line == "/status":
            print(f"  Broker: {broker_host}:{broker_port}")
            print(f"  User:   {broker_user}")
            print(f"  Board:  {board_name} ({board_key})")
            print(f"  JWT:    {'yes' if jwt_token else 'no'}")
            continue
        if line.startswith("/post "):
            if not jwt_token:
                print("[ERR] 未注册")
                continue
            msg = json.dumps({
                "v": 1, "action": "post", "source": board_name,
                "token": jwt_token,
                "content": {"text": line[6:]},
            })
            client.publish(f"agent/board/{board_key}/post", msg)
            print("[OK] 已发布")
            continue
        if line.startswith("/subscribe "):
            topic = line[11:]
            client.subscribe(topic)
            print(f"[OK] 已订阅 {topic}")
            continue
        if line.startswith("/publish "):
            rest = line[9:].split(" ", 1)
            if len(rest) == 2:
                client.publish(rest[0], rest[1])
                print(f"[OK] 已发布到 {rest[0]}")
            continue
        print(f"未知命令: {line}")

    client.loop_stop()


if __name__ == "__main__":
    main()
