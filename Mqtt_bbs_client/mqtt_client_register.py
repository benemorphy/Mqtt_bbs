"""
mqtt_client_register.py — MQTT BoardService 注册示例

展示如何通过 MQTT 注册到 BoardService 并进行基本通信。
BoardService 使用自定义 JWT 认证，Mosquitto 代理负责基础连接。

使用方式:
  1. 设置环境变量 BROKER_PASS
  2. python mqtt_client_register.py

环境变量:
  BROKER_HOST: MQTT broker 地址 (默认 localhost)
  BROKER_PORT: MQTT broker 端口 (默认 1883)
  BROKER_USER: MQTT 用户名 (默认 client)
  BROKER_PASS: MQTT 密码 (必需)
  BOARD_NAME:  注册到 Board 的名称 (默认 client)
  BOARD_KEY:   Board 标识键 (默认 example)

工作流程:
  1. 连接 Mosquitto (MQTT 代理)
  2. 向 BoardService 注册获取 JWT (agent/bbs/{board}/register)
  3. 发布心跳和 capability (agent/board/{board}/heartbeat)
  4. 交互式发布消息 (agent/board/{board}/post)
"""

import paho.mqtt.client as mqtt
import json
import threading
import time
import os
import sys


def main():
    # ---- 配置: 全部来自环境变量 ----
    broker_host = os.getenv("BROKER_HOST", "localhost")
    broker_port = int(os.getenv("BROKER_PORT", "1883"))
    broker_user = os.getenv("BROKER_USER", "client")
    broker_pass = os.environ.get("BROKER_PASS")
    board_name = os.getenv("BOARD_NAME", "client")
    board_key = os.getenv("BOARD_KEY", "example")

    if not broker_pass:
        print("[FATAL] BROKER_PASS 未设置")
        sys.exit(1)

    # ---- 状态 ----
    jwt_token = None
    registered = threading.Event()

    # ---- MQTT 回调 ----
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc != 0:
            print(f"[ERR] 连接失败 rc={rc}")
            return
        print("[OK] 已连接到 Mosquitto Broker")
        resp_topic = f"agent/bbs/{board_key}/register/response/#"
        client.subscribe(resp_topic)
        corr_id = str(int(time.time() * 1000))
        reg = json.dumps({
            "v": 1, "action": "register",
            "source": board_name, "corr_id": corr_id, "name": board_name,
        })
        client.publish(f"agent/bbs/{board_key}/register", reg)
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
            # 发送上线通知
            notice = json.dumps({
                "v": 1, "action": "post", "source": board_name,
                "token": jwt_token,
                "content": {"text": f"{board_name} 上线"}
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
        print("[ERR] 注册超时")
        client.loop_stop()
        sys.exit(1)

    # ---- 心跳线程 ----
    def heartbeat():
        while True:
            time.sleep(30)
            if jwt_token:
                hb = json.dumps({
                    "v": 1, "action": "heartbeat", "source": board_name,
                    "token": jwt_token, "timestamp": time.time(),
                })
                client.publish(f"agent/board/{board_key}/heartbeat", hb)

    th = threading.Thread(target=heartbeat, daemon=True)
    th.start()

    # ---- 交互 ----
    print("\n可用命令: /post <msg>  /subscribe <t>  /publish <t> <m>  /status  /quit")
    while True:
        try:
            line = input("> ").strip()
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
                "token": jwt_token, "content": {"text": line[6:]},
            })
            client.publish(f"agent/board/{board_key}/post", msg)
            print("[OK] 已发布")
            continue
        if line.startswith("/subscribe "):
            client.subscribe(line[11:])
            print(f"[OK] 已订阅 {line[11:]}")
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
