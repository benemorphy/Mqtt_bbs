/// Rust BBSClient — MQTT 客户端封装
///
/// 对标 Python Mqtt_bbs/client.py
/// - 连接管理 (LWT/认证/QoS)
/// - 心跳发布
/// - 标准消息信封 build_payload
/// - 流式消息 publish_stream (seq, QoS=0)
/// - 回调派发 + 通配符匹配
///
/// 用法:
/// ```rust
/// let (client, event_loop) = BBSClient::new("agent_alpha", "127.0.0.1", 1883, None, None);
/// // 处理事件
/// tokio::spawn(BBSClient::process_events(client.connected.clone(), ...));
/// client.publish("topic", &payload).await;
/// ```

use rumqttc::{AsyncClient, MqttOptions, QoS, LastWill};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc};
use std::time::Duration;

/// MQTT 回调类型: (topic_suffix, payload)
pub type Callback = Arc<dyn Fn(String, Value) + Send + Sync>;

/// MQTT 客户端封装
pub struct BBSClient {
    pub agent_id: String,
    pub prefix: String,
    pub client: AsyncClient,
    pub connected: Arc<AtomicBool>,
    callbacks: Arc<Mutex<HashMap<String, Vec<Callback>>>>,
    hb_tx: Option<mpsc::Sender<()>>,
}

impl BBSClient {
    /// 创建新 BBSClient
    ///
    /// 返回 (BBSClient, EventLoop)。EventLoop 需要在另一 task 中驱动。
    pub fn new(
        agent_id: &str,
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    ) -> (Self, rumqttc::EventLoop) {
        let agent_id_str = agent_id.to_string();
        let prefix = "agent/".to_string();

        let mut opts = MqttOptions::new(agent_id, host, port);
        opts.set_keep_alive(Duration::from_secs(60));
        opts.set_clean_session(true);

        // 认证
        if let (Some(u), Some(p)) = (username, password) {
            opts.set_credentials(&u, &p);
        }

        // LWT: 断线后发布 status=offline
        let lwt_topic = format!("node/{}/status", agent_id);
        opts.set_last_will(LastWill::new(
            &lwt_topic, "offline", QoS::AtLeastOnce, false,
        ));

        let (client, event_loop) = AsyncClient::new(opts, 1024);
        let connected = Arc::new(AtomicBool::new(false));

        let bbs = Self {
            agent_id: agent_id_str,
            prefix,
            client,
            connected,
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            hb_tx: None,
        };

        (bbs, event_loop)
    }

    /// 处理 EventLoop 事件（在独立 task 中运行）
    pub fn process_events(
        connected: Arc<AtomicBool>,
        callbacks: Arc<Mutex<HashMap<String, Vec<Callback>>>>,
        prefix: String,
        agent_id: String,
        client: AsyncClient,
        event_loop: rumqttc::EventLoop,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut el = event_loop;
            loop {
                match el.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                        connected.store(true, Ordering::SeqCst);
                        // 发布 online
                        let payload = serde_json::json!({"status": "online"});
                        let topic = format!("node/{}/status", agent_id);
                        if let Ok(bytes) = serde_json::to_vec(&payload) {
                            let _ = client.publish(&topic, QoS::AtLeastOnce, true, bytes).await;
                        }
                    }
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg))) => {
                        let topic = msg.topic;
                        let suffix = if topic.starts_with(&prefix) {
                            topic[prefix.len()..].to_string()
                        } else {
                            topic.clone()
                        };
                        let payload: Value = serde_json::from_slice(&msg.payload)
                            .unwrap_or(Value::String(String::from_utf8_lossy(&msg.payload).to_string()));
                        let cbs = callbacks.lock().await;
                        for (pattern, handlers) in cbs.iter() {
                            if topic_matches(pattern, &suffix) {
                                for cb in handlers {
                                    cb(suffix.clone(), payload.clone());
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(_) => {
                        connected.store(false, Ordering::SeqCst);
                    }
                }
            }
        })
    }

    /// 标准消息信封
    pub fn build_payload(source: &str, corr_id: &str, reply_to: &str, action: &str) -> Value {
        serde_json::json!({
            "v": 1,
            "action": action,
            "source": source,
            "corr_id": corr_id,
            "reply_to": reply_to,
        })
    }

    /// 发布消息（自动处理前缀）
    pub async fn publish(&self, topic_suffix: &str, payload: &Value) {
        self.publish_with_qos(topic_suffix, payload, false, QoS::AtLeastOnce).await;
    }

    /// 发布消息（指定 QoS 和 retain）
    pub async fn publish_with_qos(&self, topic_suffix: &str, payload: &Value, retain: bool, qos: QoS) {
        let topic = if topic_suffix.starts_with("v2/") || topic_suffix.starts_with("board/") {
            topic_suffix.to_string()
        } else {
            format!("{}{}", self.prefix, topic_suffix)
        };
        if let Ok(bytes) = serde_json::to_vec(payload) {
            let _ = self.client.publish(&topic, qos, retain, bytes).await;
        }
    }

    /// 流式消息 (QoS=0, 不 retain)
    pub async fn publish_stream(&self, topic_suffix: &str, seq: u64, data: &str) {
        let payload = serde_json::json!({
            "seq": seq,
            "ts": chrono::Utc::now().to_rfc3339(),
            "data": data,
        });
        self.publish_with_qos(topic_suffix, &payload, false, QoS::AtMostOnce).await;
    }

    /// 订阅主题
    pub async fn subscribe(&self, topic_suffix: &str, callback: Callback) {
        let topic = if topic_suffix.starts_with("v2/") || topic_suffix.starts_with("board/") {
            topic_suffix.to_string()
        } else {
            format!("{}{}", self.prefix, topic_suffix)
        };
        {
            let mut cbs = self.callbacks.lock().await;
            cbs.entry(topic_suffix.to_string()).or_default().push(callback);
        }
        let _ = self.client.subscribe(&topic, QoS::AtLeastOnce).await;
    }

    /// 取消订阅
    pub async fn unsubscribe(&self, topic_suffix: &str) {
        let topic = format!("{}{}", self.prefix, topic_suffix);
        let _ = self.client.unsubscribe(&topic).await;
        let mut cbs = self.callbacks.lock().await;
        cbs.remove(topic_suffix);
    }

    /// 等待连接
    pub async fn wait_connected(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.connected.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.connected.load(Ordering::SeqCst)
    }

    /// 是否已连接
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// 启动心跳
    pub fn start_heartbeat(&self, interval_secs: u64) -> mpsc::Sender<()> {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        let client = self.client.clone();
        let agent_id = self.agent_id.clone();
        let connected = self.connected.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if connected.load(Ordering::SeqCst) {
                            let payload = serde_json::json!({
                                "agent_id": agent_id,
                                "timestamp": chrono::Utc::now().timestamp(),
                                "status": "online",
                            });
                            let topic = format!("node/{}/heartbeat", agent_id);
                            if let Ok(bytes) = serde_json::to_vec(&payload) {
                                let _ = client.publish(&topic, QoS::AtLeastOnce, false, bytes).await;
                            }
                        }
                    }
                    _ = rx.recv() => break,
                }
            }
        });
        tx
    }

    /// 断开连接
    pub async fn disconnect(&self) {
        let payload = serde_json::json!({"status": "offline"});
        let topic = format!("node/{}/status", self.agent_id);
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            let _ = self.client.publish(&topic, QoS::AtLeastOnce, true, bytes).await;
        }
        let _ = self.client.disconnect().await;
    }
}

/// 通配符主题匹配
pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    let pat_parts: Vec<&str> = pattern.split('/').collect();
    let top_parts: Vec<&str> = topic.split('/').collect();
    let mut i = 0;
    for (j, p) in pat_parts.iter().enumerate() {
        if *p == "#" { return true; }
        if j >= top_parts.len() { return false; }
        if *p == "+" { i = j + 1; continue; }
        if *p != top_parts[j] { return false; }
        i = j + 1;
    }
    i == top_parts.len()
}
