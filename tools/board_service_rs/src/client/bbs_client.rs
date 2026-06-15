/// Rust BBSClient — MQTT 客户端封装 (代替 Python client.py)
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use std::time::Duration;

type Callback = Arc<dyn Fn(String, Value) + Send + Sync>;

pub struct BBSClient {
    pub client: AsyncClient,
    pub agent_id: String,
    callbacks: Arc<Mutex<HashMap<String, Vec<Callback>>>>,
    #[allow(dead_code)]
    rx: mpsc::Receiver<()>,
}

impl BBSClient {
    pub async fn new(agent_id: &str, host: &str, port: u16) -> Self {
        let client_id = format!("{}_{}", agent_id, uuid::Uuid::new_v4().to_string().split('-').next().unwrap());
        let mut opts = MqttOptions::new(&client_id, host, port);
        opts.set_keep_alive(Duration::from_secs(30));
        opts.set_clean_session(true);
        let (client, mut event_loop) = AsyncClient::new(opts, 100);
        let (_tx, rx) = mpsc::channel(100);
        let callbacks: Arc<Mutex<HashMap<String, Vec<Callback>>>> = Arc::new(Mutex::new(HashMap::new()));
        
        // 事件循环线程
        let cb = callbacks.clone();
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(p))) => {
                        let topic = p.topic.to_string();
                        let payload: Value = serde_json::from_slice(&p.payload).unwrap_or(Value::Null);
                        let map = cb.lock().await;
                        for (pattern, callbacks) in map.iter() {
                            if topic_matches(pattern, &topic) {
                                for callback in callbacks {
                                    callback(topic.clone(), payload.clone());
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("BBSClient MQTT error: {:?}, reconnecting in 3s", e);
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });
        
        Self { client, agent_id: agent_id.to_string(), callbacks, rx }
    }

    pub async fn publish(&self, topic: &str, payload: &Value) {
        let bytes = serde_json::to_vec(payload).unwrap_or_default();
        self.client.publish(topic, QoS::AtLeastOnce, false, bytes).await.ok();
    }

    pub async fn subscribe<F>(&self, pattern: &str, callback: F)
    where F: Fn(String, Value) + Send + Sync + 'static
    {
        let mut cb = self.callbacks.lock().await;
        cb.entry(pattern.to_string()).or_default().push(Arc::new(callback));
        self.client.subscribe(pattern, QoS::AtLeastOnce).await.ok();
    }
}

/// MQTT 主题通配符匹配 (支持 + 和 #)
fn topic_matches(pattern: &str, topic: &str) -> bool {
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
