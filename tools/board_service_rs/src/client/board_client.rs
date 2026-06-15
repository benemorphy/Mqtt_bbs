/// Rust BoardClient — BBS 协议客户端 (代替 Python board_client.py)
use mqtt_bbs_rs::client::bbs_client::BBSClient;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct BoardClient {
    pub agent_id: String,
    pub board: String,
    pub client: Arc<BBSClient>,
    reply_to: String,
    pending: Arc<Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<Value>>>>,
}

impl BoardClient {
    pub async fn new(agent_id: &str, board: &str, host: &str, port: u16) -> Self {
        let client = Arc::new(BBSClient::new(agent_id, host, port).await);
        let reply_to = format!("v2/agent/{}/rpc/res/", agent_id);
        let pending: Arc<Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<Value>>>> = 
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        
        // 订阅响应槽
        let p = pending.clone();
        let aid = agent_id.to_string();
        client.subscribe(&format!("{}#", reply_to), move |topic, payload| {
            let corr_id = topic.rsplit('/').next().unwrap_or("").to_string();
            let p = p.clone();
            tokio::spawn(async move {
                let mut map = p.lock().await;
                if let Some(tx) = map.remove(&corr_id) {
                    let _ = tx.send(payload);
                }
            });
        }).await;
        
        Self { agent_id: aid, board: board.to_string(), client, reply_to, pending }
    }

    pub async fn register(&self, name: &str, timeout: u64) -> Option<Value> {
        let corr_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        
        self.client.publish(
            &format!("agent/bbs/{}/register", self.board),
            &serde_json::json!({
                "agent_id": self.agent_id, "name": name,
                "corr_id": corr_id, "reply_to": self.reply_to,
            }),
        ).await;
        
        match tokio::time::timeout(Duration::from_secs(timeout), rx).await {
            Ok(Ok(val)) => Some(val),
            _ => None,
        }
    }
}
