/// Rust BoardClient — BBS 协议客户端
use crate::client::bbs_client::{BBSClient, Callback};
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
    cached_token: Arc<Mutex<Option<Value>>>,
}

impl BoardClient {
    pub async fn new(agent_id: &str, board: &str, host: &str, port: u16) -> Self {
        let (inner, _el) = BBSClient::new(agent_id, host, port, None, None);
        let client = Arc::new(inner);
        let reply_to = format!("v2/agent/{}/rpc/res/", agent_id);
        let pending = Arc::new(Mutex::new(std::collections::HashMap::<String, tokio::sync::oneshot::Sender<Value>>::new()));
        let p = pending.clone();
        let aid = agent_id.to_string();
        client.subscribe(&format!("{}#", reply_to), Arc::new(move |topic: String, payload: Value| {
            let corr_id = topic.rsplit("/").next().unwrap_or("").to_string();
            let p = p.clone();
            tokio::spawn(async move {
                let mut map = p.lock().await;
                if let Some(tx) = map.remove(&corr_id) {
                    let _ = tx.send(payload);
                }
            });
        }) as Callback).await;
        Self { agent_id: aid, board: board.to_string(), client, reply_to, pending, cached_token: Arc::new(Mutex::new(None)) }
    }

    pub async fn connect(&self) -> Result<(), String> {
        let reply_to = &self.reply_to;
        let board = &self.board;
        self.client.subscribe(&format!("{}#", reply_to), Arc::new(move |_t: String, _p: Value| {}) as Callback).await;
        self.client.publish(&format!("agent/bbs/{}/register", board), &serde_json::json!({"action": "ping"})).await;
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.client.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn disconnect(&self) {
        // disconnect handled by BBSClient drop
    }

    fn _build_payload(source: &str, corr_id: &str, reply_to: &str, action: &str, extra: Value) -> Value {
        serde_json::json!({"v": 1, "action": action, "source": source, "corr_id": corr_id, "reply_to": reply_to, "extra": extra})
    }

    async fn _wait_response(&self, corr_id: &str, timeout_secs: u64) -> Option<Value> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.to_string(), tx);
        }
        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(val)) => Some(val),
            _ => None,
        }
    }

    pub async fn register(&self, name: &str, timeout_secs: u64) -> Result<Value, String> {
        {
            let cached = self.cached_token.lock().await;
            if let Some(token) = cached.as_ref() {
                return Ok(token.clone());
            }
        }
        let corr_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        self.client.publish(&format!("agent/bbs/{}/register", self.board),
            &serde_json::json!({"agent_id": self.agent_id, "name": name, "corr_id": corr_id, "reply_to": self.reply_to}),
        ).await;
        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(val)) => {
                let mut cached = self.cached_token.lock().await;
                *cached = Some(val.clone());
                Ok(val)
            }
            _ => Err("register timeout".to_string()),
        }
    }

    pub async fn post(&self, content: &str, token: &str, timeout_secs: u64) -> Result<Value, String> {
        let corr_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        self.client.publish(&format!("agent/bbs/{}/post", self.board),
            &serde_json::json!({"agent_id": self.agent_id, "token": token, "content": content, "corr_id": corr_id, "reply_to": self.reply_to}),
        ).await;
        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(val)) => Ok(val),
            _ => Err("post timeout".to_string()),
        }
    }

    pub async fn query_posts(&self, author: Option<&str>, limit: u32, offset: u32, timeout_secs: u64) -> Result<Vec<Value>, String> {
        let corr_id = self.gen_corr_id();
        let params = serde_json::json!({"author": author, "limit": limit, "offset": offset});
        let result = self._query("posts", params, timeout_secs).await?;
        Ok(result.get("data").and_then(|v| v.as_array().cloned()).unwrap_or_default())
    }

    pub async fn poll(&self, since_id: u64, limit: u32, timeout_secs: u64) -> Result<Vec<Value>, String> {
        let corr_id = self.gen_corr_id();
        let params = serde_json::json!({"since_id": since_id, "limit": limit});
        let result = self._query("poll", params, timeout_secs).await?;
        Ok(result.get("data").and_then(|v| v.as_array().cloned()).unwrap_or_default())
    }

    pub async fn count_posts(&self, author: Option<&str>, timeout_secs: u64) -> Result<u64, String> {
        let corr_id = self.gen_corr_id();
        let params = serde_json::json!({"author": author});
        let result = self._query("count", params, timeout_secs).await?;
        Ok(result.get("data").and_then(|v| v.get("total")).and_then(|v| v.as_u64()).unwrap_or(0))
    }

    pub async fn list_authors(&self, timeout_secs: u64) -> Result<Vec<Value>, String> {
        let corr_id = self.gen_corr_id();
        let result = self._query("authors", serde_json::json!({}), timeout_secs).await?;
        Ok(result.get("data").and_then(|v| v.as_array().cloned()).unwrap_or_default())
    }

    // ── 实时订阅 ──

    pub async fn subscribe_posts(&self, callback: Arc<dyn Fn(Value) + Send + Sync + 'static>) {
        // Subscribe to new_post events from BoardService
        let cb = callback.clone();
        let board = self.board.clone();
        self.client.subscribe(&format!("agent/bbs/{}/new_post", board), Arc::new(move |_topic: String, payload: Value| {
            cb(payload);
        }) as Callback).await;
    }

    // ── 文件上传 ──

    pub async fn upload_file(&self, filename: &str, data: &[u8], token: &str, timeout_secs: u64) -> Result<Value, String> {
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data);
        let corr_id = self.gen_corr_id();
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        self.client.publish(&format!("agent/bbs/{}/file_chunk", self.board),
            &serde_json::json!({"agent_id": self.agent_id, "token": token, "filename": filename, "data": b64, "corr_id": corr_id, "reply_to": self.reply_to}),
        ).await;
        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(val)) => Ok(val),
            _ => Err("upload timeout".to_string()),
        }
    }

    async fn _query(&self, query_type: &str, params: Value, timeout_secs: u64) -> Result<Value, String> {
        let corr_id = self.gen_corr_id();
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        self.client.publish(&format!("agent/bbs/{}/query", self.board),
            &serde_json::json!({"agent_id": self.agent_id, "corr_id": corr_id, "reply_to": self.reply_to, "action": "query", "type": query_type, "params": params}),
        ).await;
        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(val)) => Ok(val),
            _ => Err("query timeout".to_string()),
        }
    }

    fn gen_corr_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()[..8].to_string()
    }
}

pub async fn quick_post(board: &str, name: &str, content: &str) -> Result<Value, String> {
    let client = BoardClient::new("quick", board, "127.0.0.1", 1883).await;
    client.connect().await?;
    let info = client.register(name, 10).await?;
    let token = info.get("token").and_then(|v| v.as_str()).ok_or("no token")?;
    client.post(content, token, 10).await
}