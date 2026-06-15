/// Rust AgentBoard — 任务发布者 (替代 Python bbs.py AgentBoard)
///
/// 通过 MQTT 发布任务到 BoardService, 等待 WorkerAgent 认领执行。
/// 完全向后兼容 Python AgentBoard 的协议和主题格式。
use serde::{Serialize, Deserialize};
use mqtt_bbs_rs::models::BbsRequest;
use mqtt_bbs_rs::client::bbs_client::BBSClient;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::oneshot;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// v2/task 命名空间 (与 bbs.py V2_TASK_TOPIC 一致)
const V2_TASK_TOPIC: &str = "v2/task";

/// HMAC 签名 (与 Python _calc_hmac 完全兼容)
pub fn calc_hmac(task_id: &str, task_type: &str, input: &Value) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"bbs-secret-key").unwrap();
    mac.update(task_id.as_bytes());
    mac.update(task_type.as_bytes());
    mac.update(&serde_json::to_vec(input).unwrap());
    hex::encode(mac.finalize().into_bytes())
}

/// 任务状态枚举 (与 bbs.py TaskStatus 一致)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }
}

/// 任务消息 (与 bbs.py TaskMessage 一致)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMessage {
    pub task_id: String,
    #[serde(rename = "type")]
    pub task_type: String,
    pub input: Value,
    pub agent_id: String,
    pub corr_id: String,
    pub created_at: f64,
}

/// 任务输出 (与 bbs.py TaskOutput 一致)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOutput {
    pub task_id: String,
    pub agent_id: String,
    pub status: String,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub started_at: f64,
    pub completed_at: f64,
}

/// AgentBoard — 任务发布者
pub struct AgentBoard {
    client: Arc<BBSClient>,
    agent_id: String,
    pending: Arc<Mutex<std::collections::HashMap<String, oneshot::Sender<Value>>>>,
}

impl AgentBoard {
    /// 创建新的 AgentBoard
    pub async fn new(agent_id: &str, host: &str, port: u16) -> Self {
        let client = Arc::new(BBSClient::new(agent_id, host, port).await);
        let pending: Arc<Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<Value>>>> = 
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        
        // 订阅响应槽
        let p = pending.clone();
        let c = client.clone();
        let aid = agent_id.to_string();
        client.subscribe(&format!("v2/agent/{}/rpc/res/#", agent_id), move |topic, payload| {
            let corr_id = topic.rsplit('/').next().unwrap_or("").to_string();
            let p = p.clone();
            tokio::spawn(async move {
                let mut map = p.lock().await;
                if let Some(tx) = map.remove(&corr_id) {
                    let _ = tx.send(payload);
                }
            });
        }).await;
        
        Self { client, agent_id: aid, pending }
    }
    
    /// 发布任务 (对标 Python AgentBoard.post_task)
    pub async fn post_task(
        &self,
        task_type: &str,
        task_input: Value,
        task_id: Option<&str>,
    ) -> Result<String, String> {
        let task_id = task_id.unwrap_or(&format!("task_{}", &Uuid::new_v4().to_string()[..8])).to_string();
        let corr_id = Uuid::new_v4().to_string()[..8].to_string();
        
        let msg = TaskMessage {
            task_id: task_id.clone(),
            task_type: task_type.to_string(),
            input: task_input.clone(),
            agent_id: self.agent_id.clone(),
            corr_id: corr_id.clone(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(),
        };
        let payload = serde_json::to_value(&msg).map_err(|e| e.to_string())?;
        let sig = calc_hmac(&task_id, task_type, &task_input);
        
        // 发布 input + 状态 (v2 双写)
        self.client.publish(
            &format!("board/task/{}/input", task_id), &payload
        ).await;
        self.client.publish(
            &format!("{}/{}/input", V2_TASK_TOPIC, task_id), &payload
        ).await;
        self.client.publish(
            &format!("board/task/{}/status", task_id),
            &Value::String(TaskStatus::Pending.as_str().to_string()),
        ).await;
        self.client.publish(
            &format!("board/open",), &Value::String(task_id.clone())
        ).await;
        
        Ok(task_id)
    }
    
    /// 等待任务完成 (对标 Python AgentBoard.wait_task)
    pub async fn wait_task(&self, task_id: &str, timeout: Duration) -> Result<TaskOutput, String> {
        let corr_id = Uuid::new_v4().to_string()[..8].to_string();
        let (tx, rx) = oneshot::channel();
        
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        
        // 订阅 output 和 signal 主题
        let resp_topic = format!("v2/agent/{}/rpc/res/{}", self.agent_id, corr_id);
        // 使用现有响应槽
        
        let result = tokio::time::timeout(timeout, rx).await
            .map_err(|_| "等待超时".to_string())?
            .map_err(|_| "通道关闭".to_string())?;
        
        serde_json::from_value(result).map_err(|e| e.to_string())
    }
    
    /// 取消任务 (对标 Python AgentBoard.cancel_task)
    pub async fn cancel_task(&self, task_id: &str) -> Result<(), String> {
        self.client.publish(
            &format!("board/task/{}/signal", task_id),
            &Value::String("CANCEL".to_string()),
        ).await;
        self.client.publish(
            &format!("{}/{}/signal", V2_TASK_TOPIC, task_id),
            &Value::String("CANCEL".to_string()),
        ).await;
        Ok(())
    }
    
    /// 查询 Agent 能力
    pub async fn query_capabilities(&self, filter: Option<&str>) -> Result<Vec<Value>, String> {
        let corr_id = Uuid::new_v4().to_string()[..8].to_string();
        let (tx, rx) = oneshot::channel();
        
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id.clone(), tx);
        }
        
        self.client.publish("board/capability/query", &serde_json::json!({
            "corr_id": corr_id,
            "filter": filter,
            "reply_to": format!("v2/agent/{}/rpc/res/", self.agent_id),
        })).await;
        
        let result = tokio::time::timeout(Duration::from_secs(10), rx).await
            .map_err(|_| "能力查询超时".to_string())?
            .map_err(|_| "通道关闭".to_string())?;
        
        Ok(result["agents"].as_array().cloned().unwrap_or_default())
    }
}
