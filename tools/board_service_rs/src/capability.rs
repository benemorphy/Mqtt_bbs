use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Agent 能力信息 — 含版本号和TTL用于僵尸检测（P1.1）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub agent_id: String,
    pub capabilities: Vec<String>,
    pub version: u64,          // 每次声明递增，Consumer据此判断新旧
    pub status: String,
    pub last_seen: i64,
    pub load: f64,
    pub ttl: u64,              // 超过 last_seen + ttl 视为僵尸（默认180s）
}

impl AgentInfo {
    pub fn new(agent_id: &str) -> Self {
        Self {
            agent_id: agent_id.to_string(),
            capabilities: vec![],
            version: 1,
            status: "online".to_string(),
            last_seen: chrono::Utc::now().timestamp(),
            load: 0.0,
            ttl: 180,
        }
    }
    
    pub fn is_zombie(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        now - self.last_seen > self.ttl as i64
    }
}

/// CapabilityRegistry — 去中心化 Agent 能力注册表
pub struct CapabilityRegistry {
    pub agents: HashMap<String, AgentInfo>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self { agents: HashMap::new() }
    }
    
    pub fn cleanup_agents(&mut self) -> usize {
        let before = self.agents.len();
        self.agents.retain(|_, info| !info.is_zombie());
        before - self.agents.len()
    }
}
