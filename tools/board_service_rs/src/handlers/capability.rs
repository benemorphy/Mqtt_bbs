use std::sync::Arc;
use crate::AppState;
use crate::capability::AgentInfo;

pub async fn handle_status(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let agent_id = topic.split('/').nth(1).unwrap_or("");
    if agent_id.is_empty() { return; }
    let status = String::from_utf8_lossy(payload).trim().to_string();
    
    let mut caps = state.capabilities.write().await;
    let entry = caps.entry(agent_id.to_string()).or_insert_with(|| AgentInfo::new(agent_id));
    entry.status = status.clone();
    entry.last_seen = chrono::Utc::now().timestamp();
    entry.version += 1;
    tracing::debug!("Agent 状态: {} = {} (v{})", agent_id, status, entry.version);
}

pub async fn handle_heartbeat(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let agent_id = topic.split('/').nth(1).unwrap_or("");
    if agent_id.is_empty() { return; }
    
    let mut caps = state.capabilities.write().await;
    if let Some(entry) = caps.get_mut(agent_id) {
        entry.last_seen = chrono::Utc::now().timestamp();
        entry.status = "online".to_string();
        if let Ok(p) = serde_json::from_slice::<serde_json::Value>(payload) {
            entry.load = p.get("load").and_then(|v| v.as_f64()).unwrap_or(entry.load);
        }
    }
}

pub async fn handle_capability(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let agent_id = topic.split('/').nth(1).unwrap_or("");
    if agent_id.is_empty() { return; }
    
    if let Ok(caps) = serde_json::from_slice::<Vec<String>>(payload) {
        let mut cap_reg = state.capabilities.write().await;
        let entry = cap_reg.entry(agent_id.to_string()).or_insert_with(|| AgentInfo::new(agent_id));
        entry.capabilities = caps;
        entry.last_seen = chrono::Utc::now().timestamp();
        entry.status = "online".to_string();
        entry.version += 1;
        tracing::info!("Agent 能力: {} v{} = {:?}", agent_id, entry.version, entry.capabilities);
    }
}

pub async fn handle_cap_query(state: &Arc<AppState>, payload: &[u8]) {
    let req: serde_json::Value = serde_json::from_slice(payload).unwrap_or_default();
    let corr_id = req.get("corr_id").and_then(|v| v.as_str()).unwrap_or("");
    
    let caps = state.capabilities.read().await;
    let agent_list: Vec<&AgentInfo> = caps.values().filter(|a| !a.is_zombie()).collect();
    
    let resp = serde_json::json!({
        "agents": agent_list.iter().map(|a| serde_json::json!({
            "agent_id": a.agent_id,
            "capabilities": a.capabilities,
            "version": a.version,
            "status": a.status,
            "last_seen": a.last_seen,
            "load": a.load,
        })).collect::<Vec<_>>(),
        "total": agent_list.len(),
    });
    
    tracing::debug!("Capability 查询: {} active agents (corr_id={})", agent_list.len(), corr_id);
    drop(caps);
    
    let resp_topic = "board/capability/query/response";
    if let Ok(payload) = serde_json::to_vec(&resp) {
        let _ = state.mqtt_client.publish(
            resp_topic, rumqttc::QoS::AtLeastOnce, false, payload
        ).await;
    }
}
