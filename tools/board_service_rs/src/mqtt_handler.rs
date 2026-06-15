use std::sync::Arc;
use rumqttc::{AsyncClient, Event, Incoming, Packet, QoS};
use crate::AppState;
use crate::handlers;

/// 根据主题类型判断是否需要 retain 标记
/// 设计文档要求：状态/信息类/灵感板/数据流状态需 retain
fn should_retain(topic: &str, resp_type: &str) -> bool {
    // 状态类: online_status, capability, status
    if resp_type == "online_status" || resp_type == "capability" || resp_type == "status" {
        return true;
    }
    // 灵感板: inspiration
    if resp_type == "inspiration" || resp_type == "inspiration_list" {
        return true;
    }
    // 板块信息: board_info, board_list
    if resp_type == "board_info" || resp_type == "board_list" {
        return true;
    }
    // 注册/去注册: register / unregister
    if resp_type == "register" || resp_type == "unregister" {
        return true;
    }
    // 系统配置类
    if topic.contains("webhook/config") {
        return true;
    }
    false
}

/// 心跳发布循环
pub async fn heartbeat_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        let topic = format!("node/{}/heartbeat", state.config.agent_id);
        let payload = serde_json::json!({"ts": chrono::Utc::now().timestamp()});
        if let Err(e) = state.mqtt_client.publish(
            &topic, QoS::AtMostOnce, false,
            serde_json::to_vec(&payload).unwrap(),
        ).await {
            tracing::warn!("心跳发布失败: {}", e);
        }
    }
}

/// MQTT 事件循环 — 主题分发 (含超时保护防止静默挂死)
pub async fn event_loop(state: Arc<AppState>, mut event_loop: rumqttc::EventLoop) -> anyhow::Result<()> {
    let mut idle_since = std::time::Instant::now();
    const POLL_TIMEOUT_SECS: u64 = 60;
    const MAX_IDLE_SECS: u64 = 120; // 2分钟无有效消息则触发重启 (watchdog每2分钟检查一次)

    loop {
        let poll_result = tokio::time::timeout(
            std::time::Duration::from_secs(POLL_TIMEOUT_SECS),
            event_loop.poll()
        ).await;

        match poll_result {
            Ok(Ok(Event::Incoming(Incoming::Publish(publish)))) => {
                idle_since = std::time::Instant::now(); // 重置空闲计时
                let topic = publish.topic.clone();
                let payload = publish.payload.to_vec();
                let topic_str = topic.as_str();
                
                // 按主题模式分发 (Python BBSClient 使用 agent/ 前缀)
                if topic_str.starts_with("agent/bbs/") {
                    handle_bbs_topic(&state, topic_str, &payload).await;
                } else if topic_str.starts_with("node/") {
                    handle_node_topic(&state, topic_str, &payload).await;
                } else if topic_str == "board/capability/query" {
                    handlers::capability::handle_cap_query(&state, &payload).await;
                }
            }
            Ok(Ok(Event::Incoming(Incoming::ConnAck(_)))) => {
                idle_since = std::time::Instant::now();
                tracing::info!("MQTT 连接成功");
            }
            Ok(Ok(Event::Outgoing(_))) => {}
            Ok(Err(e)) => {
                tracing::warn!("MQTT 事件循环错误: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Ok(_) => {}
            Err(_elapsed) => {
                // poll() 超时 — 可能 event_loop 挂死
                tracing::warn!(
                    "MQTT poll() 超时 ({}s)，触发空闲检查 (idle={}s)",
                    POLL_TIMEOUT_SECS,
                    idle_since.elapsed().as_secs()
                );
                if idle_since.elapsed().as_secs() > MAX_IDLE_SECS {
                    tracing::error!(
                        "event_loop 空闲超过 {}s，主动退出以触发重启",
                        MAX_IDLE_SECS
                    );
                    return Err(anyhow::anyhow!("event_loop idle timeout"));
                }
            }
        }
    }
}

/// 分发 bbs/ 主题
async fn handle_bbs_topic(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    // agent/bbs/{board}/{operation}
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() < 4 { return; }
    
    let operation = parts[3];
    match operation {
        "register" => handlers::register::handle_register(state, topic, payload).await,
        "post" => handlers::post::handle_post(state, topic, payload).await,
        "query" => handlers::query::handle_query(state, topic, payload).await,
        "file_init" => handlers::file::handle_file_init(state, topic, payload).await,
        "file_chunk" => handlers::file::handle_file_chunk(state, topic, payload).await,
        "file_commit" => handlers::file::handle_file_commit(state, topic, payload).await,
        "file_download" => handlers::file::handle_file_download(state, topic, payload).await,
        "webhook" | "webhook/config" => handlers::webhook::handle_webhook_config(state, topic, payload).await,
        _ => tracing::debug!("未知 bbs 操作: {}", operation),
    }
}

/// 分发 node/ 主题
async fn handle_node_topic(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    // node/{agent_id}/{type}
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() < 3 { return; }
    
    let msg_type = parts[2];
    match msg_type {
        "status" => handlers::capability::handle_status(state, topic, payload).await,
        "heartbeat" => handlers::capability::handle_heartbeat(state, topic, payload).await,
        "capability" => handlers::capability::handle_capability(state, topic, payload).await,
        _ => tracing::debug!("未知 node 消息类型: {}", msg_type),
    }
}

/// 发布 MQTT 响应 (reply_to 优先, 向后兼容)
pub async fn publish_response(
    client: &AsyncClient,
    reply_to: Option<&str>,
    board_key: &str,
    resp_type: &str,
    corr_id: &str,
    payload: &serde_json::Value,
) {
    let topic = if let Some(rt) = reply_to {
        if corr_id.is_empty() {
            rt.to_string()
        } else {
            format!("{}{}", rt, corr_id)
        }
    } else {
        if corr_id.is_empty() {
            tracing::warn!("响应无 reply_to 也无 corr_id, 丢弃");
            return;
        }
        format!("agent/bbs/{}/{}/{}", board_key, resp_type, corr_id)
    };
    
    let bytes = serde_json::to_vec(payload).unwrap();
    let retain = should_retain(&topic, resp_type);
    tracing::info!("准备发布响应: topic={}, payload={:?}, retain={}", topic, payload, retain);
    match client.publish(&topic, QoS::AtLeastOnce, retain, bytes).await {
        Ok(()) => tracing::info!("响应发布成功: {} (retain={})", topic, retain),
        Err(e) => tracing::error!("MQTT 发布失败 [{}]: {}", topic, e),
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}
