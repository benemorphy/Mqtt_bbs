use std::sync::Arc;
use crate::{AppState, models::BbsRequest};
use crate::mqtt_handler::publish_response;
use crate::db;

pub async fn handle_post(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = match parse_post(topic, payload) {
        Some(v) => v,
        None => return,
    };
    
    let token = req.token.as_deref().unwrap_or("");
    let content = req.content.as_deref().unwrap_or("");
    let corr_id = req.corr_id.as_deref().unwrap_or("");
    let reply_to = req.reply_to.as_deref();
    
    if token.is_empty() || content.is_empty() { return; }
    
    // 验证 token
    let user = match db::find_user_by_token(&state.db_pool, token).await {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("token 查询错误: {}", e);
            return;
        }
    };
    
    let user = match user {
        Some(u) => u,
        None => {
            tracing::warn!("无效 token (board: {})", board_key);
            publish_response(&state.mqtt_client, reply_to, &board_key, "post/response", corr_id,
                &serde_json::json!({"error": "invalid token"})).await;
            return;
        }
    };
    
    // 写入 DB
    let post_id = match db::insert_post(&state.db_pool, &board_key, &user.name, content).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("发帖 DB 错误: {}", e);
            return;
        }
    };
    
    let created_at = chrono::Utc::now().timestamp();
    
    let resp = serde_json::json!({"id": post_id, "author": user.name, "created_at": created_at});
    publish_response(&state.mqtt_client, reply_to, &board_key, "post/response", corr_id, &resp).await;
    
    // 发布新帖通知 (兼容 Python BoardClient)
    let notify_topic = format!("agent/bbs/{}/new_post", board_key);
    let notify_payload = serde_json::json!({
        "id": post_id, "author": user.name, "content": content,
        "board": board_key, "created_at": created_at
    });
    let _ = state.mqtt_client.publish(&notify_topic, rumqttc::QoS::AtMostOnce, false,
        serde_json::to_vec(&notify_payload).unwrap()).await;
    
    // 发布事件 (for Plugin订阅)
    let event_topic = format!("events/{}/post", board_key);
    let event_payload = serde_json::json!({
        "post_id": post_id, "author": user.name, "board": board_key
    });
    if let Err(e) = state.mqtt_client.publish(&event_topic, rumqttc::QoS::AtLeastOnce, false,
        serde_json::to_vec(&event_payload).unwrap()).await {
        tracing::warn!("事件发布失败: {}", e);
    }
    
    tracing::info!("发帖: {} → post_id={} (board: {})", user.name, post_id, board_key);
    // Webhook 转发 (spawn_blocking 避免 lifetime 问题)
    let whs = state.webhooks.read().await;
    if let Some(urls) = whs.get(&board_key) {
        let url_list: Vec<String> = urls.clone();
        let bk = board_key.clone();
        let author_name = user.name.clone();
        let content_str = content.to_string();
        tokio::task::spawn_blocking(move || {
            for url in &url_list {
                let wh_payload = serde_json::json!({
                    "board": bk, "event": "new_post",
                    "post_id": post_id, "author": author_name, "content": content_str
                });
                if let Err(e) = ureq::post(url)
                    .set("Content-Type", "application/json")
                    .send_json(wh_payload) {
                    tracing::warn!("Webhook 失败 [{}]: {}", url, e);
                }
            }
        });
    }
}

fn parse_post(topic: &str, payload: &[u8]) -> Option<(String, BbsRequest)> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() < 4 { return None; }
    let board_key = parts[2].to_string();
    let req: BbsRequest = serde_json::from_slice(payload).ok()?;
    Some((board_key, req))
}
