use std::sync::Arc;
use crate::AppState;
use mqtt_bbs_rs::models::BbsRequest;

pub async fn handle_webhook_config(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = match parse_webhook(topic, payload) {
        Some(v) => v,
        None => return,
    };
    
    let action = req.action.as_deref().unwrap_or("set");
    let url = req.url.as_deref().unwrap_or("");
    if url.is_empty() { return; }
    
    let mut wh = state.webhooks.write().await;
    match action {
        "set" => {
            wh.entry(board_key.clone()).or_insert_with(Vec::new).push(url.to_string());
            tracing::info!("Webhook 添加: {} -> {}", board_key, url);
        }
        "del" => {
            if let Some(urls) = wh.get_mut(&board_key) {
                urls.retain(|u| u != url);
                tracing::info!("Webhook 删除: {} -> {}", board_key, url);
            }
        }
        _ => tracing::warn!("未知 webhook 操作: {}", action),
    }
}

fn parse_webhook(topic: &str, payload: &[u8]) -> Option<(String, BbsRequest)> {
    let parts: Vec<&str> = topic.split('/').collect();
    // topic: bbs/{board}/webhook/config 或 webhook/config
    let board_key = if parts.len() >= 3 { parts[2].to_string() } else { return None };
    let req: BbsRequest = serde_json::from_slice(payload).ok()?;
    Some((board_key, req))
}
