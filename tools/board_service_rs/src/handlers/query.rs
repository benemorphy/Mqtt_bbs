use std::sync::Arc;
use crate::{AppState, models::BbsRequest};
use crate::mqtt_handler::publish_response;
use crate::db;

pub async fn handle_query(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = match parse_query(topic, payload) {
        Some(v) => v,
        None => return,
    };
    
    let corr_id = req.corr_id.as_deref().unwrap_or("");
    let reply_to = req.reply_to.as_deref();
    let query_type = req.req_type.as_deref().unwrap_or("posts");
    let params = req.params.unwrap_or(serde_json::json!({}));
    
    let result = match query_type {
        "posts" => {
            let author = params.get("author").and_then(|v| v.as_str());
            let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);
            let offset = params.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
            let posts = db::query_posts(&state.db_pool, &board_key, author, limit, offset).await;
            match posts {
                Ok(p) => serde_json::to_value(p).unwrap_or_default(),
                Err(e) => { tracing::error!("查询帖子错误: {}", e); return; }
            }
        }
        "count" => {
            let author = params.get("author").and_then(|v| v.as_str());
            let count = db::count_posts(&state.db_pool, &board_key, author).await;
            match count {
                Ok(c) => serde_json::json!({"total": c}),
                Err(e) => { tracing::error!("统计错误: {}", e); return; }
            }
        }
        "authors" => {
            let authors = db::list_authors(&state.db_pool, &board_key).await;
            match authors {
                Ok(a) => serde_json::to_value(a).unwrap_or_default(),
                Err(e) => { tracing::error!("列出作者错误: {}", e); return; }
            }
        }
        "poll" => {
            let since_id = params.get("since_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);
            // 使用 id > since_id 查询新帖子
            let rows = sqlx::query_as::<_, crate::models::Post>(
                "SELECT id, board, author, content, created_at FROM bbs_posts
                 WHERE board = ? AND id > ? ORDER BY id DESC LIMIT ?"
            )
            .bind(&board_key).bind(since_id).bind(limit)
            .fetch_all(&state.db_pool).await;
            match rows {
                Ok(r) => serde_json::to_value(r).unwrap_or_default(),
                Err(e) => { tracing::error!("轮询错误: {}", e); return; }
            }
        }
        _ => {
            tracing::warn!("未知查询类型: {}", query_type);
            return;
        }
    };
    
    let resp = serde_json::json!({"type": query_type, "data": result});
    publish_response(&state.mqtt_client, reply_to, &board_key, "query/response", corr_id, &resp).await;
    tracing::debug!("查询: {} (board: {}, corr: {})", query_type, board_key, &corr_id[..corr_id.len().min(8)]);
}

fn parse_query(topic: &str, payload: &[u8]) -> Option<(String, BbsRequest)> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() < 4 { return None; }
    let board_key = parts[2].to_string();
    let req: BbsRequest = serde_json::from_slice(payload).ok()?;
    Some((board_key, req))
}
