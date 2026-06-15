use std::sync::Arc;
use crate::{AppState, models::BbsRequest};
use crate::mqtt_handler::publish_response;
use crate::db;

pub async fn handle_file_init(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = parse_file_req(topic, payload);
    let corr_id = req.corr_id.as_deref().unwrap_or("");
    let reply_to = req.reply_to.as_deref();
    let filename = req.filename.as_deref().unwrap_or("unknown");
    let total_size = req.total_size.unwrap_or(0);
    let chunk_count = req.chunk_count.unwrap_or(0);
    
    let session_id = format!("{}_{}", uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("x"), board_key);
    
    if let Err(e) = db::create_file_session(&state.db_pool, &session_id, &board_key, filename, total_size, chunk_count, "").await {
        tracing::error!("创建文件会话错误: {}", e);
        return;
    }
    
    let resp = serde_json::json!({"session_id": session_id});
    publish_response(&state.mqtt_client, reply_to, &board_key, "file/response", corr_id, &resp).await;
    tracing::info!("文件初始化: {} ({}B, {} chunks) -> {}", filename, total_size, chunk_count, session_id);
}

pub async fn handle_file_chunk(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (_board_key, req) = parse_file_req(topic, payload);
    let session_id = req.session_id.as_deref().unwrap_or("");
    let seq = req.seq.unwrap_or(0);
    let data_b64 = req.data.as_deref().unwrap_or("");
    
    if session_id.is_empty() || data_b64.is_empty() { return; }
    
    let data = match base64_decode(data_b64) {
        Some(d) => d,
        None => {
            tracing::warn!("base64 解码失败: seq={}", seq);
            return;
        }
    };
    
    if let Err(e) = db::insert_file_chunk(&state.db_pool, session_id, seq, &data).await {
        tracing::error!("写入分片错误: {}", e);
    }
}

pub async fn handle_file_commit(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = parse_file_req(topic, payload);
    let corr_id = req.corr_id.as_deref().unwrap_or("");
    let reply_to = req.reply_to.as_deref();
    let session_id = req.session_id.as_deref().unwrap_or("");
    if session_id.is_empty() { return; }
    
    let session = match db::get_file_session(&state.db_pool, session_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            publish_response(&state.mqtt_client, reply_to, &board_key, "file/response", corr_id,
                &serde_json::json!({"error": "session_not_found"})).await;
            return;
        }
        Err(e) => {
            tracing::error!("查询会话错误: {}", e);
            return;
        }
    };
    
    if session.received_chunks < session.chunk_count {
        publish_response(&state.mqtt_client, reply_to, &board_key, "file/response", corr_id,
            &serde_json::json!({"error": format!("incomplete: {}/{}", session.received_chunks, session.chunk_count)})).await;
        return;
    }
    
    // 读取所有分片并合并
    let chunks = match db::get_file_chunks(&state.db_pool, session_id).await {
        Ok(c) => c,
        Err(e) => { tracing::error!("读取分片错误: {}", e); return; }
    };
    
    let mut data = Vec::new();
    for (_, chunk) in &chunks {
        data.extend_from_slice(chunk);
    }
    
    let file_ref = format!("{}/{}", &session_id[..6], session.filename);
    
    // 存储到 file_store 表
    if let Err(e) = sqlx::query(
        "INSERT INTO file_store (hash, filename, data, size, uploader) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(&file_ref).bind(&session.filename).bind(&data).bind(data.len() as i32).bind("")
    .execute(&state.db_pool).await {
        tracing::warn!("file_store 写入错误 (可能重复): {}", e);
    }
    
    db::complete_file_session(&state.db_pool, session_id).await.ok();
    db::cleanup_file_chunks(&state.db_pool, session_id).await.ok();
    
    publish_response(&state.mqtt_client, reply_to, &board_key, "file/response", corr_id,
        &serde_json::json!({"ref": file_ref})).await;
    tracing::info!("文件合并完成: {} ({} chunks)", file_ref, chunks.len());
}

pub async fn handle_file_download(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = parse_file_req(topic, payload);
    let corr_id = req.corr_id.as_deref().unwrap_or("");
    let reply_to = req.reply_to.as_deref();
    let file_ref = req.file_ref.as_deref().unwrap_or("");
    if file_ref.is_empty() { return; }
    
    let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT data FROM file_store WHERE hash = ?")
        .bind(file_ref)
        .fetch_optional(&state.db_pool).await.unwrap_or(None);
    
    if let Some((data,)) = row {
        let data_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &data);
        publish_response(&state.mqtt_client, reply_to, &board_key, "file/response", corr_id,
            &serde_json::json!({"ref": file_ref, "data": data_b64, "size": data.len()})).await;
    } else {
        publish_response(&state.mqtt_client, reply_to, &board_key, "file/response", corr_id,
            &serde_json::json!({"error": "not_found"})).await;
    }
}

fn parse_file_req(topic: &str, payload: &[u8]) -> (String, BbsRequest) {
    let parts: Vec<&str> = topic.split('/').collect();
    let board_key = parts.get(1).map(|s| s.to_string()).unwrap_or_default();
    let req = serde_json::from_slice(payload).unwrap_or_default();
    (board_key, req)
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}
