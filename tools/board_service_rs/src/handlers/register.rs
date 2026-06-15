use std::sync::Arc;
use crate::{AppState, models::BbsRequest};
use crate::mqtt_handler::publish_response;
use crate::db;

pub async fn handle_register(state: &Arc<AppState>, topic: &str, payload: &[u8]) {
    let (board_key, req) = match parse_register(topic, payload) {
        Some(v) => v,
        None => return,
    };

    // ── 验证 JWT（Gateway:8000 签发的用户认证 JWT） ──
    let jwt_payload = if let Some(ref jwt_str) = req.token {
        match jsonwebtoken::decode::<serde_json::Value>(
            jwt_str,
            &jsonwebtoken::DecodingKey::from_secret(state.config.jwt_secret.as_bytes()),
            &jsonwebtoken::Validation::default(),
        ) {
            Ok(data) => data.claims,
            Err(e) => {
                tracing::warn!("JWT 验证失败: {}", e);
                return;
            }
        }
    } else {
        // allow_anonymous: 无 JWT 仍可注册（匿名模式）
        serde_json::json!({"sub": "anonymous"})
    };

    let name = jwt_payload["sub"].as_str().unwrap_or("anonymous");
    let agent_id = req.agent_id.as_deref().unwrap_or(req.name.as_deref().unwrap_or(name));

    // ── 生成 BoardService token（16 位短 token，用于后续消息认证） ──
    let token = uuid::Uuid::new_v4().to_string()[..16].to_string();

    // ── 签发 JWT（BoardService 的业务 JWT，供后续 API 调用） ──
    let jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &serde_json::json!({
            "sub": name, "agent_id": agent_id,
            "exp": chrono::Utc::now().timestamp() + 86400,
            "iat": chrono::Utc::now().timestamp(),
        }),
        &jsonwebtoken::EncodingKey::from_secret(state.config.jwt_secret.as_bytes())
    ).unwrap_or_default();

    // ── 注册时尝试同步 user_id（从 JWT payload 提取） ──
    let user_id: Option<i64> = jwt_payload["user_id"].as_i64()
        .or_else(|| jwt_payload["sub"].as_str()
            .and_then(|s| s.parse::<i64>().ok()));

    if let Some(uid) = user_id {
        if let Err(e) = db::upsert_user_with_id(&state.db_pool, &board_key, name, &token, uid).await {
            tracing::warn!("注册同步 user_id 失败(将用无ID方式重试): {}", e);
            let _ = db::upsert_user(&state.db_pool, &board_key, name, &token).await;
        }
    } else {
        if let Err(e) = db::upsert_user(&state.db_pool, &board_key, name, &token).await {
            tracing::error!("注册 DB 错误: {}", e);
            return;
        }
    }

    let reply_to = req.reply_to.as_deref();
    let corr_id = req.corr_id.as_deref().unwrap_or("");

    let resp = serde_json::json!({"token": token, "name": name, "jwt": jwt});
    publish_response(&state.mqtt_client, reply_to, &board_key, "register/response", corr_id, &resp).await;

    tracing::info!("注册: {} → token={} (board: {}, jwt={})", name, &token[..8], board_key, &jwt[..20]);
}

fn parse_register(topic: &str, payload: &[u8]) -> Option<(String, BbsRequest)> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() < 4 { return None; }
    let board_key = parts[2].to_string();
    
    let req: BbsRequest = serde_json::from_slice(payload).ok()?;
    if req.name.as_deref().unwrap_or("").is_empty() {
        return None;
    }
    Some((board_key, req))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_register_valid() {
        let topic = "agent/bbs/test-board/register";
        let payload = br#"{"name": "agent-001", "token": "abc"}"#;
        let result = parse_register(topic, payload);
        assert!(result.is_some());
        let (board, req) = result.unwrap();
        assert_eq!(board, "test-board");
        assert_eq!(req.name.as_deref(), Some("agent-001"));
    }

    #[test]
    fn test_parse_register_short_topic() {
        let result = parse_register("short", b"{}");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_register_empty_name() {
        let topic = "agent/bbs/test-board/register";
        let payload = br#"{"name": ""}"#;
        let result = parse_register(topic, payload);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_register_invalid_json() {
        let topic = "agent/bbs/test-board/register";
        let result = parse_register(topic, b"not-json");
        assert!(result.is_none());
    }
}
