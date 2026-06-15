use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// BBS 帖子
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Post {
    pub id: i64,
    pub board: String,
    pub author: String,
    pub content: String,
    pub created_at: chrono::NaiveDateTime,
}

/// 注册用户
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UserRow {
    pub token: String,
    pub name: String,
    pub board: String,
}

/// 文件上传会话
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FileSession {
    pub session_id: String,
    pub board_key: String,
    pub filename: String,
    pub total_size: i64,
    pub chunk_count: i32,
    pub received_chunks: i32,
    pub status: String,
    pub uploader: String,
    pub created_at: chrono::NaiveDateTime,
}

/// MQTT 请求消息的通用结构
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BbsRequest {
    pub agent_id: Option<String>,
    pub token: Option<String>,
    pub content: Option<String>,
    pub name: Option<String>,
    pub corr_id: Option<String>,
    pub reply_to: Option<String>,
    #[serde(rename = "type")]
    pub req_type: Option<String>,
    pub params: Option<serde_json::Value>,
    pub filename: Option<String>,
    pub total_size: Option<i64>,
    pub chunk_count: Option<i32>,
    pub session_id: Option<String>,
    pub seq: Option<i32>,
    pub data: Option<String>,        // base64 编码的数据
    pub file_ref: Option<String>,
    pub action: Option<String>,
    pub url: Option<String>,
}
