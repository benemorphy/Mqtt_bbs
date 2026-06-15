//! # LLM Cache Daemon — Rust 核心库
//!
//! 常驻内存的 LRU+LFU+TTL 混合淘汰缓存，通过 Named Pipe 提供 IPC 服务。
//!
//! ## 模块结构
//! - `cache` — 缓存核心 (CacheEntry + LlmCache)
//! - `protocol` — 序列化协议 (CacheRequest / CacheResponse)
//! - `stats` — 统计与健康状态
//! - `server` — Named Pipe 守护进程 (服务端)
//! - `client` — Named Pipe 客户端 (本模块内置)
//!
//! ## 快速开始 (客户端)
//! ```no_run
//! use llm_cache_rs::CacheClient;
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = CacheClient::new(r"\\.\pipe\llm_cache".to_string());
//!
//!     // 健康检查
//!     let resp = client.ping().await.unwrap();
//!     println!("Ping: {:?}", resp);
//!
//!     // 存储缓存
//!     client.store(
//!         "abc123".to_string(),
//!         Some("model:gpt4".to_string()),
//!         vec!["Hello, world!".to_string()],
//!         None,
//!     ).await.unwrap();
//!
//!     // 查找缓存
//!     let resp = client.lookup("abc123".to_string()).await.unwrap();
//!     println!("Lookup: {:?}", resp);
//! }
//! ```

pub mod cache;
pub mod protocol;
pub mod stats;
pub mod server;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::windows::named_pipe::ClientOptions;
use tracing::{debug, warn};

use crate::protocol::*;

/// 默认 Named Pipe 名称
pub const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\llm_cache";

/// 默认超时时间 (5 秒)
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// # Named Pipe 客户端
///
/// 封装与 LLM Cache Daemon 的 IPC 通信，提供类型安全的接口。
/// 每个方法内部自动连接/断开 Pipe，支持并发调用。
#[derive(Debug, Clone)]
pub struct CacheClient {
    pipe_name: String,
    timeout: Duration,
}

impl Default for CacheClient {
    fn default() -> Self {
        Self {
            pipe_name: DEFAULT_PIPE_NAME.to_string(),
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl CacheClient {
    /// 创建新的缓存客户端
    pub fn new(pipe_name: String) -> Self {
        Self {
            pipe_name,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// 创建使用默认 Pipe 名称的客户端
    pub fn new_default() -> Self {
        Self::default()
    }

    /// 设置超时时间
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 精确查找缓存
    pub async fn lookup(&self, key: String) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::Lookup { key };
        self.send_request(&req).await
    }

    /// 存储缓存条目
    pub async fn store(
        &self,
        key: String,
        hkey: Option<String>,
        value: Vec<String>,
        ttl_secs: Option<u64>,
    ) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::Store {
            key,
            hkey,
            value,
            ttl_secs,
        };
        self.send_request(&req).await
    }

    /// 删除缓存条目
    pub async fn delete(&self, key: String) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::Delete { key };
        self.send_request(&req).await
    }

    /// 清空全部缓存
    pub async fn clear(&self) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::Clear;
        self.send_request(&req).await
    }

    /// 查询统计信息
    pub async fn stats(&self) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::Stats;
        self.send_request(&req).await
    }

    /// 健康检查
    pub async fn ping(&self) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::Ping;
        self.send_request(&req).await
    }

    /// 批量查找 (精确 + 分层)
    pub async fn batch_lookup(
        &self,
        exact_key: String,
        hkey_prefix: Option<String>,
    ) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        let req = CacheRequest::BatchLookup {
            exact_key,
            hkey_prefix,
        };
        self.send_request(&req).await
    }

    // ---- 内部方法 ----

    /// 发送请求并接收响应
    async fn send_request(
        &self,
        req: &CacheRequest,
    ) -> Result<CacheResponse, Box<dyn std::error::Error>> {
        // 连接 Named Pipe
        let mut client = ClientOptions::new()
            .open(&self.pipe_name)?;

        // 序列化请求
        let json = serde_json::to_string(req)?;
        let len = json.len() as u32;

        debug!("Sending request ({} bytes): {}", len, &json[..64.min(json.len())]);

        // 发送长度前缀 + 消息体
        client.write_all(&len.to_le_bytes()).await?;
        client.write_all(json.as_bytes()).await?;
        client.flush().await?;

        // 读取响应长度 (4字节)
        let mut len_buf = [0u8; 4];
        client.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;

        if resp_len > 1024 * 1024 {
            // 响应超过 1MB，视为异常
            warn!("Response too large: {} bytes", resp_len);
            return Err("response too large (>1MB)".into());
        }

        // 读取响应体
        let mut buf = vec![0u8; resp_len];
        client.read_exact(&mut buf).await?;

        // 反序列化
        let resp: CacheResponse = serde_json::from_slice(&buf)?;

        debug!("Received response ({} bytes): ok={}", resp_len, resp.ok);

        Ok(resp)
    }
}

/// 便捷函数：创建默认客户端
pub fn default_client() -> CacheClient {
    CacheClient::new_default()
}

/// 便捷函数：发送 Ping 检查服务是否运行
pub async fn ping_server() -> Result<CacheResponse, Box<dyn std::error::Error>> {
    let client = CacheClient::new_default();
    client.ping().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_default() {
        let client = CacheClient::new_default();
        assert_eq!(client.pipe_name, DEFAULT_PIPE_NAME);
        assert_eq!(client.timeout, DEFAULT_TIMEOUT);
    }

    #[test]
    fn test_client_custom_pipe() {
        let client = CacheClient::new(r"\\.\pipe\test_cache".to_string());
        assert_eq!(client.pipe_name, r"\\.\pipe\test_cache");
    }
}
