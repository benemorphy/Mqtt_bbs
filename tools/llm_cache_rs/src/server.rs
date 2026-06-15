use std::sync::Arc;
use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use tracing::{info, warn, error, debug};

use crate::cache::LlmCache;
use crate::protocol::*;

/// Named Pipe 名称
const PIPE_NAME: &str = r"\\.\pipe\llm_cache";
/// 创建管道实例失败后的重试间隔
const RETRY_INTERVAL: Duration = Duration::from_millis(500);
/// 最大并发管道实例数 — 5实例支持多客户端并发
const MAX_INSTANCES: usize = 5;

/// 缓存守护进程
pub struct CacheServer {
    cache: Arc<LlmCache>,
}

impl CacheServer {
    pub fn new(capacity: usize, default_ttl_secs: u64, max_entry_bytes: usize) -> Self {
        Self {
            cache: Arc::new(LlmCache::new(capacity, default_ttl_secs, max_entry_bytes)),
        }
    }

    /// 获取缓存核心引用（用于后台监控）
    pub fn cache_ref(&self) -> Arc<LlmCache> {
        self.cache.clone()
    }

    /// 启动 Named Pipe 服务 (异步循环, 多实例并发模式)
    ///
    /// 架构说明:
    /// - 每个 loop 迭代创建一个新的管道实例 (最多 MAX_PIPE_INSTANCES = 255 个)
    /// - 每个实例通过 tokio::spawn 派生子任务独立处理客户端
    /// - 主循环持续创建新的实例, 实现多客户端并发接入
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let semaphore = Arc::new(Semaphore::new(MAX_INSTANCES));
        info!("LLM Cache Daemon 启动中... (实例池模式)");
        info!("  管道: {}", PIPE_NAME);
        info!("  最大实例: {}", MAX_INSTANCES);
        info!("  容量: {} 条目", self.cache.capacity());
        info!("  默认 TTL: {:?}", Duration::from_secs(3600));

        loop {
            // 获取实例许可 — 达到 MAX_INSTANCES 后阻塞等待现有实例释放
            let permit = semaphore.clone().acquire_owned().await?;
            info!("实例许可已获取 (活跃实例: {}/{} 待分配)",
                  MAX_INSTANCES - semaphore.available_permits(),
                  MAX_INSTANCES);

            // 创建 Named Pipe 服务端实例
            let pipe = match ServerOptions::new()
                .create(PIPE_NAME)
            {
                Ok(p) => p,
                Err(e) => {
                    error!("创建管道实例失败: {} (将在 {:?} 后重试)", e, RETRY_INTERVAL);
                    sleep(RETRY_INTERVAL).await;
                    continue;
                }
            };

            let cache = self.cache.clone();

            // 为每个管道实例派生子任务, permit 随 task 生命周期自动释放
            tokio::spawn(async move {
                // 等待客户端连接到此实例
                if let Err(e) = pipe.connect().await {
                    error!("管道连接等待失败: {}", e);
                    drop(permit);
                    return;
                }

                debug!("新客户端已连接 (管道实例)");

                // 处理客户端请求 (循环处理, 直到客户端断开)
                if let Err(e) = handle_client(pipe, cache).await {
                    error!("客户端处理异常: {}", e);
                }

                debug!("客户端已断开 (管道实例), 释放实例许可");
                drop(permit);
            });
        }
    }
}

/// 处理单个客户端连接
async fn handle_client(
    mut pipe: tokio::net::windows::named_pipe::NamedPipeServer,
    cache: Arc<LlmCache>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = vec![0u8; 524288]; // 512KB buffer (容纳200KB条目+JSON开销)
    let max_msg = buf.len();

    loop {
        // 读取消息长度 (4字节, 小端序 u32)
        let mut len_buf = [0u8; 4];
        let n = pipe.read_exact(&mut len_buf).await;
        if n.is_err() {
            // 客户端断开
            debug!("客户端断开连接 (读取长度失败)");
            break;
        }
        let msg_len = u32::from_le_bytes(len_buf) as usize;

        if msg_len > max_msg {
            warn!("消息过大: {} bytes, 排空跳过", msg_len);
            // 排空剩余数据以保持管道同步
            let drain_size = msg_len.min(max_msg);
            if let Err(e) = pipe.read_exact(&mut buf[..drain_size]).await {
                warn!("排空跳过时读取失败: {}", e);
                break;
            }
            continue;
        }

        // 读取消息体
        pipe.read_exact(&mut buf[..msg_len]).await?;
        let raw = String::from_utf8_lossy(&buf[..msg_len]);

        // 解析请求
        let req: CacheRequest = match serde_json::from_str(&raw) {
            Ok(r) => r,
            Err(e) => {
                error!("JSON 解析失败: {} | raw: {}", e, &raw[..raw.len().min(200)]);
                let resp = CacheResponse {
                    ok: false,
                    data: None,
                    error: Some(format!("JSON parse error: {}", e)),
                };
                send_response(&mut pipe, &resp).await?;
                continue;
            }
        };

        // 处理请求
        let response = process_request(req, &cache);

        // 发送响应
        send_response(&mut pipe, &response).await?;
    }

    Ok(())
}

/// 处理请求
fn process_request(req: CacheRequest, cache: &LlmCache) -> CacheResponse {
    match req {
        CacheRequest::Lookup { key } => {
            match cache.lookup(&key) {
                Some(value) => CacheResponse {
                    ok: true,
                    data: Some(serde_json::json!({
                        "hit": true,
                        "value": value,
                    })),
                    error: None,
                },
                None => CacheResponse {
                    ok: true,
                    data: Some(serde_json::json!({
                        "hit": false,
                        "value": null,
                    })),
                    error: None,
                },
            }
        }
        CacheRequest::Store { key, hkey, value, ttl_secs } => {
            cache.store(key, hkey, value, ttl_secs);
            CacheResponse { ok: true, data: None, error: None }
        }
        CacheRequest::Delete { key } => {
            let deleted = cache.delete(&key);
            CacheResponse {
                ok: true,
                data: Some(serde_json::json!({"deleted": deleted})),
                error: None,
            }
        }
        CacheRequest::Clear => {
            cache.clear();
            CacheResponse { ok: true, data: None, error: None }
        }
        CacheRequest::Stats => {
            let stats = cache.stats();
            CacheResponse {
                ok: true,
                data: Some(serde_json::to_value(&stats).unwrap_or_default()),
                error: None,
            }
        }
        CacheRequest::Ping => {
            let stats = cache.stats();
            CacheResponse {
                ok: true,
                data: Some(serde_json::json!({
                    "pong": true,
                    "uptime_secs": stats.uptime_secs,
                    "entries": stats.entries,
                    "hit_rate": stats.hit_rate,
                })),
                error: None,
            }
        }
        CacheRequest::BatchLookup { exact_key, hkey_prefix } => {
            let exact_hit = cache.lookup(&exact_key);
            let (hierarchical_hits, hierarchical_values) = if let Some(prefix) = hkey_prefix {
                let results = cache.lookup_hkey(&prefix);
                let hits: Vec<String> = results.iter().map(|(k, _)| k.clone()).collect();
                let vals: Vec<Vec<String>> = results.into_iter().map(|(_, v)| v).collect();
                (hits, vals)
            } else {
                (vec![], vec![])
            };

            CacheResponse {
                ok: true,
                data: Some(serde_json::json!({
                    "exact_hit": exact_hit.is_some(),
                    "exact_value": exact_hit,
                    "hierarchical_hits": hierarchical_hits,
                    "hierarchical_values": hierarchical_values,
                })),
                error: None,
            }
        }
    }
}

/// 发送响应 (4字节长度前缀 + JSON)
async fn send_response(
    pipe: &mut tokio::net::windows::named_pipe::NamedPipeServer,
    resp: &CacheResponse,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string(resp)?;
    let len = json.len() as u32;
    pipe.write_all(&len.to_le_bytes()).await?;
    pipe.write_all(json.as_bytes()).await?;
    pipe.flush().await?;
    Ok(())
}

/// 检查缓存守护进程是否在运行
pub async fn ping_server() -> Result<CacheResponse, Box<dyn std::error::Error>> {
    let mut client = ClientOptions::new()
        .open(PIPE_NAME)?;

    let req = CacheRequest::Ping;
    let json = serde_json::to_string(&req)?;
    let len = json.len() as u32;

    client.write_all(&len.to_le_bytes()).await?;
    client.write_all(json.as_bytes()).await?;
    client.flush().await?;

    let mut len_buf = [0u8; 4];
    client.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;

    let mut buf = vec![0u8; resp_len];
    client.read_exact(&mut buf).await?;
    let resp: CacheResponse = serde_json::from_slice(&buf)?;

    Ok(resp)
}
