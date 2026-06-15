use std::time::Duration;
use tokio::signal;
use tracing::{info, error, warn};
use tracing_subscriber::EnvFilter;

mod cache;
mod protocol;
mod server;

/// 容量绝对硬上限 — 2GB内存 / 512KB每条目 ≈ 4096
const MAX_CAPACITY: usize = 4096;
/// 默认容量 (可被 LLM_CACHE_CAPACITY 环境变量覆盖)
const DEFAULT_CAPACITY: usize = 2000;
/// 单条目最大字节 — LLM响应可到150KB (超限会触发Named Pipe缓冲区问题)
const MAX_ENTRY_BYTES: usize = 150 * 1024;

/// 估算单条目最大内存 (bytes) — 用于启动时预警
const ESTIMATED_BYTES_PER_ENTRY: u64 = 1024 * 512; // 512KB

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .with_target(true)
        .with_ansi(false) // 禁用 ANSI 颜色码，避免管道重定向时乱码
        .init();

    info!("================================================");
    info!("  LLM Cache Daemon v0.1.0 (hardened)");
    info!("  缓存守护进程 — 永久驻留内存");
    info!("================================================");

    // ═══ 容量安全解析 ═══
    let raw_capacity: usize = std::env::var("LLM_CACHE_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CAPACITY);
    let capacity = raw_capacity.min(MAX_CAPACITY);
    if raw_capacity != capacity {
        warn!(
            "LLM_CACHE_CAPACITY={} 超过硬上限 {}, 已自动裁剪",
            raw_capacity, MAX_CAPACITY
        );
    }
    let estimated_max_mb = (capacity as u64 * ESTIMATED_BYTES_PER_ENTRY) / 1048576;
    info!("容量: {} 条目 (上限: {})", capacity, MAX_CAPACITY);
    info!("预估最大内存: ~{} MB (单条目 ~{} KB)", estimated_max_mb, ESTIMATED_BYTES_PER_ENTRY / 1024);

    let ttl_secs: u64 = std::env::var("LLM_CACHE_TTL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3600);
    let pipe_name: String = std::env::var("LLM_CACHE_PIPE")
        .unwrap_or_else(|_| r"\\.\pipe\llm_cache".to_string());

    info!("配置:");
    info!("  管道: {}", pipe_name);
    info!("  容量: {} 条目", capacity);
    info!("  默认 TTL: {} 秒", ttl_secs);

    // 创建缓存服务
    let server = server::CacheServer::new(capacity, ttl_secs, MAX_ENTRY_BYTES);

    // 提前克隆后台监控引用（必须在 move 之前）
    let monitor_cache = server.cache_ref();

    // 启动服务
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("缓存服务异常退出: {}", e);
        }
    });

    // ═══ 后台TTL淘汰任务 + 内存监控 ═══
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;

            // 1) TTL过期条目淘汰
            let evicted = monitor_cache.evict_expired();

            // 2) 内存监控
            let stats = monitor_cache.stats();
            let mem_bytes = monitor_cache.memory_estimate();
            let mem_mb = mem_bytes as f64 / 1048576.0;

            tracing::info!(
                "[Memory Monitor] entries={}/{}, hit_rate={:.1}%, mem_estimate={:.2}MB, evicted={}, total_evict={}, total_expire={}",
                stats.entries,
                stats.capacity,
                stats.hit_rate,
                mem_mb,
                evicted,
                stats.evict_count,
                stats.expire_count,
            );

            // 3) 内存预警
            if stats.entries as f64 > stats.capacity as f64 * 0.9 {
                tracing::warn!(
                    "[Memory Monitor] 缓存接近容量上限! {}/{} ({:.0}%)",
                    stats.entries,
                    stats.capacity,
                    stats.entries as f64 / stats.capacity as f64 * 100.0
                );
            }
        }
    });

    info!("后台监控: TTL淘汰周期=60秒, 内存统计实时上报");
    info!("按 Ctrl+C 停止服务...");
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("收到中断信号, 正在优雅关闭...");
        }
        Err(e) => {
            error!("无法注册信号处理器: {}", e);
        }
    }

    // 等待服务结束
    server_handle.abort();
    monitor_handle.abort();
    info!("LLM Cache Daemon 已停止");
}
