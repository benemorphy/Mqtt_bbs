/// 统计信息 — 桥接模块
/// 从 cache::StatsSnapshot 和 protocol::StatsData 统一导出
pub use crate::cache::StatsSnapshot;
pub use crate::protocol::StatsData;

/// 缓存健康状态
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthStatus {
    pub pipe_ok: bool,
    pub entries: usize,
    pub hit_rate: f64,
    pub uptime_secs: u64,
}
