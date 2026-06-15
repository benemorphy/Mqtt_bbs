use serde::{Deserialize, Serialize};

/// 请求类型
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum CacheRequest {
    /// 精确查找缓存 (SHA256 hash key)
    Lookup { key: String },
    /// 存储缓存条目
    Store {
        key: String,           // 精确 key (SHA256)
        hkey: Option<String>,  // 分层 key (可选, 同时存两份)
        value: Vec<String>,    // 响应 chunks 列表
        ttl_secs: Option<u64>, // 自定义 TTL (None = 使用默认)
    },
    /// 删除缓存条目
    Delete { key: String },
    /// 清空全部缓存
    Clear,
    /// 查询统计信息
    Stats,
    /// 健康检查
    Ping,
    /// 批量查找 (用于 P2 分层匹配)
    BatchLookup {
        exact_key: String,
        hkey_prefix: Option<String>,
    },
}

/// 响应类型
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 查找响应数据
#[derive(Debug, Serialize)]
pub struct LookupData {
    pub hit: bool,
    pub value: Option<Vec<String>>,
    pub age_secs: Option<f64>,
    pub access_count: Option<u64>,
}

/// 批量查找响应
#[derive(Debug, Serialize)]
pub struct BatchLookupData {
    pub exact_hit: bool,
    pub exact_value: Option<Vec<String>>,
    pub hierarchical_hits: Vec<String>,
    pub hierarchical_values: Vec<Vec<String>>,
}

/// 统计信息
#[derive(Debug, Serialize)]
pub struct StatsData {
    pub entries: usize,
    pub capacity: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub store_count: u64,
    pub evict_count: u64,
    pub expire_count: u64,
    pub hit_rate: f64,
    pub memory_estimate_bytes: u64,
    pub uptime_secs: u64,
    pub oldest_entry_secs: f64,
    pub newest_entry_secs: f64,
}
