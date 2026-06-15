use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tracing::warn;

/// 缓存条目
pub struct CacheEntry {
    /// 精确 key (SHA256 hex) — Arc 共享减少副本
    pub key: Arc<str>,
    /// 分层 key (可选)
    pub hkey: Option<String>,
    /// 缓存值 (LLM response chunks)
    pub value: Vec<String>,
    /// 创建时间
    pub created_at: Instant,
    /// 最后访问时间
    pub last_access: Instant,
    /// 访问频率计数
    pub access_count: u64,
    /// TTL
    pub ttl: Duration,
    /// 预估内存占用 (bytes)
    pub estimated_size: u64,
}

impl CacheEntry {
    pub fn new(key: Arc<str>, hkey: Option<String>, value: Vec<String>, ttl: Duration) -> Self {
        let estimated_size = std::mem::size_of::<Self>() as u64
            + key.len() as u64 + std::mem::size_of::<Arc<str>>() as u64
            + hkey.as_ref().map_or(0, |h| h.capacity() as u64 + std::mem::size_of::<String>() as u64)
            + value.iter().map(|v| v.capacity() as u64 + std::mem::size_of::<String>() as u64).sum::<u64>()
            + (value.capacity() * std::mem::size_of::<String>()) as u64;  // Vec内部指针数组
        Self {
            key,
            hkey,
            value,
            created_at: Instant::now(),
            last_access: Instant::now(),
            access_count: 1,
            ttl,
            estimated_size,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() >= self.ttl
    }

    pub fn age_secs(&self) -> f64 {
        self.created_at.elapsed().as_secs_f64()
    }

    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }
}

/// LFU + LRU 混合淘汰缓存
pub struct LlmCache {
    /// 主存储: key -> CacheEntry (Arc<str>共享key减少内存副本)
    entries: RwLock<HashMap<Arc<str>, CacheEntry>>,
    /// 分层 key 索引: hkey_prefix -> [exact_keys]
    hkey_index: RwLock<HashMap<String, Vec<Arc<str>>>>,
    /// LFU 频率链表: access_count -> Vec<key> (用于淘汰)
    freq_list: RwLock<HashMap<u64, VecDeque<Arc<str>>>>,
    /// 最小频率 (LFU 淘汰起点)
    min_freq: AtomicU64,
    /// 容量上限
    capacity: usize,
    /// 当前条目数
    count: AtomicU64,
    /// 统计计数
    pub hit_count: AtomicU64,
    pub miss_count: AtomicU64,
    pub store_count: AtomicU64,
    pub evict_count: AtomicU64,
    pub expire_count: AtomicU64,
    /// 默认 TTL
    default_ttl: Duration,
    /// 单条目最大字节上限 (防内存泄漏)
    max_entry_bytes: usize,
    /// 启动时间
    started_at: Instant,
}

impl LlmCache {
    pub fn new(capacity: usize, default_ttl_secs: u64, max_entry_bytes: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::with_capacity(capacity)),
            hkey_index: RwLock::new(HashMap::new()),
            freq_list: RwLock::new(HashMap::new()),
            min_freq: AtomicU64::new(1),
            capacity,
            count: AtomicU64::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            store_count: AtomicU64::new(0),
            evict_count: AtomicU64::new(0),
            expire_count: AtomicU64::new(0),
            default_ttl: Duration::from_secs(default_ttl_secs),
            max_entry_bytes,
            started_at: Instant::now(),
        }
    }

    /// 获取容量上限
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 精确查找 (读写锁分离: 读锁检查, 微写锁更新元数据)
    pub fn lookup(&self, key: &str) -> Option<Vec<String>> {
        // 阶段1: 读锁快速检查存在性和过期 (不阻塞写入)
        let (found, expired, value) = {
            let entries = self.entries.read();
            match entries.get(key) {
                None => (false, false, None),
                Some(e) if e.is_expired() => (true, true, None),
                Some(e) => (true, false, Some(e.value.clone())),
            }
        };

        if !found {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        if expired {
            // 阶段2: 写锁清理过期条目
            let mut entries = self.entries.write();
            if let Some(entry) = entries.remove(key) {
                self.remove_from_freq(key);
                if let Some(hk) = entry.hkey {
                    let mut index = self.hkey_index.write();
                    if let Some(keys) = index.get_mut(&hk) {
                        keys.retain(|k| k.as_ref() != key);
                        if keys.is_empty() { index.remove(&hk); }
                    }
                }
                self.expire_count.fetch_add(1, Ordering::Relaxed);
                self.count.fetch_sub(1, Ordering::Relaxed);
            }
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // 阶段3: 微写锁仅更新访问元数据 (缩小写锁窗口至最小)
        {
            let mut entries = self.entries.write();
            if let Some(entry) = entries.get_mut(key) {
                let old_freq = entry.access_count;
                entry.touch();
                self.update_freq(entry.key.clone(), old_freq, entry.access_count);
            }
        }
        self.hit_count.fetch_add(1, Ordering::Relaxed);
        Some(value.unwrap())
    }

    /// 分层 key 查找 (前缀匹配)
    /// 注意：过期条目会在查找时收集并在锁释放后清理（避免锁序死锁）
    pub fn lookup_hkey(&self, prefix: &str) -> Vec<(String, Vec<String>)> {
        let mut expired_keys: Vec<Arc<str>> = Vec::new();
        let results = {
            let index = self.hkey_index.read();
            let entries = self.entries.read();
            let mut results = Vec::new();
            for (hkey, keys) in index.iter() {
                if hkey.starts_with(prefix) {
                    for k in keys {
                        if let Some(entry) = entries.get(k.as_ref()) {
                            if !entry.is_expired() {
                                results.push((hkey.clone(), entry.value.clone()));
                            } else {
                                expired_keys.push(k.clone());
                            }
                        }
                    }
                }
            }
            results
        };
        // 释放读锁后，单独清理过期条目（避免 entries→hkey_index 与 hkey_index→entries 锁序死锁）
        if !expired_keys.is_empty() {
            let mut entries = self.entries.write();
            for key in &expired_keys {
                if let Some(entry) = entries.remove(key.as_ref()) {
                    self.remove_from_freq(key.as_ref());
                    if let Some(hk) = entry.hkey {
                        let mut index = self.hkey_index.write();
                        if let Some(keys) = index.get_mut(&hk) {
                            keys.retain(|k| k.as_ref() != key.as_ref());
                            if keys.is_empty() {
                                index.remove(&hk);
                            }
                        }
                    }
                    self.expire_count.fetch_add(1, Ordering::Relaxed);
                    self.count.fetch_sub(1, Ordering::Relaxed);
                }
            }
            drop(entries);
            self.min_freq.store(
                self.freq_list.read().keys().min().copied().unwrap_or(1),
                Ordering::Relaxed,
            );
            tracing::debug!("lookup_hkey: 顺手清理 {} 个过期条目", expired_keys.len());
        }
        results
    }

    /// 存储缓存
    pub fn store(&self, key: String, hkey: Option<String>, value: Vec<String>, ttl_secs: Option<u64>) {
        let ttl = ttl_secs.map(Duration::from_secs).unwrap_or(self.default_ttl);

        // 防内存泄漏: 检查条目大小, 超大值跳过 (上限由构造参数控制)
        let value_size: usize = value.iter().map(|s| s.len()).sum();
        if value_size > self.max_entry_bytes {
            warn!(
                "条目过大: key={}.. ({} bytes > {} 上限), 跳过存储",
                &key[..key.len().min(16)],
                value_size,
                self.max_entry_bytes
            );
            return;
        }

        // 转换为 Arc<str> 共享 key 副本
        let arc_key: Arc<str> = Arc::from(key);
        let entry = CacheEntry::new(arc_key.clone(), hkey.clone(), value, ttl);
        let _estimated_size = entry.estimated_size;

        // 检查是否需要淘汰
        let mut entries = self.entries.write();

        // BUG FIX: 防索引/计数膨胀 — 如果 key 已存在, 先完整清理旧条目引用
        let is_replacement = entries.contains_key(arc_key.as_ref());
        if is_replacement {
            if let Some(old_entry) = entries.remove(arc_key.as_ref()) {
                self.remove_from_freq(arc_key.as_ref());
                if let Some(old_hk) = old_entry.hkey {
                    let mut index = self.hkey_index.write();
                    if let Some(keys) = index.get_mut(&old_hk) {
                        keys.retain(|k| k.as_ref() != arc_key.as_ref());
                        if keys.is_empty() {
                            index.remove(&old_hk);
                        }
                    }
                }
                // 替换: 不调整 count (后续 insert 也不 fetch_add)
            }
        } else if entries.len() >= self.capacity {
            self.evict_one(&mut entries);
        }

        let freq = entry.access_count;
        entries.insert(arc_key.clone(), entry);
        if !is_replacement {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        self.store_count.fetch_add(1, Ordering::Relaxed);

        // 更新频率索引
        let mut freq_list = self.freq_list.write();
        freq_list.entry(freq).or_insert_with(VecDeque::new).push_back(arc_key.clone());
        if freq < self.min_freq.load(Ordering::Relaxed) {
            self.min_freq.store(freq, Ordering::Relaxed);
        }

        // 更新分层索引 (检查重复, 避免 Vec 膨胀; 替换路径已在上方清理旧引用)
        if let Some(hk) = hkey {
            drop(freq_list);
            let mut index = self.hkey_index.write();
            let keys_vec = index.entry(hk).or_insert_with(Vec::new);
            if !keys_vec.contains(&arc_key) {
                keys_vec.push(arc_key);
            }
        }
    }

    /// 删除缓存
    /// 注意: 必须同时清理 freq_list 和 hkey_index, 否则索引悬空导致内存泄漏
    pub fn delete(&self, key: &str) -> bool {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(key) {
            self.remove_from_freq(key);
            // BUG FIX: 清理分层索引, 防止 hkey_index 悬空膨胀
            if let Some(hk) = entry.hkey {
                let mut index = self.hkey_index.write();
                if let Some(keys) = index.get_mut(&hk) {
                    keys.retain(|k| k.as_ref() != key);
                    if keys.is_empty() {
                        index.remove(&hk);
                    }
                }
            }
            self.count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// 清空全部
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        let mut freq_list = self.freq_list.write();
        freq_list.clear();
        let mut index = self.hkey_index.write();
        index.clear();
        self.count.store(0, Ordering::Relaxed);
        self.min_freq.store(1, Ordering::Relaxed);
    }

    /// 获取统计信息
    pub fn stats(&self) -> StatsSnapshot {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 * 100.0 } else { 0.0 };

        let entries = self.entries.read();
        let mut oldest = 0.0f64;
        let mut newest = f64::MAX;
        let mut mem_estimate = 0u64;
        for e in entries.values() {
            let age = e.age_secs();
            if age > oldest { oldest = age; }
            if age < newest { newest = age; }
            mem_estimate += e.estimated_size;
        }
        drop(entries);

        StatsSnapshot {
            entries: self.count.load(Ordering::Relaxed) as usize,
            capacity: self.capacity,
            hit_count: hits,
            miss_count: misses,
            store_count: self.store_count.load(Ordering::Relaxed),
            evict_count: self.evict_count.load(Ordering::Relaxed),
            expire_count: self.expire_count.load(Ordering::Relaxed),
            hit_rate,
            memory_estimate_bytes: mem_estimate,
            uptime_secs: self.started_at.elapsed().as_secs(),
            oldest_entry_secs: oldest,
            newest_entry_secs: if newest == f64::MAX { 0.0 } else { newest },
        }
    }

    // ---- 内部方法 ----

    /// LFU 淘汰: 淘汰最小频率条目中最早访问的
    fn evict_one(&self, entries: &mut HashMap<Arc<str>, CacheEntry>) {
        let mut freq_list = self.freq_list.write();
        let min_freq = self.min_freq.load(Ordering::Relaxed);

        // 找到有条目的最小频率
        let evict_freq = (min_freq..).find(|f| {
            freq_list.get(f).map_or(false, |q| !q.is_empty())
        });

        if let Some(freq) = evict_freq {
            if let Some(queue) = freq_list.get_mut(&freq) {
                // LRU 风格: 弹出最早入队的
                if let Some(evict_key) = queue.pop_front() {
                    if queue.is_empty() {
                        freq_list.remove(&freq);
                    }
                    drop(freq_list);

                    if let Some(entry) = entries.remove(evict_key.as_ref()) {
                        self.evict_count.fetch_add(1, Ordering::Relaxed);
                        self.count.fetch_sub(1, Ordering::Relaxed);

                        // 清理分层索引
                        if let Some(hk) = entry.hkey {
                            let mut index = self.hkey_index.write();
                            if let Some(keys) = index.get_mut(&hk) {
                                keys.retain(|k| k.as_ref() != evict_key.as_ref());
                                if keys.is_empty() {
                                    index.remove(&hk);
                                }
                            }
                        }
                    }
                }
            }
        }
        // 更新最小频率 (重新获取锁，因上面freq_list已被drop)
        self.min_freq.store(
            self.freq_list.read().keys().min().copied().unwrap_or(1),
            Ordering::Relaxed,
        );
    }

    fn update_freq(&self, key: Arc<str>, old_freq: u64, new_freq: u64) {
        let mut freq_list = self.freq_list.write();
        if let Some(queue) = freq_list.get_mut(&old_freq) {
            queue.retain(|k| k.as_ref() != key.as_ref());
            if queue.is_empty() {
                freq_list.remove(&old_freq);
            }
        }
        // 传入 Arc<str> 避免锁逆序 (freq_list→entries) 和重入读锁
        freq_list.entry(new_freq).or_insert_with(VecDeque::new).push_back(key);
    }

    fn remove_from_freq(&self, key: &str) {
        let mut freq_list = self.freq_list.write();
        // 在所有频率中查找并移除 (最多扫描所有频率)
        let target_freqs: Vec<u64> = freq_list.keys().copied().collect();
        for freq in target_freqs {
            if let Some(queue) = freq_list.get_mut(&freq) {
                queue.retain(|k| k.as_ref() != key);
                if queue.is_empty() {
                    freq_list.remove(&freq);
                }
            }
        }
    }

    /// 后台淘汰：扫描全部过期条目并清理，返回淘汰数量
    /// 安全处理锁序：先只读收集，再写锁批量删除
    pub fn evict_expired(&self) -> usize {
        // 阶段1: 只读锁下收集过期key
        let expired_keys: Vec<Arc<str>> = {
            let entries = self.entries.read();
            entries.iter()
                .filter(|(_, entry)| entry.is_expired())
                .map(|(key, _)| key.clone())
                .collect()
        };

        if expired_keys.is_empty() {
            return 0;
        }

        let count = expired_keys.len();

        // 阶段2: 释放读锁，获取写锁批量删除（按 entries → freq_list → hkey_index 顺序避免死锁）
        let mut entries = self.entries.write();
        for key in &expired_keys {
            if let Some(entry) = entries.remove(key.as_ref()) {
                // 清理频率索引
                self.remove_from_freq(key.as_ref());

                // 清理分层索引
                if let Some(hk) = entry.hkey {
                    let mut index = self.hkey_index.write();
                    if let Some(keys) = index.get_mut(&hk) {
                        keys.retain(|k| k.as_ref() != key.as_ref());
                        if keys.is_empty() {
                            index.remove(&hk);
                        }
                    }
                }

                self.expire_count.fetch_add(1, Ordering::Relaxed);
                self.count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        drop(entries);

        // 更新最小频率
        self.min_freq.store(
            self.freq_list.read().keys().min().copied().unwrap_or(1),
            Ordering::Relaxed,
        );

        tracing::info!(
            "后台淘汰: 清理 {} 个过期条目, 剩余 {} 条目",
            count,
            self.count.load(Ordering::Relaxed)
        );

        count
    }

    /// 获取当前内存估算 (bytes)
    pub fn memory_estimate(&self) -> u64 {
        let entries = self.entries.read();
        entries.values().map(|e| e.estimated_size).sum()
    }
}

/// 统计快照
#[derive(Debug, Clone, serde::Serialize)]
pub struct StatsSnapshot {
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
