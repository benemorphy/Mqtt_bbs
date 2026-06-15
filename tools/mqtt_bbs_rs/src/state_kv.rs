/// StateKV — MariaDB KV 存储 + 持久化层
///
/// 对标 Python persistence.py:
/// - state_kv: 通用 KV 存储 (get/set/delete/keys)
/// - retained_messages: Retain 消息持久化 (upsert/recover)
/// - agent_sessions: Agent 在线状态 (online/offline/status)
/// - session_queue: 离线消息队列 (enqueue/replay/delivered)
use sqlx::MySqlPool;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct StateKV {
    pool: MySqlPool,
}

impl StateKV {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    // ── 通用 KV ──

    pub async fn get(&self, key: &str) -> Result<Option<String>, sqlx::Error> {
        let row: Option<(String,)> = sqlx::query_as("SELECT value FROM state_kv WHERE `key` = ?").bind(key).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO state_kv (`key`, `value`, created_at, updated_at) VALUES (?, ?, NOW(3), NOW(3)) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`), updated_at = NOW(3)")
            .bind(key).bind(value).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM state_kv WHERE `key` = ?").bind(key).execute(&self.pool).await?;
        Ok(())
    }

    // ── Retained Messages ──

    pub async fn upsert_retained(&self, topic: &str, payload: &str, qos: i32, source: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO retained_messages (topic, payload, qos, source_agent, created_at, updated_at) \
             VALUES (?, ?, ?, ?, NOW(3), NOW(3)) \
             ON DUPLICATE KEY UPDATE payload=VALUES(payload), qos=VALUES(qos), source_agent=VALUES(source_agent), updated_at=NOW(3)"
        ).bind(topic).bind(payload).bind(qos).bind(source).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn recover_retained(&self, pattern_like: &str) -> Result<Vec<(String, String)>, sqlx::Error> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT topic, payload FROM retained_messages WHERE topic LIKE ? ORDER BY updated_at ASC"
        ).bind(pattern_like).fetch_all(&self.pool).await?;
        Ok(rows)
    }

    // ── Agent Sessions ──

    pub async fn set_agent_online(&self, agent_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO agent_sessions (agent_id, last_online, status, updated_at) \
             VALUES (?, NOW(3), 'online', NOW(3)) \
             ON DUPLICATE KEY UPDATE last_online=NOW(3), status='online', updated_at=NOW(3)"
        ).bind(agent_id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_agent_offline(&self, agent_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO agent_sessions (agent_id, last_offline, status, updated_at) \
             VALUES (?, NOW(3), 'offline', NOW(3)) \
             ON DUPLICATE KEY UPDATE last_offline=NOW(3), status='offline', updated_at=NOW(3)"
        ).bind(agent_id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_agent_status(&self, agent_id: &str) -> Result<Option<String>, sqlx::Error> {
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM agent_sessions WHERE agent_id = ?")
            .bind(agent_id).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| r.0))
    }

    // ── Session Queue ──

    pub async fn enqueue_session(&self, target: &str, topic: &str, payload: &str, qos: i32, seq: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO session_queue (target_agent, topic, payload, qos, seq, created_at) VALUES (?, ?, ?, ?, ?, NOW(3))"
        ).bind(target).bind(topic).bind(payload).bind(qos).bind(seq).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn replay_session_queue(&self, target: &str) -> Result<Vec<(String, String, i32, i64)>, sqlx::Error> {
        let rows: Vec<(String, String, i32, i64)> = sqlx::query_as(
            "SELECT topic, payload, qos, seq FROM session_queue WHERE target_agent = ? AND delivered = 0 ORDER BY seq ASC"
        ).bind(target).fetch_all(&self.pool).await?;
        Ok(rows)
    }

    pub async fn mark_delivered(&self, target: &str, seq: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE session_queue SET delivered = 1, delivered_at = NOW(3) WHERE target_agent = ? AND seq = ?")
            .bind(target).bind(seq).execute(&self.pool).await?;
        Ok(())
    }

    // ── Schema 初始化 ──

    pub async fn ensure_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS state_kv (\
             `key` VARCHAR(255) PRIMARY KEY, `value` TEXT, \
             created_at DATETIME(3), updated_at DATETIME(3))"
        ).execute(&self.pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS retained_messages (\
             topic VARCHAR(512) PRIMARY KEY, payload TEXT, qos INT, source_agent VARCHAR(128), \
             created_at DATETIME(3), updated_at DATETIME(3))"
        ).execute(&self.pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS agent_sessions (\
             agent_id VARCHAR(128) PRIMARY KEY, last_online DATETIME(3), last_offline DATETIME(3), \
             status VARCHAR(16), updated_at DATETIME(3))"
        ).execute(&self.pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS session_queue (\
             id BIGINT AUTO_INCREMENT PRIMARY KEY, target_agent VARCHAR(128), topic VARCHAR(512), \
             payload TEXT, qos INT, seq BIGINT, is_retained BOOLEAN DEFAULT FALSE, \
             delivered BOOLEAN DEFAULT FALSE, created_at DATETIME(3), delivered_at DATETIME(3))"
        ).execute(&self.pool).await?;
        Ok(())
    }
}
