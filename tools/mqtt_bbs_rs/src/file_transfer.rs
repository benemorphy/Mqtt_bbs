/// FileTransfer — MariaDB LONGBLOB 文件存储
///
/// 对标 Python file_transfer_v2.py (P2.7)
/// 文件数据存 MariaDB file_store 表（LONGBLOB），hash 去重。
/// MQTT 通知走标准主题：v2/agent/{agent_id}/file/{hash}/meta
use sqlx::MySqlPool;
use sha2::{Sha256, Digest};
use hex;

pub struct FileTransfer {
    pool: MySqlPool,
}

/// 文件元信息
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub hash: String,
    pub filename: String,
    pub size: i64,
    pub created_at: Option<chrono::NaiveDateTime>,
}

impl FileTransfer {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// 上传数据到 MariaDB（hash 去重）
    pub async fn upload(&self, filename: &str, data: &[u8]) -> Result<FileMeta, sqlx::Error> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hex::encode(hasher.finalize());

        // 检查是否已存在（hash 去重）
        let existing: Option<(String, String, i64, Option<chrono::NaiveDateTime>)> = sqlx::query_as(
            "SELECT hash, filename, size, created_at FROM file_store WHERE hash = ?"
        )
        .bind(&hash)
        .fetch_optional(&self.pool)
        .await?;

        if let Some((h, fname, size, created)) = existing {
            return Ok(FileMeta { hash: h, filename: fname, size, created_at: created });
        }

        // 写入新记录
        let size = data.len() as i64;
        sqlx::query(
            "INSERT INTO file_store (hash, filename, data, size, created_at) VALUES (?, ?, ?, ?, NOW(3))"
        )
        .bind(&hash)
        .bind(filename)
        .bind(data)
        .bind(size)
        .execute(&self.pool)
        .await?;

        Ok(FileMeta { hash, filename: filename.to_string(), size, created_at: None })
    }

    /// 下载文件数据
    pub async fn download(&self, hash: &str) -> Result<Option<(FileMeta, Vec<u8>)>, sqlx::Error> {
        let row: Option<(String, String, i64, Option<chrono::NaiveDateTime>, Vec<u8>)> = sqlx::query_as(
            "SELECT hash, filename, size, created_at, data FROM file_store WHERE hash = ?"
        )
        .bind(hash)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(h, fname, size, created, data)| {
            (FileMeta { hash: h, filename: fname, size, created_at: created }, data)
        }))
    }

    /// 删除文件
    pub async fn delete(&self, hash: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM file_store WHERE hash = ?")
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// 检查文件是否存在
    pub async fn exists(&self, hash: &str) -> Result<bool, sqlx::Error> {
        let row: Option<(String,)> = sqlx::query_as("SELECT hash FROM file_store WHERE hash = ?")
            .bind(hash)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }
}
