"""
P2.7: 分片/大消息协议 — MariaDB 文件传输（无文件系统依赖）

文件数据直接存到 MariaDB file_store 表（LONGBLOB）。
元数据走 MQTT 通知，数据体走 DB。

主题结构（MQTT 通知用）:
    v2/agent/{agent_id}/file/{hash}/meta     <- 文件元信息 (retain)
    v2/agent/{agent_id}/file/{hash}/chunk/{seq}  <- 分片内容

用法:
    ft = FileTransferV2("agent-alpha")
    ft.connect()
    ref = ft.upload_file("/path/to/file.bin")
    local_path = ft.download_file(ref["hash"])
    ft.delete_file(ref["hash"])
"""

import os, time, hashlib, logging, tempfile
from typing import Optional, Callable

from Mqtt_bbs_client.client import BBSClient
from Mqtt_bbs_client import config as cfg

log = logging.getLogger("Mqtt_bbs.file_transfer_v2")

V2_FILE_PREFIX = "v2/agent"        # v2/agent/{agent_id}/file/{hash}/...
CHUNK_SIZE = 64 * 1024             # 64KB 分片


class FileTransferV2:
    """P2.7: MariaDB 文件传输（纯 DB 存储，无文件系统依赖）"""

    def __init__(self, agent_id: str, host: str = None, port: int = None):
        self.agent_id = agent_id
        self._client = BBSClient(agent_id, host=host, port=port)
        self._connected = False
        self._db = None

    # ── 连接管理 ──

    def connect(self):
        self._client.connect()
        self._client.wait_connected(5)
        import pymysql
        self._db = pymysql.connect(**cfg.DB_CONFIG)
        self._connected = True

    def disconnect(self):
        if self._db:
            self._db.close()
        self._client.disconnect()
        self._connected = False

    def wait_connected(self, timeout=5):
        return self._client.wait_connected(timeout)

    # ── 文件上传 ──

    def upload_file(self, file_path: str, chunk_size: int = CHUNK_SIZE) -> dict:
        """
        上传文件到 MariaDB（hash 去重）。

        返回: {hash, filename, size, exists}
        """
        filepath = os.path.abspath(file_path)
        if not os.path.exists(filepath):
            return {"error": "file_not_found"}

        with open(filepath, "rb") as f:
            data = f.read()
        file_hash = hashlib.sha256(data).hexdigest()
        filename = os.path.basename(filepath)

        # 检查是否已存在（hash 去重）
        cur = self._db.cursor()
        cur.execute("SELECT hash FROM file_store WHERE hash=%s", (file_hash,))
        if cur.fetchone():
            log.info(f"  [P2.7] hash 已存在，跳过上传: {filename} hash={file_hash[:12]}")
            return {"hash": file_hash, "filename": filename, "size": len(data), "exists": True}

        # 存入 MariaDB
        cur.execute(
            "INSERT INTO file_store (`hash`, filename, data, size, uploader) VALUES (%s, %s, %s, %s, %s)",
            (file_hash, filename, data, len(data), self.agent_id))
        self._db.commit()

        # 发布 MQTT 通知
        meta_topic = f"{V2_FILE_PREFIX}/{self.agent_id}/file/{file_hash}/meta"
        self._client.publish(meta_topic, {
            "filename": filename, "size": len(data), "hash": file_hash,
            "uploader": self.agent_id, "uploaded_at": time.time(),
        }, retain=True, qos=1)

        log.info(f"  [P2.7] 上传到 MariaDB: {filename} ({len(data)}B) hash={file_hash[:12]}")
        return {"hash": file_hash, "filename": filename, "size": len(data), "exists": False}

    # ── 文件下载 ──

    def download_file(self, file_hash: str, save_path: str = None) -> Optional[str]:
        """
        从 MariaDB 下载文件。

        参数:
            file_hash: SHA256 hash
            save_path: 保存路径（默认: 临时目录）

        返回: 保存路径，DB 中不存在返回 None
        """
        cur = self._db.cursor()
        cur.execute("SELECT filename, data FROM file_store WHERE hash=%s", (file_hash,))
        row = cur.fetchone()
        if not row:
            log.warning(f"  [P2.7] 文件未找到: hash={file_hash[:12]}")
            return None

        filename, data = row[0], row[1]
        if not save_path:
            save_path = os.path.join(tempfile.gettempdir(), f"ft_{file_hash[:8]}_{filename}")

        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(data)

        log.info(f"  [P2.7] 从 MariaDB 下载: {filename} ({len(data)}B) → {save_path}")
        return save_path

    # ── 文件删除 ──

    def delete_file(self, file_hash: str):
        """从 MariaDB 删除文件"""
        cur = self._db.cursor()
        cur.execute("DELETE FROM file_store WHERE hash=%s", (file_hash,))
        deleted = cur.rowcount
        self._db.commit()

        # 清理 MQTT retain
        meta_topic = f"{V2_FILE_PREFIX}/{self.agent_id}/file/{file_hash}/meta"
        self._client.publish(meta_topic, "", retain=True)

        if deleted:
            log.info(f"  [P2.7] 从 MariaDB 删除: hash={file_hash[:12]}")
        return deleted > 0

    @property
    def is_connected(self) -> bool:
        return self._connected
