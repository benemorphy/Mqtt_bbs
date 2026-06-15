"""DB 迁移系统 — 自动执行 Schema 迁移"""

import logging
log = logging.getLogger("Mqtt_bbs.migrations")

MIGRATIONS = {
    1: """
        CREATE TABLE IF NOT EXISTS bbs_users (
            token VARCHAR(32) PRIMARY KEY, name VARCHAR(128) NOT NULL UNIQUE,
            board VARCHAR(128) NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS bbs_posts (
            id BIGINT AUTO_INCREMENT PRIMARY KEY, board VARCHAR(128) NOT NULL,
            author VARCHAR(64) NOT NULL, content LONGTEXT,
            created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
            KEY idx_board (board), KEY idx_author (author)
        );
    """,
    2: "ALTER TABLE bbs_posts ADD COLUMN edited_at DATETIME NULL",
    3: """
        CREATE TABLE IF NOT EXISTS bbs_webhooks (
            id INT AUTO_INCREMENT PRIMARY KEY, board VARCHAR(128) NOT NULL,
            url VARCHAR(512) NOT NULL, events VARCHAR(256) DEFAULT 'post',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """,
    4: """
        CREATE TABLE IF NOT EXISTS web_users (
            id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(64) NOT NULL UNIQUE,
            password_hash VARCHAR(128) NOT NULL, display_name VARCHAR(128) NOT NULL DEFAULT '',
            role VARCHAR(16) DEFAULT 'viewer', created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_login DATETIME NULL
        );
    """,
}

def run_migrations(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS _schema_version (version INT PRIMARY KEY, applied_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
    cursor.execute("SELECT MAX(version) FROM _schema_version")
    row = cursor.fetchone()
    current = row[0] if row and row[0] else 0
    executed = 0
    for ver, sql in sorted(MIGRATIONS.items()):
        if ver > current:
            log.info(f"  [Migration] v{ver}")
            for stmt in sql.strip().split(";"):
                s = stmt.strip()
                if s:
                    cursor.execute(s)
            cursor.execute("INSERT INTO _schema_version (version) VALUES (%s)", (ver,))
            executed += 1
    log.info(f"  Migration: {executed} 个迁移 v{current}->{max(MIGRATIONS.keys())}")
    return executed
