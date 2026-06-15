use clap::Parser;

/// Rust BoardService — MQTT 公告板持久化服务
#[derive(Parser, Debug, Clone)]
#[command(name = "board_service_rs", version, about)]
pub struct Config {
    /// MQTT Broker 地址
    #[arg(long, default_value = "127.0.0.1", env = "BROKER_HOST")]
    pub broker_host: String,

    /// MQTT Broker 端口
    #[arg(long, default_value_t = 1883, env = "BROKER_PORT")]
    pub broker_port: u16,

    /// MQTT 用户名
    #[arg(long, default_value = "", env = "MQTT_USERNAME")]
    pub broker_username: String,

    /// MQTT 密码
    #[arg(long, default_value = "", env = "MQTT_PASSWORD")]
    pub broker_password: String,

    /// 数据库连接 URL (mysql://user:pass@host/db)
    #[arg(long, default_value = "mysql://root:mariadb@127.0.0.1/Mqtt_bbs", env = "DATABASE_URL")]
    pub db_url: String,

    /// 数据库连接池大小
    #[arg(long, default_value_t = 8, env = "DB_POOL_SIZE")]
    pub db_pool_size: u32,

    /// Python Plugin Bridge 命令
    #[arg(long, default_value = "", env = "PLUGIN_CMD")]
    pub plugin_cmd: String,

    /// 数据目录
    #[arg(long, default_value = "./data", env = "DATA_DIR")]
    pub data_dir: String,

    /// 主题前缀
    #[arg(long, default_value = "bbs", env = "TOPIC_BBS")]
    pub topic_bbs: String,

    /// 日志级别 (trace/debug/info/warn/error)
    #[arg(long, default_value = "info", env = "LOG_LEVEL")]
    pub log_level: String,

    /// Agent ID
    #[arg(long, default_value = "board-service-rs")]
    pub agent_id: String,

    /// JWT 密钥 (与 Gateway 共享，用于验证用户 JWT)
    #[arg(long, default_value = "bbs-browser-dev-secret-change-in-production", env = "JWT_SECRET")]
    pub jwt_secret: String,

    /// Metrics HTTP 端口 (0=禁用)
    #[arg(long, default_value_t = 9100, env = "METRICS_PORT")]
    pub metrics_port: u16,

    /// 结构化日志 (json/text)
    #[arg(long, default_value = "text", env = "LOG_FORMAT")]
    pub log_format: String,

    /// Heartbeat 超时 (秒)
    #[arg(long, default_value_t = 90)]
    pub heartbeat_timeout: u64,
}
