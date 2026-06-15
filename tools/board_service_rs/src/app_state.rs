/// 共享应用状态 — 供 lib 和 bin 共用
pub struct AppState {
    pub config: crate::config::Config,
    pub db_pool: crate::db::DbPool,
    pub mqtt_client: rumqttc::AsyncClient,
    pub capabilities: tokio::sync::RwLock<std::collections::HashMap<String, crate::capability::AgentInfo>>,
    pub webhooks: tokio::sync::RwLock<std::collections::HashMap<String, Vec<String>>>,
    pub plugin_ipc: tokio::sync::RwLock<Option<crate::plugin_ipc::PluginIpc>>,
}
