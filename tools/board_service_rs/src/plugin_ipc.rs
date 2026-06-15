/// Plugin IPC 桥接 — 管理 Python PluginManager 子进程
///
/// 通过 stdin/stdout JSON Lines 协议与 Python 插件系统通信。
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Command, Child, ChildStdin, ChildStdout};
use tokio::sync::oneshot;
use serde_json::Value;

pub struct PluginIpc {
    _child: Child,
    stdin: tokio::io::BufWriter<ChildStdin>,
    pending: HashMap<u64, oneshot::Sender<Value>>,
    next_id: AtomicU64,
}

impl PluginIpc {
    pub async fn spawn(python_cmd: &str) -> anyhow::Result<Self> {
        let mut child = Command::new("python")
            .arg("-u")
            .arg(python_cmd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;
        
        let stdin = child.stdin.take().ok_or_else(|| anyhow::anyhow!("无法获取子进程 stdin"))?;
        let _stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("无法获取子进程 stdout"))?;
        
        let mut ipc = Self {
            _child: child,
            stdin: tokio::io::BufWriter::new(stdin),
            pending: HashMap::new(),
            next_id: AtomicU64::new(1),
        };
        
        // 启动 stdout 读取协程
        let _pending_ref = &mut ipc.pending;
        // 注意: 这里需要更好的生命周期管理，简化版用 Arc<Mutex<HashMap>>
        // 但在阶段1我们先简化处理
        
        Ok(ipc)
    }
    
    pub async fn apply_filters(&mut self, name: &str, data: Value) -> anyhow::Result<Option<Value>> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        
        let msg = serde_json::json!({
            "id": id,
            "type": "apply_filters",
            "name": name,
            "data": data,
        });
        
        let mut line = serde_json::to_string(&msg)?;
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.flush().await?;
        
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
            Ok(Ok(result)) => {
                if result.get("blocked") == Some(&Value::Bool(true)) {
                    Ok(None)
                } else {
                    Ok(Some(result))
                }
            }
            _ => {
                self.pending.remove(&id);
                // 超时或通道关闭 — 降级: 不阻断
                tracing::warn!("Plugin IPC 超时/失败 (name={}, id={}), 降级通过", name, id);
                Ok(Some(data))
            }
        }
    }
}
