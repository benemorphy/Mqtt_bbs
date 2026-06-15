/// Observability — HTTP Healthcheck + Prometheus Metrics endpoint
///
/// 轻量 HTTP 服务（纯 tokio::net，无框架依赖）:
///   GET /healthz  -> 存活检查 (always 200)
///   GET /readyz   -> 就绪检查 (200 if connected)
///   GET /metrics  -> Prometheus 文本格式（预留）
///

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 就绪状态（MQTT 连接后设为 true）
pub static READY: AtomicBool = AtomicBool::new(false);

/// 启动 Metrics HTTP 服务
pub async fn serve_metrics(port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            tracing::info!("Metrics HTTP 监听: {}", addr);
            l
        }
        Err(e) => {
            tracing::error!("Metrics HTTP 绑定失败: {}", e);
            return;
        }
    };

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            if n == 0 { return; }

            let request = String::from_utf8_lossy(&buf[..n]);
            let path = request.split_whitespace().nth(1).unwrap_or("/");

            let (status_line, content_type, body) = match path {
                "/healthz" => (
                    "200 OK",
                    "text/plain",
                    b"ok" as &[u8],
                ),
                "/readyz" => {
                    if READY.load(Ordering::Relaxed) {
                        ("200 OK", "text/plain", b"ok" as &[u8])
                    } else {
                        ("503 Service Unavailable", "text/plain", b"not ready" as &[u8])
                    }
                }
                "/metrics" => (
                    "200 OK",
                    "text/plain; charset=utf-8",
                    b"# HELP agents_online Online agents\n# TYPE agents_online gauge\nagents_online 0\n" as &[u8],
                ),
                _ => (
                    "404 Not Found",
                    "text/plain",
                    b"not found" as &[u8],
                ),
            };

            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                status_line, content_type, body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(body).await;
        });
    }
}
