// rmqtt_auth_rs — RMQTT HTTP Auth service
// 验证 MQTT 用户名密码 against MariaDB bbs_users
// 端点: POST /mqtt/auth  → {"result": "allow"|"deny"}
//       POST /mqtt/acl   → {"result": "allow"} (临时全放行)

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::thread;

const AUTH_PORT: u16 = 9090;
const DB_HOST: &str = "127.0.0.1:3306";
const DB_USER: &str = "root";
const DB_PASS: &str = "mariadb";
const DB_NAME: &str = "Mqtt_bbs";

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let addr = format!("127.0.0.1:{}", AUTH_PORT);
    let listener = TcpListener::bind(&addr).expect("bind failed");
    tracing::info!("RMQTT Auth Service on {} (MariaDB: {})", addr, DB_HOST);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => { thread::spawn(|| handle(s)); }
            Err(e) => tracing::error!("accept: {}", e),
        }
    }
}

fn handle(mut stream: TcpStream) {
    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return,
    };
    let req = String::from_utf8_lossy(&buf[..n]);

    // Parse request line
    let lines: Vec<&str> = req.lines().collect();
    if lines.is_empty() { return; }
    let parts: Vec<&str> = lines[0].split_whitespace().collect();
    if parts.len() < 2 { return; }
    let method = parts[0];
    let path = parts[1];

    // Parse body (URL-encoded form data)
    let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
    let body = &req[body_start..];
    let params = parse_form(body);

    let response = match (method, path) {
        ("POST", "/mqtt/auth") => handle_auth(&params),
        ("POST", "/mqtt/acl") => r#"{"result":"allow"}"#.to_string(),
        _ => r#"{"result":"deny"}"#.to_string(),
    };

    let http_resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        response.len(), response
    );
    let _ = stream.write_all(http_resp.as_bytes());
}

fn handle_auth(params: &HashMap<String, String>) -> String {
    let username = params.get("username").map(|s| s.as_str()).unwrap_or("");
    let password = params.get("password").map(|s| s.as_str()).unwrap_or("");
    let clientid = params.get("clientid").map(|s| s.as_str()).unwrap_or("");

    if username.is_empty() || password.is_empty() {
        tracing::warn!("Auth: missing credentials (client={})", clientid);
        return r#"{"result":"deny"}"#.to_string();
    }


    // Mode 4: JWT auth — validate JWT in password field
    if password.starts_with("ey") {
        let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "bbs-jwt-secret-key".to_string());
        let token = jsonwebtoken::decode::<serde_json::Value>(
            &password,
            &jsonwebtoken::DecodingKey::from_secret(jwt_secret.as_bytes()),
            &jsonwebtoken::Validation::default()
        );
        match token {
            Ok(data) => {
                let claims = &data.claims;
                if let Some(sub) = claims.get("sub").and_then(|v| v.as_str()) {
                    tracing::info!("Auth OK (JWT): {}", sub);
                    return r#"{"result":"allow"}"#.to_string();
                }
            }
            Err(e) => {
                tracing::warn!("JWT 验证失败: {}", e);
            }
        }
    }
    // Query MariaDB: validate username (name) against password (token)
    // Mode 1: Database auth — username/password match bbs_users
    // Mode 2: Self-auth — username == clientid (trust-on-first-use)
    // Mode 3: Service auth — special "board-service-rs" username
    if username == "board-service-rs" && password.len() >= 8 {
        tracing::info!("Auth OK (service): {}", username);
        return r#"{"result":"allow"}"#.to_string();
    }
    // Self-auth: allow connections where username == clientid
    if username == clientid && !username.is_empty() && password.len() >= 4 {
        tracing::info!("Auth OK (self): {}", username);
        return r#"{"result":"allow"}"#.to_string();
    }

    match query_user(username, password) {
        Ok(true) => {
            tracing::info!("Auth OK: {}", username);
            r#"{"result":"allow"}"#.to_string()
        }
        Ok(false) => {
            tracing::warn!("Auth FAIL: username={}, client={}", username, clientid);
            r#"{"result":"deny"}"#.to_string()
        }
        Err(e) => {
            tracing::error!("DB error: {}", e);
            // deny_if_error=true in config, so return deny
            r#"{"result":"deny"}"#.to_string()
        }
    }
}

fn query_user(username: &str, token: &str) -> Result<bool, String> {
    // Use mysql command-line client to query (avoid heavy deps)
    // Format: mysql -u root -pmariadb -e "SELECT 1 FROM Mqtt_bbs.bbs_users WHERE name='X' AND token='Y'"
    let output = std::process::Command::new("mysql")
        .arg(format!("-u{}", DB_USER))
        .arg(format!("-p{}", DB_PASS))
        .arg("-h127.0.0.1")
        .arg("-P3306")
        .arg("-e")
        .arg(format!(
            "SELECT 1 FROM Mqtt_bbs.bbs_users WHERE name='{}' AND token='{}' LIMIT 1",
            username.replace('\'', "''"),
            token.replace('\'', "''")
        ))
        .output()
        .map_err(|e| format!("mysql exec: {}", e))?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        return Err(format!("mysql error: {}", err));
    }

    // Output contains "1" if found
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.contains('\t') || stdout.contains("1\n"))
}

fn parse_form(body: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
            let val = url_decode(v);
            map.insert(k.to_string(), val);
        }
    }
    map
}

fn url_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}
