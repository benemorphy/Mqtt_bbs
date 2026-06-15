// mqtt_webui_rs — MQTT Broker 监控面板 (Rust)
// 端口 8900, 监听 RMQTT Broker + MQTT 消息
//
// 端点:
//   GET /               Dashboard (HTML+ECharts)
//   GET /events         SSE 实时推送
//   GET /api/agents     Agent 状态 JSON
//   GET /api/tasks      任务状态 JSON
//   GET /api/broker     Broker 指标 JSON
// 环境变量:
//   BROKER_HOST=127.0.0.1  MQTT Broker 地址
//   BROKER_API=127.0.0.1:6060  RMQTT HTTP API

use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::Serialize;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const PORT: u16 = 8900;
const BROKER: &str = "127.0.0.1";
const RMQTT_API: &str = "http://127.0.0.1:6060";
const MQTT_PORT: u16 = 1883;

#[derive(Clone, Serialize)]
struct AgentInfo {
    id: String,
    status: String,
    last_seen: String,
}

#[derive(Clone, Serialize)]
struct TaskInfo {
    id: String,
    agent: String,
    status: String,
}

#[derive(Clone, Serialize, Default)]
struct BrokerStats {
    connections: u32,
    topics: u32,
    subscriptions: u32,
    routes: u32,
}

struct AppState {
    agents: Vec<AgentInfo>,
    tasks: Vec<TaskInfo>,
    stats: BrokerStats,
    sse_clients: Vec<String>,
}

fn main() {
    let state = Arc::new(Mutex::new(AppState {
        agents: Vec::new(),
        tasks: Vec::new(),
        stats: BrokerStats::default(),
        sse_clients: Vec::new(),
    }));

    // MQTT listener thread
    let mqtt_state = state.clone();
    thread::spawn(move || mqtt_loop(mqtt_state));

    // Broker poller thread
    let poll_state = state.clone();
    thread::spawn(move || broker_poll(poll_state));

    // HTTP server (synchronous, like md_server_rs)
    let addr = format!("0.0.0.0:{}", PORT);
    let listener = TcpListener::bind(&addr).expect("Cannot bind to port");
    println!("[rmqtt] Web UI on http://127.0.0.1:{}", PORT);

    for stream in listener.incoming() {
        if let Ok(s) = stream {
            let sse = state.clone();
            thread::spawn(move || handle(s, sse));
        }
    }
}

fn handle(mut s: TcpStream, state: Arc<Mutex<AppState>>) {
    let mut buf = [0u8; 8192];
    let n = match s.read(&mut buf) { Ok(n) if n > 0 => n, _ => return };
    let req = String::from_utf8_lossy(&buf[..n]);
    let lines: Vec<&str> = req.lines().collect();
    if lines.is_empty() { return; }
    let parts: Vec<&str> = lines[0].split_whitespace().collect();
    if parts.len() < 2 { return; }
    let path = parts[1];

    match path {
        "/events" => handle_sse(&mut s, state),
        "/api/agents" => send_json(&mut s, &serde_json::to_string(&state.lock().unwrap().agents).unwrap_or_default()),
        "/api/tasks" => send_json(&mut s, &serde_json::to_string(&state.lock().unwrap().tasks).unwrap_or_default()),
        "/api/broker" => send_json(&mut s, &serde_json::to_string(&state.lock().unwrap().stats).unwrap_or_default()),
        _ => {
            if path.starts_with("/api/") {
                send_json(&mut s, "{\"error\":\"not found\"}");
            } else {
                send_html(&mut s);
            }
        }
    }
}

fn handle_sse(s: &mut TcpStream, _state: Arc<Mutex<AppState>>) {
    let resp = "HTTP/1.0 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nAccess-Control-Allow-Origin: *\r\n\r\n";
    let _ = s.write_all(resp.as_bytes());
    let _ = s.flush();

    // Register client
    // Keep connection alive with periodic heartbeat
    let mut last = Instant::now();
    loop {
        thread::sleep(Duration::from_secs(1));
        if last.elapsed() > Duration::from_secs(15) {
            let msg = "event: heartbeat\ndata: {}\n\n";
            if s.write_all(msg.as_bytes()).is_err() { break; }
            let _ = s.flush();
            last = Instant::now();
        }
    }

}

fn send_json(s: &mut TcpStream, data: &str) {
    let h = format!("HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}", data.len(), data);
    let _ = s.write_all(h.as_bytes());
}

fn send_html(s: &mut TcpStream) {
    let nav = r##"<nav id="sidebar">
<h3>RMQTT 监控</h3>
<a href="/" class="active">Dashboard</a>
<a href="#">Agent 状态</a>
<a href="#">任务队列</a>
<a href="#">Broker 指标</a>
</nav>"##;

    let html = format!(r##"<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>RMQTT Web UI</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/echarts/5.4.3/echarts.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box;font-family:system-ui,sans-serif}}
body{{display:flex;height:100vh;overflow:hidden;background:#1a1a2e;color:#e0e0e0}}
#sidebar{{width:240px;min-width:240px;background:#16213e;padding:20px;overflow-y:auto}}
#sidebar h3{{font-size:16px;color:#e94560;margin-bottom:15px;padding-bottom:8px;border-bottom:1px solid #333}}
#sidebar a{{display:block;padding:8px 12px;color:#a8b2d1;text-decoration:none;border-radius:4px;margin:2px 0}}
#sidebar a:hover,#sidebar a.active{{background:#1a1a2e;color:#e94560}}
#main{{flex:1;padding:24px;overflow-y:auto;background:#0f3460}}
.card{{background:#16213e;border-radius:8px;padding:16px;margin-bottom:16px}}
.card h2{{font-size:18px;color:#e94560;margin-bottom:10px}}
table{{width:100%;border-collapse:collapse;margin:8px 0}}
th,td{{border:1px solid #333;padding:8px 12px;text-align:left}}
th{{background:#1a1a2e;color:#fff}}
tr:nth-child(even){{background:#1a1a2e}}
#charts{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
.chart-box{{background:#16213e;border-radius:8px;padding:16px;height:250px}}
</style>
</head><body>
{}
<main id="main">
<div class="card"><h2>Broker 状态</h2>
<div id="stat-bar" style="display:flex;gap:20px;flex-wrap:wrap">
<div style="text-align:center;padding:10px;background:#1a1a2e;border-radius:6px;flex:1"><div style="font-size:28px;color:#e94560" id="conn-count">-</div><div style="font-size:12px;color:#888">连接数</div></div>
<div style="text-align:center;padding:10px;background:#1a1a2e;border-radius:6px;flex:1"><div style="font-size:28px;color:#e94560" id="topic-count">-</div><div style="font-size:12px;color:#888">主题数</div></div>
<div style="text-align:center;padding:10px;background:#1a1a2e;border-radius:6px;flex:1"><div style="font-size:28px;color:#e94560" id="sub-count">-</div><div style="font-size:12px;color:#888">订阅数</div></div>
</div></div>
<div id="charts"><div class="chart-box" id="chart-conn"></div><div class="chart-box" id="chart-topics"></div></div>
<div class="card"><h2>Agent 列表</h2><div id="agent-list">(等待数据...)</div></div>
<div class="card"><h2>任务列表</h2><div id="task-list">(等待数据...)</div></div>
</main>
<script>
var evt = new EventSource("/events");
evt.onmessage = function(e){{}};
setInterval(function(){{
fetch("/api/broker").then(r=>r.json()).then(d=>{{
document.getElementById("conn-count").textContent=d.connections||"-";
document.getElementById("topic-count").textContent=d.topics||"-";
document.getElementById("sub-count").textContent=d.subscriptions||"-";
}});
fetch("/api/agents").then(r=>r.json()).then(d=>{{
var h="<table><tr><th>Agent</th><th>状态</th><th>最后可见</th></tr>";
d.forEach(function(a){{h+="<tr><td>"+a.id+"</td><td>"+a.status+"</td><td>"+a.last_seen+"</td></tr>";}});
document.getElementById("agent-list").innerHTML=h+"</table>";
}});
fetch("/api/tasks").then(r=>r.json()).then(d=>{{
var h="<table><tr><th>ID</th><th>Agent</th><th>状态</th></tr>";
d.forEach(function(t){{h+="<tr><td>"+t.id+"</td><td>"+t.agent+"</td><td>"+t.status+"</td></tr>";}});
document.getElementById("task-list").innerHTML=h+"</table>";
}});
}},3000);
</script>
</body></html>"##, nav);
    let h = format!("HTTP/1.0 200 OK\r\nContent-Type:text/html;charset=utf-8\r\nContent-Length:{}\r\n\r\n", html.len());
    let _ = s.write_all(h.as_bytes());
    let _ = s.write_all(html.as_bytes());
}

fn mqtt_loop(state: Arc<Mutex<AppState>>) {
    use tokio::runtime::Builder;
    let rt = match Builder::new_current_thread().enable_io().enable_time().build() {
        Ok(r) => r,
        Err(e) => { eprintln!("[rmqtt] Cannot create tokio runtime: {}", e); return; }
    };
    rt.block_on(async {
        let mut opts = MqttOptions::new("mqtt_webui_rs", BROKER, MQTT_PORT);
        opts.set_keep_alive(Duration::from_secs(30));
        // MQTT auth from env vars
        if let Ok(u) = std::env::var("MQTT_USERNAME") {
            let p = std::env::var("MQTT_PASSWORD").unwrap_or_default();
            opts.set_credentials(&u, &p);
        }
        let (client, mut eventloop) = AsyncClient::new(opts, 100);

        let _ = client.subscribe("node/#", QoS::AtMostOnce).await;
        // Also subscribe to Mosquitto $SYS topics for broker stats
        let _ = client.subscribe("$SYS/broker/#", QoS::AtMostOnce).await;

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = p.topic.clone();
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    let parts: Vec<&str> = topic.split('/').collect();

                    // Agent status updates - actual topic: node/{id}/status
                    if parts.len() >= 3 && parts[0] == "node" && parts[2] == "status" {
                        let mut st = state.lock().unwrap();
                        let id = parts[1].to_string();
                        if let Some(a) = st.agents.iter_mut().find(|a| a.id == id) {
                            a.status = payload.clone();
                            a.last_seen = format!("{}s", Instant::now().elapsed().as_secs());
                        } else {
                            st.agents.push(AgentInfo { id, status: payload.clone(), last_seen: "now".to_string() });
                        }
                    }
                    // Agent capability declaration - also register agent
                    if parts.len() >= 3 && parts[0] == "node" && parts[2] == "capability" {
                        let mut st = state.lock().unwrap();
                        let id = parts[1].to_string();
                        if !st.agents.iter().any(|a| a.id == id) {
                            st.agents.push(AgentInfo { id: id.clone(), status: "online".to_string(), last_seen: "now".to_string() });
                        }
                    }
                    // Task status updates - actual topic: board/task/{id}/status or board/task/{id}/output
                    if parts.len() >= 4 && parts[0] == "board" && parts[1] == "task" && parts[3] == "status" {
                        let mut st = state.lock().unwrap();
                        st.tasks.push(TaskInfo { id: parts[2].to_string(), agent: parts.get(4).unwrap_or(&"?").to_string(), status: payload.clone() });
                        if st.tasks.len() > 100 { st.tasks.remove(0); }
                    }
                    // Mosquitto $SYS broker stats
                    if topic.starts_with("$SYS/broker/") {
                        let mut st = state.lock().unwrap();
                        match topic.as_str() {
                            "$SYS/broker/clients/connected" => {
                                st.stats.connections = payload.trim().parse().unwrap_or(0);
                            }
                            "$SYS/broker/clients/total" => {
                                // kept for info but connections is what we display
                            }
                            "$SYS/broker/subscriptions/count" => {
                                st.stats.subscriptions = payload.trim().parse().unwrap_or(0);
                            }
                            _ => {}
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[rmqtt] MQTT error: {:?}, reconnecting in 3s...", e);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    });
}

fn broker_poll(state: Arc<Mutex<AppState>>) {
    // Mosquitto 没有 HTTP API，broker 统计通过 $SYS MQTT 主题获取
    // 此线程仅保留占位，stats 由 mqtt_loop 中的 $SYS 订阅更新
    loop {
        thread::sleep(Duration::from_secs(30));
        // 定期清理过期的 agents（30s 无更新视为离线）
        let mut st = state.lock().unwrap();
        st.agents.retain(|a| {
            let secs = a.last_seen.trim_end_matches('s').parse::<u64>().unwrap_or(0);
            secs < 60
        });
    }
}
