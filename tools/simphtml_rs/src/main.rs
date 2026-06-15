// simphtml_rs — HTML 简化引擎 (CLI + HTTP 服务)
// 端点: POST /          优化+截断
//       POST /cutlist   列表检测+标记+优化+截断
//       GET  /health    健康检查

use regex::Regex;
use scraper::{Html, Selector};
use serde::Deserialize;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut max_chars = 35_000usize;
    let mut serve = false;
    let mut port = 8901u16;
    let mut from_file: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--serve" => serve = true,
            "--port" if i + 1 < args.len() => { i += 1; port = args[i].parse().unwrap_or(8901); }
            "--max-chars" if i + 1 < args.len() => { i += 1; max_chars = args[i].parse().unwrap_or(35000); }
            "--file" if i + 1 < args.len() => { i += 1; from_file = Some(args[i].clone()); }
            "--help" | "-h" => { eprintln!("Usage: simphtml_rs [--serve] [--port N] [--max-chars N] [--file F]"); return; }
            _ => {}
        }
        i += 1;
    }
    if serve {
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr).unwrap();
        eprintln!("[simphtml] listening on {}", addr);
        for stream in listener.incoming() {
            if let Ok(s) = stream { thread::spawn(move || handle(s, max_chars)); }
        }
    } else {
        let mut buf = String::new();
        if let Some(p) = from_file { buf = std::fs::read_to_string(&p).unwrap_or_default(); }
        else { std::io::stdin().read_to_string(&mut buf).unwrap_or(0); }
        print!("{}", process_html(&buf, max_chars));
    }
}

fn handle(mut stream: TcpStream, dmax: usize) {
    let mut raw = [0u8; 16384];
    let n = match stream.read(&mut raw) { Ok(n) if n > 0 => n, _ => return };
    let req = String::from_utf8_lossy(&raw[..n]);
    let first = req.lines().next().unwrap_or("");
    let parts: Vec<&str> = first.split_whitespace().collect();
    if parts.len() < 2 { return; }
    let path = parts[1];
    if parts[0] == "GET" && path == "/health" {
        let _ = stream.write_all(b"HTTP/1.0 200 OK\r\nContent-Length:10\r\n\r\nsimphtml_rs");
        return;
    }
    if parts[0] != "POST" && parts[0] != "GET" { let _ = stream.write_all(b"HTTP/1.0 405\r\n\r\n"); return; }
    // GET / 返回状态信息（替代405）
    if parts[0] == "GET" {
        let info = "simphtml_rs: use POST with HTML body, or GET ?html=...";
        let resp = format!("HTTP/1.0 200 OK\r\nContent-Type:text/plain;charset=utf-8\r\nAccess-Control-Allow-Origin:*\r\nContent-Length:{}\r\n\r\n{}", info.len(), info);
        let _ = stream.write_all(resp.as_bytes());
        return;
    }
    let mc: usize = if path.contains("max_chars=") {
        path.split("max_chars=").nth(1).and_then(|s| s.split('&').next()).and_then(|s| s.parse().ok()).unwrap_or(dmax)
    } else { dmax };
    let mut body = String::new();
    let mut in_body = false;
    for line in req.lines() {
        if in_body { body.push_str(line); body.push('\n'); }
        if line.trim().is_empty() { in_body = true; }
    }
    let result = if path.starts_with("/cutlist") {
        let mut h = String::new(); let mut sels: Vec<SelReq> = Vec::new();
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            if let Some(html) = v.get("html").and_then(|x| x.as_str()) { h = html.to_string(); }
            if let Some(arr) = v.get("selectors").and_then(|x| x.as_array()) {
                for item in arr {
                    if let Some(sel) = item.get("selector").and_then(|x| x.as_str()) {
                        sels.push(SelReq { selector: sel.to_string(), keep: item.get("keep").and_then(|x| x.as_u64()).map(|x| x as usize) });
                    }
                }
            }
        }
        let opt = simple_opt(&h);
        let hints = detect_cutlist_hints(&opt, &sels);
        let mut out = opt;
        for hint in hints { out.push_str(&hint); }
        smart_truncate(&out, mc)
    } else {
        process_html(&body, mc)
    };
    let resp = format!("HTTP/1.0 200 OK\r\nContent-Type:text/plain;charset=utf-8\r\nAccess-Control-Allow-Origin:*\r\nContent-Length:{}\r\n\r\n{}", result.len(), result);
    let _ = stream.write_all(resp.as_bytes());
}

fn process_html(html: &str, max_chars: usize) -> String {
    smart_truncate(&simple_opt(html), max_chars)
}

#[derive(Deserialize)]
struct SelReq { selector: String, keep: Option<usize> }

fn detect_cutlist_hints(html: &str, selectors: &[SelReq]) -> Vec<String> {
    let mut hints = Vec::new();
    for se in selectors {
        let sel = match Selector::parse(&se.selector) { Ok(s) => s, Err(_) => continue };
        let k = se.keep.unwrap_or(3).max(1);
        let dom = Html::parse_fragment(html);
        let els: Vec<_> = dom.select(&sel).collect();
        if els.len() <= k + 2 { continue; }
        let texts: Vec<String> = els.iter().map(|e| e.text().collect::<String>()).collect();
        let total: usize = texts.iter().map(|t| t.len()).sum();
        if total / els.len() < 20 && total < 500 { continue; }
        hints.push(format!("\n<!-- FAKE ELEMENT: {} more items hidden, selector: {} -->\n", els.len() - k, se.selector));
    }
    hints
}

fn simple_opt(html: &str) -> String {
    let re1 = Regex::new(r#"\s+[a-zA-Z_-]+=(?:""|''|"[\s]*"|'[\s]*')"#).unwrap();
    let re2 = Regex::new(r">\s+<").unwrap();
    re2.replace_all(&re1.replace_all(html, ""), "><").to_string()
}

fn count_text(html: &str) -> usize {
    let no_tags = Regex::new(r"<[^>]*>").unwrap().replace_all(html, " ");
    Regex::new(r"\s+").unwrap().replace_all(no_tags.trim(), " ").len()
}

fn smart_truncate(html: &str, max: usize) -> String {
    if count_text(html) <= max { return html.to_string(); }
    let mut r = String::new(); let mut c = 0; let mut tag = false;
    for ch in html.chars() {
        if c >= max && !tag { break; }
        if ch == '<' { tag = true; r.push(ch); continue; }
        if ch == '>' { tag = false; r.push(ch); continue; }
        if tag { r.push(ch); } else { c += 1; r.push(ch); }
    }
    format!("{}<!-- TRUNCATED at {} chars -->", r.trim_end(), max)
}
