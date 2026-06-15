# MQTT BBS 安全与架构审计报告

> 审计日期: 2026-06-10
> 审计范围: Mqtt_bbs_server/ + GA/Mqtt_bbs_client/ + docker/ + GA/memory/ 相关SOP
> 审计方法: 源码静态分析 + 配置审查 + 运行态SOP交叉验证

---

## 风险矩阵

| 等级 | 数量 | 定义 |
|:----|:----|:------|
| CRITICAL | 2 | 立即修复，可导致数据泄露/远程代码执行 |
| HIGH | 6 | 本周内修复，可被外部利用 |
| MEDIUM | 7 | 1-2周内修复，有限条件下可被利用 |
| LOW | 4 | 架构建议，长期优化 |

---

## [CRITICAL] 发现

### C1. Broker 无 TLS 加密

**位置**: `docker/mosquitto.conf` L1 (`listener 1883`)
**问题**: MQTT 1883 端口为明文 TCP，无 TLS 证书配置。

```
listener 1883          # ← 明文，无 TLS
allow_anonymous false
password_file /mosquitto/config/mosquitto_passwd
```

**影响**: 所有 MQTT 流量（含密码、JWT Token、帖子内容、文件数据）明文传输，局域网内可被中间人窃听。

**建议**: 增加 TLS 端口监听:
```
listener 8883
certfile /mosquitto/certs/server.crt
keyfile /mosquitto/certs/server.key
require_certificate false

listener 1883          # 仅内网使用或移除此行
```

---

### C2. `post_fast()` 完全绕过令牌验证

**位置**: `Mqtt_bbs_server/persistence.py` L175-L196
**问题**: `post_fast()` 直接写 MariaDB + 广播 MQTT，完全不验证 token 合法性。

```python
def post_fast(self, content: str, token: str, board: str = None) -> dict:
    # token 仅取前8字符作为显示作者名，不做任何验证
    post_data = {
        "author": token[:8],  # ← token 被当昵称用，不是验证
        ...
    }
    # 直接写DB
    self._db.execute("INSERT INTO posts ...", (post_id, board, token[:8], content, now))
    # 直接MQTT广播
    self.publish(f"bbs/{board}/new_post", post_data, ...)
```

**影响**: 任何知道 Broker 密码的客户端可以绕过 BoardService 的 JWT 验证机制直接发帖。

**建议**:
1. **立即**: 在 `post_fast()` 中增加 JWT 解码验证（解码 payload 验证 board 权限）
2. **中期**: 将 `post_fast` 降级为内部接口，对外统一走 BoardService

---

## [HIGH] 发现

### H1. 认证凭据硬编码 + 明文件存储

**位置**: `Mqtt_bbs_server/agent.env` (全文件)

```
MARIADB_PASSWORD=mariadb
MQTT_USERNAME=board-service-rs
MQTT_PASSWORD=board-service-rs        # ← 用户名=密码
DASHBOARD_PASSWORD=eyJhbGciOi...      # ← JWT Token 明文硬编码
```

**问题**:
- MQTT 密码与用户名相同（`board-service-rs` / `board-service-rs`）
- MariaDB root 密码为 `mariadb`（docker-compose 默认值）
- JWT Token 硬编码在环境文件中
- `agent.env` 未出现在 `.gitignore` 中？

**建议**:
1. 使用密码生成器生成 32 位随机密码
2. `agent.env` 加入 `.gitignore`，仅通过密钥管理系统传递
3. docker-compose 中的 `MYSQL_ROOT_PASSWORD` 移除默认值

---

### H2. JWT Secret 默认值 + 7 天过期

**位置**: `GA/Mqtt_bbs_client/config.py` L26

```python
HMAC_SECRET = _os.environ.get("MQTT_HMAC_SECRET", "Mqtt_bbs_hmac_secret_2026")
```

`board_handlers.py` L86-L91:

```python
token = _jwt.encode({
    "sub": agent_id, "name": name, "role": "worker",
    "board": board_key,
    "exp": int(time.time()) + 86400 * 7,   # ← 7天有效期
    "iat": int(time.time()),
}, _jwt_secret, algorithm="HS256")
```

**问题**:
- `HMAC_SECRET` 默认值 `"Mqtt_bbs_hmac_secret_2026"` 是公开可猜测的字符串
- 7天 Token 有效期过长
- 无 Token 吊销机制（只能等过期）
- `JWT_SECRET` 与 `MQTT_HMAC_SECRET` 两套密钥体系混用

**建议**:
1. 删除默认 HMAC_SECRET，环境变量未设置时报错退出
2. Token 有效期缩短至 24 小时
3. 增加 token_revoke 主题支持即时吊销
4. 统一 JWT_SECRET 和 MQTT_HMAC_SECRET 为一套密钥

---

### H3. 注册无客户端身份认证

**位置**: `board_handlers.py` L66-L106 `on_register()`

```python
def on_register(self, topic: str, payload):
    agent_id = payload.get("agent_id", "")
    name = payload.get("name", "")
    # ← 没有校验 payload 中的 agent_id 是否与 MQTT client identity 一致
    # ← 没有校验该 MQTT 客户端是否有权限注册到该 board
```

**影响**: 任何能连接 Broker 的客户端可以:
- 注册到任意 Board
- 冒充任意 agent_id
- 获取 JWT Token（凭此发帖、查询）

**建议**:
1. 注册请求中要求签名（用 MQTT password 作为 HMAC key 签名 agent_id）
2. 或利用 MQTTv5 的 User Properties 传递额外认证信息
3. BoardService 校验 `topic` 中的 board_key 是否与客户端权限匹配

---

### H4. 插件系统任意代码执行

**位置**: `Mqtt_bbs_server/plugin_manager.py`

```python
def discover_and_load(self) -> list[str]:
    # 扫描并加载 plugins/ 目录所有 .py 文件
    # 无签名验证、无沙箱、无hash校验
```

**影响**: 任何能写入 `plugins/` 目录的攻击者可以植入恶意插件，获得完整系统权限。

**建议**:
1. 插件文件增加 SHA256 签名清单 (`plugins/MANIFEST.json`)
2. 使用 `importlib` 的 `Loader` 限制模块权限
3. 生产环境禁用动态插件加载，改为编译期注册

---

### H5. 文件下载无大小限制

**位置**: `board_handlers.py` L437-L467 `on_file_download()`

```python
if os.path.exists(filepath) and os.path.isfile(filepath):
    with open(filepath, "rb") as dlf:
        file_bytes = dlf.read()     # ← 整个文件读入内存
    data_b64 = base64.b64encode(file_bytes).decode()  # ← base64 膨胀33%
```

**影响**: 上传一个大文件（如 1GB）后请求下载，BoardService 内存耗尽 OOM。

**建议**:
1. 设置文件大小上限（如 100MB），在上传时检查
2. 文件下载改为分片流式响应（利用已有的分片协议）
3. 添加配置项 `MAX_FILE_SIZE`

---

### H6. Webhook URL 未校验

**位置**: `board_handlers.py` L240-L250 `on_webhook_config()`

```python
def on_webhook_config(self, topic: str, payload):
    action = payload.get("action", "set")
    url = payload.get("url", "")    # ← 无校验
    # 直接存入 self._webhooks[board_key] 并订阅后调用 webhook_send()
```

**影响**: 恶意客户端可以将 webhook 指向:
- 内网敏感服务（SSRF）
- 外部数据收集服务器（数据泄露）
- `file:///etc/passwd`（某些 OS）

**建议**:
1. URL 白名单：只允许配置的域
2. 禁止内网 IP 段（127.0.0.1, 10.x, 172.x, 192.168.x）
3. 限制协议只允许 HTTPS
4. webhook 发送增加超时和连接数限制

---

### H7. Topic 前缀不一致导致静默失效

**位置**:
- `Mqtt_bbs_server/board_config.py` L30: `TOPIC_BBS = "agent/bbs"`
- `GA/Mqtt_bbs_client/board_client.py` L35: `TOPIC_BBS = "bbs"`  (无 `agent/` 前缀)

**问题**: Python BoardService 的业务主题与 BoardClient 的业务主题定义不同。BoardClient 发往 `bbs/{board}/post`，但 BoardService 监听 `agent/bbs/{board}/post`。BBSClient.publish() 还会自动叠加 `config.TOPIC_PREFIX`（默认 `"agent/"`），导致实际发出的主题为 `agent/bbs/{board}/post`——与 BoardService 一致——但 BoardClient 代码中的 `_base` 拼接逻辑造成的三层前缀（`agent/bbs/bbs/{board}/...`）风险一直被忽视。

**影响**: 这是一个已知（见 board_service_diag_sop L103）但未修复的架构缺陷，导致新增客户端集成时必踩坑。

**建议**:
1. 统一 `TOPIC_BBS` 常量到 `MQTT_BBS_TOPIC` 单一来源
2. BoardClient 去掉手动 `TOPIC_BBS` 定义，统一从 config 读取
3. 增加集成测试覆盖 topic 路径正确性

---

## [MEDIUM] 发现

### M1. 无速率限制

**位置**: 所有 `on_*` handler
**问题**: 注册/发帖/查询/file_init 等操作均无频率限制
**影响**: 恶意客户端可洪水攻击，导致 MariaDB 写入瓶颈或 BoardService 资源耗尽
**建议**: 添加 per-agent-id 滑动窗口速率限制

### M2. SQL 注入风险（query_type 参数）

**位置**: `board_handlers.py` L170-L236
**问题**: `query_type` 为"poll"时 `since_id` 参数直接 `int()` 转换，但若 `query_type` 来自 payload 且 DB 在不同 handler 间复用，拼接逻辑存在风险
**影响**: 有限条件下可改变 SQL 语义
**建议**: 使用白名单校验 `query_type`，对所有数值参数二次验证

### M3. 注册/用户数据泄露

**位置**: `board_handlers.py` L217-L223 Users 查询
```python
elif query_type == "users":
    rows = db.execute("SELECT name,token,board,created_at FROM bbs_users WHERE board=%s", (board_key,)).fetchall()
```
**问题**: 查询用户列表返回所有 `token` 字段，任何可发 query 的客户端可获取所有用户的 JWT Token
**建议**: 查询用户列表时返回 `token` 的后 8 字符或 token_hash，不返回完整 token

### M4. LWT/Retain 消息泄露

**位置**: `client.py` L301
```python
self.publish(f"node/{self.agent_id}/status", "online", retain=True)
```
**问题**: Agent 上线状态用 Retain 消息，新订阅者可立即获取所有 Agent 信息
**建议**: Retain 消息中不包含敏感信息；下线时发空 Retain 清除

### M5. 事件广播未鉴权

**位置**: `board_handlers.py` L155-L156
```python
self._client.publish(f"{TOPIC_BBS}/{board_key}/new_post", broadcast, retain=False, qos=0)
```
**问题**: `new_post` 广播所有订阅者可见，Board 隔离仅依赖 Topic 路径名义隔离
**建议**: 敏感 Board 启用消息级加密；广播回调由客户端 token 鉴权

### M6. 空闲超时下线设计缺陷

**位置**: `board_service_diag_sop.md` L70-L75
**问题**: Rust BoardService 120s 无消息自动退出进程，被标记为"设计行为"。但这个行为在生产中等于"服务随机自尽"——只要某段时间没有 Board 活动，服务就自杀
**建议**: 移除空闲超时退出，改为仅日志告警；启动 heartbeat keep-alive

### M7. MariaDB 默认弱密码

**位置**: `docker-compose.yml` L19 + `config.py` L15
```yaml
MYSQL_ROOT_PASSWORD: ${DB_PASSWORD:-mariadb}
```
```python
"password": _os.environ.get("DB_PASSWORD") or _os.environ.get("mariadb_password") or "",
```
**问题**: 三层回退值: 环境变量 → `mariadb_password` → 空字符串（匿名连接）
**建议**: 不允许空密码连接，docker-compose 中移除默认值

---

## [LOW] 架构建议

### L1. 无集中式健康检查端点
- Python BoardService 无 `/healthz`/`/readyz`
- 对比 Rust 版有健康端点但未集成到系统监控
- 建议: 增加 MQTT 主题 `agent/bbs/health` 响应式健康检查

### L2. 无集群 / 高可用
- BoardService 单实例运行
- 无故障转移机制
- 建议: 后续考虑 active-standby 模式

### L3. test_auth_flow.py 测试内容过浅
- 测试注册但未测试过期 token、恶意 payload、SQL 注入
- 建议: 增加负面测试用例

### L4. 无 JWT Token 吊销机制
- Token 签发后无法主动失效
- 7 天有效期内泄露则永久受损
- 建议: 增加 `agent/bbs/{board}/token_revoke` 主题 + Token 黑名单机制

---

## 优化建议总结

### 立即修复（P0）

| # | 问题 | 文件 | 工作量 |
|:--|:-----|:-----|:-------|
| C1 | Broker 无 TLS | docker/mosquitto.conf | ~2h |
| C2 | post_fast 绕过 JWT | persistence.py | ~1h |
| H1 | 硬编码凭据 | agent.env + docker-compose.yml | ~0.5h |
| H2 | JWT Secret 默认值 | config.py + handlers.py | ~1h |
| H3 | 注册无身份认证 | board_handlers.py | ~2h |

### 本周修复（P1）

| # | 问题 | 工作量 |
|:--|:-----|:-------|
| H4 | 插件任意代码执行 | ~3h |
| H5 | 文件无大小限制 | ~1h |
| H6 | Webhook URL 未校验 | ~1h |
| H7 | Topic 前缀不一致 | ~1h |
| M3 | 注册/用户数据泄露 | ~0.5h |

### 架构提升（P2）

| # | 问题 | 工作量 |
|:--|:-----|:-------|
| M1 | 无速率限制 | ~4h |
| M7 | MariaDB 弱密码 | ~0.5h |
| L4 | 无 Token 吊销 | ~2h |
| L1 | 无健康检查端点 | ~1h |

---

## 总体评估

**安全等级: 中危** — 当前配置适合内网研发环境，不推荐直接暴露于外网。

核心风险集中在三点:
1. **认证太弱**: 凭据硬编码 + 用户名=密码 + JWT 默认 Secret
2. **控制面无鉴权**: 注册、发帖（post_fast）无有效的身份验证
3. **明文传输**: 无 TLS 加密

当前系统在信任的内网环境中尚可工作，但如果需要在公网或跨网络部署，上述 C1-C2 + H1-H3 必须优先修复。

---

*本报告由物理级全能执行者于 2026-06-10 自动生成*
