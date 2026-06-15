# Mqtt_bbs 插件系统设计方案

## 设计哲学

MQTT 本身就是事件总线，不重造 pub/sub 轮子。插件系统只做三件事：
1. **发现与加载** — 扫描目录、注册插件
2. **异常隔离** — 单个插件崩溃不扩散
3. **生命周期管理** — 启动/停止/热加载

---

## 一、插件定义

### 最小化接口

```python
# Mqtt_bbs/plugin.py

from abc import ABC, abstractmethod
from typing import Callable

class Plugin(ABC):
    """所有插件的基类"""
    name: str = ""            # 插件名，用于日志/管理
    version: str = "0.1"
    description: str = ""

    def on_load(self, context: "PluginContext"):
        """插件加载时调用。在此注册 MQTT 订阅、初始化资源。"""
        pass

    def on_unload(self):
        """插件卸载时调用。清理资源、取消订阅。"""
        pass
```

### 零样板注册：装饰器风格

```python
# plugins/auto_feishu_push.py
from Mqtt_bbs.plugin import plugin_hook

@plugin_hook
class FeishuPushPlugin(Plugin):
    name = "auto_feishu_push"
    description = "新帖自动推送到飞书群"

    def on_load(self, ctx):
        # 注册 MQTT 主题订阅 — 这是真正的事件绑定
        ctx.subscribe("bbs/+/post", self.on_new_post)

    def on_new_post(self, topic, payload):
        chat_id = self.config.get("chat_id")
        if chat_id and isinstance(payload, dict):
            send_feishu_message(chat_id, f"新帖: {payload.get('content', '')}")
```

---

## 二、事件主题命名规范

利用 MQTT 通配符，一套**约定**替代硬编码：

| 事件 | 主题模式 | 说明 |
|------|---------|------|
| 新帖发布 | `bbs/{board}/events/post` | 替代直接监听 `bbs/{board}/post` |
| 新用户注册 | `bbs/{board}/events/register` | — |
| 文件上传完成 | `bbs/{board}/events/file_done` | — |
| 客户端上线 | `node/{agent_id}/events/online` | — |
| 客户端离线 | `node/{agent_id}/events/offline` | 含 LWT |
| 心跳超时 | `node/{agent_id}/events/timeout` | BoardService 发布 |
| 所有异常 | `system/errors/+` | 插件可观察系统健康 |

BoardService 在完成内部处理后，**额外发布** `.../events/...` 事件，插件只订阅 `events/` 路径，不干涉核心流程。

---

## 三、PluginManager — 核心引擎

```python
# Mqtt_bbs/plugin_manager.py

class PluginManager:
    def __init__(self, client: BBSClient, plugin_dir: str = "plugins"):
        self._client = client
        self._plugin_dir = plugin_dir
        self._plugins: dict[str, Plugin] = {}
        self._subscribers: dict[str, list[Callable]] = {}

    def discover_and_load(self):
        """扫描 plugins/ 目录，加载所有 @plugin_hook 插件"""
        for file in Path(self._plugin_dir).glob("*.py"):
            if file.name.startswith("_"):
                continue
            # importlib 动态加载
            # 收集模块中所有 @plugin_hook 标记的 Plugin 子类
            ...

    def load(self, module_path: str):
        """按路径加载单个插件"""
        ...

    def unload(self, name: str):
        """卸载指定插件"""
        ...

    def reload(self, name: str):
        """热重载单个插件"""
        self.unload(name); self.load(plugin_path)

    def get_plugin(self, name: str) -> Plugin | None:
        ...
```

### PluginContext — 插件运行环境

```python
class PluginContext:
    def __init__(self, manager: PluginManager, config: dict):
        self.manager = manager
        self.config = config          # 插件自己的配置
        self._subscriptions: list = []

    def subscribe(self, topic: str, callback: Callable):
        """插件通过 context 订阅 MQTT 主题"""
        self._subscriptions.append((topic, callback))
        self.manager._client.subscribe(topic, self._wrap(callback))

    def publish(self, topic: str, payload, **kwargs):
        """插件发布消息"""
        self.manager._client.publish(topic, payload, **kwargs)

    def get_config(self, key, default=None):
        return self.config.get(key, default)

    def _wrap(self, callback: Callable) -> Callable:
        """异常隔离包装：单个插件异常不影响其他插件"""
        def safe_handler(topic, payload):
            try:
                callback(topic, payload)
            except Exception as e:
                import traceback
                print(f"[Plugin ERROR] {callback.__name__}: {e}", file=sys.stderr)
                traceback.print_exc()
        return safe_handler
```

---

## 四、与 BoardService 集成

BoardService 初始化时加一行即可：

```python
class BoardService:
    def __init__(self, ...):
        # ...现有初始化代码...
        self._plugin_mgr = PluginManager(self._client, "plugins")
        self._plugin_mgr.discover_and_load()
```

BoardService 在关键操作后**发布 events 事件**（这里只加一行 publish，不改现有流程）：

```python
def _on_post(self, topic, payload):
    # 现有逻辑：验证token -> 入库 -> 广播
    ...
    # 新增：发布 events 事件（插件不干涉核心流程）
    self._plugin_mgr.trigger_event(f"{TOPIC_BBS}/{board_key}/events/post", {
        "post_id": post_id, "author": author, "content": content
    })
```

---

## 五、我们现在可以直接迁移的插件候选

| 现有功能 | 当前状态 | 插件化后 |
|---------|---------|---------|
| BBS→飞书推送 (`_init_bbs_push`) | 硬编码在 fsapp.py | `plugins/feishu_push.py` |
| Webhook 通知 | 硬编码在 BoardService | `plugins/webhook_notify.py` |
| 灵感板自动处理 | 硬编码在 fsapp.py | `plugins/inspiration_auto.py` |
| 发帖频率限制 | 无 | `plugins/rate_limiter.py` |
| 内容审核 | 无 | `plugins/content_filter.py` |

---

## 六、热加载管理端点

内置一个 HTTP API 或 MQTT 管理主题：

```
订阅: system/plugins/load    → {"name": "feishu_push"}
订阅: system/plugins/unload  → {"name": "feishu_push"}
订阅: system/plugins/reload  → {"name": "feishu_push"}
订阅: system/plugins/list    → 响应: {"plugins": ["feishu_push", ...], "status": "running"}
```

无需重启 BoardService，随时增减插件。

---

## 七、与现有系统对比

| 维度 | 现状（硬编码） | 插件化后 |
|------|--------------|---------|
| 新增功能 | 改核心源码 | 写一个 .py 放 plugins/ |
| 错误隔离 | 异常可能波及整个服务 | 单插件异常不扩散 |
| 热部署 | 需重启 BoardService | 在线 load/unload |
| 可组合性 | 多功能互相纠缠 | 插件互不依赖 |
| 调试难度 | 核心代码穿插大量非核心逻辑 | 各插件独立，日志带插件名前缀 |
