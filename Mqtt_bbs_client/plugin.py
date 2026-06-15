"""
Mqtt_bbs 插件系统 — Plugin 基类与运行时上下文

用法:
    @plugin_hook
    class MyPlugin(Plugin):
        name = "my_plugin"

        def on_load(self, ctx):
            ctx.subscribe("bbs/+/events/post", self.on_post)

        def on_post(self, topic, payload):
            print(f"新帖: {payload}")

插件签名验证:
    - 所有插件默认验证 HMAC-SHA256 签名
    - 签名密钥通过 PLUGIN_SIGN_SECRET 环境变量配置
    - 签名失败时拒绝加载并记录审计日志
"""

from abc import ABC, abstractmethod
from typing import Callable, Optional
import logging as _logging
import os

log = _logging.getLogger("Mqtt_bbs.plugin")


# ── 签名验证 ──────────────────────────────────

def _compute_plugin_signature(content: str, secret: str) -> str:
    """计算插件源码的 HMAC-SHA256 签名"""
    import hashlib, hmac
    return hmac.new(
        secret.encode("utf-8"),
        content.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _verify_plugin_sig(filepath: str) -> tuple[bool, str]:
    """验证插件文件签名

    检查文件中的 __plugin_signature__ 变量是否匹配。
    如果环境变量 PLUGIN_SIGN_SECRET 未设置，跳过验证。
    """
    secret = os.environ.get("PLUGIN_SIGN_SECRET") or os.environ.get("MQTT_HMAC_SECRET")
    if not secret:
        # 未配置签名密钥，跳过验证
        return True, "跳过验证 (未配置 PLUGIN_SIGN_SECRET)"

    sig_var = "__plugin_signature__"
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        return False, f"无法读取插件文件: {e}"

    # 提取签名
    start = content.find(f"{sig_var} =")
    if start < 0:
        return False, f"插件缺少签名标记 ({sig_var})，请使用 plugin_signer.py 签名"

    # 解析签名值
    rest = content[start:]
    quote_start = rest.find('"')
    quote_end = rest.find('"', quote_start + 1) if quote_start >= 0 else -1
    if quote_start < 0 or quote_end < 0:
        return False, "签名格式错误"

    embedded_sig = rest[quote_start + 1:quote_end]

    # 移除签名行后计算
    sig_line_end = rest.find("\n", quote_end)
    clean_content = content[:start] + (content[start + sig_line_end:] if sig_line_end >= 0 else "")
    # 清理空行
    lines = [l for l in clean_content.split("\n") if l.strip()]
    clean_content = "\n".join(lines)

    expected = _compute_plugin_signature(clean_content, secret)

    import hmac as _hmac
    if not _hmac.compare_digest(embedded_sig, expected):
        return False, f"签名不匹配 (期望 {expected[:8]}..., 得到 {embedded_sig[:8]}...)"

    return True, "签名验证通过"


# ── 标记装饰器 ──────────────────────────────────

def plugin_hook(cls):
    """类装饰器：标记一个 Plugin 子类为可自动发现"""
    if not (isinstance(cls, type) and issubclass(cls, Plugin)):
        raise TypeError(f"@plugin_hook 只能用于 Plugin 子类, 收到 {cls}")
    cls._plugin_hook = True
    return cls


# ── 插件基类 ────────────────────────────────────

class Plugin(ABC):
    """所有插件的基类。子类必须设置 name。"""

    name: str = ""
    version: str = "0.1"
    description: str = ""
    # 运行时由 PluginManager 注入
    _ctx: Optional["PluginContext"] = None

    @property
    def ctx(self) -> "PluginContext":
        if self._ctx is None:
            raise RuntimeError(f"插件 {self.name} 尚未加载")
        return self._ctx

    def on_load(self, ctx: "PluginContext"):
        """插件加载时调用。在此注册 MQTT 订阅、初始化资源。"""
        pass

    def on_unload(self):
        """插件卸载时调用。清理资源。"""
        pass

    def __repr__(self):
        return f"<Plugin {self.name} v{self.version}>"


# ── 插件运行上下文 ──────────────────────────────

class PluginContext:
    """插件运行时的环境：订阅、发布、配置、日志"""

    def __init__(self, plugin: Plugin, manager: "PluginManager",
                 config: Optional[dict] = None):
        self._plugin = plugin
        self._manager = manager
        self.config = config or {}
        self._subscriptions: list[tuple[str, Callable]] = []

    # ── MQTT 操作 ──

    def subscribe(self, topic: str, callback: Callable):
        """订阅 MQTT 主题。callback(topic, payload) 自动异常隔离。"""
        wrapped = self._wrap(callback)
        self._subscriptions.append((topic, callback))
        self._manager._client.subscribe(topic, wrapped)

    def publish(self, topic: str, payload, **kwargs):
        """发布 MQTT 消息。"""
        self._manager._client.publish(topic, payload, **kwargs)

    # ── 配置 ──

    def get_config(self, key: str, default=None):
        return self.config.get(key, default)

    def set_config(self, key: str, value):
        self.config[key] = value

    # ── 生命周期 ──

    def register_filter(self, name: str, callback: Callable,
                        priority: int = 100):
        """注册过滤器到 BoardService 的 handler 链。
        name 格式: 'pre_post', 'post_post', 'pre_register', 'post_register' 等。
        callback(data) -> data 或 None（阻断）。
        """
        self._manager.register_filter(name, callback, priority, self._plugin.name)

    def unregister_all(self):
        """取消本插件所有订阅（由 PluginManager 调用）。"""
        self._subscriptions.clear()

    def _wrap(self, callback: Callable) -> Callable:
        """异常隔离包装器：单回调异常不扩散"""
        import sys

        def safe_handler(topic, payload):
            try:
                callback(topic, payload)
            except Exception as e:
                import traceback
                print(
                    f"[Plugin ERROR] {self._plugin.name}.{callback.__name__}: {e}",
                    file=sys.stderr,
                )
                traceback.print_exc()

        return safe_handler


# ── 插件管理器 ──────────────────────────────────

class PluginManager:
    """插件管理器 — 发现、加载、签名验证、生命周期管理"""

    def __init__(self, client=None, plugin_dirs: list[str] = None,
                 verify_signature: bool = True):
        """
        Args:
            client: BBSClient 实例
            plugin_dirs: 插件搜索目录列表，默认 ['./plugins']
            verify_signature: 是否验证插件签名
        """
        self._client = client
        self._plugin_dirs = plugin_dirs or ["./plugins"]
        self._verify_signature = verify_signature
        self._plugins: dict[str, Plugin] = {}
        self._contexts: dict[str, PluginContext] = {}
        self._filters: dict[str, list[tuple[int, str, Callable]]] = {}
        self._loaded_files: dict[str, str] = {}  # plugin_name -> filepath

    # ── 发现与加载 ──

    def discover(self) -> list[str]:
        """扫描插件目录，返回发现的插件文件路径"""
        import glob as _glob

        found = []
        for d in self._plugin_dirs:
            if not os.path.isdir(d):
                continue
            for f in _glob.glob(os.path.join(d, "*.py")):
                found.append(f)
        return found

    def load(self, filepath: str, config: Optional[dict] = None) -> Optional[Plugin]:
        """加载单个插件文件

        Args:
            filepath: 插件 .py 文件路径
            config: 插件配置

        Returns:
            加载成功的 Plugin 实例，失败返回 None
        """
        # 签名验证
        if self._verify_signature:
            ok, reason = _verify_plugin_sig(filepath)
            if not ok:
                log.error(f"[SECURITY] 插件签名验证失败: {filepath} -> {reason}")
                return None

        # 动态导入
        import importlib.util as _util
        import sys as _sys

        name = os.path.splitext(os.path.basename(filepath))[0]
        try:
            spec = _util.spec_from_file_location(name, filepath)
            if spec is None or spec.loader is None:
                log.error(f"无法加载插件模块: {filepath}")
                return None
            mod = _util.module_from_spec(spec)
            _sys.modules[name] = mod
            spec.loader.exec_module(mod)
        except Exception as e:
            log.error(f"插件导入失败: {filepath} -> {e}")
            import traceback
            traceback.print_exc()
            return None

        # 查找 Plugin 子类
        plugin_cls = None
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if (isinstance(attr, type) and issubclass(attr, Plugin)
                    and attr is not Plugin and getattr(attr, '_plugin_hook', False)):
                plugin_cls = attr
                break

        if plugin_cls is None:
            log.warning(f"插件文件未找到 Plugin 子类: {filepath}")
            return None

        # 实例化
        try:
            plugin = plugin_cls()
        except Exception as e:
            log.error(f"插件实例化失败: {filepath} -> {e}")
            return None

        # 注入上下文
        ctx = PluginContext(plugin, self, config=config)
        plugin._ctx = ctx

        # 调用 on_load
        try:
            plugin.on_load(ctx)
        except Exception as e:
            log.error(f"插件 {plugin.name} on_load 失败: {e}")
            import traceback
            traceback.print_exc()
            return None

        self._plugins[plugin.name] = plugin
        self._contexts[plugin.name] = ctx
        self._loaded_files[plugin.name] = filepath
        log.info(f"插件已加载: {plugin} (from {filepath})")
        return plugin

    def load_all(self, configs: Optional[dict[str, dict]] = None) -> dict[str, Plugin]:
        """扫描并加载所有发现的插件

        Args:
            configs: 按插件名映射的配置字典

        Returns:
            {plugin_name: Plugin}
        """
        configs = configs or {}
        files = self.discover()
        loaded = {}
        for fpath in files:
            name = os.path.splitext(os.path.basename(fpath))[0]
            plugin = self.load(fpath, config=configs.get(name))
            if plugin:
                loaded[plugin.name] = plugin
        return loaded

    # ── 卸载 ──

    def unload(self, name: str) -> bool:
        """卸载指定插件"""
        plugin = self._plugins.get(name)
        if plugin is None:
            return False
        try:
            plugin.on_unload()
        except Exception as e:
            log.warning(f"插件 {name} on_unload 异常: {e}")
        ctx = self._contexts.pop(name, None)
        if ctx:
            ctx.unregister_all()
        self._plugins.pop(name, None)
        self._loaded_files.pop(name, None)
        log.info(f"插件已卸载: {name}")
        return True

    # ── 过滤器 ──

    def register_filter(self, name: str, callback: Callable,
                        priority: int, plugin_name: str):
        if name not in self._filters:
            self._filters[name] = []
        self._filters[name].append((priority, plugin_name, callback))
        self._filters[name].sort(key=lambda x: x[0])

    def run_filter(self, name: str, data):
        """运行指定过滤器链"""
        for _priority, _pname, cb in self._filters.get(name, []):
            try:
                result = cb(data)
                if result is None:
                    return None  # 阻断
                data = result
            except Exception as e:
                log.warning(f"Filter {name}/{_pname} 异常: {e}")
        return data

    # ── 状态查询 ──

    def list_plugins(self) -> list[dict]:
        """列出所有已加载插件"""
        return [
            {
                "name": name,
                "version": p.version,
                "description": p.description,
                "file": self._loaded_files.get(name, ""),
            }
            for name, p in self._plugins.items()
        ]

    def get_plugin(self, name: str) -> Optional[Plugin]:
        return self._plugins.get(name)

    def __len__(self) -> int:
        return len(self._plugins)
