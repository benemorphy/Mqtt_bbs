"""
PluginManager — 插件发现、加载、卸载、热重载

用法:
    mgr = PluginManager(client, plugin_dir="./plugins")
    mgr.discover_and_load()       # 自动扫描加载
    mgr.load("plugins/my_ext.py") # 手动加载单个
    mgr.unload("my_ext")          # 卸载
    mgr.reload("my_ext")          # 热重载
    print(mgr.list_plugins())     # 列出所有插件
"""

import importlib.util
import inspect
import os
import sys
import threading
import traceback
from pathlib import Path
from typing import Optional, Callable, Any
from collections import defaultdict
import bisect

from Mqtt_bbs_client import config as cfg
from Mqtt_bbs_client.plugin import Plugin, PluginContext, log


class FilterChain:
    """过滤器链 — 按优先级有序执行 filter，支持修改/阻断"""

    def __init__(self):
        self._filters: dict[str, list[tuple[int, Callable, str]]] = defaultdict(list)
        # ^ {filter_name: [(priority, callback, plugin_name), ...]}

    def register(self, name: str, callback: Callable, priority: int = 100,
                 plugin_name: str = ""):
        """注册过滤器。priority 越小越先执行。callback(data) -> data|None。
        callback 返回 None 表示阻断消息。"""
        filters = self._filters[name]
        # 保持 priority 排序
        idx = bisect.bisect_left([f[0] for f in filters], priority)
        filters.insert(idx, (priority, callback, plugin_name))
        log.info(f"  [Filter] 注册: {name} (pri={priority}, plugin={plugin_name})")

    def unregister(self, name: str, callback: Callable = None, plugin_name: str = None):
        """取消注册。可指定 callback 或 plugin_name。"""
        self._filters[name] = [
            (p, cb, pn) for p, cb, pn in self._filters.get(name, [])
            if not (callback and cb == callback) and not (plugin_name and pn == plugin_name)
        ]

    def apply(self, name: str, data: dict) -> Optional[dict]:
        """依次执行过滤器。任一返回 None 则阻断。返回最终 data 或 None。"""
        for priority, callback, plugin_name in self._filters.get(name, []):
            try:
                result = callback(data)
                if result is None:
                    log.info(f"  [Filter] 阻断: {name} → {plugin_name} (pri={priority})")
                    return None
                data = result
            except Exception as e:
                log.error(f"  [Filter] 异常: {name} → {plugin_name}: {e}")
                import traceback; traceback.print_exc()
                return None
        return data

    def list_filters(self, name: str = None) -> list[dict]:
        """列出过滤器。name=None 列出全部。"""
        result = []
        for n, flist in self._filters.items():
            if name and n != name:
                continue
            for p, cb, pn in flist:
                result.append({"name": n, "plugin": pn, "priority": p,
                               "callback": cb.__name__})
        return result


class PluginManager:
    """插件管理器"""

    def __init__(self, client, plugin_dir: str = None, configs: dict = None,
                 verify_signature: bool = True):
        """
        Args:
            client: BBSClient 实例（用于订阅/发布）
            plugin_dir: 插件目录（默认 ./plugins）
            configs: {plugin_name: {key: val}} 插件配置字典
            verify_signature: 是否验证插件签
        """
        self._client = client
        self._plugin_dir = plugin_dir or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugins"
        )
        self._configs = configs or {}
        self._verify_signature = verify_signature
        self._plugins: dict[str, Plugin] = {}       # name -> Plugin
        self._modules: dict[str, str] = {}           # name -> source path
        self._lock = threading.Lock()
        self._filter_chain = FilterChain()            # P1.6: 过滤器链

    # ── 公开 API ──

    def discover_and_load(self):
        """扫描插件目录，自动加载所有 @plugin_hook 标记的插件"""
        plugin_dir = Path(self._plugin_dir)
        if not plugin_dir.is_dir():
            log.warning(f"插件目录不存在: {self._plugin_dir}")
            return []

        loaded = []
        for pyfile in sorted(plugin_dir.glob("*.py")):
            if pyfile.name.startswith("_"):
                continue
            try:
                plugins = self._load_module(str(pyfile))
                loaded.extend(plugins)
            except Exception as e:
                log.error(f"加载插件文件失败 {pyfile.name}: {e}")
                traceback.print_exc()
        return loaded

    def load(self, module_path: str) -> list[str]:
        """从指定路径加载插件文件"""
        return self._load_module(module_path)

    def unload(self, name: str) -> bool:
        """卸载指定插件"""
        with self._lock:
            plugin = self._plugins.get(name)
            if plugin is None:
                log.warning(f"插件未加载: {name}")
                return False
            try:
                plugin.on_unload()
            except Exception as e:
                log.error(f"插件 {name} on_unload 失败: {e}")
            # 清理 PluginContext 中的订阅
            if plugin._ctx:
                plugin._ctx.unregister_all()
            del self._plugins[name]
            mod_path = self._modules.pop(name, None)
            # 尝试从 sys.modules 移除
            if mod_path:
                mod_name = self._path_to_modname(mod_path)
                sys.modules.pop(mod_name, None)
            log.info(f"  [Plugin] 已卸载: {name}")
            return True

    def reload(self, name: str) -> bool:
        """热重载指定插件"""
        mod_path = self._modules.get(name)
        if not mod_path:
            log.warning(f"插件 {name} 无源文件路径，无法重载")
            return False
        self.unload(name)
        # 清除缓存
        for key in list(sys.modules.keys()):
            if name in key:
                sys.modules.pop(key, None)
        loaded = self._load_module(mod_path)
        return len(loaded) > 0

    def list_plugins(self) -> list[dict]:
        """列出所有已加载插件"""
        result = []
        with self._lock:
            for name, plugin in self._plugins.items():
                result.append({
                    "name": name,
                    "version": plugin.version,
                    "description": plugin.description,
                    "source": self._modules.get(name, ""),
                })
        return result

    def get_plugin(self, name: str) -> Optional[Plugin]:
        return self._plugins.get(name)

    def trigger_event(self, topic: str, data: dict):
        """发布 events 主题事件（供 BoardService 调用）"""
        self._client.publish(topic, data, retain=False, qos=1)

    # ── P1.6 过滤器链 ──

    def register_filter(self, name: str, callback: Callable,
                        priority: int = 100, plugin_name: str = ""):
        """注册过滤器到链。name 格式: 'pre_{handler}' / 'post_{handler}'"""
        self._filter_chain.register(name, callback, priority, plugin_name)

    def apply_filters(self, name: str, data: dict) -> Optional[dict]:
        """应用过滤器链。返回 None 表示阻断。"""
        return self._filter_chain.apply(name, data)

    def list_filters(self, name: str = None) -> list[dict]:
        """列出过滤器"""
        return self._filter_chain.list_filters(name)

    # ── 内部方法 ──

    def _load_module(self, filepath: str) -> list[str]:
        """加载单个 .py 文件，返回发现的插件名列表"""
        filepath = os.path.abspath(filepath)
        filename = os.path.basename(filepath)
        mod_name = filename.replace(".py", "")

        # Phase2 安全: 插件签名校验
        if self._verify_signature:
            try:
                from .plugin_signer import verify_plugin_signature
                secret = os.environ.get("PLUGIN_SIGN_SECRET") or os.environ.get("HMAC_SECRET", "")
                if secret:
                    ok, reason = verify_plugin_signature(filepath, secret)
                    if not ok:
                        log.error(f"  [Security] 插件签名验证失败: {filename} — {reason}")
                        raise ImportError(f"插件签名无效: {filename} ({reason})")
                    log.info(f"  [Security] 插件签名通过: {filename}")
                else:
                    log.warning(f"  [Security] PLUGIN_SIGN_SECRET未设置, 跳过签名验证: {filename}")
            except ImportError as e:
                if "No module named" in str(e):
                    log.warning(f"  [Security] plugin_signer不可用, 跳过签名验证: {filename}")
                else:
                    raise

        # 动态导入
        spec = importlib.util.spec_from_file_location(mod_name, filepath)
        if spec is None or spec.loader is None:
            raise ImportError(f"无法加载模块: {filepath}")
        mod = importlib.util.module_from_spec(spec)
        # 注入到 sys.modules 避免重复导入问题
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)

        # 扫描模块中 @plugin_hook 标记的 Plugin 子类
        loaded = []
        for name, obj in inspect.getmembers(mod):
            if (inspect.isclass(obj) and issubclass(obj, Plugin)
                    and obj is not Plugin and getattr(obj, "_plugin_hook", False)):
                plugin = obj()
                plugin._ctx = PluginContext(plugin, self,
                                            self._configs.get(plugin.name, {}))
                with self._lock:
                    self._plugins[plugin.name] = plugin
                    self._modules[plugin.name] = filepath
                try:
                    plugin.on_load(plugin._ctx)
                    log.info(f"  [Plugin] 已加载: {plugin}  ({filepath})")
                    loaded.append(plugin.name)
                except Exception as e:
                    log.error(f"  [Plugin] {plugin.name} on_load 失败: {e}")
                    traceback.print_exc()
                    with self._lock:
                        self._plugins.pop(plugin.name, None)
                        self._modules.pop(plugin.name, None)
        return loaded

    @staticmethod
    def _path_to_modname(path: str) -> str:
        return os.path.splitext(os.path.basename(path))[0]
