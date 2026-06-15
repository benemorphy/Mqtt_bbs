"""
Plugin 签名验证工具 — 确保加载的插件未被篡改

用法:
    # 签名一个插件文件
    python -c "from plugin_signer import sign_plugin; sign_plugin('my_plugin.py', 'my_secret')"

    # 验证签名
    if verify_plugin('my_plugin.py', 'my_secret'):
        load_plugin(...)
    else:
        raise SecurityError("Plugin 签名无效")

安全设计:
    - 使用 HMAC-SHA256，基于插件源码内容生成签名
    - 签名嵌入插件文件本身的 __plugin_signature__ 变量中
    - PluginManager 加载时自动验证，失败则拒绝加载
"""

import hashlib
import hmac
import re
import os

SIGNATURE_VAR = "__plugin_signature__"
SIGNATURE_PATTERN = re.compile(
    rf'{SIGNATURE_VAR}\s*=\s*["\']([a-f0-9]{{64}})["\']'
)


def compute_signature(content: str, secret: str) -> str:
    """用 HMAC-SHA256 计算插件源码签名

    Args:
        content: 插件源码完整内容
        secret: 签名密钥（应与 MQTT_HMAC_SECRET 一致或使用独立的 PLUGIN_SIGN_SECRET）

    Returns:
        64 字符的 hex 签名
    """
    return hmac.new(
        secret.encode("utf-8"),
        content.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_plugin_signature(filepath: str, secret: str) -> tuple[bool, str]:
    """验证插件文件的签名完整性

    Args:
        filepath: 插件 .py 文件路径
        secret: 签名密钥

    Returns:
        (ok, reason)
    """
    if not os.path.isfile(filepath):
        return False, "文件不存在"

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # 提取嵌入的签名
    match = SIGNATURE_PATTERN.search(content)
    if not match:
        return False, "插件缺少签名标记"

    embedded_sig = match.group(1)

    # 移除签名行后重新计算
    clean_content = SIGNATURE_PATTERN.sub("", content)
    # 清理空行
    lines = [l for l in clean_content.split("\n") if l.strip()]
    clean_content = "\n".join(lines)

    expected = compute_signature(clean_content, secret)

    if not hmac.compare_digest(embedded_sig, expected):
        return False, f"签名不匹配 (期望 {expected[:8]}..., 得到 {embedded_sig[:8]}...)"

    return True, "签名验证通过"


def sign_plugin_file(filepath: str, secret: str, inplace: bool = True) -> str:
    """为插件文件添加签名（就地修改或返回新内容）

    Args:
        filepath: 插件 .py 文件路径
        secret: 签名密钥
        inplace: 是否就地修改文件

    Returns:
        签名后的文件内容
    """
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # 移除旧签名
    clean_content = SIGNATURE_PATTERN.sub("", content)
    lines = [l for l in clean_content.split("\n") if l.strip()]
    clean_content = "\n".join(lines)

    signature = compute_signature(clean_content, secret)

    # 在文件末尾添加签名
    signed_content = f"{clean_content}\n\n# Auto-generated plugin signature\n{SIGNATURE_VAR} = \"{signature}\"\n"

    if inplace:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(signed_content)

    return signed_content
