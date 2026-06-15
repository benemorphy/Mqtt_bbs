#!/usr/bin/env python3
"""
Plugin 签名工具 — 为插件文件添加 HMAC-SHA256 签名

用法:
    python sign_plugin.py <plugin_file.py> [--secret <secret>]
    
    如不指定 --secret，默认从环境变量 PLUGIN_SIGN_SECRET 或 HMAC_SECRET 读取
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plugin_signer import sign_plugin_file

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    filepath = sys.argv[1]
    secret = None
    
    for i, arg in enumerate(sys.argv):
        if arg == '--secret' and i + 1 < len(sys.argv):
            secret = sys.argv[i + 1]
    
    if not secret:
        secret = os.environ.get("PLUGIN_SIGN_SECRET") or os.environ.get("HMAC_SECRET", "")
    
    if not secret:
        print("ERROR: 未提供签名密钥。请设置 PLUGIN_SIGN_SECRET 环境变量或使用 --secret 参数")
        sys.exit(1)
    
    result = sign_plugin_file(filepath, secret)
    print(f"签名成功: {filepath}")
    print(f"签名值: {result.split()[-1].strip('\"')}")

if __name__ == "__main__":
    main()
