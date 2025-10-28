import sys
import os
import json
import importlib

# 彻底修复 JSONDecodeError 问题
def fix_json_decode_error():
    # 修复标准库的 json 模块
    if not hasattr(json, 'JSONDecodeError'):
        class JSONDecodeError(ValueError):
            def __init__(self, msg, doc, pos):
                self.msg = msg
                self.doc = doc
                self.pos = pos
                self.lineno = doc.count('\n', 0, pos) + 1
                self.colno = pos - doc.rfind('\n', 0, pos)
                super().__init__(f"{msg}: line {self.lineno} column {self.colno} (char {pos})")

        json.JSONDecodeError = JSONDecodeError
        sys.modules['json'].JSONDecodeError = JSONDecodeError

    # 修复 pip vendor 中的 json 模块
    pip_vendor_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Lib', 'site-packages', 'pip', '_vendor')
    if os.path.exists(pip_vendor_path):
        vendor_json_path = os.path.join(pip_vendor_path, 'json')
        if os.path.exists(vendor_json_path):
            # 重新加载 pip vendor 的 json 模块
            for module_name in list(sys.modules.keys()):
                if 'pip._vendor.json' in module_name:
                    del sys.modules[module_name]

fix_json_decode_error()
print("JSONDecodeError 修复完成")

# 现在直接安装包，不通过 pip 命令
packages = [
    'kafka-python==2.0.2',
    'pandas',
    'pymssql',
    'psycopg2-binary',
    'requests',
    'minio',
    'tqdm',
    'ujson'
]

print("开始安装包...")
for pkg in packages:
    print(f"安装 {pkg}...")
    os.system(f'"{sys.executable}" -c "import subprocess, sys; subprocess.run([sys.executable, \\\"-m\\\", \\\"pip\\\", \\\"install\\\", \\\"{pkg}\\\"])"')