#!/usr/bin/env python3
"""调试500路由错误"""
import sys
sys.path.insert(0, '/opt/mira')

import traceback

print("=== 测试 rules.py 路由 ===")
try:
    from api.rules import router as rules_router
    routes = [(r.path, r.methods) for r in rules_router.routes]
    for path, methods in routes:
        print(f"  {methods} {path}")
except Exception as e:
    print(f"FAIL 导入rules router: {e}")
    traceback.print_exc()

print("\n=== 测试 dashboard.py 路由 ===")
try:
    from api.dashboard import router as dash_router
    routes = [(r.path, r.methods) for r in dash_router.routes]
    for path, methods in routes:
        print(f"  {methods} {path}")
except Exception as e:
    print(f"FAIL 导入dashboard router: {e}")
    traceback.print_exc()

print("\n=== 直接调用 get_rule_types ===")
try:
    from api.rules import router as rules_router
    # 找到 /meta/types 路由
    for route in rules_router.routes:
        if 'types' in route.path:
            print(f"  找到路由: {route.path} -> {route.endpoint.__name__}")
except Exception as e:
    print(f"FAIL: {e}")
    traceback.print_exc()

print("\n=== 检查 rules.py 末尾内容 ===")
with open('/opt/mira/api/rules.py', 'r') as f:
    lines = f.readlines()
print(f"  总行数: {len(lines)}")
print("  最后30行:")
for i, line in enumerate(lines[-30:], start=len(lines)-29):
    print(f"  {i}: {line}", end='')
