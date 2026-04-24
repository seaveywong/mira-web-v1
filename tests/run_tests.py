#!/usr/bin/env python3
"""
Mira 内部测试套件 v1.1.0
覆盖：单元测试 + 前后端联动测试
"""
import sys, os, json, time, sqlite3, requests

BASE = "http://127.0.0.1:8000/api"
TOKEN = None
PASS = 0
FAIL = 0
WARNS = []

def ok(name):
    global PASS
    PASS += 1
    print(f"  OK  {name}")

def fail(name, reason=""):
    global FAIL
    FAIL += 1
    print(f"  FAIL {name}" + (f": {reason}" if reason else ""))

def warn(name, reason=""):
    WARNS.append(f"{name}: {reason}")
    print(f"  WARN {name}" + (f": {reason}" if reason else ""))

def api(method, path, body=None, expect=200):
    global TOKEN
    headers = {"Content-Type": "application/json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    try:
        r = getattr(requests, method.lower())(
            f"{BASE}{path}", json=body, headers=headers, timeout=15
        )
        if r.status_code != expect:
            return None, r.status_code, r.text[:300]
        try:
            return r.json(), r.status_code, None
        except:
            return r.text, r.status_code, None
    except Exception as e:
        return None, 0, str(e)

print("\n" + "="*60)
print("  Mira 内部测试套件 v1.1.0")
print("="*60)

# 1. 基础连通性
print("\n[1] 基础连通性")
d, code, err = api("GET", "/health")
if code == 200:
    ok("健康检查 /health")
else:
    fail("健康检查 /health", f"HTTP {code} {err}")

# 2. 认证系统
print("\n[2] 认证系统")
d, code, err = api("POST", "/auth/login", {"password": "wrong_password"})
if code == 401:
    ok("错误密码返回401")
else:
    fail("错误密码应返回401", f"实际返回 {code}")

d, code, err = api("POST", "/auth/login", {"password": "Seaveywork@98."})
if code == 200 and d and (d.get("token") or d.get("access_token")):
    TOKEN = d.get("token") or d.get("access_token")
    ok("正确密码登录成功，获取token")
else:
    fail("登录失败", f"HTTP {code}, resp={d}, err={err}")
    print("  STOP 无法继续测试，认证失败")
    sys.exit(1)

d, code, err = api("GET", "/auth/me")
if code == 200:
    ok("Token验证 /auth/me")
else:
    fail("Token验证失败", f"HTTP {code}")

# 3. 系统设置
print("\n[3] 系统设置")
d, code, err = api("GET", "/settings")
if code == 200 and isinstance(d, (dict, list)) and len(d) > 0:
    ok(f"获取设置列表 ({len(d)} 项)")
else:
    fail("获取设置列表", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("PUT", "/settings", {"settings": [{"key": "dry_run", "value": "0"}]})
if code == 200 and d and d.get("updated") is not None:
    ok(f"保存设置 PUT /settings (updated={d.get('updated')})")
else:
    fail("保存设置", f"HTTP {code}, resp={str(d)[:100]}, err={err}")

# 4. AI 连接测试
print("\n[4] AI 连接测试")
d, code, err = api("GET", "/settings/ai-providers")
if code == 200 and isinstance(d, (dict, list)) and len(d) > 0:
    ok(f"AI厂商列表 ({len(d)} 个厂商)")
else:
    fail("AI厂商列表", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("POST", "/settings/test-ai")
if code == 200 and d is not None:
    if isinstance(d, dict) and d.get("success"):
        ok(f"AI连接测试成功: {d.get('message','')[:50]}")
    elif isinstance(d, dict) and not d.get("success"):
        warn("AI连接测试返回失败", d.get("message","")[:80])
    else:
        fail("AI连接测试响应格式异常", str(d)[:100])
else:
    fail("AI连接测试接口", f"HTTP {code}, err={err}, resp={str(d)[:100]}")

# 5. TG 推送测试
print("\n[5] TG 推送测试")
d, code, err = api("POST", "/settings/test-tg")
if code == 200 and d is not None:
    if isinstance(d, dict) and d.get("success"):
        ok("TG推送测试成功")
    elif isinstance(d, dict) and not d.get("success"):
        warn("TG推送测试返回失败", d.get("message","")[:80])
    else:
        fail("TG推送测试响应格式异常", str(d)[:100])
else:
    fail("TG推送测试接口", f"HTTP {code}, err={err}, resp={str(d)[:100]}")

# 6. Token 管理
print("\n[6] Token 管理")
d, code, err = api("GET", "/accounts/tokens")
if code == 200 and isinstance(d, list):
    ok(f"获取Token列表 ({len(d)} 个Token)")
else:
    fail("获取Token列表", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("POST", "/accounts/tokens", {
    "token_alias": "测试Token",
    "access_token": "EAAtest_invalid_token_12345"
})
if code in (400, 422):
    ok(f"无效Token被正确拒绝 (HTTP {code})")
elif code == 200:
    warn("无效Token被接受（可能跳过验证）")
else:
    fail("无效Token处理", f"HTTP {code}, resp={str(d)[:100]}")

# 7. 账户管理
print("\n[7] 账户管理")
d, code, err = api("GET", "/accounts")
if code == 200 and isinstance(d, list):
    ok(f"获取账户列表 ({len(d)} 个账户)")
else:
    fail("获取账户列表", f"HTTP {code}, resp={str(d)[:100]}")

# 8. 规则引擎
print("\n[8] 规则引擎")
d, code, err = api("GET", "/rules/types")
if code == 200 and isinstance(d, dict):
    guard_types = d.get("guard_types", [])
    scale_types = d.get("scale_types", [])
    ok(f"规则类型元数据 (止损:{len(guard_types)}种, 拉量:{len(scale_types)}种)")
else:
    fail("规则类型元数据", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("POST", "/rules/guard", {
    "act_id": "test_act_001",
    "rule_type": "bleed_abs",
    "action": "pause",
    "param_value": 20.0,
    "rule_name": "测试止血规则"
})
test_rule_id = None
if code == 200 and d and (d.get("success") or d.get("id")):
    test_rule_id = d.get("id", "ok")
    ok(f"添加止损规则成功")
else:
    fail("添加止损规则", f"HTTP {code}, resp={str(d)[:100]}, err={err}")

d, code, err = api("GET", "/rules/guard?act_id=test_act_001")
if code == 200 and isinstance(d, list):
    ok(f"获取止损规则 ({len(d)} 条)")
else:
    fail("获取止损规则", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("POST", "/rules/scale", {
    "act_id": "test_act_001",
    "rule_type": "slow_scale",
    "cpa_ratio": 0.8,
    "scale_pct": 0.15,
    "rule_name": "测试拉量规则"
})
test_scale_id = None
if code == 200 and d and (d.get("success") or d.get("id")):
    test_scale_id = d.get("id", "ok")
    ok(f"添加拉量规则成功")
else:
    fail("添加拉量规则", f"HTTP {code}, resp={str(d)[:100]}, err={err}")

# 9. KPI 配置
print("\n[9] KPI 配置")
d, code, err = api("GET", "/kpi/list")
if code == 200 and isinstance(d, list):
    ok(f"获取KPI列表 ({len(d)} 条)")
else:
    fail("获取KPI列表", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("POST", "/kpi", {
    "act_id": "test_act_001",
    "level": "account",
    "target_id": "test_act_001",
    "target_name": "测试账户",
    "kpi_field": "purchase",
    "target_cpa": 30.0
})
test_kpi_id = None
if code == 200 and d and (d.get("success") or d.get("id")):
    test_kpi_id = d.get("id", "ok")
    ok(f"添加KPI配置成功")
else:
    fail("添加KPI配置", f"HTTP {code}, resp={str(d)[:100]}, err={err}")

# 10. 看板数据
print("\n[10] 看板数据")
d, code, err = api("GET", "/dashboard/stats")
if code == 200 and isinstance(d, dict):
    ok(f"看板统计数据 (消耗=${d.get('total_spend',0):.2f})")
else:
    fail("看板统计数据", f"HTTP {code}, resp={str(d)[:100]}")

d, code, err = api("GET", "/dashboard/trend")
if code == 200 and isinstance(d, list):
    ok(f"看板趋势数据 ({len(d)} 天)")
else:
    fail("看板趋势数据", f"HTTP {code}, resp={str(d)[:100]}")

# 11. 操作日志
print("\n[11] 操作日志")
d, code, err = api("GET", "/logs")
if code == 200:
    if isinstance(d, dict):
        ok(f"操作日志 (total={d.get('total',0)})")
    elif isinstance(d, list):
        ok(f"操作日志 ({len(d)} 条)")
    else:
        fail("操作日志格式异常", str(d)[:100])
else:
    fail("操作日志", f"HTTP {code}, resp={str(d)[:100]}")

# 12. 手动触发巡检
print("\n[12] 手动触发巡检")
d, code, err = api("POST", "/rules/inspect-now")
if code == 200:
    ok(f"手动触发巡检: {str(d)[:80]}")
else:
    fail("手动触发巡检", f"HTTP {code}, resp={str(d)[:100]}")

# 13. 清理测试数据
print("\n[13] 清理测试数据")
if test_rule_id:
    d, code, err = api("DELETE", f"/rules/guard/{test_rule_id}")
    ok(f"删除测试止损规则") if code == 200 else warn(f"删除测试止损规则", f"HTTP {code}")

if test_scale_id:
    d, code, err = api("DELETE", f"/rules/scale/{test_scale_id}")
    ok(f"删除测试拉量规则") if code == 200 else warn(f"删除测试拉量规则", f"HTTP {code}")

if test_kpi_id:
    d, code, err = api("DELETE", f"/kpi/{test_kpi_id}")
    ok(f"删除测试KPI配置") if code == 200 else warn(f"删除测试KPI配置", f"HTTP {code}")

# 14. 数据库一致性检查
print("\n[14] 数据库一致性检查")
try:
    conn = sqlite3.connect('/opt/mira/data/mira.db')
    conn.row_factory = sqlite3.Row
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    required = ['settings','accounts','fb_tokens','guard_rules','scale_rules','kpi_configs','action_logs']
    missing = [t for t in required if t not in tables]
    if not missing:
        ok(f"所有必要表存在 ({len(tables)} 张表)")
    else:
        fail("缺少必要表", str(missing))

    key_settings = ['tg_bot_token','tg_chat_ids','ai_api_key','inspect_interval','dry_run']
    existing_keys = [r['key'] for r in conn.execute("SELECT key FROM settings").fetchall()]
    missing_keys = [k for k in key_settings if k not in existing_keys]
    if not missing_keys:
        ok("关键配置项完整")
    else:
        fail("缺少关键配置项", str(missing_keys))

    # 确认没有 system_settings 残留
    sys_tbl = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='system_settings'").fetchone()
    if not sys_tbl:
        ok("配置表统一性检查通过 (无 system_settings 残留)")
    else:
        warn("system_settings 表仍存在（不影响功能，但建议清理）")

    conn.close()
except Exception as e:
    fail("数据库一致性检查", str(e))

# 汇总
print("\n" + "="*60)
print(f"  测试完成: OK {PASS} 通过  FAIL {FAIL} 失败  WARN {len(WARNS)} 警告")
if WARNS:
    print("\n  警告详情:")
    for w in WARNS:
        print(f"    - {w}")
print("="*60 + "\n")
sys.exit(0 if FAIL == 0 else 1)
