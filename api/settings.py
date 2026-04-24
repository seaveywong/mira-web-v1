"""
系统设置 API v1.1.0
支持: AI多厂商配置、TG多ID、AI提供商列表、密码修改
"""
import hashlib
import os
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from core.app_meta import APP_VERSION
from core.auth import get_current_user
from core.database import get_conn

router = APIRouter()

SENSITIVE_KEYS = {"ai_api_key", "tg_bot_token", "vision_api_key"}


class SettingItem(BaseModel):
    key: str
    value: str


class PasswordChange(BaseModel):
    old_password: str
    new_password: str


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "****"
    return value[:4] + "****" + value[-4:]


def _run_storage_cleanup() -> dict:
    from services.storage_manager import (
        clean_journal_logs,
        clean_old_backups,
        clean_pending_images,
        clean_thumbs,
    )

    results = {}

    r1 = clean_pending_images(mode="rejected")
    results["pending_rejected"] = r1

    r2 = clean_pending_images(mode="old", days=30)
    results["pending_old"] = r2

    r3 = clean_old_backups(keep_latest=2)
    results["backups"] = r3

    r4 = clean_thumbs(days=90)
    results["thumbs"] = r4

    r5 = clean_journal_logs(max_size_mb=100)
    results["journal"] = r5

    total_freed = sum([
        r1.get("freed_mb", 0),
        r2.get("freed_mb", 0),
        r3.get("freed_mb", 0),
        r4.get("freed_mb", 0),
    ])
    total_freed = round(total_freed, 2)

    return {
        "success": True,
        "message": f"已清理过期文件，释放 {total_freed:.2f}MB",
        "total_freed_mb": total_freed,
        "details": results,
    }


@router.get("")
def get_settings(user=Depends(get_current_user)):
    conn = get_conn()
    rows = conn.execute(
        "SELECT key, value, label, description, placeholder, category, sort_order FROM settings ORDER BY category, sort_order, key"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        if d["key"] in SENSITIVE_KEYS:
            d["value"] = _mask(d["value"] or "")
        result.append(d)
    return result


@router.post("/batch")
def update_settings(items: List[SettingItem], user=Depends(get_current_user)):
    conn = get_conn()
    for item in items:
        # 敏感字段：如果传入的是脱敏值则跳过
        if item.key in SENSITIVE_KEYS and "****" in item.value:
            continue
        conn.execute(
            "UPDATE settings SET value=? WHERE key=?",
            (item.value, item.key)
        )
    conn.commit()
    conn.close()
    return {"success": True, "message": "设置保存成功"}


@router.post("/change-password")
def change_password(body: PasswordChange, user=Depends(get_current_user)):
    """修改登录密码"""
    env_path = "/opt/mira/.env"
    if not os.path.exists(env_path):
        raise HTTPException(500, "环境配置文件不存在")

    # 验证旧密码
    old_hash = hashlib.sha256(body.old_password.encode()).hexdigest()
    with open(env_path, "r") as f:
        content = f.read()

    current_hash = ""
    for line in content.splitlines():
        if line.startswith("ADMIN_PASSWORD_HASH="):
            current_hash = line.split("=", 1)[1].strip()
            break

    if old_hash != current_hash:
        raise HTTPException(400, "旧密码不正确")

    if len(body.new_password) < 6:
        raise HTTPException(400, "新密码长度不能少于6位")

    new_hash = hashlib.sha256(body.new_password.encode()).hexdigest()
    new_content = []
    for line in content.splitlines():
        if line.startswith("ADMIN_PASSWORD_HASH="):
            new_content.append(f"ADMIN_PASSWORD_HASH={new_hash}")
        else:
            new_content.append(line)

    with open(env_path, "w") as f:
        f.write("\n".join(new_content) + "\n")

    return {"success": True, "message": "密码修改成功，下次登录生效"}


@router.get("/ai-providers")
def get_ai_providers(user=Depends(get_current_user)):
    """返回支持的AI厂商列表（供前端下拉框使用）"""
    from services.ai_advisor import get_providers_config
    return get_providers_config()


@router.post("/ai-test")
def test_ai_connection(user=Depends(get_current_user)):
    """测试AI连接是否正常"""
    from services.ai_advisor import get_ai_client, is_ai_enabled
    if not is_ai_enabled():
        return {"success": False, "message": "AI未启用或未配置API Key"}

    client, model = get_ai_client()
    if not client:
        return {"success": False, "message": "AI客户端初始化失败，请检查配置"}

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "回复'OK'"}],
            max_tokens=10
        )
        return {"success": True, "message": f"连接成功，模型: {model}",
                "response": response.choices[0].message.content}
    except Exception as e:
        return {"success": False, "message": f"连接失败: {e}"}


@router.post("/tg-test")
def test_tg_connection(user=Depends(get_current_user)):
    """测试Telegram Bot连接"""
    import requests as req
    conn = get_conn()
    token = conn.execute("SELECT value FROM settings WHERE key='tg_bot_token'").fetchone()
    chat_ids_row = conn.execute("SELECT value FROM settings WHERE key='tg_chat_ids'").fetchone()
    conn.close()

    tg_token = token["value"] if token else ""
    chat_ids_str = chat_ids_row["value"] if chat_ids_row else ""

    if not tg_token:
        return {"success": False, "message": "TG Bot Token 未配置"}
    if not chat_ids_str:
        return {"success": False, "message": "TG Chat IDs 未配置"}

    chat_ids = [cid.strip() for cid in chat_ids_str.split(",") if cid.strip()]
    results = []
    for chat_id in chat_ids:
        try:
            resp = req.post(
                f"https://api.telegram.org/bot{tg_token}/sendMessage",
                json={"chat_id": chat_id, "text": "✅ Mira 系统测试消息 - 连接正常"},
                timeout=10
            )
            data = resp.json()
            if data.get("ok"):
                results.append({"chat_id": chat_id, "success": True})
            else:
                results.append({"chat_id": chat_id, "success": False, "error": data.get("description")})
        except Exception as e:
            results.append({"chat_id": chat_id, "success": False, "error": str(e)})

    all_ok = all(r["success"] for r in results)
    if all_ok:
        msg = f"推送成功，共发送 {len(results)} 个 Chat ID"
    else:
        failed = [r for r in results if not r["success"]]
        msg = "部分失败：" + "、".join(
            r["chat_id"] + "(" + r.get("error", "未知错误") + ")" for r in failed
        )
    return {"success": all_ok, "message": msg, "results": results}


@router.get("/version")
def get_version():
    return {"version": APP_VERSION}


@router.post("/clean-storage", tags=["system"])
def clean_storage_alias(user=Depends(get_current_user)):
    """兼容前端旧入口：/api/system/clean-storage"""
    try:
        return _run_storage_cleanup()
    except Exception as e:
        return {"success": False, "message": f"清理失败: {e}", "error": str(e)}

# ── 路由别名（前端兼容） ──
class SettingsBatchWrap(BaseModel):
    settings: list = []

@router.put("")
def update_settings_put(body: SettingsBatchWrap, user=Depends(get_current_user)):
    """PUT /settings 别名，接收 {settings: [{key,value},...]}"""
    conn = get_conn()
    updated = 0
    for item in body.settings:
        if isinstance(item, dict):
            k = item.get("key") or item.get("k")
            v = item.get("value") if item.get("value") is not None else item.get("v", "")
        else:
            continue
        if k:
            # 敏感字段：如果传入的是脱敏值（包含****）则跳过，防止覆盖真实 Key
            if k in SENSITIVE_KEYS and "****" in str(v):
                continue
            # 先尝试 UPDATE，如果不存在再 INSERT（保留 label/description/category 等元数据）
            result = conn.execute("UPDATE settings SET value=? WHERE key=?", (str(v), k))
            if result.rowcount == 0:
                conn.execute("INSERT INTO settings(key,value) VALUES(?,?)", (k, str(v)))
            updated += 1
    conn.commit()
    conn.close()
    return {"updated": updated}

@router.post("/test-ai")
def test_ai_alias(user=Depends(get_current_user)):
    return test_ai_connection(user)

@router.post("/test-tg")
def test_tg_alias(user=Depends(get_current_user)):
    return test_tg_connection(user)


# ── 修改用户名 ──
class UsernameChange(BaseModel):
    username: str

@router.post("/change-username")
def change_username(body: UsernameChange, user=Depends(get_current_user)):
    """修改登录用户名（防爆破）"""
    env_path = "/opt/mira/.env"
    if not os.path.exists(env_path):
        raise HTTPException(500, "环境配置文件不存在")

    username = body.username.strip()
    if len(username) < 3:
        raise HTTPException(400, "用户名至少3位")
    if len(username) > 32:
        raise HTTPException(400, "用户名不超过32位")

    with open(env_path, "r") as f:
        content = f.read()

    new_lines = []
    found = False
    for line in content.splitlines():
        if line.startswith("MIRA_USERNAME="):
            new_lines.append(f"MIRA_USERNAME={username}")
            found = True
        else:
            new_lines.append(line)

    if not found:
        new_lines.append(f"MIRA_USERNAME={username}")

    with open(env_path, "w") as f:
        f.write("\n".join(new_lines) + "\n")

    return {"success": True, "message": f"用户名已修改为 {username}，请重新登录"}


@router.get("/ai-status")
def check_ai_status(user=Depends(get_current_user)):
    """
    实时检测 AI API Key 有效性。
    通过发送一个最小化请求到 AI 服务商验证 Key 是否有效。
    返回: {enabled: bool, valid: bool, error: str|None, model: str}
    """
    conn = get_conn()
    rows = conn.execute("SELECT key, value FROM settings WHERE key IN ('ai_enabled','ai_api_key','ai_api_base','ai_model')").fetchall()
    conn.close()

    cfg = {r["key"]: r["value"] for r in rows}
    enabled = cfg.get("ai_enabled") == "1"
    api_key = cfg.get("ai_api_key", "")
    api_base = cfg.get("ai_api_base", "https://api.deepseek.com/v1")
    model = cfg.get("ai_model", "deepseek-chat")

    if not enabled:
        return {"enabled": False, "valid": False, "error": "AI 功能未启用", "model": model}

    if not api_key:
        return {"enabled": True, "valid": False, "error": "API Key 未配置", "model": model}

    # 发送最小化请求验证 Key
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=api_base)
        # 用最小 token 消耗验证 Key
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=1,
            timeout=8
        )
        return {"enabled": True, "valid": True, "error": None, "model": model}
    except Exception as e:
        err_str = str(e)
        # 提取关键错误信息
        if "401" in err_str or "authentication" in err_str.lower() or "invalid" in err_str.lower():
            friendly = "API Key 无效或已过期"
        elif "429" in err_str or "rate" in err_str.lower():
            friendly = "API 调用频率超限"
        elif "timeout" in err_str.lower() or "connect" in err_str.lower():
            friendly = "AI 服务连接超时"
        elif "404" in err_str or "model" in err_str.lower():
            friendly = f"模型 {model} 不存在"
        else:
            friendly = err_str[:80]
        return {"enabled": True, "valid": False, "error": friendly, "model": model}


# ── 服务器资源监控 ──────────────────────────────────────────────────────────────
@router.get("/resource", tags=["system"])
def get_server_resource(user=Depends(get_current_user)):
    """获取服务器 CPU / 内存 / 磁盘使用情况"""
    import shutil, subprocess, os

    result = {}

    # CPU 使用率（读取 /proc/stat 两次，间隔 0.2s）
    try:
        def _cpu_times():
            with open('/proc/stat') as f:
                line = f.readline()
            vals = list(map(int, line.split()[1:]))
            idle = vals[3]
            total = sum(vals)
            return idle, total

        idle1, total1 = _cpu_times()
        import time; time.sleep(0.2)
        idle2, total2 = _cpu_times()
        d_idle = idle2 - idle1
        d_total = total2 - total1
        cpu_pct = round((1 - d_idle / d_total) * 100, 1) if d_total else 0
        result['cpu_pct'] = cpu_pct
        result['cpu_percent'] = cpu_pct
    except Exception as e:
        result['cpu_pct'] = None
        result['cpu_percent'] = None
        result['cpu_err'] = str(e)

    # 内存（/proc/meminfo）
    try:
        mem = {}
        with open('/proc/meminfo') as f:
            for line in f:
                k, v = line.split(':')
                mem[k.strip()] = int(v.split()[0])  # kB
        total_mb = mem.get('MemTotal', 0) // 1024
        avail_mb = mem.get('MemAvailable', 0) // 1024
        used_mb = total_mb - avail_mb
        mem_pct = round(used_mb / total_mb * 100, 1) if total_mb else 0
        result['mem_total_mb'] = total_mb
        result['mem_used_mb'] = used_mb
        result['mem_pct'] = mem_pct
        result['memory_percent'] = mem_pct
    except Exception as e:
        result['mem_pct'] = None
        result['memory_percent'] = None
        result['mem_err'] = str(e)

    # 磁盘（shutil.disk_usage）
    try:
        du = shutil.disk_usage('/')
        disk_total_gb = round(du.total / 1024**3, 1)
        disk_used_gb = round(du.used / 1024**3, 1)
        disk_pct = round(du.used / du.total * 100, 1) if du.total else 0
        result['disk_total_gb'] = disk_total_gb
        result['disk_used_gb'] = disk_used_gb
        result['disk_pct'] = disk_pct
        result['disk_percent'] = disk_pct
    except Exception as e:
        result['disk_pct'] = None
        result['disk_percent'] = None
        result['disk_err'] = str(e)

    # 系统运行时间
    try:
        with open('/proc/uptime') as f:
            uptime_sec = float(f.read().split()[0])
        days = int(uptime_sec // 86400)
        hours = int((uptime_sec % 86400) // 3600)
        mins = int((uptime_sec % 3600) // 60)
        result['uptime'] = f'{days}天 {hours}小时 {mins}分钟'
    except:
        result['uptime'] = None

    try:
        conn = get_conn()
        result['pending_creatives'] = conn.execute(
            "SELECT COUNT(*) AS c FROM pending_creatives"
        ).fetchone()["c"]
        result['asset_count'] = conn.execute(
            "SELECT COUNT(*) AS c FROM ad_assets"
        ).fetchone()["c"]
        conn.close()
    except Exception as e:
        result['pending_creatives'] = 0
        result['asset_count'] = 0
        result['db_err'] = str(e)

    try:
        log_paths = [
            "/var/log/mira/app.log",
            "/var/log/mira/error.log",
            "/var/log/mira/backup.log",
        ]
        total_bytes = sum(
            os.path.getsize(path) for path in log_paths if os.path.exists(path)
        )
        result['log_size_mb'] = round(total_bytes / 1024**2, 2)
    except Exception as e:
        result['log_size_mb'] = 0
        result['log_err'] = str(e)

    return result
