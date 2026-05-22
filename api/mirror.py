"""
镜像模式 API v1.0.0
账户级安全功能：开启后自动暂停不在快照白名单中的活跃广告
"""
import json
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from core.auth import get_current_user
from core.database import get_conn

router = APIRouter()
logger = logging.getLogger("mira.api.mirror")


class MirrorToggleRequest(BaseModel):
    act_id: str


# ── 获取镜像状态 ──────────────────────────────────────────────────────────────

@router.get("/status")
def mirror_status(act_id: str = "", user=Depends(get_current_user)):
    """获取单个账户或所有账户的镜像状态"""
    conn = get_conn()
    if act_id:
        row = conn.execute(
            "SELECT act_id, name, mirror_enabled FROM accounts WHERE act_id=?", (act_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "账户不存在")
        snap_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM mirror_snapshots WHERE act_id=?", (act_id,)
        ).fetchone()["cnt"]
        captured = conn.execute(
            "SELECT MAX(captured_at) as ts FROM mirror_snapshots WHERE act_id=?", (act_id,)
        ).fetchone()["ts"]
        conn.close()
        return {
            "act_id": act_id,
            "name": row["name"],
            "mirror_enabled": row["mirror_enabled"],
            "snapshot_count": snap_count,
            "captured_at": captured
        }
    else:
        rows = conn.execute(
            "SELECT act_id, name, mirror_enabled FROM accounts WHERE enabled=1 ORDER BY name"
        ).fetchall()
        result = []
        for r in rows:
            snap_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM mirror_snapshots WHERE act_id=?", (r["act_id"],)
            ).fetchone()["cnt"]
            result.append({
                "act_id": r["act_id"],
                "name": r["name"],
                "mirror_enabled": r["mirror_enabled"],
                "snapshot_count": snap_count
            })
        conn.close()
        return {"accounts": result, "total": len(result)}


# ── 获取快照广告列表 ──────────────────────────────────────────────────────────

@router.get("/snapshot")
def mirror_snapshot(act_id: str, user=Depends(get_current_user)):
    """获取账户的镜像快照中的广告ID列表"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT ad_id, ad_name, captured_at FROM mirror_snapshots WHERE act_id=? ORDER BY captured_at",
        (act_id,)
    ).fetchall()
    conn.close()
    return {
        "act_id": act_id,
        "snapshot_count": len(rows),
        "ads": [{"ad_id": r["ad_id"], "ad_name": r["ad_name"], "captured_at": r["captured_at"]} for r in rows]
    }


# ── 开启镜像模式 ──────────────────────────────────────────────────────────────

@router.post("/enable")
def mirror_enable(body: MirrorToggleRequest, user=Depends(get_current_user)):
    """开启镜像模式：抓取当前活跃广告作为快照白名单"""
    act_id = body.act_id
    conn = get_conn()
    acc = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, "账户不存在")

    # 获取当前活跃广告
    from services.guard_engine import _get_token_for_account, _fb_get, FB_AD_FIELDS
    token = _get_token_for_account(dict(acc))
    if not token:
        conn.close()
        raise HTTPException(400, "该账户无可用Token，无法抓取快照")

    try:
        data = _fb_get(
            f"{act_id}/ads", token,
            {"fields": FB_AD_FIELDS,
             "effective_status": '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED","PENDING_REVIEW","PENDING_BILLING_INFO"]',
             "limit": 200}
        )
        ads = data.get("data", [])
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"FB API调用失败: {e}")

    # 捕获快照
    from services.guard_engine import _capture_mirror_snapshot
    count = _capture_mirror_snapshot(act_id, ads)

    # 启用镜像模式
    conn.execute("UPDATE accounts SET mirror_enabled=1, updated_at=datetime('now','+8 hours') WHERE act_id=?", (act_id,))
    conn.commit()
    conn.close()

    logger.info(f"镜像模式已开启: {act_id}, 快照 {count} 条广告, 操作者: {user.get('username','unknown')}")
    return {"status": "ok", "act_id": act_id, "mirror_enabled": 1, "snapshot_count": count}


# ── 关闭镜像模式 ──────────────────────────────────────────────────────────────

@router.post("/disable")
def mirror_disable(body: MirrorToggleRequest, user=Depends(get_current_user)):
    """关闭镜像模式"""
    act_id = body.act_id
    conn = get_conn()
    acc = conn.execute("SELECT act_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, "账户不存在")
    conn.execute("UPDATE accounts SET mirror_enabled=0, updated_at=datetime('now','+8 hours') WHERE act_id=?", (act_id,))
    conn.commit()
    conn.close()
    logger.info(f"镜像模式已关闭: {act_id}, 操作者: {user.get('username','unknown')}")
    return {"status": "ok", "act_id": act_id, "mirror_enabled": 0}


# ── 全局开启：批量捕获所有账户快照 ─────────────────────────────────────────────

@router.post("/enable-all")
def mirror_enable_all(user=Depends(get_current_user)):
    """全局开启镜像模式：为所有活跃账户批量捕获快照"""
    from services.guard_engine import _get_token_for_account, _fb_get, FB_AD_FIELDS, _capture_mirror_snapshot
    conn = get_conn()
    accounts = conn.execute(
        "SELECT * FROM accounts WHERE account_status NOT IN (3,7,9)"
    ).fetchall()
    conn.close()

    total_captured = 0
    results = []
    for acc in accounts:
        a = dict(acc)
        act_id = a["act_id"]
        try:
            token = _get_token_for_account(a)
            if not token:
                results.append({"act_id": act_id, "name": a["name"], "status": "skipped", "reason": "无可用Token"})
                continue

            data = _fb_get(
                f"{act_id}/ads", token,
                {"fields": FB_AD_FIELDS,
                 "effective_status": '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED","PENDING_REVIEW","PENDING_BILLING_INFO"]',
                 "limit": 200}
            )
            ads = data.get("data", [])
            count = _capture_mirror_snapshot(act_id, ads)
            total_captured += count
            results.append({"act_id": act_id, "name": a["name"], "status": "ok", "captured": count})
        except Exception as e:
            results.append({"act_id": act_id, "name": a.get("name", act_id), "status": "error", "reason": str(e)})

    logger.info(f"全局镜像快照已捕获: {total_captured} 条广告, {len(results)} 个账户, 操作者: {user.get('username','unknown')}")
    return {"status": "ok", "total_captured": total_captured, "accounts": len(results), "details": results}
