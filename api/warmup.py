"""
账户预热 API
POST /scan        — 批量扫描并预热
POST /{act_id}/rewarm — 单个账户重新预热
GET  /status      — 查看预热状态
"""
from fastapi import APIRouter, Depends, HTTPException
from core.database import get_conn
from core.auth import get_current_user

router = APIRouter()


@router.post("/scan")
def manual_scan(user=Depends(get_current_user)):
    """手动批量扫描所有符合条件的账户并预热"""
    if user.get("role") not in ("admin", "operator", "superadmin"):
        raise HTTPException(403, "仅管理员和操作员可执行")
    from services.warmup_engine import check_and_warmup
    return check_and_warmup()


@router.post("/{act_id}/rewarm")
def re_warmup(act_id: str, user=Depends(get_current_user)):
    """对单个账户重置状态并立即执行预热"""
    if user.get("role") not in ("admin", "operator", "superadmin"):
        raise HTTPException(403, "仅管理员和操作员可执行")

    conn = get_conn()
    acc = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, "账户不存在")

    acc = dict(acc)
    if acc.get("warmup_state") == "warming":
        conn.close()
        return {"status": "skipped", "reason": "已在预热中，请等待当前预热完成"}

    conn.close()

    from services.warmup_engine import rewarm_account
    status, detail = rewarm_account(acc)
    return {"status": status, "detail": detail}


@router.get("/status")
def warmup_status(user=Depends(get_current_user)):
    """查看所有有预热记录的账户状态"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT act_id, name, warmup_state, warmup_triggered_at,
               warmup_campaign_id,
               CAST(COALESCE(warmup_last_spend, 0) AS REAL) as warmup_last_spend,
               CAST(COALESCE(amount_spent, 0) AS REAL) as amount_spent
        FROM accounts
        WHERE warmup_state IS NOT NULL
        ORDER BY warmup_triggered_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]
