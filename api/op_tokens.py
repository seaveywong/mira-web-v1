"""
操作号池 API。
路由前缀由 main.py 挂载为 /api/op-tokens。
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.auth import get_current_user
from core.database import get_conn
from services.token_manager import (
    ensure_token_source_columns,
    get_op_token_status,
    invalidate_token_cache,
    is_operate_token_eligible,
    run_heartbeat_check,
)

router = APIRouter()


class BindOpToken(BaseModel):
    token_id: int
    priority: Optional[int] = 0
    note: Optional[str] = None


class UpdateOpToken(BaseModel):
    priority: Optional[int] = None
    status: Optional[str] = None
    note: Optional[str] = None


def _require_system_operate_token(conn, token_id: int):
    token_row = conn.execute(
        """
        SELECT id, token_alias, token_type, token_source, status
        FROM fb_tokens
        WHERE id = ?
        """,
        (token_id,),
    ).fetchone()
    if not token_row:
        raise HTTPException(404, f"Token id={token_id} 不存在")
    if token_row["status"] != "active":
        raise HTTPException(400, "该 Token 当前不是有效状态，不能加入操作号池")
    if not is_operate_token_eligible(token_row["token_type"], token_row["token_source"]):
        raise HTTPException(400, "只有来源为 System User 的操作号 Token 才能加入操作号池")
    return token_row


@router.get("/{act_id}")
def list_op_tokens(act_id: str, user=Depends(get_current_user)):
    conn = get_conn()
    ensure_token_source_columns(conn)
    rows = conn.execute(
        """
        SELECT aot.id, aot.token_id, aot.priority, aot.status AS bind_status,
               aot.note, aot.created_at,
               t.token_alias, t.token_type, t.token_source,
               t.status AS token_status, t.last_verified_at
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id = ?
        ORDER BY aot.priority DESC, aot.id ASC
        """,
        (act_id,),
    ).fetchall()
    conn.close()
    result = []
    for row in rows:
        item = dict(row)
        item["op_eligible"] = is_operate_token_eligible(
            item.get("token_type"),
            item.get("token_source"),
        )
        result.append(item)
    return result


@router.get("/{act_id}/status")
def op_token_pool_status(act_id: str, user=Depends(get_current_user)):
    return get_op_token_status(act_id)


@router.post("/{act_id}/heartbeat")
def trigger_heartbeat(act_id: str, user=Depends(get_current_user)):
    result = run_heartbeat_check(act_id)
    return {"success": True, **result}


@router.post("/{act_id}")
def bind_op_token(act_id: str, body: BindOpToken, user=Depends(get_current_user)):
    conn = get_conn()
    ensure_token_source_columns(conn)
    acc = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, f"账户 {act_id} 不存在")

    _require_system_operate_token(conn, body.token_id)

    try:
        conn.execute(
            """
            INSERT INTO account_op_tokens (act_id, token_id, priority, note, token_type)
            VALUES (?, ?, ?, ?, 'operate')
            ON CONFLICT(act_id, token_id) DO UPDATE SET
                priority = excluded.priority,
                note = excluded.note,
                token_type = 'operate',
                status = 'active'
            """,
            (act_id, body.token_id, body.priority or 0, body.note),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise HTTPException(500, f"绑定失败: {exc}")
    finally:
        conn.close()

    invalidate_token_cache(body.token_id)
    return {"success": True, "message": f"System User 操作号已绑定到账户 {act_id}"}


@router.put("/{act_id}/{token_id}")
def update_op_token(act_id: str, token_id: int, body: UpdateOpToken, user=Depends(get_current_user)):
    conn = get_conn()
    ensure_token_source_columns(conn)
    row = conn.execute(
        "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
        (act_id, token_id),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "绑定关系不存在")

    updates = []
    params = []
    if body.priority is not None:
        updates.append("priority=?")
        params.append(body.priority)
    if body.status is not None:
        if body.status not in {"active", "disabled"}:
            conn.close()
            raise HTTPException(400, "status 只能是 active 或 disabled")
        if body.status == "active":
            _require_system_operate_token(conn, token_id)
        updates.append("status=?")
        params.append(body.status)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if not updates:
        conn.close()
        raise HTTPException(400, "没有需要更新的字段")

    params.extend([act_id, token_id])
    conn.execute(
        f"UPDATE account_op_tokens SET {', '.join(updates)} WHERE act_id=? AND token_id=?",
        params,
    )
    conn.commit()
    conn.close()
    invalidate_token_cache(token_id)
    return {"success": True}


@router.delete("/{act_id}/{token_id}")
def unbind_op_token(act_id: str, token_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute(
        "DELETE FROM account_op_tokens WHERE act_id=? AND token_id=?",
        (act_id, token_id),
    )
    conn.commit()
    conn.close()
    invalidate_token_cache(token_id)
    return {"success": True, "message": "操作号已解绑"}
