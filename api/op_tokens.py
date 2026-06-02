"""
操作号池 API。
路由前缀由 main.py 挂载为 /api/op-tokens。
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.auth import get_current_user
from core.database import get_conn
from core.tenancy import assert_row_access, claim_row_for_team
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


def _teams_compatible(account_team_id, token_team_id) -> bool:
    return account_team_id == token_team_id


def _require_system_operate_token(conn, token_id: int, user, account_team_id=None):
    assert_row_access(conn, "fb_tokens", token_id, user)
    token_row = conn.execute(
        """
        SELECT id, token_alias, token_type, token_source, status, team_id
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
    token_team_id = token_row["team_id"]
    if token_team_id is None and account_team_id is not None:
        claimed_team_id = claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
        if claimed_team_id is not None:
            token_team_id = claimed_team_id
    if not _teams_compatible(account_team_id, token_team_id):
        raise HTTPException(403, "该 Token 与账户不属于同一团队，不能加入操作号池")
    return token_row


@router.get("/{act_id}")
def list_op_tokens(act_id: str, user=Depends(get_current_user)):
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    rows = conn.execute(
        """
        SELECT aot.id, aot.token_id, aot.priority, aot.status AS bind_status,
               aot.note, aot.created_at,
               t.token_alias, t.token_type, t.token_source,
               t.status AS token_status, t.last_verified_at
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        JOIN accounts a ON a.act_id = aot.act_id
        WHERE aot.act_id = ?
          AND (
            (a.team_id IS NULL AND t.team_id IS NULL)
            OR (a.team_id IS NOT NULL AND t.team_id=a.team_id)
          )
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
    conn = get_conn()
    try:
        assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    finally:
        conn.close()
    return get_op_token_status(act_id)


@router.post("/{act_id}/heartbeat")
def trigger_heartbeat(act_id: str, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    finally:
        conn.close()
    result = run_heartbeat_check(act_id)
    return {"success": True, **result}


@router.post("/{act_id}")
def bind_op_token(act_id: str, body: BindOpToken, user=Depends(get_current_user)):
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    acc = conn.execute("SELECT id, team_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, f"账户 {act_id} 不存在")

    _require_system_operate_token(conn, body.token_id, user, acc["team_id"])

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
        claim_row_for_team(conn, "fb_tokens", "id", body.token_id, user)
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
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    assert_row_access(conn, "fb_tokens", token_id, user)
    acc = conn.execute("SELECT team_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    token = conn.execute("SELECT team_id FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    token_team_id = token["team_id"] if token else None
    if token and token_team_id is None and acc and acc["team_id"] is not None:
        claimed_team_id = claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
        if claimed_team_id is not None:
            token_team_id = claimed_team_id
    if not acc or not token or not _teams_compatible(acc["team_id"], token_team_id):
        conn.close()
        raise HTTPException(403, "该 Token 与账户不属于同一团队，不能修改绑定")
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
            _require_system_operate_token(conn, token_id, user, acc["team_id"])
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
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    assert_row_access(conn, "fb_tokens", token_id, user)
    acc = conn.execute("SELECT team_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    token = conn.execute("SELECT team_id FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    token_team_id = token["team_id"] if token else None
    if token and token_team_id is None and acc and acc["team_id"] is not None:
        claimed_team_id = claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
        if claimed_team_id is not None:
            token_team_id = claimed_team_id
    if not acc or not token or not _teams_compatible(acc["team_id"], token_team_id):
        conn.close()
        raise HTTPException(403, "该 Token 与账户不属于同一团队，不能解绑")
    conn.execute(
        "DELETE FROM account_op_tokens WHERE act_id=? AND token_id=?",
        (act_id, token_id),
    )
    conn.commit()
    conn.close()
    invalidate_token_cache(token_id)
    return {"success": True, "message": "操作号已解绑"}
