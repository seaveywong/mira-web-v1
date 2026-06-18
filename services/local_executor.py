from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException

from core.auth import ROLE_LEVELS, is_superadmin, normalize_user_claims
from core.database import get_conn
from core.tenancy import assert_row_access, is_operator_user, team_id_for_create, user_id
from services.local_token_bridge import authenticate_node


TASK_STATUSES = {"queued", "running", "need_user", "success", "failed", "cancelled"}
TERMINAL_STATUSES = {"success", "failed", "cancelled"}
SUPPORTED_API_TASKS = {"graph_account_probe", "graph_update_status"}


def _now_cst() -> str:
    return datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_act_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("act_") else f"act_{raw}"


def ensure_local_executor_tables(conn=None) -> None:
    own = conn is None
    conn = conn or get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS local_executor_tasks (
                id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                node_id TEXT,
                team_id INTEGER,
                owner_user_id INTEGER,
                created_by INTEGER,
                created_by_name TEXT,
                account_id TEXT,
                params_json TEXT DEFAULT '{}',
                result_json TEXT DEFAULT '{}',
                progress TEXT DEFAULT '',
                error TEXT DEFAULT '',
                screenshot_data_url TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now','+8 hours')),
                updated_at TEXT DEFAULT (datetime('now','+8 hours')),
                started_at TEXT,
                completed_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_local_executor_status ON local_executor_tasks(status, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_local_executor_node ON local_executor_tasks(node_id, status)")
        conn.commit()
    finally:
        if own:
            conn.close()


def _require_operator(user: dict) -> dict:
    user = normalize_user_claims(user)
    if ROLE_LEVELS.get(user.get("role", "viewer"), 0) < ROLE_LEVELS["operator"]:
        raise HTTPException(status_code=403, detail="Operator permission required")
    return user


def _row_to_task(row) -> dict:
    if not row:
        return {}
    item = dict(row)
    for key in ("params_json", "result_json"):
        raw = item.pop(key, "") or "{}"
        try:
            item[key.replace("_json", "")] = json.loads(raw)
        except Exception:
            item[key.replace("_json", "")] = {}
    item["has_screenshot"] = bool(item.get("screenshot_data_url"))
    if item.get("screenshot_data_url"):
        item["screenshot_data_url"] = ""
    return item


def _scope_where_for_user(user: dict, alias: str = "") -> tuple[list[str], list]:
    user = normalize_user_claims(user)
    prefix = f"{alias}." if alias else ""
    where: list[str] = []
    params: list = []
    if is_superadmin(user):
        return where, params
    team_id = team_id_for_create(user)
    where.append(f"{prefix}team_id=?")
    params.append(team_id)
    if is_operator_user(user):
        where.append(f"{prefix}owner_user_id=?")
        params.append(user_id(user))
    return where, params


def _node_can_take_task(node: dict, task_row) -> bool:
    role = str(node.get("role") or "").strip()
    if task_row["node_id"] and task_row["node_id"] != node.get("node_id"):
        return False
    if role == "superadmin":
        return True
    if task_row["team_id"] != node.get("team_id"):
        return False
    if role == "operator" and task_row["owner_user_id"] != node.get("user_id"):
        return False
    return True


def list_tasks(user: dict, limit: int = 30) -> list[dict]:
    user = _require_operator(user)
    ensure_local_executor_tables()
    where, params = _scope_where_for_user(user)
    sql_where = "WHERE " + " AND ".join(where) if where else ""
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT * FROM local_executor_tasks
            {sql_where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params + [max(1, min(int(limit or 30), 100))],
        ).fetchall()
        return [_row_to_task(r) for r in rows]
    finally:
        conn.close()


def _account_scope_for_task(conn: sqlite3.Connection, user: dict, act_id: str) -> tuple[Optional[int], Optional[int], str]:
    act_id = _normalize_act_id(act_id)
    if not act_id:
        return team_id_for_create(user), user_id(user) if is_operator_user(user) else None, ""
    acc = assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    team_id = acc["team_id"]
    if team_id is None and not is_superadmin(user):
        team_id = team_id_for_create(user)
    owner_id = acc["owner_user_id"] if "owner_user_id" in acc.keys() else None
    if owner_id is None and is_operator_user(user):
        owner_id = user_id(user)
    return team_id, owner_id, act_id


def create_api_task(
    user: dict,
    task_type: str,
    act_id: str,
    params: Optional[dict] = None,
    node_id: Optional[str] = None,
) -> dict:
    user = _require_operator(user)
    task_type = str(task_type or "").strip()
    if task_type not in SUPPORTED_API_TASKS:
        raise HTTPException(status_code=400, detail="Unsupported local API task")

    params = dict(params or {})
    conn = get_conn()
    try:
        ensure_local_executor_tables(conn)
        team_id, owner_id, scoped_act_id = _account_scope_for_task(conn, user, act_id)
        if task_type == "graph_account_probe":
            if not scoped_act_id:
                raise HTTPException(status_code=400, detail="请填写广告账户 ID")
            params.update({
                "act_id": scoped_act_id,
                "fields": params.get("fields") or "id,name,account_status,currency,timezone_name,amount_spent,spend_cap,balance",
            })
            progress = "等待本地执行器读取账户 API 状态"
        else:
            object_id = str(params.get("object_id") or "").strip()
            status = str(params.get("status") or "PAUSED").strip().upper()
            level = str(params.get("level") or "").strip().lower()
            if not scoped_act_id:
                raise HTTPException(status_code=400, detail="请填写广告账户 ID")
            if not object_id:
                raise HTTPException(status_code=400, detail="请填写要操作的广告/广告组/系列 ID")
            if status not in {"ACTIVE", "PAUSED"}:
                raise HTTPException(status_code=400, detail="状态只支持 ACTIVE 或 PAUSED")
            if level not in {"campaign", "adset", "ad"}:
                raise HTTPException(status_code=400, detail="层级只支持 campaign/adset/ad")
            params.update({"act_id": scoped_act_id, "object_id": object_id, "status": status, "level": level})
            progress = f"等待本地执行器将 {level} 更新为 {status}"

        task_id = uuid.uuid4().hex
        now = _now_cst()
        conn.execute(
            """
            INSERT INTO local_executor_tasks (
                id, task_type, status, node_id, team_id, owner_user_id, created_by,
                created_by_name, account_id, params_json, progress, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                task_id,
                task_type,
                "queued",
                str(node_id or "").strip() or None,
                team_id,
                owner_id,
                user.get("uid"),
                user.get("username") or "",
                scoped_act_id,
                json.dumps(params, ensure_ascii=False),
                progress,
                now,
                now,
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM local_executor_tasks WHERE id=?", (task_id,)).fetchone()
        return _row_to_task(row)
    finally:
        conn.close()


def poll_task(node_id: str, node_secret: str) -> dict:
    node = authenticate_node(node_id, node_secret)
    ensure_local_executor_tables()
    conn = get_conn()
    try:
        running_for_node = conn.execute(
            """
            SELECT id FROM local_executor_tasks
            WHERE status='running' AND node_id=?
            LIMIT 1
            """,
            (node_id,),
        ).fetchone()
        if running_for_node:
            return {"task": None, "server_time": _now_cst(), "reason": "node_busy"}
        rows = conn.execute(
            """
            SELECT * FROM local_executor_tasks
            WHERE status='queued'
            ORDER BY created_at ASC
            LIMIT 20
            """
        ).fetchall()
        picked = None
        for row in rows:
            if row["account_id"]:
                running_for_account = conn.execute(
                    """
                    SELECT id FROM local_executor_tasks
                    WHERE status='running' AND account_id=?
                    LIMIT 1
                    """,
                    (row["account_id"],),
                ).fetchone()
                if running_for_account:
                    continue
            if _node_can_take_task(node, row):
                picked = row
                break
        if not picked:
            return {"task": None, "server_time": _now_cst()}
        now = _now_cst()
        conn.execute(
            """
            UPDATE local_executor_tasks
            SET status='running', node_id=?, started_at=COALESCE(started_at, ?),
                updated_at=?, progress=?
            WHERE id=? AND status='queued'
            """,
            (node_id, now, now, "本地 API 执行器已领取任务", picked["id"]),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM local_executor_tasks WHERE id=?", (picked["id"],)).fetchone()
        return {"task": _row_to_task(row), "server_time": _now_cst()}
    finally:
        conn.close()


def update_task_from_node(
    task_id: str,
    node_id: str,
    node_secret: str,
    status: str,
    progress: str = "",
    result: Optional[dict] = None,
    error: str = "",
    screenshot_data_url: str = "",
) -> dict:
    node = authenticate_node(node_id, node_secret)
    status = str(status or "").strip().lower()
    if status not in TASK_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid task status")
    ensure_local_executor_tables()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM local_executor_tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Task not found")
        if row["node_id"] and row["node_id"] != node_id:
            raise HTTPException(status_code=403, detail="Task belongs to another local executor")
        if not _node_can_take_task(node, row):
            raise HTTPException(status_code=403, detail="No permission to update this task")
        now = _now_cst()
        completed_at = now if status in TERMINAL_STATUSES or status == "need_user" else row["completed_at"]
        conn.execute(
            """
            UPDATE local_executor_tasks
            SET status=?, node_id=?, progress=?, result_json=?, error=?,
                screenshot_data_url=?, updated_at=?, completed_at=?
            WHERE id=?
            """,
            (
                status,
                node_id,
                progress or "",
                json.dumps(result or {}, ensure_ascii=False),
                error or "",
                "",
                now,
                completed_at,
                task_id,
            ),
        )
        conn.commit()
        fresh = conn.execute("SELECT * FROM local_executor_tasks WHERE id=?", (task_id,)).fetchone()
        return _row_to_task(fresh)
    finally:
        conn.close()
