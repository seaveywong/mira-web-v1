from __future__ import annotations

import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import HTTPException

from core.auth import ROLE_LEVELS, is_superadmin, normalize_user_claims
from core.database import get_conn
from core.tenancy import assert_row_access, is_operator_user, team_id_for_create, user_id
from services.local_token_bridge import authenticate_node


TASK_STATUSES = {"queued", "running", "need_user", "success", "failed", "cancelled"}
TERMINAL_STATUSES = {"success", "failed", "cancelled"}
MAX_SCREENSHOT_CHARS = 2_500_000


def _now_cst() -> str:
    return datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_act_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("act_") else f"act_{raw}"


def _numeric_act_id(value: str) -> str:
    return _normalize_act_id(value).replace("act_", "", 1)


def _ads_manager_url(act_id: str) -> str:
    numeric = _numeric_act_id(act_id)
    return f"https://adsmanager.facebook.com/adsmanager/manage/campaigns?act={numeric}&nav_source=mira_local_executor"


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


def create_open_account_task(user: dict, act_id: str, node_id: Optional[str] = None) -> dict:
    user = _require_operator(user)
    act_id = _normalize_act_id(act_id)
    if not act_id:
        raise HTTPException(status_code=400, detail="请填写广告账户 ID")
    conn = get_conn()
    try:
        ensure_local_executor_tables(conn)
        acc = assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
        team_id = acc["team_id"]
        if team_id is None and not is_superadmin(user):
            team_id = team_id_for_create(user)
        owner_id = acc["owner_user_id"] if "owner_user_id" in acc.keys() else None
        if owner_id is None and is_operator_user(user):
            owner_id = user_id(user)
        task_id = uuid.uuid4().hex
        params = {
            "act_id": act_id,
            "target_url": _ads_manager_url(act_id),
            "expect": "open_ads_manager_and_detect_state",
        }
        now = _now_cst()
        conn.execute(
            """
            INSERT INTO local_executor_tasks (
                id, task_type, status, node_id, team_id, owner_user_id, created_by,
                created_by_name, account_id, params_json, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                task_id,
                "open_ads_manager",
                "queued",
                str(node_id or "").strip() or None,
                team_id,
                owner_id,
                user.get("uid"),
                user.get("username") or "",
                act_id,
                json.dumps(params, ensure_ascii=False),
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
            (node_id, now, now, "本地执行器已领取任务", picked["id"]),
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
        shot = str(screenshot_data_url or "")
        if len(shot) > MAX_SCREENSHOT_CHARS:
            shot = shot[:MAX_SCREENSHOT_CHARS]
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
                shot,
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
