from __future__ import annotations

import json
import hashlib
import sqlite3
import time
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
SUPPORTED_API_TASKS = {
    "discover_accounts",
    "graph_account_probe",
    "graph_update_status",
    "graph_get",
    "graph_post",
    "graph_upload",
    "graph_delete",
}
LOCAL_TASK_TIMEOUT_SECONDS = 45.0


def _now_cst() -> str:
    return datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _parse_cst_datetime(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def recycle_stale_running_tasks(conn=None, *, node_id: Optional[str] = None) -> int:
    own = conn is None
    conn = conn or get_conn()
    ensure_local_executor_tables(conn)
    where = "WHERE status='running'"
    params: list = []
    if node_id:
        where += " AND node_id=?"
        params.append(str(node_id))
    now_dt = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None)
    now = _now_cst()
    changed = 0
    try:
        rows = conn.execute(
            f"""
            SELECT id, timeout_sec, created_at, updated_at, started_at
            FROM local_executor_tasks
            {where}
            """,
            params,
        ).fetchall()
        for row in rows:
            ref = (
                _parse_cst_datetime(row["started_at"])
                or _parse_cst_datetime(row["updated_at"])
                or _parse_cst_datetime(row["created_at"])
            )
            if not ref:
                continue
            timeout_sec = max(LOCAL_TASK_TIMEOUT_SECONDS, float(row["timeout_sec"] or 60)) + 30
            if (now_dt - ref).total_seconds() <= timeout_sec:
                continue
            conn.execute(
                """
                UPDATE local_executor_tasks
                SET status='failed', updated_at=?, completed_at=?,
                    progress=?, error=?
                WHERE id=? AND status='running'
                """,
                (
                    now,
                    now,
                    "本地执行器任务超时，等待插件重新领取",
                    "local executor task timed out without result",
                    row["id"],
                ),
            )
            changed += 1
        if changed:
            conn.commit()
        return changed
    finally:
        if own:
            conn.close()


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
                body_json TEXT DEFAULT '{}',
                result_json TEXT DEFAULT '{}',
                idempotency_key TEXT DEFAULT '',
                operation TEXT DEFAULT '',
                method TEXT DEFAULT '',
                path TEXT DEFAULT '',
                timeout_sec INTEGER DEFAULT 60,
                attempts INTEGER DEFAULT 0,
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
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(local_executor_tasks)").fetchall()}
        extra_cols = {
            "body_json": "TEXT DEFAULT '{}'",
            "idempotency_key": "TEXT DEFAULT ''",
            "operation": "TEXT DEFAULT ''",
            "method": "TEXT DEFAULT ''",
            "path": "TEXT DEFAULT ''",
            "timeout_sec": "INTEGER DEFAULT 60",
            "attempts": "INTEGER DEFAULT 0",
        }
        for col, ddl in extra_cols.items():
            if col not in cols:
                conn.execute(f"ALTER TABLE local_executor_tasks ADD COLUMN {col} {ddl}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_local_executor_status ON local_executor_tasks(status, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_local_executor_node ON local_executor_tasks(node_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_local_executor_account ON local_executor_tasks(account_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_local_executor_idem ON local_executor_tasks(idempotency_key)")
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
    for key in ("params_json", "body_json", "result_json"):
        raw = item.pop(key, "") or "{}"
        try:
            item[key.replace("_json", "")] = json.loads(raw)
        except Exception:
            item[key.replace("_json", "")] = {}
    item["task_id"] = item.get("id")
    item["operation"] = item.get("operation") or item.get("task_type")
    item["method"] = (item.get("method") or _default_method_for_task(item.get("task_type"))).upper()
    params = item.get("params") if isinstance(item.get("params"), dict) else {}
    item["path"] = item.get("path") or params.get("path") or _default_path_for_task(item.get("task_type"), item.get("account_id"), params)
    if not isinstance(item.get("body"), dict):
        item["body"] = {}
    if not item["body"] and isinstance(params, dict):
        item["body"] = params.get("body") or params.get("data") or {}
    item["timeout_sec"] = int(item.get("timeout_sec") or 60)
    item["has_screenshot"] = bool(item.get("screenshot_data_url"))
    if item.get("screenshot_data_url"):
        item["screenshot_data_url"] = ""
    return item


def _default_method_for_task(task_type: str) -> str:
    return {
        "discover_accounts": "EXECUTE",
        "graph_get": "GET",
        "graph_post": "POST",
        "graph_upload": "POST",
        "graph_delete": "DELETE",
        "graph_update_status": "POST",
        "graph_account_probe": "GET",
    }.get(str(task_type or ""), "POST")


def _default_path_for_task(task_type: str, act_id: str, params: dict) -> str:
    task_type = str(task_type or "")
    if task_type == "discover_accounts":
        return ""
    if task_type == "graph_account_probe":
        return "/" + _normalize_act_id(params.get("act_id") or act_id).lstrip("/")
    if task_type == "graph_update_status":
        return "/" + str(params.get("object_id") or "").strip().lstrip("/")
    raw = str((params or {}).get("path") or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("/") else "/" + raw


def _task_transport(task_type: str, act_id: str, params: Optional[dict]) -> dict:
    params = dict(params or {})
    task_type = str(task_type or "").strip()
    method = _default_method_for_task(task_type)
    path = _default_path_for_task(task_type, act_id, params)
    body = {}
    query_params = {}
    if task_type == "graph_get":
        query_params = dict(params.get("params") or {})
    elif task_type == "discover_accounts":
        body = {
            "intent": "discover_accounts",
            "fields": str(params.get("fields") or "id,account_id,name,account_status,currency,timezone_name"),
            "limit": int(params.get("limit") or 200),
            "endpoint_hint": "browser_business",
        }
        query_params = {}
    elif task_type == "graph_post":
        body = dict(params.get("body") or params.get("data") or {})
        query_params = dict(params.get("params") or {})
    elif task_type == "graph_upload":
        body = {
            "fields": dict(params.get("fields") or params.get("data") or {}),
            "files": list(params.get("files") or []),
            "graph_host": str(params.get("graph_host") or params.get("host") or "graph").strip() or "graph",
        }
        query_params = dict(params.get("params") or {})
    elif task_type == "graph_delete":
        body = dict(params.get("body") or params.get("data") or {})
        query_params = dict(params.get("params") or {})
    elif task_type == "graph_update_status":
        body = {"status": str(params.get("status") or "PAUSED").upper()}
        query_params = {}
    elif task_type == "graph_account_probe":
        query_params = {"fields": params.get("fields") or "id,name,account_status,currency,timezone_name,amount_spent,spend_cap,balance"}
    timeout_sec = int(params.pop("_timeout_sec", 60) or 60)
    idem = str(params.pop("_idempotency_key", "") or "").strip()
    if not idem and params.get("_dedupe"):
        raw = json.dumps({"t": task_type, "a": _normalize_act_id(act_id), "m": method, "p": path, "q": query_params, "b": body}, sort_keys=True, ensure_ascii=False)
        idem = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    params["path"] = path.lstrip("/") if path.startswith("/") else path
    if query_params:
        params["params"] = query_params
    if body:
        params["data"] = body
    max_timeout = 600 if task_type == "graph_upload" else 180
    return {
        "operation": task_type,
        "method": method,
        "path": path,
        "params": params,
        "query_params": query_params,
        "body": body,
        "timeout_sec": max(5, min(timeout_sec, max_timeout)),
        "idempotency_key": idem,
    }


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
    if task_row["node_id"] and task_row["node_id"] == node.get("node_id") and not task_row["account_id"]:
        return True
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


def _account_scope_for_system_task(conn: sqlite3.Connection, act_id: str) -> tuple[Optional[int], Optional[int], str]:
    act_id = _normalize_act_id(act_id)
    if not act_id:
        return None, None, ""
    row = conn.execute(
        "SELECT act_id, team_id, owner_user_id FROM accounts WHERE act_id=? LIMIT 1",
        (act_id,),
    ).fetchone()
    if not row:
        return None, None, act_id
    owner_id = row["owner_user_id"] if "owner_user_id" in row.keys() else None
    return row["team_id"], owner_id, act_id


def create_local_graph_task(
    task_type: str,
    act_id: str,
    params: Optional[dict],
    node_id: str,
    created_by_name: str = "system",
) -> dict:
    task_type = str(task_type or "").strip()
    if task_type not in SUPPORTED_API_TASKS:
        raise ValueError("Unsupported local graph task")
    node_id = str(node_id or "").strip()
    if not node_id:
        raise ValueError("Local executor node_id is required")
    params = dict(params or {})
    conn = get_conn()
    try:
        ensure_local_executor_tables(conn)
        team_id, owner_id, scoped_act_id = _account_scope_for_system_task(conn, act_id)
        task_id = uuid.uuid4().hex
        now = _now_cst()
        progress = params.pop("_progress", "") or "等待本地执行器执行 Graph API 任务"
        transport = _task_transport(task_type, scoped_act_id or act_id, params)
        conn.execute(
            """
            INSERT INTO local_executor_tasks (
                id, task_type, status, node_id, team_id, owner_user_id, created_by,
                created_by_name, account_id, params_json, body_json, idempotency_key,
                operation, method, path, timeout_sec, progress, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                task_id,
                task_type,
                "queued",
                node_id,
                team_id,
                owner_id,
                None,
                created_by_name or "system",
                scoped_act_id,
                json.dumps(transport["params"], ensure_ascii=False),
                json.dumps(transport["body"], ensure_ascii=False),
                transport["idempotency_key"],
                transport["operation"],
                transport["method"],
                transport["path"],
                transport["timeout_sec"],
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


def wait_for_local_task_result(task_id: str, timeout_seconds: float = LOCAL_TASK_TIMEOUT_SECONDS) -> dict:
    deadline = time.time() + max(1.0, float(timeout_seconds or LOCAL_TASK_TIMEOUT_SECONDS))
    last_task = {}
    while time.time() < deadline:
        conn = get_conn()
        try:
            ensure_local_executor_tables(conn)
            row = conn.execute("SELECT * FROM local_executor_tasks WHERE id=?", (task_id,)).fetchone()
            if not row:
                raise RuntimeError("Local executor task not found")
            task = _row_to_task(row)
            last_task = task
            if task.get("status") in TERMINAL_STATUSES or task.get("status") == "need_user":
                if task.get("status") == "success":
                    result = task.get("result") or {}
                    if isinstance(result, dict):
                        data = result.get("data")
                        if data is None:
                            data = result.get("result")
                        if data is None:
                            data = result.get("response")
                        if data is None:
                            data = result
                        if isinstance(data, dict) and isinstance(data.get("error"), dict):
                            err = data.get("error") or {}
                            raise RuntimeError(err.get("message") or str(err))
                        return data if isinstance(data, dict) else {"data": data}
                    return {}
                raise RuntimeError(task.get("error") or task.get("progress") or f"Local task {task.get('status')}")
        finally:
            conn.close()
        time.sleep(0.5)
    progress = last_task.get("progress") if last_task else ""
    raise TimeoutError(f"本地执行器任务超时: {progress or task_id}")


def run_local_graph_task(
    candidate: dict,
    task_type: str,
    act_id: str,
    params: Optional[dict],
    timeout_seconds: float = LOCAL_TASK_TIMEOUT_SECONDS,
    created_by_name: str = "system",
) -> dict:
    if not candidate or not candidate.get("local_executor"):
        raise ValueError("candidate is not a local executor")
    task = create_local_graph_task(
        task_type=task_type,
        act_id=act_id,
        params=params or {},
        node_id=candidate.get("node_id") or "",
        created_by_name=created_by_name,
    )
    return wait_for_local_task_result(task["id"], timeout_seconds=timeout_seconds)


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
        transport = _task_transport(task_type, scoped_act_id, params)
        conn.execute(
            """
            INSERT INTO local_executor_tasks (
                id, task_type, status, node_id, team_id, owner_user_id, created_by,
                created_by_name, account_id, params_json, body_json, idempotency_key,
                operation, method, path, timeout_sec, progress, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                json.dumps(transport["params"], ensure_ascii=False),
                json.dumps(transport["body"], ensure_ascii=False),
                transport["idempotency_key"],
                transport["operation"],
                transport["method"],
                transport["path"],
                transport["timeout_sec"],
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


def poll_tasks(node_id: str, node_secret: str, capacity: int = 1, running_task_ids: Optional[list] = None) -> dict:
    node = authenticate_node(node_id, node_secret)
    ensure_local_executor_tables()
    capacity = max(1, min(int(capacity or 1), 5))
    running_task_ids = [str(x) for x in (running_task_ids or []) if str(x or "").strip()]
    conn = get_conn()
    try:
        recycle_stale_running_tasks(conn, node_id=node_id)
        running_for_node_rows = conn.execute(
            """
            SELECT id, account_id FROM local_executor_tasks
            WHERE status='running' AND node_id=?
            """,
            (node_id,),
        ).fetchall()
        current_running = [dict(r) for r in running_for_node_rows]
        if len(current_running) >= capacity:
            return {
                "task": None,
                "tasks": [],
                "server_time": _now_cst(),
                "reason": "node_busy",
                "queue": {"running": len(current_running), "waiting": 0},
            }
        rows = conn.execute(
            """
            SELECT * FROM local_executor_tasks
            WHERE status='queued'
            ORDER BY created_at ASC
            LIMIT 50
            """
        ).fetchall()
        picked = []
        busy_accounts = {r.get("account_id") for r in current_running if r.get("account_id")}
        for row in rows:
            if len(picked) >= (capacity - len(current_running)):
                break
            if row["account_id"]:
                if row["account_id"] in busy_accounts:
                    continue
            if _node_can_take_task(node, row):
                picked.append(row)
                if row["account_id"]:
                    busy_accounts.add(row["account_id"])
        if not picked:
            waiting_count = conn.execute("SELECT COUNT(*) AS c FROM local_executor_tasks WHERE status='queued'").fetchone()["c"]
            return {
                "task": None,
                "tasks": [],
                "server_time": _now_cst(),
                "queue": {"running": len(current_running), "waiting": int(waiting_count or 0)},
            }
        now = _now_cst()
        for row in picked:
            conn.execute(
                """
                UPDATE local_executor_tasks
                SET status='running', node_id=?, started_at=COALESCE(started_at, ?),
                    updated_at=?, progress=?, attempts=COALESCE(attempts,0)+1
                WHERE id=? AND status='queued'
                """,
                (node_id, now, now, "本地 API 执行器已领取任务", row["id"]),
            )
        conn.commit()
        task_rows = conn.execute(
            f"SELECT * FROM local_executor_tasks WHERE id IN ({','.join('?' for _ in picked)}) ORDER BY started_at ASC",
            [r["id"] for r in picked],
        ).fetchall()
        tasks = [_row_to_task(row) for row in task_rows]
        waiting_count = conn.execute("SELECT COUNT(*) AS c FROM local_executor_tasks WHERE status='queued'").fetchone()["c"]
        return {
            "task": tasks[0] if tasks else None,
            "tasks": tasks,
            "server_time": _now_cst(),
            "queue": {"running": len(current_running) + len(tasks), "waiting": int(waiting_count or 0)},
        }
    finally:
        conn.close()


def poll_task(node_id: str, node_secret: str) -> dict:
    data = poll_tasks(node_id, node_secret, capacity=1)
    return {"task": data.get("task"), "server_time": data.get("server_time"), "reason": data.get("reason"), "queue": data.get("queue")}


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
        if status == "success":
            try:
                params_for_task = json.loads(row["params_json"] or "{}")
            except Exception:
                params_for_task = {}
            task_path = str(row["path"] or params_for_task.get("path") or "").strip().lstrip("/")
            purpose = str(params_for_task.get("_purpose") or params_for_task.get("purpose") or "").strip()
            if row["task_type"] == "discover_accounts" or (
                row["task_type"] == "graph_get" and (task_path == "me/adaccounts" or purpose == "discover_accounts")
            ):
                try:
                    data = result if isinstance(result, dict) else {}
                    discovered = data.get("data")
                    if discovered is None:
                        discovered = data.get("result")
                    if discovered is None:
                        discovered = data.get("response")
                    if discovered is None:
                        discovered = data
                    from services.local_token_bridge import apply_discovered_accounts

                    apply_discovered_accounts(node_id, discovered if isinstance(discovered, dict) else {"data": discovered})
                except Exception:
                    pass
        fresh = conn.execute("SELECT * FROM local_executor_tasks WHERE id=?", (task_id,)).fetchone()
        return _row_to_task(fresh)
    finally:
        conn.close()
