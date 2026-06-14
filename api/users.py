import hashlib
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from core.auth import (
    ADMIN_USERNAME,
    ROLE_LABELS,
    ROLE_LEVELS,
    get_current_user,
    is_superadmin,
    require_admin,
)
from core.database import get_db
from services.notifier import ensure_notification_schema


router = APIRouter()


class CreateUserReq(BaseModel):
    username: str
    password: str
    role: str = "operator"
    display_name: Optional[str] = None
    note: Optional[str] = None
    group_name: Optional[str] = None
    team_id: Optional[int] = None
    tg_chat_id: Optional[str] = None
    notify_enabled: Optional[bool] = False
    notify_types: Optional[str] = "all"


class UpdateUserReq(BaseModel):
    password: Optional[str] = None
    role: Optional[str] = None
    display_name: Optional[str] = None
    note: Optional[str] = None
    is_active: Optional[bool] = None
    group_name: Optional[str] = None
    team_id: Optional[int] = None
    tg_chat_id: Optional[str] = None
    notify_enabled: Optional[bool] = None
    notify_types: Optional[str] = None


class MyNotifyReq(BaseModel):
    tg_chat_id: Optional[str] = None
    notify_enabled: Optional[bool] = False
    notify_types: Optional[str] = "all"


def _ensure_team(conn, name: str) -> tuple[int, str]:
    name = (name or "").strip() or "Default Team"
    row = conn.execute("SELECT id, name FROM teams WHERE name=?", (name,)).fetchone()
    if row:
        return int(row["id"]), row["name"]
    conn.execute(
        """INSERT INTO teams (name, status, created_at, updated_at)
           VALUES (?, 'active', datetime('now','+8 hours'), datetime('now','+8 hours'))""",
        (name,),
    )
    team_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    return team_id, name


def _get_team(conn, team_id: int | None, group_name: str | None) -> tuple[int | None, str | None]:
    if team_id:
        row = conn.execute("SELECT id, name FROM teams WHERE id=?", (team_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="Team not found")
        return int(row["id"]), row["name"]
    if group_name and group_name.strip():
        return _ensure_team(conn, group_name)
    row = conn.execute("SELECT id, name FROM teams WHERE name='Default Team'").fetchone()
    if row:
        return int(row["id"]), row["name"]
    return _ensure_team(conn, "Default Team")


def _actor_team(user: dict) -> tuple[int, str | None]:
    team_id = user.get("team_id")
    if not team_id:
        raise HTTPException(status_code=403, detail="Current admin has no team assigned")
    return int(team_id), user.get("team_name")


def _user_row_to_payload(row) -> dict:
    team_name = row["team_name"] or row["group_name"]
    return {
        "id": row["id"],
        "username": row["username"],
        "role": row["role"],
        "role_label": ROLE_LABELS.get(row["role"], row["role"]),
        "display_name": row["display_name"],
        "note": row["note"],
        "is_active": bool(row["is_active"]),
        "last_login_at": row["last_login_at"],
        "last_active_at": row["last_active_at"],
        "last_ip": row["last_ip"],
        "created_at": row["created_at"],
        "group_name": team_name,
        "team_id": row["team_id"],
        "team_name": team_name,
        "tg_chat_id": row["tg_chat_id"] if "tg_chat_id" in row.keys() else "",
        "notify_enabled": bool(row["notify_enabled"]) if "notify_enabled" in row.keys() and row["notify_enabled"] is not None else False,
        "notify_types": row["notify_types"] if "notify_types" in row.keys() else "all",
    }


def _upsert_membership(conn, user_id: int, team_id: int | None, role: str) -> None:
    if not team_id:
        return
    membership_role = "admin" if role == "admin" else "member"
    conn.execute("UPDATE user_team_memberships SET is_primary=0 WHERE user_id=?", (user_id,))
    conn.execute(
        """INSERT INTO user_team_memberships (user_id, team_id, role, is_primary)
           VALUES (?, ?, ?, 1)
           ON CONFLICT(user_id, team_id)
           DO UPDATE SET role=excluded.role, is_primary=1""",
        (user_id, team_id, membership_role),
    )


@router.get("")
def list_users(user=Depends(require_admin)):
    conn = get_db()
    ensure_notification_schema(conn)
    base_sql = """SELECT u.id, u.username, u.role, u.display_name, u.note, u.is_active,
                         u.last_login_at, u.last_active_at, u.last_ip, u.created_at,
                         u.group_name, u.team_id, u.tg_chat_id, u.notify_enabled, u.notify_types,
                         t.name AS team_name
                  FROM users u
                  LEFT JOIN teams t ON t.id = u.team_id"""
    if is_superadmin(user):
        rows = conn.execute(base_sql + " ORDER BY t.name IS NULL, t.name, u.id").fetchall()
    else:
        team_id, _ = _actor_team(user)
        rows = conn.execute(base_sql + " WHERE u.team_id=? ORDER BY u.id", (team_id,)).fetchall()
    conn.close()
    return {"users": [_user_row_to_payload(row) for row in rows]}


@router.get("/groups")
def list_groups(user=Depends(require_admin)):
    conn = get_db()
    if is_superadmin(user):
        rows = conn.execute("SELECT name FROM teams WHERE status='active' ORDER BY name").fetchall()
    else:
        team_id, _ = _actor_team(user)
        rows = conn.execute("SELECT name FROM teams WHERE id=?", (team_id,)).fetchall()
    conn.close()
    return {"groups": [row["name"] for row in rows]}


@router.post("")
def create_user(body: CreateUserReq, user=Depends(require_admin)):
    if body.role not in ROLE_LEVELS or body.role == "superadmin":
        raise HTTPException(status_code=400, detail="Invalid role")
    if len(body.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    if not is_superadmin(user) and body.role == "admin":
        raise HTTPException(status_code=403, detail="Team admins can only create operator/viewer users")

    conn = get_db()
    ensure_notification_schema(conn)
    try:
        if is_superadmin(user):
            team_id, team_name = _get_team(conn, body.team_id, body.group_name)
        else:
            team_id, team_name = _actor_team(user)

        pw_hash = hashlib.sha256(body.password.encode()).hexdigest()
        conn.execute(
            """INSERT INTO users
               (username, password_hash, role, display_name, note, group_name, team_id,
                tg_chat_id, notify_enabled, notify_types)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (body.username, pw_hash, body.role, body.display_name, body.note, team_name, team_id,
             body.tg_chat_id or "", 1 if body.notify_enabled else 0, body.notify_types or "all"),
        )
        user_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        _upsert_membership(conn, user_id, team_id, body.role)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=400, detail=f"Username '{body.username}' already exists")
        if isinstance(exc, HTTPException):
            raise exc
        raise HTTPException(status_code=500, detail=str(exc))
    conn.close()
    return {"success": True, "id": user_id, "username": body.username, "role": body.role, "team_id": team_id}


@router.patch("/{user_id}")
def update_user(user_id: int, body: UpdateUserReq, user=Depends(require_admin)):
    conn = get_db()
    ensure_notification_schema(conn)
    row = conn.execute("SELECT id, role, team_id FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")

    actor_is_super = is_superadmin(user)
    if not actor_is_super:
        actor_team_id, _ = _actor_team(user)
        if row["team_id"] != actor_team_id:
            conn.close()
            raise HTTPException(status_code=403, detail="Permission denied")
        if row["role"] == "admin" and user_id != user.get("uid"):
            conn.close()
            raise HTTPException(status_code=403, detail="Team admins cannot edit other admins")

    updates = []
    params = []
    new_role = row["role"]
    new_team_id = row["team_id"]

    if body.password is not None:
        if len(body.password) < 6:
            conn.close()
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
        updates.append("password_hash=?")
        params.append(hashlib.sha256(body.password.encode()).hexdigest())
    if body.role is not None:
        if body.role not in ROLE_LEVELS or body.role == "superadmin":
            conn.close()
            raise HTTPException(status_code=400, detail="Invalid role")
        if not actor_is_super and body.role == "admin":
            conn.close()
            raise HTTPException(status_code=403, detail="Team admins cannot grant admin role")
        updates.append("role=?")
        params.append(body.role)
        new_role = body.role
    if body.display_name is not None:
        updates.append("display_name=?")
        params.append(body.display_name)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if body.is_active is not None:
        updates.append("is_active=?")
        params.append(1 if body.is_active else 0)
    if body.tg_chat_id is not None:
        updates.append("tg_chat_id=?")
        params.append(body.tg_chat_id)
    if body.notify_enabled is not None:
        updates.append("notify_enabled=?")
        params.append(1 if body.notify_enabled else 0)
    if body.notify_types is not None:
        updates.append("notify_types=?")
        params.append(body.notify_types or "all")
    if actor_is_super and (body.team_id is not None or body.group_name is not None):
        new_team_id, team_name = _get_team(conn, body.team_id, body.group_name)
        updates.append("team_id=?")
        params.append(new_team_id)
        updates.append("group_name=?")
        params.append(team_name)
    elif body.group_name is not None and not actor_is_super:
        actor_team_id, actor_team_name = _actor_team(user)
        if body.group_name.strip() and body.group_name.strip() != (actor_team_name or ""):
            conn.close()
            raise HTTPException(status_code=403, detail="Team admins cannot move users between teams")
        new_team_id = actor_team_id

    if not updates:
        conn.close()
        return {"success": True, "message": "No changes"}

    try:
        params.append(user_id)
        conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", params)
        _upsert_membership(conn, user_id, new_team_id, new_role)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=400, detail="Username or team already exists")
        if isinstance(exc, HTTPException):
            raise exc
        raise HTTPException(status_code=500, detail=str(exc))
    conn.close()
    return {"success": True}


@router.delete("/{user_id}")
def delete_user(user_id: int, user=Depends(require_admin)):
    conn = get_db()
    row = conn.execute("SELECT id, role, team_id FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")
    if not is_superadmin(user):
        actor_team_id, _ = _actor_team(user)
        if row["team_id"] != actor_team_id or row["role"] == "admin":
            conn.close()
            raise HTTPException(status_code=403, detail="Permission denied")
    conn.execute("DELETE FROM user_team_memberships WHERE user_id=?", (user_id,))
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.get("/me")
def get_me(user=Depends(get_current_user)):
    role = user.get("role", "viewer")
    uid = user.get("uid", 0)
    if uid == 0 or is_superadmin(user):
        return {
            "id": 0,
            "username": ADMIN_USERNAME,
            "role": "superadmin",
            "role_label": "超级管理员",
            "display_name": None,
            "group_name": None,
            "team_id": None,
            "team_name": None,
            "is_superadmin": True,
        }
    conn = get_db()
    ensure_notification_schema(conn)
    row = conn.execute(
        """SELECT u.id, u.username, u.role, u.display_name, u.group_name, u.team_id,
                  u.tg_chat_id, u.notify_enabled, u.notify_types,
                  t.name AS team_name
           FROM users u
           LEFT JOIN teams t ON t.id = u.team_id
           WHERE u.id=?""",
        (uid,),
    ).fetchone()
    conn.close()
    if not row:
        return {
            "id": uid,
            "username": "unknown",
            "role": role,
            "role_label": ROLE_LABELS.get(role, role),
            "display_name": None,
            "group_name": user.get("team_name"),
            "team_id": user.get("team_id"),
            "team_name": user.get("team_name"),
            "is_superadmin": False,
        }
    team_name = row["team_name"] or row["group_name"]
    return {
        "id": row["id"],
        "username": row["username"],
        "role": row["role"],
        "role_label": ROLE_LABELS.get(row["role"], row["role"]),
        "display_name": row["display_name"],
        "group_name": team_name,
        "team_id": row["team_id"],
        "team_name": team_name,
        "is_superadmin": False,
        "tg_chat_id": row["tg_chat_id"] or "",
        "notify_enabled": bool(row["notify_enabled"]),
        "notify_types": row["notify_types"] or "all",
    }


@router.get("/me/notify-settings")
def get_my_notify_settings(user=Depends(get_current_user)):
    uid = user.get("uid", 0)
    if uid == 0 or is_superadmin(user):
        return {"tg_chat_id": "", "notify_enabled": False, "notify_types": "all", "is_superadmin": True}
    conn = get_db()
    ensure_notification_schema(conn)
    row = conn.execute(
        "SELECT tg_chat_id, COALESCE(notify_enabled, 0) AS notify_enabled, COALESCE(notify_types, 'all') AS notify_types FROM users WHERE id=?",
        (uid,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {"tg_chat_id": row["tg_chat_id"] or "", "notify_enabled": bool(row["notify_enabled"]), "notify_types": row["notify_types"] or "all"}


@router.patch("/me/notify-settings")
def update_my_notify_settings(body: MyNotifyReq, user=Depends(get_current_user)):
    uid = user.get("uid", 0)
    if uid == 0 or is_superadmin(user):
        raise HTTPException(status_code=403, detail="Superadmin TG is configured in system settings")
    conn = get_db()
    ensure_notification_schema(conn)
    row = conn.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")
    conn.execute(
        "UPDATE users SET tg_chat_id=?, notify_enabled=?, notify_types=? WHERE id=?",
        (body.tg_chat_id or "", 1 if body.notify_enabled else 0, body.notify_types or "all", uid),
    )
    conn.commit()
    conn.close()
    return {"success": True}
