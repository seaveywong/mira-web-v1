"""
用户管理 API
权限：
  - GET /users          → 管理员及以上
  - POST /users         → 超级管理员
  - PATCH /users/{id}   → 超级管理员
  - DELETE /users/{id}  → 超级管理员
  - GET /users/me       → 所有登录用户
"""
import hashlib
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from core.auth import require_superadmin, require_admin, get_current_user, ROLE_LEVELS, ROLE_LABELS
from core.database import get_db

router = APIRouter()

class CreateUserReq(BaseModel):
    username: str
    password: str
    role: str = "operator"
    display_name: Optional[str] = None
    note: Optional[str] = None
    group_name: Optional[str] = None

class UpdateUserReq(BaseModel):
    password: Optional[str] = None
    role: Optional[str] = None
    display_name: Optional[str] = None
    note: Optional[str] = None
    is_active: Optional[bool] = None
    group_name: Optional[str] = None

@router.get("")
def list_users(user=Depends(require_admin)):
    """列出所有用户（管理员及以上可查看）"""
    conn = get_db()
    rows = conn.execute(
        "SELECT id, username, role, display_name, note, is_active, last_login_at, created_at, group_name FROM users ORDER BY group_name NULLS LAST, id"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "username": r["username"],
            "role": r["role"],
            "role_label": ROLE_LABELS.get(r["role"], r["role"]),
            "display_name": r["display_name"],
            "note": r["note"],
            "is_active": bool(r["is_active"]),
            "last_login_at": r["last_login_at"],
            "created_at": r["created_at"],
            "group_name": r["group_name"],
        })
    return {"users": result}

@router.get("/groups")
def list_groups(user=Depends(require_admin)):
    """列出所有分组名称"""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT group_name FROM users WHERE group_name IS NOT NULL AND group_name != '' ORDER BY group_name"
    ).fetchall()
    conn.close()
    return {"groups": [r["group_name"] for r in rows]}

@router.post("")
def create_user(body: CreateUserReq, user=Depends(require_superadmin)):
    """创建新用户（仅超级管理员）"""
    if body.role not in ROLE_LEVELS:
        raise HTTPException(400, f"无效角色，可选：{list(ROLE_LEVELS.keys())}")
    if body.role == "superadmin":
        raise HTTPException(400, "不能创建超级管理员账户")
    if len(body.password) < 6:
        raise HTTPException(400, "密码至少6位")
    pw_hash = hashlib.sha256(body.password.encode()).hexdigest()
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, display_name, note, group_name) VALUES (?,?,?,?,?,?)",
            (body.username, pw_hash, body.role, body.display_name, body.note, body.group_name)
        )
        conn.commit()
        uid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except Exception as e:
        conn.close()
        if "UNIQUE" in str(e):
            raise HTTPException(400, f"用户名 '{body.username}' 已存在")
        raise HTTPException(500, str(e))
    conn.close()
    return {"success": True, "id": uid, "username": body.username, "role": body.role}

@router.patch("/{user_id}")
def update_user(user_id: int, body: UpdateUserReq, user=Depends(require_superadmin)):
    """修改用户信息（仅超级管理员）"""
    conn = get_db()
    row = conn.execute("SELECT id, role FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "用户不存在")
    updates = []
    params = []
    if body.password is not None:
        if len(body.password) < 6:
            conn.close()
            raise HTTPException(400, "密码至少6位")
        updates.append("password_hash=?")
        params.append(hashlib.sha256(body.password.encode()).hexdigest())
    if body.role is not None:
        if body.role not in ROLE_LEVELS or body.role == "superadmin":
            conn.close()
            raise HTTPException(400, "无效角色")
        updates.append("role=?")
        params.append(body.role)
    if body.display_name is not None:
        updates.append("display_name=?")
        params.append(body.display_name)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if body.is_active is not None:
        updates.append("is_active=?")
        params.append(1 if body.is_active else 0)
    if body.group_name is not None:
        updates.append("group_name=?")
        params.append(body.group_name if body.group_name.strip() else None)
    if not updates:
        conn.close()
        return {"success": True, "message": "无变更"}
    params.append(user_id)
    conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", params)
    conn.commit()
    conn.close()
    return {"success": True}

@router.delete("/{user_id}")
def delete_user(user_id: int, user=Depends(require_superadmin)):
    """删除用户（仅超级管理员）"""
    conn = get_db()
    row = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "用户不存在")
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"success": True}

@router.get("/me")
def get_me(user=Depends(get_current_user)):
    """返回当前登录用户信息"""
    from core.auth import ADMIN_USERNAME, _reload_env, ROLE_LABELS
    _reload_env()
    role = user.get("role", "viewer")
    uid = user.get("uid", 0)
    if uid == 0:
        # 超级管理员（ENV账户）
        return {
            "id": 0,
            "username": ADMIN_USERNAME,
            "role": "superadmin",
            "role_label": "超级管理员",
            "display_name": None,
            "group_name": None,
        }
    conn = get_db()
    row = conn.execute(
        "SELECT id, username, role, display_name, group_name FROM users WHERE id=?", (uid,)
    ).fetchone()
    conn.close()
    if not row:
        return {"id": uid, "username": "unknown", "role": role, "role_label": ROLE_LABELS.get(role, role), "display_name": None, "group_name": None}
    return {
        "id": row["id"],
        "username": row["username"],
        "role": row["role"],
        "role_label": ROLE_LABELS.get(row["role"], row["role"]),
        "display_name": row["display_name"],
        "group_name": row["group_name"],
    }
