"""管理后台 API — 用户活动监控（仅 admin/superadmin 可访问）"""
from fastapi import APIRouter, Depends, Query
from core.database import get_conn
from core.auth import require_admin, is_superadmin

router = APIRouter()


@router.get("/online-users")
def online_users(user=Depends(require_admin)):
    """最近5分钟有活动的用户"""
    conn = get_conn()
    where = [
        "is_active=1",
        "last_active_at IS NOT NULL",
        "last_active_at >= datetime('now','+8 hours','-5 minutes')",
    ]
    params = []
    if not is_superadmin(user):
        where.append("team_id=?")
        params.append(user.get("team_id"))
    rows = conn.execute(
        """SELECT id, username, role, display_name, team_id, group_name AS team_name, last_active_at, last_ip
           FROM users WHERE """ + " AND ".join(where) + """
           ORDER BY last_active_at DESC""",
        params,
    ).fetchall()
    conn.close()
    return {
        "online": [dict(r) for r in rows],
        "count": len(rows)
    }


@router.get("/user-activity")
def user_activity(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    username: str = Query(""),
    method: str = Query(""),
    user=Depends(require_admin)
):
    """分页查询用户活动日志"""
    conn = get_conn()
    where = []
    params = []
    if username:
        where.append("username=?")
        params.append(username)
    if method:
        where.append("method=?")
        params.append(method.upper())
    if not is_superadmin(user):
        where.append("team_id=?")
        params.append(user.get("team_id"))

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    count_row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM user_activity_log {where_clause}", params
    ).fetchone()
    total = count_row["cnt"] if count_row else 0

    rows = conn.execute(
        f"""SELECT id, user_id, username, role, team_id, team_name, method, path, status_code,
                   ip_address, duration_ms, created_at
            FROM user_activity_log {where_clause}
            ORDER BY id DESC LIMIT ? OFFSET ?""",
        params + [limit, offset]
    ).fetchall()
    conn.close()
    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset
    }


@router.get("/activity-usernames")
def activity_usernames(user=Depends(require_admin)):
    where = []
    params = []
    if not is_superadmin(user):
        where.append("team_id=?")
        params.append(user.get("team_id"))
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    """返回有活动记录的用户名列表（供前端下拉筛选）"""
    conn = get_conn()
    rows = conn.execute(
        f"SELECT DISTINCT username, role FROM user_activity_log {where_clause} ORDER BY username",
        params,
    ).fetchall()
    conn.close()
    return {"users": [dict(r) for r in rows]}
