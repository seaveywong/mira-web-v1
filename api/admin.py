"""管理后台 API — 用户活动监控（仅 admin/superadmin 可访问）"""
from fastapi import APIRouter, Depends, Query
from core.database import get_conn
from core.auth import require_admin

router = APIRouter()


@router.get("/online-users")
def online_users(user=Depends(require_admin)):
    """最近5分钟有活动的用户"""
    conn = get_conn()
    rows = conn.execute(
        """SELECT id, username, role, display_name, last_active_at, last_ip
           FROM users WHERE is_active=1 AND last_active_at IS NOT NULL
           AND last_active_at >= datetime('now','+8 hours','-5 minutes')
           ORDER BY last_active_at DESC"""
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

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    count_row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM user_activity_log {where_clause}", params
    ).fetchone()
    total = count_row["cnt"] if count_row else 0

    rows = conn.execute(
        f"""SELECT id, user_id, username, role, method, path, status_code,
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
    """返回有活动记录的用户名列表（供前端下拉筛选）"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT username, role FROM user_activity_log ORDER BY username"
    ).fetchall()
    conn.close()
    return {"users": [dict(r) for r in rows]}
