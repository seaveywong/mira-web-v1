from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from core.auth import (
    verify_credentials, verify_password,
    create_token, check_login_lock, get_remaining_attempts, record_fail, record_success,
    ADMIN_USERNAME, ROLE_LABELS
)
from core.database import get_db
router = APIRouter()

class LoginReq(BaseModel):
    password: str
    username: Optional[str] = None

@router.post("/login")
def login(req: LoginReq, request: Request):
    ip = request.client.host if request.client else "unknown"
    try:
        check_login_lock(ip)
    except HTTPException as exc:
        if exc.status_code == 429:
            return JSONResponse(
                status_code=429,
                content={"detail": exc.detail, "remaining": 0},
            )
        raise
    username = req.username or ADMIN_USERNAME
    ok, role, uid = verify_credentials(username, req.password)
    if not ok:
        record_fail(ip)
        return JSONResponse(
            status_code=401,
            content={
                "detail": "用户名或密码错误",
                "remaining": get_remaining_attempts(ip),
            },
        )
    record_success(ip)
    token = create_token({"role": role, "uid": uid or 0, "username": username})
    # 更新最后登录时间（非超级管理员ENV账户）
    if uid and uid > 0:
        try:
            conn = get_db()
            conn.execute("UPDATE users SET last_login_at=datetime('now') WHERE id=?", (uid,))
            conn.commit()
            conn.close()
        except Exception:
            pass
    return {"token": token, "role": role, "role_label": ROLE_LABELS.get(role, role)}

@router.get("/me")
def get_me(request: Request):
    """返回当前用户信息（用于前端显示）"""
    from core.auth import ADMIN_USERNAME, _reload_env, decode_token, ROLE_LABELS
    _reload_env()
    # 尝试从token解析
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        try:
            payload = decode_token(auth_header[7:])
            uid = payload.get("uid", 0)
            role = payload.get("role", "superadmin")
            if uid and uid > 0:
                conn = get_db()
                row = conn.execute("SELECT username, display_name FROM users WHERE id=?", (uid,)).fetchone()
                conn.close()
                if row:
                    return {
                        "username": row["username"],
                        "display_name": row["display_name"],
                        "role": role,
                        "role_label": ROLE_LABELS.get(role, role)
                    }
            return {"username": ADMIN_USERNAME, "display_name": None, "role": "superadmin", "role_label": "超级管理员"}
        except Exception:
            pass
    return {"username": ADMIN_USERNAME, "display_name": None, "role": "superadmin", "role_label": "超级管理员"}
