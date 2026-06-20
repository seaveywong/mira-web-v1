from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.auth import (
    ADMIN_USERNAME,
    ROLE_LABELS,
    build_user_claims,
    check_login_lock,
    create_token,
    decode_token,
    get_remaining_attempts,
    normalize_user_claims,
    record_fail,
    record_success,
    verify_credentials,
)
from core.database import get_db

router = APIRouter()


class LoginReq(BaseModel):
    password: str
    username: Optional[str] = None


def _login_key(username: str, ip: str) -> str:
    name = (username or "").strip().lower()[:80] or "unknown"
    return f"login:{name}:{ip or 'unknown'}"


@router.post("/login")
def login(req: LoginReq, request: Request):
    ip = request.client.host if request.client else "unknown"
    username = (req.username or ADMIN_USERNAME).strip()
    login_key = _login_key(username, ip)

    try:
        check_login_lock(login_key)
    except HTTPException as exc:
        if exc.status_code == 429:
            return JSONResponse(
                status_code=429,
                content={"detail": exc.detail, "remaining": 0},
            )
        raise

    ok, role, uid = verify_credentials(username, req.password)
    if not ok:
        record_fail(login_key)
        return JSONResponse(
            status_code=401,
            content={
                "detail": "用户名或密码错误",
                "remaining": get_remaining_attempts(login_key),
            },
        )

    record_success(login_key)
    claims = build_user_claims(username, role, uid)
    token = create_token(claims)

    if uid and uid > 0:
        try:
            conn = get_db()
            conn.execute("UPDATE users SET last_login_at=datetime('now') WHERE id=?", (uid,))
            conn.commit()
            conn.close()
        except Exception:
            pass

    return {
        "token": token,
        "username": username,
        "role": claims.get("role", role),
        "role_label": ROLE_LABELS.get(claims.get("role", role), role),
        "team_id": claims.get("team_id"),
        "team_name": claims.get("team_name"),
        "team_status": claims.get("team_status"),
        "is_superadmin": claims.get("is_superadmin", False),
    }


@router.get("/me")
def get_me(request: Request):
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")

    payload = normalize_user_claims(decode_token(auth_header[7:]))
    uid = payload.get("uid", 0)
    role = payload.get("role", "viewer")

    if uid and uid > 0:
        conn = get_db()
        try:
            row = conn.execute(
                """SELECT u.username, u.display_name, u.team_id, u.group_name,
                          t.name AS team_name, t.status AS team_status
                   FROM users u
                   LEFT JOIN teams t ON t.id = u.team_id
                   WHERE u.id=?""",
                (uid,),
            ).fetchone()
        finally:
            conn.close()
        if row:
            return {
                "username": row["username"],
                "display_name": row["display_name"],
                "role": role,
                "role_label": ROLE_LABELS.get(role, role),
                "team_id": row["team_id"],
                "team_name": row["team_name"] or row["group_name"],
                "team_status": (row["team_status"] or "active") if row["team_id"] else None,
                "is_superadmin": False,
            }
        raise HTTPException(status_code=401, detail="用户不存在或已被删除，请重新登录")

    if payload.get("is_superadmin"):
        return {
            "username": payload.get("username") or ADMIN_USERNAME,
            "display_name": None,
            "role": "superadmin",
            "role_label": ROLE_LABELS.get("superadmin", "superadmin"),
            "team_id": None,
            "team_name": None,
            "team_status": None,
            "is_superadmin": True,
        }

    return {
        "username": payload.get("username") or "unknown",
        "display_name": None,
        "role": role,
        "role_label": ROLE_LABELS.get(role, role),
        "team_id": payload.get("team_id"),
        "team_name": payload.get("team_name"),
        "team_status": payload.get("team_status"),
        "is_superadmin": False,
    }
