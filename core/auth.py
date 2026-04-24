"""
JWT 认证核心模块 v3.0
支持：多用户、四级权限（superadmin/admin/operator/viewer）、登录失败锁定
"""
import os
import jwt
import hashlib
import time
from datetime import datetime, timedelta
from collections import defaultdict
from fastapi import HTTPException, Security, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SECRET_KEY = os.environ.get("JWT_SECRET", "mira-secret-change-me-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

# ── 超级管理员（向后兼容，从ENV读取）──
ADMIN_USERNAME = os.environ.get("MIRA_USERNAME", "admin")
ADMIN_PASSWORD_HASH = os.environ.get(
    "ADMIN_PASSWORD_HASH",
    hashlib.sha256("Mira@2024!".encode()).hexdigest()
)

# ── 角色权限等级 ──
ROLE_LEVELS = {
    "superadmin": 100,
    "admin": 80,
    "operator": 50,
    "viewer": 10,
}

ROLE_LABELS = {
    "superadmin": "超级管理员",
    "admin": "管理员",
    "operator": "运营",
    "viewer": "只读观察者",
}

# ── 登录失败锁定 ──
_fail_count: dict = defaultdict(int)
_fail_time: dict = defaultdict(float)
MAX_FAIL = 5
LOCK_SECONDS = 900  # 15分钟

security = HTTPBearer()


def _reload_env():
    """从 .env 文件重新加载超级管理员用户名和密码"""
    global ADMIN_USERNAME, ADMIN_PASSWORD_HASH
    env_path = "/opt/mira/.env"
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("MIRA_USERNAME="):
                    ADMIN_USERNAME = line.split("=", 1)[1].strip()
                elif line.startswith("ADMIN_PASSWORD_HASH="):
                    ADMIN_PASSWORD_HASH = line.split("=", 1)[1].strip()
    except Exception:
        pass


def check_login_lock(ip: str):
    now = time.time()
    if _fail_count[ip] >= MAX_FAIL:
        elapsed = now - _fail_time[ip]
        if elapsed < LOCK_SECONDS:
            remaining = int(LOCK_SECONDS - elapsed)
            raise HTTPException(
                status_code=429,
                detail=f"登录失败次数过多，请 {remaining // 60} 分 {remaining % 60} 秒后再试"
            )
        else:
            _fail_count[ip] = 0


def record_fail(ip: str):
    _fail_count[ip] += 1
    _fail_time[ip] = time.time()


def record_success(ip: str):
    _fail_count[ip] = 0


def get_remaining_attempts(ip: str) -> int:
    return max(MAX_FAIL - _fail_count[ip], 0)


def verify_credentials(username: str, password: str):
    """
    验证用户名+密码，返回 (ok: bool, role: str, user_id: int|None)
    先查多用户表，再兜底超级管理员
    """
    from core.database import get_db
    _reload_env()
    pw_hash = hashlib.sha256(password.encode()).hexdigest()

    # 1. 先查 users 表（多用户）
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT id, role, is_active FROM users WHERE username=? AND password_hash=?",
            (username, pw_hash)
        ).fetchone()
        conn.close()
        if row:
            if not row["is_active"]:
                return False, None, None
            return True, row["role"], row["id"]
    except Exception:
        pass

    # 2. 兜底：超级管理员（从ENV读取）
    if username == ADMIN_USERNAME and pw_hash == ADMIN_PASSWORD_HASH:
        return True, "superadmin", 0

    return False, None, None


def verify_password(plain: str) -> bool:
    """仅验证密码（向后兼容旧版）"""
    _reload_env()
    return hashlib.sha256(plain.encode()).hexdigest() == ADMIN_PASSWORD_HASH


def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token 已过期，请重新登录")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token 无效")


def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)):
    return decode_token(credentials.credentials)


def require_role(min_role: str):
    """
    权限中间件工厂：require_role("operator") 表示需要至少运营权限
    用法：user = Depends(require_role("operator"))
    """
    min_level = ROLE_LEVELS.get(min_role, 0)

    def _checker(credentials: HTTPAuthorizationCredentials = Security(security)):
        payload = decode_token(credentials.credentials)
        role = payload.get("role", "viewer")
        # 向后兼容：旧版 token 中 uid=0 且 role="admin" 的是超级管理员
        uid = payload.get("uid", -1)
        if role == "admin" and uid == 0:
            role = "superadmin"
            payload = dict(payload)
            payload["role"] = "superadmin"
        level = ROLE_LEVELS.get(role, 0)
        if level < min_level:
            raise HTTPException(
                status_code=403,
                detail=f"权限不足：需要 {ROLE_LABELS.get(min_role, min_role)} 或更高权限"
            )
        return payload

    return _checker


# 常用权限依赖快捷方式
require_viewer = require_role("viewer")        # 只读及以上
require_operator = require_role("operator")    # 运营及以上
require_admin = require_role("admin")          # 管理员及以上
require_superadmin = require_role("superadmin") # 仅超级管理员
