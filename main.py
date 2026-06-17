from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import os, time, jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from core.app_meta import APP_VERSION, get_allowed_origins
from core.database import init_db, get_conn
from core.auth import SECRET_KEY, ALGORITHM, ROLE_LEVELS, normalize_user_claims
from core.tenancy import team_write_block_reason
from api.auth import router as auth_router
from api.accounts import router as accounts_router
from api.rules import router as rules_router
from api.kpi import router as kpi_router
from api.settings import router as settings_router
from api.dashboard import router as dashboard_router
from api.logs import router as logs_router
from api.op_tokens import router as op_tokens_router
from api.assets import router as assets_router
from api.ad_templates import router as ad_templates_router
from api.storage import router as storage_router
from api.users import router as users_router
from api.teams import router as teams_router
from api.admin import router as admin_router
from api.mirror import router as mirror_router
from api.warmup import router as warmup_router
from api.ad_ops import router as ad_ops_router
from api.landing_pages import router as landing_pages_router
from api.meta_oauth import router as meta_oauth_router
from core.scheduler import start_scheduler
import logging
import sys

# ── 日志配置：将 mira.* 的日志输出到 stderr（由 systemd 重定向到 error.log）──
_log_handler = logging.StreamHandler(sys.stderr)
_log_handler.setLevel(logging.DEBUG)
_log_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
_mira_logger = logging.getLogger('mira')
_mira_logger.setLevel(logging.INFO)
_mira_logger.addHandler(_log_handler)
_mira_logger.propagate = False




class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.time()
        try:
            response = await call_next(request)
        except RuntimeError as exc:
            if "No response returned" in str(exc):
                logging.getLogger("mira").info(
                    "client disconnected before response: %s %s",
                    request.method,
                    request.url.path,
                )
                return Response(status_code=499)
            raise
        response.headers["x-process-time"] = f"{time.time()-start:.4f}"
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: blob: https:; "
            "connect-src 'self' https://graph.facebook.com; "
            "base-uri 'self'; form-action 'self'; frame-ancestors 'self'"
        )
        if request.url.path.startswith("/api/"):
            response.headers.setdefault("Cache-Control", "no-store")
        return response


class ActivityAuditMiddleware(BaseHTTPMiddleware):
    """记录每个 API 请求到 user_activity_log，更新 users.last_active_at"""
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)

        start = time.time()
        response = await call_next(request)
        duration_ms = int((time.time() - start) * 1000)

        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                token = auth_header[7:]
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                uid = payload.get("uid")
                username = payload.get("username", "unknown")
                role = payload.get("role", "viewer")
                team_id = payload.get("team_id")
                team_name = payload.get("team_name")
                ip = request.client.host if request.client else ""

                conn = get_conn()
                conn.execute(
                    """INSERT INTO user_activity_log
                       (user_id, username, role, team_id, team_name, method, path, status_code, ip_address, duration_ms)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (uid, username, role, team_id, team_name, request.method, path, response.status_code, ip, duration_ms)
                )
                conn.execute(
                    "UPDATE users SET last_active_at=datetime('now','+8 hours'), last_ip=? WHERE username=?",
                    (ip, username)
                )
                if role in ("superadmin", "admin", "operator"):
                    result = conn.execute(
                        "UPDATE settings SET value=datetime('now','+8 hours') WHERE key='last_admin_activity'"
                    )
                    if result.rowcount == 0:
                        conn.execute(
                            "INSERT INTO settings(key,value) VALUES('last_admin_activity', datetime('now','+8 hours'))"
                        )
                conn.commit()
                conn.close()
            except Exception:
                pass  # 审计日志失败不影响正常请求

        return response


class TeamWriteGuardMiddleware(BaseHTTPMiddleware):
    WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if not path.startswith("/api/") or request.method.upper() not in self.WRITE_METHODS:
            return await call_next(request)
        if path == "/api/auth/login":
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return await call_next(request)

        try:
            payload = jwt.decode(auth_header[7:], SECRET_KEY, algorithms=[ALGORITHM])
            user = normalize_user_claims(payload)
        except Exception:
            return await call_next(request)

        if ROLE_LEVELS.get(user.get("role", "viewer"), 0) < ROLE_LEVELS["operator"]:
            return JSONResponse(
                {"detail": "当前账号为只读角色，不能执行写入操作", "code": "role_read_only"},
                status_code=403,
            )

        conn = None
        try:
            conn = get_conn()
            reason = team_write_block_reason(conn, user)
        except Exception as exc:
            logging.getLogger("mira").warning(f"team write guard warning: {exc}")
            return await call_next(request)
        finally:
            if conn:
                conn.close()

        if reason:
            return JSONResponse(
                {"detail": reason, "code": "team_write_disabled"},
                status_code=403,
            )
        return await call_next(request)


app = FastAPI(title="Mira Ads Guard", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TimingMiddleware)
app.add_middleware(TeamWriteGuardMiddleware)
app.add_middleware(ActivityAuditMiddleware)
app.add_middleware(SecurityHeadersMiddleware)


@app.on_event("startup")
async def startup():
    init_db()
    try:
        import db_migrate_users
        db_migrate_users.run()
    except Exception as _e:
        import logging
        logging.getLogger("mira").warning(f"migrate_users warning: {_e}")
    # v3.0 数据库迁移（幂等，可重复执行）
    try:
        import db_migrate_v3
        db_migrate_v3.run()
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"v3 migrate warning: {e}")
    # v4.0 数据库迁移（修复 auto_campaigns 缺失字段，幂等可重复执行）
    try:
        import db_migrate_v4
        db_migrate_v4.run()
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"v4 migrate warning: {e}")
    # v5.0 数据库迁移（用户活动监控相关列，幂等可重复执行）
    try:
        import db_migrate_v5
        db_migrate_v5.run()
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"v5 migrate warning: {e}")
    try:
        import db_migrate_v6
        db_migrate_v6.run()
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"v6 migrate warning: {e}")
    try:
        from services.notifier import ensure_notification_schema
        ensure_notification_schema()
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"notification schema warning: {e}")
    try:
        from services.default_owner_rules import ensure_operator_default_stoploss_rules
        result = ensure_operator_default_stoploss_rules()
        if result.get("created"):
            logging.getLogger("mira").info(f"default owner stoploss rules created: {result['created']}")
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"default owner stoploss rules warning: {e}")
    # v3.10: 预热列自愈
    try:
        from services.warmup_engine import _ensure_schema
        _ensure_schema()
    except Exception as e:
        import logging
        logging.getLogger("mira").warning(f"warmup schema warning: {e}")
    start_scheduler()


# ── API 路由（必须在静态文件挂载之前注册）──────────────────────────────────
app.include_router(auth_router,      prefix="/api/auth",      tags=["auth"])
app.include_router(accounts_router,  prefix="/api/accounts",  tags=["accounts"])
app.include_router(rules_router,     prefix="/api/rules",     tags=["rules"])
app.include_router(kpi_router,       prefix="/api/kpi",       tags=["kpi"])
app.include_router(settings_router,  prefix="/api/settings",  tags=["settings"])
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(logs_router,      prefix="/api/logs",      tags=["logs"])
app.include_router(op_tokens_router, prefix="/api/op-tokens", tags=["op-tokens"])
app.include_router(assets_router,    prefix="/api/assets",    tags=["assets"])
app.include_router(ad_templates_router, prefix="/api/ad-templates", tags=["ad-templates"])
app.include_router(storage_router,       prefix="/api/storage",       tags=["storage"])
app.include_router(users_router,        prefix="/api/users",        tags=["users"])
app.include_router(teams_router,        prefix="/api/teams",        tags=["teams"])
app.include_router(admin_router,       prefix="/api/admin",       tags=["admin"])
app.include_router(mirror_router,      prefix="/api/mirror",     tags=["mirror"])
app.include_router(warmup_router,      prefix="/api/warmup",     tags=["warmup"])
app.include_router(ad_ops_router,      prefix="/api/ad-ops",     tags=["ad-ops"])
app.include_router(landing_pages_router, prefix="/api/landing-pages", tags=["landing-pages"])
app.include_router(meta_oauth_router,  prefix="/api/meta-oauth", tags=["meta-oauth"])
app.include_router(settings_router,  prefix="/api/system",   tags=["system"])


# ── 健康检查（独立路由，不被 catch_all 拦截）──────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION}


# ── 前端静态文件服务（放在最后，避免拦截 API 路由）─────────────────────────
FRONTEND = "/opt/mira/frontend"


def _index_response():
    return FileResponse(
        os.path.join(FRONTEND, "index.html"),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/")
async def root():
    return _index_response()


# 静态资源（CSS/JS/图片等）
@app.get("/favicon.ico")
async def favicon():
    fp = os.path.join(FRONTEND, "favicon.ico")
    if os.path.exists(fp):
        return FileResponse(fp)
    return _index_response()


# SPA 兜底路由：只处理非 /api 路径
@app.get("/{path:path}")
async def catch_all(path: str):
    # API 路径不应该到达这里，但以防万一返回 404 而不是 HTML
    if path.startswith("api/") or path.startswith("api"):
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    fp = os.path.join(FRONTEND, path)
    fp = os.path.realpath(fp)
    if not fp.startswith(os.path.realpath(FRONTEND) + os.sep):
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    if os.path.exists(fp) and os.path.isfile(fp):
        return FileResponse(fp)
    return _index_response()
