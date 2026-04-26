from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os, time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from core.app_meta import APP_VERSION, get_allowed_origins
from core.database import init_db
from core.auth import get_current_user
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
from api.creative_gen import router as creative_gen_router
from api.storage import router as storage_router
from api.autopilot import router as autopilot_router
from api.users import router as users_router
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
        response = await call_next(request)
        response.headers["x-process-time"] = f"{time.time()-start:.4f}"
        return response


app = FastAPI(title="Mira Ads Guard", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TimingMiddleware)


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
app.include_router(creative_gen_router, prefix="/api/creative-gen", tags=["creative-gen"])
app.include_router(storage_router,       prefix="/api/storage",       tags=["storage"])
app.include_router(autopilot_router,    prefix="/api/autopilot",    tags=["autopilot"])
app.include_router(users_router,        prefix="/api/users",        tags=["users"])



# ── 健康检查（独立路由，不被 catch_all 拦截）──────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION}


# ── 前端静态文件服务（放在最后，避免拦截 API 路由）─────────────────────────
FRONTEND = "/opt/mira/frontend"


@app.get("/")
async def root():
    return FileResponse(os.path.join(FRONTEND, "index.html"))


# 静态资源（CSS/JS/图片等）
@app.get("/favicon.ico")
async def favicon():
    fp = os.path.join(FRONTEND, "favicon.ico")
    if os.path.exists(fp):
        return FileResponse(fp)
    return FileResponse(os.path.join(FRONTEND, "index.html"))


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
    return FileResponse(os.path.join(FRONTEND, "index.html"))
