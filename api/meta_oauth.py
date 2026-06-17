import html
import json
import secrets
import time
from typing import Optional
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.accounts import (
    ACCOUNT_DETAIL_FIELDS,
    _build_token_permission_snapshot,
    _ensure_fb_token_permission_columns,
    _fetch_all_fb_adaccounts,
    _verify_fb_token,
)
from core.auth import get_current_user, is_superadmin, normalize_user_claims
from core.database import decrypt_token, encrypt_token, get_conn
from core.tenancy import is_operator_user, team_id_for_create, user_id
from services.token_manager import (
    TOKEN_SOURCE_OAUTH_USER,
    ensure_token_source_columns,
)

router = APIRouter()

DEFAULT_GRAPH_VERSION = "v22.0"
DEFAULT_SCOPES = (
    "public_profile,ads_read,ads_management,business_management,"
    "pages_show_list,pages_manage_ads"
)
STATE_TTL_SECONDS = 15 * 60


class MetaOAuthConfigIn(BaseModel):
    name: Optional[str] = None
    app_id: Optional[str] = None
    app_secret: Optional[str] = None
    redirect_uri: Optional[str] = None
    scopes: Optional[str] = None
    graph_version: Optional[str] = None


class MetaOAuthConnectIn(BaseModel):
    token_alias: Optional[str] = None
    token_type: str = "operate"
    scopes: Optional[str] = None
    force_reauth: bool = True


def _now_cst_expr() -> str:
    return "datetime('now','+8 hours')"


def _ensure_schema(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS meta_oauth_configs (
           config_scope TEXT PRIMARY KEY,
           team_id INTEGER,
           name TEXT,
           app_id TEXT,
           app_secret_enc TEXT,
           redirect_uri TEXT,
           scopes TEXT,
           graph_version TEXT DEFAULT 'v22.0',
           created_by INTEGER,
           created_at TEXT DEFAULT (datetime('now','+8 hours')),
           updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS meta_oauth_states (
           state TEXT PRIMARY KEY,
           config_scope TEXT,
           user_id INTEGER,
           username TEXT,
           role TEXT,
           team_id INTEGER,
           owner_user_id INTEGER,
           token_alias TEXT,
           token_type TEXT DEFAULT 'operate',
           scopes TEXT,
           redirect_uri TEXT,
           status TEXT DEFAULT 'pending',
           token_id INTEGER,
           error TEXT,
           created_at TEXT DEFAULT (datetime('now','+8 hours')),
           expires_at INTEGER,
           completed_at TEXT
        )"""
    )
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(meta_oauth_states)").fetchall()}
    if "config_scope" not in cols:
        conn.execute("ALTER TABLE meta_oauth_states ADD COLUMN config_scope TEXT")
    if "redirect_uri" not in cols:
        conn.execute("ALTER TABLE meta_oauth_states ADD COLUMN redirect_uri TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_oauth_states_status ON meta_oauth_states(status, expires_at)")
    conn.commit()


def _role(user: dict) -> str:
    return normalize_user_claims(user).get("role") or "viewer"


def _can_configure(user: dict) -> bool:
    return _role(user) in ("superadmin", "admin")


def _scope_for_user(user: dict) -> tuple[str, Optional[int]]:
    if is_superadmin(user):
        return "global", None
    team_id = team_id_for_create(user)
    return f"team:{team_id}", team_id


def _fallback_config_row(conn, user: dict):
    scope, _team_id = _scope_for_user(user)
    row = conn.execute("SELECT * FROM meta_oauth_configs WHERE config_scope=?", (scope,)).fetchone()
    if row:
        return row, scope, False
    if scope != "global":
        global_row = conn.execute("SELECT * FROM meta_oauth_configs WHERE config_scope='global'").fetchone()
        if global_row:
            return global_row, "global", True
    return None, scope, False


def _config_row_by_scope(conn, config_scope: str):
    row = conn.execute("SELECT * FROM meta_oauth_configs WHERE config_scope=?", (config_scope,)).fetchone()
    if not row:
        raise HTTPException(status_code=400, detail="Meta OAuth config not found")
    return row


def _decrypt_secret(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        return decrypt_token(value)
    except Exception:
        return value


def _clean_graph_version(value: Optional[str]) -> str:
    raw = str(value or DEFAULT_GRAPH_VERSION).strip()
    if not raw:
        return DEFAULT_GRAPH_VERSION
    raw = raw.lower().lstrip("/")
    if raw.startswith("v"):
        return raw
    return "v" + raw


def _clean_scopes(value: Optional[str]) -> str:
    raw = str(value or DEFAULT_SCOPES).replace("\n", ",").strip()
    scopes = []
    seen = set()
    for part in raw.split(","):
        scope = part.strip()
        if scope and scope not in seen:
            seen.add(scope)
            scopes.append(scope)
    return ",".join(scopes) or DEFAULT_SCOPES


def _default_redirect_uri(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}/api/meta-oauth/callback"


def _public_config(row, request: Request, inherited: bool, effective_scope: str, current_scope: str) -> dict:
    redirect_uri = (row["redirect_uri"] if row else "") or _default_redirect_uri(request)
    return {
        "configured": bool(row and row["app_id"] and row["app_secret_enc"]),
        "name": (row["name"] if row else "") or "",
        "app_id": (row["app_id"] if row else "") or "",
        "has_app_secret": bool(row and row["app_secret_enc"]),
        "redirect_uri": redirect_uri,
        "scopes": (row["scopes"] if row else "") or DEFAULT_SCOPES,
        "graph_version": (row["graph_version"] if row else "") or DEFAULT_GRAPH_VERSION,
        "effective_scope": effective_scope,
        "current_scope": current_scope,
        "inherited_from_global": inherited,
    }


def _has_granted_scope(snapshot: dict, scope: str) -> bool:
    if not isinstance(snapshot, dict):
        return False
    status = snapshot.get("permission_status") or {}
    if str(status.get(scope) or "").lower() == "granted":
        return True
    granted = snapshot.get("granted_permissions") or []
    return scope in granted


def _owner_id_for_oauth(user: dict) -> Optional[int]:
    if is_operator_user(user):
        return user_id(user)
    return None


def _link_existing_accounts(conn, token_id: int, access_token: str, team_id: Optional[int], owner_user_id: Optional[int]) -> dict:
    fb_accounts = _fetch_all_fb_adaccounts(access_token, ACCOUNT_DETAIL_FIELDS, timeout=30)
    fb_ids = {item.get("id") for item in fb_accounts if item.get("id")}
    where = []
    params = []
    if team_id is None:
        where.append("team_id IS NULL")
    else:
        where.append("team_id=?")
        params.append(team_id)
    if owner_user_id is not None:
        where.append("owner_user_id=?")
        params.append(owner_user_id)
    imported = conn.execute(
        f"SELECT id, act_id, account_status FROM accounts WHERE {' AND '.join(where)}",
        params,
    ).fetchall()

    matched = 0
    already = 0
    status_updated = 0
    fb_map = {item.get("id"): item for item in fb_accounts if item.get("id")}
    for acc in imported:
        act_id = acc["act_id"]
        fb_info = fb_map.get(act_id)
        if not fb_info:
            continue
        exists = conn.execute(
            "SELECT id, status FROM account_op_tokens WHERE act_id=? AND token_id=?",
            (act_id, token_id),
        ).fetchone()
        if exists:
            already += 1
            if exists["status"] != "active":
                conn.execute(
                    "UPDATE account_op_tokens SET status='active', note=? WHERE id=?",
                    ("Meta OAuth授权自动恢复", exists["id"]),
                )
            continue
        max_pri = conn.execute(
            "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?",
            (act_id,),
        ).fetchone()[0] or 0
        conn.execute(
            f"""INSERT INTO account_op_tokens
               (act_id, token_id, priority, status, note, token_type, created_at)
               VALUES (?, ?, ?, 'active', 'Meta OAuth授权自动匹配',
                       (SELECT token_type FROM fb_tokens WHERE id=?), {_now_cst_expr()})""",
            (act_id, token_id, max_pri + 1, token_id),
        )
        matched += 1
        new_status = fb_info.get("account_status", acc["account_status"])
        if new_status != acc["account_status"]:
            conn.execute(
                "UPDATE accounts SET account_status=?, updated_at=datetime('now','+8 hours') WHERE id=?",
                (new_status, acc["id"]),
            )
            status_updated += 1
    return {
        "fb_total": len(fb_accounts),
        "imported_total": len(imported),
        "matched": matched,
        "already_linked": already,
        "status_updated": status_updated,
    }


@router.get("/config")
def get_meta_oauth_config(request: Request, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _ensure_schema(conn)
        current_scope, _team_id = _scope_for_user(user)
        row, effective_scope, inherited = _fallback_config_row(conn, user)
        data = _public_config(row, request, inherited, effective_scope, current_scope)
        data["can_configure"] = _can_configure(user)
        data["role"] = _role(user)
        return data
    finally:
        conn.close()


@router.patch("/config")
def save_meta_oauth_config(body: MetaOAuthConfigIn, request: Request, user=Depends(get_current_user)):
    if not _can_configure(user):
        raise HTTPException(status_code=403, detail="Only superadmin or team admin can configure Meta OAuth")
    conn = get_conn()
    try:
        _ensure_schema(conn)
        config_scope, team_id = _scope_for_user(user)
        existing = conn.execute(
            "SELECT * FROM meta_oauth_configs WHERE config_scope=?",
            (config_scope,),
        ).fetchone()

        app_id = (body.app_id or (existing["app_id"] if existing else "") or "").strip()
        name = (body.name or (existing["name"] if existing else "") or "Meta 官方授权").strip()
        redirect_uri = (body.redirect_uri or (existing["redirect_uri"] if existing else "") or _default_redirect_uri(request)).strip()
        scopes = _clean_scopes(body.scopes or (existing["scopes"] if existing else None))
        graph_version = _clean_graph_version(body.graph_version or (existing["graph_version"] if existing else None))
        if not app_id:
            raise HTTPException(status_code=400, detail="App ID is required")
        if body.app_secret and body.app_secret.strip():
            app_secret_enc = encrypt_token(body.app_secret.strip())
        elif existing and existing["app_secret_enc"]:
            app_secret_enc = existing["app_secret_enc"]
        else:
            raise HTTPException(status_code=400, detail="App Secret is required")

        conn.execute(
            f"""INSERT INTO meta_oauth_configs
               (config_scope, team_id, name, app_id, app_secret_enc, redirect_uri,
                scopes, graph_version, created_by, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {_now_cst_expr()})
               ON CONFLICT(config_scope) DO UPDATE SET
                 team_id=excluded.team_id,
                 name=excluded.name,
                 app_id=excluded.app_id,
                 app_secret_enc=excluded.app_secret_enc,
                 redirect_uri=excluded.redirect_uri,
                 scopes=excluded.scopes,
                 graph_version=excluded.graph_version,
                 updated_at={_now_cst_expr()}""",
            (
                config_scope,
                team_id,
                name,
                app_id,
                app_secret_enc,
                redirect_uri,
                scopes,
                graph_version,
                user.get("uid"),
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM meta_oauth_configs WHERE config_scope=?", (config_scope,)).fetchone()
        return _public_config(row, request, False, config_scope, config_scope)
    finally:
        conn.close()


@router.post("/connect-url")
def create_meta_oauth_connect_url(body: MetaOAuthConnectIn, request: Request, user=Depends(get_current_user)):
    if _role(user) not in ("superadmin", "admin", "operator"):
        raise HTTPException(status_code=403, detail="Operator permission required")
    token_type = str(body.token_type or "operate").strip().lower()
    if token_type not in ("operate", "manage", "user"):
        raise HTTPException(status_code=400, detail="token_type must be operate, manage or user")

    conn = get_conn()
    try:
        _ensure_schema(conn)
        current_scope, team_id = _scope_for_user(user)
        row, effective_scope, inherited = _fallback_config_row(conn, user)
        if not row or not row["app_id"] or not row["app_secret_enc"]:
            raise HTTPException(status_code=400, detail="Meta OAuth is not configured")
        scopes = _clean_scopes(body.scopes or row["scopes"])
        redirect_uri = (row["redirect_uri"] or _default_redirect_uri(request)).strip()
        graph_version = _clean_graph_version(row["graph_version"])
        state = secrets.token_urlsafe(32)
        expires_at = int(time.time() + STATE_TTL_SECONDS)
        owner_user_id = _owner_id_for_oauth(user)
        alias = (body.token_alias or "").strip()
        if not alias:
            alias = f"Meta OAuth - {user.get('username') or 'user'}"
        conn.execute(
            """INSERT INTO meta_oauth_states
               (state, config_scope, user_id, username, role, team_id, owner_user_id,
                token_alias, token_type, scopes, redirect_uri, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                state,
                effective_scope,
                user.get("uid"),
                user.get("username"),
                _role(user),
                team_id,
                owner_user_id,
                alias,
                token_type,
                scopes,
                redirect_uri,
                expires_at,
            ),
        )
        conn.commit()
        params = {
            "client_id": row["app_id"],
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": scopes,
            "response_type": "code",
        }
        if body.force_reauth:
            params["auth_type"] = "rerequest"
        auth_url = f"https://www.facebook.com/{graph_version}/dialog/oauth?{urlencode(params)}"
        return {
            "auth_url": auth_url,
            "state": state,
            "expires_in": STATE_TTL_SECONDS,
            "redirect_uri": redirect_uri,
            "inherited_from_global": inherited,
            "current_scope": current_scope,
            "effective_scope": effective_scope,
        }
    finally:
        conn.close()


def _oauth_html(title: str, message: str, ok: bool = False, payload: Optional[dict] = None) -> HTMLResponse:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    color = "#0f766e" if ok else "#b91c1c"
    bg = "#ecfdf5" if ok else "#fef2f2"
    border = "#99f6e4" if ok else "#fecaca"
    script = ""
    if ok:
        script = f"""
        <script>
        try {{
          if (window.opener) window.opener.postMessage(Object.assign({payload_json}, {{type:'mira_meta_oauth_success'}}), '*');
        }} catch (e) {{}}
        setTimeout(function() {{ try {{ window.close(); }} catch(e) {{}} }}, 1800);
        </script>
        """
    return HTMLResponse(
        f"""<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(title)}</title>
        <style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f6f7fb;margin:0;display:grid;place-items:center;min-height:100vh;color:#111827}}.box{{width:min(520px,92vw);background:#fff;border:1px solid #e5e7eb;border-radius:18px;padding:28px;box-shadow:0 18px 60px rgba(15,23,42,.12)}}.msg{{background:{bg};border:1px solid {border};color:{color};border-radius:12px;padding:14px 16px;line-height:1.7;white-space:pre-wrap}}h1{{font-size:22px;margin:0 0 14px}}p{{color:#6b7280;font-size:13px}}</style>
        </head><body><div class="box"><h1>{html.escape(title)}</h1><div class="msg">{html.escape(message)}</div><p>可以关闭此窗口并返回 Mira。</p></div>{script}</body></html>"""
    )


@router.get("/callback", name="meta_oauth_callback")
def meta_oauth_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
    error_message: Optional[str] = None,
    error_code: Optional[str] = None,
):
    if not state:
        if error or error_description or error_message or error_code:
            parts = [p for p in (error_message, error_description, error, f"code={error_code}" if error_code else "") if p]
            return _oauth_html("授权失败", "Meta 返回授权错误：\n" + "\n".join(parts))
        return _oauth_html("授权失败", "缺少 state，无法确认请求来源。")

    conn = get_conn()
    try:
        _ensure_schema(conn)
        st = conn.execute("SELECT * FROM meta_oauth_states WHERE state=?", (state,)).fetchone()
        if not st:
            return _oauth_html("授权失败", "授权状态不存在或已过期，请回到 Mira 重新发起授权。")
        if st["status"] != "pending":
            return _oauth_html("授权已处理", "该授权请求已经处理过，请回到 Mira 查看 Token。", ok=st["status"] == "completed")
        if int(st["expires_at"] or 0) < int(time.time()):
            conn.execute(
                f"UPDATE meta_oauth_states SET status='expired', error=?, completed_at={_now_cst_expr()} WHERE state=?",
                ("OAuth state expired", state),
            )
            conn.commit()
            return _oauth_html("授权超时", "授权链接已超过 15 分钟，请回到 Mira 重新生成。")
        if error:
            msg = error_message or error_description or error
            conn.execute(
                f"UPDATE meta_oauth_states SET status='failed', error=?, completed_at={_now_cst_expr()} WHERE state=?",
                (msg, state),
            )
            conn.commit()
            return _oauth_html("授权被取消", msg)
        if not code:
            return _oauth_html("授权失败", "Meta 没有返回授权 code。")

        cfg = _config_row_by_scope(conn, st["config_scope"] or "global")
        app_secret = _decrypt_secret(cfg["app_secret_enc"])
        graph_version = _clean_graph_version(cfg["graph_version"])
        redirect_uri = st["redirect_uri"] or cfg["redirect_uri"] or _default_redirect_uri(request)
        token_resp = requests.get(
            f"https://graph.facebook.com/{graph_version}/oauth/access_token",
            params={
                "client_id": cfg["app_id"],
                "client_secret": app_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            },
            timeout=20,
        )
        token_data = token_resp.json()
        if token_resp.status_code >= 400 or "error" in token_data:
            raise RuntimeError((token_data.get("error") or {}).get("message") or "OAuth token exchange failed")
        access_token = token_data.get("access_token")
        if not access_token:
            raise RuntimeError("Meta did not return access_token")

        long_resp = requests.get(
            f"https://graph.facebook.com/{graph_version}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": cfg["app_id"],
                "client_secret": app_secret,
                "fb_exchange_token": access_token,
            },
            timeout=20,
        )
        long_data = long_resp.json()
        if long_resp.status_code < 400 and "error" not in long_data and long_data.get("access_token"):
            access_token = long_data["access_token"]
        expires_in = long_data.get("expires_in") or token_data.get("expires_in")

        ok, info = _verify_fb_token(access_token)
        if not ok:
            raise RuntimeError(f"Token verify failed: {info}")
        snapshot = _build_token_permission_snapshot(access_token, info if isinstance(info, dict) else None)
        token_type = (st["token_type"] or "operate").strip().lower()
        if token_type == "operate" and not _has_granted_scope(snapshot, "ads_management"):
            msg = "授权成功，但没有授予 ads_management，不能作为铺广告/改预算/关停的操作号。请确认 Meta App 权限审核和授权勾选后重试。"
            conn.execute(
                f"UPDATE meta_oauth_states SET status='failed', error=?, completed_at={_now_cst_expr()} WHERE state=?",
                (msg, state),
            )
            conn.commit()
            return _oauth_html("授权权限不足", msg)

        ensure_token_source_columns(conn)
        _ensure_fb_token_permission_columns(conn)
        enc = encrypt_token(access_token)
        note_parts = [
            "Meta OAuth官方授权",
            f"授权用户:{st['username'] or st['user_id'] or '-'}",
        ]
        if expires_in:
            note_parts.append(f"expires_in:{expires_in}")
        cursor = conn.execute(
            f"""INSERT INTO fb_tokens (
                   token_alias, access_token_enc, token_type, token_source, status,
                   last_verified_at, note, matrix_id, permission_snapshot, permission_checked_at,
                   team_id, owner_user_id
               ) VALUES (?, ?, ?, ?, 'active', {_now_cst_expr()}, ?, NULL, ?, {_now_cst_expr()}, ?, ?)""",
            (
                st["token_alias"] or "Meta OAuth授权",
                enc,
                token_type,
                TOKEN_SOURCE_OAUTH_USER,
                "；".join(note_parts),
                json.dumps(snapshot, ensure_ascii=False),
                st["team_id"],
                st["owner_user_id"],
            ),
        )
        token_id = cursor.lastrowid
        match_result = {}
        if token_type in ("operate", "manage"):
            match_result = _link_existing_accounts(conn, token_id, access_token, st["team_id"], st["owner_user_id"])
        conn.execute(
            f"UPDATE meta_oauth_states SET status='completed', token_id=?, completed_at={_now_cst_expr()} WHERE state=?",
            (token_id, state),
        )
        conn.commit()
        message = (
            f"Meta 官方授权完成。\nToken ID: {token_id}\n"
            f"已扫描 FB 账户: {match_result.get('fb_total', 0)} 个\n"
            f"自动关联已导入账户: {match_result.get('matched', 0)} 个"
        )
        return _oauth_html(
            "授权成功",
            message,
            ok=True,
            payload={"token_id": token_id, "matched": match_result.get("matched", 0), "fb_total": match_result.get("fb_total", 0)},
        )
    except Exception as exc:
        try:
            conn.execute(
                f"UPDATE meta_oauth_states SET status='failed', error=?, completed_at={_now_cst_expr()} WHERE state=?",
                (str(exc), state),
            )
            conn.commit()
        except Exception:
            pass
        return _oauth_html("授权失败", str(exc))
    finally:
        conn.close()
