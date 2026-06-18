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
BLOCKED_SCOPES = {
    "leads_retrieval": "leads_retrieval 当前容易被 Meta OAuth 判定为 Invalid Scopes；请先不要加入授权范围。Lead 表单创建使用 pages_manage_ads。",
}
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
    matrix_id: Optional[int] = None


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
           matrix_id INTEGER,
           scopes TEXT,
           redirect_uri TEXT,
           status TEXT DEFAULT 'pending',
           token_id INTEGER,
           match_result_json TEXT,
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
    if "matrix_id" not in cols:
        conn.execute("ALTER TABLE meta_oauth_states ADD COLUMN matrix_id INTEGER")
    if "match_result_json" not in cols:
        conn.execute("ALTER TABLE meta_oauth_states ADD COLUMN match_result_json TEXT")
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
    blocked = []
    for part in raw.split(","):
        scope = part.strip()
        if scope in BLOCKED_SCOPES:
            blocked.append(scope)
            continue
        if scope and scope not in seen:
            seen.add(scope)
            scopes.append(scope)
    if blocked:
        detail = "；".join(BLOCKED_SCOPES[s] for s in blocked)
        raise HTTPException(status_code=400, detail=detail)
    return ",".join(scopes) or DEFAULT_SCOPES


def _scope_parts(value: Optional[str]) -> list[str]:
    raw = str(value or DEFAULT_SCOPES).replace("\n", ",").strip()
    out = []
    seen = set()
    for part in raw.split(","):
        scope = part.strip()
        if scope and scope not in seen:
            seen.add(scope)
            out.append(scope)
    return out


def _check(status: str, key: str, label: str, detail: str) -> dict:
    return {"status": status, "key": key, "label": label, "detail": detail}


def _diagnose_app_credentials(app_id: str, app_secret: str, graph_version: str) -> dict:
    try:
        resp = requests.get(
            f"https://graph.facebook.com/{graph_version}/{app_id}",
            params={
                "fields": "id,name,link",
                "access_token": f"{app_id}|{app_secret}",
            },
            timeout=10,
        )
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        return _check("warn", "app_credentials", "App ID / Secret", f"Graph network check failed: {exc}")
    except ValueError:
        return _check("warn", "app_credentials", "App ID / Secret", f"Graph returned non-json response: HTTP {resp.status_code}")
    if resp.status_code >= 400 or (isinstance(data, dict) and data.get("error")):
        err = data.get("error") if isinstance(data, dict) else {}
        msg = (err or {}).get("message") or f"HTTP {resp.status_code}"
        return _check("fail", "app_credentials", "App ID / Secret", msg)
    name = data.get("name") if isinstance(data, dict) else ""
    return _check("pass", "app_credentials", "App ID / Secret", f"Graph can read App {name or app_id}")


def _diagnose_redirect_uri(redirect_uri: str) -> list[dict]:
    checks = []
    uri = (redirect_uri or "").strip()
    if not uri:
        return [_check("fail", "redirect_uri", "OAuth callback", "Redirect URI is empty")]
    if not uri.endswith("/api/meta-oauth/callback"):
        checks.append(_check("warn", "redirect_path", "OAuth callback", "Callback path should end with /api/meta-oauth/callback"))
    if not (uri.startswith("https://") or uri.startswith("http://localhost") or uri.startswith("http://127.0.0.1")):
        checks.append(_check("warn", "redirect_https", "OAuth callback", "Meta production OAuth should use HTTPS"))
    try:
        resp = requests.get(uri, timeout=8, allow_redirects=False)
        if resp.status_code < 500:
            checks.append(_check("pass", "redirect_reachable", "OAuth callback", f"Callback route is reachable: HTTP {resp.status_code}"))
        else:
            checks.append(_check("fail", "redirect_reachable", "OAuth callback", f"Callback returned HTTP {resp.status_code}"))
    except requests.exceptions.RequestException as exc:
        checks.append(_check("warn", "redirect_reachable", "OAuth callback", f"Server could not reach callback URL: {exc}"))
    return checks


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


def _act_num(act_id: str) -> str:
    raw = str(act_id or "").strip()
    return raw[4:] if raw.startswith("act_") else raw


def _oauth_match_item(act_id: str, name: Optional[str] = None, fb_info: Optional[dict] = None) -> dict:
    info = fb_info or {}
    label = name or info.get("name") or act_id
    return {
        "act_id": act_id,
        "act_num": _act_num(act_id),
        "name": label,
        "currency": info.get("currency") or "",
        "account_status": info.get("account_status"),
    }


def _limit_items(items: list[dict], limit: int = 30) -> list[dict]:
    return items[:limit]


def _link_existing_accounts(conn, token_id: int, access_token: str, team_id: Optional[int], owner_user_id: Optional[int]) -> dict:
    fb_accounts = _fetch_all_fb_adaccounts(access_token, ACCOUNT_DETAIL_FIELDS, timeout=30)
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
        f"SELECT id, act_id, name, account_status FROM accounts WHERE {' AND '.join(where)}",
        params,
    ).fetchall()

    matched = 0
    already = 0
    restored = 0
    status_updated = 0
    fb_map = {item.get("id"): item for item in fb_accounts if item.get("id")}
    imported_ids = {acc["act_id"] for acc in imported}
    matched_accounts = []
    already_accounts = []
    restored_accounts = []
    unmatched_imported = []
    for acc in imported:
        act_id = acc["act_id"]
        fb_info = fb_map.get(act_id)
        if not fb_info:
            unmatched_imported.append(_oauth_match_item(act_id, acc["name"]))
            continue
        exists = conn.execute(
            "SELECT id, status FROM account_op_tokens WHERE act_id=? AND token_id=?",
            (act_id, token_id),
        ).fetchone()
        if exists:
            if exists["status"] != "active":
                conn.execute(
                    "UPDATE account_op_tokens SET status='active', note=? WHERE id=?",
                    ("Meta OAuth授权自动恢复", exists["id"]),
                )
                restored += 1
                restored_accounts.append(_oauth_match_item(act_id, acc["name"], fb_info))
            else:
                already += 1
                already_accounts.append(_oauth_match_item(act_id, acc["name"], fb_info))
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
        matched_accounts.append(_oauth_match_item(act_id, acc["name"], fb_info))
        new_status = fb_info.get("account_status", acc["account_status"])
        if new_status != acc["account_status"]:
            conn.execute(
                "UPDATE accounts SET account_status=?, updated_at=datetime('now','+8 hours') WHERE id=?",
                (new_status, acc["id"]),
            )
            status_updated += 1
    fb_only_accounts = [
        _oauth_match_item(item.get("id"), item.get("name"), item)
        for item in fb_accounts
        if item.get("id") and item.get("id") not in imported_ids
    ]
    return {
        "fb_total": len(fb_accounts),
        "imported_total": len(imported),
        "matched": matched,
        "restored": restored,
        "already_linked": already,
        "status_updated": status_updated,
        "matched_accounts": _limit_items(matched_accounts),
        "restored_accounts": _limit_items(restored_accounts),
        "already_accounts": _limit_items(already_accounts),
        "unmatched_imported_count": len(unmatched_imported),
        "unmatched_imported_accounts": _limit_items(unmatched_imported),
        "fb_only_count": len(fb_only_accounts),
        "fb_only_accounts": _limit_items(fb_only_accounts),
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


@router.get("/diagnose")
def diagnose_meta_oauth_config(request: Request, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _ensure_schema(conn)
        current_scope, _team_id = _scope_for_user(user)
        row, effective_scope, inherited = _fallback_config_row(conn, user)
        data = _public_config(row, request, inherited, effective_scope, current_scope)
        checks = []
        checks.append(_check("pass" if row else "fail", "configured", "Meta App config", "Configured" if row else "No Meta App config found"))
        if inherited:
            checks.append(_check("warn", "scope", "Config scope", "Using global App config because this team has no dedicated config"))
        else:
            checks.append(_check("pass", "scope", "Config scope", f"Using {effective_scope} config"))

        app_id = (data.get("app_id") or "").strip()
        if app_id and app_id.isdigit():
            checks.append(_check("pass", "app_id", "App ID", app_id))
        elif app_id:
            checks.append(_check("warn", "app_id", "App ID", "App ID is present but does not look numeric"))
        else:
            checks.append(_check("fail", "app_id", "App ID", "Missing App ID"))

        has_secret = bool(row and row["app_secret_enc"])
        checks.append(_check("pass" if has_secret else "fail", "app_secret", "App Secret", "Saved on server" if has_secret else "Missing App Secret"))
        graph_version = _clean_graph_version(data.get("graph_version"))
        if graph_version.startswith("v") and "." in graph_version:
            checks.append(_check("pass", "graph_version", "Graph version", graph_version))
        else:
            checks.append(_check("warn", "graph_version", "Graph version", f"Unusual Graph version: {graph_version}"))

        scopes = _scope_parts(data.get("scopes"))
        blocked = [s for s in scopes if s in BLOCKED_SCOPES]
        required = {"ads_read", "ads_management"}
        recommended = {"business_management", "pages_show_list", "pages_manage_ads"}
        missing_required = sorted(required - set(scopes))
        missing_recommended = sorted(recommended - set(scopes))
        if blocked:
            detail = "；".join(BLOCKED_SCOPES[s] for s in blocked)
            checks.append(_check("fail", "scopes", "OAuth scopes", detail))
        elif missing_required:
            checks.append(_check("fail", "scopes", "OAuth scopes", "Missing required scopes: " + ", ".join(missing_required)))
        elif missing_recommended:
            checks.append(_check("warn", "scopes", "OAuth scopes", "Recommended scopes missing: " + ", ".join(missing_recommended)))
        else:
            checks.append(_check("pass", "scopes", "OAuth scopes", ", ".join(scopes)))

        checks.extend(_diagnose_redirect_uri(data.get("redirect_uri") or _default_redirect_uri(request)))
        if row and app_id and has_secret:
            app_secret = _decrypt_secret(row["app_secret_enc"])
            checks.append(_diagnose_app_credentials(app_id, app_secret, graph_version))

        recent = [
            dict(r)
            for r in conn.execute(
                """SELECT status, token_type, token_id, error, created_at, completed_at
                   FROM meta_oauth_states
                   WHERE user_id=?
                   ORDER BY created_at DESC LIMIT 8""",
                (user.get("uid"),),
            ).fetchall()
        ]
        return {
            "success": not any(c["status"] == "fail" for c in checks),
            "checks": checks,
            "recent": recent,
            "configured": data.get("configured"),
            "effective_scope": effective_scope,
            "current_scope": current_scope,
        }
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
        matrix_id = None
        if token_type == "operate" and body.matrix_id:
            try:
                matrix_id = int(body.matrix_id)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="matrix_id must be an integer")
            if matrix_id <= 0:
                matrix_id = None
        conn.execute(
            f"""UPDATE meta_oauth_states
               SET status='replaced', error='Replaced by a newer authorization link',
                   completed_at={_now_cst_expr()}
               WHERE status='pending' AND user_id=? AND token_type=?""",
            (user.get("uid"), token_type),
        )
        conn.execute(
            """INSERT INTO meta_oauth_states
               (state, config_scope, user_id, username, role, team_id, owner_user_id,
                token_alias, token_type, matrix_id, scopes, redirect_uri, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                matrix_id,
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
            "expires_at": expires_at,
            "redirect_uri": redirect_uri,
            "inherited_from_global": inherited,
            "current_scope": current_scope,
            "effective_scope": effective_scope,
        }
    finally:
        conn.close()


@router.get("/state/{state}")
def get_meta_oauth_state(state: str, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _ensure_schema(conn)
        st = conn.execute("SELECT * FROM meta_oauth_states WHERE state=?", (state,)).fetchone()
        if not st:
            raise HTTPException(status_code=404, detail="OAuth state not found")
        if st["user_id"] != user.get("uid") and not is_superadmin(user):
            raise HTTPException(status_code=403, detail="OAuth state belongs to another user")
        status = st["status"] or "pending"
        now = int(time.time())
        expired = bool(status == "pending" and int(st["expires_at"] or 0) < now)
        if expired:
            conn.execute(
                f"UPDATE meta_oauth_states SET status='expired', error=?, completed_at={_now_cst_expr()} WHERE state=?",
                ("OAuth state expired", state),
            )
            conn.commit()
            status = "expired"
        token = None
        linked_count = 0
        match_result = {}
        if st["token_id"]:
            token_row = conn.execute(
                "SELECT id, token_alias, token_type, token_source, status, matrix_id, created_at FROM fb_tokens WHERE id=?",
                (st["token_id"],),
            ).fetchone()
            if token_row:
                token = dict(token_row)
                linked_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM account_op_tokens WHERE token_id=? AND status='active'",
                        (st["token_id"],),
                    ).fetchone()[0]
                    or 0
                )
        if st["match_result_json"]:
            try:
                parsed = json.loads(st["match_result_json"])
                if isinstance(parsed, dict):
                    match_result = parsed
            except Exception:
                match_result = {}
        return {
            "success": status == "completed",
            "state": state,
            "status": status,
            "token_id": st["token_id"],
            "token": token,
            "linked_count": linked_count,
            "match_result": match_result,
            "error": st["error"],
            "created_at": st["created_at"],
            "completed_at": st["completed_at"],
            "expires_at": st["expires_at"],
            "expires_in": max(0, int(st["expires_at"] or 0) - now),
        }
    finally:
        conn.close()


def _oauth_failure_message(prefix: str, *parts: Optional[str]) -> str:
    details = [str(p).strip() for p in parts if p and str(p).strip()]
    text = "\n".join(details) if details else "Meta 没有返回具体错误。"
    lower = text.lower()
    hints = []
    if "invalid scopes" in lower or "invalid scope" in lower:
        hints.append("从授权范围移除 leads_retrieval，只保留 ads_read、ads_management、business_management、pages_show_list、pages_manage_ads。")
        hints.append("确认 Meta App 已添加 Facebook Login，并且 App 处于 Live；开发模式下该个号必须加入开发者或测试用户。")
    if "redirect" in lower and "uri" in lower:
        hints.append("检查 Meta App 的 Valid OAuth Redirect URIs，必须和 Mira 配置的回调地址完全一致。")
    if "app not active" in lower or "not available" in lower:
        hints.append("开发模式下只有开发者/测试用户可授权；给运营使用前需要 App Live 和权限审核。")
    if not hints:
        hints.append("回到 Mira 的 Meta 官方授权中心，点击“诊断配置”查看 App、回调地址和授权范围。")
    return prefix + "：\n" + text + "\n\n排查建议：\n- " + "\n- ".join(hints)


def _oauth_html(title: str, message: str, ok: bool = False, payload: Optional[dict] = None) -> HTMLResponse:
    payload = payload or {}
    payload_json = json.dumps(payload, ensure_ascii=False)
    color = "#0f766e" if ok else "#b91c1c"
    bg = "#ecfdf5" if ok else "#fef2f2"
    border = "#99f6e4" if ok else "#fecaca"
    badge = "授权完成" if ok else "需要处理"
    safe_title = html.escape(title)
    safe_message = html.escape(message)
    summary_html = ""
    if ok and payload:
        match = payload.get("match_result") or {}
        try:
            effective = int(match.get("matched") or payload.get("matched") or 0) + int(match.get("restored") or payload.get("restored") or 0) + int(match.get("already_linked") or payload.get("already_linked") or 0)
        except Exception:
            effective = 0
        stats = [
            ("Token ID", str(payload.get("token_id") or "--")),
            ("FB 可见账户", str(match.get("fb_total") or payload.get("fb_total") or 0)),
            ("已覆盖账户", str(effective)),
            ("系统已导入", str(match.get("imported_total") or 0)),
        ]
        summary_html = '<div class="stats">' + "".join(
            f'<div class="stat"><b>{html.escape(value)}</b><span>{html.escape(label)}</span></div>'
            for label, value in stats
        ) + "</div>"
        if effective:
            summary_html += '<div class="hint ok">授权 Token 已自动加入匹配到账户的 Token 池。回到 Mira 后可直接用于铺广告、预热、预算和关停。</div>'
        else:
            summary_html += '<div class="hint warn">授权成功，但暂未覆盖已导入账户。回到 Mira 后请点击“导入账户（自动匹配 Token）”，系统会自动补齐关联。</div>'
    script = ""
    if ok:
        script = f"""
        <script>
        try {{
          var hasOpener = !!(window.opener && !window.opener.closed);
          if (hasOpener) window.opener.postMessage(Object.assign({payload_json}, {{type:'mira_meta_oauth_success'}}), '*');
          if (hasOpener) setTimeout(function() {{ try {{ window.close(); }} catch(e) {{}} }}, 2600);
        }} catch (e) {{}}
        </script>
        """
    return HTMLResponse(
        f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{safe_title}</title>
        <style>
        *{{box-sizing:border-box}}body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:linear-gradient(180deg,#f7f8fb 0%,#eef2f7 100%);margin:0;display:grid;place-items:center;min-height:100vh;color:#111827;padding:24px}}
        .box{{width:min(720px,94vw);background:#fff;border:1px solid #e5e7eb;border-radius:24px;padding:30px;box-shadow:0 24px 80px rgba(15,23,42,.14)}}
        .top{{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:16px}}
        .brand{{font-size:13px;color:#6b7280;font-weight:700;letter-spacing:.02em}}.badge{{display:inline-flex;align-items:center;border-radius:999px;background:{bg};border:1px solid {border};color:{color};font-size:12px;font-weight:800;padding:6px 10px}}
        h1{{font-size:24px;line-height:1.25;margin:0 0 14px;color:#111827}}.msg{{background:{bg};border:1px solid {border};color:{color};border-radius:14px;padding:15px 16px;line-height:1.7;white-space:pre-wrap;font-size:14px}}
        .stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:16px 0 0}}.stat{{border:1px solid #e5e7eb;border-radius:14px;padding:12px;background:#fafafa}}.stat b{{display:block;font-size:20px;color:#111827;line-height:1.1}}.stat span{{display:block;color:#6b7280;font-size:12px;margin-top:5px}}
        .hint{{margin-top:12px;border-radius:14px;padding:12px 14px;font-size:13px;line-height:1.6}}.hint.ok{{background:#ecfdf5;color:#047857;border:1px solid #bbf7d0}}.hint.warn{{background:#fffbeb;color:#b45309;border:1px solid #fde68a}}
        p{{color:#6b7280;font-size:13px;line-height:1.6;margin:14px 0 0}}.actions{{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}}
        button,a.btn{{appearance:none;border:1px solid #d1d5db;border-radius:999px;background:#fff;color:#111827;padding:9px 16px;font-size:13px;font-weight:800;cursor:pointer;text-decoration:none}}
        button.primary,a.primary{{background:#0071e3;border-color:#0071e3;color:#fff}}button:hover,a.btn:hover{{filter:brightness(.98)}}
        @media(max-width:640px){{.stats{{grid-template-columns:repeat(2,minmax(0,1fr))}}.box{{padding:22px}}}}
        </style>
        </head><body><div class="box"><div class="top"><div class="brand">Mira · Meta OAuth</div><div class="badge">{badge}</div></div><h1>{safe_title}</h1><div class="msg">{safe_message}</div>{summary_html}<p>如果 Mira 授权中心还开着，结果会自动同步；复制到其他浏览器授权时，本页会保留结果，便于核对。</p><div class="actions"><button class="primary" onclick="window.close()">关闭窗口</button><a class="btn" href="/">返回 Mira</a></div></div>{script}</body></html>"""
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
            return _oauth_html(
                "授权失败",
                _oauth_failure_message(
                    "Meta 返回授权错误",
                    error_message,
                    error_description,
                    error,
                    f"code={error_code}" if error_code else "",
                ),
            )
        return _oauth_html("授权失败", "缺少 state，无法确认请求来源。请回到 Mira 重新生成授权链接。")

    conn = get_conn()
    try:
        _ensure_schema(conn)
        st = conn.execute("SELECT * FROM meta_oauth_states WHERE state=?", (state,)).fetchone()
        if not st:
            return _oauth_html("授权失败", "授权状态不存在或已过期，请回到 Mira 重新发起授权。")
        if st["status"] != "pending":
            status = st["status"]
            if status == "completed":
                return _oauth_html("授权已完成", "该授权请求已经完成，请回到 Mira 查看 Token。", ok=True)
            if status == "replaced":
                return _oauth_html("授权链接已失效", "你已经生成了新的授权链接，请使用最新链接授权。")
            if status == "expired":
                return _oauth_html("授权超时", "授权链接已超过 15 分钟，请回到 Mira 重新生成。")
            if status == "failed":
                return _oauth_html("授权失败", st["error"] or "该授权请求已失败，请回到 Mira 重新生成。")
            return _oauth_html("授权已处理", "该授权请求已经处理过，请回到 Mira 查看 Token。")
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
            return _oauth_html("授权被取消", _oauth_failure_message("Meta 返回授权错误", msg))
        if not code:
            return _oauth_html("授权失败", "Meta 没有返回授权 code，请回到 Mira 重新生成授权链接。")

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
            msg = "授权成功，但没有授予 ads_management，不能作为铺广告、改预算或关停广告的操作号。请确认 Meta App 权限审核和授权勾选后重试。"
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
            "Meta OAuth 官方授权",
            f"授权用户:{st['username'] or st['user_id'] or '-'}",
        ]
        if expires_in:
            note_parts.append(f"expires_in:{expires_in}")
        cursor = conn.execute(
            f"""INSERT INTO fb_tokens (
                   token_alias, access_token_enc, token_type, token_source, status,
                   last_verified_at, note, matrix_id, permission_snapshot, permission_checked_at,
                   team_id, owner_user_id
               ) VALUES (?, ?, ?, ?, 'active', {_now_cst_expr()}, ?, ?, ?, {_now_cst_expr()}, ?, ?)""",
            (
                st["token_alias"] or "Meta OAuth 授权",
                enc,
                token_type,
                TOKEN_SOURCE_OAUTH_USER,
                "；".join(note_parts),
                st["matrix_id"] if token_type == "operate" else None,
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
            f"UPDATE meta_oauth_states SET status='completed', token_id=?, match_result_json=?, completed_at={_now_cst_expr()} WHERE state=?",
            (token_id, json.dumps(match_result, ensure_ascii=False), state),
        )
        conn.commit()
        message = (
            f"Meta 官方授权完成。\nToken ID: {token_id}\n"
            f"已扫描 FB 账户: {match_result.get('fb_total', 0)} 个\n"
            f"新增关联: {match_result.get('matched', 0)} 个\n"
            f"恢复关联: {match_result.get('restored', 0)} 个\n"
            f"已有可用关联: {match_result.get('already_linked', 0)} 个"
        )
        return _oauth_html(
            "授权成功",
            message,
            ok=True,
            payload={
                "token_id": token_id,
                "matched": match_result.get("matched", 0),
                "restored": match_result.get("restored", 0),
                "already_linked": match_result.get("already_linked", 0),
                "fb_total": match_result.get("fb_total", 0),
                "match_result": match_result,
            },
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
