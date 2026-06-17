import hashlib
import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.auth import get_current_user, is_superadmin
from core.database import decrypt_token, encrypt_token, get_conn, mask_token
from core.tenancy import assert_row_access, is_operator_user, team_id_for_create, user_id
from services.landing_publisher import (
    DEFAULT_TEMPLATE_DIR,
    CloudflareError,
    deploy_pages_static,
    list_pages_projects,
    prepare_template,
    sanitize_project_name,
    verify_token_and_accounts,
)


logger = logging.getLogger("mira.landing_pages")
router = APIRouter()
CST = timezone(timedelta(hours=8))


class CloudflareTokenCreate(BaseModel):
    name: str
    api_token: str
    team_id: Optional[int] = None


class CloudflareTokenAccountPatch(BaseModel):
    account_id: str


class LandingPublishReq(BaseModel):
    token_id: int
    template_id: int = 1
    title: str
    project_name: Optional[str] = None
    pixel_id: Optional[str] = ""
    target_urls: list[str] = []
    rotation_mode: str = "sequential"
    link_kind: str = "landing"
    form_link_enabled: bool = False
    note: Optional[str] = ""
    bind_act_ids: list[str] = []
    bind_target: str = "none"
    tracking_enabled: bool = True
    protection_enabled: bool = False
    protection_rules: dict[str, Any] = Field(default_factory=dict)


class LandingEventIngest(BaseModel):
    page_id: int
    secret: str
    event_type: str
    decision: Optional[str] = None
    reason: Optional[str] = None
    path: Optional[str] = None
    target_url: Optional[str] = None
    referrer: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    colo: Optional[str] = None
    asn: Optional[str] = None
    platform: Optional[str] = None
    device_type: Optional[str] = None
    browser: Optional[str] = None
    os: Optional[str] = None
    user_agent: Optional[str] = None
    ip_hash: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def _ensure_schema():
    conn = get_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cf_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            access_token_enc TEXT NOT NULL,
            token_mask TEXT,
            cf_accounts_json TEXT DEFAULT '[]',
            selected_account_id TEXT,
            cf_account_id TEXT,
            cf_account_name TEXT,
            status TEXT DEFAULT 'active',
            last_verified_at TEXT,
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            template_path TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            link_kind TEXT DEFAULT 'landing',
            form_link_enabled INTEGER DEFAULT 0,
            template_id INTEGER DEFAULT 1,
            cf_token_id INTEGER,
            cf_account_id TEXT,
            cf_account_name TEXT,
            project_name TEXT,
            deployment_id TEXT,
            pages_url TEXT,
            custom_domain TEXT,
            pixel_id TEXT,
            target_urls TEXT,
            rotation_mode TEXT DEFAULT 'sequential',
            bound_act_ids TEXT DEFAULT '[]',
            bind_target TEXT DEFAULT 'none',
            tracking_enabled INTEGER DEFAULT 1,
            protection_enabled INTEGER DEFAULT 0,
            protection_rules TEXT DEFAULT '{}',
            ingest_secret TEXT,
            worker_enabled INTEGER DEFAULT 0,
            status TEXT DEFAULT 'draft',
            last_error TEXT,
            raw_response TEXT,
            note TEXT,
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            decision TEXT,
            reason TEXT,
            path TEXT,
            target_url TEXT,
            referrer TEXT,
            country TEXT,
            region TEXT,
            city TEXT,
            colo TEXT,
            asn TEXT,
            platform TEXT,
            device_type TEXT,
            browser TEXT,
            os TEXT,
            user_agent_hash TEXT,
            ip_hash TEXT,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours'))
        );
        CREATE INDEX IF NOT EXISTS idx_landing_events_page_created ON landing_events(page_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_landing_events_type ON landing_events(page_id, event_type);
        """
    )
    try:
        page_cols = {r["name"] for r in conn.execute("PRAGMA table_info(landing_pages)").fetchall()}
        page_alters = {
            "template_id": "INTEGER DEFAULT 1",
            "bound_act_ids": "TEXT DEFAULT '[]'",
            "bind_target": "TEXT DEFAULT 'none'",
            "tracking_enabled": "INTEGER DEFAULT 1",
            "protection_enabled": "INTEGER DEFAULT 0",
            "protection_rules": "TEXT DEFAULT '{}'",
            "ingest_secret": "TEXT",
            "worker_enabled": "INTEGER DEFAULT 0",
        }
        for name, ddl in page_alters.items():
            if name not in page_cols:
                conn.execute(f"ALTER TABLE landing_pages ADD COLUMN {name} {ddl}")
    except Exception:
        logger.exception("landing_pages schema patch failed")
    try:
        token_cols = {r["name"] for r in conn.execute("PRAGMA table_info(cf_tokens)").fetchall()}
        if "cf_accounts_json" not in token_cols:
            conn.execute("ALTER TABLE cf_tokens ADD COLUMN cf_accounts_json TEXT DEFAULT '[]'")
        if "selected_account_id" not in token_cols:
            conn.execute("ALTER TABLE cf_tokens ADD COLUMN selected_account_id TEXT")
    except Exception:
        logger.exception("cf_tokens schema patch failed")
    row = conn.execute("SELECT id FROM landing_templates WHERE id=1").fetchone()
    if not row and DEFAULT_TEMPLATE_DIR.exists():
        conn.execute(
            """INSERT INTO landing_templates
               (id, name, template_path, status, created_by)
               VALUES (1, 'RH FP 高级默认模板', ?, 'active', 'system')""",
            (str(DEFAULT_TEMPLATE_DIR),),
        )
    else:
        conn.execute(
            "UPDATE landing_templates SET name='RH FP 高级默认模板' WHERE id=1 AND COALESCE(created_by,'system')='system'"
        )
    conn.commit()
    conn.close()


_ensure_schema()


def _now_cst() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def _truncate(value: Optional[str], limit: int = 255) -> str:
    if value is None:
        return ""
    return str(value).strip()[:limit]


def _json_loads(raw: Optional[str], default):
    try:
        return json.loads(raw or "")
    except Exception:
        return default


def _normalize_cf_accounts(accounts):
    if isinstance(accounts, dict) and "result" in accounts:
        accounts = accounts.get("result") or []
    if not isinstance(accounts, list):
        return []
    output = []
    for acct in accounts:
        if not isinstance(acct, dict):
            continue
        aid = acct.get("id")
        name = acct.get("name")
        if isinstance(aid, str) and aid.strip():
            output.append({"id": aid.strip(), "name": (name.strip() if isinstance(name, str) and name.strip() else aid.strip())})
    return output


def _public_accounts(raw: Optional[str]):
    return _json_loads(raw, [])


def _resolve_token_account(row: dict) -> tuple[Optional[str], Optional[str]]:
    selected = (row.get("selected_account_id") or "").strip() if row else ""
    accounts = _public_accounts(row.get("cf_accounts_json") if row else None)
    if selected:
        for acct in accounts:
            if isinstance(acct, dict) and acct.get("id") == selected:
                return selected, (acct.get("name") or selected)
    if accounts:
        first = accounts[0]
        return first.get("id"), first.get("name")
    return (row.get("cf_account_id"), row.get("cf_account_name")) if row else (None, None)


def _clean_act_ids(values: list[str]) -> list[str]:
    output = []
    seen = set()
    for value in values or []:
        raw = (value or "").strip()
        if raw.startswith("act_"):
            raw = raw[4:]
        raw = "".join(ch for ch in raw if ch.isdigit())
        if raw and raw not in seen:
            seen.add(raw)
            output.append(raw)
    return output


def _scope_where(user, alias: str = "") -> tuple[list[str], list]:
    prefix = f"{alias}." if alias else ""
    where, params = [], []
    if is_superadmin(user):
        return where, params
    team_id = team_id_for_create(user)
    where.append(f"{prefix}team_id=?")
    params.append(team_id)
    if is_operator_user(user):
        where.append(f"({prefix}owner_user_id=? OR {prefix}owner_user_id IS NULL)")
        params.append(user_id(user))
    return where, params


def _stamp(user, requested_team_id: Optional[int] = None) -> tuple[Optional[int], Optional[int]]:
    if is_superadmin(user):
        return requested_team_id, None
    tid = team_id_for_create(user)
    owner = user_id(user) if is_operator_user(user) else None
    return tid, owner


def _ingest_url() -> str:
    raw = (
        os.environ.get("MIRA_LANDING_INGEST_URL")
        or os.environ.get("MIRA_PUBLIC_BASE_URL")
        or os.environ.get("PUBLIC_BASE_URL")
        or "http://43.129.230.237:8000"
    ).strip().rstrip("/")
    if raw.endswith("/api/landing-pages/events/ingest"):
        return raw
    return raw + "/api/landing-pages/events/ingest"


def _assert_token_access(conn, token_id: int, user) -> dict:
    row = conn.execute("SELECT * FROM cf_tokens WHERE id=?", (token_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Cloudflare API token not found")
    if is_superadmin(user):
        return dict(row)
    tid = team_id_for_create(user)
    if row["team_id"] != tid:
        raise HTTPException(status_code=403, detail="Cloudflare token belongs to another team")
    if is_operator_user(user) and row["owner_user_id"] not in (None, user_id(user)):
        raise HTTPException(status_code=403, detail="Cloudflare token belongs to another operator")
    return dict(row)


def _assert_template_access(conn, template_id: int, user) -> dict:
    row = conn.execute("SELECT * FROM landing_templates WHERE id=? AND status='active'", (template_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Landing template not found")
    if is_superadmin(user) or row["team_id"] is None:
        return dict(row)
    tid = team_id_for_create(user)
    if row["team_id"] != tid:
        raise HTTPException(status_code=403, detail="Landing template belongs to another team")
    return dict(row)


def _assert_page_access(conn, page_id: int, user) -> dict:
    row = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Landing page not found")
    item = dict(row)
    if is_superadmin(user):
        return item
    tid = team_id_for_create(user)
    if item.get("team_id") != tid:
        raise HTTPException(status_code=403, detail="Landing page belongs to another team")
    if is_operator_user(user) and item.get("owner_user_id") not in (None, user_id(user)):
        raise HTTPException(status_code=403, detail="Landing page belongs to another operator")
    return item


def _bind_page_to_accounts(conn, act_ids: list[str], bind_target: str, url: str, user) -> dict:
    bind_target = (bind_target or "none").strip().lower()
    if bind_target not in {"landing", "form", "both", "none"}:
        raise HTTPException(status_code=400, detail="bind_target must be landing, form, both, or none")
    clean_ids = _clean_act_ids(act_ids)
    if bind_target == "none" or not clean_ids:
        return {"requested": clean_ids, "bound": [], "skipped": [], "target": bind_target}
    if not url:
        raise HTTPException(status_code=400, detail="No published URL available for account binding")
    bound, skipped = [], []
    for act_id in clean_ids:
        row = conn.execute("SELECT id, act_id, name FROM accounts WHERE act_id=?", (act_id,)).fetchone()
        if not row:
            skipped.append({"act_id": act_id, "reason": "account not found"})
            continue
        try:
            assert_row_access(conn, "accounts", row["id"], user, allow_unassigned=False)
        except HTTPException as exc:
            skipped.append({"act_id": act_id, "reason": exc.detail})
            continue
        updates, params = [], []
        if bind_target in {"landing", "both"}:
            updates.append("landing_url=?")
            params.append(url)
        if bind_target in {"form", "both"}:
            updates.append("form_link=?")
            params.append(url)
        updates.append("updated_at=datetime('now','+8 hours')")
        params.append(row["id"])
        conn.execute(f"UPDATE accounts SET {', '.join(updates)} WHERE id=?", params)
        bound.append({"act_id": act_id, "name": row["name"] or ""})
    return {"requested": clean_ids, "bound": bound, "skipped": skipped, "target": bind_target}


def _public_token(row) -> dict:
    accounts = _public_accounts(row["cf_accounts_json"] if "cf_accounts_json" in row.keys() else "[]")
    return {
        "id": row["id"],
        "name": row["name"],
        "token_mask": row["token_mask"],
        "cf_account_id": row["cf_account_id"],
        "cf_account_name": row["cf_account_name"],
        "cf_accounts": accounts,
        "cf_accounts_count": len(accounts),
        "selected_account_id": row["selected_account_id"] if "selected_account_id" in row.keys() else None,
        "status": row["status"],
        "last_verified_at": row["last_verified_at"],
        "team_id": row["team_id"],
        "owner_user_id": row["owner_user_id"],
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _public_page(row) -> dict:
    item = dict(row)
    item["target_urls"] = _json_loads(item.get("target_urls"), [])
    item["bound_act_ids"] = _json_loads(item.get("bound_act_ids"), [])
    item["protection_rules"] = _json_loads(item.get("protection_rules"), {})
    item["tracking_enabled"] = bool(item.get("tracking_enabled"))
    item["protection_enabled"] = bool(item.get("protection_enabled"))
    item["worker_enabled"] = bool(item.get("worker_enabled"))
    item.pop("raw_response", None)
    item.pop("ingest_secret", None)
    return item


def _safe_rules(rules: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(rules, dict):
        return {}
    allowed = {
        "country_allow",
        "country_block",
        "platform_block",
        "device_block",
        "ua_block",
        "referer_block",
        "query_block",
        "required_query",
    }
    clean: dict[str, Any] = {}
    for key in allowed:
        value = rules.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            parts = [x.strip()[:80] for x in value.replace("，", ",").split(",") if x.strip()]
        elif isinstance(value, list):
            parts = [str(x).strip()[:80] for x in value if str(x).strip()]
        else:
            continue
        clean[key] = parts[:80]
    return clean


@router.get("/tokens")
def list_cf_tokens(user=Depends(get_current_user)):
    where, params = _scope_where(user)
    sql = "SELECT * FROM cf_tokens"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_public_token(r) for r in rows]


@router.post("/tokens")
def create_cf_token(body: CloudflareTokenCreate, user=Depends(get_current_user)):
    name = (body.name or "").strip()
    raw_token = (body.api_token or "").strip()
    if not name or not raw_token:
        raise HTTPException(status_code=400, detail="API name and token are required")
    try:
        info = verify_token_and_accounts(raw_token)
    except CloudflareError as exc:
        raise HTTPException(status_code=400, detail=f"Cloudflare token verify failed: {exc}") from exc
    accounts = _normalize_cf_accounts(info.get("accounts") or [])
    if not accounts:
        raise HTTPException(
            status_code=400,
            detail="Cloudflare token verified, but no accessible account was returned. Use an Account API Token with Account Settings Read and Cloudflare Pages Edit permissions.",
        )
    account = accounts[0]
    selected_account_id = account.get("id") if len(accounts) == 1 else None
    selected_account_name = account.get("name") if selected_account_id else None
    team_id, owner_id = _stamp(user, body.team_id)
    conn = get_conn()
    conn.execute(
        """INSERT INTO cf_tokens
           (name, access_token_enc, token_mask, cf_accounts_json, selected_account_id,
            cf_account_id, cf_account_name, status, last_verified_at, team_id, owner_user_id, created_by)
           VALUES (?,?,?,?,?,?,?,'active',datetime('now','+8 hours'),?,?,?)""",
        (
            name,
            encrypt_token(raw_token),
            mask_token(raw_token),
            json.dumps(accounts, ensure_ascii=False),
            selected_account_id,
            account.get("id"),
            selected_account_name,
            team_id,
            owner_id,
            user.get("username", "unknown"),
        ),
    )
    token_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    row = conn.execute("SELECT * FROM cf_tokens WHERE id=?", (token_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "token": _public_token(row), "accounts": accounts}


@router.post("/tokens/{token_id}/verify")
def verify_cf_token(token_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _assert_token_access(conn, token_id, user)
    try:
        raw = decrypt_token(row["access_token_enc"])
        info = verify_token_and_accounts(raw)
        accounts = _normalize_cf_accounts(info.get("accounts") or [])
        if not accounts:
            raise ValueError("Cloudflare token verified, but no accessible account was returned")
        account = accounts[0]
        selected_account_id = row.get("selected_account_id")
        if selected_account_id and not any(a.get("id") == selected_account_id for a in accounts):
            selected_account_id = None
        if not selected_account_id and len(accounts) == 1:
            selected_account_id = account.get("id")
        selected_account_name = next((a.get("name") for a in accounts if a.get("id") == selected_account_id), None)
        conn.execute(
            """UPDATE cf_tokens
               SET status='active', cf_accounts_json=?, selected_account_id=?,
                   cf_account_id=?, cf_account_name=?,
                   last_verified_at=datetime('now','+8 hours'), updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (json.dumps(accounts, ensure_ascii=False), selected_account_id, account.get("id"), selected_account_name, token_id),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM cf_tokens WHERE id=?", (token_id,)).fetchone()
        return {"success": True, "token": _public_token(updated), "accounts": accounts}
    except Exception as exc:
        conn.execute(
            "UPDATE cf_tokens SET status='error', updated_at=datetime('now','+8 hours') WHERE id=?",
            (token_id,),
        )
        conn.commit()
        raise HTTPException(status_code=400, detail=f"Cloudflare verify failed: {exc}") from exc
    finally:
        conn.close()


@router.patch("/tokens/{token_id}/account")
def set_cf_token_account(token_id: int, body: CloudflareTokenAccountPatch, user=Depends(get_current_user)):
    account_id = (body.account_id or "").strip()
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id is required")
    conn = get_conn()
    row = _assert_token_access(conn, token_id, user)
    accounts = _public_accounts(row.get("cf_accounts_json"))
    matched = next((acct for acct in accounts if isinstance(acct, dict) and acct.get("id") == account_id), None)
    if not matched:
        conn.close()
        raise HTTPException(status_code=400, detail="This Cloudflare account is not available for this token")
    conn.execute(
        """UPDATE cf_tokens
           SET selected_account_id=?, cf_account_id=?, cf_account_name=?, updated_at=datetime('now','+8 hours')
           WHERE id=?""",
        (matched.get("id"), matched.get("id"), matched.get("name"), token_id),
    )
    conn.commit()
    updated = conn.execute("SELECT * FROM cf_tokens WHERE id=?", (token_id,)).fetchone()
    conn.close()
    return {"success": True, "token": _public_token(updated)}


@router.get("/tokens/{token_id}/diagnose")
def diagnose_cf_token(token_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _assert_token_access(conn, token_id, user)
    conn.close()
    try:
        raw = decrypt_token(row["access_token_enc"])
        accounts = _public_accounts(row.get("cf_accounts_json"))
        account_id, account_name = _resolve_token_account(row)
        if not accounts:
            info = verify_token_and_accounts(raw)
            accounts = _normalize_cf_accounts(info.get("accounts") or [])
        checks = [{"key": "token", "status": "pass", "label": "API Token 有效", "detail": "Cloudflare API 已通过验证"}]
        if account_id:
            try:
                projects = list_pages_projects(raw, account_id)
                checks.append({"key": "pages", "status": "pass", "label": "Pages 权限可用", "detail": f"可读取 {len(projects)} 个 Pages 项目"})
            except CloudflareError as exc:
                checks.append({"key": "pages", "status": "fail", "label": "Pages 权限不可用", "detail": str(exc)})
        else:
            checks.append({"key": "account", "status": "warn", "label": "需要选择 Cloudflare 账号", "detail": "该 Token 能看到多个账号，请先选择默认发布账号"})
        return {"success": True, "accounts": accounts, "selected_account_id": account_id, "selected_account_name": account_name, "checks": checks}
    except Exception as exc:
        return {
            "success": False,
            "checks": [{"key": "token", "status": "fail", "label": "API Token 不可用", "detail": str(exc)}],
            "hint": "Pages 发布需要 Cloudflare Account API Token，不是 R2/S3 Access Key 或 Secret Key。",
        }


@router.get("/templates")
def list_landing_templates(user=Depends(get_current_user)):
    where, params = ["status='active'"], []
    if not is_superadmin(user):
        where.append("(team_id=? OR team_id IS NULL)")
        params.append(team_id_for_create(user))
    sql = "SELECT id, name, status, team_id, owner_user_id, created_by, created_at FROM landing_templates WHERE " + " AND ".join(where)
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/preflight")
def preflight_landing_page(body: LandingPublishReq, user=Depends(get_current_user)):
    title = (body.title or "").strip()
    urls = [u.strip() for u in body.target_urls if u and u.strip()]
    rules = _safe_rules(body.protection_rules)
    checks = []
    checks.append({"key": "title", "status": "pass" if title else "fail", "label": "发布名称", "detail": title or "不能为空"})
    if urls:
        bad_urls = [u for u in urls if not (u.startswith("http://") or u.startswith("https://"))]
        checks.append({"key": "target_urls", "status": "fail" if bad_urls else "pass", "label": "按钮跳转链接", "detail": f"{len(urls)} 个链接" + (f"，{len(bad_urls)} 个格式异常" if bad_urls else "")})
    else:
        checks.append({"key": "target_urls", "status": "fail", "label": "按钮跳转链接", "detail": "至少填写一个跳转链接"})
    if body.tracking_enabled:
        checks.append({"key": "tracking", "status": "pass", "label": "边缘统计", "detail": "将通过 Cloudflare Worker 同域采集访问、点击、跳转、拦截事件"})
    if body.protection_enabled:
        checks.append({"key": "protection", "status": "pass" if rules else "warn", "label": "防护规则", "detail": "已配置防护规则" if rules else "已启用防护，但当前没有规则"})
    conn = get_conn()
    try:
        token_row = _assert_token_access(conn, body.token_id, user)
        account_id, account_name = _resolve_token_account(token_row)
        if account_id:
            checks.append({"key": "cloudflare_account", "status": "pass", "label": "Cloudflare 账号", "detail": account_name or account_id})
            raw_token = decrypt_token(token_row["access_token_enc"])
            try:
                list_pages_projects(raw_token, account_id)
                checks.append({"key": "pages_permission", "status": "pass", "label": "Pages 权限", "detail": "可读取 Pages 项目"})
            except CloudflareError as exc:
                checks.append({"key": "pages_permission", "status": "fail", "label": "Pages 权限", "detail": str(exc)})
        else:
            checks.append({"key": "cloudflare_account", "status": "fail", "label": "Cloudflare 账号", "detail": "请先在 API 卡片里选择默认发布账号"})
        _assert_template_access(conn, body.template_id, user)
        checks.append({"key": "template", "status": "pass", "label": "模板", "detail": "模板可用"})
        clean_ids = _clean_act_ids(body.bind_act_ids)
        if clean_ids and body.bind_target != "none":
            ok_count, fail_count = 0, 0
            for act_id in clean_ids:
                row = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
                if not row:
                    fail_count += 1
                    continue
                try:
                    assert_row_access(conn, "accounts", row["id"], user, allow_unassigned=False)
                    ok_count += 1
                except HTTPException:
                    fail_count += 1
            checks.append({"key": "account_binding", "status": "warn" if fail_count else "pass", "label": "发布后绑定", "detail": f"可绑定 {ok_count} 个账户" + (f"，{fail_count} 个无权限或不存在" if fail_count else "")})
        else:
            checks.append({"key": "account_binding", "status": "pass", "label": "发布后绑定", "detail": "未启用自动绑定"})
    except HTTPException as exc:
        checks.append({"key": "token", "status": "fail", "label": "Cloudflare API", "detail": str(exc.detail)})
    finally:
        conn.close()
    return {"success": not any(c["status"] == "fail" for c in checks), "checks": checks}


@router.get("/pages")
def list_landing_pages(user=Depends(get_current_user)):
    where, params = _scope_where(user, "p")
    sql = """SELECT p.*, t.name AS token_name, lt.name AS template_name
             FROM landing_pages p
             LEFT JOIN cf_tokens t ON t.id=p.cf_token_id
             LEFT JOIN landing_templates lt ON lt.id=COALESCE(p.template_id, 1)"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.id DESC LIMIT 200"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_public_page(r) for r in rows]


@router.post("/publish")
def publish_landing_page(body: LandingPublishReq, user=Depends(get_current_user)):
    title = (body.title or "").strip()
    urls = [u.strip() for u in body.target_urls if u and u.strip()]
    if not title:
        raise HTTPException(status_code=400, detail="Title is required")
    if not urls:
        raise HTTPException(status_code=400, detail="At least one target URL is required")
    if any(not (u.startswith("http://") or u.startswith("https://")) for u in urls):
        raise HTTPException(status_code=400, detail="Target URLs must start with http:// or https://")
    link_kind = (body.link_kind or "landing").strip().lower()
    if link_kind not in ("landing", "form"):
        raise HTTPException(status_code=400, detail="link_kind must be landing or form")

    conn = get_conn()
    token_row = _assert_token_access(conn, body.token_id, user)
    template = _assert_template_access(conn, body.template_id, user)
    raw_token = decrypt_token(token_row["access_token_enc"])
    cf_account_id, cf_account_name = _resolve_token_account(token_row)
    if not cf_account_id:
        conn.close()
        raise HTTPException(status_code=400, detail="Cloudflare token has no selected account; choose a Cloudflare account first")

    team_id, owner_id = _stamp(user, None)
    if team_id is None:
        team_id = token_row.get("team_id")
    project_name = sanitize_project_name(body.project_name or title)
    protection_rules = _safe_rules(body.protection_rules)
    worker_enabled = bool(body.tracking_enabled or body.protection_enabled)
    ingest_secret = secrets.token_urlsafe(32)
    work_dir = None
    page_id = None
    try:
        conn.execute(
            """INSERT INTO landing_pages
               (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                project_name, pixel_id, target_urls, rotation_mode, bound_act_ids, bind_target,
                tracking_enabled, protection_enabled, protection_rules, ingest_secret, worker_enabled,
                status, note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'deploying', ?,?,?,?)""",
            (
                title,
                link_kind,
                1 if body.form_link_enabled else 0,
                body.template_id,
                body.token_id,
                cf_account_id,
                cf_account_name,
                project_name,
                body.pixel_id or "",
                json.dumps(urls, ensure_ascii=False),
                body.rotation_mode,
                json.dumps(_clean_act_ids(body.bind_act_ids), ensure_ascii=False),
                body.bind_target,
                1 if body.tracking_enabled else 0,
                1 if body.protection_enabled else 0,
                json.dumps(protection_rules, ensure_ascii=False),
                ingest_secret,
                1 if worker_enabled else 0,
                body.note or "",
                team_id,
                owner_id,
                user.get("username", "unknown"),
            ),
        )
        page_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        work_dir = prepare_template(
            template["template_path"],
            pixel_id=body.pixel_id or "",
            target_urls=urls,
            rotation_mode=body.rotation_mode,
            worker_enabled=worker_enabled,
            tracking_enabled=body.tracking_enabled,
            protection_enabled=body.protection_enabled,
            protection_rules=protection_rules,
            page_id=page_id,
            ingest_secret=ingest_secret,
            ingest_url=_ingest_url(),
        )
        response = deploy_pages_static(raw_token, cf_account_id, project_name, work_dir)
        deployment_id = str(response.get("id") or "")
        pages_url = response.get("url") or response.get("aliases", [None])[0] or ""
        binding = _bind_page_to_accounts(conn, body.bind_act_ids, body.bind_target, pages_url, user)
        conn.execute(
            """UPDATE landing_pages
               SET deployment_id=?, pages_url=?, bound_act_ids=?, status='published',
                   raw_response=?, note=?, updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (
                deployment_id,
                pages_url,
                json.dumps([x["act_id"] for x in binding.get("bound", [])], ensure_ascii=False),
                json.dumps(response, ensure_ascii=False),
                (body.note or "") + (("\nBinding skipped: " + json.dumps(binding.get("skipped", []), ensure_ascii=False)) if binding.get("skipped") else ""),
                page_id,
            ),
        )
        conn.commit()
        saved = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
        item = _public_page(saved)
        item["binding"] = binding
        return {"success": True, "page": item}
    except Exception as exc:
        if page_id:
            conn.execute(
                "UPDATE landing_pages SET status='failed', last_error=?, updated_at=datetime('now','+8 hours') WHERE id=?",
                (str(exc), page_id),
            )
        else:
            conn.execute(
                """INSERT INTO landing_pages
                   (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                    project_name, pixel_id, target_urls, rotation_mode, bound_act_ids, bind_target,
                    tracking_enabled, protection_enabled, protection_rules, worker_enabled,
                    status, last_error, note, team_id, owner_user_id, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'failed', ?,?,?,?,?)""",
                (
                    title,
                    link_kind,
                    1 if body.form_link_enabled else 0,
                    body.template_id,
                    body.token_id,
                    cf_account_id,
                    cf_account_name,
                    project_name,
                    body.pixel_id or "",
                    json.dumps(urls, ensure_ascii=False),
                    body.rotation_mode,
                    json.dumps(_clean_act_ids(body.bind_act_ids), ensure_ascii=False),
                    body.bind_target,
                    1 if body.tracking_enabled else 0,
                    1 if body.protection_enabled else 0,
                    json.dumps(protection_rules, ensure_ascii=False),
                    1 if worker_enabled else 0,
                    str(exc),
                    body.note or "",
                    team_id,
                    owner_id,
                    user.get("username", "unknown"),
                ),
            )
        conn.commit()
        raise HTTPException(status_code=400, detail=f"Publish failed: {exc}") from exc
    finally:
        if work_dir:
            try:
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)
            except Exception:
                pass
        conn.close()


@router.delete("/pages/{page_id}")
def archive_landing_page(page_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    _assert_page_access(conn, page_id, user)
    conn.execute("UPDATE landing_pages SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?", (page_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/events/ingest")
async def ingest_landing_event(body: LandingEventIngest, request: Request):
    allowed_events = {"visit", "pass", "block", "click", "redirect", "error"}
    event_type = (body.event_type or "").strip().lower()
    if event_type not in allowed_events:
        raise HTTPException(status_code=400, detail="invalid event_type")
    conn = get_conn()
    page = conn.execute("SELECT id, ingest_secret, status FROM landing_pages WHERE id=?", (body.page_id,)).fetchone()
    if not page or not page["ingest_secret"] or not secrets.compare_digest(str(page["ingest_secret"]), str(body.secret or "")):
        conn.close()
        raise HTTPException(status_code=403, detail="invalid landing event secret")
    ua = _truncate(body.user_agent or request.headers.get("user-agent") or "", 500)
    ua_hash = hashlib.sha256(ua.encode("utf-8", "ignore")).hexdigest() if ua else ""
    metadata = body.metadata if isinstance(body.metadata, dict) else {}
    conn.execute(
        """INSERT INTO landing_events
           (page_id, event_type, decision, reason, path, target_url, referrer, country, region, city,
            colo, asn, platform, device_type, browser, os, user_agent_hash, ip_hash, metadata, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            body.page_id,
            event_type,
            _truncate(body.decision, 40),
            _truncate(body.reason, 500),
            _truncate(body.path, 500),
            _truncate(body.target_url, 1000),
            _truncate(body.referrer, 1000),
            _truncate(body.country, 10).upper(),
            _truncate(body.region, 80),
            _truncate(body.city, 80),
            _truncate(body.colo, 20),
            _truncate(body.asn, 32),
            _truncate(body.platform, 80),
            _truncate(body.device_type, 40),
            _truncate(body.browser, 80),
            _truncate(body.os, 80),
            ua_hash,
            _truncate(body.ip_hash, 128),
            json.dumps(metadata, ensure_ascii=False)[:4000],
            _now_cst(),
        ),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@router.get("/pages/{page_id}/stats")
def landing_page_stats(page_id: int, days: int = 7, user=Depends(get_current_user)):
    days = max(1, min(int(days or 7), 90))
    since = (datetime.now(CST) - timedelta(days=days - 1)).strftime("%Y-%m-%d 00:00:00")
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    params = (page_id, since)
    by_type = {
        r["event_type"]: int(r["cnt"] or 0)
        for r in conn.execute(
            "SELECT event_type, COUNT(*) AS cnt FROM landing_events WHERE page_id=? AND created_at>=? GROUP BY event_type",
            params,
        ).fetchall()
    }
    by_country = [
        dict(r)
        for r in conn.execute(
            "SELECT COALESCE(country,'') AS country, COUNT(*) AS cnt FROM landing_events WHERE page_id=? AND created_at>=? GROUP BY country ORDER BY cnt DESC LIMIT 20",
            params,
        ).fetchall()
    ]
    by_device = [
        dict(r)
        for r in conn.execute(
            "SELECT COALESCE(device_type,'') AS device_type, COUNT(*) AS cnt FROM landing_events WHERE page_id=? AND created_at>=? GROUP BY device_type ORDER BY cnt DESC LIMIT 20",
            params,
        ).fetchall()
    ]
    by_day = [
        dict(r)
        for r in conn.execute(
            "SELECT substr(created_at,1,10) AS day, event_type, COUNT(*) AS cnt FROM landing_events WHERE page_id=? AND created_at>=? GROUP BY day,event_type ORDER BY day",
            params,
        ).fetchall()
    ]
    recent = [
        dict(r)
        for r in conn.execute(
            """SELECT event_type, decision, reason, path, target_url, country, device_type, platform, created_at
               FROM landing_events WHERE page_id=? AND created_at>=?
               ORDER BY id DESC LIMIT 60""",
            params,
        ).fetchall()
    ]
    conn.close()
    return {
        "success": True,
        "page": _public_page(page),
        "days": days,
        "summary": {
            "visits": by_type.get("visit", 0),
            "blocks": by_type.get("block", 0),
            "clicks": by_type.get("click", 0),
            "redirects": by_type.get("redirect", 0),
            "errors": by_type.get("error", 0),
        },
        "by_type": by_type,
        "by_country": by_country,
        "by_device": by_device,
        "by_day": by_day,
        "recent": recent,
    }
