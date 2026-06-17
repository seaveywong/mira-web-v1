import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

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
        """
    )
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(landing_pages)").fetchall()}
        if "template_id" not in cols:
            conn.execute("ALTER TABLE landing_pages ADD COLUMN template_id INTEGER DEFAULT 1")
        if "bound_act_ids" not in cols:
            conn.execute("ALTER TABLE landing_pages ADD COLUMN bound_act_ids TEXT DEFAULT '[]'")
        if "bind_target" not in cols:
            conn.execute("ALTER TABLE landing_pages ADD COLUMN bind_target TEXT DEFAULT 'none'")
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
        conn.execute("UPDATE landing_templates SET name='RH FP 高级默认模板' WHERE id=1 AND COALESCE(created_by,'system')='system'")
    conn.commit()
    conn.close()


_ensure_schema()


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
    try:
        return json.loads(raw or "[]") if raw is not None else []
    except Exception:
        return []


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
        if not raw:
            continue
        if raw.startswith("act_"):
            raw = raw[4:]
        raw = "".join(ch for ch in raw if ch.isdigit())
        if raw and raw not in seen:
            seen.add(raw)
            output.append(raw)
    return output


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
            detail="Cloudflare token verified, but no accessible account was returned. Use an Account API Token with Account:Cloudflare Pages Edit and Account:Account Settings Read permissions.",
        )
    account = accounts[0] if accounts else {}
    selected_account_id = account.get("id") if len(accounts) == 1 else None
    selected_account_name = account.get("name") if selected_account_id else None
    team_id, owner_id = _stamp(user, body.team_id)
    conn = get_conn()
    conn.execute(
        """INSERT INTO cf_tokens
           (name, access_token_enc, token_mask, cf_accounts_json, selected_account_id,
            cf_account_id, cf_account_name, status,
            last_verified_at, team_id, owner_user_id, created_by)
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
        account = accounts[0] if accounts else {}
        selected_account_id = row.get("selected_account_id") if isinstance(row, dict) else row["selected_account_id"]
        if selected_account_id and not any(a.get("id") == selected_account_id for a in accounts):
            selected_account_id = None
        if not selected_account_id and len(accounts) == 1:
            selected_account_id = account.get("id")
        selected_account_name = None
        for acct in accounts:
            if acct.get("id") == selected_account_id:
                selected_account_name = acct.get("name")
                break
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
    matched = None
    for acct in accounts:
        if isinstance(acct, dict) and acct.get("id") == account_id:
            matched = acct
            break
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
            "hint": "Pages 发布需要 Cloudflare Account API Token，不是 R2/S3 的 Access Key 或 Secret Key。",
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
    checks = []
    if title:
        checks.append({"key": "title", "status": "pass", "label": "发布名称", "detail": title})
    else:
        checks.append({"key": "title", "status": "fail", "label": "发布名称", "detail": "不能为空"})
    if urls:
        bad_urls = [u for u in urls if not (u.startswith("http://") or u.startswith("https://"))]
        checks.append({"key": "target_urls", "status": "fail" if bad_urls else "pass", "label": "按钮跳转链接", "detail": f"{len(urls)} 个链接" + (f"，{len(bad_urls)} 个格式异常" if bad_urls else "")})
    else:
        checks.append({"key": "target_urls", "status": "fail", "label": "按钮跳转链接", "detail": "至少填写一个跳转链接"})
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
    data = []
    for r in rows:
        item = dict(r)
        try:
            item["target_urls"] = json.loads(item.get("target_urls") or "[]")
        except Exception:
            item["target_urls"] = []
        try:
            item["bound_act_ids"] = json.loads(item.get("bound_act_ids") or "[]")
        except Exception:
            item["bound_act_ids"] = []
        item.pop("raw_response", None)
        data.append(item)
    return data


@router.post("/publish")
def publish_landing_page(body: LandingPublishReq, user=Depends(get_current_user)):
    title = (body.title or "").strip()
    urls = [u.strip() for u in body.target_urls if u and u.strip()]
    if not title:
        raise HTTPException(status_code=400, detail="Title is required")
    if not urls:
        raise HTTPException(status_code=400, detail="At least one target URL is required")
    bad_urls = [u for u in urls if not (u.startswith("http://") or u.startswith("https://"))]
    if bad_urls:
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
    page_id = None
    work_dir = None
    try:
        work_dir = prepare_template(
            template["template_path"],
            pixel_id=body.pixel_id or "",
            target_urls=urls,
            rotation_mode=body.rotation_mode,
        )
        response = deploy_pages_static(raw_token, cf_account_id, project_name, work_dir)
        deployment_id = str(response.get("id") or "")
        pages_url = response.get("url") or response.get("aliases", [None])[0] or ""
        binding = _bind_page_to_accounts(conn, body.bind_act_ids, body.bind_target, pages_url, user)
        conn.execute(
            """INSERT INTO landing_pages
               (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                project_name, deployment_id, pages_url, pixel_id, target_urls, rotation_mode, bound_act_ids, bind_target,
                status, raw_response, note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'published', ?,?,?,?,?)""",
            (
                title,
                link_kind,
                1 if body.form_link_enabled else 0,
                body.template_id,
                body.token_id,
                cf_account_id,
                cf_account_name,
                project_name,
                deployment_id,
                pages_url,
                body.pixel_id or "",
                json.dumps(urls, ensure_ascii=False),
                body.rotation_mode,
                json.dumps([x["act_id"] for x in binding.get("bound", [])], ensure_ascii=False),
                body.bind_target,
                json.dumps(response, ensure_ascii=False),
                (body.note or "") + (("\nBinding skipped: " + json.dumps(binding.get("skipped", []), ensure_ascii=False)) if binding.get("skipped") else ""),
                team_id,
                owner_id,
                user.get("username", "unknown"),
            ),
        )
        page_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        saved = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
        item = dict(saved)
        item["target_urls"] = urls
        item["bound_act_ids"] = [x["act_id"] for x in binding.get("bound", [])]
        item["binding"] = binding
        item.pop("raw_response", None)
        return {"success": True, "page": item}
    except Exception as exc:
        conn.execute(
            """INSERT INTO landing_pages
               (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                project_name, pixel_id, target_urls, rotation_mode, bound_act_ids, bind_target, status, last_error,
                note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 'failed', ?,?,?,?,?)""",
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
    row = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Landing page not found")
    if not is_superadmin(user):
        tid = team_id_for_create(user)
        if row["team_id"] != tid:
            conn.close()
            raise HTTPException(status_code=403, detail="Landing page belongs to another team")
        if is_operator_user(user) and row["owner_user_id"] not in (None, user_id(user)):
            conn.close()
            raise HTTPException(status_code=403, detail="Landing page belongs to another operator")
    conn.execute("UPDATE landing_pages SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?", (page_id,))
    conn.commit()
    conn.close()
    return {"success": True}
