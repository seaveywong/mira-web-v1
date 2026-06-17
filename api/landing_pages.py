import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.auth import get_current_user, is_superadmin
from core.database import decrypt_token, encrypt_token, get_conn, mask_token
from core.tenancy import is_operator_user, team_id_for_create, user_id
from services.landing_publisher import (
    DEFAULT_TEMPLATE_DIR,
    CloudflareError,
    deploy_pages_static,
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


def _ensure_schema():
    conn = get_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cf_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            access_token_enc TEXT NOT NULL,
            token_mask TEXT,
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
    except Exception:
        logger.exception("landing_pages schema patch failed")
    row = conn.execute("SELECT id FROM landing_templates WHERE id=1").fetchone()
    if not row and DEFAULT_TEMPLATE_DIR.exists():
        conn.execute(
            """INSERT INTO landing_templates
               (id, name, template_path, status, created_by)
               VALUES (1, 'RH FP 高级默认模板', ?, 'active', 'system')""",
            (str(DEFAULT_TEMPLATE_DIR),),
        )
    conn.commit()
    conn.close()


_ensure_schema()


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
    return {
        "id": row["id"],
        "name": row["name"],
        "token_mask": row["token_mask"],
        "cf_account_id": row["cf_account_id"],
        "cf_account_name": row["cf_account_name"],
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
    accounts = info.get("accounts") or []
    account = accounts[0] if accounts else {}
    team_id, owner_id = _stamp(user, body.team_id)
    conn = get_conn()
    conn.execute(
        """INSERT INTO cf_tokens
           (name, access_token_enc, token_mask, cf_account_id, cf_account_name, status,
            last_verified_at, team_id, owner_user_id, created_by)
           VALUES (?,?,?,?,?,'active',datetime('now','+8 hours'),?,?,?)""",
        (
            name,
            encrypt_token(raw_token),
            mask_token(raw_token),
            account.get("id"),
            account.get("name"),
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
        accounts = info.get("accounts") or []
        account = accounts[0] if accounts else {}
        conn.execute(
            """UPDATE cf_tokens
               SET status='active', cf_account_id=?, cf_account_name=?,
                   last_verified_at=datetime('now','+8 hours'), updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (account.get("id"), account.get("name"), token_id),
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
    link_kind = (body.link_kind or "landing").strip().lower()
    if link_kind not in ("landing", "form"):
        raise HTTPException(status_code=400, detail="link_kind must be landing or form")
    conn = get_conn()
    token_row = _assert_token_access(conn, body.token_id, user)
    template = _assert_template_access(conn, body.template_id, user)
    raw_token = decrypt_token(token_row["access_token_enc"])
    cf_account_id = token_row.get("cf_account_id")
    if not cf_account_id:
        conn.close()
        raise HTTPException(status_code=400, detail="Cloudflare token has no account id; verify it first")
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
        conn.execute(
            """INSERT INTO landing_pages
               (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                project_name, deployment_id, pages_url, pixel_id, target_urls, rotation_mode,
                status, raw_response, note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 'published', ?,?,?,?,?)""",
            (
                title,
                link_kind,
                1 if body.form_link_enabled else 0,
                body.template_id,
                body.token_id,
                cf_account_id,
                token_row.get("cf_account_name"),
                project_name,
                deployment_id,
                pages_url,
                body.pixel_id or "",
                json.dumps(urls, ensure_ascii=False),
                body.rotation_mode,
                json.dumps(response, ensure_ascii=False),
                body.note or "",
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
        item.pop("raw_response", None)
        return {"success": True, "page": item}
    except Exception as exc:
        conn.execute(
            """INSERT INTO landing_pages
               (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                project_name, pixel_id, target_urls, rotation_mode, status, last_error,
                note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?, 'failed', ?,?,?,?,?)""",
            (
                title,
                link_kind,
                1 if body.form_link_enabled else 0,
                body.template_id,
                body.token_id,
                cf_account_id,
                token_row.get("cf_account_name"),
                project_name,
                body.pixel_id or "",
                json.dumps(urls, ensure_ascii=False),
                body.rotation_mode,
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
