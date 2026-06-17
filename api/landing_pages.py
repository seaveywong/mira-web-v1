import hashlib
import ipaddress
import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.auth import get_current_user, is_superadmin
from core.app_meta import DEFAULT_ALLOWED_ORIGINS
from core.database import decrypt_token, encrypt_token, get_conn, mask_token
from core.tenancy import assert_row_access, is_operator_user, team_id_for_create, user_id
from services.landing_publisher import (
    DEFAULT_TEMPLATE_DIR,
    CloudflareError,
    add_pages_custom_domain,
    delete_pages_project,
    deploy_pages_static,
    get_pages_custom_domain_status,
    list_pages_projects,
    normalize_custom_domain,
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
    account_id: Optional[str] = None
    team_id: Optional[int] = None


class CloudflareTokenAccountPatch(BaseModel):
    account_id: str


class LandingPublishReq(BaseModel):
    token_id: int
    template_id: int = 1
    title: str
    project_name: Optional[str] = None
    custom_domain: Optional[str] = ""
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
            "custom_domain": "TEXT",
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


def _host_is_ip_or_local(host: Optional[str]) -> bool:
    value = (host or "").strip().lower()
    if not value or value in {"localhost"} or value.endswith(".local"):
        return True
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _normalize_public_base(raw: Optional[str]) -> Optional[str]:
    value = (raw or "").strip().rstrip("/")
    if not value:
        return None
    if value.endswith("/api/landing-pages/events/ingest"):
        value = value[: -len("/api/landing-pages/events/ingest")]
    if "://" not in value:
        value = "https://" + value
    parsed = urlparse(value)
    host = parsed.hostname or ""
    if _host_is_ip_or_local(host):
        return None
    return f"https://{parsed.netloc}"


def _request_public_base(request: Optional[Request]) -> Optional[str]:
    if not request:
        return None
    host = (
        request.headers.get("x-forwarded-host")
        or request.headers.get("host")
        or request.url.netloc
    )
    if not host:
        return None
    return _normalize_public_base(host)


def _ingest_url(request: Optional[Request] = None) -> str:
    candidates = [
        os.environ.get("MIRA_LANDING_INGEST_URL"),
        os.environ.get("MIRA_PUBLIC_BASE_URL"),
        os.environ.get("PUBLIC_BASE_URL"),
        _request_public_base(request),
        *(DEFAULT_ALLOWED_ORIGINS or []),
        "https://shouhu.asia",
    ]
    raw = next((base for base in (_normalize_public_base(v) for v in candidates) if base), "https://shouhu.asia")
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


def _domain_status_usable(domain_status: Any, last_error: Optional[str]) -> bool:
    err = str(last_error or "").strip().lower()
    if err and "custom domain" in err:
        return False
    if not domain_status:
        return not err
    if isinstance(domain_status, dict):
        raw_values = [
            domain_status.get("status"),
            domain_status.get("validation_status"),
            domain_status.get("verification_status"),
            domain_status.get("state"),
            domain_status.get("ssl_status"),
        ]
        text = " ".join(str(v or "").strip().lower() for v in raw_values if v is not None)
        if any(bad in text for bad in ("not_found", "error", "failed", "rejected", "missing", "pending", "verifying", "initializing", "inactive")):
            return False
        if any(ok in text for ok in ("active", "verified", "success", "complete", "deployed")):
            return True
        if text:
            return False
    return False


def _domain_status_text(domain_status: Any) -> str:
    if isinstance(domain_status, dict):
        for key in ("status", "validation_status", "verification_status", "state", "ssl_status", "name"):
            value = domain_status.get(key)
            if value not in (None, ""):
                return str(value)
    if domain_status:
        return str(domain_status)
    return ""


def _public_page(row) -> dict:
    item = dict(row)
    item["target_urls"] = _json_loads(item.get("target_urls"), [])
    item["bound_act_ids"] = _json_loads(item.get("bound_act_ids"), [])
    item["protection_rules"] = _json_loads(item.get("protection_rules"), {})
    item["tracking_enabled"] = bool(item.get("tracking_enabled"))
    item["protection_enabled"] = bool(item.get("protection_enabled"))
    item["worker_enabled"] = bool(item.get("worker_enabled"))
    custom_domain = (item.get("custom_domain") or "").strip()
    pages_url = (item.get("pages_url") or "").strip()
    raw_response = _json_loads(item.get("raw_response"), {})
    domain_status = None
    if isinstance(raw_response, dict):
        domain_status = raw_response.get("domain_status") or raw_response.get("custom_domain_result") or None
        item["domain_status"] = domain_status
    custom_domain_usable = bool(custom_domain and _domain_status_usable(domain_status, item.get("last_error")))
    item["custom_domain_usable"] = custom_domain_usable
    item["public_url"] = f"https://{custom_domain}" if custom_domain_usable else pages_url
    item["public_url_source"] = "custom_domain" if custom_domain_usable else ("pages_url" if pages_url else "")
    item.pop("raw_response", None)
    item.pop("ingest_secret", None)
    return item


def _normalize_url_for_match(value: Optional[str]) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    return raw.rstrip("/").lower()


def _page_url_candidates(item: dict) -> list[str]:
    urls = []
    pages_url = (item.get("pages_url") or "").strip()
    custom_domain = (item.get("custom_domain") or "").strip()
    public_url = (item.get("public_url") or "").strip()
    if public_url:
        urls.append(public_url)
    if pages_url:
        urls.append(pages_url)
    if custom_domain:
        urls.append(f"https://{custom_domain}")
    out, seen = [], set()
    for url in urls:
        key = _normalize_url_for_match(url)
        if key and key not in seen:
            seen.add(key)
            out.append(url)
    return out


def _matrix_ids_for_account(conn, act_id: str) -> list[int]:
    raw = str(act_id or "").strip()
    if not raw:
        return []
    num = raw[4:] if raw.startswith("act_") else raw
    candidates = [num, f"act_{num}"]
    try:
        rows = conn.execute(
            """SELECT t.matrix_id
               FROM accounts a
               JOIN fb_tokens t ON t.id=a.token_id
               WHERE a.act_id IN (?,?)
                 AND t.matrix_id IS NOT NULL
               UNION
               SELECT t.matrix_id
               FROM account_op_tokens aot
               JOIN fb_tokens t ON t.id=aot.token_id
               WHERE aot.act_id IN (?,?)
                 AND COALESCE(aot.status,'active')='active'
                 AND t.matrix_id IS NOT NULL
               ORDER BY matrix_id""",
            candidates + candidates,
        ).fetchall()
    except Exception:
        return []
    out = []
    seen = set()
    for row in rows:
        try:
            mid = int(row["matrix_id"])
        except Exception:
            continue
        if mid > 0 and mid not in seen:
            seen.add(mid)
            out.append(mid)
    return out


def _matrix_ids_for_accounts(conn, act_ids: list[str]) -> list[int]:
    out = []
    seen = set()
    for act_id in _clean_act_ids(act_ids):
        for mid in _matrix_ids_for_account(conn, act_id):
            if mid > 0 and mid not in seen:
                seen.add(mid)
                out.append(mid)
    return sorted(out)


def _has_table(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _landing_page_usage(conn, item: dict, user) -> dict:
    candidates = {_normalize_url_for_match(v) for v in _page_url_candidates(item)}
    candidates.discard("")
    usage = {"total": 0, "accounts": [], "campaigns": []}
    if not candidates:
        return usage

    account_where, account_params = ["(COALESCE(a.landing_url,'')!='' OR COALESCE(a.form_link,'')!='')"], []
    scoped_where, scoped_params = _scope_where(user, "a")
    account_where.extend(scoped_where)
    account_params.extend(scoped_params)
    for row in conn.execute(
        f"""SELECT a.id, a.act_id, a.name, a.landing_url, a.form_link
            FROM accounts a
            WHERE {' AND '.join(account_where)}
            ORDER BY a.updated_at DESC LIMIT 800""",
        account_params,
    ).fetchall():
        matched_fields = []
        if _normalize_url_for_match(row["landing_url"]) in candidates:
            matched_fields.append("landing_url")
        if _normalize_url_for_match(row["form_link"]) in candidates:
            matched_fields.append("form_link")
        if matched_fields:
            usage["accounts"].append({
                "id": row["id"],
                "act_id": row["act_id"],
                "name": row["name"] or row["act_id"],
                "fields": matched_fields,
                "linked_matrix_ids": _matrix_ids_for_account(conn, row["act_id"]),
            })

    if _has_table(conn, "auto_campaigns"):
        try:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(auto_campaigns)").fetchall()}
        except Exception:
            cols = set()
        if "landing_url" in cols:
            campaign_where, campaign_params = ["COALESCE(c.landing_url,'')!=''"], []
            if not is_superadmin(user):
                scoped_where, scoped_params = _scope_where(user, "a")
                campaign_where.extend(scoped_where)
                campaign_params.extend(scoped_params)
            for row in conn.execute(
                f"""SELECT c.id, c.act_id, c.name, c.status, c.landing_url
                    FROM auto_campaigns c
                    LEFT JOIN accounts a ON a.act_id=c.act_id
                    WHERE {' AND '.join(campaign_where)}
                    ORDER BY c.updated_at DESC LIMIT 800""",
                campaign_params,
            ).fetchall():
                if _normalize_url_for_match(row["landing_url"]) in candidates:
                    usage["campaigns"].append({
                        "id": row["id"],
                        "act_id": row["act_id"],
                        "name": row["name"] or f"Campaign {row['id']}",
                        "status": row["status"] or "",
                        "linked_matrix_ids": _matrix_ids_for_account(conn, row["act_id"]),
                    })
    usage["total"] = len(usage["accounts"]) + len(usage["campaigns"])
    usage["accounts"] = usage["accounts"][:20]
    usage["campaigns"] = usage["campaigns"][:20]
    return usage


def _refresh_landing_domain_record(conn, page: dict, user) -> dict:
    custom_domain = (page.get("custom_domain") or "").strip()
    if not custom_domain:
        raise HTTPException(status_code=400, detail="This landing page has no custom domain")
    token_id = page.get("cf_token_id")
    if not token_id:
        raise HTTPException(status_code=400, detail="This landing page has no Cloudflare token")
    token_row = _assert_token_access(conn, int(token_id), user)
    raw_token = decrypt_token(token_row["access_token_enc"])
    cf_account_id = page.get("cf_account_id") or token_row.get("cf_account_id")
    if not cf_account_id:
        raise HTTPException(status_code=400, detail="Cloudflare token has no selected account; choose a Cloudflare account first")
    status = get_pages_custom_domain_status(
        raw_token,
        cf_account_id,
        page.get("project_name") or "",
        custom_domain,
    )
    raw_payload = _json_loads(page.get("raw_response"), {})
    if not isinstance(raw_payload, dict):
        raw_payload = {}
    raw_payload["domain_status"] = status
    detail = json.dumps(status, ensure_ascii=False)
    last_error = "" if (status.get("status") or "").lower() not in {"not_found", "error"} else detail
    conn.execute(
        """UPDATE landing_pages
           SET last_error=?, raw_response=?, updated_at=datetime('now','+8 hours')
           WHERE id=?""",
        (last_error, json.dumps(raw_payload, ensure_ascii=False), page["id"]),
    )
    conn.commit()
    updated = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page["id"],)).fetchone()
    item = _public_page(updated)
    item["domain_status"] = status
    binding = None
    if item.get("custom_domain_usable") and item.get("public_url"):
        binding = _bind_page_to_accounts(
            conn,
            item.get("bound_act_ids") or [],
            item.get("bind_target") or "none",
            item.get("public_url"),
            user,
        )
        if binding and binding.get("bound"):
            conn.commit()
    item["usage"] = _landing_page_usage(conn, item, user)
    return {"page": item, "domain_status": status, "binding": binding}


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
    account_id = (body.account_id or "").strip()
    if not name or not raw_token:
        raise HTTPException(status_code=400, detail="API name and token are required")
    try:
        info = verify_token_and_accounts(raw_token, account_id=account_id or None)
    except CloudflareError as exc:
        raise HTTPException(status_code=400, detail=f"Cloudflare token verify failed: {exc}") from exc
    accounts = _normalize_cf_accounts(info.get("accounts") or [])
    if not accounts:
        raise HTTPException(
            status_code=400,
            detail="Cloudflare token verified, but no accessible account was returned. Fill Account ID for account-scoped tokens, or use a token with Account Settings Read and Cloudflare Pages Edit permissions.",
        )
    account = accounts[0]
    selected_account_id = account_id or (account.get("id") if len(accounts) == 1 else None)
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
        existing_account_id = (row["selected_account_id"] or row["cf_account_id"] or "").strip()
        info = verify_token_and_accounts(raw, account_id=existing_account_id or None)
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
            info = verify_token_and_accounts(raw, account_id=account_id or None)
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
def preflight_landing_page(body: LandingPublishReq, request: Request, user=Depends(get_current_user)):
    title = (body.title or "").strip()
    urls = [u.strip() for u in body.target_urls if u and u.strip()]
    rules = _safe_rules(body.protection_rules)
    link_kind = (body.link_kind or "landing").strip().lower()
    bind_target = (body.bind_target or "none").strip().lower()
    custom_domain = ""
    custom_domain_error = ""
    try:
        custom_domain = normalize_custom_domain(body.custom_domain)
    except ValueError as exc:
        custom_domain_error = str(exc)
    checks = []
    if custom_domain_error:
        checks.append({"key": "custom_domain", "status": "fail", "label": "自定义域名", "detail": custom_domain_error})
    elif custom_domain:
        checks.append({
            "key": "custom_domain",
            "status": "warn",
            "label": "自定义域名",
            "detail": f"{custom_domain} 将绑定到 Cloudflare Pages；请确保域名已在 Cloudflare 可管理并完成 DNS 指向。",
        })
    checks.append({"key": "title", "status": "pass" if title else "fail", "label": "发布名称", "detail": title or "不能为空"})
    if urls:
        bad_urls = [u for u in urls if not (u.startswith("http://") or u.startswith("https://"))]
        checks.append({"key": "target_urls", "status": "fail" if bad_urls else "pass", "label": "按钮跳转链接", "detail": f"{len(urls)} 个链接" + (f"，{len(bad_urls)} 个格式异常" if bad_urls else "")})
    else:
        checks.append({"key": "target_urls", "status": "fail", "label": "按钮跳转链接", "detail": "至少填写一个跳转链接"})
    if link_kind == "form":
        checks.append({"key": "link_kind", "status": "pass", "label": "表单投放链接", "detail": "访问根路径会直接按轮询策略 302 跳转，不展示落地页正文"})
        if bind_target == "landing":
            checks.append({"key": "bind_target", "status": "warn", "label": "绑定位置", "detail": "当前是表单链接模式，建议绑定到账户表单链接，避免铺 Lead Form 时取不到链接"})
    else:
        checks.append({"key": "link_kind", "status": "pass", "label": "普通落地页", "detail": "访问时展示模板，按钮点击后按轮询策略跳转"})
    if body.tracking_enabled:
        checks.append({"key": "tracking", "status": "pass", "label": "边缘统计", "detail": "将通过 Cloudflare Worker 同域采集访问、点击、跳转、拦截事件"})
    if body.protection_enabled:
        checks.append({"key": "protection", "status": "pass" if rules else "warn", "label": "防护规则", "detail": "已配置防护规则" if rules else "已启用防护，但当前没有规则"})
    if body.tracking_enabled or body.protection_enabled or link_kind == "form":
        ingest_url = _ingest_url(request)
        checks.append({
            "key": "ingest_url",
            "status": "pass",
            "label": "统计回传域名",
            "detail": f"{ingest_url}（仅使用 HTTPS 公网域名，不写入服务器裸 IP）",
        })
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
    pages = []
    for row in rows:
        item = _public_page(row)
        item["linked_matrix_ids"] = _matrix_ids_for_accounts(conn, item.get("bound_act_ids") or [])
        item["usage"] = _landing_page_usage(conn, item, user)
        pages.append(item)
    conn.close()
    return pages


@router.post("/publish")
def publish_landing_page(body: LandingPublishReq, request: Request, user=Depends(get_current_user)):
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
    try:
        custom_domain = normalize_custom_domain(body.custom_domain)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

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
    worker_enabled = bool(body.tracking_enabled or body.protection_enabled or link_kind == "form")
    ingest_secret = secrets.token_urlsafe(32)
    work_dir = None
    page_id = None
    try:
        conn.execute(
            """INSERT INTO landing_pages
               (title, link_kind, form_link_enabled, template_id, cf_token_id, cf_account_id, cf_account_name,
                project_name, custom_domain, pixel_id, target_urls, rotation_mode, bound_act_ids, bind_target,
                tracking_enabled, protection_enabled, protection_rules, ingest_secret, worker_enabled,
                status, note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'deploying', ?,?,?,?)""",
            (
                title,
                link_kind,
                1 if (body.form_link_enabled or link_kind == "form") else 0,
                body.template_id,
                body.token_id,
                cf_account_id,
                cf_account_name,
                project_name,
                custom_domain,
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
            link_kind=link_kind,
            worker_enabled=worker_enabled,
            tracking_enabled=body.tracking_enabled,
            protection_enabled=body.protection_enabled,
            protection_rules=protection_rules,
            page_id=page_id,
            ingest_secret=ingest_secret,
            ingest_url=_ingest_url(request),
        )
        response = deploy_pages_static(raw_token, cf_account_id, project_name, work_dir)
        deployment_id = str(response.get("id") or "")
        pages_url = response.get("url") or response.get("aliases", [None])[0] or ""
        public_url = pages_url
        domain_error = ""
        domain_notice = ""
        domain_result = None
        if custom_domain:
            try:
                domain_result = add_pages_custom_domain(raw_token, cf_account_id, project_name, custom_domain)
                if str((domain_result or {}).get("status") or "").lower() == "already_exists":
                    try:
                        domain_result = get_pages_custom_domain_status(
                            raw_token,
                            cf_account_id,
                            project_name,
                            custom_domain,
                        )
                    except Exception:
                        pass
                if _domain_status_usable(domain_result, None):
                    public_url = f"https://{custom_domain}"
                else:
                    status_text = _domain_status_text(domain_result) or "pending"
                    domain_notice = (
                        f"Custom domain {custom_domain} is {status_text}; "
                        "auto-binding used the Pages fallback URL until the domain is active."
                    )
            except CloudflareError as exc:
                domain_error = f"Custom domain binding failed: {exc}"
        binding = _bind_page_to_accounts(conn, body.bind_act_ids, body.bind_target, public_url, user)
        response_payload = dict(response)
        if domain_result is not None:
            response_payload["custom_domain_result"] = domain_result
        if domain_notice:
            response_payload["custom_domain_notice"] = domain_notice
        note_text = (body.note or "")
        if domain_error:
            note_text += ("\n" if note_text else "") + domain_error
        if domain_notice:
            note_text += ("\n" if note_text else "") + domain_notice
        if binding.get("skipped"):
            note_text += ("\n" if note_text else "") + "Binding skipped: " + json.dumps(binding.get("skipped", []), ensure_ascii=False)
        conn.execute(
            """UPDATE landing_pages
               SET deployment_id=?, pages_url=?, custom_domain=?, bound_act_ids=?, status='published',
                   raw_response=?, last_error=?, note=?, updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (
                deployment_id,
                pages_url,
                custom_domain,
                json.dumps([x["act_id"] for x in binding.get("bound", [])], ensure_ascii=False),
                json.dumps(response_payload, ensure_ascii=False),
                domain_error,
                note_text,
                page_id,
            ),
        )
        conn.commit()
        saved = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
        item = _public_page(saved)
        item["binding"] = binding
        item["domain_error"] = domain_error
        item["domain_notice"] = domain_notice
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
                    project_name, custom_domain, pixel_id, target_urls, rotation_mode, bound_act_ids, bind_target,
                    tracking_enabled, protection_enabled, protection_rules, worker_enabled,
                    status, last_error, note, team_id, owner_user_id, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'failed', ?,?,?,?,?)""",
                (
                    title,
                    link_kind,
                    1 if (body.form_link_enabled or link_kind == "form") else 0,
                    body.template_id,
                    body.token_id,
                    cf_account_id,
                    cf_account_name,
                    project_name,
                    custom_domain,
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


@router.post("/pages/{page_id}/refresh-domain")
def refresh_landing_page_domain(page_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    try:
        result = _refresh_landing_domain_record(conn, page, user)
        return {"success": True, **result}
    except HTTPException:
        raise
    except Exception as exc:
        conn.execute(
            "UPDATE landing_pages SET last_error=?, updated_at=datetime('now','+8 hours') WHERE id=?",
            (f"Domain status refresh failed: {exc}", page_id),
        )
        conn.commit()
        raise HTTPException(status_code=400, detail=f"Domain status refresh failed: {exc}") from exc
    finally:
        conn.close()


@router.post("/pages/refresh-domains")
def refresh_landing_page_domains(limit: int = 50, user=Depends(get_current_user)):
    limit = max(1, min(int(limit or 50), 100))
    conn = get_conn()
    where, params = _scope_where(user, "p")
    clauses = ["COALESCE(p.custom_domain,'')!=''", "COALESCE(p.status,'')!='archived'"]
    clauses.extend(where)
    rows = conn.execute(
        f"""SELECT p.*
            FROM landing_pages p
            WHERE {' AND '.join(clauses)}
            ORDER BY p.updated_at DESC, p.id DESC
            LIMIT ?""",
        params + [limit],
    ).fetchall()
    items = []
    summary = {"checked": 0, "usable": 0, "pending": 0, "failed": 0, "rebound_accounts": 0}
    try:
        for row in rows:
            page = dict(row)
            summary["checked"] += 1
            try:
                result = _refresh_landing_domain_record(conn, page, user)
                item = result["page"]
                binding = result.get("binding") or {}
                rebound = len(binding.get("bound") or [])
                summary["rebound_accounts"] += rebound
                status_text = _domain_status_text(result.get("domain_status")) or "unknown"
                usable = bool(item.get("custom_domain_usable"))
                summary["usable" if usable else "pending"] += 1
                items.append({
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "custom_domain": item.get("custom_domain"),
                    "status": status_text,
                    "usable": usable,
                    "public_url": item.get("public_url"),
                    "rebound_accounts": rebound,
                })
            except Exception as exc:
                summary["failed"] += 1
                items.append({
                    "id": page.get("id"),
                    "title": page.get("title"),
                    "custom_domain": page.get("custom_domain"),
                    "status": "failed",
                    "usable": False,
                    "error": str(getattr(exc, "detail", exc)),
                    "rebound_accounts": 0,
                })
        return {"success": True, **summary, "items": items}
    finally:
        conn.close()


@router.delete("/pages/{page_id}")
def archive_landing_page(page_id: int, cleanup: bool = False, user=Depends(get_current_user)):
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    item = _public_page(page)
    usage = _landing_page_usage(conn, item, user)
    if cleanup:
        if usage["total"] > 0:
            conn.close()
            raise HTTPException(status_code=400, detail=f"Landing page is still in use by {usage['total']} resource(s)")
        cloudflare_cleanup = {"skipped": True, "reason": "no published Cloudflare project"}
        project_name = (page.get("project_name") or "").strip()
        has_remote_project = bool(project_name and (page.get("pages_url") or page.get("deployment_id") or page.get("status") == "published"))
        if has_remote_project:
            token_id = page.get("cf_token_id")
            if not token_id:
                conn.close()
                raise HTTPException(status_code=400, detail="Cloudflare project exists but token is missing; archive it instead or restore the API token")
            token_row = _assert_token_access(conn, int(token_id), user)
            cf_account_id = (page.get("cf_account_id") or token_row.get("selected_account_id") or token_row.get("cf_account_id") or "").strip()
            if not cf_account_id:
                conn.close()
                raise HTTPException(status_code=400, detail="Cloudflare account is missing; choose the API account before deleting the project")
            try:
                raw_token = decrypt_token(token_row["access_token_enc"])
                cloudflare_cleanup = delete_pages_project(raw_token, cf_account_id, project_name)
            except CloudflareError as exc:
                conn.close()
                raise HTTPException(status_code=400, detail=f"Cloudflare project delete failed: {exc}") from exc
        conn.execute("DELETE FROM landing_pages WHERE id=?", (page_id,))
        conn.commit()
        conn.close()
        return {"success": True, "deleted": True, "cloudflare": cloudflare_cleanup, "usage": usage}
    conn.execute("UPDATE landing_pages SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?", (page_id,))
    conn.commit()
    conn.close()
    return {"success": True, "archived": True, "usage": usage}


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
    by_hour = [
        dict(r)
        for r in conn.execute(
            "SELECT substr(created_at,1,13) || ':00' AS hour, event_type, COUNT(*) AS cnt FROM landing_events WHERE page_id=? AND created_at>=? GROUP BY hour,event_type ORDER BY hour",
            params,
        ).fetchall()
    ]
    by_target = [
        dict(r)
        for r in conn.execute(
            """SELECT COALESCE(NULLIF(target_url,''),'--') AS target_url, event_type, COUNT(*) AS cnt
               FROM landing_events
               WHERE page_id=? AND created_at>=? AND event_type IN ('redirect','click')
               GROUP BY target_url,event_type
               ORDER BY cnt DESC LIMIT 30""",
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
        "by_hour": by_hour,
        "by_target": by_target,
        "recent": recent,
    }
