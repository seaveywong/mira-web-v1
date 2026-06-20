import hashlib
import ipaddress
import json
import logging
import os
import re
import secrets
import shutil
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

import requests
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from core.auth import get_current_user, is_superadmin
from core.app_meta import DEFAULT_ALLOWED_ORIGINS
from core.database import decrypt_token, encrypt_token, get_conn, mask_token
from core.perf_history import ensure_perf_snapshot_history_schema
from core.tenancy import assert_row_access, is_operator_user, team_id_for_create, user_id
from services.token_manager import ACTION_READ, get_exec_token
from services.landing_publisher import (
    DEFAULT_TEMPLATE_DIR,
    LEGACY_TEMPLATE_DIR,
    CloudflareError,
    add_pages_custom_domain,
    delete_pages_project,
    deploy_pages_static,
    ensure_pages_cname_dns_record,
    find_zone_for_domain,
    get_pages_custom_domain_status,
    list_account_zones,
    list_dns_records,
    list_pages_projects,
    normalize_custom_domain,
    pages_cname_target,
    prepare_template,
    sanitize_project_name,
    stable_pages_url,
    verify_token_and_accounts,
)


logger = logging.getLogger("mira.landing_pages")
router = APIRouter()
CST = timezone(timedelta(hours=8))
LANDING_TRACKING_RETENTION_DAYS = 7
LANDING_TEMPLATE_REFERENCE_ZIP = os.environ.get(
    "MIRA_LANDING_TEMPLATE_REFERENCE_ZIP",
    "/opt/mira/landing-template-reference.zip",
)
LANDING_TEMPLATE_UPLOAD_DIR = Path(
    os.environ.get("MIRA_LANDING_TEMPLATE_UPLOAD_DIR", "/opt/mira/landing_templates/custom")
)
LANDING_TEMPLATE_MAX_ZIP_BYTES = int(os.environ.get("MIRA_LANDING_TEMPLATE_MAX_ZIP_BYTES", str(20 * 1024 * 1024)))
LANDING_TEMPLATE_MAX_UNPACKED_BYTES = int(os.environ.get("MIRA_LANDING_TEMPLATE_MAX_UNPACKED_BYTES", str(80 * 1024 * 1024)))
LANDING_TEMPLATE_MAX_FILES = int(os.environ.get("MIRA_LANDING_TEMPLATE_MAX_FILES", "400"))
LANDING_TEMPLATE_BLOCKED_NAMES = {"_worker.js", "_routes.json", "_headers", "_redirects"}
LANDING_TEMPLATE_BLOCKED_SUFFIXES = {
    ".php",
    ".py",
    ".rb",
    ".pl",
    ".cgi",
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".bat",
    ".cmd",
    ".sh",
    ".ps1",
}
_landing_cleanup_last: Optional[datetime] = None


class CloudflareTokenCreate(BaseModel):
    name: str
    api_token: str
    account_id: Optional[str] = None
    team_id: Optional[int] = None


class CloudflareTokenAccountPatch(BaseModel):
    account_id: str


class CloudflareTokenPatch(BaseModel):
    name: Optional[str] = None
    api_token: Optional[str] = None
    account_id: Optional[str] = None


class LandingProtectionTemplateReq(BaseModel):
    name: str
    rules: dict[str, Any] = Field(default_factory=dict)
    note: Optional[str] = ""
    team_id: Optional[int] = None


class LandingAssetBindingReq(BaseModel):
    name: str
    custom_domain: Optional[str] = ""
    pixel_name: Optional[str] = ""
    pixel_id: Optional[str] = ""
    landing_page_id: Optional[int] = None
    target_urls: list[str] = []
    rotation_mode: str = "sequential"
    link_kind: str = "landing"
    protection_template_id: Optional[int] = None
    note: Optional[str] = ""
    team_id: Optional[int] = None


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


class LandingRuntimeConfigPatch(BaseModel):
    target_urls: list[str] = []
    rotation_mode: str = "sequential"
    tracking_enabled: bool = True
    protection_enabled: bool = False
    protection_rules: dict[str, Any] = Field(default_factory=dict)
    custom_domain: Optional[str] = None
    pixel_id: Optional[str] = None
    template_id: Optional[int] = None


class LandingRuntimeConfigReq(BaseModel):
    page_id: int
    secret: str


class LandingFacebookProbeReq(BaseModel):
    url: str
    act_id: Optional[str] = None


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


class LandingRouteNextReq(BaseModel):
    page_id: int
    secret: str
    path: Optional[str] = None
    referrer: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class LandingAdLinkCreate(BaseModel):
    count: int = Field(default=1, ge=1, le=200)
    target_urls: list[str] = []
    act_id: Optional[str] = None
    account_name: Optional[str] = None
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None
    adset_id: Optional[str] = None
    adset_name: Optional[str] = None
    ad_id: Optional[str] = None
    ad_name: Optional[str] = None
    target_url: Optional[str] = None
    note: Optional[str] = None


class LandingAdAutoBindReq(BaseModel):
    act_ids: list[str] = []
    limit_accounts: int = Field(default=50, ge=1, le=200)
    limit_ads_per_account: int = Field(default=500, ge=1, le=2000)


def _landing_ad_link_create_count(requested_count: int, target_urls: list[str]) -> int:
    """Return how many ad entry links to reserve.

    `target_urls` is the per-ad redirect pool. It must not decide link count:
    one ad entry can rotate through many WhatsApp/target URLs.
    """
    return max(1, min(int(requested_count or 1), 200))


class LandingAdLinkPatch(BaseModel):
    act_id: Optional[str] = None
    account_name: Optional[str] = None
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None
    adset_id: Optional[str] = None
    adset_name: Optional[str] = None
    ad_id: Optional[str] = None
    ad_name: Optional[str] = None
    target_url: Optional[str] = None
    target_urls: Optional[list[str]] = None
    status: Optional[str] = None
    note: Optional[str] = None


class LandingAdLinkResultPatch(BaseModel):
    result_date: Optional[str] = None
    confirmed_actions: int = Field(default=0, ge=0)
    confirmed_sales: int = Field(default=0, ge=0)
    confirmed_revenue: float = Field(default=0, ge=0)
    note: Optional[str] = None
    source: Optional[str] = "manual"


class LandingAdLinkResultImportRow(BaseModel):
    result_date: Optional[str] = None
    slug: Optional[str] = None
    ad_id: Optional[str] = None
    confirmed_actions: int = Field(default=0, ge=0)
    confirmed_sales: int = Field(default=0, ge=0)
    confirmed_revenue: float = Field(default=0, ge=0)
    note: Optional[str] = None


class LandingAdLinkResultImport(BaseModel):
    rows: list[LandingAdLinkResultImportRow] = []
    result_date: Optional[str] = None
    source: Optional[str] = "csv"


def _landing_tracking_cutoffs() -> tuple[str, str]:
    keep_from = datetime.now(CST) - timedelta(days=LANDING_TRACKING_RETENTION_DAYS - 1)
    return keep_from.strftime("%Y-%m-%d 00:00:00"), keep_from.strftime("%Y-%m-%d")


def _cleanup_landing_tracking(conn, force: bool = False) -> None:
    global _landing_cleanup_last
    now = datetime.now(CST)
    if (
        not force
        and _landing_cleanup_last is not None
        and (now - _landing_cleanup_last).total_seconds() < 1800
    ):
        return
    event_cutoff, result_cutoff = _landing_tracking_cutoffs()
    conn.execute("DELETE FROM landing_events WHERE created_at < ?", (event_cutoff,))
    conn.execute("DELETE FROM landing_ad_link_results WHERE result_date < ?", (result_cutoff,))
    _landing_cleanup_last = now


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
            source TEXT DEFAULT 'system',
            original_filename TEXT,
            size_bytes INTEGER DEFAULT 0,
            validation_json TEXT DEFAULT '{}',
            note TEXT,
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_protection_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            rules TEXT DEFAULT '{}',
            note TEXT,
            status TEXT DEFAULT 'active',
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_asset_bindings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            custom_domain TEXT,
            pixel_name TEXT,
            pixel_id TEXT,
            landing_page_id INTEGER,
            target_urls TEXT DEFAULT '[]',
            rotation_mode TEXT DEFAULT 'sequential',
            link_kind TEXT DEFAULT 'landing',
            protection_template_id INTEGER,
            note TEXT,
            status TEXT DEFAULT 'active',
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );
        CREATE INDEX IF NOT EXISTS idx_landing_asset_bindings_scope ON landing_asset_bindings(team_id, owner_user_id, status);
        CREATE INDEX IF NOT EXISTS idx_landing_asset_bindings_page ON landing_asset_bindings(landing_page_id);

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

        CREATE TABLE IF NOT EXISTS landing_route_state (
            page_id INTEGER PRIMARY KEY,
            cursor INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_ad_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            public_url TEXT,
            act_id TEXT,
            account_name TEXT,
            campaign_id TEXT,
            campaign_name TEXT,
            adset_id TEXT,
            adset_name TEXT,
            ad_id TEXT,
            ad_name TEXT,
            target_url TEXT,
            target_urls TEXT DEFAULT '[]',
            status TEXT DEFAULT 'reserved',
            note TEXT,
            team_id INTEGER,
            owner_user_id INTEGER,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );
        CREATE INDEX IF NOT EXISTS idx_landing_ad_links_page ON landing_ad_links(page_id);
        CREATE INDEX IF NOT EXISTS idx_landing_ad_links_ad ON landing_ad_links(ad_id);
        CREATE INDEX IF NOT EXISTS idx_landing_ad_links_act ON landing_ad_links(act_id);

        CREATE TABLE IF NOT EXISTS landing_ad_route_state (
            link_id INTEGER PRIMARY KEY,
            cursor INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        );

        CREATE TABLE IF NOT EXISTS landing_ad_link_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id INTEGER NOT NULL,
            result_date TEXT NOT NULL DEFAULT (date('now','+8 hours')),
            confirmed_actions INTEGER DEFAULT 0,
            confirmed_sales INTEGER DEFAULT 0,
            confirmed_revenue REAL DEFAULT 0,
            source TEXT DEFAULT 'manual',
            note TEXT,
            updated_by TEXT,
            updated_at TEXT DEFAULT (datetime('now','+8 hours')),
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            UNIQUE(link_id, result_date)
        );
        CREATE INDEX IF NOT EXISTS idx_landing_ad_link_results_link ON landing_ad_link_results(link_id);
        """
    )
    try:
        result_cols = {r["name"] for r in conn.execute("PRAGMA table_info(landing_ad_link_results)").fetchall()}
        has_link_only_unique = False
        for idx in conn.execute("PRAGMA index_list(landing_ad_link_results)").fetchall():
            if not int(idx["unique"] or 0):
                continue
            cols = [r["name"] for r in conn.execute(f"PRAGMA index_info({idx['name']})").fetchall()]
            if cols == ["link_id"]:
                has_link_only_unique = True
                break
        if "result_date" not in result_cols or has_link_only_unique:
            old_cols = result_cols
            date_expr = (
                "COALESCE(NULLIF(result_date,''), substr(COALESCE(updated_at,created_at,datetime('now','+8 hours')),1,10))"
                if "result_date" in old_cols
                else "substr(COALESCE(updated_at,created_at,datetime('now','+8 hours')),1,10)"
            )
            conn.execute("DROP TABLE IF EXISTS landing_ad_link_results_new")
            conn.execute(
                """
                CREATE TABLE landing_ad_link_results_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    link_id INTEGER NOT NULL,
                    result_date TEXT NOT NULL DEFAULT (date('now','+8 hours')),
                    confirmed_actions INTEGER DEFAULT 0,
                    confirmed_sales INTEGER DEFAULT 0,
                    confirmed_revenue REAL DEFAULT 0,
                    source TEXT DEFAULT 'manual',
                    note TEXT,
                    updated_by TEXT,
                    updated_at TEXT DEFAULT (datetime('now','+8 hours')),
                    created_at TEXT DEFAULT (datetime('now','+8 hours')),
                    UNIQUE(link_id, result_date)
                )
                """
            )
            conn.execute(
                f"""INSERT OR REPLACE INTO landing_ad_link_results_new
                    (id, link_id, result_date, confirmed_actions, confirmed_sales,
                     confirmed_revenue, source, note, updated_by, updated_at, created_at)
                    SELECT id, link_id, {date_expr},
                           confirmed_actions, confirmed_sales, confirmed_revenue,
                           source, note, updated_by, updated_at, created_at
                    FROM landing_ad_link_results
                    WHERE link_id IS NOT NULL
                    ORDER BY id"""
            )
            conn.execute("DROP TABLE landing_ad_link_results")
            conn.execute("ALTER TABLE landing_ad_link_results_new RENAME TO landing_ad_link_results")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_landing_ad_link_results_link ON landing_ad_link_results(link_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_landing_ad_link_results_date ON landing_ad_link_results(result_date)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_landing_ad_link_results_link_date ON landing_ad_link_results(link_id,result_date)")
    except Exception:
        logger.exception("landing_ad_link_results schema patch failed")
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
            "last_health_status": "TEXT",
            "last_health_summary": "TEXT",
            "last_health_checked_at": "TEXT",
            "last_health_http_code": "INTEGER",
        }
        for name, ddl in page_alters.items():
            if name not in page_cols:
                conn.execute(f"ALTER TABLE landing_pages ADD COLUMN {name} {ddl}")
    except Exception:
        logger.exception("landing_pages schema patch failed")
    try:
        binding_cols = {r["name"] for r in conn.execute("PRAGMA table_info(landing_asset_bindings)").fetchall()}
        if "pixel_name" not in binding_cols:
            conn.execute("ALTER TABLE landing_asset_bindings ADD COLUMN pixel_name TEXT")
    except Exception:
        logger.exception("landing_asset_bindings schema patch failed")
    try:
        ad_link_cols = {r["name"] for r in conn.execute("PRAGMA table_info(landing_ad_links)").fetchall()}
        if "target_urls" not in ad_link_cols:
            conn.execute("ALTER TABLE landing_ad_links ADD COLUMN target_urls TEXT DEFAULT '[]'")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS landing_ad_route_state (
                link_id INTEGER PRIMARY KEY,
                cursor INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now','+8 hours'))
            )"""
        )
    except Exception:
        logger.exception("landing_ad_links schema patch failed")
    try:
        token_cols = {r["name"] for r in conn.execute("PRAGMA table_info(cf_tokens)").fetchall()}
        if "cf_accounts_json" not in token_cols:
            conn.execute("ALTER TABLE cf_tokens ADD COLUMN cf_accounts_json TEXT DEFAULT '[]'")
        if "selected_account_id" not in token_cols:
            conn.execute("ALTER TABLE cf_tokens ADD COLUMN selected_account_id TEXT")
    except Exception:
        logger.exception("cf_tokens schema patch failed")
    try:
        template_cols = {r["name"] for r in conn.execute("PRAGMA table_info(landing_templates)").fetchall()}
        template_alters = {
            "source": "TEXT DEFAULT 'system'",
            "original_filename": "TEXT",
            "size_bytes": "INTEGER DEFAULT 0",
            "validation_json": "TEXT DEFAULT '{}'",
            "note": "TEXT",
        }
        for name, ddl in template_alters.items():
            if name not in template_cols:
                conn.execute(f"ALTER TABLE landing_templates ADD COLUMN {name} {ddl}")
    except Exception:
        logger.exception("landing_templates schema patch failed")
    row = conn.execute("SELECT id, template_path, created_by FROM landing_templates WHERE id=1").fetchone()
    if not row and DEFAULT_TEMPLATE_DIR.exists():
        conn.execute(
            """INSERT INTO landing_templates
               (id, name, template_path, status, created_by)
               VALUES (1, 'Default Template', ?, 'active', 'system')""",
            (str(DEFAULT_TEMPLATE_DIR),),
        )
    else:
        conn.execute(
            "UPDATE landing_templates SET name='Default Template' WHERE id=1 AND COALESCE(created_by,'system')='system'"
        )
        if row and DEFAULT_TEMPLATE_DIR.exists() and str(row["created_by"] or "system") == "system":
            current_path = str(row["template_path"] or "")
            if current_path in {"", str(LEGACY_TEMPLATE_DIR)} or not Path(current_path).exists():
                conn.execute(
                    "UPDATE landing_templates SET template_path=? WHERE id=1 AND COALESCE(created_by,'system')='system'",
                    (str(DEFAULT_TEMPLATE_DIR),),
                )
    conn.execute(
        "UPDATE landing_templates SET name='Default Template' WHERE id=1 AND COALESCE(created_by,'system')='system'"
    )
    try:
        _cleanup_landing_tracking(conn, force=True)
    except Exception:
        logger.exception("landing tracking retention cleanup failed")
    conn.commit()
    conn.close()


_ensure_schema()


def _now_cst() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_result_date(value: Optional[str] = None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(CST).strftime("%Y-%m-%d")
    try:
        return datetime.fromisoformat(raw[:10]).strftime("%Y-%m-%d")
    except Exception:
        raise HTTPException(status_code=400, detail="真实结果日期格式应为 YYYY-MM-DD")


def _truncate(value: Optional[str], limit: int = 255) -> str:
    if value is None:
        return ""
    return str(value).strip()[:limit]


def _provider_label(user=None) -> str:
    return "发布通道"


def _public_provider_error(message: Any, user=None) -> str:
    text = str(message or "").strip()
    replacements = {
        "Cloudflare Pages": "发布通道",
        "Cloudflare API": "发布通道 API",
        "Cloudflare Account API Token": "账号级发布 API Token",
        "Cloudflare account": "发布账号",
        "Cloudflare token": "发布通道 Token",
        "Cloudflare": "发布通道",
        "Pages": "站点发布",
        "R2/S3 Access Key": "对象存储 Access Key",
        "Secret Key": "对象存储 Secret Key",
        "S3 endpoint": "对象存储 endpoint",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def _short_slug(size: int = 8) -> str:
    alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    return "".join(secrets.choice(alphabet) for _ in range(size))


def _safe_zip_member_name(name: str) -> str:
    raw = str(name or "").replace("\\", "/").strip()
    raw = re.sub(r"/+", "/", raw)
    if not raw or raw.startswith("/") or raw.startswith("../") or "/../" in raw or raw == "..":
        raise ValueError(f"Invalid path: {name}")
    if re.match(r"^[A-Za-z]:", raw):
        raise ValueError(f"Invalid path: {name}")
    return raw.strip("/")


def _landing_template_candidate_prefix(names: list[str]) -> tuple[str, str]:
    lowered = {n.lower(): n for n in names}
    for candidate in ("landing.html", "index.html"):
        if candidate in lowered:
            return "", lowered[candidate]
    html_candidates = [n for n in names if n.lower().endswith("/landing.html") or n.lower().endswith("/index.html")]
    if not html_candidates:
        raise HTTPException(status_code=400, detail="模板包缺少入口文件：请在根目录放 landing.html 或 index.html")
    html_candidates.sort(key=lambda n: (len(n.split("/")), 0 if n.lower().endswith("/landing.html") else 1, len(n)))
    entry = html_candidates[0]
    prefix = entry.rsplit("/", 1)[0]
    return prefix, entry


def _validate_landing_template_html(html: str) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    if not re.search(r"\bvar\s+LP_PIXEL_ID\s*=", html):
        errors.append('Missing variable: var LP_PIXEL_ID = "";')
    if not re.search(r"\bvar\s+LP_TARGET_URL\s*=", html):
        errors.append('Missing variable: var LP_TARGET_URL = "";')
    if "LP_TARGET_URL" not in html:
        errors.append("CTA links or forms must use LP_TARGET_URL")
    hard_jump = re.search(
        r"""(?:href|action)\s*=\s*['\"]https?://(?:api\.whatsapp\.com|wa\.me|t\.me|line\.me|m\.me|messenger\.com|facebook\.com/messages|chat\.whatsapp\.com)""",
        html,
        re.I,
    )
    if hard_jump:
        errors.append("Do not hard-code chat or redirect links; use LP_TARGET_URL")
    if re.search(r"\b(fbq\(['\"]init['\"]\s*,\s*['\"]\d{8,}['\"])", html, re.I):
        errors.append("Do not hard-code Pixel ID; use LP_PIXEL_ID")
    if re.search(r"""(?:href|action|location\.href|location\.replace|window\.open)\s*(?:=|\()\s*['\"]https?://""", html, re.I):
        warnings.append("Fixed external links detected; confirm all campaign redirects are controlled by LP_TARGET_URL")
    if "data-lp-cta" not in html and "LP_TARGET_URL" in html:
        warnings.append("Add data-lp-cta to primary CTA elements for cleaner click attribution")
    return errors, warnings



def _validate_landing_template_zip(zip_path: Path) -> dict:
    errors, warnings = [], []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            infos = [info for info in zf.infolist() if not info.is_dir()]
            if not infos:
                raise HTTPException(status_code=400, detail="模板包为空")
            if len(infos) > LANDING_TEMPLATE_MAX_FILES:
                errors.append(f"文件数量过多：{len(infos)}，上限 {LANDING_TEMPLATE_MAX_FILES}")
            names = []
            total_unpacked = 0
            for info in infos:
                try:
                    norm = _safe_zip_member_name(info.filename)
                except ValueError as exc:
                    errors.append(str(exc))
                    continue
                names.append(norm)
                total_unpacked += max(0, int(info.file_size or 0))
                base = norm.rsplit("/", 1)[-1].lower()
                suffix = Path(base).suffix.lower()
                if base in LANDING_TEMPLATE_BLOCKED_NAMES:
                    errors.append(f"Blocked file {base}: runtime files are generated during publish")
                if suffix in LANDING_TEMPLATE_BLOCKED_SUFFIXES:
                    errors.append(f"禁止包含可执行/服务端文件：{norm}")
            if total_unpacked > LANDING_TEMPLATE_MAX_UNPACKED_BYTES:
                errors.append(f"解压后体积过大：{total_unpacked} bytes")
            prefix, entry = _landing_template_candidate_prefix(names)
            html = zf.read(entry).decode("utf-8", errors="ignore")
            html_errors, html_warnings = _validate_landing_template_html(html)
            errors.extend(html_errors)
            warnings.extend(html_warnings)
            return {
                "valid": not errors,
                "errors": errors,
                "warnings": warnings,
                "entry_file": entry,
                "prefix": prefix,
                "file_count": len(infos),
                "unpacked_bytes": total_unpacked,
            }
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="请上传有效的 zip 模板包")


def _extract_landing_template_zip(zip_path: Path, dest: Path, validation: dict) -> None:
    prefix = str(validation.get("prefix") or "").strip("/")
    entry = str(validation.get("entry_file") or "")
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            norm = _safe_zip_member_name(info.filename)
            if prefix:
                if norm != prefix and not norm.startswith(prefix + "/"):
                    continue
                rel = norm[len(prefix) + 1 :]
            else:
                rel = norm
            if not rel:
                continue
            base = rel.rsplit("/", 1)[-1].lower()
            if base in LANDING_TEMPLATE_BLOCKED_NAMES or Path(base).suffix.lower() in LANDING_TEMPLATE_BLOCKED_SUFFIXES:
                continue
            out = dest / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(out, "wb") as dst:
                shutil.copyfileobj(src, dst)
    entry_rel = entry[len(prefix) + 1 :] if prefix and entry.startswith(prefix + "/") else entry
    if entry_rel.lower() == "index.html" and not (dest / "landing.html").exists():
        shutil.copyfile(dest / entry_rel, dest / "landing.html")


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


def _route_url(request: Optional[Request] = None) -> str:
    return _ingest_url(request).replace(
        "/api/landing-pages/events/ingest",
        "/api/landing-pages/router/next",
    )


def _config_url(request: Optional[Request] = None) -> str:
    return _ingest_url(request).replace(
        "/api/landing-pages/events/ingest",
        "/api/landing-pages/edge/config",
    )


def _assert_token_access(conn, token_id: int, user) -> dict:
    row = conn.execute("SELECT * FROM cf_tokens WHERE id=?", (token_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="发布通道 API 不存在")
    if is_superadmin(user):
        return dict(row)
    tid = team_id_for_create(user)
    if row["team_id"] != tid:
        raise HTTPException(status_code=403, detail="发布通道属于其他团队")
    if is_operator_user(user) and row["owner_user_id"] not in (None, user_id(user)):
        raise HTTPException(status_code=403, detail="发布通道属于其他运营")
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


def _template_validation_summary(raw: Optional[str]) -> dict:
    data = _json_loads(raw, {})
    return data if isinstance(data, dict) else {}


def _can_delete_landing_template(item: dict, user) -> bool:
    if not item or int(item.get("id") or 0) == 1:
        return False
    if (item.get("source") or "system") == "system":
        return False
    if is_superadmin(user):
        return True
    return item.get("owner_user_id") is not None and item.get("owner_user_id") == user_id(user)


def _public_landing_template(row, user) -> dict:
    item = dict(row)
    validation = _template_validation_summary(item.get("validation_json"))
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "status": item.get("status"),
        "source": item.get("source") or "system",
        "original_filename": item.get("original_filename"),
        "size_bytes": item.get("size_bytes") or 0,
        "note": item.get("note") or "",
        "team_id": item.get("team_id"),
        "owner_user_id": item.get("owner_user_id"),
        "created_by": item.get("created_by"),
        "created_at": item.get("created_at"),
        "validation": {
            "valid": bool(validation.get("valid", True)),
            "warnings": validation.get("warnings") or [],
            "entry_file": validation.get("entry_file") or "",
            "file_count": validation.get("file_count") or 0,
        },
        "can_delete": _can_delete_landing_template(item, user),
    }


def _public_protection_template(row) -> dict:
    item = dict(row)
    item["rules"] = _safe_rules(_json_loads(item.get("rules"), {}))
    item["team_name"] = item.get("team_name")
    item["owner_user_name"] = item.get("owner_user_name")
    return item


def _assert_protection_template_access(conn, template_id: int, user) -> dict:
    row = conn.execute("SELECT * FROM landing_protection_templates WHERE id=? AND status='active'", (template_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="防护规则模板不存在")
    item = dict(row)
    if is_superadmin(user):
        return item
    tid = team_id_for_create(user)
    if item.get("team_id") != tid:
        raise HTTPException(status_code=403, detail="防护规则模板属于其他团队")
    if is_operator_user(user) and item.get("owner_user_id") not in (None, user_id(user)):
        raise HTTPException(status_code=403, detail="防护规则模板属于其他运营")
    return item


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


def _landing_auto_bind_accounts(conn, page: dict, body: LandingAdAutoBindReq, user) -> list[dict]:
    requested = _clean_act_ids(body.act_ids or [])
    if not requested:
        requested = _clean_act_ids(_json_loads(page.get("bound_act_ids"), []))
    rows = []
    if requested:
        placeholders = ",".join(["?"] * len(requested))
        rows = conn.execute(
            f"""SELECT id, act_id, name FROM accounts
                WHERE REPLACE(COALESCE(act_id,''),'act_','') IN ({placeholders})
                ORDER BY id DESC""",
            requested,
        ).fetchall()
    else:
        where, params = _scope_where(user, "a")
        sql = "SELECT a.id, a.act_id, a.name FROM accounts a"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY a.id DESC LIMIT ?"
        params.append(int(body.limit_accounts or 50))
        rows = conn.execute(sql, params).fetchall()
    out, seen = [], set()
    for row in rows:
        clean = (_clean_act_ids([row["act_id"] or ""]) or [""])[0]
        if not clean or clean in seen:
            continue
        try:
            assert_row_access(conn, "accounts", int(row["id"]), user, allow_unassigned=False)
        except HTTPException:
            continue
        seen.add(clean)
        out.append({"act_id": clean, "name": row["name"] or ""})
    return out[: int(body.limit_accounts or 50)]


def _landing_fetch_ads_for_link_binding(act_id: str, token: str, limit_ads: int) -> tuple[list[dict], Optional[str]]:
    fields = (
        "id,name,status,effective_status,adset_id,campaign_id,"
        "adset{id,name,campaign{id,name}},"
        "creative{id,name,object_story_spec,asset_feed_spec,url_tags,link_url,object_url}"
    )
    url = f"https://graph.facebook.com/v25.0/{_fb_act_id(act_id)}/ads"
    params = {
        "access_token": token,
        "fields": fields,
        "effective_status": json.dumps(["ACTIVE", "PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED", "PENDING_REVIEW"]),
        "limit": 100,
    }
    ads: list[dict] = []
    err_msg: Optional[str] = None
    while url and len(ads) < max(1, int(limit_ads or 500)):
        try:
            resp = requests.get(url, params=params if "graph.facebook.com" in url else None, timeout=35)
            data = resp.json()
        except Exception as exc:
            err_msg = str(exc)
            break
        if isinstance(data, dict) and data.get("error"):
            err = data.get("error") or {}
            err_msg = err.get("message") or str(err)
            break
        batch = (data or {}).get("data") or []
        if not isinstance(batch, list):
            break
        ads.extend([x for x in batch if isinstance(x, dict)])
        next_url = ((data or {}).get("paging") or {}).get("next")
        if not next_url:
            break
        url = next_url
        params = None
    return ads[: max(1, int(limit_ads or 500))], err_msg


def _effective_bind_target_for_link_kind(link_kind: str, bind_target: str) -> str:
    kind = (link_kind or "landing").strip().lower()
    target = (bind_target or "none").strip().lower()
    if target not in {"landing", "form", "both", "none"}:
        return "none"
    if kind == "form" and target in {"landing", "both"}:
        return "form"
    return target


def _public_token(row) -> dict:
    accounts = _public_accounts(row["cf_accounts_json"] if "cf_accounts_json" in row.keys() else "[]")
    selected_account_id = row["selected_account_id"] if "selected_account_id" in row.keys() else None
    selected_account_name = None
    if selected_account_id:
        selected_account_name = next(
            (
                acct.get("name")
                for acct in accounts
                if isinstance(acct, dict) and acct.get("id") == selected_account_id
            ),
            None,
        )
    if not selected_account_name:
        selected_account_name = row["cf_account_name"]
    return {
        "id": row["id"],
        "name": row["name"],
        "token_mask": row["token_mask"],
        "cf_account_id": row["cf_account_id"],
        "cf_account_name": row["cf_account_name"],
        "cf_accounts": accounts,
        "cf_accounts_count": len(accounts),
        "selected_account_id": selected_account_id,
        "selected_account_name": selected_account_name,
        "status": row["status"],
        "last_verified_at": row["last_verified_at"],
        "team_id": row["team_id"],
        "team_name": row["team_name"] if "team_name" in row.keys() else None,
        "owner_user_id": row["owner_user_id"],
        "owner_user_name": row["owner_user_name"] if "owner_user_name" in row.keys() else None,
        "usage_count": int(row["usage_count"] or 0) if "usage_count" in row.keys() else 0,
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _domain_status_usable(domain_status: Any, last_error: Optional[str]) -> bool:
    err = str(last_error or "").strip().lower()
    if err and "custom domain" in err:
        return False
    if not domain_status:
        return False
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


def _custom_domain_runtime_usable(custom_domain: str, worker_enabled: bool = False) -> bool:
    """Treat a custom domain as usable when the live edge runtime responds.

    Cloudflare Pages custom-domain status can remain "pending" for a short
    period even after DNS and Worker routing are already effective. We only use
    this as a fallback when the host clearly responds through the edge runtime.
    """
    host = normalize_custom_domain(custom_domain) if custom_domain else ""
    if not host:
        return False
    urls = [f"https://{host}"]
    if worker_enabled:
        urls.insert(0, f"https://{host}/__edge/redirect")
    for url in urls:
        try:
            resp = requests.get(
                url,
                allow_redirects=False,
                timeout=8,
                headers={
                    "User-Agent": "EdgeDomainProbe/1.0",
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                },
            )
        except Exception:
            continue
        if resp.headers.get("x-edge-block-reason"):
            return True
        content_type = (resp.headers.get("content-type") or "").lower()
        server = (resp.headers.get("server") or "").lower()
        if worker_enabled and resp.status_code in {301, 302, 303, 307, 308}:
            return True
        if resp.status_code == 200 and "html" in content_type and "cloudflare" in server:
            return True
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


def _setup_custom_domain_automation(
    raw_token: str,
    cf_account_id: str,
    project_name: str,
    custom_domain: str,
    pages_url: str,
    user=None,
) -> tuple[Optional[dict], Optional[dict], str, str]:
    """Ensure DNS CNAME and Pages custom-domain binding for a Pages project.

    Returns (dns_result, domain_result, error_text, notice_text). DNS failure is
    intentionally non-fatal so the Pages fallback URL remains publishable.
    """
    custom_domain = normalize_custom_domain(custom_domain)
    dns_result: Optional[dict] = None
    domain_result: Optional[dict] = None
    errors: list[str] = []
    notices: list[str] = []
    if not custom_domain:
        return dns_result, domain_result, "", ""

    try:
        dns_result = ensure_pages_cname_dns_record(
            raw_token,
            cf_account_id,
            project_name,
            custom_domain,
            pages_url,
        )
        action = (dns_result or {}).get("action") or "checked"
        target = (dns_result or {}).get("target") or pages_cname_target(project_name, pages_url)
        notices.append(f"DNS {action}: CNAME {custom_domain} -> {target}")
    except CloudflareError as exc:
        errors.append(f"DNS automation failed: {_public_provider_error(exc, user)}")

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
        status_text = _domain_status_text(domain_result) or "pending"
        if _domain_status_usable(domain_result, None):
            notices.append(f"Custom domain active: {custom_domain}")
        else:
            notices.append(f"Custom domain {custom_domain} is {status_text}; fallback URL is used until it becomes active.")
    except CloudflareError as exc:
        errors.append(f"Custom domain binding failed: {_public_provider_error(exc, user)}")

    return dns_result, domain_result, "\n".join(errors), "\n".join(notices)


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
        item["custom_domain_dns_result"] = raw_response.get("custom_domain_dns_result") or None
        item["custom_domain_runtime_usable"] = bool(raw_response.get("custom_domain_runtime_usable"))
        item["custom_domain_runtime_checked_at"] = raw_response.get("custom_domain_runtime_checked_at") or ""
        item["custom_domain_status_mismatch"] = raw_response.get("custom_domain_status_mismatch") or None
    runtime_domain_usable = bool(isinstance(raw_response, dict) and raw_response.get("custom_domain_runtime_usable"))
    custom_domain_usable = bool(
        custom_domain
        and (
            _domain_status_usable(domain_status, item.get("last_error"))
            or runtime_domain_usable
        )
    )
    item["custom_domain_usable"] = custom_domain_usable
    item["public_url"] = f"https://{custom_domain}" if custom_domain_usable else pages_url
    item["public_url_source"] = "custom_domain" if custom_domain_usable else ("pages_url" if pages_url else "")
    pages_host = urlparse(pages_url).hostname or ""
    item["custom_domain_cname_target"] = pages_host
    item["custom_domain_dns_hint"] = (
        f"CNAME {custom_domain} -> {pages_host}"
        if custom_domain and pages_host
        else ""
    )
    item.pop("raw_response", None)
    item.pop("ingest_secret", None)
    return item


def _public_asset_binding(row) -> dict:
    item = dict(row)
    item["target_urls"] = _json_loads(item.get("target_urls"), [])
    item["landing_page_id"] = item.get("landing_page_id")
    item["protection_template_id"] = item.get("protection_template_id")
    if item.get("page_id") or item.get("page_title"):
        page_public = _public_page(
            {
                "id": item.get("page_id") or item.get("landing_page_id"),
                "title": item.get("page_title"),
                "pages_url": item.get("page_pages_url"),
                "custom_domain": item.get("page_custom_domain"),
                "raw_response": item.get("page_raw_response"),
                "target_urls": item.get("page_target_urls") or "[]",
                "bound_act_ids": "[]",
                "protection_rules": "{}",
                "tracking_enabled": 0,
                "protection_enabled": 0,
                "worker_enabled": 0,
                "last_error": item.get("page_last_error"),
            }
        )
        item["landing_page"] = {
            "id": item.get("page_id") or item.get("landing_page_id"),
            "title": item.get("page_title"),
            "public_url": page_public.get("public_url"),
            "pages_url": item.get("page_pages_url"),
            "custom_domain": item.get("page_custom_domain"),
            "custom_domain_usable": page_public.get("custom_domain_usable"),
        }
    else:
        item["landing_page"] = None
    item["protection_template_name"] = item.get("protection_template_name")
    item["team_name"] = item.get("team_name")
    item["owner_user_name"] = item.get("owner_user_name")
    for key in (
        "page_id",
        "page_title",
        "page_pages_url",
        "page_custom_domain",
        "page_raw_response",
        "page_target_urls",
        "page_last_error",
    ):
        item.pop(key, None)
    return item


def _assert_asset_binding_access(conn, binding_id: int, user) -> dict:
    row = conn.execute("SELECT * FROM landing_asset_bindings WHERE id=? AND status='active'", (binding_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="资产预设不存在")
    item = dict(row)
    if is_superadmin(user):
        return item
    tid = team_id_for_create(user)
    if item.get("team_id") != tid:
        raise HTTPException(status_code=403, detail="资产预设属于其他团队")
    if is_operator_user(user) and item.get("owner_user_id") not in (None, user_id(user)):
        raise HTTPException(status_code=403, detail="资产预设属于其他运营")
    return item


def _asset_binding_values(body: LandingAssetBindingReq, conn, user) -> dict:
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="请填写预设名称")
    page = None
    page_id = int(body.landing_page_id or 0) or None
    if page_id:
        page = _assert_page_access(conn, page_id, user)
    protection_template_id = int(body.protection_template_id or 0) or None
    if protection_template_id:
        _assert_protection_template_access(conn, protection_template_id, user)
    custom_domain_raw = (body.custom_domain or "").strip()
    if not custom_domain_raw and page:
        custom_domain_raw = page.get("custom_domain") or ""
    try:
        custom_domain = normalize_custom_domain(custom_domain_raw) if custom_domain_raw else ""
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    pixel_id = (body.pixel_id or "").strip() or ((page or {}).get("pixel_id") or "")
    pixel_name = (body.pixel_name or "").strip()
    if not pixel_name and pixel_id and not custom_domain and not page_id:
        pixel_name = name
    target_urls = [u.strip() for u in body.target_urls if isinstance(u, str) and u.strip()]
    if not target_urls and page:
        target_urls = [u for u in _json_loads(page.get("target_urls"), []) if isinstance(u, str) and u.strip()]
    if any(not (u.startswith("http://") or u.startswith("https://")) for u in target_urls):
        raise HTTPException(status_code=400, detail="目标链接必须以 http:// 或 https:// 开头")
    rotation_mode = (body.rotation_mode or ((page or {}).get("rotation_mode") or "sequential")).strip().lower()
    if rotation_mode not in {"sequential", "random", "first"}:
        raise HTTPException(status_code=400, detail="rotation_mode must be sequential, random, or first")
    link_kind = (body.link_kind or ((page or {}).get("link_kind") or "landing")).strip().lower()
    if link_kind not in {"landing", "form"}:
        raise HTTPException(status_code=400, detail="link_kind must be landing or form")
    return {
        "name": name[:120],
        "custom_domain": custom_domain,
        "pixel_name": pixel_name[:120],
        "pixel_id": pixel_id[:80],
        "landing_page_id": page_id,
        "target_urls": target_urls,
        "rotation_mode": rotation_mode,
        "link_kind": link_kind,
        "protection_template_id": protection_template_id,
        "note": (body.note or "").strip()[:500],
        "page": page,
    }


def _page_public_url(item: dict) -> str:
    if not item:
        return ""
    public_url = (item.get("public_url") or item.get("pages_url") or "").strip()
    return public_url.rstrip("/")


def _landing_root_url(url: str) -> str:
    base = str(url or "").strip().rstrip("/")
    if not base:
        return ""
    base = re.sub(r"/a/[A-Za-z0-9_-]{4,64}$", "", base)
    if base.endswith("/a"):
        base = base[:-2].rstrip("/")
    return base


def _ad_link_url(page: dict, slug: str) -> str:
    base = _page_public_url(page)
    base = _landing_root_url(base)
    if not base or not slug:
        return ""
    return f"{base}/a/{slug}"


def _normalize_ad_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = re.findall(r"\d{6,}", raw)
    if parts:
        return max(parts, key=len)
    return raw


def _ad_param_url(page: dict, ad_id: str, slug: str = "") -> str:
    base = _page_public_url(page)
    base = _landing_root_url(base)
    ad = _normalize_ad_id(ad_id)
    if not base or not ad:
        return ""
    clean_slug = str(slug or "").strip()
    if clean_slug:
        return f"{base}/a/{clean_slug}?ad={ad}"
    return f"{base}/a?ad={ad}"


def _refresh_page_ad_link_urls(conn, page_id: int, page_public: dict) -> None:
    base = _page_public_url(page_public)
    if not base:
        return
    rows = conn.execute("SELECT id, slug FROM landing_ad_links WHERE page_id=?", (page_id,)).fetchall()
    for row in rows:
        url = f"{base}/a/{row['slug']}"
        conn.execute(
            "UPDATE landing_ad_links SET public_url=?, updated_at=datetime('now','+8 hours') WHERE id=?",
            (url, int(row["id"])),
        )


def _public_ad_link(row, page: Optional[dict] = None, stats: Optional[dict] = None) -> dict:
    item = dict(row)
    item["target_urls"] = _json_loads(item.get("target_urls"), [])
    if page:
        item["public_url"] = _ad_link_url(page, item.get("slug") or "")
    elif item.get("public_url"):
        item["public_url"] = str(item.get("public_url") or "").strip()
    if page:
        item["ad_param_url"] = _ad_param_url(page, item.get("ad_id") or "", item.get("slug") or "")
    if stats:
        item["stats"] = stats
    return item


def _landing_extract_url_strings(value: Any, output: Optional[list[str]] = None) -> list[str]:
    output = output if output is not None else []
    if value is None:
        return output
    if isinstance(value, dict):
        for v in value.values():
            _landing_extract_url_strings(v, output)
        return output
    if isinstance(value, list):
        for v in value:
            _landing_extract_url_strings(v, output)
        return output
    if not isinstance(value, str):
        return output
    text = value.strip()
    if not text:
        return output
    for match in re.findall(r"https?://[^\s\"'<>]+", text):
        output.append(match.rstrip("),.;]"))
    return output


def _landing_slug_from_url(raw_url: str, known_slugs: set[str]) -> str:
    if not raw_url or not known_slugs:
        return ""
    try:
        parsed = urlparse(raw_url)
        path = unquote(parsed.path or "")
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "a" and parts[1] in known_slugs:
            return parts[1]
        qs = parse_qs(parsed.query or "")
        for key in ("sid", "ad_slug", "slug"):
            for val in qs.get(key, []):
                val = str(val or "").strip()
                if val in known_slugs:
                    return val
    except Exception:
        return ""
    return ""


def _landing_ad_nested(obj: dict, *keys: str) -> str:
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(key)
    return str(cur or "").strip()


def _fb_act_id(raw: str) -> str:
    val = str(raw or "").strip()
    if not val:
        return ""
    return val if val.startswith("act_") else f"act_{val}"


def _landing_action_value(actions: list, action_types: list[str]) -> float:
    if not actions or not action_types:
        return 0.0
    wanted = [str(x or "").strip() for x in action_types if str(x or "").strip()]
    for key in wanted:
        for item in actions:
            if str((item or {}).get("action_type") or "") == key:
                try:
                    return float((item or {}).get("value") or 0)
                except (TypeError, ValueError):
                    return 0.0
    return 0.0


def _landing_count_fb_conversions(conn, ad_id: str, act_id: str, actions: list) -> tuple[float, str]:
    kpi_field = ""
    try:
        row = conn.execute(
            """SELECT kpi_field FROM kpi_configs
               WHERE target_id=? AND enabled=1
               ORDER BY CASE WHEN act_id=? THEN 0 ELSE 1 END, id DESC
               LIMIT 1""",
            (ad_id, act_id),
        ).fetchone()
        kpi_field = (row["kpi_field"] if row else "") or ""
    except Exception:
        kpi_field = ""
    try:
        from services.guard_engine import _get_kpi_aliases, _get_kpi_fallback_aliases

        if kpi_field:
            aliases = list(_get_kpi_aliases(kpi_field) or [])
            value = _landing_action_value(actions, aliases)
            if value:
                return value, kpi_field
            value = _landing_action_value(actions, list(_get_kpi_fallback_aliases(kpi_field) or []))
            if value:
                return value, kpi_field
            return 0.0, kpi_field
    except Exception:
        pass
    for field in ("purchase", "offsite_conversion.fb_pixel_purchase", "lead", "onsite_conversion.lead_grouped"):
        value = _landing_action_value(actions, [field])
        if value:
            return value, field
    return 0.0, kpi_field


def _landing_usd_from_currency(amount: float, currency: str) -> float:
    try:
        from services.guard_engine import _local_per_usd_rate

        rate = float(_local_per_usd_rate(currency or "USD") or 1.0)
        return float(amount or 0) / rate if rate > 0 else float(amount or 0)
    except Exception:
        return float(amount or 0)


def _refresh_landing_ad_link_spend(conn, row, result_date: str) -> dict:
    ad_id = str(row["ad_id"] or "").strip()
    act_id = _fb_act_id(row["act_id"] or "")
    if not ad_id:
        return {"ok": False, "reason": "missing_ad_id", "message": "该广告入口还没有绑定广告 ID，无法拉取 FB 消耗"}
    if not act_id:
        return {"ok": False, "reason": "missing_act_id", "message": "该广告入口缺少账户 ID，无法选择读取 Token"}
    acc = conn.execute(
        "SELECT act_id, name, currency FROM accounts WHERE act_id IN (?, ?) ORDER BY id DESC LIMIT 1",
        (act_id, act_id.replace("act_", "")),
    ).fetchone()
    currency = (acc["currency"] if acc else "") or "USD"
    if acc and acc["name"] and not (row["account_name"] or "").strip():
        try:
            conn.execute(
                "UPDATE landing_ad_links SET account_name=?, updated_at=datetime('now','+8 hours') WHERE id=?",
                (str(acc["name"])[:255], int(row["id"])),
            )
            conn.commit()
        except Exception:
            logger.exception("landing ad link account name fill failed: link_id=%s", row["id"])
    token = get_exec_token(act_id, ACTION_READ, notify_exhausted=False)
    if not token:
        return {"ok": False, "reason": "missing_token", "message": "没有可用读取 Token，无法实时拉取广告消耗"}
    try:
        resp = requests.get(
            f"https://graph.facebook.com/v25.0/{ad_id}/insights",
            params={
                "access_token": token,
                "fields": "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,spend,impressions,clicks,actions,action_values",
                "time_range": json.dumps({"since": result_date, "until": result_date}),
                "limit": 25,
            },
            timeout=30,
        )
        data = resp.json()
        if data.get("error"):
            err = data["error"]
            return {
                "ok": False,
                "reason": "fb_api_error",
                "message": err.get("message") or str(err),
                "code": err.get("code"),
                "subcode": err.get("error_subcode"),
            }
        items = data.get("data") or []
        if not items:
            return {"ok": True, "source": "fb_insights_api", "empty": True, "message": "FB 该日期暂无广告消耗数据"}
        item = items[0]
        spend_orig = float(item.get("spend") or 0)
        spend_usd = round(_landing_usd_from_currency(spend_orig, currency), 4)
        actions = item.get("actions") or []
        conversions, kpi_field = _landing_count_fb_conversions(conn, ad_id, act_id, actions)
        impressions = int(float(item.get("impressions") or 0))
        clicks = int(float(item.get("clicks") or 0))
        cpa = round(spend_usd / conversions, 4) if spend_usd > 0 and conversions > 0 else None
        conn.execute(
            "DELETE FROM perf_snapshots WHERE act_id=? AND ad_id=? AND snapshot_date=?",
            (act_id, ad_id, result_date),
        )
        conn.execute(
            """INSERT OR REPLACE INTO perf_snapshots
               (act_id, ad_id, adset_id, campaign_id, ad_name,
                snapshot_date, spend, impressions, clicks,
                conversions, cpa, roas, kpi_field, raw_actions)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                act_id,
                ad_id,
                item.get("adset_id") or row["adset_id"] or "",
                item.get("campaign_id") or row["campaign_id"] or "",
                item.get("ad_name") or row["ad_name"] or ad_id,
                result_date,
                spend_usd,
                impressions,
                clicks,
                conversions,
                cpa,
                None,
                kpi_field,
                json.dumps(actions, ensure_ascii=False),
            ),
        )
        from core.perf_history import append_perf_snapshot_history

        append_perf_snapshot_history(
            conn,
            act_id=act_id,
            ad_id=ad_id,
            adset_id=item.get("adset_id") or row["adset_id"] or "",
            campaign_id=item.get("campaign_id") or row["campaign_id"] or "",
            ad_name=item.get("ad_name") or row["ad_name"] or ad_id,
            snapshot_date=result_date,
            spend=spend_usd,
            impressions=impressions,
            clicks=clicks,
            conversions=conversions,
            cpa=cpa,
            roas=None,
            kpi_field=kpi_field,
            raw_actions=json.dumps(actions, ensure_ascii=False),
            currency="USD",
        )
        updates, params = [], []
        for db_key, api_key in (
            ("ad_name", "ad_name"),
            ("adset_id", "adset_id"),
            ("adset_name", "adset_name"),
            ("campaign_id", "campaign_id"),
            ("campaign_name", "campaign_name"),
        ):
            val = (item.get(api_key) or "").strip() if isinstance(item.get(api_key), str) else item.get(api_key)
            if val and not row[db_key]:
                updates.append(f"{db_key}=?")
                params.append(str(val)[:255])
        if updates:
            updates.append("updated_at=datetime('now','+8 hours')")
            params.append(int(row["id"]))
            conn.execute(f"UPDATE landing_ad_links SET {', '.join(updates)} WHERE id=?", params)
        conn.commit()
        return {
            "ok": True,
            "source": "fb_insights_api",
            "spend": spend_usd,
            "spend_original": spend_orig,
            "currency": currency,
            "conversions": conversions,
            "kpi_field": kpi_field,
        }
    except Exception as exc:
        logger.exception("landing ad link live spend refresh failed: link_id=%s ad_id=%s", row["id"], ad_id)
        return {"ok": False, "reason": "exception", "message": str(exc)[:300]}


def _ad_link_decision(stats: dict) -> dict:
    spend = float(stats.get("spend") or 0)
    has_confirmed = bool(stats.get("has_confirmed_result"))
    if has_confirmed:
        true_contact = float(stats.get("confirmed_actions") or 0)
        metric = "confirmed"
        cost = stats.get("cost_per_confirmed_action")
    else:
        true_contact = float(stats.get("effective_true_contact", stats.get("true_contact") or 0) or 0)
        metric = "unique" if stats.get("dedupe_available") else "raw"
        cost = stats.get("cost_per_effective_true_contact") or stats.get("cost_per_true_contact")
    visits = float(stats.get("unique_visit", stats.get("visit") or 0) or 0)
    clicks = float(stats.get("unique_click", stats.get("click") or 0) or 0)
    if true_contact > 0:
        return {
            "state": "good",
            "label": "true_action",
            "reason": f"{metric}_true_action={true_contact:g}; cost={cost or '--'}",
            "metric": metric,
        }
    if spend > 0:
        return {
            "state": "waste",
            "label": "spend_no_action",
            "reason": f"spend_usd={spend:.2f}; {metric}_true_action=0",
            "metric": metric,
        }
    if visits > 0 or clicks > 0:
        return {
            "state": "watch",
            "label": "traffic_no_action",
            "reason": f"{metric}_visits={visits:g}; {metric}_clicks={clicks:g}; true_action=0",
            "metric": metric,
        }
    return {"state": "no_data", "label": "no_data", "reason": "no events or spend", "metric": metric}


def _ad_link_stats(
    conn,
    page_id: int,
    slug: str,
    ad_id: str = "",
    days: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict:
    path = f"/a/{slug}"
    ad_id = _normalize_ad_id(ad_id)
    date_params = []
    result_date_from = None
    result_date_to = None
    if date_from or date_to:
        where_parts = []
        try:
            if date_from:
                start_day = datetime.fromisoformat(str(date_from)[:10]).strftime("%Y-%m-%d")
                start = f"{start_day} 00:00:00"
                where_parts.append("created_at>=?")
                date_params.append(start)
                result_date_from = start_day
            if date_to:
                end_day = datetime.fromisoformat(str(date_to)[:10]).strftime("%Y-%m-%d")
                end_dt = datetime.fromisoformat(end_day) + timedelta(days=1)
                where_parts.append("created_at<?")
                date_params.append(end_dt.strftime("%Y-%m-%d 00:00:00"))
                result_date_to = end_day
        except Exception:
            where_parts = []
            date_params = []
            result_date_from = None
            result_date_to = None
    else:
        where_parts = []
    if not where_parts and days:
        try:
            d = max(1, min(int(days), LANDING_TRACKING_RETENTION_DAYS))
            since = (datetime.now(CST) - timedelta(days=d - 1)).strftime("%Y-%m-%d 00:00:00")
            where_parts = ["created_at>=?"]
            date_params = [since]
            result_date_from = since[:10]
            result_date_to = datetime.now(CST).strftime("%Y-%m-%d")
        except Exception:
            where_parts = []
            date_params = []
            result_date_from = None
            result_date_to = None
    where_extra = (" AND " + " AND ".join(where_parts)) if where_parts else ""
    scope_parts = ["path=?"]
    event_params = [
        page_id,
        path,
    ]
    if slug:
        scope_parts.extend([
            "COALESCE(metadata,'') LIKE ?",
            "COALESCE(metadata,'') LIKE ?",
        ])
        event_params.extend([
            f'%"ad_slug":"{slug}"%',
            f'%"ad_slug": "{slug}"%',
        ])
    if ad_id:
        scope_parts.extend([
            "COALESCE(metadata,'') LIKE ?",
            "COALESCE(metadata,'') LIKE ?",
            "COALESCE(metadata,'') LIKE ?",
            "COALESCE(metadata,'') LIKE ?",
        ])
        event_params.extend([
            f'%"ad_id":"{ad_id}"%',
            f'%"ad_id": "{ad_id}"%',
            f'%"ad":"{ad_id}"%',
            f'%"ad": "{ad_id}"%',
        ])
    event_scope = "page_id=? AND (" + " OR ".join(scope_parts) + ")"
    params = event_params + date_params
    rows = conn.execute(
        f"""SELECT event_type, COUNT(*) AS cnt
           FROM landing_events
           WHERE {event_scope}{where_extra}
           GROUP BY event_type""",
        params,
    ).fetchall()
    out = {r["event_type"]: int(r["cnt"] or 0) for r in rows}
    whatsapp_click = 0
    whatsapp_redirect = 0
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS cnt
               FROM landing_events
               WHERE """
            + event_scope
            + """ AND event_type='click'
            """
            + where_extra
            + """
                  AND (
                   lower(COALESCE(target_url,'')) LIKE '%whatsapp%'
                   OR lower(COALESCE(target_url,'')) LIKE '%wa.me/%'
                   OR lower(COALESCE(target_url,'')) LIKE '%api.whatsapp.com%'
                  )""",
            params,
        ).fetchone()
        whatsapp_click = int(row["cnt"] or 0) if row else 0
        row = conn.execute(
            """SELECT COUNT(*) AS cnt
               FROM landing_events
               WHERE """
            + event_scope
            + """ AND event_type='redirect'
            """
            + where_extra
            + """
                  AND (
                   lower(COALESCE(target_url,'')) LIKE '%whatsapp%'
                   OR lower(COALESCE(target_url,'')) LIKE '%wa.me/%'
                   OR lower(COALESCE(target_url,'')) LIKE '%api.whatsapp.com%'
                  )""",
            params,
        ).fetchone()
        whatsapp_redirect = int(row["cnt"] or 0) if row else 0
    except Exception:
        whatsapp_click = 0
        whatsapp_redirect = 0

    fp_expr = "NULLIF(COALESCE(NULLIF(ip_hash,''),'') || '|' || COALESCE(NULLIF(user_agent_hash,''),''), '|')"
    unique = {}
    unique_whatsapp_click = 0
    unique_whatsapp_redirect = 0
    unique_true_contact = 0
    dedupe_available = False
    try:
        unique_rows = conn.execute(
            f"""SELECT event_type, COUNT(DISTINCT {fp_expr}) AS cnt
               FROM landing_events
               WHERE {event_scope}{where_extra}
                 AND {fp_expr} IS NOT NULL
               GROUP BY event_type""",
            params,
        ).fetchall()
        unique = {r["event_type"]: int(r["cnt"] or 0) for r in unique_rows}
        dedupe_available = any(int(v or 0) > 0 for v in unique.values())
        row = conn.execute(
            f"""SELECT COUNT(DISTINCT {fp_expr}) AS cnt
               FROM landing_events
               WHERE {event_scope} AND event_type='click'
            """
            + where_extra
            + f"""
                 AND {fp_expr} IS NOT NULL
                 AND (
                   lower(COALESCE(target_url,'')) LIKE '%whatsapp%'
                   OR lower(COALESCE(target_url,'')) LIKE '%wa.me/%'
                   OR lower(COALESCE(target_url,'')) LIKE '%api.whatsapp.com%'
                 )""",
            params,
        ).fetchone()
        unique_whatsapp_click = int(row["cnt"] or 0) if row else 0
        row = conn.execute(
            f"""SELECT COUNT(DISTINCT {fp_expr}) AS cnt
               FROM landing_events
               WHERE {event_scope} AND event_type='redirect'
            """
            + where_extra
            + f"""
                 AND {fp_expr} IS NOT NULL
                 AND (
                   lower(COALESCE(target_url,'')) LIKE '%whatsapp%'
                   OR lower(COALESCE(target_url,'')) LIKE '%wa.me/%'
                   OR lower(COALESCE(target_url,'')) LIKE '%api.whatsapp.com%'
                 )""",
            params,
        ).fetchone()
        unique_whatsapp_redirect = int(row["cnt"] or 0) if row else 0
        row = conn.execute(
            f"""SELECT COUNT(DISTINCT {fp_expr}) AS cnt
               FROM landing_events
               WHERE {event_scope}{where_extra}
                 AND {fp_expr} IS NOT NULL
                 AND (
                   event_type IN ('submit','redirect')
                   OR (
                     event_type='click'
                     AND (
                       lower(COALESCE(target_url,'')) LIKE '%whatsapp%'
                       OR lower(COALESCE(target_url,'')) LIKE '%wa.me/%'
                       OR lower(COALESCE(target_url,'')) LIKE '%api.whatsapp.com%'
                     )
                   )
                 )""",
            params,
        ).fetchone()
        unique_true_contact = int(row["cnt"] or 0) if row else 0
    except Exception:
        unique = {}
        unique_whatsapp_click = 0
        unique_whatsapp_redirect = 0
        unique_true_contact = 0
        dedupe_available = False

    spend = 0.0
    fb_conversions = 0.0
    fb_clicks = 0.0
    impressions = 0.0
    last_synced_at = None
    spend_source = ""
    link_row = None
    try:
        link_row = conn.execute(
            """SELECT id, ad_id FROM landing_ad_links
               WHERE page_id=?
                 AND (slug=? OR (?<>'' AND ad_id=?))
               ORDER BY CASE WHEN slug=? THEN 0 ELSE 1 END, id DESC
               LIMIT 1""",
            (page_id, slug, ad_id, ad_id, slug),
        ).fetchone()
        ad_id = _normalize_ad_id((link_row["ad_id"] if link_row else "") or ad_id)
        if ad_id:
            if result_date_from or result_date_to:
                try:
                    ensure_perf_snapshot_history_schema(conn)
                    hist_where = ["ad_id=?", "COALESCE(ad_id,'')<>''"]
                    hist_params = [ad_id]
                    if result_date_from:
                        hist_where.append("snapshot_date>=?")
                        hist_params.append(result_date_from)
                    if result_date_to:
                        hist_where.append("snapshot_date<=?")
                        hist_params.append(result_date_to)
                    hist_row = conn.execute(
                        f"""SELECT SUM(day_spend) AS spend,
                                   SUM(day_conversions) AS conv,
                                   SUM(day_clicks) AS clicks,
                                   SUM(day_impressions) AS impressions,
                                   MAX(day_synced_at) AS last_synced_at
                            FROM (
                                SELECT snapshot_date,
                                       MAX(COALESCE(spend,0)) AS day_spend,
                                       MAX(COALESCE(conversions,0)) AS day_conversions,
                                       MAX(COALESCE(clicks,0)) AS day_clicks,
                                       MAX(COALESCE(impressions,0)) AS day_impressions,
                                       MAX(snapshot_at) AS day_synced_at
                                FROM perf_snapshot_history
                                WHERE {' AND '.join(hist_where)}
                                GROUP BY snapshot_date
                            )""",
                        hist_params,
                    ).fetchone()
                    if hist_row and hist_row["last_synced_at"]:
                        spend = float(hist_row["spend"] or 0)
                        fb_conversions = float(hist_row["conv"] or 0)
                        fb_clicks = float(hist_row["clicks"] or 0)
                        impressions = float(hist_row["impressions"] or 0)
                        last_synced_at = hist_row["last_synced_at"]
                        spend_source = "perf_snapshot_history"
                except Exception:
                    logger.exception("landing ad link history spend lookup failed: page_id=%s slug=%s", page_id, slug)
            if not spend_source:
                snapshot_where = ["ad_id=?"]
                snapshot_params = [ad_id]
                if result_date_from:
                    snapshot_where.append("snapshot_date>=?")
                    snapshot_params.append(result_date_from)
                if result_date_to:
                    snapshot_where.append("snapshot_date<=?")
                    snapshot_params.append(result_date_to)
                try:
                    snapshot_row = conn.execute(
                        f"""SELECT SUM(COALESCE(spend,0)) AS spend,
                                   SUM(COALESCE(conversions,0)) AS conv,
                                   SUM(COALESCE(clicks,0)) AS clicks,
                                   SUM(COALESCE(impressions,0)) AS impressions,
                                   MAX(COALESCE(snapshot_at, snapshot_date)) AS last_synced_at
                            FROM perf_snapshots
                            WHERE {' AND '.join(snapshot_where)}""",
                        snapshot_params,
                    ).fetchone()
                    if snapshot_row and snapshot_row["last_synced_at"]:
                        spend = float(snapshot_row["spend"] or 0)
                        fb_conversions = float(snapshot_row["conv"] or 0)
                        fb_clicks = float(snapshot_row["clicks"] or 0)
                        impressions = float(snapshot_row["impressions"] or 0)
                        last_synced_at = snapshot_row["last_synced_at"]
                        spend_source = "perf_snapshots"
                except Exception:
                    pass
            if not spend_source:
                spend_row = conn.execute(
                    """SELECT SUM(COALESCE(spend,0)) AS spend,
                              SUM(COALESCE(conv,0)) AS conv,
                              SUM(COALESCE(clicks,0)) AS clicks,
                              SUM(COALESCE(impressions,0)) AS impressions,
                              MAX(last_synced_at) AS last_synced_at
                       FROM asset_spend_log
                       WHERE fb_ad_id=?""",
                    (ad_id,),
                ).fetchone()
                if spend_row:
                    spend = float(spend_row["spend"] or 0)
                    fb_conversions = float(spend_row["conv"] or 0)
                    fb_clicks = float(spend_row["clicks"] or 0)
                    impressions = float(spend_row["impressions"] or 0)
                    last_synced_at = spend_row["last_synced_at"]
                    spend_source = "asset_spend_log"
    except Exception:
        pass

    confirmed_actions = 0
    confirmed_sales = 0
    confirmed_revenue = 0.0
    confirmed_source = ""
    confirmed_note = ""
    confirmed_updated_at = None
    confirmed_result_date = ""
    confirmed_result_count = 0
    has_confirmed_result = False
    try:
        link_id = int(link_row["id"]) if link_row else 0
        if link_id:
            result_where = ["link_id=?"]
            result_params = [link_id]
            if result_date_from:
                result_where.append("result_date>=?")
                result_params.append(result_date_from)
            if result_date_to:
                result_where.append("result_date<=?")
                result_params.append(result_date_to)
            where_sql = " AND ".join(result_where)
            result_row = conn.execute(
                f"""SELECT COUNT(*) AS row_count,
                           SUM(COALESCE(confirmed_actions,0)) AS confirmed_actions,
                           SUM(COALESCE(confirmed_sales,0)) AS confirmed_sales,
                           SUM(COALESCE(confirmed_revenue,0)) AS confirmed_revenue,
                           MAX(updated_at) AS updated_at
                    FROM landing_ad_link_results
                    WHERE {where_sql}""",
                result_params,
            ).fetchone()
            if result_row:
                confirmed_result_count = int(result_row["row_count"] or 0)
                has_confirmed_result = confirmed_result_count > 0
                confirmed_actions = int(result_row["confirmed_actions"] or 0)
                confirmed_sales = int(result_row["confirmed_sales"] or 0)
                confirmed_revenue = float(result_row["confirmed_revenue"] or 0)
                confirmed_updated_at = result_row["updated_at"]
            latest_row = conn.execute(
                f"""SELECT source, note, updated_at, result_date
                    FROM landing_ad_link_results
                    WHERE {where_sql}
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1""",
                result_params,
            ).fetchone()
            if latest_row:
                confirmed_source = latest_row["source"] or ""
                confirmed_note = latest_row["note"] or ""
                confirmed_updated_at = latest_row["updated_at"] or confirmed_updated_at
                confirmed_result_date = latest_row["result_date"] or ""
    except Exception:
        confirmed_actions = 0
        confirmed_sales = 0
        confirmed_revenue = 0.0
        confirmed_source = ""
        confirmed_note = ""
        confirmed_updated_at = None
        confirmed_result_date = ""
        confirmed_result_count = 0
        has_confirmed_result = False

    def _cost(n: int | float) -> Optional[float]:
        try:
            n = float(n or 0)
        except Exception:
            n = 0.0
        if spend <= 0 or n <= 0:
            return None
        return round(spend / n, 4)

    visits = out.get("visit", 0)
    clicks = out.get("click", 0)
    redirects = out.get("redirect", 0)
    submits = out.get("submit", 0)
    true_contact = max(whatsapp_redirect, submits, redirects, whatsapp_click)
    effective_true_contact = unique_true_contact if dedupe_available else true_contact
    final_true_contact = confirmed_actions if has_confirmed_result else effective_true_contact
    final_metric_mode = "confirmed" if has_confirmed_result else ("unique" if dedupe_available else "raw")
    return {
        "visit": visits,
        "click": clicks,
        "redirect": redirects,
        "submit": submits,
        "block": out.get("block", 0),
        "unique_visit": unique.get("visit", 0),
        "unique_click": unique.get("click", 0),
        "unique_redirect": unique.get("redirect", 0),
        "unique_submit": unique.get("submit", 0),
        "unique_block": unique.get("block", 0),
        "whatsapp_click": whatsapp_click,
        "whatsapp_redirect": whatsapp_redirect,
        "unique_whatsapp_click": unique_whatsapp_click,
        "unique_whatsapp_redirect": unique_whatsapp_redirect,
        "message_click": whatsapp_redirect or whatsapp_click,
        "true_contact": true_contact,
        "unique_true_contact": unique_true_contact,
        "effective_true_contact": effective_true_contact,
        "confirmed_actions": confirmed_actions,
        "confirmed_sales": confirmed_sales,
        "confirmed_revenue": round(confirmed_revenue, 4),
        "confirmed_result_source": confirmed_source,
        "confirmed_result_note": confirmed_note,
        "confirmed_result_updated_at": confirmed_updated_at,
        "confirmed_result_date": confirmed_result_date,
        "confirmed_result_count": confirmed_result_count,
        "has_confirmed_result": has_confirmed_result,
        "final_true_contact": final_true_contact,
        "dedupe_available": dedupe_available,
        "metric_mode": "unique" if dedupe_available else "raw",
        "final_metric_mode": final_metric_mode,
        "spend": round(spend, 4),
        "fb_conversions": fb_conversions,
        "fb_clicks": fb_clicks,
        "impressions": impressions,
        "cost_per_visit": _cost(visits),
        "cost_per_click": _cost(clicks),
        "cost_per_unique_visit": _cost(unique.get("visit", 0)),
        "cost_per_unique_click": _cost(unique.get("click", 0)),
        "cost_per_whatsapp_click": _cost(whatsapp_click),
        "cost_per_whatsapp_redirect": _cost(whatsapp_redirect),
        "cost_per_unique_whatsapp_click": _cost(unique_whatsapp_click),
        "cost_per_unique_whatsapp_redirect": _cost(unique_whatsapp_redirect),
        "cost_per_redirect": _cost(redirects),
        "cost_per_submit": _cost(submits),
        "cost_per_true_contact": _cost(true_contact),
        "cost_per_unique_true_contact": _cost(unique_true_contact),
        "cost_per_effective_true_contact": _cost(effective_true_contact),
        "cost_per_confirmed_action": _cost(confirmed_actions),
        "cost_per_confirmed_sale": _cost(confirmed_sales),
        "cost_per_final_true_contact": _cost(final_true_contact),
        "last_synced_at": last_synced_at,
        "spend_source": spend_source,
        "total": sum(out.values()),
    }


def _target_location_matches(location: str, targets: list[str]) -> bool:
    loc = (location or "").strip()
    if not loc:
        return False
    loc_norm = _normalize_url_for_match(loc)
    for target in targets or []:
        target_norm = _normalize_url_for_match(target)
        if target_norm and (loc_norm == target_norm or loc_norm.startswith(target_norm + "?")):
            return True
    return False


def _ad_slug_from_path(path: str) -> str:
    raw = (path or "").strip().split("?", 1)[0].strip("/")
    parts = raw.split("/")
    if len(parts) == 2 and parts[0] == "a":
        slug = parts[1].strip()
        if 4 <= len(slug) <= 64 and all(ch.isalnum() or ch in "_-" for ch in slug):
            return slug
    return ""


def _target_is_current_landing(target_url: str, page: dict) -> bool:
    target = _normalize_url_for_match(target_url)
    if not target:
        return True
    if target.startswith("/__edge/"):
        return True
    candidates = []
    for key in ("pages_url", "public_url"):
        val = (page.get(key) or "").strip() if page else ""
        if val:
            candidates.append(_normalize_url_for_match(val))
    custom_domain = (page.get("custom_domain") or "").strip() if page else ""
    if custom_domain:
        candidates.append(_normalize_url_for_match("https://" + custom_domain))
    for base in [c for c in candidates if c]:
        if target == base or target.startswith(base + "/a/") or target.startswith(base + "/__edge/"):
            return True
    return False


def _landing_ad_link_targets(link: Any, page: dict) -> list[str]:
    if not link:
        return []
    data = dict(link)
    targets = [
        u.strip()
        for u in _json_loads(data.get("target_urls"), [])
        if isinstance(u, str) and u.strip()
    ]
    if not targets and data.get("target_url"):
        targets = [str(data.get("target_url") or "").strip()]
    return [
        u
        for u in targets
        if (u.startswith("http://") or u.startswith("https://")) and not _target_is_current_landing(u, page)
    ][:200]


def _select_landing_ad_target(conn, link: Any, page: dict, mode: str) -> Optional[dict]:
    targets = _landing_ad_link_targets(link, page)
    if not targets:
        return None
    link_id = int(link["id"])
    mode = str(mode or "sequential").strip().lower()
    if mode == "first" or len(targets) == 1:
        idx = 0
    elif mode == "random":
        idx = secrets.randbelow(len(targets))
    else:
        state = conn.execute(
            "SELECT cursor FROM landing_ad_route_state WHERE link_id=?",
            (link_id,),
        ).fetchone()
        cursor = int(state["cursor"] or 0) if state else 0
        idx = cursor % len(targets)
        conn.execute(
            """INSERT INTO landing_ad_route_state (link_id, cursor, updated_at)
               VALUES (?, ?, datetime('now','+8 hours'))
               ON CONFLICT(link_id) DO UPDATE SET
                 cursor=excluded.cursor,
                 updated_at=excluded.updated_at""",
            (link_id, cursor + 1),
        )
    return {
        "target_url": targets[idx],
        "index": idx,
        "total": len(targets),
        "mode": "ad_link_" + (mode if mode in {"first", "random"} else "sequential"),
    }


def _health_status_from_checks(checks: list[dict[str, Any]]) -> str:
    if any(c.get("status") == "fail" for c in checks):
        return "fail"
    if any(c.get("status") == "warn" for c in checks):
        return "warn"
    return "pass"


def _health_summary_from_checks(checks: list[dict[str, Any]]) -> str:
    important = [c for c in checks if c.get("status") in {"fail", "warn"}]
    if important:
        first = important[0]
        label = str(first.get("label") or first.get("key") or "check")
        detail = str(first.get("detail") or "").strip()
        return (label + (f": {detail}" if detail else ""))[:240]
    return "All checks passed"


def _stable_landing_health_checks(
    checks: list[dict[str, Any]],
    item: dict,
    targets: list[str],
    link_kind: str,
    http_info: dict[str, Any],
) -> list[dict[str, Any]]:
    status_raw = str(item.get("status") or "").strip().lower()
    custom_domain = (item.get("custom_domain") or "").strip()
    public_url = (item.get("public_url") or "").strip()
    pages_url = (item.get("pages_url") or "").strip()
    target_count = len(targets or [])
    http_code = http_info.get("status_code") if http_info else None
    location = str(http_info.get("location") or "") if http_info else ""
    content_type = str(http_info.get("content_type") or "") if http_info else ""
    blocked_by_protection = bool(str(http_info.get("block_reason") or "").strip()) if http_info else False
    protection_block_ok = blocked_by_protection and bool(item.get("protection_enabled"))
    out: list[dict[str, Any]] = []
    for raw in checks:
        check = dict(raw)
        key = str(check.get("key") or "")
        state = str(check.get("status") or "")
        if key == "status":
            check["label"] = "Publish status"
            check["detail"] = "Published" if status_raw == "published" else f"Current status: {status_raw or 'unknown'}"
        elif key == "public_url":
            check["label"] = "Public URL"
            check["detail"] = public_url or "No reachable fallback or custom-domain URL"
        elif key == "custom_domain":
            check["label"] = "Custom domain"
            if custom_domain:
                check["detail"] = (
                    f"https://{custom_domain} is the primary URL"
                    if item.get("custom_domain_usable")
                    else f"Domain is not active yet; currently using the fallback URL. {item.get('custom_domain_dns_hint') or 'Finish the CNAME record in DNS.'}"
                )
            elif pages_url:
                check["detail"] = "No custom domain configured; using the default fallback domain"
        elif key == "targets":
            check["label"] = "Redirect targets"
            check["detail"] = f"{target_count} target link(s) configured" if target_count else "No redirect target configured"
        elif key == "form_mode":
            check["label"] = "Form direct-link mode"
            check["detail"] = "Root path redirects directly to the rotating target" if item.get("worker_enabled") else "Form direct-link mode requires dynamic routing"
        elif key == "landing_mode":
            check["label"] = "Landing-page mode"
            check["detail"] = "Root path returns HTML; button clicks redirect to rotating targets"
        elif key == "worker":
            check["label"] = "Runtime config / tracking / protection"
            if item.get("worker_enabled"):
                check["detail"] = f"Runtime config enabled; tracking {'on' if item.get('tracking_enabled') else 'off'}, protection {'on' if item.get('protection_enabled') else 'off'}"
            else:
                check["detail"] = "Dynamic routing is disabled; tracking, protection and server-side rotation are unavailable"
        elif key == "runtime_redirect":
            check["label"] = "Live redirect"
            if protection_block_ok:
                check["status"] = "pass"
                check["detail"] = "Protection rules are active; this probe was redirected to the fallback URL"
            elif state == "pass":
                check["detail"] = f"Root path returned HTTP {http_code} and redirected to a configured target"
            elif state == "warn" and blocked_by_protection:
                check["detail"] = "The request was blocked by protection rules; test again from an allowed environment"
            elif state == "warn" and http_code in {301, 302, 303, 307, 308}:
                check["detail"] = f"Root path redirected, but Location is not in the current target list: {location[:180]}"
            elif state == "warn":
                check["detail"] = "The request was blocked by protection rules; test again from an allowed environment"
            else:
                check["detail"] = f"Form direct-link mode expects a 302 redirect; got HTTP {http_code or '--'}"
        elif key == "runtime_page":
            check["label"] = "Live page"
            if protection_block_ok:
                check["status"] = "pass"
                check["detail"] = "Protection rules are active; this probe was redirected to the fallback URL"
            elif state == "pass":
                check["detail"] = "Public URL returns HTML and the landing page is reachable"
            elif state == "warn" and (http_code == 403 or blocked_by_protection):
                check["detail"] = "The request was blocked by protection rules; the page may still be healthy"
            else:
                check["detail"] = f"Public URL returned HTTP {http_code or '--'}, Content-Type: {content_type or '--'}"
        elif key == "runtime_http":
            check["label"] = "Live access"
            if not str(check.get("detail") or "").strip():
                check["detail"] = "Public URL request failed"
        elif key == "runtime_worker_route":
            check["label"] = "Edge route"
            if state == "pass":
                check["detail"] = "Edge runtime is active; /__edge/redirect is handled"
            elif state == "warn" and "protection" in str(check.get("detail") or "").lower():
                check["status"] = "pass"
                check["detail"] = "Edge runtime is active; protection rules intercepted this probe"
            elif state == "warn":
                check["detail"] = "Runtime route was blocked by current protection rules; this still proves dynamic routing is active"
            else:
                check["detail"] = str(check.get("detail") or "Edge route is not active; republish the page once")
        out.append(check)
    return out


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


def _fb_probe_error_looks_blocked(message: str) -> bool:
    text = str(message or "").lower()
    hints = [
        "blocked",
        "spam",
        "malicious",
        "unsafe",
        "security",
        "violat",
        "policy",
        "can't be crawled",
        "cannot be crawled",
        "could not resolve",
        "ssl",
        "certificate",
        "redirect",
    ]
    return any(h in text for h in hints)


def _landing_facebook_probe_token(conn, page: dict, user, preferred_act_id: str = "") -> tuple[str, str]:
    act_ids: list[str] = []
    if preferred_act_id:
        act_ids.extend(_clean_act_ids([preferred_act_id]))
    act_ids.extend(_clean_act_ids(_json_loads(page.get("bound_act_ids"), [])))
    try:
        rows = conn.execute(
            """SELECT act_id FROM landing_ad_links
               WHERE page_id=? AND COALESCE(act_id,'')!=''
               ORDER BY updated_at DESC, id DESC LIMIT 20""",
            (page.get("id"),),
        ).fetchall()
        act_ids.extend(_clean_act_ids([r["act_id"] for r in rows]))
    except Exception:
        pass
    if not act_ids:
        try:
            where, params = _scope_where(user, "a")
            sql = "SELECT a.act_id FROM accounts a WHERE COALESCE(a.act_id,'')!=''"
            if where:
                sql += " AND " + " AND ".join(where)
            sql += " ORDER BY a.updated_at DESC, a.id DESC LIMIT 20"
            rows = conn.execute(sql, params).fetchall()
            act_ids.extend(_clean_act_ids([r["act_id"] for r in rows]))
        except Exception:
            pass
    seen = set()
    for act_id in act_ids:
        clean = (_clean_act_ids([act_id]) or [""])[0]
        if not clean or clean in seen:
            continue
        seen.add(clean)
        token = get_exec_token(_fb_act_id(clean), ACTION_READ, notify_exhausted=False)
        if token:
            return token, clean
    return "", ""


def _facebook_url_probe_check(url: str, token: str, token_source: str = "") -> dict[str, Any]:
    clean = str(url or "").strip()
    if not clean:
        return {
            "key": "facebook_scrape",
            "status": "warn",
            "label": "Facebook 抓取",
            "detail": "没有可探测的公开链接",
        }
    if not token:
        return {
            "key": "facebook_scrape",
            "status": "warn",
            "label": "Facebook 抓取",
            "detail": "未找到可用的 FB 可读 Token，已跳过抓取探测",
        }
    endpoint = "https://graph.facebook.com/v25.0/"
    payload = {
        "id": clean,
        "scrape": "true",
        "access_token": token,
    }
    try:
        resp = requests.post(endpoint, data=payload, timeout=18)
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text[:500]}
    except Exception as exc:
        return {
            "key": "facebook_scrape",
            "status": "warn",
            "label": "Facebook 抓取",
            "detail": f"Graph URL 探测请求失败：{exc}",
        }
    if isinstance(data, dict) and data.get("error"):
        err = data.get("error") or {}
        message = str(err.get("message") or err)
        code = err.get("code")
        subcode = err.get("error_subcode") or err.get("subcode")
        status = "fail" if _fb_probe_error_looks_blocked(message) else "warn"
        if str(code) in {"190", "200", "10", "4", "17", "32"}:
            status = "warn"
        detail = f"Graph 返回错误"
        if code:
            detail += f" code={code}"
        if subcode:
            detail += f" subcode={subcode}"
        detail += f"：{message[:220]}"
        return {
            "key": "facebook_scrape",
            "status": status,
            "label": "Facebook 抓取",
            "detail": detail,
        }
    og = data.get("og_object") if isinstance(data, dict) else None
    share = data.get("share") if isinstance(data, dict) else None
    object_id = ""
    if isinstance(og, dict):
        object_id = str(og.get("id") or "")
    if not object_id and isinstance(data, dict):
        object_id = str(data.get("id") or "")
    share_count = ""
    if isinstance(share, dict) and share.get("share_count") is not None:
        share_count = f"，分享计数 {share.get('share_count')}"
    source = f"，Token 来源账户 {token_source}" if token_source else ""
    detail = "Facebook crawler 已能通过 Graph URL node 读取该链接"
    if object_id:
        detail += f"，对象 ID {object_id}"
    detail += share_count + source
    return {
        "key": "facebook_scrape",
        "status": "pass",
        "label": "Facebook 抓取",
        "detail": detail,
    }


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
    usage = {"total": 0, "accounts": [], "campaigns": [], "ad_links": []}
    page_id = int(item.get("id") or 0)

    if candidates:
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
    if page_id and _has_table(conn, "landing_ad_links"):
        for row in conn.execute(
            """SELECT id, slug, public_url, act_id, account_name, ad_id, ad_name,
                      adset_id, adset_name, campaign_id, campaign_name, status
               FROM landing_ad_links
               WHERE page_id=?
                 AND (
                   COALESCE(ad_id,'')!=''
                   OR COALESCE(act_id,'')!=''
                   OR COALESCE(status,'reserved') IN ('active','paused')
                 )
               ORDER BY updated_at DESC LIMIT 80""",
            (page_id,),
        ).fetchall():
            usage["ad_links"].append({
                "id": row["id"],
                "slug": row["slug"],
                "public_url": row["public_url"] or "",
                "act_id": row["act_id"] or "",
                "account_name": row["account_name"] or "",
                "ad_id": row["ad_id"] or "",
                "ad_name": row["ad_name"] or "",
                "adset_id": row["adset_id"] or "",
                "adset_name": row["adset_name"] or "",
                "campaign_id": row["campaign_id"] or "",
                "campaign_name": row["campaign_name"] or "",
                "status": row["status"] or "",
            })
    usage["total"] = len(usage["accounts"]) + len(usage["campaigns"])
    usage["total"] += len(usage["ad_links"])
    usage["accounts"] = usage["accounts"][:20]
    usage["campaigns"] = usage["campaigns"][:20]
    usage["ad_links"] = usage["ad_links"][:20]
    return usage


def _delete_landing_page_local_rows(conn, page_id: int) -> dict:
    summary = {
        "ad_links": 0,
        "events": 0,
        "results": 0,
        "route_states": 0,
        "asset_bindings_detached": 0,
    }
    if not page_id:
        return summary
    link_ids = []
    if _has_table(conn, "landing_ad_links"):
        rows = conn.execute("SELECT id FROM landing_ad_links WHERE page_id=?", (page_id,)).fetchall()
        link_ids = [int(r["id"]) for r in rows]
    if link_ids:
        placeholders = ",".join(["?"] * len(link_ids))
        if _has_table(conn, "landing_ad_link_results"):
            cur = conn.execute(f"DELETE FROM landing_ad_link_results WHERE link_id IN ({placeholders})", link_ids)
            summary["results"] += int(cur.rowcount or 0)
        if _has_table(conn, "landing_ad_route_state"):
            cur = conn.execute(f"DELETE FROM landing_ad_route_state WHERE link_id IN ({placeholders})", link_ids)
            summary["route_states"] += int(cur.rowcount or 0)
    if _has_table(conn, "landing_ad_links"):
        cur = conn.execute("DELETE FROM landing_ad_links WHERE page_id=?", (page_id,))
        summary["ad_links"] = int(cur.rowcount or 0)
    if _has_table(conn, "landing_events"):
        cur = conn.execute("DELETE FROM landing_events WHERE page_id=?", (page_id,))
        summary["events"] = int(cur.rowcount or 0)
    if _has_table(conn, "landing_route_state"):
        cur = conn.execute("DELETE FROM landing_route_state WHERE page_id=?", (page_id,))
        summary["route_states"] += int(cur.rowcount or 0)
    if _has_table(conn, "landing_asset_bindings"):
        cur = conn.execute(
            """UPDATE landing_asset_bindings
               SET landing_page_id=NULL, updated_at=datetime('now','+8 hours')
               WHERE landing_page_id=?""",
            (page_id,),
        )
        summary["asset_bindings_detached"] = int(cur.rowcount or 0)
    return summary


def _refresh_landing_domain_record(conn, page: dict, user) -> dict:
    custom_domain = (page.get("custom_domain") or "").strip()
    if not custom_domain:
        raise HTTPException(status_code=400, detail="This landing page has no custom domain")
    token_id = page.get("cf_token_id")
    if not token_id:
        raise HTTPException(status_code=400, detail="这个落地页没有可用发布通道")
    token_row = _assert_token_access(conn, int(token_id), user)
    raw_token = decrypt_token(token_row["access_token_enc"])
    cf_account_id = page.get("cf_account_id") or token_row.get("cf_account_id")
    if not cf_account_id:
        raise HTTPException(status_code=400, detail="发布通道没有选择默认发布账号，请先选择账号")
    dns_result, status, automation_error, automation_notice = _setup_custom_domain_automation(
        raw_token,
        cf_account_id,
        page.get("project_name") or "",
        custom_domain,
        page.get("pages_url") or "",
        user,
    )
    raw_payload = _json_loads(page.get("raw_response"), {})
    if not isinstance(raw_payload, dict):
        raw_payload = {}
    if dns_result is not None:
        raw_payload["custom_domain_dns_result"] = dns_result
    raw_payload["domain_status"] = status
    raw_payload["custom_domain_result"] = status
    runtime_usable = _custom_domain_runtime_usable(
        custom_domain,
        bool(page.get("worker_enabled")),
    )
    if runtime_usable:
        raw_payload["custom_domain_runtime_usable"] = True
        raw_payload["custom_domain_runtime_checked_at"] = _now_cst()
        if not _domain_status_usable(status, automation_error):
            raw_payload["custom_domain_status_mismatch"] = {
                "runtime_usable": True,
                "cloudflare_status": _domain_status_text(status) or "unknown",
                "checked_at": raw_payload["custom_domain_runtime_checked_at"],
                "message": "Public runtime is reachable, while provider domain status is not active yet.",
            }
        else:
            raw_payload.pop("custom_domain_status_mismatch", None)
    else:
        raw_payload.pop("custom_domain_runtime_usable", None)
        raw_payload.pop("custom_domain_runtime_checked_at", None)
        raw_payload.pop("custom_domain_status_mismatch", None)
    if automation_notice:
        raw_payload["custom_domain_notice"] = automation_notice
    detail = json.dumps(status, ensure_ascii=False)
    last_error = automation_error or ("" if (status.get("status") or "").lower() not in {"not_found", "error"} else detail)
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
    refreshed_ad_links = False
    if item.get("custom_domain_usable") and item.get("public_url"):
        _refresh_page_ad_link_urls(conn, item["id"], item)
        refreshed_ad_links = True
        original_bind_target = item.get("bind_target") or "none"
        safe_bind_target = _effective_bind_target_for_link_kind(item.get("link_kind"), original_bind_target)
        binding = _bind_page_to_accounts(
            conn,
            item.get("bound_act_ids") or [],
            safe_bind_target,
            item.get("public_url"),
            user,
        )
        if binding and safe_bind_target != original_bind_target:
            binding["target_adjusted_from"] = original_bind_target
        if binding and binding.get("bound"):
            conn.commit()
    if refreshed_ad_links:
        conn.commit()
    item["usage"] = _landing_page_usage(conn, item, user)
    return {
        "page": item,
        "domain_status": status,
        "dns_result": dns_result,
        "automation_error": automation_error,
        "automation_notice": automation_notice,
        "binding": binding,
    }


def _parse_cst_time(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).replace(tzinfo=CST)
        except ValueError:
            continue
    return None


def _should_refresh_landing_domain_cache(page: dict, interval_seconds: int = 300) -> bool:
    if not page or not (page.get("custom_domain") or "").strip():
        return False
    if (page.get("status") or "").strip().lower() == "archived":
        return False
    if not page.get("cf_token_id"):
        return False
    raw = _json_loads(page.get("raw_response"), {})
    if not isinstance(raw, dict):
        raw = {}
    status = raw.get("domain_status") or raw.get("custom_domain_result") or {}
    if _domain_status_usable(status, page.get("last_error")):
        return False
    last_checked = (
        raw.get("custom_domain_runtime_checked_at")
        or raw.get("custom_domain_status_checked_at")
        or page.get("updated_at")
    )
    parsed = _parse_cst_time(last_checked)
    if parsed and datetime.now(CST) - parsed < timedelta(seconds=interval_seconds):
        return False
    return True


def _safe_rules(rules: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(rules, dict):
        return {}
    allowed = {
        "country_allow",
        "country_block",
        "source_allow",
        "source_block",
        "platform_block",
        "device_block",
        "ua_block",
        "referer_block",
        "query_block",
        "required_query",
    }
    source_alias = {
        "fb": "facebook",
        "facebook": "facebook",
        "ig": "instagram",
        "instagram": "instagram",
        "tk": "tiktok",
        "tt": "tiktok",
        "tiktok": "tiktok",
        "google": "google",
        "go": "google",
        "gg": "google",
        "bing": "bing",
        "wa": "whatsapp",
        "whatsapp": "whatsapp",
        "tg": "telegram",
        "telegram": "telegram",
        "unknown": "unknown",
    }
    device_allowed = {"mobile", "desktop", "tablet"}

    def raw_parts(value: Any) -> list[str]:
        if isinstance(value, str):
            source = re.split(r"[\s,，;；]+", value)
        elif isinstance(value, list):
            source = value
        else:
            return []
        return [str(x).strip()[:80] for x in source if str(x).strip()]

    def dedupe(items: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in items:
            if not item or item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    clean: dict[str, Any] = {}
    for key in allowed:
        parts = raw_parts(rules.get(key))
        if not parts:
            continue
        normalized: list[str] = []
        if key in {"country_allow", "country_block"}:
            for part in parts:
                code = part.upper()
                if re.fullmatch(r"[A-Z]{2}", code):
                    normalized.append(code)
        elif key in {"source_allow", "source_block"}:
            for part in parts:
                mapped = source_alias.get(part.lower())
                if mapped:
                    normalized.append(mapped)
        elif key == "device_block":
            for part in parts:
                device = part.lower()
                if device in device_allowed:
                    normalized.append(device)
        else:
            normalized = [part.lower() if key in {"query_block", "required_query"} else part for part in parts]
        normalized = dedupe(normalized)
        if normalized:
            clean[key] = normalized[:80]
    return clean


@router.get("/tokens")
def list_cf_tokens(user=Depends(get_current_user)):
    where, params = _scope_where(user, "cf_tokens")
    sql = """SELECT cf_tokens.*,
                    tm.name AS team_name,
                    COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name,
                    COUNT(lp.id) AS usage_count
             FROM cf_tokens
             LEFT JOIN teams tm ON tm.id=cf_tokens.team_id
             LEFT JOIN users ou ON ou.id=cf_tokens.owner_user_id
             LEFT JOIN landing_pages lp ON lp.cf_token_id=cf_tokens.id"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " GROUP BY cf_tokens.id ORDER BY cf_tokens.id DESC"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_public_token(r) for r in rows]


@router.get("/domains")
def list_landing_domains(token_id: Optional[int] = None, user=Depends(get_current_user)):
    """Return the domain library visible to the current user.

    Sources are merged from readable publisher accounts, existing landing pages,
    and saved asset presets. The result is intentionally metadata-light so users
    can pick a domain without seeing provider internals.
    """
    conn = get_conn()
    domains: dict[str, dict[str, Any]] = {}

    def add_domain(host: str, source: str, **extra):
        try:
            clean = normalize_custom_domain(host)
        except Exception:
            return
        if not clean:
            return
        item = domains.setdefault(
            clean,
            {
                "domain": clean,
                "sources": [],
                "token_id": None,
                "token_name": "",
                "account_id": "",
                "account_name": "",
                "status": "",
                "suggested": False,
            },
        )
        if source and source not in item["sources"]:
            item["sources"].append(source)
        for key, value in extra.items():
            if value not in (None, ""):
                if key == "suggested":
                    item[key] = bool(value)
                elif not item.get(key):
                    item[key] = value

    try:
        page_where, page_params = _scope_where(user, "p")
        page_sql = "SELECT custom_domain, raw_response FROM landing_pages p WHERE COALESCE(p.custom_domain,'')!='' AND COALESCE(p.status,'')!='archived'"
        if page_where:
            page_sql += " AND " + " AND ".join(page_where)
        for row in conn.execute(page_sql, page_params).fetchall():
            raw = _json_loads(row["raw_response"], {})
            status = _domain_status_text(raw.get("domain_status") or raw.get("custom_domain_result") or "")
            add_domain(row["custom_domain"], "已发布", status=status)

        bind_where, bind_params = _scope_where(user, "b")
        bind_sql = "SELECT custom_domain FROM landing_asset_bindings b WHERE COALESCE(b.custom_domain,'')!='' AND COALESCE(b.status,'active')='active'"
        if bind_where:
            bind_sql += " AND " + " AND ".join(bind_where)
        for row in conn.execute(bind_sql, bind_params).fetchall():
            add_domain(row["custom_domain"], "资产预设")

        token_where, token_params = _scope_where(user, "cf_tokens")
        token_sql = "SELECT * FROM cf_tokens"
        if token_where:
            token_sql += " WHERE " + " AND ".join(token_where)
        if token_id:
            token_sql += (" AND " if token_where else " WHERE ") + "cf_tokens.id=?"
            token_params.append(token_id)
        for row in conn.execute(token_sql, token_params).fetchall():
            item = dict(row)
            account_id, account_name = _resolve_token_account(item)
            if not account_id or item.get("status") == "error":
                continue
            try:
                raw_token = decrypt_token(item["access_token_enc"])
                zones = list_account_zones(raw_token, account_id)
            except Exception:
                continue
            for zone in zones:
                root = str(zone.get("name") or "").strip()
                if not root:
                    continue
                common_host = f"go.{root}"
                add_domain(
                    common_host,
                    "发布账号",
                    token_id=item.get("id"),
                    token_name=item.get("name") or "",
                    account_id=account_id,
                    account_name=account_name or "",
                    status=str(zone.get("status") or ""),
                    root_domain=root,
                    suggested=True,
                )
                add_domain(
                    root,
                    "根域名",
                    token_id=item.get("id"),
                    token_name=item.get("name") or "",
                    account_id=account_id,
                    account_name=account_name or "",
                    status=str(zone.get("status") or ""),
                    root_domain=root,
                )
        items = sorted(domains.values(), key=lambda x: (0 if x.get("suggested") else 1, x.get("domain") or ""))
        return {"success": True, "retention_days": LANDING_TRACKING_RETENTION_DAYS, "domains": items}
    finally:
        conn.close()


@router.post("/tokens")
def create_cf_token(body: CloudflareTokenCreate, user=Depends(get_current_user)):
    name = (body.name or "").strip()
    raw_token = (body.api_token or "").strip()
    account_id = (body.account_id or "").strip()
    if not name or not raw_token:
        raise HTTPException(status_code=400, detail="请填写发布通道名称和 API Token")
    try:
        info = verify_token_and_accounts(raw_token, account_id=account_id or None)
    except CloudflareError as exc:
        raise HTTPException(status_code=400, detail=f"发布通道验证失败：{_public_provider_error(exc, user)}") from exc
    accounts = _normalize_cf_accounts(info.get("accounts") or [])
    if not accounts:
        raise HTTPException(
            status_code=400,
            detail="API Token 已返回有效响应，但没有读取到可用发布账号。请填写 Account ID，或给 Token 增加账号读取和站点发布权限。",
        )
    selected_account_id = account_id or (accounts[0].get("id") if len(accounts) == 1 else None)
    selected_account = next((a for a in accounts if a.get("id") == selected_account_id), None)
    account = selected_account or accounts[0]
    selected_account_name = selected_account.get("name") if selected_account else None
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


@router.patch("/tokens/{token_id}")
def update_cf_token(token_id: int, body: CloudflareTokenPatch, user=Depends(get_current_user)):
    name = (body.name or "").strip() if body.name is not None else None
    raw_token = (body.api_token or "").strip()
    account_id = (body.account_id or "").strip()
    if name == "":
        raise HTTPException(status_code=400, detail="请填写 API 名称")
    if not any([name is not None, raw_token, account_id]):
        raise HTTPException(status_code=400, detail="请提交要修改的 API 名称、Account ID 或 API Token")
    conn = get_conn()
    try:
        current = _assert_token_access(conn, token_id, user)
        sets, params = [], []
        if name is not None:
            sets.append("name=?")
            params.append(name[:120])
        if raw_token:
            try:
                info = verify_token_and_accounts(raw_token, account_id=account_id or None)
            except CloudflareError as exc:
                raise HTTPException(status_code=400, detail=f"API Token 验证失败：{_public_provider_error(exc, user)}") from exc
            accounts = _normalize_cf_accounts(info.get("accounts") or [])
            if not accounts:
                raise HTTPException(
                    status_code=400,
                    detail="API Token 已返回有效响应，但没有读取到可用发布账号。请填写 Account ID，或给 Token 增加账号读取和站点发布权限。",
                )
            selected_account_id = account_id or current.get("selected_account_id") or current.get("cf_account_id")
            if selected_account_id and not any(a.get("id") == selected_account_id for a in accounts):
                raise HTTPException(status_code=400, detail="Account ID 不在新 API Token 可用范围内，请检查 Account ID 或 Token 权限")
            if not selected_account_id and len(accounts) == 1:
                selected_account_id = accounts[0].get("id")
            selected_account_name = next((a.get("name") for a in accounts if a.get("id") == selected_account_id), None)
            primary_account = next((a for a in accounts if a.get("id") == selected_account_id), None) or accounts[0]
            sets.extend([
                "access_token_enc=?",
                "token_mask=?",
                "cf_accounts_json=?",
                "selected_account_id=?",
                "cf_account_id=?",
                "cf_account_name=?",
                "status='active'",
                "last_verified_at=datetime('now','+8 hours')",
            ])
            params.extend([
                encrypt_token(raw_token),
                mask_token(raw_token),
                json.dumps(accounts, ensure_ascii=False),
                selected_account_id,
                primary_account.get("id"),
                selected_account_name or primary_account.get("name"),
            ])
        elif account_id:
            accounts = _public_accounts(current.get("cf_accounts_json"))
            matched = next((acct for acct in accounts if isinstance(acct, dict) and acct.get("id") == account_id), None)
            if not matched:
                raise HTTPException(status_code=400, detail="该发布账号不在当前 API Token 可用范围内；请先修正 API Token 或重新验证")
            sets.extend(["selected_account_id=?", "cf_account_id=?", "cf_account_name=?"])
            params.extend([matched.get("id"), matched.get("id"), matched.get("name")])
        sets.append("updated_at=datetime('now','+8 hours')")
        params.append(token_id)
        conn.execute(f"UPDATE cf_tokens SET {', '.join(sets)} WHERE id=?", params)
        conn.commit()
        row = conn.execute(
            """SELECT cf_tokens.*,
                      tm.name AS team_name,
                      COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name,
                      (SELECT COUNT(1) FROM landing_pages lp WHERE lp.cf_token_id=cf_tokens.id) AS usage_count
               FROM cf_tokens
               LEFT JOIN teams tm ON tm.id=cf_tokens.team_id
               LEFT JOIN users ou ON ou.id=cf_tokens.owner_user_id
               WHERE cf_tokens.id=?""",
            (token_id,),
        ).fetchone()
        return {"success": True, "token": _public_token(row)}
    finally:
        conn.close()


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
            raise ValueError("API Token 已验证，但没有读取到可用发布账号；请填写 Account ID 后重试")
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
        raise HTTPException(status_code=400, detail=f"发布通道验证失败：{_public_provider_error(exc, user)}") from exc
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
        raise HTTPException(status_code=400, detail="该发布账号不在当前 API Token 可用范围内")
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
        provider = _provider_label(user)
        checks = [{"key": "token", "status": "pass", "label": "API Token 有效", "detail": f"{provider} API 已通过验证"}]
        if account_id:
            try:
                projects = list_pages_projects(raw, account_id)
                try:
                    zones = list_account_zones(raw, account_id)
                    zone_names = [str(z.get("name") or "").strip() for z in zones if z.get("name")]
                    checks.append({
                        "key": "zones",
                        "status": "pass",
                        "label": "自定义域名读取",
                        "detail": f"可读取 {len(zone_names)} 个域名" + (("：" + "、".join(zone_names[:6])) if zone_names else "。"),
                    })
                except CloudflareError as zone_exc:
                    checks.append({
                        "key": "zones",
                        "status": "warn",
                        "label": "自定义域名读取受限",
                        "detail": "如需自动识别账号下可用域名，请给 API Token 增加 Zone Read 权限；当前仍可手动填写域名，并在发布或复检时验证。"
                        + _public_provider_error(zone_exc, user),
                    })
                checks.append({"key": "pages", "status": "pass", "label": "发布权限可用", "detail": f"可读取 {len(projects)} 个站点项目"})
                checks.append({
                    "key": "usage",
                    "status": "pass",
                    "label": "发布资源概览",
                    "detail": f"当前发布账号可读取 {len(projects)} 个 Pages 项目。发布 API 未提供统一的免费额度剩余字段，系统只展示实际可读资源数量。",
                })
            except CloudflareError as exc:
                checks.append({"key": "pages", "status": "fail", "label": "发布权限不可用", "detail": _public_provider_error(exc, user)})
        else:
            checks.append({"key": "account", "status": "warn", "label": "需要选择发布账号", "detail": "该 Token 能看到多个账号，请先选择默认发布账号"})
        return {"success": True, "accounts": accounts, "selected_account_id": account_id, "selected_account_name": account_name, "checks": checks}
    except Exception as exc:
        return {
            "success": False,
            "checks": [{"key": "token", "status": "fail", "label": "API Token 不可用", "detail": _public_provider_error(exc, user)}],
            "hint": "站点发布需要账号级 API Token，不是对象存储 Access Key 或 Secret Key。",
        }


@router.delete("/tokens/{token_id}")
def delete_cf_token(token_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _assert_token_access(conn, token_id, user)
        usage = conn.execute(
            "SELECT COUNT(*) AS c FROM landing_pages WHERE cf_token_id=?",
            (token_id,),
        ).fetchone()
        usage_count = int(usage["c"] or 0) if usage else 0
        if usage_count > 0:
            raise HTTPException(
                status_code=400,
                detail=f"发布通道仍被 {usage_count} 条落地页记录引用。为保留域名复检和远程项目清理能力，请先处理相关落地页记录后再清理 API。",
            )
        conn.execute("DELETE FROM cf_tokens WHERE id=?", (token_id,))
        conn.commit()
        return {"success": True, "deleted": True, "id": token_id}
    finally:
        conn.close()


@router.get("/templates")
def list_landing_templates(user=Depends(get_current_user)):
    where, params = ["status='active'"], []
    if not is_superadmin(user):
        where.append("(team_id=? OR team_id IS NULL)")
        params.append(team_id_for_create(user))
    sql = "SELECT * FROM landing_templates WHERE " + " AND ".join(where) + " ORDER BY id ASC"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_public_landing_template(r, user) for r in rows]


@router.post("/templates/upload")
async def upload_landing_template(
    name: str = Form(...),
    note: str = Form(""),
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
    title = (name or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="请填写模板名称")
    filename = (file.filename or "").strip()
    if not filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="请上传 zip 模板包")
    tmp_dir = Path(tempfile.mkdtemp(prefix="mira_tpl_upload_"))
    tmp_zip = tmp_dir / "template.zip"
    size = 0
    try:
        with open(tmp_zip, "wb") as dst:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > LANDING_TEMPLATE_MAX_ZIP_BYTES:
                    raise HTTPException(status_code=400, detail=f"模板包过大，上限 {LANDING_TEMPLATE_MAX_ZIP_BYTES // 1024 // 1024}MB")
                dst.write(chunk)
        validation = _validate_landing_template_zip(tmp_zip)
        if not validation.get("valid"):
            raise HTTPException(status_code=400, detail={"message": "模板检测未通过", "validation": validation})
        team_id = None if is_superadmin(user) else team_id_for_create(user)
        owner_id = user_id(user)
        slug = _short_slug(10)
        target_dir = LANDING_TEMPLATE_UPLOAD_DIR / f"tpl_{slug}"
        while target_dir.exists():
            slug = _short_slug(10)
            target_dir = LANDING_TEMPLATE_UPLOAD_DIR / f"tpl_{slug}"
        _extract_landing_template_zip(tmp_zip, target_dir, validation)
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO landing_templates
                   (name, template_path, status, source, original_filename, size_bytes, validation_json, note,
                    team_id, owner_user_id, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    title[:120],
                    str(target_dir),
                    "active",
                    "upload",
                    filename[:255],
                    size,
                    json.dumps(validation, ensure_ascii=False),
                    (note or "").strip()[:500],
                    team_id,
                    owner_id,
                    user.get("username", "unknown"),
                ),
            )
            template_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            row = conn.execute("SELECT * FROM landing_templates WHERE id=?", (template_id,)).fetchone()
            return {"success": True, "template": _public_landing_template(row, user), "validation": validation}
        finally:
            conn.close()
    except Exception:
        if "target_dir" in locals() and isinstance(target_dir, Path) and target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@router.delete("/templates/{template_id}")
def delete_landing_template(template_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    template_path = None
    try:
        item = _assert_template_access(conn, template_id, user)
        if not _can_delete_landing_template(item, user):
            raise HTTPException(status_code=403, detail="只能删除自己上传且未锁定的模板包")
        usage = conn.execute(
            "SELECT COUNT(*) FROM landing_pages WHERE COALESCE(template_id,1)=? AND status!='archived'",
            (template_id,),
        ).fetchone()[0]
        if usage:
            raise HTTPException(status_code=400, detail=f"该模板仍被 {usage} 个落地页引用，不能删除")
        template_path = item.get("template_path")
        conn.execute(
            "UPDATE landing_templates SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?",
            (template_id,),
        )
        conn.commit()
    finally:
        conn.close()
    if template_path:
        try:
            root = LANDING_TEMPLATE_UPLOAD_DIR.resolve()
            path = Path(template_path).resolve()
            if root in path.parents and path.exists():
                shutil.rmtree(path, ignore_errors=True)
        except Exception:
            logger.exception("delete landing template files failed")
    return {"success": True, "archived": True, "id": template_id}


@router.get("/template-reference.zip")
def download_landing_template_reference(user=Depends(get_current_user)):
    path = LANDING_TEMPLATE_REFERENCE_ZIP
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="模板参考包尚未部署")
    return FileResponse(
        path,
        media_type="application/zip",
        filename="landing-template-reference.zip",
    )


@router.get("/protection-templates")
def list_landing_protection_templates(user=Depends(get_current_user)):
    where, params = ["landing_protection_templates.status='active'"], []
    scope_where, scope_params = _scope_where(user, "landing_protection_templates")
    where.extend(scope_where)
    params.extend(scope_params)
    sql = """SELECT landing_protection_templates.*,
                    tm.name AS team_name,
                    COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
             FROM landing_protection_templates
             LEFT JOIN teams tm ON tm.id=landing_protection_templates.team_id
             LEFT JOIN users ou ON ou.id=landing_protection_templates.owner_user_id
             WHERE """ + " AND ".join(where) + " ORDER BY landing_protection_templates.id DESC"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_public_protection_template(r) for r in rows]


@router.post("/protection-templates")
def create_landing_protection_template(body: LandingProtectionTemplateReq, user=Depends(get_current_user)):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="请填写模板名称")
    rules = _safe_rules(body.rules)
    if not rules:
        raise HTTPException(status_code=400, detail="请至少配置一条防护规则")
    team_id, owner_id = _stamp(user, body.team_id)
    conn = get_conn()
    try:
        conn.execute(
            """INSERT INTO landing_protection_templates
               (name, rules, note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?)""",
            (
                name[:120],
                json.dumps(rules, ensure_ascii=False),
                (body.note or "").strip()[:500],
                team_id,
                owner_id,
                user.get("username", "unknown"),
            ),
        )
        template_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        row = conn.execute(
            """SELECT landing_protection_templates.*,
                      tm.name AS team_name,
                      COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
               FROM landing_protection_templates
               LEFT JOIN teams tm ON tm.id=landing_protection_templates.team_id
               LEFT JOIN users ou ON ou.id=landing_protection_templates.owner_user_id
               WHERE landing_protection_templates.id=?""",
            (template_id,),
        ).fetchone()
        return {"success": True, "template": _public_protection_template(row)}
    finally:
        conn.close()


@router.patch("/protection-templates/{template_id}")
def update_landing_protection_template(template_id: int, body: LandingProtectionTemplateReq, user=Depends(get_current_user)):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="请填写模板名称")
    rules = _safe_rules(body.rules)
    if not rules:
        raise HTTPException(status_code=400, detail="请至少配置一条防护规则")
    conn = get_conn()
    try:
        _assert_protection_template_access(conn, template_id, user)
        conn.execute(
            """UPDATE landing_protection_templates
               SET name=?, rules=?, note=?, updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (name[:120], json.dumps(rules, ensure_ascii=False), (body.note or "").strip()[:500], template_id),
        )
        conn.commit()
        row = conn.execute(
            """SELECT landing_protection_templates.*,
                      tm.name AS team_name,
                      COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
               FROM landing_protection_templates
               LEFT JOIN teams tm ON tm.id=landing_protection_templates.team_id
               LEFT JOIN users ou ON ou.id=landing_protection_templates.owner_user_id
               WHERE landing_protection_templates.id=?""",
            (template_id,),
        ).fetchone()
        return {"success": True, "template": _public_protection_template(row)}
    finally:
        conn.close()


@router.delete("/protection-templates/{template_id}")
def delete_landing_protection_template(template_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _assert_protection_template_access(conn, template_id, user)
        conn.execute(
            "UPDATE landing_protection_templates SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?",
            (template_id,),
        )
        conn.commit()
        return {"success": True, "archived": True, "id": template_id}
    finally:
        conn.close()


@router.get("/asset-bindings")
def list_landing_asset_bindings(user=Depends(get_current_user)):
    where, params = ["b.status='active'"], []
    scope_where, scope_params = _scope_where(user, "b")
    where.extend(scope_where)
    params.extend(scope_params)
    sql = """SELECT b.*,
                    p.id AS page_id,
                    p.title AS page_title,
                    p.pages_url AS page_pages_url,
                    p.custom_domain AS page_custom_domain,
                    p.raw_response AS page_raw_response,
                    p.target_urls AS page_target_urls,
                    p.last_error AS page_last_error,
                    pt.name AS protection_template_name,
                    tm.name AS team_name,
                    COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
             FROM landing_asset_bindings b
             LEFT JOIN landing_pages p ON p.id=b.landing_page_id
             LEFT JOIN landing_protection_templates pt ON pt.id=b.protection_template_id
             LEFT JOIN teams tm ON tm.id=b.team_id
             LEFT JOIN users ou ON ou.id=b.owner_user_id
             WHERE """ + " AND ".join(where) + " ORDER BY b.id DESC LIMIT 300"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_public_asset_binding(r) for r in rows]


@router.post("/asset-bindings")
def create_landing_asset_binding(body: LandingAssetBindingReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        vals = _asset_binding_values(body, conn, user)
        team_id, owner_id = _stamp(user, body.team_id)
        if team_id is None and vals.get("page"):
            team_id = vals["page"].get("team_id")
        conn.execute(
            """INSERT INTO landing_asset_bindings
               (name, custom_domain, pixel_name, pixel_id, landing_page_id, target_urls, rotation_mode,
                link_kind, protection_template_id, note, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                vals["name"],
                vals["custom_domain"],
                vals["pixel_name"],
                vals["pixel_id"],
                vals["landing_page_id"],
                json.dumps(vals["target_urls"], ensure_ascii=False),
                vals["rotation_mode"],
                vals["link_kind"],
                vals["protection_template_id"],
                vals["note"],
                team_id,
                owner_id,
                user.get("username", "unknown"),
            ),
        )
        binding_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        row = conn.execute(
            """SELECT b.*,
                      p.id AS page_id, p.title AS page_title, p.pages_url AS page_pages_url,
                      p.custom_domain AS page_custom_domain, p.raw_response AS page_raw_response,
                      p.target_urls AS page_target_urls, p.last_error AS page_last_error,
                      pt.name AS protection_template_name,
                      tm.name AS team_name,
                      COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
               FROM landing_asset_bindings b
               LEFT JOIN landing_pages p ON p.id=b.landing_page_id
               LEFT JOIN landing_protection_templates pt ON pt.id=b.protection_template_id
               LEFT JOIN teams tm ON tm.id=b.team_id
               LEFT JOIN users ou ON ou.id=b.owner_user_id
               WHERE b.id=?""",
            (binding_id,),
        ).fetchone()
        return {"success": True, "binding": _public_asset_binding(row)}
    finally:
        conn.close()


@router.patch("/asset-bindings/{binding_id}")
def update_landing_asset_binding(binding_id: int, body: LandingAssetBindingReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _assert_asset_binding_access(conn, binding_id, user)
        vals = _asset_binding_values(body, conn, user)
        conn.execute(
            """UPDATE landing_asset_bindings
               SET name=?, custom_domain=?, pixel_name=?, pixel_id=?, landing_page_id=?, target_urls=?,
                   rotation_mode=?, link_kind=?, protection_template_id=?, note=?,
                   updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (
                vals["name"],
                vals["custom_domain"],
                vals["pixel_name"],
                vals["pixel_id"],
                vals["landing_page_id"],
                json.dumps(vals["target_urls"], ensure_ascii=False),
                vals["rotation_mode"],
                vals["link_kind"],
                vals["protection_template_id"],
                vals["note"],
                binding_id,
            ),
        )
        conn.commit()
        row = conn.execute(
            """SELECT b.*,
                      p.id AS page_id, p.title AS page_title, p.pages_url AS page_pages_url,
                      p.custom_domain AS page_custom_domain, p.raw_response AS page_raw_response,
                      p.target_urls AS page_target_urls, p.last_error AS page_last_error,
                      pt.name AS protection_template_name,
                      tm.name AS team_name,
                      COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
               FROM landing_asset_bindings b
               LEFT JOIN landing_pages p ON p.id=b.landing_page_id
               LEFT JOIN landing_protection_templates pt ON pt.id=b.protection_template_id
               LEFT JOIN teams tm ON tm.id=b.team_id
               LEFT JOIN users ou ON ou.id=b.owner_user_id
               WHERE b.id=?""",
            (binding_id,),
        ).fetchone()
        return {"success": True, "binding": _public_asset_binding(row)}
    finally:
        conn.close()


@router.delete("/asset-bindings/{binding_id}")
def delete_landing_asset_binding(binding_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _assert_asset_binding_access(conn, binding_id, user)
        conn.execute(
            "UPDATE landing_asset_bindings SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?",
            (binding_id,),
        )
        conn.commit()
        return {"success": True, "archived": True, "id": binding_id}
    finally:
        conn.close()


@router.post("/preflight")
def preflight_landing_page(body: LandingPublishReq, request: Request, user=Depends(get_current_user)):
    title = (body.title or "").strip()
    urls = [u.strip() for u in body.target_urls if u and u.strip()]
    rules = _safe_rules(body.protection_rules)
    link_kind = (body.link_kind or "landing").strip().lower()
    bind_target = (body.bind_target or "none").strip().lower()
    custom_domain = ""
    custom_domain_error = ""
    project_hint = sanitize_project_name(body.project_name or title or "site-landing")
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
            "detail": f"{custom_domain} 将绑定到发布站点；请确保域名可管理并完成 DNS 指向。",
        })
        project_hint = sanitize_project_name(body.project_name or title or "site-landing")
        checks.append({
            "key": "custom_domain_dns",
            "status": "warn",
            "label": "DNS 提示",
            "detail": f"通常配置 CNAME {custom_domain} -> 系统分配的备用域；域名未激活前系统只会把备用域写入账户，复检可用后自动回绑自定义域。",
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
    if link_kind == "form":
        if bind_target in {"landing", "both"}:
            checks.append({"key": "bind_target_guard", "status": "fail", "label": "绑定位置", "detail": "表单直跳链接只能绑定到账户表单链接，不能写入落地页字段，避免普通落地页投放误用直跳链接"})
        elif bind_target == "form":
            checks.append({"key": "bind_target_guard", "status": "pass", "label": "绑定位置", "detail": "将写入账户表单链接字段，Lead Form / 聊单投放会优先读取这里"})
    rotation_mode = (body.rotation_mode or "sequential").strip().lower()
    if rotation_mode == "sequential":
        checks.append({"key": "rotation", "status": "pass", "label": "轮询方式", "detail": "服务端全局游标轮询；所有访客共享同一顺序，不依赖单个浏览器缓存。"})
    elif rotation_mode == "random":
        checks.append({"key": "rotation", "status": "pass", "label": "轮询方式", "detail": "每次访问随机选择一个跳转目标。"})
    else:
        checks.append({"key": "rotation", "status": "warn", "label": "轮询方式", "detail": "固定第一个跳转目标，适合临时验证，不适合多链接分流。"})
    if body.tracking_enabled:
        checks.append({"key": "tracking", "status": "pass", "label": "边缘统计", "detail": "将通过同域边缘脚本采集访问、点击、跳转、拦截事件"})
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
            checks.append({"key": "cloudflare_account", "status": "pass", "label": "发布账号", "detail": account_name or account_id})
            raw_token = decrypt_token(token_row["access_token_enc"])
            try:
                list_pages_projects(raw_token, account_id)
                checks.append({"key": "pages_permission", "status": "pass", "label": "发布权限", "detail": "可读取站点项目"})
            except CloudflareError as exc:
                checks.append({"key": "pages_permission", "status": "fail", "label": "发布权限", "detail": _public_provider_error(exc, user)})
        else:
            checks.append({"key": "cloudflare_account", "status": "fail", "label": "发布账号", "detail": "请先在 API 卡片里选择默认发布账号"})
        if account_id and custom_domain:
            try:
                zone = find_zone_for_domain(raw_token, account_id, custom_domain)
                zone_id = str(zone.get("id") or "")
                records = list_dns_records(raw_token, zone_id, custom_domain)
                cname = [r for r in records if str(r.get("type") or "").upper() == "CNAME"]
                conflicts = [r for r in records if str(r.get("type") or "").upper() != "CNAME"]
                if conflicts and not cname:
                    types = ", ".join(sorted({str(r.get("type") or "UNKNOWN").upper() for r in conflicts}))
                    checks.append({
                        "key": "custom_domain_dns_write",
                        "status": "fail",
                        "label": "DNS 自动化",
                        "detail": f"{custom_domain} 已存在 {types} 记录，不能自动改成 Pages CNAME。请换子域名或先清理冲突记录。",
                    })
                else:
                    action = "更新" if cname else "创建"
                    checks.append({
                        "key": "custom_domain_dns_write",
                        "status": "pass",
                        "label": "DNS 自动化",
                        "detail": f"可在 {zone.get('name') or '匹配 Zone'} 中自动{action} CNAME {custom_domain} -> {pages_cname_target(project_hint)}。",
                    })
            except CloudflareError as exc:
                checks.append({
                    "key": "custom_domain_dns_write",
                    "status": "fail",
                    "label": "DNS 自动化",
                    "detail": _public_provider_error(exc, user),
                })
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
        checks.append({"key": "token", "status": "fail", "label": "发布通道 API", "detail": _public_provider_error(exc.detail, user)})
    finally:
        conn.close()
    return {"success": not any(c["status"] == "fail" for c in checks), "checks": checks}


@router.get("/pages")
def list_landing_pages(user=Depends(get_current_user)):
    where, params = _scope_where(user, "p")
    sql = """SELECT p.*, t.name AS token_name, lt.name AS template_name,
                    tm.name AS team_name,
                    COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name
             FROM landing_pages p
             LEFT JOIN cf_tokens t ON t.id=p.cf_token_id
             LEFT JOIN landing_templates lt ON lt.id=COALESCE(p.template_id, 1)
             LEFT JOIN teams tm ON tm.id=p.team_id
             LEFT JOIN users ou ON ou.id=p.owner_user_id"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.id DESC LIMIT 200"
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    pages = []
    refresh_budget = 3
    for row in rows:
        row_dict = dict(row)
        item = None
        if refresh_budget > 0 and _should_refresh_landing_domain_cache(row_dict):
            try:
                refreshed = _refresh_landing_domain_record(conn, row_dict, user)
                item = refreshed.get("page")
                refresh_budget -= 1
            except Exception:
                logger.exception("landing domain cache auto refresh failed: page_id=%s", row_dict.get("id"))
        if not item:
            item = _public_page(row)
        item["linked_matrix_ids"] = _matrix_ids_for_accounts(conn, item.get("bound_act_ids") or [])
        item["usage"] = _landing_page_usage(conn, item, user)
        pages.append(item)
    conn.close()
    return pages


def _landing_runtime_config(page: dict) -> dict[str, Any]:
    link_kind = (page.get("link_kind") or "landing").strip().lower()
    if link_kind not in {"landing", "form"}:
        link_kind = "landing"
    rotation_mode = (page.get("rotation_mode") or "sequential").strip().lower()
    if rotation_mode not in {"sequential", "random", "first"}:
        rotation_mode = "sequential"
    config = {
        "link_kind": link_kind,
        "target_urls": [u for u in _json_loads(page.get("target_urls"), []) if isinstance(u, str) and u.strip()],
        "rotation_mode": rotation_mode,
        "tracking_enabled": bool(page.get("tracking_enabled")),
        "protection_enabled": bool(page.get("protection_enabled")),
        "protection_rules": _safe_rules(_json_loads(page.get("protection_rules"), {})),
    }
    updated_at = str(page.get("updated_at") or "").strip()
    version_payload = json.dumps(
        {**config, "updated_at": updated_at},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    config["config_updated_at"] = updated_at
    config["config_version"] = hashlib.sha256(version_payload.encode("utf-8", "ignore")).hexdigest()[:16]
    return config


@router.post("/edge/config")
def landing_edge_runtime_config(body: LandingRuntimeConfigReq):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM landing_pages WHERE id=?", (body.page_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Landing page not found")
        page = dict(row)
        if not secrets.compare_digest(str(page.get("ingest_secret") or ""), str(body.secret or "")):
            raise HTTPException(status_code=403, detail="invalid landing config secret")
        if str(page.get("status") or "").lower() == "archived":
            raise HTTPException(status_code=410, detail="landing page archived")
        return {"success": True, "cache_seconds": 30, "config": _landing_runtime_config(page)}
    finally:
        conn.close()


@router.patch("/pages/{page_id}/runtime-config")
def update_landing_runtime_config(page_id: int, body: LandingRuntimeConfigPatch, request: Request, user=Depends(get_current_user)):
    urls = [u.strip() for u in body.target_urls if isinstance(u, str) and u.strip()]
    if not urls:
        raise HTTPException(status_code=400, detail="At least one target URL is required")
    if any(not (u.startswith("http://") or u.startswith("https://")) for u in urls):
        raise HTTPException(status_code=400, detail="Target URLs must start with http:// or https://")
    rotation_mode = (body.rotation_mode or "sequential").strip().lower()
    if rotation_mode not in {"sequential", "random", "first"}:
        raise HTTPException(status_code=400, detail="rotation_mode must be sequential, random, or first")
    rules = _safe_rules(body.protection_rules)
    conn = get_conn()
    should_republish = False
    try:
        page = _assert_page_access(conn, page_id, user)
        pixel_id = page.get("pixel_id") or ""
        custom_domain = page.get("custom_domain") or ""
        template_id = int(page.get("template_id") or 1)
        if body.pixel_id is not None:
            new_pixel_id = (body.pixel_id or "").strip()[:80]
            should_republish = should_republish or new_pixel_id != pixel_id
            pixel_id = new_pixel_id
        if body.custom_domain is not None:
            try:
                new_custom_domain = normalize_custom_domain(body.custom_domain)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            should_republish = should_republish or new_custom_domain != custom_domain
            custom_domain = new_custom_domain
        if body.template_id is not None:
            new_template_id = int(body.template_id or 1)
            _assert_template_access(conn, new_template_id, user)
            should_republish = should_republish or new_template_id != template_id
            template_id = new_template_id
        worker_enabled = bool(page.get("worker_enabled") or body.tracking_enabled or body.protection_enabled or (page.get("link_kind") or "landing") == "form")
        conn.execute(
            """UPDATE landing_pages
               SET target_urls=?, rotation_mode=?, tracking_enabled=?, protection_enabled=?,
                   protection_rules=?, worker_enabled=?, custom_domain=?, pixel_id=?, template_id=?,
                   updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (
                json.dumps(urls, ensure_ascii=False),
                rotation_mode,
                1 if body.tracking_enabled else 0,
                1 if body.protection_enabled else 0,
                json.dumps(rules, ensure_ascii=False),
                1 if worker_enabled else 0,
                custom_domain,
                pixel_id,
                template_id,
                page_id,
            ),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
        item = _public_page(updated)
        _refresh_page_ad_link_urls(conn, page_id, item)
        conn.commit()
        item["usage"] = _landing_page_usage(conn, item, user)
        republish_needed = bool(should_republish and str(page.get("status") or "").lower() == "published")
    finally:
        conn.close()
    if republish_needed:
        result = republish_landing_page(page_id, request, user)
        result["asset_republished"] = True
        return result
    return {"success": True, "page": item, "requires_republish_once": not bool(page.get("worker_enabled")), "asset_republished": False}


@router.get("/pages/{page_id}/ad-links")
def list_landing_ad_links(page_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        page = _assert_page_access(conn, page_id, user)
        page_public = _public_page(page)
        rows = conn.execute(
            "SELECT * FROM landing_ad_links WHERE page_id=? ORDER BY id DESC LIMIT 500",
            (page_id,),
        ).fetchall()
        return [
            _public_ad_link(row, page_public, _ad_link_stats(conn, page_id, row["slug"], ad_id=row["ad_id"], days=LANDING_TRACKING_RETENTION_DAYS))
            for row in rows
        ]
    finally:
        conn.close()


@router.get("/ad-links/performance")
def landing_ad_link_performance(days: int = 7, limit: int = 200, user=Depends(get_current_user)):
    days = max(1, min(int(days or 7), LANDING_TRACKING_RETENTION_DAYS))
    limit = max(20, min(int(limit or 200), 500))
    where, params = _scope_where(user, "p")
    sql = """SELECT l.*, p.title AS page_title, p.pages_url AS page_pages_url,
                    p.custom_domain AS page_custom_domain, p.raw_response AS page_raw_response,
                    p.status AS page_status
             FROM landing_ad_links l
             JOIN landing_pages p ON p.id=l.page_id"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY l.id DESC LIMIT ?"
    conn = get_conn()
    try:
        rows = conn.execute(sql, params + [limit]).fetchall()
        items = []
        summary = {
            "total": 0,
            "good": 0,
            "waste": 0,
            "watch": 0,
            "no_data": 0,
            "spend": 0.0,
            "true_contact": 0.0,
            "unique_true_contact": 0.0,
            "effective_true_contact": 0.0,
            "confirmed_actions": 0.0,
            "confirmed_sales": 0.0,
            "confirmed_revenue": 0.0,
            "final_true_contact": 0.0,
            "cost_per_true_contact": None,
            "cost_per_unique_true_contact": None,
            "cost_per_effective_true_contact": None,
            "cost_per_confirmed_action": None,
            "cost_per_confirmed_sale": None,
            "cost_per_final_true_contact": None,
        }
        for row in rows:
            item = dict(row)
            page_public = _public_page(
                {
                    "id": item.get("page_id"),
                    "title": item.get("page_title"),
                    "pages_url": item.get("page_pages_url"),
                    "custom_domain": item.get("page_custom_domain"),
                    "raw_response": item.get("page_raw_response"),
                    "status": item.get("page_status"),
                    "target_urls": "[]",
                    "bound_act_ids": "[]",
                    "protection_rules": "{}",
                    "tracking_enabled": 0,
                    "protection_enabled": 0,
                    "worker_enabled": 0,
                    "last_error": "",
                }
            )
            stats = _ad_link_stats(conn, int(item["page_id"]), item.get("slug") or "", ad_id=item.get("ad_id") or "", days=days)
            decision = _ad_link_decision(stats)
            public_link = _public_ad_link(row, page_public, stats)
            public_link["page_title"] = item.get("page_title") or ""
            public_link["page_status"] = item.get("page_status") or ""
            public_link["decision"] = decision
            items.append(public_link)

            state = decision.get("state") or "no_data"
            if state not in summary:
                state = "no_data"
            summary[state] += 1
            summary["spend"] += float(stats.get("spend") or 0)
            summary["true_contact"] += float(stats.get("true_contact") or 0)
            summary["unique_true_contact"] += float(stats.get("unique_true_contact") or 0)
            summary["effective_true_contact"] += float(stats.get("effective_true_contact", stats.get("true_contact") or 0) or 0)
            summary["confirmed_actions"] += float(stats.get("confirmed_actions") or 0)
            summary["confirmed_sales"] += float(stats.get("confirmed_sales") or 0)
            summary["confirmed_revenue"] += float(stats.get("confirmed_revenue") or 0)
            summary["final_true_contact"] += float(stats.get("final_true_contact", stats.get("effective_true_contact", stats.get("true_contact") or 0)) or 0)

        severity = {"waste": 0, "watch": 1, "good": 2, "no_data": 3}
        items.sort(
            key=lambda x: (
                severity.get((x.get("decision") or {}).get("state"), 9),
                -float((x.get("stats") or {}).get("spend") or 0),
                float((x.get("stats") or {}).get("cost_per_final_true_contact") or 999999),
            )
        )
        summary["total"] = len(items)
        summary["spend"] = round(summary["spend"], 4)
        summary["true_contact"] = round(summary["true_contact"], 4)
        summary["unique_true_contact"] = round(summary["unique_true_contact"], 4)
        summary["effective_true_contact"] = round(summary["effective_true_contact"], 4)
        summary["confirmed_actions"] = round(summary["confirmed_actions"], 4)
        summary["confirmed_sales"] = round(summary["confirmed_sales"], 4)
        summary["confirmed_revenue"] = round(summary["confirmed_revenue"], 4)
        summary["final_true_contact"] = round(summary["final_true_contact"], 4)
        if summary["spend"] > 0 and summary["true_contact"] > 0:
            summary["cost_per_true_contact"] = round(summary["spend"] / summary["true_contact"], 4)
        if summary["spend"] > 0 and summary["unique_true_contact"] > 0:
            summary["cost_per_unique_true_contact"] = round(summary["spend"] / summary["unique_true_contact"], 4)
        if summary["spend"] > 0 and summary["effective_true_contact"] > 0:
            summary["cost_per_effective_true_contact"] = round(summary["spend"] / summary["effective_true_contact"], 4)
        if summary["spend"] > 0 and summary["confirmed_actions"] > 0:
            summary["cost_per_confirmed_action"] = round(summary["spend"] / summary["confirmed_actions"], 4)
        if summary["spend"] > 0 and summary["confirmed_sales"] > 0:
            summary["cost_per_confirmed_sale"] = round(summary["spend"] / summary["confirmed_sales"], 4)
        if summary["spend"] > 0 and summary["final_true_contact"] > 0:
            summary["cost_per_final_true_contact"] = round(summary["spend"] / summary["final_true_contact"], 4)
        return {"success": True, "days": days, "limit": limit, "summary": summary, "items": items}
    finally:
        conn.close()


@router.post("/pages/{page_id}/ad-links/auto-bind")
def auto_bind_landing_ad_links(page_id: int, body: LandingAdAutoBindReq = LandingAdAutoBindReq(), user=Depends(get_current_user)):
    conn = get_conn()
    try:
        page = _assert_page_access(conn, page_id, user)
        rows = conn.execute(
            """SELECT * FROM landing_ad_links
               WHERE page_id=?
                 AND COALESCE(status,'reserved') NOT IN ('archived','failed','unused')
               ORDER BY id ASC""",
            (page_id,),
        ).fetchall()
        links = [dict(r) for r in rows]
        unbound = {
            str(item.get("slug") or ""): item
            for item in links
            if str(item.get("slug") or "").strip() and not str(item.get("ad_id") or "").strip()
        }
        known_slugs = {str(item.get("slug") or "") for item in links if str(item.get("slug") or "").strip()}
        accounts = _landing_auto_bind_accounts(conn, page, body, user)
        result = {
            "success": True,
            "page_id": page_id,
            "accounts_checked": 0,
            "ads_checked": 0,
            "links_total": len(links),
            "links_unbound": len(unbound),
            "bound": [],
            "skipped": [],
            "conflicts": [],
        }
        if not unbound:
            return result
        for acc in accounts:
            act_id = acc["act_id"]
            token = get_exec_token(_fb_act_id(act_id), ACTION_READ, notify_exhausted=False)
            if not token:
                result["skipped"].append({"act_id": act_id, "reason": "missing_read_token"})
                continue
            result["accounts_checked"] += 1
            ads, err_msg = _landing_fetch_ads_for_link_binding(act_id, token, body.limit_ads_per_account)
            if err_msg:
                result["skipped"].append({"act_id": act_id, "reason": "fb_api_error", "message": err_msg})
                continue
            for ad in ads:
                result["ads_checked"] += 1
                ad_id = _normalize_ad_id(ad.get("id") or "")
                if not ad_id:
                    continue
                urls = _landing_extract_url_strings(ad)
                matched_slug = ""
                matched_url = ""
                for url in urls:
                    slug = _landing_slug_from_url(url, known_slugs)
                    if slug:
                        matched_slug = slug
                        matched_url = url
                        break
                if not matched_slug:
                    continue
                link = unbound.get(matched_slug)
                if not link:
                    result["conflicts"].append({"slug": matched_slug, "ad_id": ad_id, "reason": "already_bound_or_not_found"})
                    continue
                adset = ad.get("adset") if isinstance(ad.get("adset"), dict) else {}
                campaign = adset.get("campaign") if isinstance(adset.get("campaign"), dict) else {}
                campaign_id = ad.get("campaign_id") or campaign.get("id") or ""
                campaign_name = campaign.get("name") or ""
                adset_id = ad.get("adset_id") or adset.get("id") or ""
                adset_name = adset.get("name") or ""
                conn.execute(
                    """UPDATE landing_ad_links
                       SET act_id=?, account_name=?, campaign_id=?, campaign_name=?,
                           adset_id=?, adset_name=?, ad_id=?, ad_name=?,
                           status=CASE WHEN COALESCE(status,'reserved')='reserved' THEN 'active' ELSE status END,
                           updated_at=datetime('now','+8 hours')
                       WHERE id=? AND (COALESCE(ad_id,'')='' OR ad_id=?)""",
                    (
                        act_id,
                        acc.get("name") or "",
                        str(campaign_id or "")[:80],
                        str(campaign_name or "")[:255],
                        str(adset_id or "")[:80],
                        str(adset_name or "")[:255],
                        ad_id,
                        str(ad.get("name") or ad_id)[:255],
                        int(link["id"]),
                        ad_id,
                    ),
                )
                result["bound"].append({
                    "slug": matched_slug,
                    "ad_id": ad_id,
                    "ad_name": ad.get("name") or ad_id,
                    "act_id": act_id,
                    "account_name": acc.get("name") or "",
                    "url": matched_url,
                })
                unbound.pop(matched_slug, None)
                if not unbound:
                    break
            if not unbound:
                break
        conn.commit()
        result["links_unbound_after"] = len(unbound)
        return result
    finally:
        conn.close()


@router.post("/pages/{page_id}/ad-links")
def create_landing_ad_links(page_id: int, body: LandingAdLinkCreate, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        page = _assert_page_access(conn, page_id, user)
        page_public = _public_page(page)
        base_url = _page_public_url(page_public)
        if not base_url:
            raise HTTPException(status_code=400, detail="该落地页还没有可用主链接，发布成功后才能生成广告入口")
        target_urls = [u.strip() for u in (body.target_urls or []) if isinstance(u, str) and u.strip()][:200]
        if not target_urls and body.target_url and str(body.target_url).strip():
            target_urls = [str(body.target_url).strip()]
        if any(not (u.startswith("http://") or u.startswith("https://")) for u in target_urls):
            raise HTTPException(status_code=400, detail="target_urls must be http(s) URLs")
        count = _landing_ad_link_create_count(body.count, target_urls)
        act_id = (_clean_act_ids([body.act_id or ""]) or [""])[0]
        team_id = page.get("team_id")
        owner_id = page.get("owner_user_id") if page.get("owner_user_id") is not None else (user_id(user) if is_operator_user(user) else None)
        created = []
        for idx in range(count):
            slug = _short_slug()
            for _attempt in range(8):
                exists = conn.execute("SELECT 1 FROM landing_ad_links WHERE slug=?", (slug,)).fetchone()
                if not exists:
                    break
                slug = _short_slug()
            else:
                raise HTTPException(status_code=500, detail="短链生成冲突，请重试")
            public_url = _ad_link_url(page_public, slug)
            conn.execute(
                """INSERT INTO landing_ad_links
                   (page_id, slug, public_url, act_id, account_name, campaign_id, campaign_name,
                    adset_id, adset_name, ad_id, ad_name, target_url, target_urls, status, note,
                    team_id, owner_user_id, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    page_id,
                    slug,
                    public_url,
                    act_id,
                    _truncate(body.account_name, 255),
                    _truncate(body.campaign_id, 80),
                    _truncate(body.campaign_name, 255),
                    _truncate(body.adset_id, 80),
                    _truncate(body.adset_name, 255),
                    _truncate(body.ad_id, 80),
                    _truncate(body.ad_name, 255),
                    _truncate((target_urls[0] if target_urls else body.target_url), 1000),
                    json.dumps(target_urls, ensure_ascii=False),
                    "active" if body.ad_id else "reserved",
                    _truncate(body.note, 1000),
                    team_id,
                    owner_id,
                    user.get("username", "unknown"),
                ),
            )
            link_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            row = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
            created.append(_public_ad_link(row, page_public, {"visit": 0, "click": 0, "redirect": 0, "block": 0, "total": 0}))
        conn.commit()
        return {"success": True, "page_id": page_id, "links": created}
    finally:
        conn.close()


@router.patch("/ad-links/{link_id}")
def update_landing_ad_link(link_id: int, body: LandingAdLinkPatch, user=Depends(get_current_user)):
    allowed_status = {"reserved", "active", "paused", "archived", "failed", "unused"}
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="广告入口不存在")
        page = _assert_page_access(conn, int(row["page_id"]), user)
        updates, params = [], []
        mapping = {
            "account_name": _truncate(body.account_name, 255) if body.account_name is not None else None,
            "campaign_id": _truncate(body.campaign_id, 80) if body.campaign_id is not None else None,
            "campaign_name": _truncate(body.campaign_name, 255) if body.campaign_name is not None else None,
            "adset_id": _truncate(body.adset_id, 80) if body.adset_id is not None else None,
            "adset_name": _truncate(body.adset_name, 255) if body.adset_name is not None else None,
            "ad_id": _truncate(body.ad_id, 80) if body.ad_id is not None else None,
            "ad_name": _truncate(body.ad_name, 255) if body.ad_name is not None else None,
            "target_url": _truncate(body.target_url, 1000) if body.target_url is not None else None,
            "note": _truncate(body.note, 1000) if body.note is not None else None,
        }
        if body.target_urls is not None:
            target_urls = [u.strip() for u in (body.target_urls or []) if isinstance(u, str) and u.strip()][:200]
            if any(not (u.startswith("http://") or u.startswith("https://")) for u in target_urls):
                raise HTTPException(status_code=400, detail="target_urls must be http(s) URLs")
            mapping["target_urls"] = json.dumps(target_urls, ensure_ascii=False)
            mapping["target_url"] = _truncate(target_urls[0] if target_urls else "", 1000)
        if body.act_id is not None:
            mapping["act_id"] = (_clean_act_ids([body.act_id]) or [""])[0]
        if body.status is not None:
            status = (body.status or "").strip().lower()
            if status not in allowed_status:
                raise HTTPException(status_code=400, detail="状态只能是 reserved / active / paused / archived")
            mapping["status"] = status
        for key, value in mapping.items():
            if value is not None:
                updates.append(f"{key}=?")
                params.append(value)
        if not updates:
            page_public = _public_page(page)
            return {"success": True, "link": _public_ad_link(row, page_public, _ad_link_stats(conn, int(row["page_id"]), row["slug"], ad_id=row["ad_id"]))}
        updates.append("updated_at=datetime('now','+8 hours')")
        params.append(link_id)
        conn.execute(f"UPDATE landing_ad_links SET {', '.join(updates)} WHERE id=?", params)
        conn.commit()
        updated = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
        page_public = _public_page(page)
        return {"success": True, "link": _public_ad_link(updated, page_public, _ad_link_stats(conn, int(updated["page_id"]), updated["slug"], ad_id=updated["ad_id"]))}
    finally:
        conn.close()


@router.delete("/ad-links/{link_id}")
def delete_landing_ad_link(link_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="广告入口不存在")
        page = _assert_page_access(conn, int(row["page_id"]), user)
        if str(row["ad_id"] or "").strip():
            raise HTTPException(status_code=400, detail="该广告入口已绑定广告，不能删除；如需停用请先解除绑定或归档。")

        slug = str(row["slug"] or "").strip()
        event_count = 0
        if _has_table(conn, "landing_events"):
            event_count = int(
                (
                    conn.execute(
                        """SELECT COUNT(*) AS cnt
                           FROM landing_events
                           WHERE page_id=?
                             AND (
                              path=?
                              OR COALESCE(metadata,'') LIKE ?
                              OR COALESCE(metadata,'') LIKE ?
                             )""",
                        (
                            int(row["page_id"]),
                            f"/a/{slug}",
                            f'%"ad_slug":"{slug}"%',
                            f'%"ad_slug": "{slug}"%',
                        ),
                    ).fetchone()
                    or {"cnt": 0}
                )["cnt"]
                or 0
            )
        if event_count > 0:
            raise HTTPException(status_code=400, detail="该广告入口已有访问/点击/跳转数据，不能删除；可保留用于追溯。")

        result_count = 0
        if _has_table(conn, "landing_ad_link_results"):
            result_count = int(
                (
                    conn.execute(
                        "SELECT COUNT(*) AS cnt FROM landing_ad_link_results WHERE link_id=?",
                        (link_id,),
                    ).fetchone()
                    or {"cnt": 0}
                )["cnt"]
                or 0
            )
        if result_count > 0:
            raise HTTPException(status_code=400, detail="该广告入口已有真实结果回填，不能删除。")

        if _has_table(conn, "landing_ad_route_state"):
            conn.execute("DELETE FROM landing_ad_route_state WHERE link_id=?", (link_id,))
        cur = conn.execute("DELETE FROM landing_ad_links WHERE id=?", (link_id,))
        conn.commit()
        return {
            "success": True,
            "deleted": int(cur.rowcount or 0),
            "page_id": int(page["id"]),
            "slug": slug,
        }
    finally:
        conn.close()


@router.get("/ad-links/{link_id}/result-preview")
def landing_ad_link_result_preview(
    link_id: int,
    result_date: Optional[str] = None,
    refresh: int = 0,
    user=Depends(get_current_user),
):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="广告入口不存在")
        page = _assert_page_access(conn, int(row["page_id"]), user)
        result_day = _normalize_result_date(result_date)
        stats = _ad_link_stats(conn, int(row["page_id"]), row["slug"], ad_id=row["ad_id"], date_from=result_day, date_to=result_day)
        live_refresh = {"ok": False, "skipped": True, "reason": "local_data_available"}
        needs_refresh = bool(refresh) or (
            row["ad_id"]
            and row["act_id"]
            and not stats.get("spend_source")
            and float(stats.get("spend") or 0) <= 0
        )
        if needs_refresh:
            live_refresh = _refresh_landing_ad_link_spend(conn, row, result_day)
            row = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
            stats = _ad_link_stats(conn, int(row["page_id"]), row["slug"], ad_id=row["ad_id"], date_from=result_day, date_to=result_day)
        page_public = _public_page(page)
        return {
            "success": True,
            "result_date": result_day,
            "refresh": live_refresh,
            "link": _public_ad_link(row, page_public, stats),
            "stats": stats,
            "ad": {
                "act_id": row["act_id"] or "",
                "account_name": row["account_name"] or "",
                "campaign_id": row["campaign_id"] or "",
                "campaign_name": row["campaign_name"] or "",
                "adset_id": row["adset_id"] or "",
                "adset_name": row["adset_name"] or "",
                "ad_id": row["ad_id"] or "",
                "ad_name": row["ad_name"] or "",
            },
        }
    finally:
        conn.close()


@router.patch("/ad-links/{link_id}/result")
def update_landing_ad_link_result(link_id: int, body: LandingAdLinkResultPatch, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="广告入口不存在")
        page = _assert_page_access(conn, int(row["page_id"]), user)
        result_date = _normalize_result_date(body.result_date)
        conn.execute(
            """INSERT INTO landing_ad_link_results
               (link_id, result_date, confirmed_actions, confirmed_sales, confirmed_revenue, source, note, updated_by, updated_at, created_at)
               VALUES (?,?,?,?,?,?,?,?,datetime('now','+8 hours'),datetime('now','+8 hours'))
               ON CONFLICT(link_id,result_date) DO UPDATE SET
                 confirmed_actions=excluded.confirmed_actions,
                 confirmed_sales=excluded.confirmed_sales,
                 confirmed_revenue=excluded.confirmed_revenue,
                 source=excluded.source,
                 note=excluded.note,
                 updated_by=excluded.updated_by,
                 updated_at=datetime('now','+8 hours')""",
            (
                link_id,
                result_date,
                int(body.confirmed_actions or 0),
                int(body.confirmed_sales or 0),
                float(body.confirmed_revenue or 0),
                _truncate((body.source or "manual").strip().lower(), 40),
                _truncate(body.note, 1000),
                user.get("username", "unknown"),
            ),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM landing_ad_links WHERE id=?", (link_id,)).fetchone()
        page_public = _public_page(page)
        return {
            "success": True,
            "result_date": result_date,
            "link": _public_ad_link(
                updated,
                page_public,
                _ad_link_stats(conn, int(updated["page_id"]), updated["slug"], ad_id=updated["ad_id"], date_from=result_date, date_to=result_date),
            ),
        }
    finally:
        conn.close()


@router.post("/ad-links/results/import")
def import_landing_ad_link_results(body: LandingAdLinkResultImport, user=Depends(get_current_user)):
    rows = body.rows or []
    if not rows:
        raise HTTPException(status_code=400, detail="没有可导入的真实结果行")
    default_result_date = _normalize_result_date(body.result_date)
    conn = get_conn()
    updated, errors = [], []
    try:
        for idx, item in enumerate(rows, start=1):
            slug = (item.slug or "").strip()
            ad_id = (item.ad_id or "").strip()
            if not slug and not ad_id:
                errors.append({"row": idx, "reason": "缺少入口码或广告ID"})
                continue
            if slug:
                row = conn.execute("SELECT * FROM landing_ad_links WHERE slug=?", (slug,)).fetchone()
            else:
                row = conn.execute("SELECT * FROM landing_ad_links WHERE ad_id=? ORDER BY id DESC LIMIT 1", (ad_id,)).fetchone()
            if not row:
                errors.append({"row": idx, "slug": slug, "ad_id": ad_id, "reason": "未匹配到广告入口"})
                continue
            try:
                _assert_page_access(conn, int(row["page_id"]), user)
            except HTTPException as exc:
                errors.append({"row": idx, "slug": slug, "ad_id": ad_id, "reason": exc.detail})
                continue
            try:
                result_date = _normalize_result_date(item.result_date or default_result_date)
            except HTTPException as exc:
                errors.append({"row": idx, "slug": slug, "ad_id": ad_id, "reason": exc.detail})
                continue
            conn.execute(
                """INSERT INTO landing_ad_link_results
                   (link_id, result_date, confirmed_actions, confirmed_sales, confirmed_revenue, source, note, updated_by, updated_at, created_at)
                   VALUES (?,?,?,?,?,?,?,?,datetime('now','+8 hours'),datetime('now','+8 hours'))
                   ON CONFLICT(link_id,result_date) DO UPDATE SET
                     confirmed_actions=excluded.confirmed_actions,
                     confirmed_sales=excluded.confirmed_sales,
                     confirmed_revenue=excluded.confirmed_revenue,
                     source=excluded.source,
                     note=excluded.note,
                     updated_by=excluded.updated_by,
                     updated_at=datetime('now','+8 hours')""",
                (
                    int(row["id"]),
                    result_date,
                    int(item.confirmed_actions or 0),
                    int(item.confirmed_sales or 0),
                    float(item.confirmed_revenue or 0),
                    _truncate((body.source or "csv").strip().lower(), 40),
                    _truncate(item.note, 1000),
                    user.get("username", "unknown"),
                ),
            )
            updated.append({"row": idx, "id": int(row["id"]), "slug": row["slug"], "ad_id": row["ad_id"], "result_date": result_date})
        conn.commit()
        return {"success": True, "updated": updated, "errors": errors, "updated_count": len(updated), "error_count": len(errors)}
    finally:
        conn.close()


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
    bind_target = (body.bind_target or "none").strip().lower()
    if link_kind == "form" and bind_target in {"landing", "both"}:
        raise HTTPException(
            status_code=400,
            detail="Form redirect links can only bind to account form_link or no account field; do not write them to landing_url",
        )
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
        raise HTTPException(status_code=400, detail="发布通道没有选择默认发布账号，请先选择账号")

    team_id, owner_id = _stamp(user, None)
    if team_id is None:
        team_id = token_row.get("team_id")
    if owner_id is None and token_row.get("owner_user_id") is not None:
        owner_id = token_row.get("owner_user_id")
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
                bind_target,
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
            route_url=_route_url(request),
            config_url=_config_url(request),
        )
        response = deploy_pages_static(raw_token, cf_account_id, project_name, work_dir)
        deployment_id = str(response.get("id") or "")
        pages_url = response.get("stable_url") or stable_pages_url(project_name, deployment=response) or response.get("url") or response.get("aliases", [None])[0] or ""
        public_url = pages_url
        domain_error = ""
        domain_notice = ""
        domain_result = None
        dns_result = None
        if custom_domain:
            dns_result, domain_result, domain_error, domain_notice = _setup_custom_domain_automation(
                raw_token,
                cf_account_id,
                project_name,
                custom_domain,
                pages_url,
                user,
            )
            if _domain_status_usable(domain_result, None):
                public_url = f"https://{custom_domain}"
        binding = _bind_page_to_accounts(conn, body.bind_act_ids, bind_target, public_url, user)
        response_payload = dict(response)
        if dns_result is not None:
            response_payload["custom_domain_dns_result"] = dns_result
        if domain_result is not None:
            response_payload["custom_domain_result"] = domain_result
            response_payload["domain_status"] = domain_result
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
        _refresh_page_ad_link_urls(conn, page_id, item)
        conn.commit()
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
                    bind_target,
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


@router.post("/pages/{page_id}/republish")
def republish_landing_page(page_id: int, request: Request, user=Depends(get_current_user)):
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    if str(page.get("status") or "").lower() == "archived":
        conn.close()
        raise HTTPException(status_code=400, detail="已归档的落地页不能重新发布")

    token_row = _assert_token_access(conn, int(page.get("cf_token_id") or 0), user)
    template = _assert_template_access(conn, int(page.get("template_id") or 1), user)
    raw_token = decrypt_token(token_row["access_token_enc"])
    cf_account_id, cf_account_name = _resolve_token_account(token_row)
    if not cf_account_id:
        conn.close()
        raise HTTPException(status_code=400, detail="发布通道没有选择默认发布账号，请先选择账号")

    title = (page.get("title") or "").strip() or f"Landing {page_id}"
    project_name = (page.get("project_name") or "").strip() or sanitize_project_name(title)
    custom_domain = (page.get("custom_domain") or "").strip()
    urls = [u for u in _json_loads(page.get("target_urls"), []) if isinstance(u, str) and u.strip()]
    if not urls:
        conn.close()
        raise HTTPException(status_code=400, detail="该落地页没有可用跳转链接，无法重新发布")
    link_kind = (page.get("link_kind") or "landing").strip().lower()
    if link_kind not in {"landing", "form"}:
        link_kind = "landing"
    protection_rules = _safe_rules(_json_loads(page.get("protection_rules"), {}))
    tracking_enabled = bool(page.get("tracking_enabled"))
    protection_enabled = bool(page.get("protection_enabled"))
    worker_enabled = bool(tracking_enabled or protection_enabled or link_kind == "form")
    ingest_secret = (page.get("ingest_secret") or "").strip() or secrets.token_urlsafe(32)
    bind_target = _effective_bind_target_for_link_kind(link_kind, page.get("bind_target") or "none")
    bound_act_ids = _clean_act_ids(_json_loads(page.get("bound_act_ids"), []))
    work_dir = None
    try:
        conn.execute(
            """UPDATE landing_pages
               SET status='deploying', ingest_secret=?, worker_enabled=?,
                   last_error='', updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (ingest_secret, 1 if worker_enabled else 0, page_id),
        )
        conn.commit()
        work_dir = prepare_template(
            template["template_path"],
            pixel_id=page.get("pixel_id") or "",
            target_urls=urls,
            rotation_mode=page.get("rotation_mode") or "sequential",
            link_kind=link_kind,
            worker_enabled=worker_enabled,
            tracking_enabled=tracking_enabled,
            protection_enabled=protection_enabled,
            protection_rules=protection_rules,
            page_id=page_id,
            ingest_secret=ingest_secret,
            ingest_url=_ingest_url(request),
            route_url=_route_url(request),
            config_url=_config_url(request),
        )
        response = deploy_pages_static(raw_token, cf_account_id, project_name, work_dir)
        deployment_id = str(response.get("id") or "")
        pages_url = response.get("stable_url") or stable_pages_url(project_name, deployment=response) or response.get("url") or response.get("aliases", [None])[0] or page.get("pages_url") or ""
        public_url = pages_url
        domain_error = ""
        domain_notice = ""
        domain_result = None
        dns_result = None
        if custom_domain:
            dns_result, domain_result, domain_error, domain_notice = _setup_custom_domain_automation(
                raw_token,
                cf_account_id,
                project_name,
                custom_domain,
                pages_url,
                user,
            )
            if _domain_status_usable(domain_result, None):
                public_url = f"https://{custom_domain}"
        binding = _bind_page_to_accounts(conn, bound_act_ids, bind_target, public_url, user)
        response_payload = dict(response)
        response_payload["republished_at"] = _now_cst()
        if dns_result is not None:
            response_payload["custom_domain_dns_result"] = dns_result
        if domain_result is not None:
            response_payload["custom_domain_result"] = domain_result
            response_payload["domain_status"] = domain_result
        if domain_notice:
            response_payload["custom_domain_notice"] = domain_notice
        note = page.get("note") or ""
        if domain_error:
            note += ("\n" if note else "") + domain_error
        if domain_notice:
            note += ("\n" if note else "") + domain_notice
        if binding.get("skipped"):
            note += ("\n" if note else "") + "Republish binding skipped: " + json.dumps(binding.get("skipped", []), ensure_ascii=False)
        conn.execute(
            """UPDATE landing_pages
               SET deployment_id=?, pages_url=?, cf_account_id=?, cf_account_name=?,
                   raw_response=?, last_error=?, note=?, status='published',
                   worker_enabled=?, updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (
                deployment_id,
                pages_url,
                cf_account_id,
                cf_account_name,
                json.dumps(response_payload, ensure_ascii=False),
                domain_error,
                note,
                1 if worker_enabled else 0,
                page_id,
            ),
        )
        conn.commit()
        saved = conn.execute("SELECT * FROM landing_pages WHERE id=?", (page_id,)).fetchone()
        item = _public_page(saved)
        _refresh_page_ad_link_urls(conn, page_id, item)
        conn.commit()
        item["binding"] = binding
        item["domain_error"] = domain_error
        return {"success": True, "page": item}
    except HTTPException:
        raise
    except Exception as exc:
        conn.execute(
            "UPDATE landing_pages SET status='failed', last_error=?, updated_at=datetime('now','+8 hours') WHERE id=?",
            (str(exc), page_id),
        )
        conn.commit()
        raise HTTPException(status_code=400, detail=f"Republish failed: {exc}") from exc
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


@router.post("/pages/{page_id}/repair-domain")
def repair_landing_page_domain(page_id: int, user=Depends(get_current_user)):
    """Re-run DNS CNAME creation/update, Pages custom-domain binding and status refresh."""
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    try:
        result = _refresh_landing_domain_record(conn, page, user)
        return {"success": True, "repaired": True, **result}
    except HTTPException:
        raise
    except Exception as exc:
        conn.execute(
            "UPDATE landing_pages SET last_error=?, updated_at=datetime('now','+8 hours') WHERE id=?",
            (f"Domain repair failed: {exc}", page_id),
        )
        conn.commit()
        raise HTTPException(status_code=400, detail=f"Domain repair failed: {exc}") from exc
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


@router.post("/pages/repair-domains")
def repair_landing_page_domains(limit: int = 50, user=Depends(get_current_user)):
    """Batch repair custom domains visible to the current user."""
    return refresh_landing_page_domains(limit=limit, user=user)


@router.delete("/pages/{page_id}")
def archive_landing_page(page_id: int, cleanup: bool = False, user=Depends(get_current_user)):
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    item = _public_page(page)
    usage = _landing_page_usage(conn, item, user)
    if cleanup:
        if usage["total"] > 0:
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Landing page is still in use by {usage['total']} resource(s): "
                    f"{len(usage.get('accounts') or [])} account(s), "
                    f"{len(usage.get('campaigns') or [])} campaign(s), "
                    f"{len(usage.get('ad_links') or [])} ad link(s)"
                ),
            )
        cloudflare_cleanup = {"skipped": True, "reason": "no published remote project"}
        project_name = (page.get("project_name") or "").strip()
        has_remote_project = bool(project_name and (page.get("pages_url") or page.get("deployment_id") or page.get("status") == "published"))
        if has_remote_project:
            token_id = page.get("cf_token_id")
            if not token_id:
                conn.close()
                raise HTTPException(status_code=400, detail="远程项目存在但发布通道已缺失，请先归档或恢复 API Token")
            token_row = _assert_token_access(conn, int(token_id), user)
            cf_account_id = (page.get("cf_account_id") or token_row.get("selected_account_id") or token_row.get("cf_account_id") or "").strip()
            if not cf_account_id:
                conn.close()
                raise HTTPException(status_code=400, detail="发布账号缺失，请先选择 API 账号再删除远程项目")
            try:
                raw_token = decrypt_token(token_row["access_token_enc"])
                cloudflare_cleanup = delete_pages_project(raw_token, cf_account_id, project_name)
            except CloudflareError as exc:
                conn.close()
                raise HTTPException(status_code=400, detail=f"远程项目删除失败：{_public_provider_error(exc, user)}") from exc
        local_cleanup = _delete_landing_page_local_rows(conn, page_id)
        conn.execute("DELETE FROM landing_pages WHERE id=?", (page_id,))
        conn.commit()
        conn.close()
        return {"success": True, "deleted": True, "cloudflare": cloudflare_cleanup, "usage": usage, "local_cleanup": local_cleanup}
    conn.execute("UPDATE landing_pages SET status='archived', updated_at=datetime('now','+8 hours') WHERE id=?", (page_id,))
    conn.commit()
    conn.close()
    return {"success": True, "archived": True, "usage": usage}


@router.post("/router/next")
def next_landing_route_target(body: LandingRouteNextReq, request: Request):
    """Return the next redirect target for Cloudflare Pages Workers.

    This endpoint is intentionally unauthenticated because it is called from the
    deployed Cloudflare Worker. The per-page ingest secret is required and the
    response only returns one target URL for the current redirect.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        page = conn.execute(
            """SELECT id, ingest_secret, target_urls, rotation_mode, status, pages_url, custom_domain
               FROM landing_pages
               WHERE id=?""",
            (body.page_id,),
        ).fetchone()
        if (
            not page
            or not page["ingest_secret"]
            or not secrets.compare_digest(str(page["ingest_secret"]), str(body.secret or ""))
            or str(page["status"] or "").lower() == "archived"
        ):
            conn.rollback()
            raise HTTPException(status_code=403, detail="invalid landing route secret")
        metadata = body.metadata or {}
        slug = _ad_slug_from_path(body.path or "") or str(metadata.get("ad_slug") or "").strip()
        ad_id = _normalize_ad_id(metadata.get("ad_id") or metadata.get("ad") or metadata.get("aid") or "")
        if slug:
            link = conn.execute(
                """SELECT id, target_url, target_urls FROM landing_ad_links
                   WHERE page_id=? AND slug=?
                     AND COALESCE(status,'reserved') NOT IN ('paused','archived','failed','unused')
                   LIMIT 1""",
                (body.page_id, slug),
            ).fetchone()
            if not link:
                conn.commit()
                return {
                    "success": False,
                    "blocked": True,
                    "reason": "entry_unavailable",
                    "slug": slug,
                }
            selected = _select_landing_ad_target(conn, link, dict(page), str(page["rotation_mode"] or "sequential"))
            if selected:
                conn.commit()
                return {
                    "success": True,
                    "target_url": selected["target_url"],
                    "index": selected["index"],
                    "total": selected["total"],
                    "mode": selected["mode"],
                    "slug": slug,
                }
        if ad_id:
            link = conn.execute(
                """SELECT id, target_url, target_urls, slug FROM landing_ad_links
                   WHERE page_id=? AND ad_id=?
                     AND COALESCE(status,'reserved') NOT IN ('paused','archived','failed','unused')
                   ORDER BY CASE
                              WHEN COALESCE(target_urls,'[]') NOT IN ('[]','') THEN 0
                              WHEN COALESCE(target_url,'')<>'' THEN 1
                              ELSE 2
                            END, updated_at DESC, id DESC
                   LIMIT 1""",
                (body.page_id, ad_id),
            ).fetchone()
            selected = _select_landing_ad_target(conn, link, dict(page), str(page["rotation_mode"] or "sequential"))
            if selected:
                conn.commit()
                return {
                    "success": True,
                    "target_url": selected["target_url"],
                    "index": selected["index"],
                    "total": selected["total"],
                    "mode": selected["mode"],
                    "ad_id": ad_id,
                    "slug": (link["slug"] if link else "") or "",
                }
        urls = [u for u in _json_loads(page["target_urls"], []) if isinstance(u, str) and u.strip()]
        if not urls:
            conn.rollback()
            raise HTTPException(status_code=404, detail="no target urls configured")
        mode = str(page["rotation_mode"] or "sequential").strip().lower()
        if mode == "first":
            idx = 0
        elif mode == "random":
            idx = secrets.randbelow(len(urls))
        else:
            state = conn.execute(
                "SELECT cursor FROM landing_route_state WHERE page_id=?",
                (body.page_id,),
            ).fetchone()
            cursor = int(state["cursor"] or 0) if state else 0
            idx = cursor % len(urls)
            next_cursor = cursor + 1
            conn.execute(
                """INSERT INTO landing_route_state (page_id, cursor, updated_at)
                   VALUES (?, ?, datetime('now','+8 hours'))
                   ON CONFLICT(page_id) DO UPDATE SET
                     cursor=excluded.cursor,
                     updated_at=excluded.updated_at""",
                (body.page_id, next_cursor),
            )
        conn.commit()
        return {
            "success": True,
            "target_url": urls[idx],
            "index": idx,
            "total": len(urls),
            "mode": mode if mode in {"first", "random"} else "sequential",
        }
    except HTTPException:
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"route target failed: {exc}") from exc
    finally:
        conn.close()


@router.post("/events/ingest")
async def ingest_landing_event(body: LandingEventIngest, request: Request):
    allowed_events = {"visit", "pass", "block", "click", "redirect", "submit", "error"}
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
    slug = _ad_slug_from_path(body.path or "") or str(metadata.get("ad_slug") or "").strip()
    ad_id = _normalize_ad_id(metadata.get("ad_id") or metadata.get("ad") or metadata.get("aid") or "")
    if slug and ad_id:
        try:
            conn.execute(
                """UPDATE landing_ad_links
                   SET ad_id=?,
                       status=CASE WHEN COALESCE(status,'reserved')='reserved' THEN 'active' ELSE status END,
                       updated_at=datetime('now','+8 hours')
                   WHERE page_id=? AND slug=?
                     AND (COALESCE(ad_id,'')='' OR ad_id=?)""",
                (ad_id, body.page_id, slug, ad_id),
            )
        except Exception:
            logger.exception("landing ad link auto-bind failed: page_id=%s slug=%s ad_id=%s", body.page_id, slug, ad_id)
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
    try:
        _cleanup_landing_tracking(conn)
    except Exception:
        logger.exception("landing tracking retention cleanup failed")
    conn.commit()
    conn.close()
    return {"success": True}


@router.get("/pages/{page_id}/health")
def landing_page_health(page_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    page = _assert_page_access(conn, page_id, user)
    page_dict = dict(page)
    item = _public_page(page)
    raw_response_update: Optional[dict[str, Any]] = None
    custom_domain_for_probe = str(item.get("custom_domain") or "").strip()
    if custom_domain_for_probe and not item.get("custom_domain_usable"):
        runtime_checked_at = _now_cst()
        if _custom_domain_runtime_usable(custom_domain_for_probe, bool(item.get("worker_enabled"))):
            raw_payload = _json_loads(page_dict.get("raw_response"), {})
            if not isinstance(raw_payload, dict):
                raw_payload = {}
            raw_payload["custom_domain_runtime_usable"] = True
            raw_payload["custom_domain_runtime_checked_at"] = runtime_checked_at
            domain_status = item.get("domain_status") or raw_payload.get("domain_status") or raw_payload.get("custom_domain_result")
            if not _domain_status_usable(domain_status, item.get("last_error")):
                raw_payload["custom_domain_status_mismatch"] = {
                    "runtime_usable": True,
                    "cloudflare_status": _domain_status_text(domain_status) or "unknown",
                    "checked_at": runtime_checked_at,
                    "message": "Public runtime is reachable, while provider domain status is not active yet.",
                }
            else:
                raw_payload.pop("custom_domain_status_mismatch", None)
            raw_response_update = raw_payload
            item["custom_domain_runtime_usable"] = True
            item["custom_domain_runtime_checked_at"] = runtime_checked_at
            item["custom_domain_usable"] = True
            item["public_url"] = f"https://{custom_domain_for_probe}"
            item["public_url_source"] = "custom_domain"
            item["custom_domain_status_mismatch"] = raw_payload.get("custom_domain_status_mismatch")
    fb_probe_token, fb_probe_source = _landing_facebook_probe_token(conn, page_dict, user)
    conn.close()

    checks: list[dict[str, str]] = []
    public_url = (item.get("public_url") or "").strip()
    pages_url = (item.get("pages_url") or "").strip()
    link_kind = str(item.get("link_kind") or "landing").strip().lower()
    targets = [u for u in item.get("target_urls") or [] if isinstance(u, str) and u.strip()]
    status = str(item.get("status") or "").strip().lower()

    checks.append({
        "key": "status",
        "status": "pass" if status == "published" else "warn",
        "label": "发布状态",
        "detail": "已发布" if status == "published" else f"当前状态：{status or '未知'}",
    })
    checks.append({
        "key": "public_url",
        "status": "pass" if public_url else "fail",
        "label": "公开链接",
        "detail": public_url or "没有可访问的备用域或自定义域链接",
    })
    if item.get("custom_domain"):
        checks.append({
            "key": "custom_domain",
            "status": "pass" if item.get("custom_domain_usable") else "warn",
            "label": "自定义域名",
            "detail": (
                f"https://{item.get('custom_domain')} 已作为主链接"
                if item.get("custom_domain_usable")
                else f"域名还未确认可用，当前使用备用域；{item.get('custom_domain_dns_hint') or '请完成 DNS CNAME 指向'}"
            ),
        })
    elif pages_url:
        checks.append({
            "key": "custom_domain",
            "status": "warn",
            "label": "自定义域名",
            "detail": "未配置自定义域名，当前使用系统备用域",
        })

    checks.append({
        "key": "targets",
        "status": "pass" if targets else "fail",
        "label": "跳转目标",
        "detail": f"已配置 {len(targets)} 个目标链接" if targets else "没有配置跳转目标",
    })
    if link_kind == "form":
        checks.append({
            "key": "form_mode",
            "status": "pass" if item.get("worker_enabled") else "fail",
            "label": "表单直跳模式",
            "detail": "根路径访问应直接 302 到轮询目标" if item.get("worker_enabled") else "表单直跳必须启用动态路由",
        })
    else:
        checks.append({
            "key": "landing_mode",
            "status": "pass",
            "label": "普通落地页模式",
            "detail": "根路径访问应返回 HTML 页面，按钮点击后再跳转",
        })
    checks.append({
        "key": "worker",
        "status": "pass" if item.get("worker_enabled") else "warn",
        "label": "动态配置 / 统计防护",
        "detail": (
            f"动态配置已启用；统计 {'开' if item.get('tracking_enabled') else '关'}，防护 {'开' if item.get('protection_enabled') else '关'}"
            if item.get("worker_enabled")
            else "未启用动态配置，无法做统计、防护或服务端直跳"
        ),
    })

    http_info: dict[str, Any] = {}
    if public_url:
        try:
            resp = requests.get(
                public_url,
                allow_redirects=False,
                timeout=12,
                headers={
                    "User-Agent": "EdgeHealthCheck/1.0",
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                },
            )
            location = resp.headers.get("location", "")
            content_type = resp.headers.get("content-type", "")
            block_reason = resp.headers.get("x-edge-block-reason", "")
            http_info = {
                "status_code": resp.status_code,
                "location": location,
                "content_type": content_type,
                "final_url": public_url,
                "block_reason": block_reason,
            }
            if link_kind == "form":
                if block_reason and item.get("protection_enabled"):
                    checks.append({
                        "key": "runtime_redirect",
                        "status": "warn",
                        "label": "线上直跳",
                        "detail": "请求被防护规则拦截，无法在自检中确认跳转；请用符合规则的访问环境复测",
                    })
                elif resp.status_code in {301, 302, 303, 307, 308} and location:
                    checks.append({
                        "key": "runtime_redirect",
                        "status": "pass" if _target_location_matches(location, targets) else "warn",
                        "label": "线上直跳",
                        "detail": (
                            f"访问根路径已 {resp.status_code} 跳转到目标链接"
                            if _target_location_matches(location, targets)
                            else f"访问根路径已跳转，但 Location 未命中当前目标列表：{location[:180]}"
                        ),
                    })
                elif resp.status_code == 403 and item.get("protection_enabled"):
                    checks.append({
                        "key": "runtime_redirect",
                        "status": "warn",
                        "label": "线上直跳",
                        "detail": "请求被防护规则拦截，无法在自检中确认跳转；请用符合规则的访问环境复测",
                    })
                else:
                    checks.append({
                        "key": "runtime_redirect",
                        "status": "fail",
                        "label": "线上直跳",
                        "detail": f"表单直跳模式期望 302，实际 HTTP {resp.status_code}",
                    })
            else:
                if block_reason and item.get("protection_enabled"):
                    checks.append({
                        "key": "runtime_page",
                        "status": "warn",
                        "label": "线上页面",
                        "detail": "请求被防护规则拦截；页面可能正常，但当前自检环境不在允许范围内",
                    })
                elif resp.status_code == 200 and "html" in content_type.lower():
                    checks.append({
                        "key": "runtime_page",
                        "status": "pass",
                        "label": "线上页面",
                        "detail": "公开链接返回 HTML，普通落地页可访问",
                    })
                elif resp.status_code == 403 and item.get("protection_enabled"):
                    checks.append({
                        "key": "runtime_page",
                        "status": "warn",
                        "label": "线上页面",
                        "detail": "请求被防护规则拦截；页面可能正常，但当前自检环境不在允许范围内",
                    })
                else:
                    checks.append({
                        "key": "runtime_page",
                        "status": "fail" if resp.status_code >= 400 else "warn",
                        "label": "线上页面",
                        "detail": f"公开链接返回 HTTP {resp.status_code}，Content-Type: {content_type or '--'}",
                    })
        except Exception as exc:
            checks.append({
                "key": "runtime_http",
                "status": "fail",
                "label": "线上访问",
                "detail": f"请求公开链接失败：{exc}",
            })

    if public_url and item.get("worker_enabled"):
        worker_probe_url = public_url.rstrip("/") + "/__edge/redirect"
        try:
            worker_resp = requests.get(
                worker_probe_url,
                allow_redirects=False,
                timeout=12,
                headers={
                    "User-Agent": "EdgeWorkerProbe/1.0",
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                },
            )
            if worker_resp.status_code in {301, 302, 303, 307, 308}:
                checks.append({
                    "key": "runtime_worker_route",
                    "status": "pass",
                    "label": "Edge route",
                    "detail": "Edge route returned redirect",
                })
            elif worker_resp.status_code == 403 and item.get("protection_enabled"):
                checks.append({
                    "key": "runtime_worker_route",
                    "status": "warn",
                    "label": "Edge route",
                    "detail": "Edge route returned protection block",
                })
            else:
                ct = worker_resp.headers.get("content-type", "")
                checks.append({
                    "key": "runtime_worker_route",
                    "status": "fail",
                    "label": "Edge route",
                    "detail": f"Edge route not active: /__edge/redirect returned HTTP {worker_resp.status_code}, Content-Type: {ct or '--'}. Republish this page once.",
                })
        except Exception as exc:
            checks.append({
                "key": "runtime_worker_route",
                "status": "fail",
                "label": "Edge route",
                "detail": f"Edge route probe failed: {exc}",
            })

    if public_url:
        checks.append(_facebook_url_probe_check(public_url, fb_probe_token, fb_probe_source))

    checks = _stable_landing_health_checks(checks, item, targets, link_kind, http_info)
    health_status = _health_status_from_checks(checks)
    health_summary = _health_summary_from_checks(checks)
    health_checked_at = _now_cst()
    health_http_code = http_info.get("status_code") if http_info else None
    conn = None
    try:
        conn = get_conn()
        if raw_response_update is not None:
            conn.execute(
                """UPDATE landing_pages
                   SET last_health_status=?,
                       last_health_summary=?,
                       last_health_checked_at=?,
                       last_health_http_code=?,
                       raw_response=?,
                       updated_at=datetime('now','+8 hours')
                   WHERE id=?""",
                (
                    health_status,
                    health_summary,
                    health_checked_at,
                    health_http_code,
                    json.dumps(raw_response_update, ensure_ascii=False),
                    page_id,
                ),
            )
        else:
            conn.execute(
                """UPDATE landing_pages
                   SET last_health_status=?,
                       last_health_summary=?,
                       last_health_checked_at=?,
                       last_health_http_code=?,
                       updated_at=datetime('now','+8 hours')
                   WHERE id=?""",
                (health_status, health_summary, health_checked_at, health_http_code, page_id),
            )
        conn.commit()
    except Exception:
        logger.exception("landing page health persistence failed: page_id=%s", page_id)
    finally:
        if conn:
            conn.close()
    item["last_health_status"] = health_status
    item["last_health_summary"] = health_summary
    item["last_health_checked_at"] = health_checked_at
    item["last_health_http_code"] = health_http_code

    return {
        "success": health_status != "fail",
        "page": item,
        "checks": checks,
        "http": http_info,
        "checked_at": health_checked_at,
    }


@router.post("/facebook/link-probe")
def facebook_link_probe(body: LandingFacebookProbeReq, user=Depends(get_current_user)):
    url = str(body.url or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="请提交完整的 http(s) 链接")
    conn = get_conn()
    try:
        page_stub = {
            "id": None,
            "bound_act_ids": json.dumps(_clean_act_ids([body.act_id or ""])) if body.act_id else "[]",
        }
        token, source = _landing_facebook_probe_token(conn, page_stub, user, body.act_id or "")
        check = _facebook_url_probe_check(url, token, source)
        return {
            "success": check.get("status") == "pass",
            "url": url,
            "check": check,
            "checked_at": _now_cst(),
        }
    finally:
        conn.close()


@router.get("/pages/{page_id}/stats")
def landing_page_stats(page_id: int, days: int = 7, user=Depends(get_current_user)):
    days = max(1, min(int(days or 7), LANDING_TRACKING_RETENTION_DAYS))
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
    by_reason = [
        dict(r)
        for r in conn.execute(
            """SELECT COALESCE(NULLIF(reason,''),'--') AS reason, COUNT(*) AS cnt
               FROM landing_events
               WHERE page_id=? AND created_at>=? AND event_type='block'
               GROUP BY reason
               ORDER BY cnt DESC LIMIT 20""",
            params,
        ).fetchall()
    ]
    raw_events = [
        dict(r)
        for r in conn.execute(
            """SELECT event_type, decision, reason, path, target_url, referrer,
                      country, region, city, colo, asn, platform, device_type,
                      browser, os, metadata, created_at
               FROM landing_events WHERE page_id=? AND created_at>=?
               ORDER BY id DESC LIMIT 5000""",
            params,
        ).fetchall()
    ]
    source_counter: dict[str, int] = {}
    recent = []
    for idx, event in enumerate(raw_events):
        metadata = _json_loads(event.get("metadata"), {})
        if not isinstance(metadata, dict):
            metadata = {}
        source_platform = str(metadata.get("source_platform") or "").strip()
        if not source_platform:
            source_platform = str(event.get("platform") or "").strip()
        source_platform = source_platform or "Unknown"
        source_counter[source_platform] = source_counter.get(source_platform, 0) + 1
        if idx < 80:
            event.pop("metadata", None)
            event["source_platform"] = source_platform
            event["ad_slug"] = str(metadata.get("ad_slug") or "").strip()
            event["ad_id"] = _normalize_ad_id(metadata.get("ad_id") or metadata.get("ad") or metadata.get("aid") or "")
            recent.append(event)
    by_source = [
        {"source": key, "cnt": value}
        for key, value in sorted(source_counter.items(), key=lambda item: item[1], reverse=True)[:20]
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
        "by_reason": by_reason,
        "by_source": by_source,
        "recent": recent,
    }
