"""
账户预热引擎 v1.0.0
自动为新账户创建 page_likes 广告 (OUTCOME_ENGAGEMENT + PAGE_LIKES)
"""
import logging
import mimetypes
import os
import requests
import json
import time
import threading
import urllib.parse
import re
from datetime import datetime, timezone, timedelta
from typing import Tuple, Optional
from core.auth import is_superadmin
from core.database import get_conn, decrypt_token
from core.tenancy import is_operator_user, team_id_for_create, user_id
from services.guard_engine import _local_per_usd_rate
from services.token_manager import get_exec_token, ACTION_CREATE, ACTION_READ, ACTION_UPDATE
from services.notifier import notify_account, notify_global, notify_team

logger = logging.getLogger("mira.warmup")

FB_API_BASE = "https://graph.facebook.com/v25.0"

_DEAD_STATUSES = {"DELETED", "ARCHIVED", "PAUSED", "CAMPAIGN_PAUSED",
                  "ADSET_PAUSED", "WITH_ISSUES"}
_NO_DECIMAL_CURRENCIES = {"JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS"}
_WARMUP_TARGET_USD = 5.0
_WARMUP_RECENT_SPEND_DAYS = 3
_warmup_lock = threading.Lock()
_ACCESS_TOKEN_PARAM_RE = re.compile(r"(access_token=)[^&\s]+")
_FB_TOKEN_VALUE_RE = re.compile(r"\bEA[A-Za-z0-9_\-]{20,}\b")


def _tw_page_good_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"COALESCE({prefix}page_id, '') != '' "
        f"AND COALESCE({prefix}page_is_published, 0)=1 "
        f"AND COALESCE({prefix}page_can_advertise, 0)=1 "
        f"AND COALESCE({prefix}page_status, '')='ok'"
    )


def _tw_page_bad_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"COALESCE({prefix}page_is_published, 1)=0 "
        f"OR COALESCE({prefix}page_can_advertise, 1)=0 "
        f"OR COALESCE({prefix}page_status, 'ok') IN ('restricted', 'unpublished')"
    )


def _sanitize_error_text(value) -> str:
    text = "" if value is None else str(value)
    text = _ACCESS_TOKEN_PARAM_RE.sub(r"\1***", text)
    return _FB_TOKEN_VALUE_RE.sub("EA***", text)


def _format_fb_response_error(resp: requests.Response) -> str:
    try:
        result = resp.json()
    except ValueError:
        return f"FB API HTTP {resp.status_code}: {_sanitize_error_text(resp.text[:300])}"
    if isinstance(result, dict) and isinstance(result.get("error"), dict):
        err = result["error"]
        code = err.get("code", resp.status_code)
        subcode = err.get("error_subcode")
        message = _sanitize_error_text(err.get("message", result))
        suffix = f", subcode={subcode}" if subcode is not None else ""
        return f"FB API error(code={code}{suffix}): {message}"
    return f"FB API HTTP {resp.status_code}: {_sanitize_error_text(result)}"


def _json_or_fb_error(resp: requests.Response) -> dict:
    if resp.status_code >= 400:
        raise RuntimeError(_format_fb_response_error(resp))
    result = resp.json()
    if isinstance(result, dict) and isinstance(result.get("error"), dict):
        raise RuntimeError(_format_fb_response_error(resp))
    return result


# ── 数据库自愈 ───────────────────────────────────────────────────
def _ensure_schema():
    conn = get_conn()
    cur = conn.execute("PRAGMA table_info(accounts)")
    cols = {r["name"] for r in cur.fetchall()}
    needed = {
        "warmup_state": "TEXT DEFAULT NULL",
        "warmup_triggered_at": "TEXT DEFAULT NULL",
        "warmup_campaign_id": "TEXT DEFAULT NULL",
        "warmup_last_spend": "REAL DEFAULT NULL",
        "warmup_last_checked_at": "TEXT DEFAULT NULL",
    }
    for col, defn in needed.items():
        if col not in cols:
            try:
                conn.execute(f"ALTER TABLE accounts ADD COLUMN {col} {defn}")
            except Exception:
                pass
    has_tw_pages = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tw_certified_pages'"
    ).fetchone()
    if has_tw_pages:
        page_cols = {r["name"] for r in conn.execute("PRAGMA table_info(tw_certified_pages)").fetchall()}
        page_needed = {
            "page_is_published": "INTEGER DEFAULT NULL",
            "page_can_advertise": "INTEGER DEFAULT NULL",
            "page_status": "TEXT DEFAULT NULL",
            "page_status_hint": "TEXT DEFAULT NULL",
        }
        for col, defn in page_needed.items():
            if col not in page_cols:
                try:
                    conn.execute(f"ALTER TABLE tw_certified_pages ADD COLUMN {col} {defn}")
                except Exception:
                    pass
    conn.execute(
        "INSERT OR IGNORE INTO settings (key, value, category) VALUES ('warmup_enabled', '0', 'warmup')")
    conn.execute(
        "INSERT OR IGNORE INTO settings (key, value, category) VALUES ('warmup_check_interval', '30', 'warmup')")
    team_cols = {r["name"] for r in conn.execute("PRAGMA table_info(teams)").fetchall()}
    for key in ("sentinel_enabled", "mirror_enabled", "heartbeat_enabled", "warmup_enabled"):
        if key not in team_cols:
            try:
                conn.execute(f"ALTER TABLE teams ADD COLUMN {key} INTEGER DEFAULT 0")
            except Exception:
                pass
    user_cols = {r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
    for key in ("sentinel_enabled", "mirror_enabled", "heartbeat_enabled", "warmup_enabled"):
        if key not in user_cols:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {key} INTEGER DEFAULT 0")
            except Exception:
                pass
    conn.commit()
    conn.close()


# ── 工具函数 ─────────────────────────────────────────────────────
def _get_setting(key: str, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def _money_factor(currency: str) -> int:
    return 1 if (currency or "USD").upper() in _NO_DECIMAL_CURRENCIES else 100


def _to_minor_units(value, currency: str) -> int:
    return int(round(float(value) * _money_factor(currency)))


def _from_minor_units(value, currency: str) -> float:
    return float(value or 0) / _money_factor(currency)


def _cst_now() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


# ── FB API 封装 ──────────────────────────────────────────────────
def _fb_post_result(path: str, token: str, data: dict) -> Tuple[bool, dict]:
    """FB 写操作，返回 (成功, 响应dict或错误dict含error字段)"""
    payload = dict(data or {})
    payload["access_token"] = token
    try:
        resp = requests.post(f"{FB_API_BASE}/{path}", json=payload, timeout=30)
        result = resp.json()
        if resp.status_code == 200 and result.get("success") is not False:
            return True, result
        err = result.get("error", {})
        return False, {
            "error": True,
            "code": err.get("code", 0),
            "subcode": err.get("error_subcode"),
            "message": err.get("message", str(result)),
            "user_title": err.get("error_user_title"),
            "user_msg": err.get("error_user_msg"),
            "fbtrace_id": err.get("fbtrace_id"),
        }
    except requests.exceptions.RequestException as e:
        return False, {"error": True, "message": f"网络错误: {e}"}


def _fb_get(path: str, token: str, params: dict = None) -> dict:
    """FB GET 请求"""
    p = dict(params or {})
    p["access_token"] = token
    url = f"{FB_API_BASE}/{path}?{urllib.parse.urlencode(p)}"
    try:
        resp = requests.get(url, timeout=20)
        return _json_or_fb_error(resp)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(_sanitize_error_text(f"Network error: {e}")) from e


def _fb_delete(path: str, token: str) -> bool:
    """删除 FB 对象，失败不抛异常"""
    try:
        resp = requests.delete(
            f"{FB_API_BASE}/{path}",
            params={"access_token": token},
            timeout=15)
        return resp.status_code == 200
    except Exception:
        return False


def _upload_image_to_fb(act_id: str, token: str, file_url: str) -> Tuple[bool, str]:
    """上传图片到 FB 广告账户，返回 (成功, image_hash或错误消息)"""
    try:
        # file_url 可能是相对路径，需要补全为文件系统路径
        if file_url.startswith("/uploads/"):
            file_path = f"/opt/mira/frontend{file_url}"
        elif file_url.startswith("/opt/"):
            file_path = file_url
        else:
            file_path = f"/opt/mira/frontend{file_url}" if file_url.startswith("/") else f"/opt/mira/frontend/{file_url}"

        filename = os.path.basename(file_path)
        mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        if not mime_type.startswith("image/"):
            return False, f"素材不是图片文件: {filename}"

        with open(file_path, "rb") as f:
            resp = requests.post(
                f"{FB_API_BASE}/{act_id}/adimages",
                files={"filename": (filename, f, mime_type)},
                data={"access_token": token},
                timeout=60)
        try:
            result = resp.json()
        except ValueError:
            return False, _format_fb_response_error(resp)
        if resp.status_code == 200:
            images = result.get("images", {})
            # FB 返回 {"images": {"filename": {"hash": "xxx"}}} 或类似结构
            for key, val in images.items():
                if isinstance(val, dict) and val.get("hash"):
                    return True, val["hash"]
            return False, f"未获取到 image_hash: {result}"
        return False, _format_fb_response_error(resp)
    except FileNotFoundError:
        return False, f"素材文件不存在: {file_url}"
    except Exception as e:
        return False, f"上传异常: {e}"


def _get_read_token(act_id: str) -> Optional[str]:
    """获取该账户可用的只读 token"""
    try:
        token = get_exec_token(act_id, ACTION_READ, notify_exhausted=False)
        if token:
            return token
    except Exception:
        pass
    # 兜底：从账户主 Token 取一个可读 Token，避免预热状态检查被单个操作号波动卡住。
    try:
        conn = get_conn()
        row = conn.execute("""
            SELECT t.access_token_enc FROM fb_tokens t
            JOIN accounts a ON a.token_id = t.id
            WHERE a.act_id = ?
        """, (act_id,)).fetchone()
        conn.close()
        if row and row[0]:
            return decrypt_token(row[0])
    except Exception:
        pass
    return None


def _warmup_campaign_alive(act_id: str, campaign_id: str) -> Tuple[bool, str]:
    """查 FB campaign 是否还在投放中。返回 (alive, effective_status)"""
    if not campaign_id:
        return False, "no_campaign_id"
    token = _get_read_token(act_id)
    if not token:
        logger.warning(f"warmup: 无法获取 {act_id} 的 read token，假定 campaign 存活")
        return True, "unknown"
    try:
        data = _fb_get(campaign_id, token, {"fields": "effective_status,status"})
        status = data.get("effective_status", data.get("status", ""))
        alive = status not in _DEAD_STATUSES
        return alive, status
    except Exception as e:
        logger.warning(f"warmup: 查询 campaign {campaign_id} 失败: {e}")
        return True, "error"


def _recent_spend_date_range(days: int = _WARMUP_RECENT_SPEND_DAYS) -> dict:
    today = (datetime.now(timezone.utc) + timedelta(hours=8)).date()
    # Insights only supports date granularity here. Include one extra boundary
    # date so "last 3 days" behaves closer to a rolling 72-hour guard.
    since = today - timedelta(days=max(1, days))
    return {"since": since.isoformat(), "until": today.isoformat()}


def _get_recent_account_spend(act_id: str, days: int = _WARMUP_RECENT_SPEND_DAYS) -> Tuple[Optional[float], str]:
    token = _get_read_token(act_id)
    if not token:
        return None, "无可读 Token，无法确认最近消耗"
    try:
        data = _fb_get(
            f"{act_id}/insights",
            token,
            {
                "fields": "spend",
                "level": "account",
                "time_range": json.dumps(_recent_spend_date_range(days)),
            },
        )
        total = 0.0
        for item in data.get("data", []) or []:
            try:
                total += float(item.get("spend") or 0)
            except Exception:
                pass
        return total, ""
    except Exception as exc:
        return None, f"最近{days}天消耗查询失败：{exc}"


def _mark_recent_spend_skip(account: dict, recent_spend: float, days: int = _WARMUP_RECENT_SPEND_DAYS) -> None:
    act_id = account.get("act_id")
    if not act_id:
        return
    now_cst = _cst_now()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE accounts
            SET warmup_state='recent_spend',
                warmup_campaign_id=NULL,
                warmup_triggered_at=COALESCE(warmup_triggered_at, ?),
                warmup_last_spend=COALESCE(amount_spent, 0),
                warmup_last_checked_at=?
            WHERE act_id=?
            """,
            (now_cst, now_cst, act_id),
        )
        conn.commit()
    finally:
        conn.close()
    _log_action(
        act_id,
        "warmup_skip_recent_spend",
        f"recent_{days}d_spend={recent_spend:.2f}",
        account.get("name") or act_id,
    )


def _recent_spend_hold_active(account: dict, days: int = _WARMUP_RECENT_SPEND_DAYS) -> bool:
    if (account.get("warmup_state") or "") != "recent_spend":
        return False
    checked_at = account.get("warmup_last_checked_at")
    if not checked_at:
        return False
    try:
        checked = datetime.strptime(str(checked_at)[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return False
    now = datetime.strptime(_cst_now(), "%Y-%m-%d %H:%M:%S")
    return now < checked + timedelta(days=max(1, days))


def _recent_spend_hold_message(account: dict, days: int = _WARMUP_RECENT_SPEND_DAYS) -> str:
    checked_at = str(account.get("warmup_last_checked_at") or "")[:19]
    return f"{account.get('act_id')}: 最近{days}天已有消耗，保护期内不重复预热" + (f"（上次确认：{checked_at}）" if checked_at else "")


def _usd_to_minor_units(usd_amount: float, currency: str) -> int:
    """把 USD 金额换成目标账户币种的 FB minor units。"""
    cur = (currency or "USD").upper()
    if cur == "USD":
        return _to_minor_units(usd_amount, cur)
    # 查实时汇率
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT rate FROM currency_rates WHERE currency=?", (cur,)
        ).fetchone()
        conn.close()
        if row and row["rate"]:
            usd_rate = float(row["rate"])
            if usd_rate > 0:
                amount_in_currency = usd_amount * usd_rate
                return _to_minor_units(amount_in_currency, cur)
    except Exception:
        pass
    # 兜底：从静态汇率估算
    _FX = {"JPY": 0.0067, "KRW": 0.00072, "IDR": 0.000063, "VND": 0.000040,
           "EUR": 1.08, "GBP": 1.27, "CNY": 0.138, "HKD": 0.128,
           "TWD": 0.031, "SGD": 0.74, "AUD": 0.65, "CAD": 0.74}
    rate = _FX.get(cur, 1.0)
    amount = usd_amount / rate if rate > 0 else usd_amount
    return _to_minor_units(amount, cur)


def _usd_to_minor_units(usd_amount: float, currency: str) -> int:
    cur = (currency or "USD").upper()
    amount_in_currency = float(usd_amount) * _local_per_usd_rate(cur)
    return _to_minor_units(amount_in_currency, cur)


def _usd5_to_minor_units(currency: str) -> int:
    return _usd_to_minor_units(_WARMUP_TARGET_USD, currency)


def _parse_countries(value) -> list:
    if not value:
        return []
    try:
        raw = json.loads(value) if isinstance(value, str) and value.strip().startswith("[") else value
    except Exception:
        raw = value
    if isinstance(raw, str):
        raw = raw.replace(";", ",").replace("|", ",").split(",")
    if not isinstance(raw, list):
        return []
    countries = []
    for item in raw:
        code = str(item or "").strip().upper()
        if len(code) == 2 and code.isalpha() and code not in countries:
            countries.append(code)
    return countries


def _pick_warmup_country(account: dict, asset: dict) -> str:
    countries = _parse_countries(asset.get("target_countries")) or _parse_countries(account.get("target_countries"))
    return countries[0] if countries else "US"


def _warmup_page_block_reason(page_id: str) -> str:
    page_id = str(page_id or "").strip()
    if not page_id:
        return ""
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT page_id, page_name, page_is_published, page_can_advertise,
                   page_status, page_status_hint
            FROM tw_certified_pages
            WHERE page_id=?
            ORDER BY CASE WHEN page_status='ok' THEN 0 ELSE 1 END, id ASC
            LIMIT 1
        """, (page_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return ""
    reasons = []
    if row["page_is_published"] == 0:
        reasons.append("主页未发布")
    if row["page_can_advertise"] == 0:
        reasons.append("不可投放")
    status = str(row["page_status"] or "").strip().lower()
    if status in {"restricted", "unpublished"}:
        hint = str(row["page_status_hint"] or "").strip()
        reasons.append(hint or f"状态={status}")
    if not reasons:
        return ""
    page_name = row["page_name"] or page_id
    return f"{page_name}({page_id}): " + " / ".join(dict.fromkeys(reasons))


def _sync_pages_for_token(token_id: int, conn) -> int:
    """从 FB 拉取操作号下的主页，写入 tw_certified_pages。返回新增可用页数。"""
    row = conn.execute(
        "SELECT access_token_enc, token_type FROM fb_tokens WHERE id=?", (token_id,)
    ).fetchone()
    if not row or not row[0]:
        return 0
    if row["token_type"] not in ("operate", "create"):
        return 0

    try:
        token = decrypt_token(row[0])
    except Exception:
        logger.warning(f"warmup: 无法解密 token_id={token_id} 的 token")
        return 0

    pages = []
    try:
        url = f"{FB_API_BASE}/me/accounts"
        params = {
            "access_token": token,
            "fields": "id,name,category,tasks,is_published",
            "limit": 200,
        }
        seen_next = set()
        for _ in range(20):
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
            if "error" in data:
                break
            for p in data.get("data", []) or []:
                pid = p.get("id")
                if not pid:
                    continue
                tasks = p.get("tasks") or []
                published = p.get("is_published", True)
                can_adv = 1 if (published is not False and "ADVERTISE" in tasks) else 0
                page_status = "ok" if can_adv else ("unpublished" if published is False else "restricted")
                hint = "" if can_adv else ("主页未发布" if published is False else "缺少 ADVERTISE 权限")
                pages.append({
                    "page_id": str(pid),
                    "page_name": p.get("name", ""),
                    "page_category": p.get("category", ""),
                    "page_is_published": 1 if published is not False else 0,
                    "page_tasks": json.dumps(tasks),
                    "page_can_advertise": can_adv,
                    "page_status": page_status,
                    "page_status_hint": hint,
                })
            next_url = data.get("paging", {}).get("next")
            if not next_url or next_url in seen_next:
                break
            seen_next.add(next_url)
            url = next_url
            params = {}
    except Exception as e:
        logger.warning(f"warmup: 同步 token_id={token_id} 的主页失败: {e}")
        return 0

    if not pages:
        return 0

    now_cst = _cst_now()
    # 取 token 的 matrix_id 以正确分配主页归属
    matrix_row = conn.execute(
        "SELECT matrix_id FROM fb_tokens WHERE id=?", (token_id,)
    ).fetchone()
    token_matrix_id = matrix_row["matrix_id"] if matrix_row else None

    good = 0
    for p in pages:
        existing = conn.execute(
            "SELECT id FROM tw_certified_pages WHERE page_id=? AND token_id=?",
            (p["page_id"], token_id)
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE tw_certified_pages
                SET page_name=?, page_category=?, page_is_published=?,
                    page_tasks=?, page_can_advertise=?, page_status=?,
                    page_status_hint=?, page_status_checked_at=?,
                    matrix_id=COALESCE(matrix_id, ?)
                WHERE id=?
            """, (p["page_name"], p["page_category"], p["page_is_published"],
                  p["page_tasks"], p["page_can_advertise"], p["page_status"],
                  p["page_status_hint"], now_cst, token_matrix_id, existing["id"]))
        else:
            conn.execute("""
                INSERT INTO tw_certified_pages
                (page_id, page_name, token_id, page_category, page_is_published,
                 page_tasks, page_can_advertise, page_status, page_status_hint,
                 page_status_checked_at, verified_source, matrix_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'fb_sync', ?)
            """, (p["page_id"], p["page_name"], token_id, p["page_category"],
                  p["page_is_published"], p["page_tasks"], p["page_can_advertise"],
                  p["page_status"], p["page_status_hint"], now_cst, token_matrix_id))
        if p["page_can_advertise"]:
            good += 1
    conn.commit()

    logger.info(f"warmup: token_id={token_id} 主页同步完成: {len(pages)} 页, {good} 可用")
    return good


def _resolve_warmup_page_id(account: dict) -> Optional[str]:
    page_id = (account.get("page_id") or "").strip()
    if page_id:
        return page_id

    act_id = account.get("act_id")
    token_ids = []
    if account.get("token_id"):
        token_ids.append(account.get("token_id"))

    conn = get_conn()
    try:
        if act_id:
            rows = conn.execute("""
                SELECT token_id FROM account_op_tokens
                WHERE act_id=? AND status='active' AND token_id IS NOT NULL
                ORDER BY CASE token_type WHEN 'operate' THEN 0 WHEN 'create' THEN 1 ELSE 2 END, priority ASC
            """, (act_id,)).fetchall()
            for row in rows:
                if row["token_id"] not in token_ids:
                    token_ids.append(row["token_id"])

        pages = []
        good_page_sql = _tw_page_good_sql()
        for token_id in token_ids:
            pages = conn.execute(f"""
                SELECT page_id FROM tw_certified_pages
                WHERE token_id=? AND {good_page_sql}
                ORDER BY id ASC
            """, (token_id,)).fetchall()
            if pages:
                break
            # 找不到页 → 自动从 FB 拉取该操作号的主页再试
            if _sync_pages_for_token(token_id, conn) > 0:
                pages = conn.execute(f"""
                    SELECT page_id FROM tw_certified_pages
                    WHERE token_id=? AND {good_page_sql}
                    ORDER BY id ASC
                """, (token_id,)).fetchall()
                if pages:
                    break
    finally:
        conn.close()

    if not pages:
        return None
    idx_seed = sum(ord(ch) for ch in str(act_id or account.get("id") or ""))
    return pages[idx_seed % len(pages)]["page_id"]


def _count_sql(conn, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0] if row else 0)


def _warmup_scan_diagnostics() -> dict:
    conn = get_conn()
    try:
        good_page_sql = _tw_page_good_sql("p")
        bad_page_sql = _tw_page_bad_sql("p")
        direct_page_clause = (
            "COALESCE(page_id, '') != '' "
            "AND NOT EXISTS (SELECT 1 FROM tw_certified_pages p "
            "WHERE p.page_id=accounts.page_id AND (" + bad_page_sql + "))"
        )
        base = "FROM accounts WHERE enabled=1 AND COALESCE(account_status, 1) NOT IN (3, 7, 9, 100, 101)"
        with_direct_page = base + " AND " + direct_page_clause
        fallback_pages = _count_sql(conn, "SELECT COUNT(*) FROM tw_certified_pages p WHERE " + good_page_sql)
        page_clause = f""" AND (
            {direct_page_clause}
            OR EXISTS (
                SELECT 1 FROM tw_certified_pages p
                WHERE p.token_id=accounts.token_id AND {good_page_sql}
            )
            OR EXISTS (
                SELECT 1 FROM account_op_tokens aot
                JOIN tw_certified_pages p ON p.token_id=aot.token_id
                WHERE aot.act_id=accounts.act_id
                  AND aot.status='active'
                  AND {good_page_sql}
            )
        )"""
        with_page = base + page_clause
        mirror_off = with_page + " AND COALESCE(mirror_enabled, 0)=0"
        create_token_clause = """ AND EXISTS (
            SELECT 1 FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id=aot.token_id
            WHERE aot.act_id=accounts.act_id
              AND aot.status='active'
              AND t.status='active'
              AND t.token_type='operate'
              AND COALESCE(t.token_source, 'system_user') IN ('system_user','oauth_user')
        )"""
        with_create_token = mirror_off + create_token_clause
        new_ready = with_create_token + " AND COALESCE(warmup_state, '')='' AND COALESCE(warmup_campaign_id, '')=''"
        spent_observed = new_ready + " AND CAST(COALESCE(amount_spent, 0) AS REAL)>0"
        dormant_ready = with_create_token + " AND warmup_state='dormant'"
        recent_spend_ready = with_create_token + " AND warmup_state='recent_spend' AND (warmup_last_checked_at IS NULL OR warmup_last_checked_at <= datetime('now','+8 hours','-" + str(_WARMUP_RECENT_SPEND_DAYS) + " days'))"
        recent_spend_hold = with_create_token + " AND warmup_state='recent_spend' AND warmup_last_checked_at > datetime('now','+8 hours','-" + str(_WARMUP_RECENT_SPEND_DAYS) + " days')"
        warming_accounts = with_create_token + " AND warmup_state='warming'"
        completed_accounts = with_create_token + " AND warmup_state='completed'"
        failed_accounts = mirror_off + " AND warmup_state='failed'"
        image_asset_sql = """
            SELECT COUNT(*) FROM ad_assets
            WHERE COALESCE(file_type, 'image') = 'image'
              AND COALESCE(upload_status, '') IN ('local_saved', 'ai_done', 'ready')
              AND UPPER(COALESCE(NULLIF(TRIM(display_name), ''), file_name, '')) LIKE 'YE%'
        """
        stats = {
            "active_accounts": _count_sql(conn, "SELECT COUNT(*) " + base),
            "with_direct_page": _count_sql(conn, "SELECT COUNT(*) " + with_direct_page),
            "fallback_pages": fallback_pages,
            "with_page": _count_sql(conn, "SELECT COUNT(*) " + with_page),
            "mirror_off": _count_sql(conn, "SELECT COUNT(*) " + mirror_off),
            "with_create_token": _count_sql(conn, "SELECT COUNT(*) " + with_create_token),
            "new_ready": _count_sql(conn, "SELECT COUNT(*) " + new_ready),
            "spent_observed": _count_sql(conn, "SELECT COUNT(*) " + spent_observed),
            "dormant_ready": _count_sql(conn, "SELECT COUNT(*) " + dormant_ready),
            "recent_spend_ready": _count_sql(conn, "SELECT COUNT(*) " + recent_spend_ready),
            "recent_spend_hold": _count_sql(conn, "SELECT COUNT(*) " + recent_spend_hold),
            "warming_accounts": _count_sql(conn, "SELECT COUNT(*) " + warming_accounts),
            "completed_accounts": _count_sql(conn, "SELECT COUNT(*) " + completed_accounts),
            "failed_accounts": _count_sql(conn, "SELECT COUNT(*) " + failed_accounts),
            "image_assets": _count_sql(conn, image_asset_sql),
        }
        stats["candidate_accounts"] = stats["new_ready"] + stats["dormant_ready"] + stats["recent_spend_ready"]
        stats["not_candidate_accounts"] = max(0, stats["with_create_token"] - stats["candidate_accounts"])
        if stats["active_accounts"] == 0:
            stats["reason"] = "没有启用且状态正常的账户"
        elif stats["with_page"] == 0:
            stats["reason"] = "账户没有配置主页，认证主页库也没有可兜底主页"
        elif stats["mirror_off"] == 0:
            stats["reason"] = "符合状态的账户都开启了镜像保护"
        elif stats["with_create_token"] == 0:
            stats["reason"] = "符合条件的账户缺少可用操作号，预热创建广告不能使用管理号"
        elif stats["candidate_accounts"] == 0 and stats.get("failed_accounts"):
            stats["reason"] = "有预热失败账户，需要手动重新预热或查看日志"
        elif stats["candidate_accounts"] == 0:
            stats["reason"] = "没有新账户或沉睡账户需要预热"
        elif stats["image_assets"] == 0:
            stats["reason"] = "没有 YE 开头的可用图片素材"
        else:
            stats["reason"] = ""
        return stats
    finally:
        conn.close()


def _pause_campaign(campaign_id: str, token: str) -> bool:
    if not campaign_id or not token:
        return False
    ok, result = _fb_post_result(campaign_id, token, {"status": "PAUSED"})
    if not ok:
        logger.warning(f"warmup: 暂停预热 campaign {campaign_id} 失败: {result}")
    return ok


def _log_action(act_id, action_type, detail, account_name="", status="success", level="info"):
    """写入 action_logs 表"""
    try:
        conn = get_conn()
        conn.execute("""
            INSERT INTO action_logs
            (act_id, level, target_id, target_name, action_type,
             trigger_type, trigger_detail, status, operator)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (act_id, level, act_id, account_name, action_type,
              "warmup", detail, status, "system"))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _send_tg(msg: str, act_id: str | None = None, team_id: int | None = None):
    """发送 TG 通知，按账户/团队路由。"""
    if act_id:
        return notify_account(act_id, msg, event_type="warmup")
    if team_id is not None:
        return notify_team(team_id, msg, event_type="warmup")
    return notify_global(msg, dedup_key="warmup")


def _account_label(account: dict) -> str:
    name = (account.get("name") or "").strip()
    act_id = account.get("act_id") or ""
    if name and name != act_id:
        return f"{name} (<code>{act_id}</code>)"
    return f"<code>{act_id}</code>"


def _strip_account_prefix(detail: str, act_id: str) -> str:
    prefix = f"{act_id}:"
    if detail.startswith(prefix):
        return detail[len(prefix):].strip()
    return detail


def _format_fb_error(result: dict) -> str:
    if not isinstance(result, dict):
        return str(result)
    msg = result.get("message") or str(result)
    code = result.get("code")
    subcode = result.get("subcode")
    user_title = result.get("user_title")
    user_msg = result.get("user_msg")
    parts = []
    if user_title:
        parts.append(str(user_title))
    if user_msg and user_msg not in parts:
        parts.append(str(user_msg))
    if msg and msg not in parts:
        parts.append(str(msg))
    if code:
        parts.append(f"code={code}")
    if subcode:
        parts.append(f"subcode={subcode}")
    return " | ".join(parts) if parts else str(result)


def _cleanup_created_objects(act_id: str, token: str, created: list, reason: str, account_name: str = "") -> list:
    """Best-effort rollback for partially-created warmup objects."""
    failures = []
    cleaned = []
    for obj_type, obj_id in reversed(created):
        if not obj_id:
            continue
        ok = _fb_delete(obj_id, token)
        if not ok and obj_type in ("ad", "adset", "campaign"):
            ok, _ = _fb_post_result(obj_id, token, {"status": "DELETED"})
        if not ok and obj_type in ("ad", "adset", "campaign"):
            ok, _ = _fb_post_result(obj_id, token, {"status": "PAUSED"})
        label = f"{obj_type}:{obj_id}"
        if ok:
            cleaned.append(label)
        else:
            failures.append(label)

    if failures:
        detail = f"reason={reason}; cleanup_failed={','.join(failures)}"
        _log_action(act_id, "warmup_cleanup_failed", detail, account_name, status="failed", level="warning")
        logger.warning(f"warmup: {act_id} cleanup failed after {reason}: {failures}")
    elif cleaned:
        _log_action(act_id, "warmup_cleanup", f"reason={reason}; cleaned={','.join(cleaned)}", account_name)
        logger.info(f"warmup: {act_id} cleaned partial warmup objects after {reason}: {cleaned}")
    return failures


def _clear_warmup_record_for_campaign(act_id: str, campaign_id: str, cleanup_failed: bool) -> None:
    if not campaign_id:
        return
    conn = get_conn()
    try:
        if cleanup_failed:
            conn.execute("""
                UPDATE accounts
                SET warmup_state='failed',
                    warmup_last_spend=COALESCE(amount_spent, 0),
                    warmup_last_checked_at=?
                WHERE act_id=? AND warmup_campaign_id=?
            """, (_cst_now(), act_id, campaign_id))
        else:
            conn.execute("""
                UPDATE accounts
                SET warmup_state=NULL,
                    warmup_triggered_at=NULL,
                    warmup_campaign_id=NULL,
                    warmup_last_spend=NULL,
                    warmup_last_checked_at=NULL
                WHERE act_id=? AND warmup_campaign_id=?
            """, (act_id, campaign_id))
        conn.commit()
    finally:
        conn.close()


def _activate_warmup_objects(token: str, campaign_id: str, adset_id: str, ad_id: str) -> Tuple[bool, str, bool]:
    for obj_type, obj_id in (("ad", ad_id), ("adset", adset_id), ("campaign", campaign_id)):
        ok, result = _fb_post_result(obj_id, token, {"status": "ACTIVE"})
        if not ok:
            return False, f"activate {obj_type} {obj_id} failed: {_format_fb_error(result)}", _fb_error_is_security_hold(result)
    return True, "", False


# ── 核心逻辑 ─────────────────────────────────────────────────────

def _followup_warming():
    """跟进 warming 状态账户，每小时最多查一次"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM accounts
        WHERE warmup_state='warming'
          AND warmup_campaign_id IS NOT NULL AND warmup_campaign_id != ''
          AND (warmup_last_checked_at IS NULL
               OR warmup_last_checked_at <= datetime('now','+8 hours','-60 minutes'))
    """).fetchall()
    conn.close()

    for acc in rows:
        acc = dict(acc)
        act_id = acc["act_id"]
        cid = acc["warmup_campaign_id"]
        alive, effective_status = _warmup_campaign_alive(act_id, cid)
        now_cst = _cst_now()
        currency = acc.get("currency") or "USD"
        current_spend = float(acc.get("amount_spent") or 0)
        warmup_spend = float(acc.get("warmup_last_spend") or 0)
        target_minor = max(1, _usd_to_minor_units(_WARMUP_TARGET_USD, currency))

        if alive:
            if current_spend >= warmup_spend + target_minor:
                token = get_exec_token(act_id, ACTION_UPDATE) or get_exec_token(act_id, ACTION_CREATE)
                if _pause_campaign(cid, token):
                    conn = get_conn()
                    conn.execute("""UPDATE accounts SET warmup_state='completed',
                        warmup_last_spend=?, warmup_last_checked_at=? WHERE id=?""",
                                 (current_spend, now_cst, acc["id"]))
                    conn.commit()
                    conn.close()
                    spent_local = _from_minor_units(current_spend - warmup_spend, currency)
                    _log_action(act_id, "warmup_complete", f"campaign={cid}, spend≈{spent_local:.2f} {currency}", acc.get("name") or act_id)
                    logger.info(f"warmup: {act_id} 已达到 ${_WARMUP_TARGET_USD:g} 预热目标，campaign={cid} 已暂停")
                    continue
                logger.warning(f"warmup: {act_id} 已达到预热目标但暂停失败，等待下次复查")
            conn = get_conn()
            conn.execute("UPDATE accounts SET warmup_last_checked_at=? WHERE id=?",
                         (now_cst, acc["id"]))
            conn.commit()
            conn.close()
            continue

        conn = get_conn()
        if current_spend >= warmup_spend + 1:
            conn.execute("""UPDATE accounts SET warmup_state='completed',
                warmup_last_spend=?, warmup_last_checked_at=? WHERE id=?""",
                         (current_spend, now_cst, acc["id"]))
            logger.info(f"warmup: {act_id} 预热完成 (spend: {warmup_spend} → {current_spend})")
        else:
            conn.execute("""UPDATE accounts SET warmup_state='dormant',
                warmup_last_spend=?, warmup_last_checked_at=? WHERE id=?""",
                         (current_spend, now_cst, acc["id"]))
            logger.info(f"warmup: {act_id} 预热失败 (campaign={cid}, status={effective_status}, 无消耗)")
        conn.commit()
        conn.close()


def _followup_completed():
    """completed 超过7天且 amount_spent 无变化 → 标记 dormant"""
    conn = get_conn()
    conn.execute("""
        UPDATE accounts SET warmup_state='dormant'
        WHERE warmup_state='completed'
          AND warmup_triggered_at <= datetime('now','+8 hours','-7 days')
          AND ABS(COALESCE(amount_spent, 0) - COALESCE(warmup_last_spend, 0)) <= 0.01
    """)
    changed = conn.total_changes
    conn.commit()
    conn.close()
    if changed:
        logger.info(f"warmup: {changed} 个账户转入 dormant (7天沉睡)")


def _manual_scope_clause(user: Optional[dict]) -> tuple[str, list, str]:
    if not user or is_superadmin(user):
        return "", [], "global"
    team_id = team_id_for_create(user)
    if is_operator_user(user):
        return " AND accounts.team_id=? AND accounts.owner_user_id=?", [team_id, user_id(user)], "owner"
    return " AND accounts.team_id=?", [team_id], "team"


def check_and_warmup(user: Optional[dict] = None):
    if not _warmup_lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "warmup already running"}
    try:
        return _check_and_warmup_unlocked(user=user)
    finally:
        _warmup_lock.release()


def _check_and_warmup_unlocked(user: Optional[dict] = None):
    """主入口：扫描并预热符合条件的账户"""
    _ensure_schema()
    manual_scope_sql, manual_scope_params, manual_scope = _manual_scope_clause(user)

    # 1. 全局或团队预热开关
    global_warmup_enabled = _get_setting("warmup_enabled", "0") == "1"
    conn = get_conn()
    if manual_scope == "team":
        team_warmup_enabled = bool(conn.execute(
            "SELECT 1 FROM teams WHERE id=? AND COALESCE(warmup_enabled, 0)=1 LIMIT 1",
            manual_scope_params[:1],
        ).fetchone())
        owner_warmup_enabled = bool(conn.execute(
            "SELECT 1 FROM users WHERE team_id=? AND COALESCE(warmup_enabled, 0)=1 AND COALESCE(is_active, 1)=1 LIMIT 1",
            manual_scope_params[:1],
        ).fetchone())
    elif manual_scope == "owner":
        team_warmup_enabled = bool(conn.execute(
            "SELECT 1 FROM teams WHERE id=? AND COALESCE(warmup_enabled, 0)=1 LIMIT 1",
            manual_scope_params[:1],
        ).fetchone())
        owner_warmup_enabled = bool(conn.execute(
            "SELECT 1 FROM users WHERE id=? AND COALESCE(warmup_enabled, 0)=1 AND COALESCE(is_active, 1)=1 LIMIT 1",
            manual_scope_params[1:2],
        ).fetchone())
    else:
        team_warmup_enabled = bool(conn.execute(
            "SELECT 1 FROM teams WHERE COALESCE(warmup_enabled, 0)=1 LIMIT 1"
        ).fetchone())
        owner_warmup_enabled = bool(conn.execute(
            "SELECT 1 FROM users WHERE COALESCE(warmup_enabled, 0)=1 AND COALESCE(is_active, 1)=1 LIMIT 1"
        ).fetchone())
    conn.close()
    if not global_warmup_enabled and not team_warmup_enabled and not owner_warmup_enabled:
        return {"status": "disabled", "reason": "warmup disabled"}

    # 2. 守护模式互斥
    if _get_setting("sentinel_enabled", "0") == "1":
        return {"status": "skipped", "reason": "sentinel active"}
    if _get_setting("heartbeat_enabled", "0") == "1":
        return {"status": "skipped", "reason": "heartbeat active"}
    if _get_setting("mirror_enabled", "0") == "1":
        return {"status": "skipped", "reason": "mirror active"}

    # 3. 跟进现有 warming / completed
    _followup_warming()
    _followup_completed()

    # 4. 查候选账户
    diagnostics = _warmup_scan_diagnostics() if manual_scope == "global" else {"scope": manual_scope}
    conn = get_conn()
    good_page_sql = _tw_page_good_sql("p")
    bad_page_sql = _tw_page_bad_sql("p")
    direct_page_clause = (
        "COALESCE(page_id, '') != '' "
        "AND NOT EXISTS (SELECT 1 FROM tw_certified_pages p "
        "WHERE p.page_id=accounts.page_id AND (" + bad_page_sql + "))"
    )
    candidates = conn.execute(f"""
        SELECT accounts.*
        FROM accounts
        LEFT JOIN teams tm ON tm.id=accounts.team_id
        LEFT JOIN users ou ON ou.id=accounts.owner_user_id AND COALESCE(ou.is_active, 1)=1
        WHERE enabled=1
          {manual_scope_sql}
          AND (?=1 OR COALESCE(tm.warmup_enabled, 0)=1 OR COALESCE(ou.warmup_enabled, 0)=1)
          AND COALESCE(account_status, 1) NOT IN (3, 7, 9, 100, 101)
          AND (
              {direct_page_clause}
              OR EXISTS (
                  SELECT 1 FROM tw_certified_pages p
                  WHERE p.token_id=accounts.token_id AND {good_page_sql}
              )
              OR EXISTS (
                  SELECT 1 FROM account_op_tokens aot
                  JOIN tw_certified_pages p ON p.token_id=aot.token_id
                  WHERE aot.act_id=accounts.act_id
                    AND aot.status='active'
                    AND {good_page_sql}
              )
          )
          AND COALESCE(mirror_enabled, 0)=0
          AND COALESCE(tm.mirror_enabled, 0)=0
          AND COALESCE(tm.sentinel_enabled, 0)=0
          AND COALESCE(ou.mirror_enabled, 0)=0
          AND COALESCE(ou.sentinel_enabled, 0)=0
          AND EXISTS (
              SELECT 1 FROM account_op_tokens aot
              JOIN fb_tokens t ON t.id=aot.token_id
              WHERE aot.act_id=accounts.act_id
                AND aot.status='active'
                AND t.status='active'
                AND t.token_type='operate'
                AND COALESCE(t.token_source, 'system_user') IN ('system_user','oauth_user')
          )
          AND (
              (COALESCE(warmup_state, '')='' AND COALESCE(warmup_campaign_id, '')='')
              OR warmup_state='dormant'
              OR (
                  warmup_state='recent_spend'
                  AND (
                      warmup_last_checked_at IS NULL
                      OR warmup_last_checked_at <= datetime('now','+8 hours','-3 days')
                  )
              )
          )
        ORDER BY CASE WHEN warmup_state='dormant' THEN 1 WHEN warmup_state='recent_spend' THEN 2 ELSE 0 END, created_at ASC
    """, (*manual_scope_params, 1 if global_warmup_enabled else 0)).fetchall()
    conn.close()

    if not candidates:
        return {"status": "ok", "scanned": 0, "started": 0, "skipped": 0, "errors": [], "diagnostics": diagnostics}

    # 5. 逐个预热
    started, skipped, security_holds, errors = 0, 0, 0, []
    started_accounts, skipped_accounts, security_hold_accounts = [], [], []
    team_buckets = {}
    for acc in candidates:
        acc_dict = dict(acc)
        team_id = acc_dict.get("team_id")
        bucket = team_buckets.setdefault(team_id, {
            "started": 0,
            "skipped": 0,
            "security_holds": 0,
            "errors": [],
            "started_accounts": [],
            "skipped_accounts": [],
            "security_hold_accounts": [],
        })
        label = _account_label(acc_dict)
        result = _warmup_account(acc_dict)
        if result[0] == "started":
            started += 1
            bucket["started"] += 1
            started_accounts.append(f"{label}: campaign=<code>{result[1]}</code>")
            bucket["started_accounts"].append(f"{label}: campaign=<code>{result[1]}</code>")
        elif result[0] == "skipped":
            detail = _strip_account_prefix(result[1], acc_dict.get('act_id', ''))
            if "身份验证" in (result[1] or ""):
                security_holds += 1
                bucket["security_holds"] += 1
                security_hold_accounts.append(f"{label}: {detail}")
                bucket["security_hold_accounts"].append(f"{label}: {detail}")
            else:
                skipped += 1
                bucket["skipped"] += 1
                skipped_accounts.append(f"{label}: {detail}")
                bucket["skipped_accounts"].append(f"{label}: {detail}")
        else:
            err_line = f"{label}: {_strip_account_prefix(result[1], acc_dict.get('act_id', ''))}"
            errors.append(err_line)
            bucket["errors"].append(err_line)

    summary = {
        "status": "ok",
        "scanned": len(candidates),
        "started": started,
        "skipped": skipped,
        "security_holds": security_holds,
        "errors": errors,
        "started_accounts": started_accounts,
        "security_hold_accounts": security_hold_accounts[:10],
        "skipped_accounts": skipped_accounts[:20],
        "diagnostics": diagnostics,
    }

    for team_id, bucket in team_buckets.items():
        if bucket["started"] <= 0 and bucket["security_holds"] <= 0 and not bucket["errors"]:
            continue
        msg = [
            f"<b>预热扫描</b>",
            f"开始预热：{bucket['started']} 个 | 跳过：{bucket['skipped']} 个 | 失败：{len(bucket['errors'])} 个 | 需验证：{bucket['security_holds']} 个",
        ]
        if bucket["started_accounts"]:
            msg.append("\n<b>已启动：</b>")
            msg.extend("• " + item for item in bucket["started_accounts"][:10])
            if len(bucket["started_accounts"]) > 10:
                msg.append(f"... 还有 {len(bucket['started_accounts']) - 10} 个")
        if bucket["security_hold_accounts"]:
            msg.append("\n<b>需FB验证（广告可能已创建，请登录Ads Manager确认）：</b>")
            msg.extend("• " + item for item in bucket["security_hold_accounts"][:10])
        if bucket["errors"]:
            msg.append("\n<b>失败：</b>")
            msg.extend("• " + item for item in bucket["errors"][:10])
            if len(bucket["errors"]) > 10:
                msg.append(f"... 还有 {len(bucket['errors']) - 10} 个")
        _send_tg("\n".join(msg), team_id=team_id)

    return summary


def rewarm_account(account: dict) -> Tuple[str, str]:
    if not _warmup_lock.acquire(blocking=False):
        return ("skipped", "预热任务正在运行，请稍后重试")
    try:
        return _warmup_account(account)
    finally:
        _warmup_lock.release()


def _fb_error_is_security_hold(result: dict) -> bool:
    """FB error 31/3858385: 账户需要身份验证，不应重试"""
    if not isinstance(result, dict):
        return False
    return result.get("code") == 31 and result.get("subcode") == 3858385


def _mark_fb_security_hold(act_id: str, account_name: str, detail: str, campaign_id: str = "") -> None:
    """标记账户需要 FB 身份验证，停止自动重试"""
    now_cst = _cst_now()
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE accounts SET warmup_state='fb_security_hold',
                warmup_last_checked_at=?,
                warmup_campaign_id=CASE WHEN ? != '' THEN ? ELSE warmup_campaign_id END
            WHERE act_id=?
        """, (now_cst, campaign_id, campaign_id, act_id))
        conn.commit()
    finally:
        conn.close()
    _log_action(act_id, "warmup_fb_security_hold", detail, account_name, status="failed", level="warning")
    logger.warning(f"warmup: {act_id} 需要 FB 身份验证，暂停自动重试 (campaign={campaign_id or 'N/A'})")


def _warmup_account(account: dict) -> Tuple[str, str]:
    """对单个账户执行预热，返回 (状态, 详情)"""
    act_id = account["act_id"]
    currency = account.get("currency", "USD")
    page_id = _resolve_warmup_page_id(account)
    account_name = account.get("name", act_id)
    token = ""
    campaign_id = ""
    adset_id = ""
    creative_id = ""
    ad_id = ""
    created = []

    try:
        if _recent_spend_hold_active(account):
            return ("skipped", _recent_spend_hold_message(account))

        recent_spend, recent_error = _get_recent_account_spend(act_id)
        if recent_spend is not None and recent_spend > 0:
            _mark_recent_spend_skip(account, recent_spend)
            return ("skipped", f"{act_id}: 最近{_WARMUP_RECENT_SPEND_DAYS}天已有消耗 {recent_spend:.2f} {currency}，跳过预热")
        if recent_error:
            logger.warning(f"warmup: {act_id} {recent_error}，跳过预热以避免误创建")
            return ("skipped", f"{act_id}: 无法确认最近{_WARMUP_RECENT_SPEND_DAYS}天消耗，已跳过预热: {recent_error}")

        if page_id:
            block_reason = _warmup_page_block_reason(page_id)
            if block_reason:
                return ("skipped", f"{act_id}: 主页不可投放，已跳过预热: {block_reason}")

        if not page_id:
            return ("skipped", f"{act_id}: 未配置主页 page_id")

        # 1. 选素材。预热只使用展示名或原文件名以 YE 开头的图片。
        conn = get_conn()
        asset = conn.execute("""
            SELECT id, file_name, display_name, file_path, target_countries
            FROM ad_assets
            WHERE COALESCE(file_type, 'image') = 'image'
              AND COALESCE(upload_status, '') IN ('local_saved', 'ai_done', 'ready')
              AND UPPER(COALESCE(NULLIF(TRIM(display_name), ''), file_name, '')) LIKE 'YE%'
            ORDER BY CASE WHEN upload_status='ai_done' THEN 0 ELSE 1 END,
                     RANDOM()
            LIMIT 1
        """).fetchone()
        conn.close()

        if not asset:
            return ("skipped", f"{act_id}: 无 YE 开头的可用图片素材")

        asset = dict(asset)

        # 2. 获取 CREATE token
        token = get_exec_token(act_id, ACTION_CREATE, notify_exhausted=False)
        if not token:
            return ("skipped", f"{act_id}: 无可用操作号 Token")

        # 3. 素材 image_hash
        ok, result = _upload_image_to_fb(act_id, token, asset.get("file_path") or asset.get("file_url", ""))
        if not ok:
            return ("error", f"{act_id}: 素材上传失败: {result}")
        image_hash = result

        mmdd = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%m%d")
        name_prefix = f"WU-{act_id}-{mmdd}"

        # 4. 创建 Campaign
        ok, result = _fb_post_result(
            f"{act_id}/campaigns", token,
            {"name": name_prefix, "objective": "OUTCOME_ENGAGEMENT",
             "status": "PAUSED", "special_ad_categories": [],
             "is_adset_budget_sharing_enabled": False,
             "buying_type": "AUCTION"})
        if not ok:
            err_msg = _format_fb_error(result)
            code = result.get("code", 0)
            if _fb_error_is_security_hold(result):
                _mark_fb_security_hold(act_id, account_name, f"campaign_create: {err_msg}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证，请登录 Ads Manager 完成验证")
            if code in (190, 200, 294):
                logger.error(f"warmup: {act_id} campaign 创建权限错误(code={code}): {err_msg}")
                return ("skipped", f"{act_id}: 权限不足(code={code})")
            return ("error", f"{act_id}: 创建 campaign 失败: {err_msg}")
        campaign_id = result.get("id")
        if not campaign_id:
            return ("error", f"{act_id}: FB 未返回 campaign_id: {result}")
        created.append(("campaign", campaign_id))
        ok, result = _fb_post_result(campaign_id, token, {"status": "PAUSED"})
        if not ok:
            err_msg = _format_fb_error(result)
            if _fb_error_is_security_hold(result):
                _mark_fb_security_hold(act_id, account_name, f"campaign_pause: {err_msg}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证")
            _cleanup_created_objects(act_id, token, created, "campaign_write_preflight_failed", account_name)
            return ("skipped", f"{act_id}: 广告账户当前不可写，无法启动预热: {err_msg}")

        # 5. 创建 AdSet
        daily_budget = _usd5_to_minor_units(currency)
        country = _pick_warmup_country(account, asset)
        ok, result = _fb_post_result(
            f"{act_id}/adsets", token,
            {"name": f"{name_prefix}-AS", "campaign_id": campaign_id,
             "daily_budget": daily_budget,
             "billing_event": "IMPRESSIONS",
             "optimization_goal": "PAGE_LIKES",
             "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
             "targeting": {
                 "geo_locations": {"countries": [country]},
                 "age_min": 18, "age_max": 65,
                 "targeting_automation": {"advantage_audience": 0}},
             "status": "PAUSED",
             "promoted_object": {"page_id": page_id},
             "destination_type": "ON_PAGE"})
        if not ok:
            err_msg = _format_fb_error(result)
            if _fb_error_is_security_hold(result):
                _mark_fb_security_hold(act_id, account_name, f"adset_create: {err_msg}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证")
            _cleanup_created_objects(act_id, token, created, "adset_create_failed", account_name)
            return ("error", f"{act_id}: 创建 adset 失败: {err_msg}")
        adset_id = result.get("id")
        if not adset_id:
            _cleanup_created_objects(act_id, token, created, "missing_adset_id", account_name)
            return ("error", f"{act_id}: FB 未返回 adset_id: {result}")
        created.append(("adset", adset_id))

        # 6. 创建 AdCreative
        object_story_spec = {
            "page_id": page_id,
            "link_data": {
                "image_hash": image_hash,
                "message": "Welcome!",
                "name": "Like our page",
                "link": f"https://www.facebook.com/{page_id}",
                "call_to_action": {"type": "LIKE_PAGE"}
            }}

        ok, result = _fb_post_result(
            f"{act_id}/adcreatives", token,
            {"name": f"{name_prefix}-CR",
             "object_story_spec": object_story_spec})
        if not ok:
            err_msg = _format_fb_error(result)
            if _fb_error_is_security_hold(result):
                _mark_fb_security_hold(act_id, account_name, f"creative_create: {err_msg}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证")
            _cleanup_created_objects(act_id, token, created, "creative_create_failed", account_name)
            return ("error", f"{act_id}: 创建 creative 失败: {err_msg}")
        creative_id = result.get("id")
        if not creative_id:
            _cleanup_created_objects(act_id, token, created, "missing_creative_id", account_name)
            return ("error", f"{act_id}: FB 未返回 creative_id: {result}")
        created.append(("creative", creative_id))

        # 7. 创建 Ad
        ok, result = _fb_post_result(
            f"{act_id}/ads", token,
            {"name": f"{name_prefix}-AD", "adset_id": adset_id,
             "creative": {"creative_id": creative_id},
             "status": "PAUSED"})
        if not ok:
            err_msg = _format_fb_error(result)
            if _fb_error_is_security_hold(result):
                _mark_fb_security_hold(act_id, account_name, f"ad_create: {err_msg}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证，请登录 Ads Manager 完成验证")
            _cleanup_created_objects(act_id, token, created, "ad_create_failed", account_name)
            return ("error", f"{act_id}: 创建 ad 失败: {err_msg}")
        ad_id = result.get("id")
        if not ad_id:
            if _fb_error_is_security_hold(result):
                _mark_fb_security_hold(act_id, account_name, f"missing_ad_id: {result}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证")
            _cleanup_created_objects(act_id, token, created, "missing_ad_id", account_name)
            return ("error", f"{act_id}: FB 未返回 ad_id: {result}")
        created.append(("ad", ad_id))

        # 8. 更新账户状态
        now_cst = _cst_now()
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE accounts SET warmup_state='warming',
                    warmup_triggered_at=?, warmup_campaign_id=?,
                    warmup_last_spend=COALESCE(amount_spent, 0),
                    warmup_last_checked_at=?
                WHERE act_id=?
            """, (now_cst, campaign_id, now_cst, act_id))
            conn.commit()
        finally:
            conn.close()

        ok, activation_error, is_sec_hold = _activate_warmup_objects(token, campaign_id, adset_id, ad_id)
        if not ok:
            if is_sec_hold:
                _mark_fb_security_hold(act_id, account_name, f"activation: {activation_error}", campaign_id)
                return ("skipped", f"{act_id}: 账户需要身份验证，广告已创建但未激活")
            cleanup_failures = _cleanup_created_objects(act_id, token, created, "activation_failed", account_name)
            _clear_warmup_record_for_campaign(act_id, campaign_id, bool(cleanup_failures))
            return ("error", f"{act_id}: 激活预热广告失败: {activation_error}")

        _log_action(act_id, "warmup_start", f"campaign={campaign_id}, country={country}, target_usd={_WARMUP_TARGET_USD:g}", account_name)
        logger.info(f"warmup: {act_id} 预热已启动 campaign={campaign_id}, country={country}, daily_budget={daily_budget}")
        return ("started", campaign_id)

    except Exception as e:
        if created and token:
            cleanup_failures = _cleanup_created_objects(act_id, token, created, "unexpected_exception", account_name)
            if campaign_id:
                _clear_warmup_record_for_campaign(act_id, campaign_id, bool(cleanup_failures))
        logger.error(f"warmup: {act_id} 异常: {e}", exc_info=True)
        return ("error", f"{act_id}: {e}")
