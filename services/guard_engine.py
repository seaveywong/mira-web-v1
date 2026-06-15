"""
广告巡检引擎 v1.1.0
新增: 关闭失败向上升级、TG多ID、操作频率限制、状态核验、新规则类型
"""
import json
import logging
import time
import os
import re
import html
from datetime import datetime, date, timedelta
from typing import Optional, Tuple
import requests

from core.database import get_conn, decrypt_token
from core.account_access import note_account_read_failure, note_account_read_success
from services.notifier import notify_account, notify_global, notify_team

logger = logging.getLogger("mira.guard")

FB_API_BASE = "https://graph.facebook.com/v25.0"
FB_AD_FIELDS = (
    "id,name,status,effective_status,adset_id,campaign_id,"
    "campaign{objective},"
    "adset{optimization_goal,destination_type},"
    "insights.date_preset(today){spend,impressions,reach,clicks,unique_clicks,ctr,unique_ctr,actions,action_values,cpc,cpm}"
)
MIRROR_AD_FIELDS = "id,name,status,effective_status,campaign_id"

_ACCESS_TOKEN_PARAM_RE = re.compile(r"(access_token=)[^&\s]+")
_FB_TOKEN_VALUE_RE = re.compile(r"\bEA[A-Za-z0-9_\-]{20,}\b")


def _sanitize_error_text(value) -> str:
    """Mask access tokens before text is logged or saved to action_logs."""
    text = "" if value is None else str(value)
    text = _ACCESS_TOKEN_PARAM_RE.sub(r"\1***", text)
    return _FB_TOKEN_VALUE_RE.sub("EA***", text)


def _format_fb_response_error(resp: requests.Response) -> str:
    try:
        result = resp.json()
    except ValueError:
        body = _sanitize_error_text(resp.text[:300])
        return f"FB API HTTP {resp.status_code}: {body}"

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

# 操作冷却：同一广告同一规则60分钟内不重复触发
_action_cooldown: dict = {}  # key: f"{ad_id}:{rule_type}" -> timestamp
_COOLDOWN_TTL = 7200  # 2小时TTL，超过此时间的冷却记录可清理

# ── 规则类型中文标签 ───────────────────────────────────────────
_rule_type_labels = {
    "bleed_abs": "空成效止血",
    "cpa_exceed": "CPA超标止损",
    "trend_drop": "ROAS趋势熔断",
    "consecutive_bad": "连续恶化止损",
    "click_no_conv": "高频点击无转化",
    "low_ctr_no_conv": "低CTR空转止损",
    "reach_no_conv": "高覆盖无转化",
    "budget_burn_fast": "瞬烧制止",
}

def _cleanup_cooldown():
    """清理过期冷却记录，防止内存泄漏"""
    now = time.time()
    expired = [k for k, v in _action_cooldown.items() if now - v > _COOLDOWN_TTL]
    for k in expired:
        del _action_cooldown[k]


def _get_setting(key: str, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


# ── 货币转换：将任意货币金额转换为 USD ───────────────────────────────────────
_FX_RATES = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
    "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74,
    "AUD": 0.65, "CAD": 0.74, "BRL": 0.20, "MXN": 0.058,
    "CLP": 0.0011, "COP": 0.00025, "PEN": 0.27, "ARS": 0.001,
    "THB": 0.028, "VND": 0.000040, "IDR": 0.000063, "PHP": 0.017,
    "MYR": 0.21, "INR": 0.012, "TRY": 0.031, "ZAR": 0.053,
    "BDT": 0.0091, "PKR": 0.0036, "LKR": 0.0031, "NPR": 0.0075,
    "KRW": 0.00072, "CHF": 1.12, "NZD": 0.60, "SEK": 0.096,
    "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "CZK": 0.044,
    "HUF": 0.0028, "RON": 0.22, "BGN": 0.55, "HRK": 0.14,
    "AED": 0.272, "SAR": 0.267, "QAR": 0.275, "KWD": 3.26,
    "BHD": 2.65, "OMR": 2.60, "JOD": 1.41, "EGP": 0.021,
    "MAD": 0.099, "TND": 0.32, "GHS": 0.067, "NGN": 0.00065,
    "KES": 0.0077, "TZS": 0.00038, "UGX": 0.00027, "ETB": 0.0088,
    "UAH": 0.027, "KZT": 0.0022, "UZS": 0.000079, "GEL": 0.37,
    "AMD": 0.0026, "AZN": 0.59, "BYN": 0.31, "MDL": 0.056,
    "RSD": 0.0093, "MKD": 0.018, "ALL": 0.011, "BAM": 0.55,
    "CRC": 0.0019, "GTQ": 0.13, "HNL": 0.040, "NIO": 0.027,
    "PAB": 1.0, "DOP": 0.017, "JMD": 0.0064, "TTD": 0.15,
    "BBD": 0.50, "BSD": 1.0, "BZD": 0.50, "GYD": 0.0048,
    "SRD": 0.029, "UYU": 0.026, "PYG": 0.000135, "BOB": 0.145,
    "VES": 0.000027, "CUP": 0.042,
}


def _to_usd_guard(amount: float, currency: str) -> float:
    """修复: 将任意货币金额转换为USD
    优先从 currency_rates 数据库读取实时汇率，如果数据库无数据则回退到静态表
    """
    if amount is None:
        return 0.0
    cur = (currency or "USD").upper().strip()
    if cur == "USD":
        return float(amount)
    # 优先查询数据库实时汇率
    try:
        _conn = get_conn()
        _row = _conn.execute(
            "SELECT rate FROM currency_rates WHERE currency=?", (cur,)
        ).fetchone()
        _conn.close()
        if _row and _row["rate"]:
            return float(amount) / float(_row["rate"])  # currency_rates 存的是 1USD=X货币
    except Exception:
        pass
    # 备用静态表
    rate = _FX_RATES.get(cur, 1.0)
    return float(amount) * rate


def _db_rate_to_usd_multiplier(currency: str, raw_rate) -> Optional[float]:
    """Return USD-per-one-local-unit from either supported DB rate direction."""
    try:
        rate = float(raw_rate)
    except (TypeError, ValueError):
        return None
    if rate <= 0:
        return None
    cur = (currency or "USD").upper().strip()
    static = _FX_RATES.get(cur)
    candidates = [rate, 1.0 / rate]
    if static and static > 0:
        return min(candidates, key=lambda x: abs(x - static) / static)
    if rate > 10:
        return 1.0 / rate
    if rate < 0.1:
        return rate
    return 1.0 / rate


def _currency_to_usd_multiplier(currency: str) -> float:
    """Return USD-per-one-local-unit with DB-first and static fallback."""
    cur = (currency or "USD").upper().strip()
    if cur == "USD":
        return 1.0
    try:
        _conn = get_conn()
        _row = _conn.execute(
            "SELECT rate FROM currency_rates WHERE currency=? ORDER BY updated_at DESC LIMIT 1",
            (cur,),
        ).fetchone()
        _conn.close()
        if _row and _row["rate"]:
            multiplier = _db_rate_to_usd_multiplier(cur, _row["rate"])
            if multiplier:
                return multiplier
    except Exception as exc:
        logger.warning("currency rate lookup failed for %s: %s", cur, exc)
    if cur in _FX_RATES:
        return _FX_RATES[cur]
    logger.warning("missing currency rate for %s; fallback to 1:1 USD", cur)
    return 1.0


def _local_per_usd_rate(currency: str) -> float:
    multiplier = _currency_to_usd_multiplier(currency)
    return (1.0 / multiplier) if multiplier > 0 else 1.0


def _to_usd_guard(amount: float, currency: str) -> float:
    """Convert local currency amount to USD. Handles DB rates in either direction."""
    if amount is None:
        return 0.0
    return float(amount) * _currency_to_usd_multiplier(currency)


def _is_dry_run() -> bool:
    return _get_setting("dry_run", "0") == "1"


def _fb_get(path: str, token: str, params: dict = None,
             paginate: bool = False, max_pages: int = 50) -> dict:
    """
    FB API GET 请求。
    注意：如果 params 中包含 effective_status，会自动将其从 params 中移出并手动拼接到URL，
    避免 requests 将方括号和引号 URL 编码导致 FB API 400 错误。

    当 paginate=True 时，自动跟随 paging.next 游标获取所有分页数据，
    返回 {"data": combined_data} 保持与单页调用兼容。
    max_pages 限制最大翻页数，防止无限循环（默认 50）。
    """
    import urllib.parse
    p = dict(params or {})
    effective_status = p.pop("effective_status", None)
    p["access_token"] = token
    base_url = f"{FB_API_BASE}/{path}?{urllib.parse.urlencode(p)}"
    if effective_status:
        base_url += f"&effective_status={effective_status}"

    if not paginate:
        try:
            resp = requests.get(base_url, timeout=20)
            return _json_or_fb_error(resp)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(_sanitize_error_text(f"Network error: {e}")) from e

    # ── 分页模式：跟随 paging.next 游标直到所有数据拉取完毕 ──────────────
    all_data = []
    next_url = base_url
    page_count = 0

    while next_url and page_count < max_pages:
        try:
            resp = requests.get(next_url, timeout=20)
            result = _json_or_fb_error(resp)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(_sanitize_error_text(f"Network error: {e}")) from e
        page_data = result.get("data", [])
        if page_data:
            all_data.extend(page_data)
        page_count += 1

        # 获取下一页游标
        paging = result.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break

    return {"data": all_data}


def _fb_post(path: str, token: str, data: dict) -> Tuple[bool, str]:
    """执行FB写操作，返回 (成功, 错误原因)"""
    import urllib.parse
    url = f"{FB_API_BASE}/{path}?access_token={urllib.parse.quote(token, safe='')}"
    try:
        resp = requests.post(url, data=data, timeout=20)
        result = resp.json()
        if resp.status_code == 200 and result.get("success"):
            return True, ""
        # 区分错误类型
        err = result.get("error", {})
        code = err.get("code", 0)
        subcode = err.get("error_subcode", 0)
        msg = _sanitize_error_text(err.get("message", str(result)))
        # 190=Token失效, 100=权限不足, 200=权限拒绝 -> 不重试，直接向上升级
        if code in (190, 100, 200, 294):
            suffix = ""
            if subcode == 3498005:
                suffix = " [Reels广告需升级到广告组操作]"
            return False, f"权限拒绝(code={code}{suffix}): {msg}"
        return False, f"API错误(code={code}): {msg}"
    except requests.exceptions.RequestException as e:
        return False, f"网络错误: {e}"



def _update_adset_budget(adset_id: str, token: str, delta_pct: float,
                         act_id: str = "", ad_name: str = "",
                         max_budget: Optional[float] = None) -> Tuple[bool, str, float, float]:
    """
    调整广告组日预算。
    delta_pct: 正数=增加，负数=减少（如 0.2 = +20%，-0.2 = -20%）
    返回: (成功, 错误信息, 原预算, 新预算)
    """
    # 零小数位货币（JPY/KRW 等：FB API 直接传整数）
    _NO_DECIMAL_CURRENCIES = {"JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS"}
    try:
        # 获取当前预算
        result = _fb_get(adset_id, token, {"fields": "daily_budget,bid_strategy,currency"})
        cur_budget = float(result.get("daily_budget", 0))
        if cur_budget <= 0:
            return False, "广告组无日预算（可能使用系列预算）", 0, 0
        _budget_currency = (result.get("currency") or "USD").upper().strip()
        _is_no_decimal = _budget_currency in _NO_DECIMAL_CURRENCIES
        # 计算新预算（FB API 预算单位为分/整数）
        new_budget = cur_budget * (1 + delta_pct)
        # 最低预算保护
        new_budget = max(new_budget, 100)
        # 最高预算保护：增加时不超过原预算的 3 倍
        if delta_pct > 0:
            new_budget = min(new_budget, cur_budget * 3)
            if max_budget and float(max_budget) > 0:
                max_budget_usd = float(max_budget)
                max_budget_major = max_budget_usd * _local_per_usd_rate(_budget_currency)
                max_budget_api = max_budget_major if _is_no_decimal else max_budget_major * 100
                if cur_budget >= max_budget_api:
                    old_display = cur_budget if _is_no_decimal else cur_budget / 100
                    return False, "budget_cap_reached", old_display, old_display
                new_budget = min(new_budget, max_budget_api)
        new_budget_int = int(new_budget)
        ok, err = _fb_post(adset_id, token, {"daily_budget": new_budget_int})
        if ok:
            if _is_no_decimal:
                return True, "", cur_budget, new_budget_int
            return True, "", cur_budget / 100, new_budget_int / 100
        if _is_no_decimal:
            return False, err, cur_budget, 0
        return False, err, cur_budget / 100, 0
    except Exception as e:
        return False, _sanitize_error_text(e), 0, 0

def _verify_status(obj_id: str, token: str, expected: str = "PAUSED") -> bool:
    """核验对象状态是否符合预期 — 必须同时检查 effective_status"""
    try:
        result = _fb_get(obj_id, token, {"fields": "status,effective_status"})
        actual = result.get("status", "")
        effective = result.get("effective_status", "")
        if expected == "PAUSED":
            cannot_spend = {"PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED",
                           "DELETED", "ARCHIVED", "DISAPPROVED", "WITH_ISSUES"}
            return actual == "PAUSED" and effective in cannot_spend
        return actual == expected
    except Exception:
        return False


def _is_silent(silent_start: str, silent_end: str) -> bool:
    if not silent_start or not silent_end:
        return False
    now = datetime.now().strftime("%H:%M")
    if silent_start <= silent_end:
        return silent_start <= now <= silent_end
    return now >= silent_start or now <= silent_end


def _check_cooldown(ad_id: str, rule_type: str, cooldown_min: int = 60) -> bool:
    """检查是否在冷却期内，True=冷却中不执行"""
    key = f"{ad_id}:{rule_type}"
    _cleanup_cooldown()
    last = _action_cooldown.get(key, 0)
    if time.time() - last < cooldown_min * 60:
        return True
    return False


def _set_cooldown(ad_id: str, rule_type: str):
    key = f"{ad_id}:{rule_type}"
    _action_cooldown[key] = time.time()


# ── 镜像模式辅助函数 ───────────────────────────────────────────────────────────

def _ensure_mirror_schema():
    """Make mirror mode DB objects idempotent so patrol cannot fail on missing schema."""
    conn = get_conn()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        if "mirror_enabled" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN mirror_enabled INTEGER DEFAULT 0")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS mirror_snapshots (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               act_id TEXT NOT NULL,
               ad_id TEXT NOT NULL,
               ad_name TEXT,
               captured_at TEXT DEFAULT (datetime('now','+8 hours')),
               UNIQUE(act_id, ad_id)
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mirror_snapshots_act ON mirror_snapshots(act_id)")
        conn.execute(
            """INSERT OR IGNORE INTO settings(key,value,label,description,category,sort_order)
               VALUES ('mirror_enabled','0','镜像模式','开启后暂停所有不在快照白名单中的活跃广告','guard',5)"""
        )
        conn.commit()
    finally:
        conn.close()


def _fill_missing_hierarchy_ids(
    act_id: str, ad_id: str = "", adset_id: str = "", campaign_id: str = ""
) -> Tuple[str, str]:
    """Use the latest local snapshot to fill missing adset/campaign ids."""
    if adset_id and campaign_id:
        return adset_id, campaign_id
    try:
        conn = get_conn()
        row = conn.execute(
            """SELECT adset_id, campaign_id
               FROM perf_snapshots
               WHERE act_id=?
                 AND (
                   (? <> '' AND ad_id=?)
                   OR (? <> '' AND adset_id=?)
                   OR (? <> '' AND campaign_id=?)
                 )
               ORDER BY snapshot_date DESC, id DESC
               LIMIT 1""",
            (
                act_id,
                ad_id or "", ad_id or "",
                adset_id or "", adset_id or "",
                campaign_id or "", campaign_id or "",
            ),
        ).fetchone()
        conn.close()
        if row:
            adset_id = adset_id or (row["adset_id"] or "")
            campaign_id = campaign_id or (row["campaign_id"] or "")
    except Exception as exc:
        logger.warning("Hierarchy id fallback failed for %s/%s: %s", act_id, ad_id, exc)
    return adset_id or "", campaign_id or ""


def _ensure_sentinel_schema():
    """Make account-level sentinel switch idempotent."""
    conn = get_conn()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        if "sentinel_enabled" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN sentinel_enabled INTEGER DEFAULT 0")
            conn.commit()
    finally:
        conn.close()


TEAM_GUARD_KEYS = ("sentinel_enabled", "mirror_enabled", "heartbeat_enabled", "warmup_enabled")
USER_GUARD_KEYS = TEAM_GUARD_KEYS
OWNER_SCOPE_ACT_ID = "__owner__"
RULE_SCOPE_ACCOUNT = "account"
RULE_SCOPE_OWNER = "owner"


def _ensure_team_guard_schema():
    conn = get_conn()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(teams)").fetchall()}
        for key in TEAM_GUARD_KEYS:
            if key not in cols:
                conn.execute(f"ALTER TABLE teams ADD COLUMN {key} INTEGER DEFAULT 0")
        conn.commit()
    finally:
        conn.close()


def _ensure_user_guard_schema():
    conn = get_conn()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
        for key in USER_GUARD_KEYS:
            if key not in cols:
                conn.execute(f"ALTER TABLE users ADD COLUMN {key} INTEGER DEFAULT 0")
        conn.commit()
    finally:
        conn.close()


def _ensure_rule_scope_schema():
    conn = get_conn()
    try:
        for table in ("guard_rules", "scale_rules"):
            cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if not cols:
                continue
            if "scope" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN scope TEXT DEFAULT 'account'")
            if "owner_user_id" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN owner_user_id INTEGER")
            if "team_id" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN team_id INTEGER")
            if "created_by" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN created_by TEXT")
            conn.execute(f"UPDATE {table} SET scope='account' WHERE scope IS NULL OR scope=''")
            conn.execute(f"DELETE FROM {table} WHERE act_id='__global__'")
            conn.execute(
                f"""UPDATE {table}
                   SET team_id=(SELECT a.team_id FROM accounts a WHERE a.act_id={table}.act_id)
                    WHERE team_id IS NULL AND act_id NOT IN ('__global__', ?)""",
                (OWNER_SCOPE_ACT_ID,),
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_scope_owner ON {table}(scope, owner_user_id, enabled)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_scope_account ON {table}(act_id, scope, enabled)")
        conn.commit()
    finally:
        conn.close()


def _load_rules_for_account(conn, table: str, account: dict) -> list[dict]:
    act_id = account.get("act_id")
    owner_user_id = account.get("owner_user_id")
    team_id = account.get("team_id")
    params = [RULE_SCOPE_ACCOUNT, act_id]
    clauses = ["(COALESCE(scope,'account')=? AND act_id=?)"]
    if owner_user_id:
        clauses.append("(scope=? AND owner_user_id=?)")
        params.extend([RULE_SCOPE_OWNER, owner_user_id])
    # Team id is only a safety fence for scoped rules; account legacy rows may have NULL.
    rows = conn.execute(
        f"""SELECT * FROM {table}
            WHERE enabled=1 AND ({' OR '.join(clauses)})
              AND (team_id IS NULL OR team_id=? OR COALESCE(scope,'account')='account')
            ORDER BY CASE WHEN COALESCE(scope,'account')='owner' THEN 0 ELSE 1 END, id""",
        params + [team_id],
    ).fetchall()
    return [dict(r) for r in rows]


def _account_team_guard_enabled(account: dict, key: str) -> bool:
    alias = f"team_{key}"
    if alias in account:
        return int(account.get(alias) or 0) == 1
    team_id = account.get("team_id")
    if not team_id:
        return False
    try:
        _ensure_team_guard_schema()
        conn = get_conn()
        row = conn.execute(f"SELECT COALESCE({key}, 0) AS enabled FROM teams WHERE id=?", (team_id,)).fetchone()
        conn.close()
        return bool(row and row["enabled"])
    except Exception:
        return False


def _account_owner_guard_enabled(account: dict, key: str) -> bool:
    alias = f"owner_{key}"
    if alias in account:
        return int(account.get(alias) or 0) == 1
    owner_user_id = account.get("owner_user_id")
    if not owner_user_id:
        return False
    try:
        _ensure_user_guard_schema()
        conn = get_conn()
        row = conn.execute(
            f"SELECT COALESCE({key}, 0) AS enabled FROM users WHERE id=? AND COALESCE(is_active, 1)=1",
            (owner_user_id,),
        ).fetchone()
        conn.close()
        return bool(row and row["enabled"])
    except Exception:
        return False


def _any_team_guard_enabled(key: str) -> bool:
    try:
        _ensure_team_guard_schema()
        conn = get_conn()
        row = conn.execute(f"SELECT 1 FROM teams WHERE COALESCE({key}, 0)=1 LIMIT 1").fetchone()
        conn.close()
        return bool(row)
    except Exception:
        return False


def _any_owner_guard_enabled(key: str) -> bool:
    try:
        _ensure_user_guard_schema()
        conn = get_conn()
        row = conn.execute(
            f"SELECT 1 FROM users WHERE COALESCE({key}, 0)=1 AND COALESCE(is_active, 1)=1 LIMIT 1"
        ).fetchone()
        conn.close()
        return bool(row)
    except Exception:
        return False


def _load_mirror_snapshot(act_id: str) -> set:
    """返回该账户镜像快照中的广告ID集合"""
    _ensure_mirror_schema()
    conn = get_conn()
    rows = conn.execute(
        "SELECT ad_id FROM mirror_snapshots WHERE act_id=?", (act_id,)
    ).fetchall()
    conn.close()
    return {r["ad_id"] for r in rows}


def _mirror_snapshotable_ads(ads: list) -> list:
    _cannot_snapshot = {"DELETED", "ARCHIVED"}
    result = []
    for ad in ads:
        eff = ad.get("effective_status", "")
        if eff in _cannot_snapshot:
            continue
        if ad.get("id"):
            result.append(ad)
    return result


def _capture_mirror_snapshot(act_id: str, ads: list,
                           source: str = "patrol",
                           note: str = "",
                           paging_complete: int = 1,
                           expected_count: int = 0,
                           verified: bool = False) -> int:
    _ensure_mirror_schema()
    conn = get_conn()
    conn.execute("DELETE FROM mirror_snapshots WHERE act_id=?", (act_id,))
    count = 0
    _cannot_snapshot = {"DELETED", "ARCHIVED"}
    for ad in ads:
        eff = ad.get("effective_status", "")
        if eff in _cannot_snapshot:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO mirror_snapshots (act_id, ad_id, ad_name) VALUES (?,?,?)",
            (act_id, ad["id"], ad.get("name", ad["id"]))
        )
        count += 1
    from datetime import datetime
    now_cst = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT OR REPLACE INTO mirror_snapshot_meta
        (act_id, captured_at, source, note, expected_count, captured_count, paging_complete, is_partial)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (act_id, now_cst, source, note, expected_count, count, paging_complete,
          0 if paging_complete else 1))
    if verified:
        conn.execute("UPDATE mirror_snapshot_meta SET verified_at=? WHERE act_id=?",
                     (now_cst, act_id))
    conn.commit()
    conn.close()
    return count

def _log_action(act_id, level, target_id, target_name,
                action_type, trigger_type, trigger_detail,
                old_value=None, new_value=None,
                status="success", error_msg=None, operator="system"):
    conn = get_conn()
    conn.execute(
        """INSERT INTO action_logs
           (act_id, level, target_id, target_name, action_type,
            trigger_type, trigger_detail, old_value, new_value,
            status, error_msg, operator)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (act_id, level, target_id, target_name, action_type,
         trigger_type, trigger_detail,
         json.dumps(old_value) if old_value else None,
         json.dumps(new_value) if new_value else None,
         status, error_msg, operator)
    )
    conn.commit()
    conn.close()


def _send_tg(
    msg: str,
    parse_mode: str = "HTML",
    act_id: str | None = None,
    team_id: int | None = None,
    event_type: str = "guard",
    include_owner: bool = True,
):
    """Route TG notifications through team/account ownership rules."""
    try:
        if act_id:
            return notify_account(act_id, msg, event_type=event_type, parse_mode=parse_mode, include_owner=include_owner)
        if team_id is not None:
            return notify_team(team_id, msg, event_type=event_type, parse_mode=parse_mode)
        return notify_global(msg, parse_mode=parse_mode, dedup_key=event_type)
    except Exception as e:
        logger.warning(f"TG 推送失败: {e}")
        return {"sent": 0, "errors": [{"error": str(e)}]}


def _tg_escape(value) -> str:
    return html.escape("" if value is None else str(value), quote=False)


def _tg_code(value) -> str:
    return f"<code>{_tg_escape(value)}</code>"


def _tg_account_line(account: dict, act_id: str) -> str:
    account_name = (account or {}).get("name") or act_id
    return f"账户：{_tg_escape(account_name)} ({_tg_code(act_id)})"


def _build_mirror_patrol_summary(results: list) -> str:
    """Build single TG summary message for the mirror patrol cycle (all accounts)."""
    accounts_checked = len(results)
    total_closed = sum(
        sum(1 for c in r.get("closures", []) if c.get("type") not in ("dry_run", "failed"))
        for r in results
    )
    total_failed = sum(
        sum(1 for c in r.get("closures", []) if c.get("type") == "failed")
        for r in results
    )
    total_dry = sum(
        sum(1 for c in r.get("closures", []) if c.get("type") == "dry_run")
        for r in results
    )
    total_review = sum(len(r.get("review_pending", [])) for r in results)
    skipped = sum(1 for r in results if r.get("status") == "no_snapshot")

    parts = ["\U0001fabe <b>镜像巡检报告</b>"]
    summary_stats = f"检查账户: {accounts_checked} | 关闭操作: {total_closed}"
    if total_dry:
        summary_stats += f" | 模拟: {total_dry}"
    if total_failed:
        summary_stats += f" | <b>失败: {total_failed}</b>"
    if total_review:
        summary_stats += f" | 审核中: {total_review}"
    if skipped:
        summary_stats += f" | 快照为空跳过: {skipped}"
    parts.append(summary_stats)

    for r in results:
        if not r.get("closures") and not r.get("review_pending"):
            continue
        name = r.get("account_name", r.get("act_id", "?"))
        act_id = r.get("act_id", "?")
        lines = []
        for c in r.get("closures", []):
            ad_names = c.get("ad_names", [])
            ad_list = ", ".join(ad_names[:3])
            if len(ad_names) > 3:
                ad_list += f" ...等{len(ad_names)}条"
            if c.get("type") == "dry_run":
                lines.append(f"  [模拟] {c.get('level','?')} <code>{c.get('id','?')}</code>: {ad_list}")
            elif c.get("type") == "failed":
                err = c.get("error", "")
                lines.append(f"  ❌ 失败 {c.get('level','?')} <code>{c.get('id','?')}</code>: {ad_list} ({err})")
            else:
                lines.append(f"  关闭 {c.get('level','?')} <code>{c.get('id','?')}</code>: {ad_list}")
        for rp in r.get("review_pending", []):
            lines.append(f"  ⚠️ 审核中 系列 <code>{rp.get('campaign_id','?')}</code>: {', '.join(rp.get('ad_names',[])[:3])}")
        if lines:
            parts.append(f"\n<b>{name}</b> (<code>{act_id}</code>)")
            parts.extend(lines)

    if total_dry:
        parts.append("\n<i>当前为模拟模式，未实际执行关闭</i>")
    return "\n".join(parts)


def _build_mirror_account_summary(act_id: str, account_name: str, events: list) -> str:
    """Build single TG message summarizing mirror actions for one account (inspect_account)."""
    paused_ok = [e for e in events if e.get("type") in ("pause_ad", "close_campaign") and e.get("status") == "success"]
    paused_fail = [e for e in events if e.get("status") == "failed"]
    reviews = [e for e in events if e.get("type") == "review"]
    dry = [e for e in events if e.get("type") == "close_campaign_dry"]

    closed_count = sum(len(e.get("ad_names", [])) for e in paused_ok)
    parts = ["\U0001fabe <b>镜像巡检 - {}</b>".format(account_name)]
    stats = f"账户: <code>{act_id}</code>"
    if closed_count:
        stats += f" | 关闭: {closed_count} 条广告"
    if paused_fail:
        stats += f" | <b>失败: {len(paused_fail)}</b>"
    if reviews:
        stats += f" | 审核中: {len(reviews)}"
    if dry:
        stats += f" | 模拟: {len(dry)}"
    parts.append(stats)

    for e in events:
        if e["type"] == "review":
            parts.append(f"  ⚠️ 审核中 系列 <code>{e['campaign_id']}</code>: {', '.join(e['ad_names'][:3])}")
        elif e["type"] == "pause_ad":
            prefix = "❌ " if e["status"] == "failed" else ""
            detail = f" ({e.get('error','')})" if e["status"] == "failed" else ""
            parts.append(f"  {prefix}暂停广告 <code>{e['ad_id']}</code>: {', '.join(e['ad_names'])}{detail}")
        elif e["type"] in ("close_campaign", "close_campaign_dry"):
            label = "[模拟] " if e["type"] == "close_campaign_dry" else ""
            if e.get("status") == "failed":
                status_label = "失败: " + e.get("error", "")
            elif e["type"] == "close_campaign_dry":
                status_label = "模拟关闭"
            else:
                status_label = "已关闭"
            parts.append(f"  {label}系列 <code>{e['campaign_id']}</code>: {status_label} {len(e['ad_names'])} 条 ({', '.join(e['ad_names'][:3])})")

    if dry:
        parts.append("\n<i>当前为模拟模式，未实际执行关闭</i>")
    return "\n".join(parts)


def _get_token_for_account(account: dict, action_type: str = "PAUSE") -> str:
    """
    v3.0 Token 调度入口（非侵入式升级）
    优先使用 TokenManager 的操作号轮询逻辑；
    如果账户没有绑定任何操作号，则回退到原有管理号逻辑（完全兼容旧版）。
    """
    act_id = account.get("act_id", "")

    # ── v3：尝试通过 TokenManager 获取操作号 ──────────────────────────────
    try:
        from services.token_manager import get_exec_token, ACTION_PAUSE, ACTION_CREATE, ACTION_UPDATE, ACTION_READ
        _action_map = {
            "PAUSE": ACTION_PAUSE,
            "CREATE": ACTION_CREATE,
            "UPDATE": ACTION_UPDATE,
            "READ": ACTION_READ,
        }
        tm_action = _action_map.get(action_type, ACTION_PAUSE)
        # 先检查是否有操作号绑定
        from core.database import get_conn as _gc
        _c = _gc()
        _has_op = _c.execute(
            "SELECT 1 FROM account_op_tokens WHERE act_id=? AND status='active' LIMIT 1",
            (act_id,)
        ).fetchone()
        _c.close()
        if _has_op:
            token = get_exec_token(act_id, tm_action)
            if token:
                return token
            # TokenManager 无可用 Token，回退到旧版兜底逻辑
            logger.warning(f"TokenManager 无可用 Token for {act_id} action={tm_action}，回退到旧版兜底")
    except Exception as e:
        logger.warning(f"TokenManager 调用失败，回退到旧逻辑: {e}")

    # ── 旧版兜底逻辑（完全保留，兼容无操作号的账户）──────────────────────
    token_id = account.get("token_id")
    if token_id:
        conn = get_conn()
        row = conn.execute(
            "SELECT access_token_enc, status FROM fb_tokens WHERE id=?",
            (token_id,)
        ).fetchone()
        conn.close()
        if row and row["status"] == "active":
            token = decrypt_token(row["access_token_enc"])
            if token:
                return token
        # 主Token失效，尝试其他active Token
        conn = get_conn()
        fallbacks = conn.execute(
            "SELECT id, access_token_enc FROM fb_tokens WHERE status='active' AND id!=? ORDER BY id",
            (token_id,)
        ).fetchall()
        conn.close()
        for fb in fallbacks:
            token = decrypt_token(fb["access_token_enc"])
            if token:
                logger.warning(f"账户 {act_id} 主Token失效，切换到备用Token id={fb['id']}")
                return token
    # 兼容旧版直接存储的 access_token
    direct = account.get("access_token", "")
    if direct:
        return direct
    # 最后兜底：取任意active Token
    conn = get_conn()
    any_tk = conn.execute("SELECT access_token_enc FROM fb_tokens WHERE status='active' LIMIT 1").fetchone()
    conn.close()
    if any_tk:
        return decrypt_token(any_tk["access_token_enc"])
    return ""


# ── 核心关闭逻辑（含向上升级）──────────────────────────────────────────────

def _pause_with_escalation(
    account: dict, ad_id: str, adset_id: str, campaign_id: str,
    ad_name: str, token: str, trigger_type: str, trigger_detail: str,
    dry_run: bool
) -> Tuple[str, str]:
    """
    尝试暂停广告，失败则向上升级到广告组，再失败则升级到系列
    返回: (最终执行级别, 状态)
    """
    act_id = account["act_id"]
    adset_id, campaign_id = _fill_missing_hierarchy_ids(act_id, ad_id, adset_id, campaign_id)
    account_line = _tg_account_line(account, act_id)
    ad_line = f"广告：{_tg_code(ad_name)}"
    ad_id_line = f"广告ID：{_tg_code(ad_id)}"
    adset_id_line = f"广告组ID：{_tg_code(adset_id or '-')}"
    campaign_id_line = f"系列ID：{_tg_code(campaign_id or '-')}"

    if dry_run:
        logger.info(f"[DRY RUN] 暂停广告 {ad_id}")
        _log_action(act_id, "ad", ad_id, ad_name, "pause",
                    trigger_type, f"[模拟] {trigger_detail}",
                    old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                    status="success", operator="system")
        return "ad", "dry_run"

    # Step 1: 尝试暂停广告
    ok, err_msg = _fb_post(ad_id, token, {"status": "PAUSED"})
    if ok:
        # 核验
        time.sleep(2)
        verified = _verify_status(ad_id, token, "PAUSED")
        status = "success" if verified else "failed"
        _log_action(act_id, "ad", ad_id, ad_name, "pause",
                    trigger_type, trigger_detail,
                    old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                    status=status, operator="system")
        if verified:
            return "ad", "success"
        err_msg = "核验失败：广告状态未变更为PAUSED"

    # 记录广告级失败
    logger.warning(f"广告 {ad_id} 无法直接暂停({err_msg})，尝试向上升级到广告组")
    _log_action(act_id, "ad", ad_id, ad_name, "pause",
                trigger_type, trigger_detail,
                status="failed", error_msg=err_msg, operator="system")

    escalate = _get_setting("escalate_on_fail", "1") == "1"
    if not escalate:
        _send_tg(
            f"❌ <b>Mira 暂停失败</b>\n"
            f"{account_line}\n"
            f"{ad_line}\n"
            f"{ad_id_line}\n"
            f"原因：{_tg_escape(err_msg)}\n"
            f"⚠️ 向上升级已关闭，请手动处理！",
            act_id=act_id,
            event_type="guard",
        )
        return "ad", "failed"

    # Step 2: 向上升级到广告组
    adset_error = ""
    if adset_id:
        ok2, err2 = _fb_post(adset_id, token, {"status": "PAUSED"})
        if ok2:
            time.sleep(2)
            verified2 = _verify_status(adset_id, token, "PAUSED")
            status2 = "escalated" if verified2 else "failed"
            _log_action(act_id, "adset", adset_id, f"[升级关闭] {ad_name}的广告组",
                        "pause", trigger_type,
                        f"因广告{ad_id}关闭失败，升级关闭广告组。原因：{err_msg}",
                        old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                        status=status2, operator="system")
            if verified2:
                _send_tg(
                    f"⬆️ <b>Mira 升级关闭</b>\n"
                    f"{account_line}\n"
                    f"{ad_line}\n"
                    f"{ad_id_line}\n"
                    f"{adset_id_line}\n"
                    f"触发原因：{_tg_escape(trigger_detail)}\n"
                    f"广告无法直接关闭，已升级关闭广告组\n"
                    f"原因：{_tg_escape(err_msg)}",
                    act_id=act_id,
                    event_type="guard",
                )
                return "adset", "escalated"
            err2 = f"广告组核验失败: {err2}"

        logger.warning(f"广告组 {adset_id} 暂停失败: {err2}，尝试向上升级到系列")
        adset_error = err2
        _log_action(act_id, "adset", adset_id, "广告组", "pause",
                    trigger_type, f"升级关闭广告组失败",
                    status="failed", error_msg=err2, operator="system")
    else:
        adset_error = "缺少广告组ID，无法升级关闭广告组"

    # Step 3: 向上升级到系列
    campaign_error = ""
    campaign_error = ""
    if campaign_id:
        ok3, err3 = _fb_post(campaign_id, token, {"status": "PAUSED"})
        if ok3:
            time.sleep(2)
            verified3 = _verify_status(campaign_id, token, "PAUSED")
            status3 = "escalated" if verified3 else "failed"
            campaign_error = "" if verified3 else "系列核验失败：状态未变更为PAUSED"
            _log_action(act_id, "campaign", campaign_id, f"[升级关闭] {ad_name}的系列",
                        "pause", trigger_type,
                        f"因广告/广告组关闭失败，升级关闭系列。",
                        old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                        status=status3, error_msg=campaign_error or None, operator="system")
            if verified3:
                _send_tg(
                    f"🚨 <b>Mira 紧急升级关闭系列</b>\n"
                    f"{account_line}\n"
                    f"{ad_line}\n"
                    f"{ad_id_line}\n"
                    f"{adset_id_line}\n"
                    f"{campaign_id_line}\n"
                    f"广告及广告组均关闭失败\n"
                    f"已自动关闭其所属广告系列！\n"
                    f"请立即检查账户状态",
                    act_id=act_id,
                    event_type="guard",
                )
                return "campaign", "escalated"
        else:
            campaign_error = err3
            logger.warning(f"系列 {campaign_id} 暂停失败: {campaign_error}")
            _log_action(act_id, "campaign", campaign_id, "广告系列", "pause",
                        trigger_type, "升级关闭系列失败",
                        status="failed", error_msg=campaign_error, operator="system")
    else:
        campaign_error = "缺少系列ID，无法升级关闭系列"
        _log_action(act_id, "campaign", f"missing:{ad_id}", "广告系列", "pause",
                    trigger_type, "升级关闭系列失败：缺少系列ID，本地快照也未能补齐",
                    status="failed", error_msg=campaign_error, operator="system")

    # 全部失败
    _send_tg(
        f"🆘 <b>Mira 严重告警</b>\n"
        f"{account_line}\n"
        f"{ad_line}\n"
        f"{ad_id_line}\n"
        f"{adset_id_line}\n"
        f"{campaign_id_line}\n"
        f"广告/广告组/系列均关闭失败！\n"
        f"广告失败：{_tg_escape(err_msg)}\n"
        f"广告组失败：{_tg_escape(adset_error or '未返回原因')}\n"
        f"系列失败：{_tg_escape(campaign_error or '未返回原因')}\n"
        f"请立即手动处理！",
        act_id=act_id,
        event_type="guard",
    )
    return "campaign", "all_failed"




def _get_ad_kpi_meta(act_id: str, ad_id: str) -> dict:
    """从 kpi_configs 获取广告的 ad_type 标签"""
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT ad_type, objective, optimization_goal, destination_type
               FROM kpi_configs
               WHERE act_id=? AND target_id=? AND level='ad'
               LIMIT 1""",
            (act_id, ad_id)
        ).fetchone()
        if row:
            return dict(row)
        return {}
    finally:
        conn.close()


def _match_kpi_filter(kpi_filter: str, ad_kpi_meta: dict) -> bool:
    """
    判断广告是否匹配规则的 kpi_filter。
    kpi_filter 存储 ad_type 标签值（如 messenger/purchase/leads），
    多个用逗号分隔。
    """
    if not kpi_filter:
        return True
    ad_type = (ad_kpi_meta.get("ad_type") or "").lower().strip()
    filters = [f.strip().lower() for f in kpi_filter.split(",") if f.strip()]
    if not filters:
        return True
    return ad_type in filters


# ── DB 驱动别名缓存 + 转化计算审计 v3.4.0 ──────────────────────────────
_KPI_ALIAS_MAP_DB = {}
_KPI_ALIAS_MAP_DB_TIME = 0
_KPI_ALIAS_MAP_DB_TTL = 300

_CONVERSION_KEYWORDS = [
    'purchase', 'lead', 'contact', 'conversion',
    'add_to_cart', 'checkout', 'subscribe', 'registration',
    'omni_', 'onsite_', 'offsite_', 'web_', 'fb_pixel'
]


_POOR_FALLBACK_TYPES = {
    'omni_view_content', 'omni_landing_page_view',
    'onsite_web_view_content', 'onsite_web_app_view_content',
    'view_content', 'landing_page_view',
    'link_click', 'page_engagement', 'post_engagement',
    'offsite_content_view_add_meta_leads',
    'onsite_conversion.post_net_like', 'onsite_conversion.post_net_comment',
    'onsite_conversion.post_net_save', 'onsite_conversion.post_save',
    'post_reaction', 'post_interaction_gross', 'post_interaction_net',
}

def _load_alias_cache():
    """从DB kpi_alias_map 加载别名缓存，300s TTL"""
    global _KPI_ALIAS_MAP_DB, _KPI_ALIAS_MAP_DB_TIME
    now = time.time()
    if now - _KPI_ALIAS_MAP_DB_TIME < _KPI_ALIAS_MAP_DB_TTL and _KPI_ALIAS_MAP_DB:
        return
    try:
        conn = get_conn()
        _KPI_ALIAS_MAP_DB['standard'] = {}
        _KPI_ALIAS_MAP_DB['fallback'] = {}
        _KPI_ALIAS_MAP_DB['all_types'] = set()
        rows = conn.execute(
            "SELECT kpi_type, fb_action_type, is_standard FROM kpi_alias_map"
        ).fetchall()
        for r in rows:
            kt, fat, is_std = r['kpi_type'], r['fb_action_type'], r['is_standard']
            _KPI_ALIAS_MAP_DB['all_types'].add(fat)
            if is_std == 1:
                _KPI_ALIAS_MAP_DB['standard'].setdefault(kt, []).append(fat)
            else:
                _KPI_ALIAS_MAP_DB['fallback'].setdefault(kt, []).append(fat)
        # 补充kpi_label_map中的字段到已知类型集合
        for r in conn.execute("SELECT DISTINCT kpi_field FROM kpi_label_map").fetchall():
            _KPI_ALIAS_MAP_DB['all_types'].add(r['kpi_field'])
        conn.close()
        _KPI_ALIAS_MAP_DB_TIME = now
    except Exception as e:
        logger.warning(f"KPI别名缓存加载失败（非致命）: {e}")

_KPI_UI_ALIAS_PRIORITY = {
    "purchase": ["offsite_conversion.fb_pixel_purchase", "purchase"],
    "offsite_conversion.fb_pixel_purchase": ["offsite_conversion.fb_pixel_purchase", "purchase"],
    "leads": ["onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead", "lead"],
    "lead": ["onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead", "lead"],
    "offsite_conversion.fb_pixel_lead": ["offsite_conversion.fb_pixel_lead", "lead"],
    "contact": ["offsite_conversion.fb_pixel_contact", "contact"],
    "offsite_conversion.fb_pixel_contact": ["offsite_conversion.fb_pixel_contact", "contact"],
    "messenger": [
        "onsite_conversion.messaging_conversation_started_7d",
        "onsite_conversion.messaging_first_reply",
    ],
    "traffic": ["link_click", "landing_page_view"],
}


def _sort_kpi_aliases_for_ui(kpi_field: str, aliases: list) -> list:
    field = (kpi_field or "").strip()
    priority = _KPI_UI_ALIAS_PRIORITY.get(field, [])
    result = []
    for item in list(priority) + list(aliases or []):
        if item and item not in result:
            result.append(item)
    return result or ([field] if field else [])


def _first_action_value_by_alias(actions: list, aliases: list) -> tuple[float, Optional[str]]:
    for alias in aliases or []:
        for action in actions or []:
            if action.get("action_type") == alias:
                try:
                    return float(action.get("value", 0) or 0), alias
                except (TypeError, ValueError):
                    return 0.0, alias
    return 0.0, None


def _get_kpi_aliases(kpi_field: str) -> list:
    """获取标准别名列表（含自身），DB优先"""
    _load_alias_cache()
    std = _KPI_ALIAS_MAP_DB.get('standard', {})
    if kpi_field in std:
        return _sort_kpi_aliases_for_ui(kpi_field, std[kpi_field])
    for kt, aliases in std.items():
        if kpi_field in aliases:
            return _sort_kpi_aliases_for_ui(kt, aliases)
    return _sort_kpi_aliases_for_ui(kpi_field, [kpi_field])


def _get_kpi_fallback_aliases(kpi_field: str) -> list:
    """获取兜底别名列表（is_standard=0）"""
    _load_alias_cache()
    fb = _KPI_ALIAS_MAP_DB.get('fallback', {})
    if kpi_field in fb:
        return fb[kpi_field]
    for kt, aliases in _KPI_ALIAS_MAP_DB.get('standard', {}).items():
        if kpi_field in aliases:
            return fb.get(kt, [])
    return []


def _is_conversion_related(action_type: str) -> bool:
    """判断action_type是否与转化相关"""
    al = action_type.lower()
    return any(kw in al for kw in _CONVERSION_KEYWORDS)


def _detect_unknown_action_types(actions_raw: list) -> list:
    """返回actions中不在kpi_alias_map/kpi_label_map的转化相关action_type"""
    _load_alias_cache()
    known = _KPI_ALIAS_MAP_DB.get('all_types', set())
    return [
        a['action_type'] for a in (actions_raw or [])
        if a.get('action_type') and a['action_type'] not in known
        and _is_conversion_related(a['action_type'])
    ]


def _calc_conversions_with_audit(actions_raw: list, kpi_field: str, spend: float, ad_id: str) -> dict:
    """
    计算转化数 + 审计信息（DB驱动别名匹配）
    返回: {conversions, matched_action, is_fallback, unknown_types, reason}
    """
    result = {'conversions': 0.0, 'matched_action': None, 'is_fallback': False, 'unknown_types': [], 'reason': None}
    if not actions_raw:
        result['reason'] = 'no_actions'
        return result

    result['unknown_types'] = _detect_unknown_action_types(actions_raw)

    # 标准别名匹配
    value, matched = _first_action_value_by_alias(actions_raw, _get_kpi_aliases(kpi_field))
    if matched:
        result['conversions'] = value
        result['matched_action'] = matched

    # 默认严格贴近 Meta UI 选定目标：只统计标准目标字段。
    # 兜底别名容易把同一成效的不同口径或上游动作算进去，造成 UI 1 个、系统 7 个这类偏差。
    if result['conversions'] == 0:
        has_purchase = any('purchase' in (a.get('action_type', '')).lower() for a in actions_raw)
        fallback_aliases = _get_kpi_fallback_aliases(kpi_field)
        allow_fallback = _get_setting("kpi_allow_fallback_alias_count", "0") == "1"
        if fallback_aliases and allow_fallback:
            for a in sorted(actions_raw, key=lambda x: fallback_aliases.index(x.get('action_type')) if x.get('action_type') in fallback_aliases else len(fallback_aliases)):
                if a.get('action_type') in fallback_aliases:
                    result['conversions'] = float(a.get('value', 0))
                    result['matched_action'] = a['action_type']
                    result['is_fallback'] = True
                    logger.info(f"转化兜底: {ad_id} kpi={kpi_field} fallback={a['action_type']}={result['conversions']}")
                    # 劣质回退检测：浏览/互动类事件不能算转化
                    if result['matched_action'] in _POOR_FALLBACK_TYPES:
                        logger.warning(f"低质量 fallback: {ad_id} kpi={kpi_field} matched={result['matched_action']}={result['conversions']} 归零")
                        result['conversions'] = 0.0
                        result['matched_action'] = None
                        result['is_fallback'] = False
                        result['reason'] = 'poor_fallback'
                    break

        if result['conversions'] == 0:
            if fallback_aliases and not allow_fallback:
                result['reason'] = f'fallback_alias_not_counted: {fallback_aliases[:5]}'
            elif has_purchase and kpi_field not in ('purchase', 'offsite_conversion.fb_pixel_purchase'):
                result['reason'] = f'kpi_mismatch: kpi={kpi_field} but ad has purchase events'
            elif result['unknown_types']:
                result['reason'] = f'unmapped_types: {result["unknown_types"]}'
            else:
                result['reason'] = 'no_matching_events'

    return result


def _record_unknown_action_type(action_type: str, ad_id: str):
    """将未知action_type记录到kpi_unknown_types表"""
    try:
        conn = get_conn()
        existing = conn.execute(
            "SELECT id, seen_count, sample_ads FROM kpi_unknown_types WHERE action_type=?"
        ).fetchone()
        if existing:
            sample = json.loads(existing['sample_ads'] or '[]')
            if ad_id not in sample:
                sample.append(ad_id)
                if len(sample) > 10:
                    sample = sample[-10:]
            conn.execute(
                "UPDATE kpi_unknown_types SET last_seen=datetime('now'), seen_count=?, sample_ads=? WHERE id=?",
                (existing['seen_count'] + 1, json.dumps(sample), existing['id'])
            )
        else:
            conn.execute(
                "INSERT INTO kpi_unknown_types (action_type, first_seen, last_seen, seen_count, sample_ads) "
                "VALUES (?, datetime('now'), datetime('now'), 1, ?)",
                (action_type, json.dumps([ad_id]))
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"记录未知action_type失败（非致命）: {e}")


def _cross_validate_kpi(ad_id: str, kpi_field: str, actions_raw: list, spend: float):
    """交叉验证: 检查kpi_field是否真实存在于FB actions[]中"""
    if not actions_raw:
        return
    action_types = {a.get('action_type', '') for a in actions_raw}
    if kpi_field in action_types:
        return
    if any(a in action_types for a in _get_kpi_aliases(kpi_field)):
        return
    logger.warning(f"KPI不对齐: ad={ad_id} kpi={kpi_field} spend={spend:.2f} "
                   f"但actions中无匹配事件. types={list(action_types)[:10]}")



# ── 过程指标保护设置（缓存60秒）────────────────────────────────────────────
_ENGAGEMENT_SETTINGS_CACHE = {}
_ENGAGEMENT_SETTINGS_TS = 0

def _get_engagement_settings() -> dict:
    global _ENGAGEMENT_SETTINGS_CACHE, _ENGAGEMENT_SETTINGS_TS
    import time as _time
    now = _time.time()
    if _ENGAGEMENT_SETTINGS_CACHE and (now - _ENGAGEMENT_SETTINGS_TS) < 60:
        return _ENGAGEMENT_SETTINGS_CACHE
    conn = get_conn()
    keys = [
        "guard_engagement_enabled", "guard_engagement_ctr_pct",
        "guard_engagement_clicks", "guard_engagement_cpm_max",
        "guard_engagement_reach", "guard_engagement_unique_ctr_pct",
        "guard_engagement_signals_relax", "guard_engagement_signals_cut",
        "guard_engagement_relax_mult", "guard_engagement_cut_pct",
        "guard_learning_protect_hours",
    ]
    placeholders = ",".join(["?"] * len(keys))
    rows = conn.execute(
        f"SELECT key, value FROM settings WHERE key IN ({placeholders})",
        keys,
    ).fetchall()
    conn.close()
    row_map = {r["key"]: (r["value"] or "") for r in rows}
    result = {
        "enabled": row_map.get("guard_engagement_enabled", "1") == "1",
        "ctr_pct": float(row_map.get("guard_engagement_ctr_pct", "1.5")),
        "clicks": int(float(row_map.get("guard_engagement_clicks", "50"))),
        "cpm_max": float(row_map.get("guard_engagement_cpm_max", "15")),
        "reach": int(float(row_map.get("guard_engagement_reach", "2000"))),
        "unique_ctr_pct": float(row_map.get("guard_engagement_unique_ctr_pct", "60")),
        "signals_relax": int(float(row_map.get("guard_engagement_signals_relax", "3"))),
        "signals_cut": int(float(row_map.get("guard_engagement_signals_cut", "1"))),
        "relax_mult": float(row_map.get("guard_engagement_relax_mult", "3.0")),
        "cut_pct": float(row_map.get("guard_engagement_cut_pct", "0.5")),
        "learning_hours": float(row_map.get("guard_learning_protect_hours", "0")),
    }
    _ENGAGEMENT_SETTINGS_CACHE = result
    _ENGAGEMENT_SETTINGS_TS = now
    return result

class GuardEngine:
    """广告巡检引擎主类 v1.1.0"""

    def __init__(self):
        self.dry_run = _is_dry_run()
        self.default_bleed_abs = float(_get_setting("default_bleed_abs", "20"))
        self.default_cpa_ratio = float(_get_setting("default_cpa_ratio", "1.3"))
        self.learning_protect = _get_setting("learning_phase_protect", "1") == "1"

    def run_all(self, operator_uid=None):
        _ensure_team_guard_schema()
        _ensure_user_guard_schema()
        if _get_setting("inspect_enabled", "1") != "1":
            logger.info("自动巡检已关闭（inspect_enabled=0），跳过")
        else:
            conn = get_conn()
            accounts = conn.execute(
                """SELECT a.*,
                          COALESCE(tm.mirror_enabled, 0) AS team_mirror_enabled,
                          COALESCE(tm.sentinel_enabled, 0) AS team_sentinel_enabled,
                          COALESCE(tm.heartbeat_enabled, 0) AS team_heartbeat_enabled,
                          COALESCE(tm.warmup_enabled, 0) AS team_warmup_enabled,
                          COALESCE(ou.mirror_enabled, 0) AS owner_mirror_enabled,
                          COALESCE(ou.sentinel_enabled, 0) AS owner_sentinel_enabled,
                          COALESCE(ou.heartbeat_enabled, 0) AS owner_heartbeat_enabled,
                          COALESCE(ou.warmup_enabled, 0) AS owner_warmup_enabled
                   FROM accounts a
                   LEFT JOIN teams tm ON tm.id=a.team_id
                   LEFT JOIN users ou ON ou.id=a.owner_user_id AND COALESCE(ou.is_active, 1)=1
                   WHERE a.enabled=1 AND a.account_status NOT IN (3, 7, 9)
                 AND (a.owner_user_id=? OR ? IS NULL)"""
            , (operator_uid, operator_uid)).fetchall()
            conn.close()
            for acc in accounts:
                try:
                    self.inspect_account(dict(acc))
                except Exception as e:
                    logger.error(f"账户 {acc['act_id']} 巡检异常: {e}")

        # ── 镜像巡逻：保护未开启巡检但存活(enabled=0)的账户 ──────────────────────
        global_mirror = _get_setting("mirror_enabled", "0")
        has_team_mirror = _any_team_guard_enabled("mirror_enabled")
        has_owner_mirror = _any_owner_guard_enabled("mirror_enabled")
        if global_mirror == "1" or has_team_mirror or has_owner_mirror:
            conn = get_conn()
            mirror_only = conn.execute(
                """SELECT a.*,
                          COALESCE(tm.mirror_enabled, 0) AS team_mirror_enabled,
                          COALESCE(ou.mirror_enabled, 0) AS owner_mirror_enabled
                   FROM accounts a
                   LEFT JOIN teams tm ON tm.id=a.team_id
                   LEFT JOIN users ou ON ou.id=a.owner_user_id AND COALESCE(ou.is_active, 1)=1
                   WHERE a.enabled=0
                     AND a.account_status NOT IN (3, 7, 9)
                     AND (?='1' OR COALESCE(a.mirror_enabled, 0)=1 OR COALESCE(tm.mirror_enabled, 0)=1 OR COALESCE(ou.mirror_enabled, 0)=1)""",
                (global_mirror,)
            ).fetchall()
            conn.close()
            patrol_results = []
            for acc in mirror_only:
                try:
                    result = self._mirror_patrol(dict(acc))
                    if result:
                        patrol_results.append(result)
                except Exception as e:
                    logger.error(f"账户 {acc['act_id']} 镜像巡逻异常: {e}")
                    patrol_results.append({
                        "act_id": acc.get("act_id", "?"),
                        "account_name": acc.get("name", acc.get("act_id", "?")),
                        "team_id": acc.get("team_id"),
                        "status": "exception",
                        "error": str(e),
                        "review_pending": [],
                        "closures": []
                    })
            # Build and send ONE aggregated TG after the entire patrol cycle
            if patrol_results:
                has_actions = any(
                    r.get("closures") or r.get("review_pending")
                    for r in patrol_results
                )
                if has_actions:
                    by_team = {}
                    for item in patrol_results:
                        by_team.setdefault(item.get("team_id"), []).append(item)
                    for team_id, items in by_team.items():
                        _send_tg(_build_mirror_patrol_summary(items), team_id=team_id, event_type="mirror", include_owner=False)

    def _evaluate_engagement_signals(self, agg: dict) -> tuple[int, dict]:
        """Count positive engagement signals. Returns (count, detail_dict)."""
        s = _get_engagement_settings()
        if not s["enabled"]:
            return 0, {"enabled": False}
        signals = 0
        detail = {}
        ctr_pct = float(agg.get("ctr") or 0)
        if ctr_pct >= s["ctr_pct"]:
            signals += 1
            detail["ctr"] = True
        else:
            detail["ctr"] = False
        clicks = int(agg.get("clicks") or 0)
        if clicks >= s["clicks"]:
            signals += 1
            detail["clicks"] = True
        else:
            detail["clicks"] = False
        cpm = float(agg.get("cpm") or 0)
        if 0 < cpm <= s["cpm_max"]:
            signals += 1
            detail["cpm"] = True
        else:
            detail["cpm"] = False
        reach = int(agg.get("reach") or 0)
        if reach >= s["reach"]:
            signals += 1
            detail["reach"] = True
        else:
            detail["reach"] = False
        uctr = float(agg.get("unique_ctr_pct") or 0)
        if uctr >= s["unique_ctr_pct"]:
            signals += 1
            detail["unique_ctr"] = True
        else:
            detail["unique_ctr"] = False
        return signals, detail

    def inspect_account(self, account: dict):
        act_id = account["act_id"]
        token = _get_token_for_account(account)
        if not token:
            note_account_read_failure(act_id, "no_read_token", status="no_read_token")
            logger.warning(f"账户 {act_id} 无有效Token，跳过巡检")
            return

        logger.info(f"开始巡检账户: {act_id}")
        try:
            data = _fb_get(
                f"{act_id}/ads", token,
                {"fields": FB_AD_FIELDS, "effective_status": '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED","PENDING_REVIEW","PENDING_BILLING_INFO"]', "limit": 200},
                paginate=True
            )
        except Exception as e:
            logger.error(f"拉取广告列表失败 {act_id}: {e}")
            note_account_read_failure(act_id, e)
            _log_action(act_id, "account", act_id, account.get("name", ""),
                        "inspect", "system", f"API拉取失败: {e}",
                        status="failed", error_msg=str(e))
            if (
                _get_setting("mirror_enabled", "0") == "1"
                or account.get("mirror_enabled", 0) == 1
                or _account_team_guard_enabled(account, "mirror_enabled")
                or _account_owner_guard_enabled(account, "mirror_enabled")
            ):
                try:
                    mirror_result = self._mirror_patrol(account)
                    if mirror_result and (mirror_result.get("closures") or mirror_result.get("review_pending")):
                        _send_tg(_build_mirror_patrol_summary([mirror_result]), act_id=act_id, event_type="mirror")
                except Exception as mirror_err:
                    logger.error(f"[Mirror] 巡检字段失败后的兜底镜像巡逻也失败 {act_id}: {mirror_err}")
            return

        ads = data.get("data", [])
        note_account_read_success(act_id)
        logger.info(f"账户 {act_id} 活跃广告数: {len(ads)}")

        # ── 镜像模式：暂停不在白名单中的未授权广告 ──────────────────────────────
        global_mirror = _get_setting("mirror_enabled", "0")
        account_mirror = account.get("mirror_enabled", 0)
        team_mirror = _account_team_guard_enabled(account, "mirror_enabled")
        owner_mirror = _account_owner_guard_enabled(account, "mirror_enabled")
        if global_mirror == "1" or account_mirror == 1 or team_mirror or owner_mirror:
            mirror_events = []  # Collect for aggregated TG notification per account
            mirrored_ids = _load_mirror_snapshot(act_id)
            if mirrored_ids:
                _cannot_spend = {"PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED",
                                 "DELETED", "ARCHIVED", "DISAPPROVED", "WITH_ISSUES"}
                _review_status = {"PENDING_REVIEW", "IN_REVIEW", "PENDING_BILLING_INFO", "PREAPPROVED"}
                unauthorized = []
                review_pending = []
                for ad in ads:
                    ad_id = ad["id"]
                    eff = ad.get("effective_status", "")
                    if eff in _cannot_spend:
                        continue
                    if ad_id in mirrored_ids:
                        continue
                    if eff in _review_status:
                        review_pending.append(ad)
                        continue
                    unauthorized.append(ad)
                # 审核中的未授权广告：无法暂停，但发送TG告警
                if review_pending:
                    rev_campaigns = {}
                    for ad in review_pending:
                        cid = ad.get("campaign_id", "")
                        if not cid:
                            cid = f"nocamp_{ad['id']}"
                        if cid not in rev_campaigns:
                            rev_campaigns[cid] = []
                        rev_campaigns[cid].append(ad)
                    for cid, rads in rev_campaigns.items():
                        ad_names = [a.get("name", a["id"]) for a in rads]
                        ad_ids = [a["id"] for a in rads]
                        statuses = list({rads[0].get("effective_status", "REVIEW")})
                        for a in rads:
                            logger.warning(
                                f"[Mirror] 未授权广告 {a.get('name', a['id'])} 处于审核状态({a.get('effective_status')})，"
                                f"无法暂停，待审核通过后将自动关闭系列 {cid}"
                            )
                            _log_action(act_id, "ad", a["id"], f"[镜像] {a.get('name', a['id'])}",
                                        "warn", "mirror_mode",
                                        f"镜像模式：广告不在快照白名单且处于审核状态({a.get('effective_status')})，无法暂停",
                                        old_value={"effective_status": a.get("effective_status")},
                                        new_value={"action": "monitoring"},
                                        status="warning", operator="system")
                        mirror_events.append({
                            "type": "review",
                            "campaign_id": cid,
                            "ad_names": ad_names,
                            "ad_ids": ad_ids,
                            "statuses": list({a.get("effective_status", "REVIEW") for a in rads})
                        })
                # 按系列(campaign)去重，同一系列只关一次
                campaigns_to_pause = {}
                for ad in unauthorized:
                    cid = ad.get("campaign_id", "")
                    if not cid:
                        cid = f"nocamp_{ad['id']}"
                    if cid not in campaigns_to_pause:
                        campaigns_to_pause[cid] = []
                    campaigns_to_pause[cid].append(ad)

                for cid, camp_ads in campaigns_to_pause.items():
                    ad_names = [a.get("name", a["id"]) for a in camp_ads]
                    is_nocamp = cid.startswith("nocamp_")
                    if is_nocamp:
                        # 无系列的广告：直接暂停广告本身
                        ad = camp_ads[0]
                        logger.warning(f"[Mirror] 未授权广告 {ad['id']} ({ad.get('name')})，无系列，直接暂停广告")
                        if self.dry_run:
                            _log_action(act_id, "ad", ad["id"], ad.get("name", ad["id"]),
                                        "pause", "mirror_mode",
                                        f"[模拟] 镜像模式：广告不在快照白名单，直接暂停",
                                        old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                        status="success", operator="system")
                        else:
                            ok, err_msg = _fb_post(ad["id"], token, {"status": "PAUSED"})
                            action_status = "success" if (ok and _verify_status(ad["id"], token, "PAUSED")) else "failed"
                            _log_action(act_id, "ad", ad["id"], ad.get("name", ad["id"]),
                                        "pause", "mirror_mode",
                                        f"镜像模式：广告不在快照白名单，直接暂停",
                                        old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                        status=action_status, error_msg=(err_msg if not ok else None),
                                        operator="system")
                            mirror_events.append({
                                "type": "pause_ad",
                                "status": action_status,
                                "ad_id": ad["id"],
                                "ad_names": [ad.get("name", ad["id"])],
                                "error": err_msg if action_status == "failed" else None
                            })
                    else:
                        logger.warning(f"[Mirror] 未授权广告 ({', '.join(ad_names)})，直接关闭系列 {cid}")
                        if self.dry_run:
                            for ad in camp_ads:
                                _log_action(act_id, "campaign", cid, f"[镜像] {ad.get('name', ad['id'])}的系列",
                                            "pause", "mirror_mode",
                                            f"[模拟] 镜像模式：广告不在快照白名单，直接关闭系列",
                                            old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                            status="success", operator="system")
                            mirror_events.append({
                                "type": "close_campaign_dry",
                                "campaign_id": cid,
                                "ad_names": ad_names
                            })
                        else:
                            ok, err_msg = _fb_post(cid, token, {"status": "PAUSED"})
                            if ok:
                                time.sleep(2)
                                verified = _verify_status(cid, token, "PAUSED")
                                action_status = "success" if verified else "failed"
                                if not verified:
                                    err_msg = "核验失败：系列effective_status未变为不可投放状态"
                            else:
                                action_status = "failed"

                            for ad in camp_ads:
                                _log_action(act_id, "campaign", cid, f"[镜像] {ad.get('name', ad['id'])}的系列",
                                            "pause", "mirror_mode",
                                            f"镜像模式：广告不在快照白名单，直接关闭系列",
                                            old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                            status=action_status,
                                            error_msg=err_msg if action_status == "failed" else None,
                                            operator="system")

                            if action_status == "success":
                                mirror_events.append({
                                    "type": "close_campaign",
                                    "status": "success",
                                    "campaign_id": cid,
                                    "ad_names": ad_names
                                })
                            else:
                                mirror_events.append({
                                    "type": "close_campaign",
                                    "status": "failed",
                                    "campaign_id": cid,
                                    "ad_names": ad_names,
                                    "error": err_msg
                                })

                # Send aggregated TG notification for this account's mirror actions
                if mirror_events:
                    _send_tg(_build_mirror_account_summary(act_id, account.get("name", act_id), mirror_events), act_id=act_id, event_type="mirror")

                if unauthorized:
                    paused_ids = {a["id"] for a in unauthorized}
                    ads = [a for a in ads if a["id"] not in paused_ids]
                    logger.info(f"[Mirror] {act_id} 本次拦截 {len(unauthorized)} 条未授权广告，涉及 {len(campaigns_to_pause)} 个系列")
            else:
                # 快照为空，跳过巡检（需手动开启镜像模式采集初始快照）
                if ads:
                    logger.warning(f"[Mirror] {act_id} 快照为空，跳过巡检（需手动开启镜像模式采集初始快照）")
                else:
                    logger.info(f"[Mirror] {act_id} 当前无活跃广告，跳过快照捕获")

        # ── 自动 KPI 预配：发现无 KPI 配置或配置非法的广告时触发扫描 ──────────────
        try:
            conn = get_conn()
            existing_kpi_ids = set(
                row[0] for row in conn.execute(
                    "SELECT target_id FROM kpi_configs WHERE act_id=? AND level='ad' AND enabled=1",
                    (act_id,)
                ).fetchall()
            )
            # 检查是否有非法/不匹配 KPI 字段需要重新推断（自愈触发）
            invalid_kpi_ids = set()
            mismatched_kpi_ids = set()
            if existing_kpi_ids:
                from services.kpi_resolver import _is_valid_kpi_field, _get_custom_event_rule, _CUSTOM_EVENT_RULES
                kpi_rows = conn.execute(
                    "SELECT target_id, kpi_field FROM kpi_configs WHERE act_id=? AND level='ad' AND enabled=1 AND source!='manual'",
                    (act_id,)
                ).fetchall()
                stored_map = {r["target_id"]: r["kpi_field"] for r in kpi_rows}
                for row in kpi_rows:
                    if not _is_valid_kpi_field(row["kpi_field"]):
                        invalid_kpi_ids.add(row["target_id"])
                # Check custom_event_type vs stored field mismatch
                for ad in ads:
                    ad_id = ad["id"]
                    if ad_id in stored_map:
                        adset_d = ad.get("adset", {})
                        if isinstance(adset_d, dict):
                            ce = (adset_d.get("custom_event_type") or "").upper()
                            expected = None
                            try:
                                ce_r = _get_custom_event_rule(ce)
                                if ce_r:
                                    expected = ce_r[0]
                            except Exception:
                                pass
                            if not expected and ce in _CUSTOM_EVENT_RULES:
                                expected = _CUSTOM_EVENT_RULES[ce][0]
                            if expected and stored_map.get(ad_id) != expected:
                                    mismatched_kpi_ids.add(ad_id)
            conn.close()

            ad_ids_active = {ad["id"] for ad in ads}
            need_scan = (ad_ids_active - existing_kpi_ids) | (invalid_kpi_ids & ad_ids_active) | (mismatched_kpi_ids & ad_ids_active)
            if mismatched_kpi_ids:
                logger.info(f"KPI预配: {act_id} {len(mismatched_kpi_ids)} ads custom_event_type mismatch, triggering rescan")
            if need_scan:
                new_count = len(ad_ids_active - existing_kpi_ids)
                invalid_count = len(invalid_kpi_ids & ad_ids_active)
                logger.info(f"账户 {act_id} {len(need_scan)} 条广告需要 KPI 预配（{new_count} 无配置 + {invalid_count} 非法字段）")
                from services.kpi_resolver import scan_and_preset_kpi
                result = scan_and_preset_kpi(act_id, token)
                logger.info(
                    f"KPI 自动预配完成 {act_id}: "
                    f"新建={result.get('created',0)}, "
                    f"更新={result.get('updated',0)}, "
                    f"跳过={result.get('skipped',0)}"
                )
        except Exception as e:
            logger.warning(f"KPI 预配异常（非致命）{act_id}: {e}")
        # ─────────────────────────────────────────────────────────────────────

        conn = get_conn()
        _ensure_rule_scope_schema()
        # Account-specific rules and owner-level rules are both effective here.
        # target_id="__global__" still means "all objects inside this account".
        rules = _load_rules_for_account(conn, "guard_rules", account)
        scale_rules = _load_rules_for_account(conn, "scale_rules", account)
        conn.close()

        account_rules, object_rules = self._split_guard_rules(act_id, rules)
        ad_metrics = []
        for ad in ads:
            try:
                metrics = self._inspect_ad(account, ad, token, object_rules, scale_rules)
                if metrics:
                    ad_metrics.append(metrics)
            except Exception as e:
                logger.error(f"广告 {ad.get('id')} 巡检异常: {e}")

        self._inspect_account_rules(account, token, account_rules, ad_metrics)

    def _default_account_bleed_rule(self, act_id: str) -> dict:
        return {
            "id": "default_account_bleed",
            "act_id": act_id,
            "rule_name": "默认账户空成效止血",
            "level": "account",
            "target_id": act_id,
            "rule_type": "bleed_abs",
            "param_value": self.default_bleed_abs,
            "param_ratio": None,
            "param_days": None,
            "action": "pause",
            "action_value": None,
            "enabled": 1,
            "silent_start": None,
            "silent_end": None,
            "note": "系统兜底：账户未配置账户级止损时自动保护",
            "kpi_filter": None,
        }

    def _split_guard_rules(self, act_id: str, rules: list) -> tuple[list, list]:
        account_rules, object_rules = [], []
        for rule in rules or []:
            target = str(rule.get("target_id") or "")
            level = str(rule.get("level") or "").lower()
            act = str(rule.get("act_id") or "")
            if act == "__owner__":
                object_rules.append(rule)
            elif level == "account" or target in ("", "__global__", act_id):
                account_rules.append(rule)
            else:
                object_rules.append(rule)
        if _get_setting("default_account_bleed_enabled", "0") == "1":
            has_bleed = any(str(r.get("rule_type")) == "bleed_abs" for r in account_rules)
            if not has_bleed:
                account_rules.append(self._default_account_bleed_rule(act_id))
        return account_rules, object_rules

    def _metric_matches_rule_filter(self, act_id: str, metric: dict, kpi_filter: str | None) -> bool:
        if not kpi_filter:
            return True
        try:
            meta = _get_ad_kpi_meta(act_id, metric.get("ad_id", ""))
            if _match_kpi_filter(kpi_filter, meta):
                return True
        except Exception:
            pass
        field = str(metric.get("kpi_field") or "").lower()
        value = str(kpi_filter or "").lower()
        if value in ("purchase", "purchases"):
            return "purchase" in field
        if value in ("lead", "leads"):
            return "lead" in field
        if value in ("messenger", "messaging"):
            return any(t in field for t in ("messaging", "messenger", "conversation"))
        if value in ("engagement",):
            return "engagement" in field or "like" in field
        if value in ("traffic",):
            return "click" in field or "landing_page_view" in field
        if value in ("contact",):
            return "contact" in field
        return True

    def _aggregate_account_metrics(self, metrics: list[dict]) -> dict:
        spend = sum(float(m.get("spend") or 0) for m in metrics)
        spend_raw = sum(float(m.get("spend_raw") or 0) for m in metrics)
        conversions = sum(float(m.get("conversions") or 0) for m in metrics)
        broader_conv = sum(float(m.get("broader_conv") or 0) for m in metrics)
        impressions = sum(int(m.get("impressions") or 0) for m in metrics)
        reach = sum(int(m.get("reach") or 0) for m in metrics)
        clicks = sum(int(m.get("clicks") or 0) for m in metrics)
        unique_clicks = sum(int(m.get("unique_clicks") or 0) for m in metrics)
        ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
        cpa = (spend / conversions) if conversions > 0 else None
        campaigns = []
        adsets = []
        ads = []
        for m in sorted(metrics, key=lambda x: float(x.get("spend") or 0), reverse=True):
            if m.get("campaign_id") and m["campaign_id"] not in campaigns:
                campaigns.append(m["campaign_id"])
            if m.get("adset_id") and m["adset_id"] not in adsets:
                adsets.append(m["adset_id"])
            if m.get("ad_id") and m["ad_id"] not in ads:
                ads.append(m["ad_id"])
        cpm = (spend / impressions * 1000) if impressions > 0 else 0.0
        unique_ctr_pct = (unique_clicks / impressions * 100) if impressions > 0 else 0.0
        objectives = set()
        oldest_created = None
        ad_details = []
        for m in sorted(metrics, key=lambda x: float(x.get("spend") or 0), reverse=True):
            detail = {
                "ad_id": m.get("ad_id", ""),
                "ad_name": m.get("ad_name", ""),
                "adset_id": m.get("adset_id", ""),
                "campaign_id": m.get("campaign_id", ""),
                "spend": float(m.get("spend") or 0),
            }
            ad_details.append(detail)
            obj = m.get("objective", "") or m.get("campaign_objective", "")
            if obj:
                objectives.add(str(obj).upper())
            created = m.get("created_time") or m.get("ad_created_time")
            if created:
                try:
                    from datetime import datetime as _dt
                    ct = _dt.strptime(str(created)[:19], "%Y-%m-%dT%H:%M:%S")
                    if oldest_created is None or ct < oldest_created:
                        oldest_created = ct
                except Exception:
                    pass
        primary_objective = sorted(objectives)[0] if objectives else ""
        hours_since_oldest = None
        if oldest_created:
            from datetime import datetime as _dt_now
            hours_since_oldest = (_dt_now.utcnow() - oldest_created).total_seconds() / 3600

        return {
            "spend": spend,
            "spend_raw": spend_raw,
            "conversions": conversions,
            "broader_conv": broader_conv,
            "impressions": impressions,
            "reach": reach,
            "clicks": clicks,
            "unique_clicks": unique_clicks,
            "ctr": ctr,
            "cpm": cpm,
            "unique_ctr_pct": unique_ctr_pct,
            "cpa": cpa,
            "campaign_ids": campaigns,
            "adset_ids": adsets,
            "ad_ids": ads,
            "ad_details": ad_details,
            "primary_objective": primary_objective,
            "oldest_ad_created_at": str(oldest_created) if oldest_created else None,
            "hours_since_oldest_ad": hours_since_oldest,
        }

    def _inspect_account_rules(self, account: dict, token: str, rules: list, ad_metrics: list[dict]) -> None:
        if not rules or not ad_metrics:
            return
        act_id = account["act_id"]
        currency = (account.get("currency") or "USD").upper().strip()
        for rule in rules:
            if rule.get("enabled", 1) == 0:
                continue
            target = str(rule.get("target_id") or "")
            if target not in ("", "__global__", act_id) and str(rule.get("level") or "").lower() != "account":
                continue
            if _is_silent(rule.get("silent_start"), rule.get("silent_end")):
                continue
            rule_type = str(rule.get("rule_type") or "")
            cooldown_key = f"{act_id}:account"
            if _check_cooldown(cooldown_key, rule_type):
                continue
            selected = [m for m in ad_metrics if self._metric_matches_rule_filter(act_id, m, rule.get("kpi_filter"))]
            if not selected:
                continue
            agg = self._aggregate_account_metrics(selected)
            triggered, reason = self._account_rule_triggered(rule, account, agg, currency)
            if not triggered:
                continue
            _set_cooldown(cooldown_key, rule_type)
            self._execute_account_rule(account, token, rule, agg, reason, selected)

    def _account_rule_triggered(self, rule: dict, account: dict, agg: dict, currency: str) -> tuple[bool, str]:
        rule_type = str(rule.get("rule_type") or "")
        spend = float(agg.get("spend") or 0)
        spend_raw = float(agg.get("spend_raw") or 0)
        conversions = float(agg.get("conversions") or 0)
        broader_conv = float(agg.get("broader_conv") or 0)
        clicks = int(agg.get("clicks") or 0)
        reach = int(agg.get("reach") or 0)
        impressions = int(agg.get("impressions") or 0)
        ctr = float(agg.get("ctr") or 0)
        cur_note = ""
        if currency != "USD":
            cur_note = f" (original {currency} {spend_raw:.2f}, converted to USD)"

        if rule_type == "bleed_abs":
            threshold = float(rule.get("param_value") or self.default_bleed_abs)
            if spend >= threshold and conversions == 0:
                if broader_conv > 0:
                    _log_action(account["act_id"], "account", account["act_id"], account.get("name", account["act_id"]),
                                "bleed_abort", "bleed_abs",
                                f"account KPI conversions=0 but broad conversions={broader_conv}; skip account stop")
                    return False, ""
                return True, f"账户今日消耗 ${spend:.2f}{cur_note} 已超过空成效止血线 ${threshold:.2f}，KPI 转化=0"

        elif rule_type == "cpa_exceed":
            cpa = agg.get("cpa")
            if cpa:
                ratio = float(rule.get("param_ratio") or self.default_cpa_ratio)
                target = rule.get("param_value")
                if target and float(target) > 0 and cpa > float(target) * ratio:
                    return True, f"账户 CPA ${cpa:.2f} 超过 ${float(target):.2f} x {ratio:.2f}"

        elif rule_type == "click_no_conv":
            threshold_clicks = int(rule.get("param_value") or 100)
            if clicks >= threshold_clicks and conversions == 0:
                return True, f"账户点击 {clicks} 已超过 {threshold_clicks}，KPI 转化=0"

        elif rule_type == "low_ctr_no_conv":
            min_spend = float(rule.get("param_value") or 10.0)
            max_ctr = float(rule.get("param_ratio") or 0.5)
            if spend >= min_spend and impressions >= 100 and conversions == 0 and float(ctr or 0) <= max_ctr:
                return True, f"账户消耗 ${spend:.2f} 且 CTR {float(ctr or 0):.2f}% <= {max_ctr:.2f}%，KPI 转化=0"

        elif rule_type == "reach_no_conv":
            threshold_reach = int(rule.get("param_value") or 1000)
            min_spend = float(rule.get("param_ratio") or 10.0)
            if int(reach or 0) >= threshold_reach and spend >= min_spend and conversions == 0:
                return True, f"账户覆盖 {int(reach or 0)} 且消耗 ${spend:.2f}，KPI 转化=0"

        elif rule_type == "budget_burn_fast":
            threshold_abs = float(rule.get("param_value") or 20.0)
            cache_id = f"{account['act_id']}:account"
            try:
                _conn = get_conn()
                cache_row = _conn.execute(
                    "SELECT data FROM inspect_cache WHERE act_id=? AND ad_id=?",
                    (account["act_id"], cache_id),
                ).fetchone()
                last_spend = 0.0
                if cache_row:
                    last_spend = float(json.loads(cache_row["data"]).get("spend", 0))
                _conn.execute(
                    "INSERT OR REPLACE INTO inspect_cache (act_id, ad_id, data, updated_at) VALUES (?,?,?,datetime('now'))",
                    (account["act_id"], cache_id, json.dumps({"spend": spend})),
                )
                _conn.commit()
                _conn.close()
                delta = spend - last_spend
                if delta > 0 and delta >= threshold_abs:
                    return True, f"账户单次巡检消耗增加 ${delta:.2f}，超过 ${threshold_abs:.2f}"
            except Exception as exc:
                logger.warning("account budget_burn_fast cache failed: %s", exc)



        return False, ""

    def _execute_account_rule(self, account: dict, token: str, rule: dict, agg: dict, reason: str, selected_metrics: list = None) -> None:
        act_id = account["act_id"]
        account_name = account.get("name", act_id)
        action = str(rule.get("action") or "pause")
        rule_type = str(rule.get("rule_type") or "")
        rule_name = str(rule.get("rule_name") or _rule_type_labels.get(rule_type, rule_type))
        if action == "alert_only":
            _log_action(act_id, "account", act_id, account_name, "alert", rule_type, reason, operator="system")
            _send_tg(
                f"⚠️ <b>Mira 账户级预警</b>\n"
                f"账户：{_tg_escape(account_name)} ({_tg_code(act_id)})\n"
                f"原因：{_tg_escape(reason)}",
                act_id=act_id,
                event_type="guard",
            )
            return

        ad_list = (selected_metrics or []) if selected_metrics else agg.get("ad_details", [])
        successes, failures = [], []
        for ad_m in ad_list:
            ad_id = ad_m.get("ad_id", "")
            ad_name = ad_m.get("ad_name", ad_id)
            adset_id = ad_m.get("adset_id", "")
            campaign_id = ad_m.get("campaign_id", "")
            if not ad_id:
                continue
            level, status = _pause_with_escalation(
                account, ad_id, adset_id, campaign_id,
                ad_name, token, rule_type,
                f"[账户止损] {reason}", self.dry_run
            )
            if status in ("success", "escalated", "dry_run"):
                successes.append(f"{ad_name}({level})")
            else:
                failures.append(f"{ad_name}: {status}")

    def _inspect_ad(self, account: dict, ad: dict, token: str, rules: list, scale_rules: list = None):
            from services.kpi_resolver import get_kpi_for_ad

            act_id = account["act_id"]
            ad_id = ad["id"]
            ad_name = ad.get("name", ad_id)
            adset_id = ad.get("adset_id", "")
            campaign_id = ad.get("campaign_id", "")
            adset_id, campaign_id = _fill_missing_hierarchy_ids(act_id, ad_id, adset_id, campaign_id)

            # 跳过已经不能花钱的广告（不需要再压制）
            eff_status = ad.get("effective_status", "")
            _cannot_spend = {"PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED", "DELETED", "ARCHIVED", "DISAPPROVED", "WITH_ISSUES"}
            if eff_status in _cannot_spend:
                return

            insights = ad.get("insights", {}).get("data", [])
            if not insights:
                return

            ins = insights[0]
            spend_raw = float(ins.get("spend", 0))  # 账户原始货币金额
            impressions = int(ins.get("impressions", 0))
            reach = int(float(ins.get("reach", 0) or 0))
            clicks = int(ins.get("clicks", 0))
            unique_clicks = int(float(ins.get("unique_clicks", 0) or 0))
            try:
                ctr = float(ins.get("ctr", 0) or 0)
            except (TypeError, ValueError):
                ctr = 0.0
            if not ctr and impressions > 0:
                ctr = (clicks / impressions) * 100
            actions_raw = ins.get("actions", [])
            action_values = ins.get("action_values", [])

            # 获取账户货币（用于止损判断前的货币转换）
            account_currency = (account.get("currency") or "USD").upper().strip()

            # 将 spend 转换为 USD（如果已是 USD 则不变）
            spend = _to_usd_guard(spend_raw, account_currency)

            # 从 ad 响应中提取 campaign objective / adset 元数据
            camp_obj = ""
            camp_data = ad.get("campaign", {})
            if isinstance(camp_data, dict):
                camp_obj = camp_data.get("objective", "")
            adset_data = ad.get("adset", {})
            adset_opt_goal = ""
            adset_dest_type = ""
            adset_custom_event = ""
            if isinstance(adset_data, dict):
                adset_opt_goal = adset_data.get("optimization_goal", "")
                adset_dest_type = adset_data.get("destination_type", "")
                adset_custom_event = adset_data.get("custom_event_type", "")

            # 获取 KPI 配置（v3.3.6: 传入完整 adset 元数据供 KpiResolver 使用）
            kpi_field, kpi_label, kpi_source = get_kpi_for_ad(
                act_id, ad_id, campaign_id,
                campaign_meta={
                    "objective": camp_obj,
                    "optimization_goal": adset_opt_goal,
                    "destination_type": adset_dest_type,
                    "custom_event_type": adset_custom_event,
                    "spend": spend,
                },
                actions=actions_raw,
                adset_id=adset_id
            )

            # ── 转化数计算（DB驱动别名 + 审计日志）v3.4.0 ─────────────────────
            conv_audit = _calc_conversions_with_audit(actions_raw, kpi_field, spend, ad_id)
            conversions = conv_audit['conversions']
            matched_action_type = conv_audit['matched_action']

            # 记录未知action_type到DB
            if conv_audit['unknown_types']:
                logger.warning(f"未知action_type: {act_id}/{ad_id} unknown={conv_audit['unknown_types']}")
                for at in conv_audit['unknown_types']:
                    _record_unknown_action_type(at, ad_id)

            # spend>0 但 conversion=0 时记录原因
            if spend > 0 and conversions == 0 and conv_audit['reason']:
                logger.info(f"转化审计: {act_id}/{ad_id} kpi={kpi_field} spend={spend:.2f} reason={conv_audit['reason']}")

            # v3.3.7: broad conversion check (宽泛匹配，与KPI字段无关)
            broader_conv = 0.0
            _CONV_BROAD = {"purchase", "offsite_conversion.fb_pixel_purchase", "offsite_conversion.purchase",
                          "lead", "offsite_conversion.fb_pixel_lead", "offsite_conversion.lead",
                          "onsite_conversion.lead_grouped", "offsite_conversion.lead_grouped",
                          "contact", "offsite_conversion.fb_pixel_contact",
                          "offsite_conversion.fb_pixel_custom",
                          "offsite_conversion.fb_pixel_add_to_cart", "add_to_cart",
                          "omni_purchase", "web_in_store_purchase"}
            for a in actions_raw:
                if a.get("action_type") in _CONV_BROAD:
                    broader_conv = float(a.get("value", 0))
                    break

            # v3.4.0: 交叉验证 — kpi_field是否真实存在于FB actions[]
            if spend > 0:
                _cross_validate_kpi(ad_id, kpi_field, actions_raw, spend)

            cpa = (spend / conversions) if conversions > 0 else None  # USD CPA

            # 计算 ROAS（revenue 也需转换）
            revenue_raw, _revenue_action_type = _first_action_value_by_alias(
                action_values, _get_kpi_aliases(kpi_field)
            )
            revenue = _to_usd_guard(revenue_raw, account_currency)
            roas = (revenue / spend) if spend > 0 else None

            # 存储快照（存 USD 化后的 spend/cpa，便于跨账户汇总分析）
            self._save_snapshot(act_id, ad_id, adset_id, campaign_id, ad_name,
                                spend, impressions, clicks, conversions, cpa, roas,
                                kpi_field, actions_raw)

            # 获取目标 CPA（单位 USD，广告级 > 广告组级 > Campaign级 > 账户级）
            # 注意：必须在 AI 决策层之前获取，否则 AI 加预算判断会引发 NameError
            target_cpa = self._get_target_cpa(act_id, ad_id, adset_id, campaign_id)

            # ── AI 托管决策层已移除 ──


            # 执行止损规则检查（所有金额均为 USD）
            for rule in rules:
                if rule["target_id"] not in ("__global__", act_id, campaign_id, adset_id, ad_id):
                    continue
                # KPI类型筛选：如果规则设置了 kpi_filter，则只对匹配类型的广告生效
                kpi_filter = rule.get("kpi_filter")
                if kpi_filter:
                    # 从 kpi_configs 获取该广告的 destination_type / objective
                    ad_kpi_cfg = _get_ad_kpi_meta(act_id, ad_id)
                    if not _match_kpi_filter(kpi_filter, ad_kpi_cfg):
                        continue
                if _is_silent(rule.get("silent_start"), rule.get("silent_end")):
                    continue
                if _check_cooldown(ad_id, rule["rule_type"]):
                    continue

                self._check_rule(
                    rule, account, token,
                    ad_id, adset_id, campaign_id, ad_name,
                    spend, conversions, clicks, cpa, roas,
                    target_cpa, kpi_label, impressions,
                    reach=reach, ctr=ctr, unique_clicks=unique_clicks,
                    account_currency=account_currency, spend_raw=spend_raw,
                    broader_conv=broader_conv
                )

            for rule in (scale_rules or []):
                if not adset_id:
                    continue
                cooldown_key = adset_id or ad_id
                rule_key = f"scale:{rule.get('id', rule.get('rule_type', 'unknown'))}"
                if _check_cooldown(cooldown_key, rule_key, cooldown_min=1440):
                    continue
                if (self._has_recent_action(act_id, cooldown_key, "increase_budget", hours=24)
                        or self._has_recent_action(act_id, cooldown_key, "increase_budget_skipped", hours=24)):
                    continue
                if self._check_scale_rule(
                    rule, account, token,
                    ad_id, adset_id, campaign_id, ad_name,
                    spend, conversions, cpa, roas,
                    target_cpa, kpi_label
                ):
                    break

            return {
                "ad_id": ad_id,
                "ad_name": ad_name,
                "adset_id": adset_id,
                "campaign_id": campaign_id,
                "spend": float(spend or 0),
                "spend_raw": float(spend_raw or 0),
                "impressions": int(impressions or 0),
                "reach": int(reach or 0),
                "clicks": int(clicks or 0),
                "unique_clicks": int(unique_clicks or 0),
                "ctr": float(ctr or 0),
                "conversions": float(conversions or 0),
                "broader_conv": float(broader_conv or 0),
                "cpa": cpa,
                "roas": roas,
                "kpi_label": kpi_label,
                "kpi_field": kpi_field,
                "target_cpa": target_cpa,
                "account_currency": account_currency,
                "objective": camp_obj,
                "created_time": ad.get("created_time", ""),
            }

    def _mirror_patrol(self, account: dict):
        """仅执行镜像检查，不做KPI/规则巡检。用于enabled=0但需镜像保护的账户"""
        act_id = account["act_id"]
        patrol_result = {
            "act_id": act_id,
            "account_name": account.get("name", act_id),
            "team_id": account.get("team_id"),
            "status": "ok",
            "review_pending": [],
            "closures": []
        }
        token = _get_token_for_account(account)
        if not token:
            patrol_result["status"] = "no_token"
            return patrol_result

        try:
            data = _fb_get(
                f"{act_id}/ads", token,
                {"fields": MIRROR_AD_FIELDS,
                 "effective_status": '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED","PENDING_REVIEW","PENDING_BILLING_INFO"]',
                 "limit": 200},
                paginate=True
            )
        except Exception as e:
            logger.error(f"[Mirror] 拉取广告列表失败 {act_id}: {e}")
            patrol_result["status"] = "api_error"
            return patrol_result

        ads = data.get("data", [])
        logger.info(f"[Mirror Patrol] 账户 {act_id} 活跃广告数: {len(ads)}")

        mirrored_ids = _load_mirror_snapshot(act_id)
        if not mirrored_ids:
            if ads:
                logger.warning(f"[Mirror Patrol] {act_id} 快照为空，跳过巡检（需手动开启镜像模式采集初始快照）")
            else:
                logger.info(f"[Mirror Patrol] {act_id} 当前无活跃广告，跳过快照捕获")
            patrol_result["status"] = "no_snapshot"
            return patrol_result

        _cannot_spend = {"PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED",
                         "DELETED", "ARCHIVED", "DISAPPROVED", "WITH_ISSUES"}
        _review_status = {"PENDING_REVIEW", "IN_REVIEW", "PENDING_BILLING_INFO", "PREAPPROVED"}
        unauthorized = []
        review_pending = []
        for ad in ads:
            ad_id = ad["id"]
            eff = ad.get("effective_status", "")
            if eff in _cannot_spend:
                continue
            if ad_id in mirrored_ids:
                continue
            if eff in _review_status:
                review_pending.append(ad)
                continue
            unauthorized.append(ad)

        # 审核中的未授权广告：无法暂停，但发送TG告警
        if review_pending:
            rev_campaigns = {}
            for ad in review_pending:
                cid = ad.get("campaign_id", "")
                if not cid:
                    cid = f"nocamp_{ad['id']}"
                if cid not in rev_campaigns:
                    rev_campaigns[cid] = []
                rev_campaigns[cid].append(ad)
            for cid, rads in rev_campaigns.items():
                ad_names = [a.get("name", a["id"]) for a in rads]
                statuses = list({a.get("effective_status", "REVIEW") for a in rads})
                for a in rads:
                    logger.warning(
                        f"[Mirror Patrol] 未授权广告 {a.get('name', a['id'])} 处于审核状态({a.get('effective_status')})，"
                        f"无法暂停，待审核通过后将自动关闭系列 {cid}"
                    )
                    _log_action(act_id, "ad", a["id"], f"[镜像] {a.get('name', a['id'])}",
                                "warn", "mirror_mode",
                                f"镜像巡逻：广告不在快照白名单且处于审核状态({a.get('effective_status')})，无法暂停",
                                old_value={"effective_status": a.get("effective_status")},
                                new_value={"action": "monitoring"},
                                status="warning", operator="system")
                patrol_result["review_pending"].append({
                    "campaign_id": cid,
                    "ad_names": ad_names,
                    "statuses": statuses
                })

        if not unauthorized:
            return patrol_result

        # 按系列去重
        campaigns_to_pause = {}
        for ad in unauthorized:
            cid = ad.get("campaign_id", "")
            if not cid:
                cid = f"nocamp_{ad['id']}"
            if cid not in campaigns_to_pause:
                campaigns_to_pause[cid] = []
            campaigns_to_pause[cid].append(ad)

        for cid, camp_ads in campaigns_to_pause.items():
            ad_names = [a.get("name", a["id"]) for a in camp_ads]
            is_nocamp = cid.startswith("nocamp_")
            if is_nocamp:
                ad = camp_ads[0]
                logger.warning(f"[Mirror Patrol] 未授权广告 {ad['id']} ({ad.get('name')})，直接暂停广告")
                if self.dry_run:
                    _log_action(act_id, "ad", ad["id"], ad.get("name", ad["id"]),
                                "pause", "mirror_mode",
                                f"[模拟][Patrol] 广告不在快照白名单",
                                old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                status="success", operator="system")
                else:
                    ok, err_msg = _fb_post(ad["id"], token, {"status": "PAUSED"})
                    action_status = "success" if (ok and _verify_status(ad["id"], token, "PAUSED")) else "failed"
                    _log_action(act_id, "ad", ad["id"], ad.get("name", ad["id"]),
                                "pause", "mirror_mode",
                                "镜像巡逻：广告不在快照白名单",
                                old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                status=action_status, error_msg=(err_msg if not ok else None),
                                operator="system")
                    patrol_result["closures"].append({
                        "type": "success" if action_status == "success" else "failed",
                        "level": "ad",
                        "id": ad["id"],
                        "ad_names": [ad.get("name", ad["id"])],
                        "error": err_msg if action_status == "failed" else None
                    })
            else:
                logger.warning(f"[Mirror Patrol] 未授权广告 ({', '.join(ad_names)})，直接关闭系列 {cid}")
                if self.dry_run:
                    for ad in camp_ads:
                        _log_action(act_id, "campaign", cid, f"[镜像] {ad.get('name', ad['id'])}的系列",
                                    "pause", "mirror_mode",
                                    f"[模拟][Patrol] 广告不在快照白名单，直接关闭系列",
                                    old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                    status="success", operator="system")
                    patrol_result["closures"].append({
                        "type": "dry_run",
                        "level": "campaign",
                        "id": cid,
                        "ad_names": ad_names
                    })
                else:
                    ok, err_msg = _fb_post(cid, token, {"status": "PAUSED"})
                    if ok:
                        time.sleep(2)
                        verified = _verify_status(cid, token, "PAUSED")
                        action_status = "success" if verified else "failed"
                        if not verified:
                            err_msg = "核验失败：系列effective_status未变为不可投放状态"
                    else:
                        action_status = "failed"
                    for ad in camp_ads:
                        _log_action(act_id, "campaign", cid, f"[镜像] {ad.get('name', ad['id'])}的系列",
                                    "pause", "mirror_mode",
                                    "镜像巡逻：广告不在快照白名单，直接关闭系列",
                                    old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                    status=action_status,
                                    error_msg=err_msg if action_status == "failed" else None,
                                    operator="system")
                    if action_status == "success":
                        patrol_result["closures"].append({
                            "type": "success",
                            "level": "campaign",
                            "id": cid,
                            "ad_names": ad_names
                        })
                    else:
                        patrol_result["closures"].append({
                            "type": "failed",
                            "level": "campaign",
                            "id": cid,
                            "ad_names": ad_names,
                            "error": err_msg
                        })

        logger.info(f"[Mirror Patrol] {act_id} 本次拦截 {len(unauthorized)} 条未授权广告，涉及 {len(campaigns_to_pause)} 个系列")
        return patrol_result

    def _check_rule(self, rule: dict, account: dict, token: str,
                    ad_id: str, adset_id: str, campaign_id: str, ad_name: str,
                    spend: float, conversions: float, clicks: int,
                    cpa: Optional[float], roas: Optional[float],
                    target_cpa: Optional[float], kpi_label: str, impressions: int,
                    reach: int = 0, ctr: float = 0.0, unique_clicks: int = 0,
                    account_currency: str = "USD", spend_raw: float = None,
                    broader_conv: float = 0.0):
        """
        所有金额参数（spend/cpa/target_cpa）均为 USD。
        account_currency: 账户原始货币（仅用于日志展示）
        spend_raw: 原始货币消耗金额（仅用于日志展示）
        """
        act_id = account["act_id"]
        rule_type = rule["rule_type"]
        action = rule.get("action", "pause")
        triggered = False
        reason = ""
        # 货币备注（非 USD 账户时展示原始金额信息）
        cur_note = ""
        if account_currency != "USD" and spend_raw is not None:
            cur_note = f"（原始 {account_currency} {spend_raw:.2f}，已转换为 USD）"

        if rule_type == "bleed_abs":
            threshold = rule.get("param_value") or self.default_bleed_abs
            if spend >= threshold and conversions == 0:
                # v3.3.7: broad check — prevent false kill when KPI field mismatches FB events
                if broader_conv > 0:
                    logger.warning(
                        f"BLEED_ABORT {ad_id}: kpi_field={kpi_label} produced 0 conversions, "
                        f"but broad check found {broader_conv} (field mismatch suspected)"
                    )
                    _log_action(act_id, "ad", ad_id, ad_name, "bleed_abort", "bleed_abs",
                                f"kpi_field mismatch: {kpi_label}=0, broad_check={broader_conv}")
                    return
                triggered = True
                reason = f"消耗 ${spend:.2f}{cur_note} 超过空成效止血线 ${threshold:.2f}，且 {kpi_label} = 0"

        elif rule_type == "cpa_exceed":
            if cpa:
                ratio = rule.get("param_ratio") or self.default_cpa_ratio
                abs_threshold = rule.get("param_value")  # 规则设置的绝对 CPA 阈值
                if abs_threshold and float(abs_threshold) > 0:
                    # 优先使用规则中设置的绝对阈值，不依赖 target_cpa
                    effective_target = float(abs_threshold)
                    threshold = effective_target * ratio
                    if cpa > threshold:
                        triggered = True
                        reason = (f"CPA ${cpa:.2f}{cur_note} 超过阈值 ${effective_target:.2f}×"
                                  f"{ratio*100:.0f}%=${threshold:.2f}")
                elif target_cpa:
                    # 回落到 target_cpa 模式
                    if cpa > target_cpa * ratio:
                        triggered = True
                        reason = (f"CPA ${cpa:.2f}{cur_note} 超过目标 ${target_cpa:.2f} 的 "
                                  f"{ratio*100:.0f}%（阈值 ${target_cpa*ratio:.2f}）")

        elif rule_type == "trend_drop":
            if roas is not None:
                threshold_pct = (rule.get("param_value") or 40) / 100
                yesterday_roas = self._get_yesterday_roas(act_id, ad_id)
                if yesterday_roas and yesterday_roas > 0:
                    drop = (yesterday_roas - roas) / yesterday_roas
                    if drop >= threshold_pct:
                        triggered = True
                        reason = (f"ROAS 从昨日 {yesterday_roas:.2f} 跌至今日 {roas:.2f}，"
                                  f"跌幅 {drop*100:.1f}% 超过熔断线 {threshold_pct*100:.0f}%")

        elif rule_type == "consecutive_bad":
            # 连续N天CPA超标
            days = rule.get("param_days") or 2
            ratio = rule.get("param_ratio") or self.default_cpa_ratio
            abs_threshold = rule.get("param_value")
            effective_target = None
            if abs_threshold and float(abs_threshold) > 0:
                effective_target = float(abs_threshold)  # 优先使用规则绝对阈值
            elif target_cpa:
                effective_target = target_cpa
            if effective_target and self._check_consecutive_bad(act_id, ad_id, effective_target, ratio, days):
                triggered = True
                reason = f"连续 {days} 天 CPA 超过目标 ${effective_target:.2f} 的 {ratio*100:.0f}%"

        elif rule_type == "click_no_conv":
            # 高频点击无转化
            threshold_clicks = int(rule.get("param_value") or 100)
            if clicks >= threshold_clicks and conversions == 0:
                triggered = True
                reason = f"点击数 {clicks} 超过 {threshold_clicks}，但 {kpi_label} = 0（疑似诱导点击）"

        elif rule_type == "budget_burn_fast":
            # 瞬烧制止：对比上次巡检消耗，单次周期内消耗增量超阈值则触发
            # param_value: 单次巡检周期内最大允许消耗增量（USD），默认20
            threshold_abs = rule.get("param_value") or 20.0
            try:
                _conn = get_conn()
                cache_row = _conn.execute(
                    "SELECT data FROM inspect_cache WHERE act_id=? AND ad_id=?",
                    (act_id, ad_id)
                ).fetchone()
                _conn.close()
                last_spend = 0.0
                if cache_row:
                    import json as _json
                    cache_data = _json.loads(cache_row["data"])
                    last_spend = float(cache_data.get("spend", 0))
                # 更新缓存（记录本次消耗）
                import json as _json2
                _conn2 = get_conn()
                _conn2.execute(
                    "INSERT OR REPLACE INTO inspect_cache (act_id, ad_id, data, updated_at) VALUES (?,?,?,datetime('now'))",
                    (act_id, ad_id, _json2.dumps({"spend": spend}))
                )
                _conn2.commit()
                _conn2.close()
                # 计算增量（当天消耗只增不减，若本次比上次少说明跨天重置）
                delta = spend - last_spend
                if delta > 0 and delta >= threshold_abs:
                    triggered = True
                    reason = (f"瞬烧预警：本次巡检消耗增量 ${delta:.2f} USD，"
                              f"超过单周期阈值 ${threshold_abs:.2f}（累计今日 ${spend:.2f}）")
            except Exception as _burn_err:
                logger.warning(f"budget_burn_fast 缓存读取失败: {_burn_err}")

        if rule_type == "low_ctr_no_conv":
            min_spend = float(rule.get("param_value") or 10.0)
            max_ctr = float(rule.get("param_ratio") or 0.5)
            if spend >= min_spend and impressions >= 100 and conversions == 0 and float(ctr or 0) <= max_ctr:
                triggered = True
                reason = (f"CTR {float(ctr or 0):.2f}% <= {max_ctr:.2f}% 且消耗 ${spend:.2f}，"
                          f"{kpi_label} = 0（点击 {clicks}，唯一点击 {unique_clicks}）")

        elif rule_type == "reach_no_conv":
            threshold_reach = int(rule.get("param_value") or 1000)
            min_spend = float(rule.get("param_ratio") or 10.0)
            if int(reach or 0) >= threshold_reach and spend >= min_spend and conversions == 0:
                triggered = True
                reason = f"覆盖 {int(reach or 0)} >= {threshold_reach} 且消耗 ${spend:.2f}，{kpi_label} = 0"

        if not triggered:
            return

        _set_cooldown(ad_id, rule_type)
        logger.info(f"触发规则 [{rule_type}] 广告 {ad_name}: {reason}")

        if action == "alert_only":
            _log_action(act_id, "ad", ad_id, ad_name, "alert",
                        rule_type, reason)
            _send_tg(
                f"⚠️ <b>Mira 预警</b>\n"
                f"账户：{account.get('name', act_id)}\n"
                f"广告：<code>{ad_name}</code>\n"
                f"原因：{reason}",
                act_id=act_id,
                event_type="guard",
            )

        elif action == "pause":
            level, status = _pause_with_escalation(
                account, ad_id, adset_id, campaign_id,
                ad_name, token, rule_type, reason, self.dry_run
            )
            if status in ("success", "escalated", "dry_run"):
                spend_display = (f"{account_currency} {spend_raw:.2f} (~${spend:.2f} USD)"
                                 if account_currency != "USD" and spend_raw is not None
                                 else f"${spend:.2f}")
                _send_tg(
                    f"🛑 <b>Mira 已暂停广告</b>\n"
                    f"账户：{account.get('name', act_id)}\n"
                    f"广告：<code>{ad_name}</code>\n"
                    f"原因：{reason}\n"
                    f"消耗：{spend_display} | {kpi_label}：{conversions:.0f}"
                    + (f"\n⬆️ 已升级关闭至{level}层级" if status == "escalated" else ""),
                    act_id=act_id,
                    event_type="guard",
                )
                # 止损后实时触发素材评分

        elif action in ("pause_adset", "pause_campaign"):
            target_level = "adset" if action == "pause_adset" else "campaign"
            target_id = adset_id if action == "pause_adset" else campaign_id
            target_label = "广告组" if action == "pause_adset" else "系列"
            if not target_id:
                _log_action(act_id, target_level, f"missing:{ad_id}", ad_name, "pause",
                            rule_type, reason, status="failed",
                            error_msg=f"缺少{target_label}ID，无法执行直接暂停", operator="system")
                _send_tg(
                    f"⚠️ <b>Mira 暂停失败</b>\n"
                    f"账户：{_tg_escape(account.get('name', act_id))} ({_tg_code(act_id)})\n"
                    f"广告：{_tg_code(ad_name)}\n"
                    f"原因：缺少{_tg_escape(target_label)}ID，无法执行直接暂停",
                    act_id=act_id,
                    event_type="guard",
                )
                return
            if self.dry_run:
                ok, err_msg, verified = True, "", True
            else:
                ok, err_msg = _fb_post(target_id, token, {"status": "PAUSED"})
                verified = False
                if ok:
                    time.sleep(2)
                    verified = _verify_status(target_id, token, "PAUSED")
                    if not verified:
                        err_msg = f"{target_label}状态校验失败"
            action_status = "success" if (ok and verified) else "failed"
            _log_action(act_id, target_level, target_id, f"[规则暂停] {ad_name}", "pause",
                        rule_type, reason,
                        old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                        status=action_status, error_msg=err_msg if action_status == "failed" else None,
                        operator="system")
            _send_tg(
                f"{'🚨' if action_status == 'success' else '⚠️'} <b>Mira 规则{target_label}暂停</b>\n"
                f"账户：{_tg_escape(account.get('name', act_id))} ({_tg_code(act_id)})\n"
                f"{target_label}ID：{_tg_code(target_id)}\n"
                f"广告：{_tg_code(ad_name)}\n"
                f"原因：{_tg_escape(reason)}"
                + (f"\n错误：{_tg_escape(err_msg)}" if action_status == "failed" else ""),
                act_id=act_id,
                event_type="guard",
            )

        elif action == "reduce_budget":
            pct = float(rule.get("action_value") or 0.2)
            if self.dry_run:
                _log_action(act_id, "adset", adset_id, ad_name, "reduce_budget",
                            rule_type, f"[DryRun] {reason} | 降幅 {pct*100:.0f}%")
            else:
                ok_b, err_b, old_b, new_b = _update_adset_budget(
                    adset_id, token, -pct, act_id, ad_name
                )
                if ok_b:
                    _log_action(act_id, "adset", adset_id, ad_name, "reduce_budget",
                                rule_type,
                                f"{reason} | 预算 ${old_b:.2f}→${new_b:.2f} (-{pct*100:.0f}%)")
                    _send_tg(
                        f"📉 <b>Mira 已降低预算</b>\n"
                        f"账户：{account.get('name', act_id)}\n"
                        f"广告：<code>{ad_name}</code>\n"
                        f"原因：{reason}\n"
                        f"预算：${old_b:.2f} → ${new_b:.2f}（-{pct*100:.0f}%）",
                        act_id=act_id,
                        event_type="guard",
                    )
                else:
                    _log_action(act_id, "adset", adset_id, ad_name, "reduce_budget_failed",
                                rule_type, f"{reason} | 降预算失败: {err_b}")
                    _send_tg(
                        f"⚠️ <b>Mira 降预算失败</b>\n"
                        f"广告：<code>{ad_name}</code>\n"
                        f"原因：{reason}\n"
                        f"错误：{err_b}",
                        act_id=act_id,
                        event_type="guard",
                    )

    def _check_scale_rule(self, rule: dict, account: dict, token: str,
                          ad_id: str, adset_id: str, campaign_id: str, ad_name: str,
                          spend: float, conversions: float,
                          cpa: Optional[float], roas: Optional[float],
                          target_cpa: Optional[float], kpi_label: str) -> bool:
        act_id = account["act_id"]
        rule_type = rule.get("rule_type") or "slow_scale"
        min_conv = max(0, int(rule.get("min_conversions") or 3))
        if float(conversions or 0) < min_conv:
            return False

        try:
            cpa_ratio = float(rule.get("cpa_ratio") or 0.8)
        except Exception:
            cpa_ratio = 0.8
        cpa_ratio = max(0.1, min(cpa_ratio, 2.0))

        roas_threshold = None
        if rule_type == "roas_scale":
            roas_threshold = float(rule.get("roas_threshold") or 3.0)

        if target_cpa:
            if cpa is None or cpa > target_cpa * cpa_ratio:
                return False
        elif not roas_threshold:
            return False

        if roas_threshold and (roas is None or roas < roas_threshold):
            return False

        days = max(1, int(rule.get("consecutive_days") or 1))
        if days > 1 and not self._check_consecutive_good(
            act_id, ad_id, target_cpa, cpa_ratio, roas_threshold, min_conv, days
        ):
            return False

        try:
            scale_pct = float(rule.get("scale_pct") or 0.15)
        except Exception:
            scale_pct = 0.15
        scale_pct = max(0.01, min(scale_pct, 1.0))

        max_budget = rule.get("max_budget")
        try:
            max_budget = float(max_budget) if max_budget not in (None, "") else None
        except Exception:
            max_budget = None

        reason_parts = [f"{kpi_label} {float(conversions or 0):.0f} >= {min_conv}"]
        if target_cpa:
            reason_parts.append(f"CPA ${cpa:.2f} <= ${target_cpa * cpa_ratio:.2f}")
        if roas_threshold:
            reason_parts.append(f"ROAS {roas:.2f} >= {roas_threshold:.2f}")
        if days > 1:
            reason_parts.append(f"{days} days passed")
        reason = " | ".join(reason_parts)
        cooldown_key = adset_id or ad_id
        rule_key = f"scale:{rule.get('id', rule_type)}"
        _set_cooldown(cooldown_key, rule_key)

        if self.dry_run:
            _log_action(act_id, "adset", adset_id, ad_name, "increase_budget",
                        rule_type, f"[DryRun] {reason} | +{scale_pct*100:.0f}%",
                        status="success", operator="system")
            return True

        ok_b, err_b, old_b, new_b = _update_adset_budget(
            adset_id, token, scale_pct, act_id, ad_name, max_budget=max_budget
        )
        if ok_b:
            _log_action(act_id, "adset", adset_id, ad_name, "increase_budget",
                        rule_type,
                        f"{reason} | budget ${old_b:.2f} -> ${new_b:.2f} (+{scale_pct*100:.0f}%)",
                        old_value={"daily_budget": old_b},
                        new_value={"daily_budget": new_b},
                        status="success", operator="system")
            _send_tg(
                f"<b>Mira 已拉量</b>\n"
                f"账户：{account.get('name', act_id)}\n"
                f"广告：<code>{ad_name}</code>\n"
                f"原因：{reason}\n"
                f"预算：${old_b:.2f} -> ${new_b:.2f}",
                act_id=act_id,
                event_type="scale",
            )
            return True

        if err_b == "budget_cap_reached":
            _log_action(act_id, "adset", adset_id, ad_name, "increase_budget_skipped",
                        rule_type,
                        f"{reason} | budget cap reached ${old_b:.2f}",
                        status="success", operator="system")
            return True

        _log_action(act_id, "adset", adset_id, ad_name, "increase_budget_failed",
                    rule_type, f"{reason} | {err_b}",
                    status="failed", error_msg=err_b, operator="system")
        _send_tg(
            f"<b>Mira 拉量失败</b>\n"
            f"账户：{account.get('name', act_id)}\n"
            f"广告：<code>{ad_name}</code>\n"
            f"错误：{err_b}",
            act_id=act_id,
            event_type="scale",
        )
        return True

    def _get_target_cpa(self, act_id, ad_id, adset_id, campaign_id) -> Optional[float]:
        conn = get_conn()
        for tid in [ad_id, adset_id, campaign_id, act_id]:
            if not tid:
                continue
            row = conn.execute(
                "SELECT target_cpa FROM kpi_configs WHERE act_id=? AND target_id=? AND enabled=1 LIMIT 1",
                (act_id, tid)
            ).fetchone()
            if row and row["target_cpa"]:
                conn.close()
                return float(row["target_cpa"])
        conn.close()
        return None

    def _check_consecutive_bad(self, act_id, ad_id, target_cpa, ratio, days) -> bool:
        conn = get_conn()
        rows = conn.execute(
            """SELECT cpa FROM perf_snapshots
               WHERE act_id=? AND ad_id=? AND snapshot_date >= date('now', '+8 hours', ?)
               ORDER BY snapshot_date DESC LIMIT ?""",
            (act_id, ad_id, f"-{days} days", days)
        ).fetchall()
        conn.close()
        if len(rows) < days:
            return False
        return all(r["cpa"] and r["cpa"] > target_cpa * ratio for r in rows)

    def _check_consecutive_good(self, act_id, ad_id, target_cpa, ratio,
                                roas_threshold, min_conversions, days) -> bool:
        conn = get_conn()
        rows = conn.execute(
            """SELECT cpa, conversions, roas FROM perf_snapshots
               WHERE act_id=? AND ad_id=? AND snapshot_date >= date('now', '+8 hours', ?)
               ORDER BY snapshot_date DESC LIMIT ?""",
            (act_id, ad_id, f"-{days} days", days)
        ).fetchall()
        conn.close()
        if len(rows) < days:
            return False
        for r in rows:
            if float(r["conversions"] or 0) < min_conversions:
                return False
            if target_cpa and (not r["cpa"] or float(r["cpa"]) > target_cpa * ratio):
                return False
            if roas_threshold and (not r["roas"] or float(r["roas"]) < roas_threshold):
                return False
        return True

    def _has_recent_action(self, act_id, target_id, action_type, hours: int = 24) -> bool:
        conn = get_conn()
        try:
            row = conn.execute(
                """SELECT 1 FROM action_logs
                   WHERE act_id=? AND target_id=? AND action_type=?
                     AND status='success'
                     AND datetime(created_at) >= datetime('now', '+8 hours', ?)
                   LIMIT 1""",
                (act_id, target_id, action_type, f"-{hours} hours")
            ).fetchone()
            return bool(row)
        finally:
            conn.close()

    def _save_snapshot(self, act_id, ad_id, adset_id, campaign_id, ad_name,
                       spend, impressions, clicks, conversions, cpa, roas,
                       kpi_field, actions_raw):
        today = date.today().isoformat()
        conn = get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO perf_snapshots
               (act_id, ad_id, adset_id, campaign_id, ad_name,
                snapshot_date, spend, impressions, clicks,
                conversions, cpa, roas, kpi_field, raw_actions)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (act_id, ad_id, adset_id, campaign_id, ad_name,
             today, spend, impressions, clicks,
             conversions, cpa, roas, kpi_field,
             json.dumps(actions_raw))
        )
        conn.commit()
        conn.close()

    def _get_yesterday_roas(self, act_id, ad_id) -> Optional[float]:
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        conn = get_conn()
        row = conn.execute(
            "SELECT roas FROM perf_snapshots WHERE act_id=? AND ad_id=? AND snapshot_date=?",
            (act_id, ad_id, yesterday)
        ).fetchone()
        conn.close()
        return row["roas"] if row else None






def emergency_pause_all(
    operator: str = "user",
    level: str = "campaign",
    team_id: Optional[int] = None,
    owner_user_id: Optional[int] = None,
) -> dict:
    """
    一键紧急暂停所有账户的所有活跃广告（按层级）
    level: campaign（系列级）| adset（广告组级）| ad（广告级）
    返回: {total, success, failed, failed_list, manual_required, level, level_label}
    """
    conn = get_conn()
    if owner_user_id is not None:
        accounts = conn.execute("SELECT * FROM accounts WHERE owner_user_id=?", (owner_user_id,)).fetchall()
    elif team_id is not None:
        accounts = conn.execute("SELECT * FROM accounts WHERE team_id=?", (team_id,)).fetchall()
    else:
        accounts = conn.execute("SELECT * FROM accounts").fetchall()  # 紧急暂停不受巡检开关限制
    conn.close()
    total = 0
    success = 0
    failed_list = []
    manual_required = []

    level_label = {"campaign": "广告系列", "adset": "广告组", "ad": "广告"}.get(level, "广告系列")
    fb_endpoint = {"campaign": "campaigns", "adset": "adsets", "ad": "ads"}.get(level, "campaigns")

    for acc in accounts:
        acc = dict(acc)
        act_id = acc["act_id"]
        token = _get_token_for_account(acc)
        if not token:
            manual_required.append({
                "act_id": act_id, "name": acc.get('name', act_id),
                "level": level, "level_label": level_label,
                "reason": "无可用Token，无法自动关闭，请手动处理"
            })
            continue
        try:
            eff_status = '["ACTIVE"]' if level == "campaign" else '["ACTIVE","CAMPAIGN_PAUSED"]'
            data = _fb_get(f"{act_id}/{fb_endpoint}", token,
                           {"fields": "id,name,status,effective_status",
                            "effective_status": eff_status, "limit": 200})
            items = data.get("data", [])
        except Exception as e:
            logger.error(f"紧急暂停：获取{level_label}失败 {act_id}: {e}")
            manual_required.append({
                "act_id": act_id, "name": acc.get('name', act_id),
                "level": level, "level_label": level_label,
                "reason": f"获取{level_label}列表失败: {str(e)}，请手动处理"
            })
            continue

        for item in items:
            total += 1
            item_id = item["id"]
            item_name = item.get("name", item_id)
            ok, err = _fb_post(item_id, token, {"status": "PAUSED"})
            if ok:
                time.sleep(0.5)
                verified = _verify_status(item_id, token, "PAUSED")
                if verified:
                    success += 1
                    _log_action(act_id, level, item_id, item_name,
                                "pause", "emergency", f"一键紧急暂停（{level_label}级）",
                                old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                status="success", operator=operator)
                else:
                    reason = "API调用成功但核验状态仍为ACTIVE，请手动关闭"
                    failed_list.append({"act_id": act_id, "level": level, "level_label": level_label,
                                        "id": item_id, "name": item_name, "reason": reason})
                    manual_required.append({"act_id": act_id, "level": level, "level_label": level_label,
                                            "id": item_id, "name": item_name, "reason": reason})
                    _log_action(act_id, level, item_id, item_name,
                                "pause", "emergency", f"一键紧急暂停（{level_label}级）",
                                status="failed", error_msg="核验失败：状态未变更", operator=operator)
            else:
                reason = f"API调用失败: {err}"
                failed_list.append({"act_id": act_id, "level": level, "level_label": level_label,
                                    "id": item_id, "name": item_name, "reason": reason})
                manual_required.append({"act_id": act_id, "level": level, "level_label": level_label,
                                        "id": item_id, "name": item_name,
                                        "reason": f"{reason}，请手动关闭"})
                _log_action(act_id, level, item_id, item_name,
                            "pause", "emergency", f"一键紧急暂停（{level_label}级）",
                            status="failed", error_msg=err, operator=operator)

    # TG 通知
    msg_parts = [f"🚨 <b>Mira 紧急暂停执行完毕</b>",
                 f"关闭层级：{level_label}级",
                 f"共 {total} 个{level_label}，成功关闭 {success}，失败 {len(failed_list)}"]
    if manual_required:
        msg_parts.append(f"\n⚠️ <b>以下 {len(manual_required)} 项需要人工处理：</b>")
        for item in manual_required[:8]:
            lbl = item.get('level_label', level_label)
            iid = item.get('id', item.get('act_id', ''))
            msg_parts.append(f"• [{lbl}] {item['name']} ({iid}): {item['reason']}")
        if len(manual_required) > 8:
            msg_parts.append(f"...及其他 {len(manual_required)-8} 项，请登录后台查看操作日志")
    _send_tg("\n".join(msg_parts), team_id=team_id, include_owner=owner_user_id is None)

    return {
        "total": total,
        "success": success,
        "failed": len(failed_list),
        "failed_list": failed_list,
        "manual_required": manual_required,
        "level": level,
        "level_label": level_label
    }


def _recent_action_log(act_id: str, target_id: str, trigger_type: str,
                       status: str, minutes: int) -> bool:
    """Return True when the same guard action was logged recently."""
    try:
        conn = get_conn()
        row = conn.execute(
            """SELECT 1 FROM action_logs
               WHERE act_id=? AND target_id=? AND trigger_type=? AND status=?
                 AND created_at >= datetime('now','+8 hours', ?)
               LIMIT 1""",
            (act_id, target_id, trigger_type, status, f"-{int(minutes)} minutes")
        ).fetchone()
        conn.close()
        return bool(row)
    except Exception:
        return False


def sentinel_patrol() -> dict:
    """
    哨兵扫描：遍历所有账户，检查是否有ACTIVE状态的系列。
    发现后立即关闭系列并发送 TG 通知。
    """
    _ensure_sentinel_schema()
    _ensure_team_guard_schema()
    _ensure_user_guard_schema()
    enabled = _get_setting("sentinel_enabled", "0")
    global_enabled = enabled == "1"
    dry_run = _is_dry_run()
    try:
        failure_cooldown = int(_get_setting("sentinel_failure_cooldown", "30"))
    except (ValueError, TypeError):
        failure_cooldown = 30
    conn = get_conn()
    if global_enabled:
        accounts = conn.execute(
            """SELECT a.*,
                      COALESCE(tm.sentinel_enabled, 0) AS team_sentinel_enabled,
                      COALESCE(ou.sentinel_enabled, 0) AS owner_sentinel_enabled
               FROM accounts a
               LEFT JOIN teams tm ON tm.id=a.team_id
               LEFT JOIN users ou ON ou.id=a.owner_user_id AND COALESCE(ou.is_active, 1)=1
               WHERE a.account_status NOT IN (3, 7, 9, 100)"""
        ).fetchall()
        mode = "global"
    else:
        accounts = conn.execute(
            """SELECT a.*,
                      COALESCE(tm.sentinel_enabled, 0) AS team_sentinel_enabled,
                      COALESCE(ou.sentinel_enabled, 0) AS owner_sentinel_enabled
               FROM accounts a
               LEFT JOIN teams tm ON tm.id=a.team_id
               LEFT JOIN users ou ON ou.id=a.owner_user_id AND COALESCE(ou.is_active, 1)=1
               WHERE (COALESCE(a.sentinel_enabled, 0)=1 OR COALESCE(tm.sentinel_enabled, 0)=1 OR COALESCE(ou.sentinel_enabled, 0)=1)
                 AND a.account_status NOT IN (3, 7, 9, 100)"""
        ).fetchall()
        mode = "team/account"
    conn.close()
    if not accounts:
        return {"status": "disabled", "mode": mode, "accounts_checked": 0, "series_closed": 0, "details": []}
    accounts_checked = 0
    series_closed = 0
    details = []
    for acc in accounts:
        acc = dict(acc)
        act_id = acc["act_id"]
        token = _get_token_for_account(acc, "PAUSE")
        if not token:
            continue
        accounts_checked += 1
        try:
            data = _fb_get(
                f"{act_id}/campaigns", token,
                {"fields": "id,name,status,effective_status",
                 "effective_status": '["ACTIVE"]', "limit": 200},
                paginate=True
            )
            campaigns = data.get("data", [])
            note_account_read_success(act_id)
        except Exception as e:
            note_account_read_failure(act_id, e)
            logger.warning(f"[Sentinel] 获取系列失败 {act_id}: {e}")
            continue
        for camp in campaigns:
            camp_id = camp["id"]
            camp_name = camp.get("name", camp_id)
            if _recent_action_log(act_id, camp_id, "sentinel", "failed", failure_cooldown):
                logger.info(f"[Sentinel] {camp_id} 最近 {failure_cooldown} 分钟已失败过，跳过重复告警")
                continue
            if dry_run:
                series_closed += 1
                details.append({"act_id": act_id, "campaign_id": camp_id, "name": camp_name, "status": "dry_run"})
                _log_action(act_id, "campaign", camp_id, camp_name,
                            "pause", "sentinel", "哨兵发现活跃系列（DryRun 未实际关闭）",
                            old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                            status="success", operator="sentinel")
                continue
            ok, err = _fb_post(camp_id, token, {"status": "PAUSED"})
            if ok:
                time.sleep(0.5)
                verified = _verify_status(camp_id, token, "PAUSED")
                if verified:
                    series_closed += 1
                    details.append({"act_id": act_id, "campaign_id": camp_id, "name": camp_name, "status": "closed"})
                    _log_action(act_id, "campaign", camp_id, camp_name,
                                "pause", "sentinel", f"哨兵发现活跃系列，已自动关闭",
                                old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                                status="success", operator="sentinel")
                    _send_tg(
                        f"🛡 <b>Mira 哨兵</b>\n"
                        f"账户：{acc.get('name', act_id)} (<code>{act_id}</code>)\n"
                        f"系列：{camp_name} (<code>{camp_id}</code>)\n"
                        f"状态：发现活跃系列，已自动关闭",
                        act_id=act_id,
                        event_type="sentinel",
                    )
                else:
                    _log_action(act_id, "campaign", camp_id, camp_name,
                                "pause", "sentinel", "哨兵关闭失败：核验状态未变更",
                                status="failed", error_msg="核验失败", operator="sentinel")
                    _send_tg(
                        f"⚠️ <b>Mira 哨兵关闭失败</b>\n"
                        f"账户：{acc.get('name', act_id)} (<code>{act_id}</code>)\n"
                        f"系列：{camp_name} (<code>{camp_id}</code>)\n"
                        f"原因：API调用成功但核验状态未变更，请手动关闭",
                        act_id=act_id,
                        event_type="sentinel",
                    )
            else:
                _log_action(act_id, "campaign", camp_id, camp_name,
                            "pause", "sentinel", f"哨兵关闭失败: {err}",
                            status="failed", error_msg=err, operator="sentinel")
                _send_tg(
                    f"⚠️ <b>Mira 哨兵关闭失败</b>\n"
                    f"账户：{acc.get('name', act_id)} (<code>{act_id}</code>)\n"
                    f"系列：{camp_name} (<code>{camp_id}</code>)\n"
                    f"原因：API调用失败: {err}，请手动关闭",
                    act_id=act_id,
                    event_type="sentinel",
                )
    if series_closed > 0:
        _send_tg(
            f"🛡 <b>Mira 哨兵扫描完成</b>\n"
            f"检查账户：{accounts_checked} 个\n"
            f"关闭系列：{series_closed} 个\n"
            f"哨兵模式保护中，所有非授权操作已被阻止"
        )
    return {"status": "ok", "mode": mode, "accounts_checked": accounts_checked, "series_closed": series_closed, "details": details}


def heartbeat_check() -> dict:
    """
    心跳检查：判断距上次管理员活动是否超过超时时间。
    若超时则触发 campaign 级别的紧急全停。
    """
    _ensure_team_guard_schema()
    _ensure_user_guard_schema()
    enabled = _get_setting("heartbeat_enabled", "0")
    global_enabled = enabled == "1"
    conn = get_conn()
    team_rows = conn.execute(
        """SELECT t.id, t.name, MAX(u.last_active_at) AS last_activity
           FROM teams t
           LEFT JOIN users u ON u.team_id=t.id AND COALESCE(u.is_active, 1)=1
           WHERE COALESCE(t.heartbeat_enabled, 0)=1 AND COALESCE(t.status, 'active')='active'
           GROUP BY t.id, t.name"""
    ).fetchall()
    user_rows = conn.execute(
        """SELECT u.id, COALESCE(NULLIF(u.display_name, ''), u.username) AS name,
                  u.team_id, t.name AS team_name, u.last_active_at AS last_activity
           FROM users u
           LEFT JOIN teams t ON t.id=u.team_id
           WHERE COALESCE(u.heartbeat_enabled, 0)=1
             AND COALESCE(u.is_active, 1)=1
             AND COALESCE(t.status, 'active')='active'"""
    ).fetchall()
    conn.close()
    if not global_enabled and not team_rows and not user_rows:
        return {"status": "disabled", "timeout": False, "action": "none"}
    try:
        timeout_min = int(_get_setting("heartbeat_timeout", "30"))
    except (ValueError, TypeError):
        timeout_min = 30
    last_activity = _get_setting("last_admin_activity", "") if global_enabled else ""
    # Both datetime.now() and SQLite datetime('now','+8 hours') are UTC+8
    # Server timezone is Asia/Shanghai, so they align directly
    now_bj = datetime.now()
    timed_out = False
    minutes_since = 0
    if last_activity:
        try:
            last_dt = datetime.strptime(last_activity, "%Y-%m-%d %H:%M:%S")
            delta = now_bj - last_dt
            minutes_since = int(delta.total_seconds() / 60)
            timed_out = minutes_since >= timeout_min
        except (ValueError, TypeError):
            # If last_activity is malformed, treat as no activity ever — do not trigger
            pass
    else:
        # First run after reboot: no activity recorded yet, don't trigger
        pass
    if timed_out and global_enabled:
        logger.warning(f"[Heartbeat] 管理员活动超时 {minutes_since} 分钟 (阈值={timeout_min}分钟)，触发紧急全停")
        action_line = "DryRun 模式：仅记录，不实际关闭广告系列" if _is_dry_run() else "正在执行紧急全停..."
        _send_tg(
            f"💓 <b>Mira 心跳超时</b>\n"
            f"距上次管理员活动已超过 <b>{minutes_since}</b> 分钟（阈值：{timeout_min}分钟）\n"
            f"{action_line}\n"
            f"请在控制台操作任意功能以恢复心跳"
        )
        if _is_dry_run():
            result = {"total": 0, "success": 0, "failed": 0, "dry_run": True}
            logger.warning("[Heartbeat] DryRun 模式，跳过实际紧急全停")
        else:
            result = emergency_pause_all(operator="heartbeat", level="campaign")
        # Log the heartbeat action
        conn = get_conn()
        conn.execute(
            "INSERT INTO action_logs (act_id, action_type, trigger_detail, status, error_msg) VALUES (?,?,?,?,?)",
            ('*', 'heartbeat', f'心跳超时 {minutes_since} 分钟，紧急全停',
             'success', f'共计 {result.get("total",0)} 个系列，成功关闭 {result.get("success",0)}')
        )
        result2 = conn.execute(
            "UPDATE settings SET value=datetime('now','+8 hours') WHERE key='last_admin_activity'"
        )
        if result2.rowcount == 0:
            conn.execute(
                "INSERT INTO settings(key,value) VALUES('last_admin_activity', datetime('now','+8 hours'))"
            )
        conn.commit()
        conn.close()
        return {"status": "ok", "timeout": True, "minutes_since": minutes_since, "action": "emergency_pause", "result": result}
    team_timeouts = []
    for team in team_rows:
        team_id = int(team["id"])
        team_name = team["name"] or f"Team {team_id}"
        marker_key = f"team_heartbeat_last_trigger_{team_id}"
        candidates = [team["last_activity"] or "", _get_setting(marker_key, "") or ""]
        last_dt = None
        for raw in candidates:
            if not raw:
                continue
            try:
                parsed = datetime.strptime(str(raw)[:19], "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue
            if last_dt is None or parsed > last_dt:
                last_dt = parsed
        if last_dt is None:
            continue
        team_minutes = int((now_bj - last_dt).total_seconds() / 60)
        if team_minutes < timeout_min:
            continue
        logger.warning(f"[Heartbeat] Team {team_id} activity timeout: {team_minutes} minutes")
        if _is_dry_run():
            result = {"total": 0, "success": 0, "failed": 0, "dry_run": True}
        else:
            result = emergency_pause_all(operator="heartbeat", level="campaign", team_id=team_id)
        conn = get_conn()
        conn.execute(
            "INSERT INTO action_logs (act_id, action_type, trigger_detail, status, error_msg) VALUES (?,?,?,?,?)",
            ('*', 'heartbeat', f'Team heartbeat timeout {team_minutes} minutes: {team_name}',
             'success', f'Total {result.get("total",0)}, closed {result.get("success",0)}')
        )
        updated = conn.execute(
            "UPDATE settings SET value=datetime('now','+8 hours') WHERE key=?",
            (marker_key,),
        )
        if updated.rowcount == 0:
            conn.execute(
                "INSERT INTO settings(key,value) VALUES(?, datetime('now','+8 hours'))",
                (marker_key,),
            )
        conn.commit()
        conn.close()
        team_timeouts.append({
            "team_id": team_id,
            "team_name": team_name,
            "minutes_since": team_minutes,
            "result": result,
        })
    owner_timeouts = []
    for owner in user_rows:
        owner_id = int(owner["id"])
        owner_name = owner["name"] or f"User {owner_id}"
        marker_key = f"owner_heartbeat_last_trigger_{owner_id}"
        candidates = [owner["last_activity"] or "", _get_setting(marker_key, "") or ""]
        last_dt = None
        for raw in candidates:
            if not raw:
                continue
            try:
                parsed = datetime.strptime(str(raw)[:19], "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue
            if last_dt is None or parsed > last_dt:
                last_dt = parsed
        if last_dt is None:
            continue
        owner_minutes = int((now_bj - last_dt).total_seconds() / 60)
        if owner_minutes < timeout_min:
            continue
        logger.warning(f"[Heartbeat] Owner {owner_id} activity timeout: {owner_minutes} minutes")
        if _is_dry_run():
            result = {"total": 0, "success": 0, "failed": 0, "dry_run": True}
        else:
            result = emergency_pause_all(operator="heartbeat", level="campaign", owner_user_id=owner_id)
        conn = get_conn()
        conn.execute(
            "INSERT INTO action_logs (act_id, action_type, trigger_detail, status, error_msg) VALUES (?,?,?,?,?)",
            ('*', 'heartbeat', f'Owner heartbeat timeout {owner_minutes} minutes: {owner_name}',
             'success', f'Total {result.get("total",0)}, closed {result.get("success",0)}')
        )
        updated = conn.execute(
            "UPDATE settings SET value=datetime('now','+8 hours') WHERE key=?",
            (marker_key,),
        )
        if updated.rowcount == 0:
            conn.execute(
                "INSERT INTO settings(key,value) VALUES(?, datetime('now','+8 hours'))",
                (marker_key,),
            )
        conn.commit()
        conn.close()
        owner_timeouts.append({
            "owner_user_id": owner_id,
            "owner_name": owner_name,
            "team_id": owner["team_id"],
            "team_name": owner["team_name"],
            "minutes_since": owner_minutes,
            "result": result,
        })
    if team_timeouts or owner_timeouts:
        action = "team_owner_emergency_pause" if team_timeouts and owner_timeouts else (
            "team_emergency_pause" if team_timeouts else "owner_emergency_pause"
        )
        return {
            "status": "ok",
            "timeout": True,
            "minutes_since": minutes_since,
            "action": action,
            "teams": team_timeouts,
            "owners": owner_timeouts,
        }
    return {"status": "ok", "timeout": False, "minutes_since": minutes_since, "action": "none"}
