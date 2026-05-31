"""
广告巡检引擎 v1.1.0
新增: 关闭失败向上升级、TG多ID、操作频率限制、状态核验、新规则类型
"""
import json
import logging
import time
import os
from datetime import datetime, date, timedelta
from typing import Optional, Tuple
import requests

from core.database import get_conn, decrypt_token

logger = logging.getLogger("mira.guard")

FB_API_BASE = "https://graph.facebook.com/v25.0"
FB_AD_FIELDS = (
    "id,name,status,effective_status,adset_id,campaign_id,"
    "campaign{objective},"
    "adset{optimization_goal,destination_type},"
    "insights.date_preset(today){spend,impressions,clicks,actions,action_values,cpc,cpm}"
)
MIRROR_AD_FIELDS = "id,name,status,effective_status,campaign_id"

# 操作冷却：同一广告同一规则60分钟内不重复触发
_action_cooldown: dict = {}  # key: f"{ad_id}:{rule_type}" -> timestamp
_COOLDOWN_TTL = 7200  # 2小时TTL，超过此时间的冷却记录可清理

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
        resp = requests.get(base_url, timeout=20)
        resp.raise_for_status()
        return resp.json()

    # ── 分页模式：跟随 paging.next 游标直到所有数据拉取完毕 ──────────────
    all_data = []
    next_url = base_url
    page_count = 0

    while next_url and page_count < max_pages:
        resp = requests.get(next_url, timeout=20)
        resp.raise_for_status()
        result = resp.json()
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
    data["access_token"] = token
    try:
        resp = requests.post(f"{FB_API_BASE}/{path}", data=data, timeout=20)
        result = resp.json()
        if resp.status_code == 200 and result.get("success"):
            return True, ""
        # 区分错误类型
        err = result.get("error", {})
        code = err.get("code", 0)
        msg = err.get("message", str(result))
        # 190=Token失效, 100=权限不足, 200=权限拒绝 -> 不重试，直接向上升级
        if code in (190, 100, 200, 294):
            return False, f"权限拒绝(code={code}): {msg}"
        return False, f"API错误(code={code}): {msg}"
    except requests.exceptions.RequestException as e:
        return False, f"网络错误: {e}"



def _update_adset_budget(adset_id: str, token: str, delta_pct: float,
                         act_id: str = "", ad_name: str = "") -> Tuple[bool, str, float, float]:
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
        return False, str(e), 0, 0

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


def _send_tg(msg: str, parse_mode: str = "HTML"):
    """发送TG通知，支持多个Chat ID"""
    token = _get_setting("tg_bot_token", "")
    chat_ids_str = _get_setting("tg_chat_ids", "")
    if not token or not chat_ids_str:
        return
    chat_ids = [cid.strip() for cid in chat_ids_str.split(",") if cid.strip()]
    for chat_id in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg, "parse_mode": parse_mode},
                timeout=10
            )
        except Exception as e:
            logger.warning(f"TG 推送失败 (chat_id={chat_id}): {e}")


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
    logger.warning(f"广告 {ad_id} 暂停失败: {err_msg}，尝试向上升级到广告组")
    _log_action(act_id, "ad", ad_id, ad_name, "pause",
                trigger_type, trigger_detail,
                status="failed", error_msg=err_msg, operator="system")

    escalate = _get_setting("escalate_on_fail", "1") == "1"
    if not escalate:
        _send_tg(
            f"❌ <b>Mira 暂停失败</b>\n"
            f"广告：{ad_name}\n"
            f"原因：{err_msg}\n"
            f"⚠️ 向上升级已关闭，请手动处理！"
        )
        return "ad", "failed"

    # Step 2: 向上升级到广告组
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
                    f"广告 <code>{ad_name}</code> 关闭失败\n"
                    f"已自动关闭其所属广告组\n"
                    f"失败原因：{err_msg}"
                )
                return "adset", "escalated"
            err2 = f"广告组核验失败: {err2}"

        logger.warning(f"广告组 {adset_id} 暂停失败: {err2}，尝试向上升级到系列")
        _log_action(act_id, "adset", adset_id, "广告组", "pause",
                    trigger_type, f"升级关闭广告组失败",
                    status="failed", error_msg=err2, operator="system")

    # Step 3: 向上升级到系列
    if campaign_id:
        ok3, err3 = _fb_post(campaign_id, token, {"status": "PAUSED"})
        if ok3:
            time.sleep(2)
            verified3 = _verify_status(campaign_id, token, "PAUSED")
            status3 = "escalated" if verified3 else "failed"
            _log_action(act_id, "campaign", campaign_id, f"[升级关闭] {ad_name}的系列",
                        "pause", trigger_type,
                        f"因广告/广告组关闭失败，升级关闭系列。",
                        old_value={"status": "ACTIVE"}, new_value={"status": "PAUSED"},
                        status=status3, operator="system")
            if verified3:
                _send_tg(
                    f"🚨 <b>Mira 紧急升级关闭系列</b>\n"
                    f"广告 <code>{ad_name}</code> 及其广告组均关闭失败\n"
                    f"已自动关闭其所属广告系列！\n"
                    f"请立即检查账户状态"
                )
                return "campaign", "escalated"

    # 全部失败
    _send_tg(
        f"🆘 <b>Mira 严重告警</b>\n"
        f"广告 <code>{ad_name}</code>\n"
        f"广告/广告组/系列均关闭失败！\n"
        f"请立即手动处理！"
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


def _get_kpi_aliases(kpi_field: str) -> list:
    """获取标准别名列表（含自身），DB优先"""
    _load_alias_cache()
    std = _KPI_ALIAS_MAP_DB.get('standard', {})
    if kpi_field in std:
        return std[kpi_field]
    for kt, aliases in std.items():
        if kpi_field in aliases:
            return aliases
    return [kpi_field]


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
    for a in actions_raw:
        if a.get('action_type') in _get_kpi_aliases(kpi_field):
            result['conversions'] = float(a.get('value', 0))
            result['matched_action'] = a['action_type']
            break

    # 无标准匹配时尝试兜底
    if result['conversions'] == 0:
        has_purchase = any('purchase' in (a.get('action_type', '')).lower() for a in actions_raw)
        fallback_aliases = _get_kpi_fallback_aliases(kpi_field)
        if fallback_aliases:
            for a in actions_raw:
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
            if has_purchase and kpi_field not in ('purchase', 'offsite_conversion.fb_pixel_purchase'):
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


class GuardEngine:
    """广告巡检引擎主类 v1.1.0"""

    def __init__(self):
        self.dry_run = _is_dry_run()
        self.default_bleed_abs = float(_get_setting("default_bleed_abs", "20"))
        self.default_cpa_ratio = float(_get_setting("default_cpa_ratio", "1.3"))
        self.learning_protect = _get_setting("learning_phase_protect", "1") == "1"

    def run_all(self):
        if _get_setting("inspect_enabled", "1") != "1":
            logger.info("自动巡检已关闭（inspect_enabled=0），跳过")
        else:
            conn = get_conn()
            accounts = conn.execute(
                "SELECT * FROM accounts WHERE enabled=1 AND account_status NOT IN (3, 7, 9)"
            ).fetchall()
            conn.close()
            for acc in accounts:
                try:
                    self.inspect_account(dict(acc))
                except Exception as e:
                    logger.error(f"账户 {acc['act_id']} 巡检异常: {e}")

        # ── 镜像巡逻：保护未开启巡检但存活(enabled=0)的账户 ──────────────────────
        global_mirror = _get_setting("mirror_enabled", "0")
        if global_mirror == "1":
            conn = get_conn()
            mirror_only = conn.execute(
                "SELECT * FROM accounts WHERE enabled=0 AND account_status NOT IN (3, 7, 9)"
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
                    _send_tg(_build_mirror_patrol_summary(patrol_results))

    def inspect_account(self, account: dict):
        act_id = account["act_id"]
        token = _get_token_for_account(account)
        if not token:
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
            _log_action(act_id, "account", act_id, account.get("name", ""),
                        "inspect", "system", f"API拉取失败: {e}",
                        status="failed", error_msg=str(e))
            if _get_setting("mirror_enabled", "0") == "1" or account.get("mirror_enabled", 0) == 1:
                try:
                    mirror_result = self._mirror_patrol(account)
                    if mirror_result and (mirror_result.get("closures") or mirror_result.get("review_pending")):
                        _send_tg(_build_mirror_patrol_summary([mirror_result]))
                except Exception as mirror_err:
                    logger.error(f"[Mirror] 巡检字段失败后的兜底镜像巡逻也失败 {act_id}: {mirror_err}")
            return

        ads = data.get("data", [])
        logger.info(f"账户 {act_id} 活跃广告数: {len(ads)}")

        # ── 镜像模式：暂停不在白名单中的未授权广告 ──────────────────────────────
        global_mirror = _get_setting("mirror_enabled", "0")
        account_mirror = account.get("mirror_enabled", 0)
        if global_mirror == "1" or account_mirror == 1:
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
                    _send_tg(_build_mirror_account_summary(act_id, account.get("name", act_id), mirror_events))

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
        # 同时拉取账户级规则和全局规则（__global__），账户级同类型规则优先
        acc_rules = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id=? AND enabled=1", (act_id,)
        ).fetchall()
        global_rules = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id='__global__' AND enabled=1"
        ).fetchall()
        conn.close()
        # 合并：账户级规则覆盖同类型全局规则
        acc_rule_types = {r["rule_type"] for r in acc_rules}
        merged_rules = [dict(r) for r in acc_rules]
        for gr in global_rules:
            if gr["rule_type"] not in acc_rule_types:
                gr_dict = dict(gr)
                gr_dict["_is_global"] = True
                merged_rules.append(gr_dict)
        rules = merged_rules

        for ad in ads:
            try:
                self._inspect_ad(account, ad, token, rules)
            except Exception as e:
                logger.error(f"广告 {ad.get('id')} 巡检异常: {e}")

    
    def _inspect_ad(self, account: dict, ad: dict, token: str, rules: list):
            from services.kpi_resolver import get_kpi_for_ad

            act_id = account["act_id"]
            ad_id = ad["id"]
            ad_name = ad.get("name", ad_id)
            adset_id = ad.get("adset_id", "")
            campaign_id = ad.get("campaign_id", "")

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
            clicks = int(ins.get("clicks", 0))
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
            revenue_raw = 0.0
            for av in action_values:
                if av.get("action_type") in _get_kpi_aliases(kpi_field):
                    revenue_raw = float(av.get("value", 0))
                    break
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
                    account_currency=account_currency, spend_raw=spend_raw,
                    broader_conv=broader_conv
                )

    def _mirror_patrol(self, account: dict):
        """仅执行镜像检查，不做KPI/规则巡检。用于enabled=0但需镜像保护的账户"""
        act_id = account["act_id"]
        patrol_result = {
            "act_id": act_id,
            "account_name": account.get("name", act_id),
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
                f"原因：{reason}"
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
                    + (f"\n⬆️ 已升级关闭至{level}层级" if status == "escalated" else "")
                )
                # 止损后实时触发素材评分

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
                        f"预算：${old_b:.2f} → ${new_b:.2f}（-{pct*100:.0f}%）"
                    )
                else:
                    _log_action(act_id, "adset", adset_id, ad_name, "reduce_budget_failed",
                                rule_type, f"{reason} | 降预算失败: {err_b}")
                    _send_tg(
                        f"⚠️ <b>Mira 降预算失败</b>\n"
                        f"广告：<code>{ad_name}</code>\n"
                        f"原因：{reason}\n"
                        f"错误：{err_b}"
                    )

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






def emergency_pause_all(operator: str = "user", level: str = "campaign") -> dict:
    """
    一键紧急暂停所有账户的所有活跃广告（按层级）
    level: campaign（系列级）| adset（广告组级）| ad（广告级）
    返回: {total, success, failed, failed_list, manual_required, level, level_label}
    """
    conn = get_conn()
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
    _send_tg("\n".join(msg_parts))

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
    enabled = _get_setting("sentinel_enabled", "0")
    if enabled != "1":
        return {"status": "disabled", "accounts_checked": 0, "series_closed": 0}
    dry_run = _is_dry_run()
    try:
        failure_cooldown = int(_get_setting("sentinel_failure_cooldown", "30"))
    except (ValueError, TypeError):
        failure_cooldown = 30
    conn = get_conn()
    accounts = conn.execute(
        "SELECT * FROM accounts WHERE account_status NOT IN (3, 7, 9, 100)"
    ).fetchall()
    conn.close()
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
        except Exception as e:
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
                        f"状态：发现活跃系列，已自动关闭"
                    )
                else:
                    _log_action(act_id, "campaign", camp_id, camp_name,
                                "pause", "sentinel", "哨兵关闭失败：核验状态未变更",
                                status="failed", error_msg="核验失败", operator="sentinel")
                    _send_tg(
                        f"⚠️ <b>Mira 哨兵关闭失败</b>\n"
                        f"账户：{acc.get('name', act_id)} (<code>{act_id}</code>)\n"
                        f"系列：{camp_name} (<code>{camp_id}</code>)\n"
                        f"原因：API调用成功但核验状态未变更，请手动关闭"
                    )
            else:
                _log_action(act_id, "campaign", camp_id, camp_name,
                            "pause", "sentinel", f"哨兵关闭失败: {err}",
                            status="failed", error_msg=err, operator="sentinel")
                _send_tg(
                    f"⚠️ <b>Mira 哨兵关闭失败</b>\n"
                    f"账户：{acc.get('name', act_id)} (<code>{act_id}</code>)\n"
                    f"系列：{camp_name} (<code>{camp_id}</code>)\n"
                    f"原因：API调用失败: {err}，请手动关闭"
                )
    if series_closed > 0:
        _send_tg(
            f"🛡 <b>Mira 哨兵扫描完成</b>\n"
            f"检查账户：{accounts_checked} 个\n"
            f"关闭系列：{series_closed} 个\n"
            f"哨兵模式保护中，所有非授权操作已被阻止"
        )
    return {"status": "ok", "accounts_checked": accounts_checked, "series_closed": series_closed, "details": details}


def heartbeat_check() -> dict:
    """
    心跳检查：判断距上次管理员活动是否超过超时时间。
    若超时则触发 campaign 级别的紧急全停。
    """
    enabled = _get_setting("heartbeat_enabled", "0")
    if enabled != "1":
        return {"status": "disabled", "timeout": False, "action": "none"}
    try:
        timeout_min = int(_get_setting("heartbeat_timeout", "30"))
    except (ValueError, TypeError):
        timeout_min = 30
    last_activity = _get_setting("last_admin_activity", "")
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
    if timed_out:
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
    return {"status": "ok", "timeout": False, "minutes_since": minutes_since, "action": "none"}
