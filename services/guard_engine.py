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


def _fb_get(path: str, token: str, params: dict = None) -> dict:
    """
    FB API GET 请求。
    注意：如果 params 中包含 effective_status，会自动将其从 params 中移出并手动拼接到URL，
    避免 requests 将方括号和引号 URL 编码导致 FB API 400 错误。
    """
    import urllib.parse
    p = dict(params or {})
    effective_status = p.pop("effective_status", None)
    p["access_token"] = token
    base_url = f"{FB_API_BASE}/{path}?{urllib.parse.urlencode(p)}"
    if effective_status:
        base_url += f"&effective_status={effective_status}"
    resp = requests.get(base_url, timeout=20)
    resp.raise_for_status()
    return resp.json()


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
    """核验对象状态是否符合预期"""
    try:
        result = _fb_get(obj_id, token, {"fields": "status,effective_status"})
        actual = result.get("status", "")
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
            # TokenManager 返回 None（操作号耗尽且非 PAUSE 操作）
            return ""
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
        conn = get_conn()
        accounts = conn.execute("SELECT * FROM accounts").fetchall()  # 紧急暂停不受巡检开关限制
        conn.close()
        for acc in accounts:
            try:
                self.inspect_account(dict(acc))
            except Exception as e:
                logger.error(f"账户 {acc['act_id']} 巡检异常: {e}")

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
                {"fields": FB_AD_FIELDS, "effective_status": '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED"]', "limit": 200}
            )
        except Exception as e:
            logger.error(f"拉取广告列表失败 {act_id}: {e}")
            _log_action(act_id, "account", act_id, account.get("name", ""),
                        "inspect", "system", f"API拉取失败: {e}",
                        status="failed", error_msg=str(e))
            return

        ads = data.get("data", [])
        logger.info(f"账户 {act_id} 活跃广告数: {len(ads)}")

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

            # ── AI 托管决策层 ────────────────────────────────────────────────────
            if account.get("ai_managed") == 1:
                try:
                    from services.ai_advisor import analyze_ad_performance, is_ai_enabled
                    if is_ai_enabled():
                        ai_data = {
                            "ad_id": ad_id, "ad_name": ad_name, "act_id": act_id,
                            "spend": spend, "conversions": conversions,
                            "cpa": cpa, "roas": roas, "target_cpa": target_cpa,
                            "kpi_field": kpi_field,
                        }
                        ai_result = analyze_ad_performance(ai_data)
                        if ai_result:
                            diagnosis = ai_result.get("diagnosis", "")
                            suggestions = ai_result.get("suggestions", [])
                            risk_level = ai_result.get("risk_level", "low")
                            action_taken = "observe"
                            executed = 0
                            error_msg = None

                            if risk_level == "high" and action != "pause":
                                if not self.dry_run:
                                    level_ai, status_ai = _pause_with_escalation(
                                        account, ad_id, adset_id, campaign_id,
                                        ad_name, token, "ai_managed",
                                        "AI高风险决策: " + diagnosis, self.dry_run
                                    )
                                    if status_ai in ("success", "escalated"):
                                        action_taken = "pause"
                                        executed = 1
                                        _send_tg(
                                            "\U0001f916 <b>AI 托管已暂停广告</b>\n"
                                            "账户：" + account.get("name", act_id) + "\n"
                                            "广告：<code>" + ad_name + "</code>\n"
                                            "AI诊断：" + diagnosis + "\n"
                                            "风险等级：\U0001f534 高"
                                        )
                                    else:
                                        error_msg = "暂停失败: " + str(status_ai)
                                else:
                                    action_taken = "pause(dry_run)"

                            elif risk_level == "low" and cpa and target_cpa and cpa < target_cpa * 0.8 and conversions >= 5:
                                if not self.dry_run:
                                    ok_b, err_b, old_b, new_b = _update_adset_budget(
                                        adset_id, token, 0.2, act_id, ad_name
                                    )
                                    if ok_b:
                                        action_taken = "increase_budget: $" + str(round(old_b, 2)) + "->" + str(round(new_b, 2))
                                        executed = 1
                                        _send_tg(
                                            "\U0001f916 <b>AI 托管已加预算</b>\n"
                                            "账户：" + account.get("name", act_id) + "\n"
                                            "广告：<code>" + ad_name + "</code>\n"
                                            "AI诊断：" + diagnosis + "\n"
                                            "预算：$" + str(round(old_b, 2)) + " → $" + str(round(new_b, 2)) + " (+20%)"
                                        )
                                    else:
                                        error_msg = "加预算失败: " + str(err_b)
                                        action_taken = "increase_budget_failed"
                                else:
                                    action_taken = "increase_budget(dry_run)"
                            else:
                                executed = 1

                            try:
                                import json as _json
                                _ai_conn = get_conn()
                                _ai_conn.execute(
                                    "INSERT INTO ai_decisions "
                                    "(act_id, ad_id, adset_id, ad_name, decision_type, "
                                    "action_taken, diagnosis, suggestions, risk_level, "
                                    "spend, cpa, roas, conversions, executed, error_msg) "
                                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (act_id, ad_id, adset_id, ad_name, "auto_inspect",
                                     action_taken, diagnosis,
                                     _json.dumps(suggestions, ensure_ascii=False),
                                     risk_level, spend, cpa, roas, conversions,
                                     executed, error_msg)
                                )
                                _ai_conn.commit()
                                _ai_conn.close()
                            except Exception as _log_err:
                                logger.warning("AI决策日志写入失败: " + str(_log_err))
                except Exception as ai_err:
                    logger.warning("AI 托管决策失败（非致命）" + act_id + "/" + ad_id + ": " + str(ai_err))
            # ── AI 托管决策层 END ─────────────────────────────────────────────────


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
                try:
                    from core.database import get_conn as _gc_score
                    _conn_score = _gc_score()
                    _asset_row = _conn_score.execute(
                        "SELECT DISTINCT asset_id FROM auto_campaign_ads WHERE fb_ad_id=? AND asset_id IS NOT NULL LIMIT 1",
                        (ad_id,)
                    ).fetchone()
                    _conn_score.close()
                    if _asset_row and _asset_row["asset_id"]:
                        _asset_id = _asset_row["asset_id"]
                        import threading as _threading
                        def _do_score(_aid):
                            try:
                                from services.asset_scorer import score_asset
                                score_asset(_aid)
                                logger.info(f"[守护引擎] 止损后实时评分完成: 素材 {_aid}")
                            except Exception as _se:
                                logger.warning(f"[守护引擎] 止损后评分失败: {_se}")
                        _threading.Thread(target=_do_score, args=(_asset_id,), daemon=True).start()
                except Exception as _score_err:
                    logger.warning(f"[守护引擎] 止损后评分异常: {_score_err}")

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



def _run_ai_decision(conn, account: dict, ad_data: dict):
    """
    对 ai_managed=1 的账户广告运行 AI 分析并自动执行决策
    """
    import datetime, requests
    try:
        from services.ai_advisor import analyze_ad_performance
    except ImportError:
        try:
            from ai_advisor import analyze_ad_performance
        except ImportError:
            return

    act_id = account.get('act_id', '')
    ad_id = ad_data.get('ad_id', ad_data.get('id', ''))
    ad_name = ad_data.get('name', ad_data.get('ad_name', ''))
    token_val = account.get('token_value', '')

    # 获取 KPI 目标
    kpi = {}
    try:
        kpi_row = conn.execute(
            "SELECT target_cpa, target_roas FROM kpi_configs WHERE act_id=? LIMIT 1",
            (act_id,)
        ).fetchone()
        if kpi_row:
            kpi = {'target_cpa': kpi_row[0], 'target_roas': kpi_row[1]}
    except Exception:
        pass

    # 调用 AI 分析
    try:
        result = analyze_ad_performance(ad_data, kpi)
    except Exception as e:
        return

    decision_type = result.get('action', 'no_action')
    risk_level = result.get('risk_level', 'low')
    diagnosis = result.get('diagnosis', '')
    suggestion = result.get('suggestion', '')
    executed = False
    exec_detail = ''

    # 根据 AI 决策自动执行
    if decision_type == 'pause_ad' and risk_level == 'high' and token_val and ad_id:
        try:
            resp = requests.post(
                f"https://graph.facebook.com/v19.0/{ad_id}",
                data={"status": "PAUSED", "access_token": token_val},
                timeout=10
            )
            if resp.status_code == 200:
                executed = True
                exec_detail = f"已暂停广告 {ad_id}"
        except Exception as e:
            exec_detail = f"暂停失败: {e}"

    elif decision_type == 'increase_budget' and risk_level in ('low',) and token_val:
        adset_id = ad_data.get('adset_id', '')
        if adset_id:
            try:
                resp = requests.get(
                    f"https://graph.facebook.com/v19.0/{adset_id}",
                    params={"fields": "daily_budget", "access_token": token_val},
                    timeout=10
                )
                if resp.status_code == 200:
                    current_budget = int(resp.json().get('daily_budget', 0))
                    if current_budget > 0:
                        new_budget = int(current_budget * 1.2)
                        resp2 = requests.post(
                            f"https://graph.facebook.com/v19.0/{adset_id}",
                            data={"daily_budget": new_budget, "access_token": token_val},
                            timeout=10
                        )
                        if resp2.status_code == 200:
                            executed = True
                            exec_detail = f"预算 {current_budget/100:.2f} → {new_budget/100:.2f}"
            except Exception as e:
                exec_detail = f"加预算失败: {e}"

    # 记录 AI 决策日志
    now_bj = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=8))
    ).strftime('%Y-%m-%d %H:%M:%S')
    try:
        conn.execute(
            """INSERT INTO ai_decisions
               (act_id, ad_id, ad_name, decision_type, risk_level, diagnosis, suggestion, executed, exec_detail, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (act_id, ad_id, ad_name, decision_type, risk_level, diagnosis, suggestion,
             1 if executed else 0, exec_detail, now_bj)
        )
        conn.commit()
    except Exception as e:
        pass


class ScaleEngine:
    """拉量引擎 v1.2.0 - 支持地区过滤 + 全局规则 + 连续天数 + 预算上限 + 冷却期"""

    def __init__(self):
        self.dry_run = _is_dry_run()

    def run_all(self):
        conn = get_conn()
        # 只对 testing/scaling 阶段的账户执行拉量，避免对预热期账户误操作
        accounts = conn.execute(
            "SELECT * FROM accounts WHERE lifecycle_stage IN ('testing','scaling') OR lifecycle_stage IS NULL"
        ).fetchall()
        all_scale_rules = conn.execute("SELECT * FROM scale_rules WHERE enabled=1").fetchall()
        # 全局规则（act_id='__global__'）：对所有账户生效，参考止损规则的全局规则逻辑
        global_rules = [dict(r) for r in all_scale_rules if dict(r)["act_id"] == "__global__"]
        conn.close()

        for acc in accounts:
            acc = dict(acc)
            act_id = acc["act_id"]
            acc_rules = [dict(r) for r in all_scale_rules if dict(r)["act_id"] == act_id]
            # 合并：账户级同类型规则优先，全局规则补充（与止损规则逻辑一致）
            acc_rule_types = {r["rule_type"] for r in acc_rules}
            merged_rules = list(acc_rules)
            for gr in global_rules:
                if gr["rule_type"] not in acc_rule_types:
                    gr_copy = dict(gr)
                    gr_copy["_is_global"] = True
                    merged_rules.append(gr_copy)
            if merged_rules:
                self._run_account(acc, merged_rules)

    def _run_account(self, account: dict, rules: list):
        act_id = account["act_id"]
        token = _get_token_for_account(account)
        today = date.today().isoformat()
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        conn = get_conn()
        ads = conn.execute(
            """SELECT ad_id, ad_name, adset_id,
                      AVG(cpa) as avg_cpa, SUM(conversions) as total_conv,
                      AVG(roas) as avg_roas
               FROM perf_snapshots
               WHERE act_id=? AND snapshot_date IN (?,?)
               GROUP BY ad_id HAVING COUNT(DISTINCT snapshot_date) >= 1""",
            (act_id, today, yesterday)
        ).fetchall()
        conn.close()

        for ad in ads:
            ad = dict(ad)
            # 获取广告组地区（用于地区过滤）
            ad_regions = self._get_adset_regions(ad.get("adset_id", ""), token)
            for rule in rules:
                self._check_scale(account, ad, rule, ad_regions, token)

    def _get_adset_regions(self, adset_id: str, token: str) -> list:
        """获取广告组定位地区"""
        if not adset_id or not token:
            return []
        try:
            result = _fb_get(adset_id, token, {"fields": "targeting"})
            targeting = result.get("targeting", {})
            geo = targeting.get("geo_locations", {})
            countries = geo.get("countries", [])
            return [c.upper() for c in countries]
        except Exception:
            return []

    def _check_consecutive_good(self, act_id: str, ad_id: str, target_cpa: float,
                                   cpa_ratio: float, min_conv: int, days: int) -> bool:
        """检查广告是否连续N天CPA优秀（低于目标×ratio且转化数达标），参考止损规则_check_consecutive_bad逻辑"""
        conn = get_conn()
        rows = conn.execute(
            """SELECT cpa, conversions FROM perf_snapshots
               WHERE act_id=? AND ad_id=? AND snapshot_date >= date('now', '+8 hours', ?)
               ORDER BY snapshot_date DESC LIMIT ?""",
            (act_id, ad_id, f"-{days} days", days)
        ).fetchall()
        conn.close()
        if len(rows) < days:
            return False
        return all(
            r["cpa"] and r["cpa"] <= target_cpa * cpa_ratio
            and (r["conversions"] or 0) >= min_conv
            for r in rows
        )

    def _check_scale(self, account: dict, ad: dict, rule: dict, ad_regions: list, token: str):
        act_id = account["act_id"]
        rule_type = rule["rule_type"]
        rule_id = rule.get("id", rule_type)
        ad_id = ad["ad_id"]
        ad_name = ad["ad_name"]

        # 冷却期检查：同一广告同一规则60分钟内不重复触发（与止损规则一致）
        if _check_cooldown(ad_id, f"scale_{rule_id}"):
            return

        # 地区过滤
        target_regions_json = rule.get("target_regions")
        if target_regions_json:
            try:
                target_regions = json.loads(target_regions_json)
                if target_regions and ad_regions:
                    if not any(r.upper() in ad_regions for r in target_regions):
                        return  # 地区不匹配，跳过
            except Exception:
                pass

        if rule_type == "winner_alert":
            if ad["avg_roas"] and ad["avg_roas"] >= rule["roas_threshold"]:
                _send_tg(
                    f"🚀 <b>Mira 赢家提示</b>\n"
                    f"广告：<code>{ad_name}</code>\n"
                    f"平均ROAS：{ad['avg_roas']:.2f}（阈值 {rule['roas_threshold']}）\n"
                    f"建议：复制该广告组并提升 30% 预算"
                )
                _log_action(act_id, "ad", ad_id, ad_name,
                            "alert", "winner_alert",
                            f"ROAS {ad['avg_roas']:.2f} 超过阈值 {rule['roas_threshold']}")
                _set_cooldown(ad_id, f"scale_{rule_id}")

        elif rule_type == "slow_scale":
            conn = get_conn()
            kpi_row = conn.execute(
                "SELECT target_cpa FROM kpi_configs WHERE act_id=? AND target_id=? AND enabled=1 LIMIT 1",
                (act_id, ad_id)
            ).fetchone()
            conn.close()
            if not kpi_row or not kpi_row["target_cpa"]:
                return

            target_cpa = float(kpi_row["target_cpa"])
            cpa_ratio = rule.get("cpa_ratio", 0.8)
            min_conv = rule.get("min_conversions", 3)
            consecutive_days = rule.get("consecutive_days") or 2
            max_budget = rule.get("max_budget")

            # 连续N天达标检查（参考止损规则_check_consecutive_bad逻辑）
            if not self._check_consecutive_good(act_id, ad_id, target_cpa, cpa_ratio, min_conv, consecutive_days):
                return

            scale_pct = rule.get("scale_pct", 0.15)
            _adset_id = ad.get("adset_id", "")
            if _adset_id and not self.dry_run:
                # 预算上限检查：当前预算已达上限则跳过加量
                if max_budget:
                    try:
                        adset_info = _fb_get(_adset_id, token, {"fields": "daily_budget"})
                        cur_budget_usd = float(adset_info.get("daily_budget", 0)) / 100
                        if cur_budget_usd >= max_budget:
                            _log_action(act_id, "adset", _adset_id, ad_name,
                                        "skip_scale", "slow_scale",
                                        f"当前预算 ${cur_budget_usd:.2f} 已达上限 ${max_budget:.2f}，跳过加量")
                            return
                    except Exception:
                        pass
                ok_s, err_s, old_s, new_s = _update_adset_budget(
                    _adset_id, token, scale_pct, act_id, ad_name
                )
                if ok_s:
                    _set_cooldown(ad_id, f"scale_{rule_id}")
                    _log_action(act_id, "adset", _adset_id, ad_name,
                                "increase_budget", "slow_scale",
                                f"CPA ${ad['avg_cpa']:.2f} 低于目标 {cpa_ratio*100:.0f}%，"
                                f"连续 {consecutive_days} 天达标，"
                                f"预算 ${old_s:.2f}→${new_s:.2f} (+{scale_pct*100:.0f}%)")
                    _send_tg(
                        f"📈 <b>Mira 自动加量</b>\n"
                        f"广告：<code>{ad_name}</code>\n"
                        f"平均CPA：${ad['avg_cpa']:.2f}（目标 ${target_cpa:.2f}）\n"
                        f"转化数：{ad['total_conv']}\n"
                        f"连续达标：{consecutive_days} 天\n"
                        f"预算：${old_s:.2f} → ${new_s:.2f}（+{scale_pct*100:.0f}%）"
                    )
                else:
                    _log_action(act_id, "adset", _adset_id, ad_name,
                                "increase_budget_failed", "slow_scale",
                                f"加量失败: {err_s}")
            else:
                _log_action(act_id, "ad", ad_id, ad_name,
                            "increase_budget", "slow_scale",
                            f"CPA ${ad['avg_cpa']:.2f} 低于目标 {cpa_ratio*100:.0f}%，"
                            f"连续 {consecutive_days} 天达标，加量 {scale_pct*100:.0f}%"
                            + (" [DryRun]" if self.dry_run else " [无广告组ID]"))


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
                "act_id": act_id, "name": acc.get("name", act_id),
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
                "act_id": act_id, "name": acc.get("name", act_id),
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

