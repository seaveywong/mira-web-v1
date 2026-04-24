"""
Mira Dashboard API v2.1
- summary/trend/ads-live 接收 date_from/date_to 固定日期参数
- 巡检引擎使用 FB date_preset(today) 自动按账户本地时区
- 货币统一换算为 USD 汇总
"""
from fastapi import APIRouter, Depends
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.auth import get_current_user
from core.database import get_conn
from datetime import date, timedelta, datetime
import requests as req
import time
from api.accounts import _calc_available_balance

router = APIRouter()
_SUMMARY_CACHE = {}
_SUMMARY_CACHE_TTL = 30

# ─── KPI字段 -> actions字段映射（v1.3.0修复：每个字段只映射到自身，禁止多字段叠加）────────────
_KPI_FIELD_TO_ACTION = {
    # 像素购买：只用像素字段，不叠加 purchase/omni_purchase（它们是同一批购买的不同口径）
    "offsite_conversion.fb_pixel_purchase":                ["offsite_conversion.fb_pixel_purchase"],
    "offsite_conversion.fb_pixel_lead":                    ["offsite_conversion.fb_pixel_lead"],
    "onsite_conversion.lead_grouped":                      ["onsite_conversion.lead_grouped"],
    "onsite_conversion.messaging_conversation_started_7d": ["onsite_conversion.messaging_conversation_started_7d"],
    "onsite_conversion.messaging_first_reply":             ["onsite_conversion.messaging_first_reply"],
    "offsite_conversion.fb_pixel_add_to_cart":             ["offsite_conversion.fb_pixel_add_to_cart"],
    "offsite_conversion.fb_pixel_initiate_checkout":       ["offsite_conversion.fb_pixel_initiate_checkout"],
    "offsite_conversion.fb_pixel_complete_registration":   ["offsite_conversion.fb_pixel_complete_registration"],
    "link_click":                                          ["link_click"],
    "landing_page_view":                                   ["landing_page_view"],
    "post_engagement":                                     ["post_engagement"],
    "page_engagement":                                     ["page_engagement"],
    "video_view":                                          ["video_view"],
    "app_install":                                         ["app_install"],
    "reach":                                               ["reach"],
    # lead 可能有多个来源，但只取最精准的一个
    "lead":                                                ["lead"],
    # purchase 字段：只用 purchase，不叠加
    "purchase":                                            ["purchase"],
    # page_likes
    "page_likes":                                          ["like"],
}

# 默认转化字段（未配置 KPI 时的展示用）：只用像素购买一个字段作为默认
_DEFAULT_CONVERSION_ACTIONS = [
    "offsite_conversion.fb_pixel_purchase",
]


def _count_conversions(actions: list, kpi_field: Optional[str] = None) -> int:
    """根据kpi_field从actions中提取正确的转化数量"""
    if not actions:
        return 0
    target_fields = _KPI_FIELD_TO_ACTION.get(kpi_field, _DEFAULT_CONVERSION_ACTIONS) if kpi_field else _DEFAULT_CONVERSION_ACTIONS
    total = 0
    for a in actions:
        if a.get("action_type") in target_fields:
            total += int(float(a.get("value", 0)))
    return total


# ─── 辅助函数 ──────────────────────────────────────────────────

def _get_token_for_account(acc: dict) -> Optional[str]:
    from core.database import decrypt_token
    conn = get_conn()
    token = None
    if acc.get('token_id'):
        tk = conn.execute(
            'SELECT access_token_enc, status FROM fb_tokens WHERE id=? AND status="active"',
            (acc['token_id'],)
        ).fetchone()
        if tk:
            token = decrypt_token(tk['access_token_enc'])
    if not token:
        token = acc.get('access_token') or ''
    if not token:
        tk = conn.execute(
            'SELECT access_token_enc FROM fb_tokens WHERE status="active" LIMIT 1'
        ).fetchone()
        if tk:
            token = decrypt_token(tk['access_token_enc'])
    conn.close()
    return token or None


def _get_rate(currency: str, conn) -> float:
    if not currency or currency.upper() == "USD":
        return 1.0
    try:
        row = conn.execute(
            "SELECT rate FROM currency_rates WHERE currency=? ORDER BY updated_at DESC LIMIT 1",
            (currency.upper(),)
        ).fetchone()
        return float(row["rate"]) if row else 1.0
    except Exception:
        return 1.0


def _default_dates(date_from: Optional[str], date_to: Optional[str]):
    """如果未传日期，默认今日"""
    today = date.today().isoformat()
    return date_from or today, date_to or today


# ─── 大盘汇总 ─────────────────────────────────────────────────
def _fetch_account_summary(acc: dict, df: str, dt: str) -> dict:
    token = _get_token_for_account(acc)
    acc_name = acc.get("name") or acc["act_id"].replace("act_", "")
    acc_tz = acc.get("timezone") or "UTC"
    currency = (acc.get("currency") or "USD").upper()
    available_balance, _, _ = _calc_available_balance(
        acc.get("balance"),
        acc.get("spend_cap"),
        acc.get("amount_spent"),
        acc.get("spending_limit"),
        currency,
    )

    base_result = {
        "act_id": acc["act_id"],
        "name": acc_name,
        "timezone": acc_tz,
        "currency": currency,
        "spend_usd": 0,
        "conversions": 0,
        "cpa_usd": None,
        "roas": None,
        "available_balance": available_balance,
    }

    if not token:
        return dict(base_result, status="no_token")

    conn = get_conn()
    rate = _get_rate(currency, conn)
    kpi_rows = conn.execute(
        'SELECT target_id, kpi_field FROM kpi_configs WHERE act_id=? AND level="ad" AND enabled=1',
        (acc["act_id"],)
    ).fetchall()
    conn.close()
    kpi_map = {row["target_id"]: row["kpi_field"] for row in kpi_rows}

    try:
        all_ad_items = []
        next_url = f'https://graph.facebook.com/v25.0/{acc["act_id"]}/insights'
        params = {
            "access_token": token,
            "fields": "ad_id,spend,actions,action_values",
            "time_range": f'{{"since":"{df}","until":"{dt}"}}',
            "level": "ad",
            "limit": 200,
        }
        fetched = 0
        while next_url and fetched < 2000:
            resp = req.get(next_url, params=params, timeout=30)
            data = resp.json()
            if "error" in data:
                raise Exception(data["error"].get("message", str(data["error"])))
            items = data.get("data", [])
            all_ad_items.extend(items)
            fetched += len(items)
            paging = data.get("paging", {})
            next_url = paging.get("next")
            params = {}

        spend_orig = 0.0
        conversions = 0
        revenue_orig = 0.0
        for ad_item in all_ad_items:
            ad_id_item = ad_item.get("ad_id", "")
            kpi_field = kpi_map.get(ad_id_item)
            ad_spend = float(ad_item.get("spend", 0) or 0)
            spend_orig += ad_spend
            ad_actions = ad_item.get("actions", [])
            ad_action_values = ad_item.get("action_values", [])
            conversions += _count_conversions(ad_actions, kpi_field)
            if kpi_field and "purchase" in kpi_field:
                for av in ad_action_values:
                    if av.get("action_type") == kpi_field:
                        revenue_orig += float(av.get("value", 0))
                        break
                else:
                    for av in ad_action_values:
                        if av.get("action_type") in (
                            "offsite_conversion.fb_pixel_purchase",
                            "purchase",
                            "omni_purchase",
                        ):
                            revenue_orig += float(av.get("value", 0))
                            break

        spend_usd = round(spend_orig / rate, 2) if rate else spend_orig
        roas = round(revenue_orig / spend_orig, 2) if spend_orig > 0 and revenue_orig > 0 else 0
        cpa_usd = round(spend_usd / conversions, 2) if conversions > 0 else 0
        return dict(
            base_result,
            spend_usd=spend_usd,
            conversions=conversions,
            cpa_usd=cpa_usd if cpa_usd > 0 else None,
            roas=roas if roas > 0 else None,
            status="ok",
        )
    except Exception as e:
        return dict(base_result, status="error", error=str(e))


@router.get("/summary")
def get_summary(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    act_id: Optional[str] = None,
    user=Depends(get_current_user)
):
    """
    大盘汇总 - 实时从FB API拉取
    date_from/date_to: YYYY-MM-DD 固定日期（用户自选）
    默认今日
    """
    df, dt = _default_dates(date_from, date_to)
    server_today = date.today().isoformat()
    cache_key = (df, dt, act_id or "")
    cached = _SUMMARY_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _SUMMARY_CACHE_TTL:
        return dict(cached["data"], source="fb_insights_api_cache")

    conn = get_conn()
    if act_id:
        accs = conn.execute('SELECT * FROM accounts WHERE act_id=?', (act_id,)).fetchall()
    else:
        accs = conn.execute('SELECT * FROM accounts').fetchall()

    # 止损统计：只统计自动止损（排除 emergency 紧急暂停，那是人工操作）
    # trigger_type: guard=巡棄自动, rule=规则触发, system=系统; emergency=紧急暂停(不算止损)
    AUTO_TRIGGERS = "('guard','rule','system','kpi')"
    log_range = conn.execute(
        f"""SELECT
           COUNT(DISTINCT CASE WHEN action_type='pause' AND status='success'
             AND trigger_type NOT IN ('emergency','user') THEN target_id END) as paused_unique,
           COUNT(DISTINCT CASE WHEN action_type='pause' AND status='success'
             AND trigger_type='emergency' THEN target_id END) as emg_unique,
           COUNT(DISTINCT CASE WHEN action_type='pause' AND status='success'
             AND trigger_type NOT IN ('emergency','user') THEN target_id END) as auto_unique,
           COUNT(DISTINCT CASE WHEN action_type='increase_budget' AND status='success' THEN target_id END) as scaled_unique
           FROM action_logs WHERE date(created_at) BETWEEN ? AND ?""",
        (df, dt)
    ).fetchone()
    # 服务器今日自动止损
    log_today = conn.execute(
        """SELECT
           COUNT(DISTINCT CASE WHEN action_type='pause' AND status='success'
             AND trigger_type NOT IN ('emergency','user') THEN target_id END) as paused_today
           FROM action_logs WHERE date(created_at)=?""",
        (server_today,)
    ).fetchone()
    # 历史累计自动止损
    log_total = conn.execute(
        """SELECT COUNT(DISTINCT target_id) as paused_total
           FROM action_logs
           WHERE action_type='pause' AND status='success'
             AND trigger_type NOT IN ('emergency','user')"""
    ).fetchone()
    # 止损明细：JOIN accounts获取账户名称，排除紧急暂停
    pause_details = conn.execute(
        """SELECT l.target_id, l.target_name, l.act_id,
                  COALESCE(a.name, l.act_id) as account_name,
                  l.level, l.trigger_type, MAX(l.created_at) as last_at
           FROM action_logs l
           LEFT JOIN accounts a ON a.act_id = l.act_id
           WHERE l.action_type='pause' AND l.status='success'
             AND l.trigger_type NOT IN ('emergency','user')
             AND date(l.created_at) BETWEEN ? AND ?
           GROUP BY l.target_id
           ORDER BY last_at DESC LIMIT 20""",
        (df, dt)
    ).fetchall()
    # 紧急暂停单独统计（仅用于展示，不计入止损）
    emg_details = conn.execute(
        """SELECT l.target_id, l.target_name, l.act_id,
                  COALESCE(a.name, l.act_id) as account_name,
                  l.level, MAX(l.created_at) as last_at
           FROM action_logs l
           LEFT JOIN accounts a ON a.act_id = l.act_id
           WHERE l.action_type='pause' AND l.status='success'
             AND l.trigger_type='emergency'
             AND date(l.created_at) BETWEEN ? AND ?
           GROUP BY l.target_id
           ORDER BY last_at DESC LIMIT 20""",
        (df, dt)
    ).fetchall()
    conn.close()

    total_spend = 0.0
    total_conversions = 0
    cpa_list = []
    roas_list = []
    account_count = len(accs)
    error_accounts = 0
    account_details = []  # 账户级明细

    account_results = []
    if accs:
        max_workers = min(4, len(accs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_fetch_account_summary, dict(acc), df, dt): dict(acc).get("act_id")
                for acc in accs
            }
            for future in as_completed(future_map):
                act_id_key = future_map[future]
                try:
                    account_results.append(future.result())
                except Exception as e:
                    account_results.append({
                        "act_id": act_id_key,
                        "name": act_id_key.replace("act_", "") if act_id_key else "",
                        "timezone": "UTC",
                        "currency": "USD",
                        "spend_usd": 0,
                        "conversions": 0,
                        "cpa_usd": None,
                        "roas": None,
                        "available_balance": 0,
                        "status": "error",
                        "error": str(e),
                    })

    for account_result in account_results:
        total_spend += account_result["spend_usd"]
        total_conversions += account_result["conversions"]
        if account_result.get("cpa_usd"):
            cpa_list.append(account_result["cpa_usd"])
        if account_result.get("roas"):
            roas_list.append(account_result["roas"])
        if account_result["status"] != "ok":
            error_accounts += 1
        account_details.append(account_result)

    for acc in []:
        acc = dict(acc)
        token = _get_token_for_account(acc)
        acc_name = acc.get('name') or acc['act_id'].replace('act_', '')
        acc_tz = acc.get('timezone') or 'UTC'
        currency = (acc.get('currency') or 'USD').upper()
        # 计算账户可用余额（从数据库字段）
        _avail_bal, _, _ = _calc_available_balance(
            acc.get('balance'), acc.get('spend_cap'),
            acc.get('amount_spent'), acc.get('spending_limit'), currency
        )
        if not token:
            error_accounts += 1
            account_details.append({
                'act_id': acc['act_id'], 'name': acc_name, 'timezone': acc_tz,
                'currency': currency, 'spend_usd': 0,
                'conversions': 0, 'cpa_usd': None, 'roas': None,
                'available_balance': _avail_bal,
                'status': 'no_token'
            })
            continue
        conn2 = get_conn()
        rate = _get_rate(currency, conn2)
        conn2.close()

        try:
            # v2.2.0: 改为广告级拉取，按各广告 kpi_field 精准计算 conversions/CPA/ROAS
            # 1. 从 kpi_configs 读取该账户所有广告的 kpi_field 映射
            kpi_conn = get_conn()
            kpi_rows = kpi_conn.execute(
                'SELECT target_id, kpi_field FROM kpi_configs WHERE act_id=? AND level="ad" AND enabled=1',
                (acc['act_id'],)
            ).fetchall()
            kpi_conn.close()
            kpi_map = {r['target_id']: r['kpi_field'] for r in kpi_rows}

            # 2. 广告级拉取 insights（支持分页，最多 2000 条）
            all_ad_items = []
            next_url = f'https://graph.facebook.com/v25.0/{acc["act_id"]}/insights'
            params = {
                'access_token': token,
                'fields': 'ad_id,spend,actions,action_values',
                'time_range': f'{{"since":"{df}","until":"{dt}"}}',
                'level': 'ad',
                'limit': 200
            }
            fetched = 0
            while next_url and fetched < 2000:
                resp = req.get(next_url, params=params, timeout=30)
                data = resp.json()
                if 'error' in data:
                    raise Exception(data['error'].get('message', str(data['error'])))
                items = data.get('data', [])
                all_ad_items.extend(items)
                fetched += len(items)
                paging = data.get('paging', {})
                next_url = paging.get('next')
                params = {}  # next URL 已含所有参数

            # 3. 按广告级 kpi_field 汇总 spend/conversions/revenue
            spend_orig = 0.0
            conversions = 0
            revenue_orig = 0.0
            for ad_item in all_ad_items:
                ad_id_item = ad_item.get('ad_id', '')
                kpi_field = kpi_map.get(ad_id_item)
                ad_spend = float(ad_item.get('spend', 0) or 0)
                spend_orig += ad_spend
                ad_actions = ad_item.get('actions', [])
                ad_action_values = ad_item.get('action_values', [])
                # 按 kpi_field 精准匹配转化数
                ad_conversions = _count_conversions(ad_actions, kpi_field)
                conversions += ad_conversions
                # ROAS 只对购买类广告有意义（kpi_field 含 purchase）
                if kpi_field and 'purchase' in kpi_field:
                    for av in ad_action_values:
                        if av.get('action_type') == kpi_field:
                            revenue_orig += float(av.get('value', 0))
                            break
                    else:
                        # 兜底：用 offsite_conversion.fb_pixel_purchase 的 action_values
                        for av in ad_action_values:
                            if av.get('action_type') in ('offsite_conversion.fb_pixel_purchase', 'purchase', 'omni_purchase'):
                                revenue_orig += float(av.get('value', 0))
                                break

            spend_usd = round(spend_orig / rate, 2) if rate else spend_orig
            roas = round(revenue_orig / spend_orig, 2) if spend_orig > 0 and revenue_orig > 0 else 0
            cpa_usd = round(spend_usd / conversions, 2) if conversions > 0 else 0
            total_spend += spend_usd
            total_conversions += conversions
            if cpa_usd > 0:
                cpa_list.append(cpa_usd)
            if roas > 0:
                roas_list.append(roas)
            account_details.append({
                'act_id': acc['act_id'], 'name': acc_name, 'timezone': acc_tz,
                'currency': currency, 'spend_usd': spend_usd,
                'conversions': conversions,
                'cpa_usd': cpa_usd if cpa_usd > 0 else None,
                'roas': roas if roas > 0 else None,
                'available_balance': _avail_bal,
                'status': 'ok'
            })
        except Exception as e:
            error_accounts += 1
            account_details.append({
                'act_id': acc['act_id'], 'name': acc_name, 'timezone': acc_tz,
                'currency': currency, 'spend_usd': 0,
                'conversions': 0, 'cpa_usd': None, 'roas': None,
                'available_balance': _avail_bal,
                'status': 'error', 'error': str(e)
            })

    # 按消耗降序排序
    account_details.sort(key=lambda x: x['spend_usd'], reverse=True)
    avg_cpa = round(sum(cpa_list) / len(cpa_list), 2) if cpa_list else None
    avg_roas = round(sum(roas_list) / len(roas_list), 2) if roas_list else None

    result = {
        "date_from": df,
        "date_to": dt,
        "server_today": server_today,
        "account_count": account_count,
        "error_accounts": error_accounts,
        "total_spend": round(total_spend, 2),
        "total_conversions": total_conversions,
        "avg_cpa": avg_cpa,
        "avg_roas": avg_roas,
        # 止损统计（仅自动止损，已排除紧急暂停）
        "paused_in_range": log_range["paused_unique"] or 0,   # 所选日期范围内自动止损（去重）
        "paused_emg": log_range["emg_unique"] or 0,           # 紧急暂停数（不计入止损）
        "paused_auto": log_range["auto_unique"] or 0,         # 自动止损数
        "paused_today": log_today["paused_today"] or 0,       # 服务器今日自动止损
        "paused_total": log_total["paused_total"] or 0,       # 历史累计自动止损
        "scaled_in_range": log_range["scaled_unique"] or 0,
        "pause_details": [dict(r) for r in pause_details],    # 自动止损明细
        "emg_details": [dict(r) for r in emg_details],        # 紧急暂停明细（仅展示）
        "account_details": account_details,                   # 账户级明细（按消耗降序）
        "source": "fb_insights_api",
    }
    _SUMMARY_CACHE[cache_key] = {"ts": time.time(), "data": result}
    return result


# ─── 趋势图 ───────────────────────────────────────────────────
@router.get("/trend")
def get_trend(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    days: Optional[int] = None,
    act_id: Optional[str] = None,
    user=Depends(get_current_user)
):
    """
    近N日趋势 - 实时从FB API拉取
    date_from/date_to: 固定日期（优先）
    days: 如果没有传date_from/date_to，用days计算（默认7）
    """
    if date_from and date_to:
        df, dt = date_from, date_to
    else:
        n = days or 7
        n = max(1, min(n, 90))
        today = date.today()
        df = (today - timedelta(days=n - 1)).isoformat()
        dt = today.isoformat()

    # 生成日期轴
    start = datetime.strptime(df, '%Y-%m-%d').date()
    end = datetime.strptime(dt, '%Y-%m-%d').date()
    day_list = []
    cur = start
    while cur <= end:
        day_list.append(cur.isoformat())
        cur += timedelta(days=1)

    conn = get_conn()
    if act_id:
        accs = conn.execute('SELECT * FROM accounts WHERE act_id=?', (act_id,)).fetchall()
    else:
        accs = conn.execute('SELECT * FROM accounts').fetchall()
    conn.close()

    daily_spend = {d: 0.0 for d in day_list}
    daily_conv = {d: 0 for d in day_list}

    for acc in accs:
        acc = dict(acc)
        token = _get_token_for_account(acc)
        if not token:
            continue
        currency = (acc.get('currency') or 'USD').upper()
        conn2 = get_conn()
        rate = _get_rate(currency, conn2)
        conn2.close()

        try:
            resp = req.get(
                f'https://graph.facebook.com/v25.0/{acc["act_id"]}/insights',
                params={
                    'access_token': token,
                    'fields': 'date_start,spend,actions',
                    'time_range': f'{{"since":"{df}","until":"{dt}"}}',
                    'time_increment': 1,
                    'level': 'account',
                    'limit': 100
                },
                timeout=20
            )
            data = resp.json()
            if 'error' in data:
                continue
            for item in data.get('data', []):
                d = item.get('date_start', '')
                if d not in daily_spend:
                    continue
                spend_orig = float(item.get('spend', 0) or 0)
                spend_usd = round(spend_orig / rate, 2) if rate else spend_orig
                daily_spend[d] += spend_usd
                for a in item.get('actions', []):
                    if a.get('action_type') in ('offsite_conversion.fb_pixel_purchase',
                                                 'purchase', 'omni_purchase',
                                                 'offsite_conversion.fb_pixel_lead', 'lead'):
                        daily_conv[d] += int(float(a.get('value', 0)))
        except Exception:
            continue

    spend_arr = [round(daily_spend[d], 2) for d in day_list]
    cpa_arr = [round(daily_spend[d] / daily_conv[d], 2) if daily_conv[d] > 0 else None for d in day_list]

    return {
        "date_from": df,
        "date_to": dt,
        "labels": [d[5:] for d in day_list],   # MM-DD
        "full_dates": day_list,
        "spend": spend_arr,
        "cpa": cpa_arr,
        "source": "fb_insights_api",
    }


# ─── 广告列表（实时） ─────────────────────────────────────────
@router.get("/ads-live")
def get_ads_live(
    act_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    user=Depends(get_current_user)
):
    """
    从FB API实时拉取广告列表
    date_from/date_to: 固定日期（用户自选），默认近7天
    """
    today = date.today()
    if not date_from:
        date_from = (today - timedelta(days=6)).isoformat()
    if not date_to:
        date_to = today.isoformat()

    tr = '{' + f'"since":"{date_from}","until":"{date_to}"' + '}'
    insights_field = f'insights.time_range({tr}){{spend,impressions,clicks,actions,action_values}}'

    conn = get_conn()
    if act_id:
        accs = conn.execute('SELECT * FROM accounts WHERE act_id=?', (act_id,)).fetchall()
    else:
        accs = conn.execute('SELECT * FROM accounts').fetchall()
    conn.close()

    kpi_conn = get_conn()
    try:
        kpi_rows = kpi_conn.execute('SELECT * FROM kpi_configs WHERE level="ad"').fetchall()
        kpi_map = {r['target_id']: dict(r) for r in kpi_rows}
    except Exception:
        kpi_map = {}
    kpi_conn.close()

    all_ads = []
    for acc in accs:
        acc = dict(acc)
        currency = (acc.get('currency') or 'USD').upper()
        token = _get_token_for_account(acc)
        if not token:
            continue
        conn2 = get_conn()
        rate = _get_rate(currency, conn2)
        conn2.close()

        try:
            fields = f'id,name,status,effective_status,adset_id,campaign_id,{insights_field}'
            resp = req.get(
                f'https://graph.facebook.com/v25.0/{acc["act_id"]}/ads',
                params={'access_token': token, 'fields': fields, 'limit': 200},
                timeout=20
            )
            data = resp.json()
            if 'error' in data:
                continue
            for ad in data.get('data', []):
                ins_data = ad.get('insights', {})
                ins = ins_data.get('data', []) if isinstance(ins_data, dict) else []
                spend_orig = 0.0
                raw_actions = []
                roas = 0.0
                if ins:
                    spend_orig = float(ins[0].get('spend', 0) or 0)
                    raw_actions = ins[0].get('actions', [])
                    revenue = sum(float(v.get('value', 0)) for v in ins[0].get('action_values', [])
                                  if v.get('action_type') in ('offsite_conversion.fb_pixel_purchase',
                                                               'purchase', 'omni_purchase'))
                    roas = round(revenue / spend_orig, 2) if spend_orig > 0 and revenue > 0 else 0
                spend_usd = round(spend_orig / rate, 2) if rate else spend_orig
                # v1.2.0: 根据kpi_field选择正确的转化字段
                kpi = kpi_map.get(ad['id'], {})
                kpi_field = kpi.get('kpi_field')
                conversions = _count_conversions(raw_actions, kpi_field)
                cpa = round(spend_usd / conversions, 2) if conversions > 0 else 0
                all_ads.append({
                    'ad_id': ad['id'],
                    'ad_name': ad.get('name', ad['id']),
                    'act_id': acc['act_id'],
                    'account_name': acc.get('name', ''),
                    'currency': currency,
                    'timezone': acc.get('timezone', 'UTC'),
                    'date_from': date_from,
                    'date_to': date_to,
                    'status': ad.get('status', ''),
                    'effective_status': ad.get('effective_status', ''),
                    'spend': spend_usd,
                    'conversions': conversions,
                    'cpa': cpa,
                    'roas': roas,
                    'adset_id': ad.get('adset_id', ''),
                    'campaign_id': ad.get('campaign_id', ''),
                    'target_cpa': kpi.get('target_cpa'),   # 已是USD
                    'kpi_field': kpi_field,
                    'kpi_label': kpi.get('kpi_label', ''),
                    'kpi_source': kpi.get('source', ''),
                })
        except Exception:
            continue

    all_ads.sort(key=lambda x: x['spend'], reverse=True)
    return all_ads


# ─── 消耗查询（自定义日期） ───────────────────────────────────
@router.get("/spend-query")
def spend_query(
    date_from: str,
    date_to: str,
    account_id: Optional[str] = None,
    user=Depends(get_current_user)
):
    """自定义日期范围消耗查询，固定日期字符串，适用于历史数据查询"""
    conn = get_conn()
    if account_id:
        accs = conn.execute('SELECT * FROM accounts WHERE act_id=?', (account_id,)).fetchall()
    else:
        accs = conn.execute('SELECT * FROM accounts').fetchall()
    conn.close()

    result_rows = []
    total_usd = 0.0
    total_conversions = 0
    total_cpa_list = []
    total_roas_list = []

    for acc in accs:
        acc = dict(acc)
        token = _get_token_for_account(acc)
        if not token:
            continue
        act_id = acc['act_id']
        currency = (acc.get('currency') or 'USD').upper()
        conn2 = get_conn()
        rate = _get_rate(currency, conn2)
        conn2.close()

        try:
            resp = req.get(
                f'https://graph.facebook.com/v25.0/{act_id}/insights',
                params={
                    'access_token': token,
                    'fields': 'date_start,date_stop,spend,impressions,clicks,actions,action_values,cpc,cpm',
                    'time_range': f'{{"since":"{date_from}","until":"{date_to}"}}',
                    'time_increment': 1,
                    'level': 'account',
                    'limit': 100
                },
                timeout=30
            )
            data = resp.json()
            if 'error' in data:
                # API报错时也生成占位行（每天一行$0.00）
                from datetime import date as _date, timedelta
                d_from = _date.fromisoformat(date_from)
                d_to = _date.fromisoformat(date_to)
                cur = d_from
                while cur <= d_to:
                    result_rows.append({
                        'date': cur.isoformat(),
                        'act_id': act_id,
                        'account_name': acc.get('name', act_id),
                        'currency': currency,
                        'timezone': acc.get('timezone', 'UTC'),
                        'spend_orig': 0.0, 'spend_usd': 0.0,
                        'conversions': 0, 'cpa_orig': 0, 'cpa_usd': 0,
                        'roas': 0, 'impressions': 0, 'clicks': 0,
                        'note': data['error'].get('message', 'API Error'),
                    })
                    cur += timedelta(days=1)
                continue
            items = data.get('data', [])
            if not items:
                # FB返回空数据（该时段无消耗），生成占位行
                from datetime import date as _date, timedelta
                d_from = _date.fromisoformat(date_from)
                d_to = _date.fromisoformat(date_to)
                cur = d_from
                while cur <= d_to:
                    result_rows.append({
                        'date': cur.isoformat(),
                        'act_id': act_id,
                        'account_name': acc.get('name', act_id),
                        'currency': currency,
                        'timezone': acc.get('timezone', 'UTC'),
                        'spend_orig': 0.0, 'spend_usd': 0.0,
                        'conversions': 0, 'cpa_orig': 0, 'cpa_usd': 0,
                        'roas': 0, 'impressions': 0, 'clicks': 0,
                    })
                    cur += timedelta(days=1)
            for item in items:
                spend_orig = float(item.get('spend', 0) or 0)
                spend_usd = round(spend_orig / rate, 2) if rate else spend_orig
                actions = item.get('actions', [])
                conversions = 0
                for a in actions:
                    if a.get('action_type') in ('offsite_conversion.fb_pixel_purchase',
                                                 'purchase', 'omni_purchase',
                                                 'offsite_conversion.fb_pixel_lead', 'lead'):
                        conversions += int(float(a.get('value', 0)))
                cpa_orig = round(spend_orig / conversions, 2) if conversions > 0 else 0
                cpa_usd = round(spend_usd / conversions, 2) if conversions > 0 else 0
                action_values = item.get('action_values', [])
                revenue = sum(float(v.get('value', 0)) for v in action_values
                              if v.get('action_type') in ('offsite_conversion.fb_pixel_purchase',
                                                           'purchase', 'omni_purchase'))
                roas = round(revenue / spend_orig, 2) if spend_orig > 0 and revenue > 0 else 0
                row = {
                    'date': item.get('date_start', ''),
                    'act_id': act_id,
                    'account_name': acc.get('name', act_id),
                    'currency': currency,
                    'timezone': acc.get('timezone', 'UTC'),
                    'spend_orig': spend_orig,
                    'spend_usd': spend_usd,
                    'conversions': conversions,
                    'cpa_orig': cpa_orig,
                    'cpa_usd': cpa_usd,
                    'roas': roas,
                    'impressions': int(item.get('impressions', 0) or 0),
                    'clicks': int(item.get('clicks', 0) or 0),
                }
                result_rows.append(row)
                total_usd += spend_usd
                total_conversions += conversions
                if cpa_usd > 0:
                    total_cpa_list.append(cpa_usd)
                if roas > 0:
                    total_roas_list.append(roas)
        except Exception:
            # 请求异常时也生成占位行
            from datetime import date as _date, timedelta
            try:
                d_from = _date.fromisoformat(date_from)
                d_to = _date.fromisoformat(date_to)
                cur = d_from
                while cur <= d_to:
                    result_rows.append({
                        'date': cur.isoformat(),
                        'act_id': act_id,
                        'account_name': acc.get('name', act_id),
                        'currency': currency,
                        'timezone': acc.get('timezone', 'UTC'),
                        'spend_orig': 0.0, 'spend_usd': 0.0,
                        'conversions': 0, 'cpa_orig': 0, 'cpa_usd': 0,
                        'roas': 0, 'impressions': 0, 'clicks': 0,
                        'note': 'Request failed',
                    })
                    cur += timedelta(days=1)
            except Exception:
                pass
            continue

    result_rows.sort(key=lambda x: x['date'], reverse=True)
    avg_cpa = round(sum(total_cpa_list) / len(total_cpa_list), 2) if total_cpa_list else 0
    avg_roas = round(sum(total_roas_list) / len(total_roas_list), 2) if total_roas_list else 0
    return {
        "total_usd": round(total_usd, 2),
        "total_conversions": total_conversions,
        "avg_cpa": avg_cpa,
        "avg_roas": avg_roas,
        "rows": result_rows,
        "source": "fb_insights_api",
        "date_from": date_from,
        "date_to": date_to,
    }


# ─── 其他接口（保持不变） ─────────────────────────────────────
@router.get("/ads")
def get_ads(act_id: Optional[str] = None, user=Depends(get_current_user)):
    today = date.today().isoformat()
    conn = get_conn()
    if act_id:
        rows = conn.execute(
            """SELECT p.*, a.name as account_name, k.target_cpa, k.kpi_field
               FROM perf_snapshots p
               LEFT JOIN accounts a ON a.act_id = p.act_id
               LEFT JOIN kpi_configs k ON k.target_id = p.ad_id AND k.level = 'ad'
               WHERE p.snapshot_date = ? AND p.act_id = ?
               ORDER BY p.spend DESC""", (today, act_id)).fetchall()
    else:
        rows = conn.execute(
            """SELECT p.*, a.name as account_name, k.target_cpa, k.kpi_field
               FROM perf_snapshots p
               LEFT JOIN accounts a ON a.act_id = p.act_id
               LEFT JOIN kpi_configs k ON k.target_id = p.ad_id AND k.level = 'ad'
               WHERE p.snapshot_date = ?
               ORDER BY p.spend DESC""", (today,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/trigger-inspect")
def trigger_inspect(user=Depends(get_current_user)):
    import threading
    from services.guard_engine import GuardEngine
    def run():
        engine = GuardEngine()
        engine.run_all()
    threading.Thread(target=run, daemon=True).start()
    return {"message": "巡检已触发"}


@router.get("/stats")
def get_stats(user=Depends(get_current_user)):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM perf_snapshots ORDER BY snapshot_date DESC LIMIT 100").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/system-logs")
def get_system_logs(lines: int = 100, user=Depends(get_current_user)):
    import os, re
    log_paths = ["/var/log/mira/app.log", "/var/log/mira/error.log", "/opt/mira/logs/app.log", "/opt/mira/app.log", "/var/log/mira.log"]
    content = []
    for path in log_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.readlines()[-lines:]
                break
            except Exception as e:
                content = [f"读取日志失败: {e}"]
    if not content:
        content = ["暂无日志文件，请检查日志路径配置"]
    parsed = []
    for line in content:
        line = line.strip()
        if not line:
            continue
        level = 'info'
        if 'ERROR' in line or 'error' in line.lower():
            level = 'error'
        elif 'WARN' in line or 'warn' in line.lower():
            level = 'warn'
        ts = ''
        msg = line
        m = re.match(r'^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})', line)
        if m:
            ts = m.group(1)
            msg = line[len(ts):].strip(' -|:')
        parsed.append({'ts': ts, 'level': level, 'msg': msg, 'raw': line})
    return parsed


@router.get("/emergency-prescan")
def emergency_prescan(level: str = "campaign", user=Depends(get_current_user)):
    conn = get_conn()
    accs = conn.execute('SELECT * FROM accounts').fetchall()
    conn.close()
    level_label = {"campaign": "广告系列", "adset": "广告组", "ad": "广告"}.get(level, "广告系列")
    fb_endpoint = {"campaign": "campaigns", "adset": "adsets", "ad": "ads"}.get(level, "campaigns")
    scan_results = []
    total_active = 0
    no_token_count = 0
    for acc in accs:
        acc = dict(acc)
        act_id = acc['act_id']
        token = _get_token_for_account(acc)
        if not token:
            no_token_count += 1
            scan_results.append({'act_id': act_id, 'account_name': acc.get('name', act_id),
                                  'active_count': 0, 'items': [],
                                  'error': '无可用Token，需人工处理', 'status': 'no_token'})
            continue
        try:
            eff_status = '["ACTIVE"]' if level == "campaign" else '["ACTIVE","CAMPAIGN_PAUSED"]'
            resp = req.get(
                f'https://graph.facebook.com/v25.0/{act_id}/{fb_endpoint}',
                params={'access_token': token, 'fields': 'id,name,status,effective_status',
                        'effective_status': eff_status, 'limit': 200},
                timeout=20
            )
            data = resp.json()
            if 'error' in data:
                scan_results.append({'act_id': act_id, 'account_name': acc.get('name', act_id),
                                      'active_count': 0, 'items': [],
                                      'error': f"API错误: {data['error'].get('message', '')}",
                                      'status': 'api_error'})
                continue
            items = data.get('data', [])
            active_count = len(items)
            total_active += active_count
            scan_results.append({
                'act_id': act_id, 'account_name': acc.get('name', act_id),
                'active_count': active_count,
                'items': [{'id': i['id'], 'name': i.get('name', i['id']),
                           'status': i.get('effective_status', '')} for i in items[:20]],
                'error': None, 'status': 'ok'
            })
        except Exception as e:
            scan_results.append({'act_id': act_id, 'account_name': acc.get('name', act_id),
                                  'active_count': 0, 'items': [], 'error': str(e), 'status': 'error'})
    return {
        'level': level, 'level_label': level_label,
        'total_accounts': len(scan_results),
        'no_token_count': no_token_count,
        'total_active': total_active,
        'accounts': scan_results
    }


# ─── 批量设定CPA（v1.2.0新增）─────────────────────────────────────────────
from pydantic import BaseModel as _BaseModel
from typing import List as _List

class _CpaItem(_BaseModel):
    act_id: str
    level: str = "ad"
    target_id: str
    target_cpa: float
    kpi_field: Optional[str] = None

class _BatchCpaRequest(_BaseModel):
    items: _List[_CpaItem]

class _DefaultCpaItem(_BaseModel):
    act_id: str
    kpi_field: str
    target_cpa: float
    note: Optional[str] = None


@router.post("/batch-set-cpa")
def batch_set_cpa(req_body: _BatchCpaRequest, user=Depends(get_current_user)):
    """
    批量设定CPA目标（v1.2.0新增）
    支持按广告/广告组/广告系列/账户级别批量设定
    target_cpa 单位为 USD
    """
    conn = get_conn()
    updated = 0
    created = 0
    errors = []
    for item in req_body.items:
        try:
            existing = conn.execute(
                "SELECT id FROM kpi_configs WHERE act_id=? AND target_id=? AND level=?",
                (item.act_id, item.target_id, item.level)
            ).fetchone()
            if existing:
                if item.kpi_field:
                    from services.kpi_resolver import get_kpi_label
                    conn.execute(
                        """UPDATE kpi_configs SET target_cpa=?, kpi_field=?, kpi_label=?,
                           updated_at=datetime('now') WHERE act_id=? AND target_id=? AND level=?""",
                        (item.target_cpa, item.kpi_field, get_kpi_label(item.kpi_field),
                         item.act_id, item.target_id, item.level)
                    )
                else:
                    conn.execute(
                        """UPDATE kpi_configs SET target_cpa=?,
                           updated_at=datetime('now') WHERE act_id=? AND target_id=? AND level=?""",
                        (item.target_cpa, item.act_id, item.target_id, item.level)
                    )
                updated += 1
            else:
                from services.kpi_resolver import get_kpi_label
                kpi_field = item.kpi_field or "link_click"
                conn.execute(
                    """INSERT INTO kpi_configs
                       (act_id, level, target_id, kpi_field, kpi_label, target_cpa, source, enabled)
                       VALUES (?,?,?,?,?,?,'manual',1)""",
                    (item.act_id, item.level, item.target_id,
                     kpi_field, get_kpi_label(kpi_field), item.target_cpa)
                )
                created += 1
        except Exception as e:
            errors.append({"target_id": item.target_id, "error": str(e)})
    conn.commit()
    conn.close()
    return {"updated": updated, "created": created, "errors": errors}


@router.get("/default-cpa")
def get_default_cpa(act_id: Optional[str] = None, user=Depends(get_current_user)):
    """
    获取默认精细化CPA配置（v1.2.0新增）
    按账户+KPI字段维度存储默认CPA
    """
    conn = get_conn()
    if act_id:
        rows = conn.execute(
            """SELECT * FROM kpi_configs WHERE level='account' AND act_id=? ORDER BY updated_at DESC""",
            (act_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM kpi_configs WHERE level='account' ORDER BY act_id, updated_at DESC"""
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── 账户健康度大盘（v2.1.0新增）─────────────────────────────────────────────
@router.get("/health")
def get_account_health(user=Depends(get_current_user)):
    """
    账户健康度红绿灯大盘接口（v2.1.0）
    返回所有账户的今日健康状态：
    - green: 正常（有消耗且有转化，CPA 达标）
    - yellow: 预警（有消耗但转化偏少，或有防瞬烧/预警日志）
    - red: 已止损（今日有广告被自动暂停）
    - grey: 无数据（无消耗或无 Token）
    每个账户附带今日触发止损的广告列表
    """
    today = date.today().isoformat()
    conn = get_conn()
    accs = conn.execute('SELECT * FROM accounts').fetchall()

    # 今日止损日志（按账户聚合）
    paused_logs = conn.execute(
        """SELECT act_id, target_id, target_name, level, trigger_type,
                  trigger_detail, MAX(created_at) as last_at
           FROM action_logs
           WHERE action_type='pause' AND status='success'
             AND trigger_type NOT IN ('emergency','user')
             AND date(created_at)=?
           GROUP BY act_id, target_id
           ORDER BY last_at DESC""",
        (today,)
    ).fetchall()

    # 今日防瞬烧预警日志（按账户聚合）
    burn_alerts = conn.execute(
        """SELECT act_id, MAX(created_at) as last_at
           FROM action_logs
           WHERE action_type='alert' AND trigger_type='burn_rate'
             AND date(created_at)=?
           GROUP BY act_id""",
        (today,)
    ).fetchall()
    burn_alert_acts = {r['act_id'] for r in burn_alerts}

    # 今日 perf_snapshots 汇总（账户级）
    snapshots = conn.execute(
        """SELECT act_id,
                  SUM(spend) as total_spend,
                  SUM(conversions) as total_conv,
                  AVG(cpa) as avg_cpa,
                  AVG(roas) as avg_roas,
                  COUNT(DISTINCT ad_id) as ad_count
           FROM perf_snapshots
           WHERE snapshot_date=?
           GROUP BY act_id""",
        (today,)
    ).fetchall()
    conn.close()

    snap_map = {r['act_id']: dict(r) for r in snapshots}

    # 按账户聚合止损广告列表
    paused_map = {}
    for log in paused_logs:
        act_id = log['act_id']
        if act_id not in paused_map:
            paused_map[act_id] = []
        paused_map[act_id].append({
            'target_id': log['target_id'],
            'target_name': log['target_name'],
            'level': log['level'],
            'trigger_type': log['trigger_type'],
            'trigger_detail': log['trigger_detail'],
            'last_at': log['last_at'],
        })

    result = []
    for acc in accs:
        acc = dict(acc)
        act_id = acc['act_id']
        acc_name = acc.get('name') or act_id
        snap = snap_map.get(act_id, {})
        paused_ads = paused_map.get(act_id, [])
        has_burn_alert = act_id in burn_alert_acts

        total_spend = float(snap.get('total_spend') or 0)
        total_conv = float(snap.get('total_conv') or 0)
        avg_cpa = snap.get('avg_cpa')
        avg_roas = snap.get('avg_roas')
        ad_count = int(snap.get('ad_count') or 0)

        # 健康度判断逻辑
        if total_spend == 0 and ad_count == 0:
            health = 'grey'   # 无数据
            health_label = '无数据'
        elif paused_ads:
            health = 'red'    # 今日有广告被止损
            health_label = f'已止损 {len(paused_ads)} 条广告'
        elif has_burn_alert:
            health = 'yellow' # 防瞬烧预警
            health_label = '防瞬烧预警'
        elif total_spend > 0 and total_conv == 0:
            health = 'yellow' # 有消耗无转化
            health_label = '有消耗无转化'
        elif total_spend > 0 and total_conv > 0:
            health = 'green'  # 正常
            health_label = '正常'
        else:
            health = 'grey'
            health_label = '无数据'

        result.append({
            'act_id': act_id,
            'name': acc_name,
            'currency': acc.get('currency', 'USD'),
            'timezone': acc.get('timezone', 'UTC'),
            'health': health,
            'health_label': health_label,
            'total_spend_usd': round(total_spend, 2),
            'total_conversions': int(total_conv),
            'avg_cpa_usd': round(float(avg_cpa), 2) if avg_cpa else None,
            'avg_roas': round(float(avg_roas), 2) if avg_roas else None,
            'ad_count': ad_count,
            'paused_ads': paused_ads,
            'has_burn_alert': has_burn_alert,
            'updated_at': today,
        })

    # 排序：red > yellow > green > grey
    order = {'red': 0, 'yellow': 1, 'green': 2, 'grey': 3}
    result.sort(key=lambda x: order.get(x['health'], 4))
    return result


@router.post("/default-cpa")
def set_default_cpa(item: _DefaultCpaItem, user=Depends(get_current_user)):
    """
    设定默认精细化CPA（v1.2.0新增）
    act_id="*" 表示全局默认，否则为账户级默认
    同一账户+KPI字段组合唯一
    """
    from services.kpi_resolver import get_kpi_label
    conn = get_conn()
    target_id = f"__default__{item.kpi_field}"
    existing = conn.execute(
        "SELECT id FROM kpi_configs WHERE act_id=? AND target_id=? AND level='account'",
        (item.act_id, target_id)
    ).fetchone()
    if existing:
        conn.execute(
            """UPDATE kpi_configs SET target_cpa=?, note=?, updated_at=datetime('now')
               WHERE act_id=? AND target_id=? AND level='account'""",
            (item.target_cpa, item.note, item.act_id, target_id)
        )
    else:
        conn.execute(
            """INSERT INTO kpi_configs
               (act_id, level, target_id, kpi_field, kpi_label, target_cpa, source, note, enabled)
               VALUES (?,'account',?,?,?,?,'manual',?,1)""",
            (item.act_id, target_id, item.kpi_field,
             get_kpi_label(item.kpi_field), item.target_cpa, item.note)
        )
    conn.commit()
    conn.close()
    return {"status": "ok", "act_id": item.act_id, "kpi_field": item.kpi_field, "target_cpa": item.target_cpa}
