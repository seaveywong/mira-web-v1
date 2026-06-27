"""
Mira Dashboard API v2.1
- summary/trend/ads-live 接收 date_from/date_to 固定日期参数
- 巡检引擎使用 FB date_preset(today) 自动按账户本地时区
- 货币统一换算为 USD 汇总
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock, Thread
from core.auth import get_current_user, is_superadmin
from core.database import get_conn
from core.account_access import is_read_blocking_status, note_account_read_failure, note_account_read_success
from core.tenancy import apply_account_owner_scope, apply_team_scope, assert_row_access
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
import requests as req
import time
import json
from api.accounts import _calc_available_balance
from core.perf_history import ensure_perf_snapshot_history_schema
from services.token_manager import (
    ACTION_READ,
    TOKEN_SOURCE_OAUTH_USER,
    TOKEN_SOURCE_SYSTEM_USER,
    get_exec_token,
    is_operate_token_eligible,
)
from services.guard_engine import _get_kpi_aliases, _get_kpi_fallback_aliases, _get_setting, _local_per_usd_rate
from api.landing_pages import _ad_link_stats

router = APIRouter()
_SUMMARY_CACHE = {}
_SUMMARY_CACHE_TTL = 30
_ADS_LIVE_LOCKS = {}
_ADS_LIVE_LOCKS_GUARD = Lock()
_META_SPEND_RECONCILE_LOCK = Lock()
_META_SPEND_RECONCILE_STOP = Event()
_META_SPEND_RECONCILE_THREAD = None


def _require_superadmin_user(user):
    if not is_superadmin(user):
        raise HTTPException(status_code=403, detail="Superadmin only")


def _require_operator_user(user):
    if not isinstance(user, dict) or user.get("role") not in ("superadmin", "admin", "operator"):
        raise HTTPException(status_code=403, detail="Operator permission required")


def _fetch_visible_accounts(conn, user, act_id: Optional[str] = None):
    where, params = [], []
    if act_id:
        where.append("act_id=?")
        params.append(act_id)
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    apply_account_owner_scope(where, params, user, "owner_user_id")
    sql = "SELECT * FROM accounts"
    if where:
        sql += " WHERE " + " AND ".join(where)
    return conn.execute(sql, params).fetchall()


def _ensure_ads_live_cache_schema(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS ads_live_cache (
               act_id TEXT NOT NULL,
               date_from TEXT NOT NULL,
               date_to TEXT NOT NULL,
               payload_json TEXT NOT NULL,
               updated_ts INTEGER NOT NULL,
               updated_at TEXT DEFAULT (datetime('now','+8 hours')),
               PRIMARY KEY(act_id, date_from, date_to)
           )"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_live_cache_act ON ads_live_cache(act_id)")


def _ads_live_lock_key(act_id: str, date_from: str, date_to: str) -> str:
    return "|".join([str(act_id or ""), str(date_from or ""), str(date_to or "")])


def _ads_live_lock_for(act_id: str, date_from: str, date_to: str):
    key = _ads_live_lock_key(act_id, date_from, date_to)
    with _ADS_LIVE_LOCKS_GUARD:
        lock = _ADS_LIVE_LOCKS.get(key)
        if lock is None:
            lock = Lock()
            _ADS_LIVE_LOCKS[key] = lock
        return lock


def _ads_live_now_text() -> str:
    return (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


def _ads_live_cache_get(conn, act_id: str, date_from: str, date_to: str):
    _ensure_ads_live_cache_schema(conn)
    row = conn.execute(
        """SELECT payload_json, updated_ts, updated_at
           FROM ads_live_cache
           WHERE act_id=? AND date_from=? AND date_to=?""",
        (act_id, date_from, date_to),
    ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(row["payload_json"] or "[]")
        if not isinstance(payload, list):
            return None
    except Exception:
        return None
    age = max(0, int(time.time()) - int(row["updated_ts"] or 0))
    updated_at = row["updated_at"] or ""
    for item in payload:
        if isinstance(item, dict):
            item["ads_live_cached"] = True
            item["ads_live_synced_at"] = updated_at
            item["ads_live_cache_age_seconds"] = age
    return payload


def _ads_live_cache_set(conn, act_id: str, date_from: str, date_to: str, rows: list[dict]):
    _ensure_ads_live_cache_schema(conn)
    now_ts = int(time.time())
    synced_at = _ads_live_now_text()
    payload = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        item.pop("landing", None)
        item.pop("page_brief", None)
        item["ads_live_cached"] = False
        item["ads_live_synced_at"] = synced_at
        item["ads_live_cache_age_seconds"] = 0
        payload.append(item)
    conn.execute(
        "DELETE FROM ads_live_cache WHERE act_id=? AND NOT (date_from=? AND date_to=?)",
        (act_id, date_from, date_to),
    )
    conn.execute(
        """INSERT INTO ads_live_cache(act_id,date_from,date_to,payload_json,updated_ts,updated_at)
           VALUES(?,?,?,?,?,datetime('now','+8 hours'))
           ON CONFLICT(act_id,date_from,date_to) DO UPDATE SET
             payload_json=excluded.payload_json,
             updated_ts=excluded.updated_ts,
             updated_at=datetime('now','+8 hours')""",
        (act_id, date_from, date_to, json.dumps(payload, ensure_ascii=False), now_ts),
    )
    conn.commit()


def _ads_live_cached_for_accounts(conn, accounts: list[dict], date_from: str, date_to: str):
    cached_rows = []
    for acc in accounts or []:
        hit = _ads_live_cache_get(conn, acc.get("act_id"), date_from, date_to)
        if hit is None:
            return None
        cached_rows.extend(hit)
    return cached_rows


def invalidate_ads_live_cache(act_id: Optional[str] = None):
    conn = get_conn()
    try:
        _ensure_ads_live_cache_schema(conn)
        if act_id:
            conn.execute("DELETE FROM ads_live_cache WHERE act_id=?", (act_id,))
        else:
            conn.execute("DELETE FROM ads_live_cache")
        conn.commit()
    finally:
        conn.close()


def _patch_ads_live_cache_payload(act_id: str, patcher) -> bool:
    if not act_id:
        return False
    conn = get_conn()
    touched_any = False
    try:
        _ensure_ads_live_cache_schema(conn)
        rows = conn.execute(
            "SELECT act_id,date_from,date_to,payload_json FROM ads_live_cache WHERE act_id=?",
            (act_id,),
        ).fetchall()
        now_ts = int(time.time())
        for cache_row in rows:
            try:
                payload = json.loads(cache_row["payload_json"] or "[]")
            except Exception:
                continue
            if not isinstance(payload, list):
                continue
            touched = False
            for item in payload:
                if isinstance(item, dict) and patcher(item):
                    touched = True
            if not touched:
                continue
            conn.execute(
                """UPDATE ads_live_cache
                   SET payload_json=?, updated_ts=?, updated_at=datetime('now','+8 hours')
                   WHERE act_id=? AND date_from=? AND date_to=?""",
                (
                    json.dumps(payload, ensure_ascii=False),
                    now_ts,
                    cache_row["act_id"],
                    cache_row["date_from"],
                    cache_row["date_to"],
                ),
            )
            touched_any = True
        if touched_any:
            conn.commit()
        return touched_any
    finally:
        conn.close()


def patch_ads_live_cache_status(act_id: str, level: str, target_id: str, desired_status: str, result: Optional[dict] = None) -> bool:
    actual = desired_status
    if isinstance(result, dict):
        actual = result.get("effective_status") or result.get("actual_status") or result.get("status") or actual
    level = (level or "").lower().strip()

    def patcher(item: dict) -> bool:
        if level == "campaign" and str(item.get("campaign_id") or "") == str(target_id):
            item["campaign_status"] = actual
            item["campaign_effective_status"] = actual
            if actual != "ACTIVE":
                item["effective_status"] = actual
            elif item.get("adset_status") == "ACTIVE" and item.get("status") == "ACTIVE":
                item["effective_status"] = "ACTIVE"
            return True
        if level == "adset" and str(item.get("adset_id") or "") == str(target_id):
            item["adset_status"] = actual
            item["adset_effective_status"] = actual
            if actual != "ACTIVE":
                item["effective_status"] = actual
            elif item.get("campaign_status") == "ACTIVE" and item.get("status") == "ACTIVE":
                item["effective_status"] = "ACTIVE"
            return True
        if level == "ad" and str(item.get("ad_id") or "") == str(target_id):
            item["status"] = actual
            item["effective_status"] = actual
            return True
        return False

    return _patch_ads_live_cache_payload(act_id, patcher)


def patch_ads_live_cache_budget(act_id: str, level: str, target_id: str, daily_budget, result: Optional[dict] = None) -> bool:
    amount = daily_budget
    currency = None
    if isinstance(result, dict):
        amount = result.get("daily_budget") if result.get("daily_budget") is not None else result.get("budget", amount)
        currency = result.get("currency")
    level = (level or "").lower().strip()

    def patcher(item: dict) -> bool:
        if level == "campaign" and str(item.get("campaign_id") or "") == str(target_id):
            item["campaign_daily_budget"] = amount
            if currency:
                item["currency"] = currency
            return True
        if level == "adset" and str(item.get("adset_id") or "") == str(target_id):
            item["adset_daily_budget"] = amount
            if currency:
                item["currency"] = currency
            return True
        return False

    return _patch_ads_live_cache_payload(act_id, patcher)


def _act_id_filter_sql(act_ids: list[str], column: str):
    if not act_ids:
        return " AND 1=0", []
    placeholders = ",".join("?" for _ in act_ids)
    return f" AND {column} IN ({placeholders})", list(act_ids)


def _landing_ad_metrics_for_ads(
    conn,
    ad_ids: list[str],
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    spend_by_ad: Optional[dict[str, float]] = None,
) -> dict[str, dict]:
    clean = []
    seen = set()
    for ad_id in ad_ids or []:
        raw = str(ad_id or "").strip()
        if raw and raw not in seen:
            seen.add(raw)
            clean.append(raw)
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    try:
        rows = conn.execute(
            f"""SELECT l.*,
                       b.ad_id AS binding_ad_id,
                       p.title AS page_title,
                       p.status AS landing_page_status,
                       p.pages_url AS landing_pages_url,
                       p.public_url AS landing_public_url,
                       p.custom_domain AS landing_custom_domain,
                       p.health_status AS landing_health_status,
                       p.health_summary AS landing_health_summary
                FROM landing_ad_links l
                LEFT JOIN landing_ad_link_bindings b ON b.link_id=l.id
                LEFT JOIN landing_pages p ON p.id=l.page_id
                WHERE (l.ad_id IN ({placeholders}) OR b.ad_id IN ({placeholders}))
                  AND COALESCE(l.status,'') NOT IN ('archived','failed','unused')
                ORDER BY l.id DESC, b.last_seen_at DESC""",
            clean + clean,
        ).fetchall()
    except Exception:
        return {}
    out = {}
    for row in rows:
        ad_id = row["binding_ad_id"] or row["ad_id"]
        if not ad_id or ad_id in out:
            continue
        try:
            stats = _ad_link_stats(
                conn,
                int(row["page_id"]),
                row["slug"],
                ad_id=ad_id,
                date_from=date_from,
                date_to=date_to,
            )
        except Exception:
            stats = {}
        final_true_contact = stats.get("final_true_contact", stats.get("effective_true_contact", stats.get("true_contact", 0))) or 0
        row_spend = None
        if spend_by_ad and ad_id in spend_by_ad:
            try:
                row_spend = float(spend_by_ad.get(ad_id) or 0)
            except Exception:
                row_spend = None
        cost_per_final = stats.get("cost_per_final_true_contact") or stats.get("cost_per_effective_true_contact") or stats.get("cost_per_true_contact")
        if row_spend is not None:
            try:
                n = float(final_true_contact or 0)
                cost_per_final = round(row_spend / n, 4) if row_spend > 0 and n > 0 else None
            except Exception:
                cost_per_final = None
        out[ad_id] = {
            "link_id": int(row["id"]),
            "page_id": int(row["page_id"]),
            "page_title": row["page_title"] or "",
            "landing_page_status": row["landing_page_status"] or "",
            "landing_pages_url": row["landing_pages_url"] or "",
            "landing_public_url": row["landing_public_url"] or "",
            "landing_custom_domain": row["landing_custom_domain"] or "",
            "landing_health_status": row["landing_health_status"] or "",
            "landing_health_summary": row["landing_health_summary"] or "",
            "slug": row["slug"],
            "public_url": row["public_url"] or "",
            "target_url": row["target_url"] or "",
            "target_urls": _json_loads(row["target_urls"], []),
            "status": row["status"] or "",
            "spend": row_spend if row_spend is not None else stats.get("spend", 0),
            "spend_source": "ads_live" if row_spend is not None else stats.get("spend_source", ""),
            "attribution_ad_count": stats.get("attribution_ad_count", 0),
            "attribution_mode": stats.get("attribution_mode", "ad"),
            "final_true_contact": final_true_contact,
            "final_metric_mode": stats.get("final_metric_mode", stats.get("metric_mode", "raw")),
            "cost_per_final_true_contact": cost_per_final,
            "confirmed_actions": stats.get("confirmed_actions", 0),
            "confirmed_sales": stats.get("confirmed_sales", 0),
            "confirmed_revenue": stats.get("confirmed_revenue", 0),
            "unique_true_contact": stats.get("unique_true_contact", 0),
            "true_contact": stats.get("true_contact", 0),
            "has_confirmed_result": bool(stats.get("has_confirmed_result")),
            "result_note": stats.get("confirmed_result_note", ""),
            "result_date": stats.get("confirmed_result_date", ""),
            "result_updated_at": stats.get("confirmed_result_updated_at"),
        }
    return out


def _json_loads(raw, default):
    try:
        if raw in (None, ""):
            return default
        value = json.loads(raw) if isinstance(raw, str) else raw
        return value if isinstance(value, type(default)) else default
    except Exception:
        return default


def _extract_ad_page_id(ad: dict) -> str:
    creative = ad.get("creative") if isinstance(ad.get("creative"), dict) else {}
    spec = creative.get("object_story_spec") if isinstance(creative.get("object_story_spec"), dict) else {}
    for key in ("page_id", "actor_id"):
        value = str(spec.get(key) or "").strip()
        if value:
            return value
    return ""


def _page_brief_map(conn, page_ids: list[str]) -> dict[str, dict]:
    clean = []
    seen = set()
    for page_id in page_ids or []:
        value = str(page_id or "").strip()
        if value and value not in seen:
            seen.add(value)
            clean.append(value)
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    out: dict[str, dict] = {}
    try:
        page_columns = {r["name"] for r in conn.execute("PRAGMA table_info(tw_certified_pages)").fetchall()}
        metric_selects = []
        for column in (
            "page_category",
            "page_fan_count",
            "page_followers_count",
            "page_metrics_status",
            "page_metrics_hint",
        ):
            metric_selects.append(column if column in page_columns else f"NULL AS {column}")
        rows = conn.execute(
            f"""SELECT page_id, page_name, verified_identity_id, page_status, page_status_hint,
                       {', '.join(metric_selects)},
                       page_is_published, page_can_advertise, page_lead_form_status,
                       page_status_checked_at
                FROM tw_certified_pages
                WHERE page_id IN ({placeholders})""",
            clean,
        ).fetchall()
        for row in rows:
            status = (row["page_status"] or "unknown").strip().lower()
            if row["page_is_published"] == 0:
                status = "unpublished"
            elif row["page_can_advertise"] == 0 and status in ("", "ok", "unknown"):
                status = "restricted"
            out[str(row["page_id"])] = {
                "page_id": str(row["page_id"] or ""),
                "page_name": row["page_name"] or "",
                "verified_identity_id": row["verified_identity_id"] or "",
                "page_status": status or "unknown",
                "page_status_hint": row["page_status_hint"] or "",
                "page_category": row["page_category"] or "",
                "page_fan_count": row["page_fan_count"],
                "page_followers_count": row["page_followers_count"],
                "page_metrics_status": row["page_metrics_status"] or "unavailable",
                "page_metrics_hint": row["page_metrics_hint"] or "",
                "page_is_published": row["page_is_published"],
                "page_can_advertise": row["page_can_advertise"],
                "page_lead_form_status": row["page_lead_form_status"] or "",
                "page_status_checked_at": row["page_status_checked_at"] or "",
                "source": "tw_certified_pages",
            }
    except Exception:
        return out
    return out

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
    "lead":                                                ["lead", "offsite_conversion.fb_pixel_lead"],
    # purchase 字段：只用 purchase，不叠加
    "purchase":                                            ["purchase"],
    # page_likes
    "page_likes":                                          ["like"],
    # contact
    "contact":                                             ["contact", "offsite_conversion.fb_pixel_contact"],
}

# 默认转化字段（未配置 KPI 时的展示用）：按优先级取一个字段，避免 purchase/offsite/omni 重复相加
_DEFAULT_CONVERSION_ACTIONS = [
    "purchase",
    "offsite_conversion.fb_pixel_purchase",
]


def _first_action_value(actions: list, action_types: list[str]) -> float:
    if not actions or not action_types:
        return 0.0
    for action_type in action_types:
        for action in actions:
            if action.get("action_type") == action_type:
                try:
                    return float(action.get("value", 0) or 0)
                except (TypeError, ValueError):
                    return 0.0
    return 0.0


def _count_conversions(actions: list, kpi_field: Optional[str] = None) -> int:
    """根据kpi_field从actions中提取正确的转化数量"""
    if not actions:
        return 0
    if kpi_field:
        try:
            value = _first_action_value(actions, _get_kpi_aliases(kpi_field) or [])
            if value:
                return int(value)
            if _get_setting("kpi_allow_fallback_alias_count", "0") == "1":
                return int(_first_action_value(actions, _get_kpi_fallback_aliases(kpi_field) or []))
            return 0
        except Exception:
            return int(_first_action_value(actions, _KPI_FIELD_TO_ACTION.get(kpi_field, [kpi_field])))
    return int(_first_action_value(actions, _DEFAULT_CONVERSION_ACTIONS))


def _count_revenue(action_values: list, kpi_field: Optional[str] = None) -> float:
    if not action_values:
        return 0.0
    if kpi_field:
        try:
            value = _first_action_value(action_values, _get_kpi_aliases(kpi_field) or [])
            if value:
                return value
            if _get_setting("kpi_allow_fallback_alias_count", "0") == "1":
                return _first_action_value(action_values, _get_kpi_fallback_aliases(kpi_field) or [])
            return 0.0
        except Exception:
            return _first_action_value(action_values, _KPI_FIELD_TO_ACTION.get(kpi_field, [kpi_field]))
    return _first_action_value(action_values, _DEFAULT_CONVERSION_ACTIONS)


def _score_ad_performance(
    spend_usd: float,
    conversions: int,
    clicks: int,
    impressions: int,
    reach: int,
    cpa: float,
    target_cpa,
    roas: float,
    kpi_field: Optional[str],
) -> dict:
    spend_usd = float(spend_usd or 0)
    conversions = int(conversions or 0)
    clicks = int(clicks or 0)
    impressions = int(impressions or 0)
    reach = int(reach or 0)
    ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
    score = 50
    reasons: list[str] = []

    if kpi_field:
        reasons.append(f"目标：{kpi_field}")
    else:
        score -= 8
        reasons.append("未配置 KPI，评分可信度降低")

    try:
        target = float(target_cpa) if target_cpa not in (None, "") else None
    except (TypeError, ValueError):
        target = None

    if conversions > 0:
        score += 20
        reasons.append(f"已有 {conversions} 个目标成效")
        if target and cpa:
            if cpa <= target:
                score += 18
                reasons.append(f"CPA ${cpa:.2f} 低于目标 ${target:.2f}")
            elif cpa <= target * 1.3:
                score += 6
                reasons.append(f"CPA ${cpa:.2f} 略高于目标 ${target:.2f}")
            else:
                score -= 18
                reasons.append(f"CPA ${cpa:.2f} 明显高于目标 ${target:.2f}")
    else:
        if spend_usd >= 20:
            score -= 32
            reasons.append(f"消耗 ${spend_usd:.2f} 仍无目标成效")
        elif spend_usd >= 10:
            score -= 18
            reasons.append(f"消耗 ${spend_usd:.2f} 暂无目标成效")
        elif spend_usd > 0:
            score -= 4
            reasons.append("仍在早期消耗观察区间")

    if impressions >= 100:
        if ctr >= 2.0:
            score += 12
            reasons.append(f"CTR {ctr:.2f}% 较好")
        elif ctr >= 1.0:
            score += 5
            reasons.append(f"CTR {ctr:.2f}% 可观察")
        elif ctr < 0.5:
            score -= 12
            reasons.append(f"CTR {ctr:.2f}% 偏低")
    elif spend_usd > 0:
        reasons.append("曝光样本较少")

    if conversions == 0 and clicks >= 20:
        score -= 14
        reasons.append(f"{clicks} 次点击仍无目标成效")
    elif conversions == 0 and clicks >= 10:
        score -= 7
        reasons.append(f"{clicks} 次点击暂无目标成效")

    if conversions == 0 and reach >= 1000 and spend_usd >= 10:
        score -= 8
        reasons.append(f"覆盖 {reach} 人仍无目标成效")

    if roas:
        if roas >= 2:
            score += 8
            reasons.append(f"ROAS {roas:.2f}x 表现较好")
        elif roas < 1:
            score -= 8
            reasons.append(f"ROAS {roas:.2f}x 偏低")

    score = max(0, min(100, int(round(score))))
    label = "优秀" if score >= 80 else ("观察" if score >= 60 else ("风险" if score >= 40 else "较差"))
    level = "good" if score >= 80 else ("warn" if score >= 60 else ("bad" if score >= 40 else "critical"))
    return {
        "score": score,
        "label": label,
        "level": level,
        "ctr": round(ctr, 2),
        "reasons": reasons[:6],
    }


_DASH_KPI_FILTERS = (
    "purchase",
    "lead",
    "add_to_cart",
    "initiate_checkout",
    "complete_registration",
    "messaging",
    "view_content",
    "other",
)
_DASH_KPI_MAIN_TOKENS = (
    "purchase",
    "lead",
    "add_to_cart",
    "initiate_checkout",
    "complete_registration",
    "messaging",
    "messenger",
    "conversation",
    "view_content",
)


def _normalize_dash_kpi_filter(kpi_filter: Optional[str]) -> str:
    value = (kpi_filter or "").strip().lower()
    return value if value in _DASH_KPI_FILTERS else ""


def _kpi_field_matches_filter(kpi_field: Optional[str], kpi_filter: Optional[str]) -> bool:
    value = _normalize_dash_kpi_filter(kpi_filter)
    if not value:
        return True
    field = (kpi_field or "").lower()
    if value == "other":
        return not any(token in field for token in _DASH_KPI_MAIN_TOKENS)
    if value == "messaging":
        return any(token in field for token in ("messaging", "messenger", "conversation"))
    return value in field


# ─── 辅助函数 ──────────────────────────────────────────────────

def _get_token_for_account(acc: dict) -> Optional[str]:
    from core.database import decrypt_token
    try:
        if acc.get("act_id"):
            token = get_exec_token(acc["act_id"], ACTION_READ, notify_exhausted=False)
            if token:
                return token
    except Exception:
        pass
    conn = get_conn()
    token = None
    account_team_id = acc.get("team_id")
    if acc.get('token_id'):
        if account_team_id is None:
            team_sql, team_params = "AND team_id IS NULL", []
        else:
            team_sql, team_params = "AND team_id=?", [account_team_id]
        tk = conn.execute(
            f'SELECT access_token_enc, status FROM fb_tokens WHERE id=? AND status="active" {team_sql}',
            [acc['token_id']] + team_params,
        ).fetchone()
        if tk:
            token = decrypt_token(tk['access_token_enc'])
    if not token:
        token = acc.get('access_token') or ''
    if not token:
        if account_team_id is None:
            team_sql, team_params = "AND team_id IS NULL", []
        else:
            team_sql, team_params = "AND team_id=?", [account_team_id]
        tk = conn.execute(
            f'SELECT access_token_enc FROM fb_tokens WHERE status="active" {team_sql} LIMIT 1',
            team_params,
        ).fetchone()
        if tk:
            token = decrypt_token(tk['access_token_enc'])
    conn.close()
    return token or None


def _get_rate(currency: str, conn) -> float:
    return _local_per_usd_rate(currency)


def _ensure_meta_spend_reconcile_schema(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS meta_spend_reconcile_audit (
               act_id TEXT NOT NULL,
               snapshot_date TEXT NOT NULL,
               ad_id TEXT NOT NULL,
               currency TEXT NOT NULL DEFAULT 'USD',
               spend_original REAL NOT NULL DEFAULT 0,
               spend_usd REAL NOT NULL DEFAULT 0,
               fx_local_per_usd REAL NOT NULL DEFAULT 1,
               conversions REAL NOT NULL DEFAULT 0,
               source TEXT NOT NULL DEFAULT 'manual',
               synced_at TEXT NOT NULL DEFAULT (datetime('now','+8 hours')),
               PRIMARY KEY(act_id, snapshot_date, ad_id)
           )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_meta_spend_reconcile_date "
        "ON meta_spend_reconcile_audit(snapshot_date, act_id)"
    )


def _normalize_meta_sync_dates(date_from: Optional[str], date_to: Optional[str]) -> tuple[str, str]:
    df, dt = _default_dates(date_from, date_to)
    try:
        start = datetime.strptime(df, "%Y-%m-%d").date()
        end = datetime.strptime(dt, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD") from exc
    if start > end:
        start, end = end, start
    if (end - start).days > 30:
        raise HTTPException(status_code=400, detail="Meta reconciliation supports up to 31 days per request")
    return start.isoformat(), end.isoformat()


def _meta_sync_days(date_from: str, date_to: str) -> list[str]:
    start = datetime.strptime(date_from, "%Y-%m-%d").date()
    end = datetime.strptime(date_to, "%Y-%m-%d").date()
    out = []
    while start <= end:
        out.append(start.isoformat())
        start += timedelta(days=1)
    return out


def _meta_sync_fx_rate(currency: str) -> float:
    rate = float(_get_rate(currency, None) or 0)
    if rate <= 0 or rate != rate:
        raise RuntimeError(f"invalid exchange rate for {currency or 'USD'}")
    return rate


def _fetch_meta_spend_rows(act_id: str, token: str, date_from: str, date_to: str) -> list[dict]:
    next_url = f"https://graph.facebook.com/v25.0/{act_id}/insights"
    params = {
        "access_token": token,
        "fields": "date_start,ad_id,ad_name,adset_id,campaign_id,spend,impressions,clicks,actions,action_values",
        "time_range": json.dumps({"since": date_from, "until": date_to}),
        "time_increment": 1,
        "level": "ad",
        "limit": 250,
    }
    rows = []
    seen_urls = set()
    for _ in range(100):
        if not next_url or next_url in seen_urls:
            break
        seen_urls.add(next_url)
        response = req.get(next_url, params=params, timeout=35)
        data = response.json()
        if data.get("error"):
            error = data["error"]
            raise RuntimeError(error.get("message") or str(error))
        rows.extend(data.get("data") or [])
        next_url = (data.get("paging") or {}).get("next")
        params = {}
    return rows


def _sync_meta_spend_for_accounts(
    accounts: list[dict],
    date_from: str,
    date_to: str,
    *,
    source: str,
) -> dict:
    requested_days = _meta_sync_days(date_from, date_to)
    result = {
        "date_from": date_from,
        "date_to": date_to,
        "synced_accounts": 0,
        "failed_accounts": 0,
        "synced_ads": 0,
        "spend_usd": 0.0,
        "accounts": [],
    }
    for account in accounts:
        act_id = str(account.get("act_id") or "").strip()
        if not act_id:
            continue
        account_name = str(account.get("name") or act_id)
        currency = str(account.get("currency") or "USD").upper()
        token = get_exec_token(act_id, ACTION_READ, notify_exhausted=False)
        if not token:
            result["failed_accounts"] += 1
            result["accounts"].append({
                "act_id": act_id,
                "name": account_name,
                "status": "missing_read_token",
            })
            continue
        try:
            source_rows = _fetch_meta_spend_rows(act_id, token, date_from, date_to)
            fx_rate = _meta_sync_fx_rate(currency)
            conn = get_conn()
            try:
                _ensure_meta_spend_reconcile_schema(conn)
                kpi_rows = conn.execute(
                    """SELECT target_id, kpi_field
                       FROM kpi_configs
                       WHERE act_id=? AND level='ad' AND enabled=1""",
                    (act_id,),
                ).fetchall()
                kpi_map = {str(row["target_id"]): row["kpi_field"] for row in kpi_rows}
                for day in requested_days:
                    conn.execute(
                        "DELETE FROM perf_snapshots WHERE act_id=? AND snapshot_date=?",
                        (act_id, day),
                    )
                    conn.execute(
                        "DELETE FROM meta_spend_reconcile_audit WHERE act_id=? AND snapshot_date=?",
                        (act_id, day),
                    )

                synced_rows = 0
                account_spend_usd = 0.0
                for item in source_rows:
                    day = str(item.get("date_start") or "").strip()
                    ad_id = str(item.get("ad_id") or "").strip()
                    if day not in requested_days or not ad_id:
                        continue
                    spend_original = float(item.get("spend") or 0)
                    spend_usd = round(spend_original / fx_rate, 4)
                    actions = item.get("actions") or []
                    action_values = item.get("action_values") or []
                    kpi_field = kpi_map.get(ad_id)
                    conversions = _count_conversions(actions, kpi_field)
                    revenue_original = _count_revenue(action_values, kpi_field) if kpi_field else 0.0
                    roas = round(revenue_original / spend_original, 4) if spend_original > 0 and revenue_original > 0 else 0.0
                    cpa = round(spend_usd / conversions, 4) if conversions > 0 else None
                    conn.execute(
                        """INSERT INTO perf_snapshots
                           (act_id, ad_id, adset_id, campaign_id, ad_name, snapshot_date,
                            spend, impressions, clicks, conversions, cpa, roas, kpi_field,
                            raw_actions, currency)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            act_id,
                            ad_id,
                            str(item.get("adset_id") or ""),
                            str(item.get("campaign_id") or ""),
                            str(item.get("ad_name") or ad_id),
                            day,
                            spend_usd,
                            int(float(item.get("impressions") or 0)),
                            int(float(item.get("clicks") or 0)),
                            conversions,
                            cpa,
                            roas,
                            kpi_field,
                            json.dumps(actions, ensure_ascii=False),
                            "USD",
                        ),
                    )
                    conn.execute(
                        """INSERT INTO meta_spend_reconcile_audit
                           (act_id, snapshot_date, ad_id, currency, spend_original, spend_usd,
                            fx_local_per_usd, conversions, source, synced_at)
                           VALUES (?,?,?,?,?,?,?,?,?,datetime('now','+8 hours'))""",
                        (
                            act_id,
                            day,
                            ad_id,
                            currency,
                            spend_original,
                            spend_usd,
                            fx_rate,
                            conversions,
                            source,
                        ),
                    )
                    synced_rows += 1
                    account_spend_usd += spend_usd
                for day in requested_days:
                    conn.execute(
                        """INSERT INTO meta_spend_reconcile_audit
                           (act_id, snapshot_date, ad_id, currency, spend_original, spend_usd,
                            fx_local_per_usd, conversions, source, synced_at)
                           VALUES (?,?,?,?,?,?,?,?,?,datetime('now','+8 hours'))""",
                        (
                            act_id,
                            day,
                            "__account__",
                            currency,
                            0,
                            0,
                            fx_rate,
                            0,
                            source,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
            result["synced_accounts"] += 1
            result["synced_ads"] += synced_rows
            result["spend_usd"] += account_spend_usd
            result["accounts"].append({
                "act_id": act_id,
                "name": account_name,
                "currency": currency,
                "fx_local_per_usd": fx_rate,
                "ads": synced_rows,
                "spend_usd": round(account_spend_usd, 2),
                "status": "ok",
            })
        except Exception as exc:
            result["failed_accounts"] += 1
            result["accounts"].append({
                "act_id": act_id,
                "name": account_name,
                "status": "error",
                "error": str(exc)[:300],
            })
    _SUMMARY_CACHE.clear()
    result["spend_usd"] = round(result["spend_usd"], 2)
    return result


def reconcile_completed_meta_spend() -> dict:
    if not _META_SPEND_RECONCILE_LOCK.acquire(blocking=False):
        return {"status": "busy"}
    try:
        conn = get_conn()
        try:
            _ensure_meta_spend_reconcile_schema(conn)
            accounts = [dict(row) for row in conn.execute("SELECT * FROM accounts").fetchall()]
            finalized = []
            now_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            for account in accounts:
                try:
                    account_now = now_utc.astimezone(ZoneInfo(account.get("timezone") or "UTC"))
                except Exception:
                    account_now = now_utc
                completed_day = (account_now.date() - timedelta(days=1)).isoformat()
                exists = conn.execute(
                    """SELECT 1 FROM meta_spend_reconcile_audit
                       WHERE act_id=? AND snapshot_date=? LIMIT 1""",
                    (account.get("act_id"), completed_day),
                ).fetchone()
                if not exists:
                    finalized.append((account, completed_day))
        finally:
            conn.close()
        synced = 0
        failed = 0
        for account, completed_day in finalized:
            outcome = _sync_meta_spend_for_accounts(
                [account],
                completed_day,
                completed_day,
                source="scheduled_finalization",
            )
            synced += int(outcome.get("synced_accounts") or 0)
            failed += int(outcome.get("failed_accounts") or 0)
        return {"status": "ok", "synced_accounts": synced, "failed_accounts": failed}
    finally:
        _META_SPEND_RECONCILE_LOCK.release()


def start_meta_spend_reconciliation_worker() -> None:
    global _META_SPEND_RECONCILE_THREAD
    if _META_SPEND_RECONCILE_THREAD and _META_SPEND_RECONCILE_THREAD.is_alive():
        return

    def worker() -> None:
        if _META_SPEND_RECONCILE_STOP.wait(120):
            return
        while not _META_SPEND_RECONCILE_STOP.is_set():
            try:
                reconcile_completed_meta_spend()
            except Exception:
                pass
            _META_SPEND_RECONCILE_STOP.wait(3600)

    _META_SPEND_RECONCILE_THREAD = Thread(
        target=worker,
        name="mira-meta-spend-reconcile",
        daemon=True,
    )
    _META_SPEND_RECONCILE_THREAD.start()


def _beijing_today() -> date:
    return (datetime.utcnow() + timedelta(hours=8)).date()


def _hour_window_for_day(target_day: str) -> tuple[int, date, datetime]:
    now_bj = datetime.utcnow() + timedelta(hours=8)
    today = now_bj.date()
    try:
        target_date = datetime.strptime(target_day, "%Y-%m-%d").date()
    except ValueError:
        target_date = today
    if target_date < today:
        return 23, target_date, now_bj
    if target_date > today:
        return -1, target_date, now_bj
    return now_bj.hour, target_date, now_bj


def _hour_labels(target_day: str, max_hour: int) -> tuple[list[str], list[str]]:
    if max_hour < 0:
        return [], []
    labels = [f"{h:02d}:00" for h in range(max_hour + 1)]
    return labels, [f"{target_day} {label}" for label in labels]


def _parse_fb_hour(value: str) -> Optional[int]:
    try:
        hour = int(str(value or "")[:2])
        return hour if 0 <= hour <= 23 else None
    except (TypeError, ValueError):
        return None


def _fetch_account_hourly_trend(acc: dict, target_day: str, kpi_filter: str) -> dict:
    spend = [0.0 for _ in range(24)]
    conv = [0.0 for _ in range(24)]
    token = _get_token_for_account(acc)
    if not token:
        return {"status": "no_token", "spend": spend, "conv": conv, "rows": 0}

    currency = (acc.get("currency") or "USD").upper()
    rate = _get_rate(currency, None)
    conn = get_conn()
    try:
        kpi_rows = conn.execute(
            'SELECT target_id, kpi_field FROM kpi_configs WHERE act_id=? AND level="ad" AND enabled=1',
            (acc["act_id"],),
        ).fetchall()
    finally:
        conn.close()
    kpi_map = {row["target_id"]: row["kpi_field"] for row in kpi_rows}

    next_url = f'https://graph.facebook.com/v25.0/{acc["act_id"]}/insights'
    params = {
        "access_token": token,
        "fields": "date_start,ad_id,spend,actions",
        "time_range": f'{{"since":"{target_day}","until":"{target_day}"}}',
        "time_increment": 1,
        "level": "ad",
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
        "limit": 500,
    }
    fetched = 0
    row_count = 0
    while next_url and fetched < 8000:
        resp = req.get(next_url, params=params, timeout=30)
        data = resp.json()
        if "error" in data:
            raise RuntimeError(data["error"].get("message", str(data["error"])))
        items = data.get("data", [])
        fetched += len(items)
        for item in items:
            if item.get("date_start") != target_day:
                continue
            hour = _parse_fb_hour(item.get("hourly_stats_aggregated_by_advertiser_time_zone"))
            if hour is None:
                continue
            kpi_field = kpi_map.get(item.get("ad_id", ""))
            if not _kpi_field_matches_filter(kpi_field, kpi_filter):
                continue
            spend_orig = float(item.get("spend", 0) or 0)
            spend[hour] += round(spend_orig / rate, 2) if rate else spend_orig
            conv[hour] += _count_conversions(item.get("actions", []), kpi_field)
            row_count += 1
        next_url = data.get("paging", {}).get("next")
        params = {}
    return {"status": "ok", "spend": spend, "conv": conv, "rows": row_count}


def _hourly_trend_from_fb_insights(conn, user, target_day: str, act_id: Optional[str], kpi_filter: str) -> Optional[dict]:
    max_hour, _target_date, _now_bj = _hour_window_for_day(target_day)
    labels, full_hours = _hour_labels(target_day, max_hour)
    accs = [dict(r) for r in _fetch_visible_accounts(conn, user, act_id)]
    if not accs:
        return None
    if max_hour < 0:
        return {
            "date_from": target_day,
            "date_to": target_day,
            "granularity": "hour",
            "labels": [],
            "full_hours": [],
            "spend": [],
            "cpa": [],
            "conversions": [],
            "hourly_spend": [],
            "hourly_conversions": [],
            "sample_counts": [],
            "account_count": len(accs),
            "error_accounts": 0,
            "has_data": False,
            "source": "fb_insights_hourly_api",
        }

    hourly_spend = [0.0 for _ in range(24)]
    hourly_conv = [0.0 for _ in range(24)]
    error_accounts = 0
    row_count = 0
    max_workers = min(4, len(accs))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_account_hourly_trend, acc, target_day, kpi_filter) for acc in accs]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception:
                error_accounts += 1
                continue
            if result.get("status") != "ok":
                error_accounts += 1
            row_count += int(result.get("rows") or 0)
            for hour in range(max_hour + 1):
                hourly_spend[hour] += float(result["spend"][hour] or 0)
                hourly_conv[hour] += float(result["conv"][hour] or 0)

    if row_count == 0 and error_accounts >= len(accs):
        return None

    spend_arr = []
    cpa_arr = []
    conv_arr = []
    running_spend = 0.0
    running_conv = 0.0
    for hour in range(max_hour + 1):
        running_spend += hourly_spend[hour]
        running_conv += hourly_conv[hour]
        spend_arr.append(round(running_spend, 2))
        conv_arr.append(round(running_conv, 2))
        cpa_arr.append(round(running_spend / running_conv, 2) if running_conv > 0 else None)

    return {
        "date_from": target_day,
        "date_to": target_day,
        "granularity": "hour",
        "labels": labels,
        "full_hours": full_hours,
        "spend": spend_arr,
        "cpa": cpa_arr,
        "conversions": conv_arr,
        "hourly_spend": [round(v, 2) for v in hourly_spend[: max_hour + 1]],
        "hourly_conversions": [round(v, 2) for v in hourly_conv[: max_hour + 1]],
        "sample_counts": [],
        "account_count": len(accs),
        "error_accounts": error_accounts,
        "has_data": row_count > 0 or any(spend_arr) or any(conv_arr),
        "source": "fb_insights_hourly_api",
    }


def _hourly_trend_from_local_snapshots(conn, user, target_day: str, act_id: Optional[str], kpi_filter: str) -> dict:
    max_hour, target_date, now_bj = _hour_window_for_day(target_day)
    today = now_bj.date()
    labels, full_hours = _hour_labels(target_day, max_hour)
    ensure_perf_snapshot_history_schema(conn)
    accs = _fetch_visible_accounts(conn, user, act_id)
    act_ids = [r["act_id"] for r in accs]
    spend_arr = [0.0 for _ in range(24)]
    cpa_arr = [None for _ in range(24)]
    conv_arr = [0.0 for _ in range(24)]
    sample_counts = [0 for _ in range(24)]
    if not act_ids:
        return {
            "date_from": target_day,
            "date_to": target_day,
            "granularity": "hour",
            "labels": labels,
            "full_hours": full_hours,
            "spend": spend_arr[: max_hour + 1] if max_hour >= 0 else [],
            "cpa": cpa_arr[: max_hour + 1] if max_hour >= 0 else [],
            "conversions": conv_arr[: max_hour + 1] if max_hour >= 0 else [],
            "sample_counts": sample_counts[: max_hour + 1] if max_hour >= 0 else [],
            "has_data": False,
            "source": "local_perf_snapshot_history",
        }

    placeholders = ",".join("?" for _ in act_ids)
    params = [target_day] + act_ids
    rows = conn.execute(
        f"""SELECT h.*, CAST(substr(h.snapshot_at, 12, 2) AS INTEGER) AS hour_no
            FROM perf_snapshot_history h
            WHERE h.snapshot_date=? AND h.act_id IN ({placeholders})
            ORDER BY hour_no, h.act_id, h.ad_id, h.snapshot_at, h.id""",
        params,
    ).fetchall()

    per_ad: dict[tuple[str, str], list] = {}

    def add_row(row, hour_no: int) -> None:
        if hour_no < 0 or hour_no > 23:
            return
        if kpi_filter and not _kpi_field_matches_filter(row["kpi_field"], kpi_filter):
            return
        key = (
            row["act_id"],
            row["ad_id"] or row["adset_id"] or row["campaign_id"] or str(row["id"]),
        )
        if key not in per_ad:
            per_ad[key] = [None for _ in range(24)]
        per_ad[key][hour_no] = (
            float(row["spend"] or 0),
            float(row["conversions"] or 0),
        )

    for row in rows:
        try:
            add_row(row, int(row["hour_no"]))
        except (TypeError, ValueError):
            continue

    fallback_hour = now_bj.hour if target_date == today else 23

    latest_rows = conn.execute(
        f"""SELECT p.*, NULL AS hour_no
            FROM perf_snapshots p
            WHERE p.snapshot_date=? AND p.act_id IN ({placeholders})""",
        params,
    ).fetchall()
    for row in latest_rows:
        add_row(row, fallback_hour)

    has_data = bool(per_ad)
    for series in per_ad.values():
        last = None
        for hour_no in range(24):
            if series[hour_no] is not None:
                last = series[hour_no]
            if last is None or hour_no > max_hour:
                continue
            spend_arr[hour_no] += last[0]
            conv_arr[hour_no] += last[1]
            sample_counts[hour_no] += 1

    for hour_no in range(24):
        if hour_no > max_hour:
            spend_arr[hour_no] = None
            cpa_arr[hour_no] = None
            continue
        spend_arr[hour_no] = round(spend_arr[hour_no], 2)
        conv_arr[hour_no] = round(conv_arr[hour_no], 2)
        cpa_arr[hour_no] = round(spend_arr[hour_no] / conv_arr[hour_no], 2) if conv_arr[hour_no] > 0 else None

    return {
        "date_from": target_day,
        "date_to": target_day,
        "granularity": "hour",
        "labels": labels,
        "full_hours": full_hours,
        "spend": spend_arr[: max_hour + 1] if max_hour >= 0 else [],
        "cpa": cpa_arr[: max_hour + 1] if max_hour >= 0 else [],
        "conversions": conv_arr[: max_hour + 1] if max_hour >= 0 else [],
        "sample_counts": sample_counts[: max_hour + 1] if max_hour >= 0 else [],
        "has_data": has_data,
        "source": "local_perf_snapshot_history",
    }


_ZERO_DECIMAL_CURRENCIES = {"JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS"}


def _minor_to_amount(value, currency: str):
    if value in (None, ""):
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if (currency or "USD").upper() in _ZERO_DECIMAL_CURRENCIES:
        return round(raw, 2)
    return round(raw / 100, 2)


def _fb_ads_paginated(act_id: str, token: str, fields: str, limit: int = 200, max_pages: int = 20):
    rows = []
    next_url = f"https://graph.facebook.com/v25.0/{act_id}/ads"
    params = {"access_token": token, "fields": fields, "limit": limit}
    pages = 0
    last_error = None
    while next_url and pages < max_pages:
        try:
            if pages == 0:
                resp = req.get(next_url, params=params, timeout=25)
            else:
                resp = req.get(next_url, timeout=25)
            data = resp.json()
        except Exception as exc:
            last_error = str(exc)
            break
        if data.get("error"):
            last_error = data["error"].get("message") or str(data["error"])
            break
        rows.extend(data.get("data", []) or [])
        pages += 1
        next_url = (data.get("paging") or {}).get("next")
    return rows, last_error


def _account_writeable_status(acc: dict) -> bool:
    try:
        return int(acc.get("account_status") or 1) == 1
    except (TypeError, ValueError):
        return False


def _default_dates(date_from: Optional[str], date_to: Optional[str]):
    """如果未传日期，默认今日"""
    today = date.today().isoformat()
    return date_from or today, date_to or today


# ─── 大盘汇总 ─────────────────────────────────────────────────

def _local_snapshot_account_rows(conn, user, df: str, dt: str, act_id: Optional[str], kpi_filter: str) -> tuple[list[dict], dict]:
    ensure_perf_snapshot_history_schema(conn)
    accs = [dict(a) for a in _fetch_visible_accounts(conn, user, act_id)]
    if not accs:
        return [], {"account_count": 0, "snapshot_rows": 0, "source": "local_perf_snapshots"}
    act_ids = [a["act_id"] for a in accs]
    placeholders = ",".join("?" for _ in act_ids)
    rows = conn.execute(
        f"""SELECT p.act_id,
                   COALESCE(a.name, p.act_id) AS name,
                   COALESCE(a.timezone, 'UTC') AS timezone,
                   COALESCE(a.currency, p.currency, 'USD') AS currency,
                   a.balance, a.spend_cap, a.amount_spent, a.spending_limit,
                   p.kpi_field,
                   COUNT(*) AS snapshot_rows,
                   SUM(COALESCE(p.spend, 0)) AS spend_usd,
                   SUM(COALESCE(p.conversions, 0)) AS conversions,
                   AVG(CASE WHEN p.roas IS NOT NULL AND p.roas > 0 THEN p.roas END) AS avg_roas
            FROM perf_snapshots p
            LEFT JOIN accounts a ON a.act_id = p.act_id
            WHERE p.snapshot_date BETWEEN ? AND ?
              AND p.act_id IN ({placeholders})
            GROUP BY p.act_id, a.name, a.timezone, a.currency, a.balance, a.spend_cap, a.amount_spent, a.spending_limit, p.kpi_field""",
        [df, dt] + act_ids,
    ).fetchall()
    grouped = {}
    snapshot_rows = 0
    for row in rows:
        snapshot_rows += int(row["snapshot_rows"] or 0)
        if kpi_filter and not _kpi_field_matches_filter(row["kpi_field"], kpi_filter):
            continue
        act = row["act_id"]
        item = grouped.setdefault(act, {
            "act_id": act,
            "name": row["name"] or act,
            "timezone": row["timezone"] or "UTC",
            "currency": row["currency"] or "USD",
            "balance": row["balance"],
            "spend_cap": row["spend_cap"],
            "amount_spent": row["amount_spent"],
            "spending_limit": row["spending_limit"],
            "spend_usd": 0.0,
            "conversions": 0.0,
            "roas_values": [],
        })
        item["spend_usd"] += float(row["spend_usd"] or 0)
        item["conversions"] += float(row["conversions"] or 0)
        if row["avg_roas"]:
            item["roas_values"].append(float(row["avg_roas"] or 0))
    out = []
    for item in grouped.values():
        spend = float(item["spend_usd"] or 0)
        conv = float(item["conversions"] or 0)
        avail, _, _ = _calc_available_balance(
            item.get("balance"), item.get("spend_cap"), item.get("amount_spent"), item.get("spending_limit"), item.get("currency") or "USD"
        )
        out.append({
            "act_id": item["act_id"],
            "name": item["name"],
            "timezone": item["timezone"],
            "currency": item["currency"],
            "spend_usd": round(spend, 2),
            "conversions": conv,
            "cpa_usd": round(spend / conv, 2) if spend > 0 and conv > 0 else None,
            "roas": round(sum(item["roas_values"]) / len(item["roas_values"]), 2) if item["roas_values"] else None,
            "available_balance": avail,
            "status": "ok",
            "source": "local_perf_snapshots",
        })
    out.sort(key=lambda x: x["spend_usd"], reverse=True)
    return out, {"account_count": len(accs), "snapshot_rows": snapshot_rows, "source": "local_perf_snapshots"}


def _local_snapshot_daily_trend(conn, user, df: str, dt: str, act_id: Optional[str], kpi_filter: str) -> dict:
    start = datetime.strptime(df, "%Y-%m-%d").date()
    end = datetime.strptime(dt, "%Y-%m-%d").date()
    day_list = []
    cur = start
    while cur <= end:
        day_list.append(cur.isoformat())
        cur += timedelta(days=1)
    accs = [dict(a) for a in _fetch_visible_accounts(conn, user, act_id)]
    act_ids = [a["act_id"] for a in accs]
    daily_spend = {d: 0.0 for d in day_list}
    daily_conv = {d: 0.0 for d in day_list}
    if act_ids:
        placeholders = ",".join("?" for _ in act_ids)
        rows = conn.execute(
            f"""SELECT snapshot_date, spend, conversions, kpi_field
                FROM perf_snapshots
                WHERE snapshot_date BETWEEN ? AND ?
                  AND act_id IN ({placeholders})""",
            [df, dt] + act_ids,
        ).fetchall()
        for row in rows:
            if kpi_filter and not _kpi_field_matches_filter(row["kpi_field"], kpi_filter):
                continue
            d = row["snapshot_date"]
            if d in daily_spend:
                daily_spend[d] += float(row["spend"] or 0)
                daily_conv[d] += float(row["conversions"] or 0)
    spend_arr = [round(daily_spend[d], 2) for d in day_list]
    cpa_arr = [round(daily_spend[d] / daily_conv[d], 2) if daily_conv[d] > 0 else None for d in day_list]
    return {
        "date_from": df,
        "date_to": dt,
        "labels": [d[5:] for d in day_list],
        "full_dates": day_list,
        "spend": spend_arr,
        "cpa": cpa_arr,
        "conversions": [round(daily_conv[d], 2) for d in day_list],
        "source": "local_perf_snapshots",
    }
def _fetch_account_summary(acc: dict, df: str, dt: str, kpi_filter: Optional[str] = None) -> dict:
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
            if not _kpi_field_matches_filter(kpi_field, kpi_filter):
                continue
            ad_spend = float(ad_item.get("spend", 0) or 0)
            spend_orig += ad_spend
            ad_actions = ad_item.get("actions", [])
            ad_action_values = ad_item.get("action_values", [])
            conversions += _count_conversions(ad_actions, kpi_field)
            if kpi_field and "purchase" in kpi_field:
                revenue_orig += _count_revenue(ad_action_values, kpi_field)

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
    kpi: Optional[str] = None,
    user=Depends(get_current_user)
):
    """
    大盘汇总 - 默认读取本地巡检快照
    date_from/date_to: YYYY-MM-DD 固定日期（用户自选）
    默认今日
    """
    df, dt = _default_dates(date_from, date_to)
    server_today = date.today().isoformat()
    kpi_filter = _normalize_dash_kpi_filter(kpi)
    cache_key = (df, dt, act_id or "", kpi_filter, user.get("uid"), user.get("team_id"), user.get("role"))
    cached = _SUMMARY_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _SUMMARY_CACHE_TTL:
        return dict(cached["data"], source="local_perf_snapshots_cache")

    conn = get_conn()
    accs = _fetch_visible_accounts(conn, user, act_id)
    visible_act_ids = [dict(a)["act_id"] for a in accs]
    log_filter_sql, log_filter_params = _act_id_filter_sql(visible_act_ids, "act_id")
    log_join_filter_sql, log_join_filter_params = _act_id_filter_sql(visible_act_ids, "l.act_id")

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
           FROM action_logs WHERE date(created_at) BETWEEN ? AND ?{log_filter_sql}""",
        (df, dt, *log_filter_params)
    ).fetchone()
    # 服务器今日自动止损
    log_today = conn.execute(
        f"""SELECT
           COUNT(DISTINCT CASE WHEN action_type='pause' AND status='success'
             AND trigger_type NOT IN ('emergency','user') THEN target_id END) as paused_today
           FROM action_logs WHERE date(created_at)=?{log_filter_sql}""",
        (server_today, *log_filter_params)
    ).fetchone()
    # 历史累计自动止损
    log_total = conn.execute(
        f"""SELECT COUNT(DISTINCT target_id) as paused_total
           FROM action_logs
           WHERE action_type='pause' AND status='success'
              AND trigger_type NOT IN ('emergency','user')
              {log_filter_sql}"""
        ,
        log_filter_params
    ).fetchone()
    # 止损明细：JOIN accounts获取账户名称，排除紧急暂停
    pause_details = conn.execute(
        f"""SELECT l.target_id, l.target_name, l.act_id,
                  COALESCE(a.name, l.act_id) as account_name,
                  l.level, l.trigger_type, MAX(l.created_at) as last_at
           FROM action_logs l
           LEFT JOIN accounts a ON a.act_id = l.act_id
           WHERE l.action_type='pause' AND l.status='success'
              AND l.trigger_type NOT IN ('emergency','user')
              AND date(l.created_at) BETWEEN ? AND ?
              {log_join_filter_sql}
            GROUP BY l.target_id
            ORDER BY last_at DESC LIMIT 20""",
        (df, dt, *log_join_filter_params)
    ).fetchall()
    # 紧急暂停单独统计（仅用于展示，不计入止损）
    emg_details = conn.execute(
        f"""SELECT l.target_id, l.target_name, l.act_id,
                  COALESCE(a.name, l.act_id) as account_name,
                  l.level, MAX(l.created_at) as last_at
           FROM action_logs l
           LEFT JOIN accounts a ON a.act_id = l.act_id
           WHERE l.action_type='pause' AND l.status='success'
              AND l.trigger_type='emergency'
              AND date(l.created_at) BETWEEN ? AND ?
              {log_join_filter_sql}
            GROUP BY l.target_id
            ORDER BY last_at DESC LIMIT 20""",
        (df, dt, *log_join_filter_params)
    ).fetchall()
    account_results, local_meta = _local_snapshot_account_rows(conn, user, df, dt, act_id, kpi_filter)
    conn.close()

    total_spend = 0.0
    total_conversions = 0
    cpa_list = []
    roas_list = []
    account_count = int(local_meta.get("account_count") or len(accs))
    error_accounts = 0
    account_details = []  # 账户级明细

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
                    revenue_orig += _count_revenue(ad_action_values, kpi_field)

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
    avg_cpa = round(total_spend / total_conversions, 2) if total_conversions > 0 else None
    avg_roas = round(sum(roas_list) / len(roas_list), 2) if roas_list else None

    result = {
        "date_from": df,
        "date_to": dt,
        "kpi_filter": kpi_filter,
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
        "source": "local_perf_snapshots",
        "snapshot_rows": int(local_meta.get("snapshot_rows") or 0),
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
    kpi: Optional[str] = None,
    user=Depends(get_current_user)
):
    """
    近N日趋势 - 默认读取本地巡检快照
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

    kpi_filter = _normalize_dash_kpi_filter(kpi)
    if len(day_list) == 1:
        conn = get_conn()
        try:
            return _hourly_trend_from_local_snapshots(conn, user, day_list[0], act_id, kpi_filter)
        finally:
            conn.close()

    conn = get_conn()
    try:
        return _local_snapshot_daily_trend(conn, user, df, dt, act_id, kpi_filter)
    finally:
        conn.close()


# ─── 广告列表（实时） ─────────────────────────────────────────
@router.get("/ads-live")
def get_ads_live(
    act_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    refresh: Optional[str] = None,
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
    insights_field = f'insights.time_range({tr}){{spend,impressions,reach,clicks,actions,action_values}}'

    conn = get_conn()
    accs = _fetch_visible_accounts(conn, user, act_id)
    accs = [dict(a) for a in accs]
    act_ids = [a["act_id"] for a in accs]

    settings_map = {}
    try:
        settings_rows = conn.execute(
            "SELECT key,value FROM settings WHERE key IN ('mirror_enabled','sentinel_enabled','heartbeat_enabled')"
        ).fetchall()
        settings_map = {r["key"]: str(r["value"]) for r in settings_rows}
    except Exception:
        settings_map = {}

    team_guard_map = {}
    owner_guard_map = {}
    try:
        team_ids = sorted({int(a.get("team_id")) for a in accs if a.get("team_id")})
        if team_ids:
            placeholders = ",".join("?" for _ in team_ids)
            rows = conn.execute(
                f"""SELECT id,
                          COALESCE(mirror_enabled, 0) AS mirror_enabled,
                          COALESCE(sentinel_enabled, 0) AS sentinel_enabled,
                          COALESCE(heartbeat_enabled, 0) AS heartbeat_enabled
                   FROM teams WHERE id IN ({placeholders})""",
                team_ids,
            ).fetchall()
            team_guard_map = {int(r["id"]): dict(r) for r in rows}
    except Exception:
        team_guard_map = {}
    try:
        owner_ids = sorted({int(a.get("owner_user_id")) for a in accs if a.get("owner_user_id")})
        if owner_ids:
            placeholders = ",".join("?" for _ in owner_ids)
            rows = conn.execute(
                f"""SELECT id,
                          COALESCE(mirror_enabled, 0) AS mirror_enabled,
                          COALESCE(sentinel_enabled, 0) AS sentinel_enabled,
                          COALESCE(heartbeat_enabled, 0) AS heartbeat_enabled
                   FROM users WHERE id IN ({placeholders}) AND COALESCE(is_active, 1)=1""",
                owner_ids,
            ).fetchall()
            owner_guard_map = {int(r["id"]): dict(r) for r in rows}
    except Exception:
        owner_guard_map = {}

    cap_map = {a["act_id"]: {
        "manage_token_ok": False,
        "write_token_ok": False,
        "pause_token_ok": False,
        "update_token_ok": False,
        "read_token_ok": False,
    } for a in accs}
    if act_ids:
        try:
            placeholders = ",".join("?" for _ in act_ids)
            token_rows = conn.execute(
                f"""SELECT aot.act_id, t.token_type, COALESCE(t.token_source, '') as token_source,
                          t.status as token_status, aot.status as bind_status
                   FROM account_op_tokens aot
                   JOIN fb_tokens t ON t.id = aot.token_id
                   JOIN accounts a ON a.act_id = aot.act_id
                   WHERE aot.act_id IN ({placeholders})
                     AND (
                       (a.team_id IS NULL AND t.team_id IS NULL)
                       OR (a.team_id IS NOT NULL AND t.team_id=a.team_id)
                       OR (
                         a.team_id IS NOT NULL
                         AND t.team_id IS NULL
                         AND t.token_type='operate'
                         AND COALESCE(t.token_source, '')=?
                       )
                     )""",
                act_ids + [TOKEN_SOURCE_OAUTH_USER],
            ).fetchall()
            for row in token_rows:
                active = row["token_status"] == "active" and row["bind_status"] == "active"
                if not active:
                    continue
                meta = cap_map.setdefault(row["act_id"], {})
                if row["token_type"] == "manage":
                    meta["manage_token_ok"] = True
                    meta["read_token_ok"] = True
                elif row["token_type"] in ("operate", "user"):
                    meta["read_token_ok"] = True
                if is_operate_token_eligible(row["token_type"], row["token_source"] or TOKEN_SOURCE_SYSTEM_USER):
                    meta["write_token_ok"] = True
        except Exception:
            pass
    if act_ids:
        try:
            placeholders = ",".join("?" for _ in act_ids)
            primary_rows = conn.execute(
                f"""SELECT a.act_id, t.token_type, t.status as token_status
                    FROM accounts a
                    JOIN fb_tokens t ON t.id = a.token_id
                    WHERE a.act_id IN ({placeholders})""",
                act_ids,
            ).fetchall()
            for row in primary_rows:
                if row["token_status"] != "active":
                    continue
                meta = cap_map.setdefault(row["act_id"], {})
                meta["read_token_ok"] = True
                if row["token_type"] == "manage":
                    meta["manage_token_ok"] = True
        except Exception:
            pass
    for acc in accs:
        meta = cap_map.setdefault(acc["act_id"], {})
        if is_read_blocking_status(acc.get("read_permission_status")):
            meta["read_token_ok"] = False
            if not meta.get("write_token_ok"):
                meta["manage_token_ok"] = False
        writeable_account = _account_writeable_status(acc)
        meta["pause_token_ok"] = bool(meta.get("write_token_ok") or meta.get("manage_token_ok"))
        meta["update_token_ok"] = bool(meta.get("write_token_ok") and writeable_account)
        meta["account_writeable"] = writeable_account
    conn.close()

    kpi_conn = get_conn()
    try:
        kpi_rows = kpi_conn.execute('SELECT * FROM kpi_configs WHERE level="ad" AND enabled=1').fetchall()
        kpi_map = {(r['act_id'], r['target_id']): dict(r) for r in kpi_rows}
    except Exception:
        kpi_map = {}
    kpi_conn.close()

    refresh_requested = str(refresh or "").strip().lower() in ("1", "true", "yes", "force", "refresh")
    all_ads = []
    served_from_cache = False
    cache_lock = None
    cache_lock_acquired = False
    cache_scope = act_id or "__visible__"
    cache_conn = get_conn()
    try:
        _ensure_ads_live_cache_schema(cache_conn)
        if not refresh_requested:
            cached = _ads_live_cached_for_accounts(cache_conn, accs, date_from, date_to)
            if cached is not None:
                all_ads = cached
                served_from_cache = True
        if not served_from_cache:
            cache_lock = _ads_live_lock_for(cache_scope, date_from, date_to)
            cache_lock.acquire()
            cache_lock_acquired = True
            if not refresh_requested:
                cached = _ads_live_cached_for_accounts(cache_conn, accs, date_from, date_to)
                if cached is not None:
                    all_ads = cached
                    served_from_cache = True
    finally:
        cache_conn.close()
    if served_from_cache:
        accs = []
    for acc in accs:
        currency = (acc.get('currency') or 'USD').upper()
        token = _get_token_for_account(acc)
        if not token:
            continue
        conn2 = get_conn()
        rate = _get_rate(currency, conn2)
        conn2.close()

        try:
            rich_fields = (
                "id,name,status,effective_status,adset_id,campaign_id,"
                "creative{id,name,effective_object_story_id,object_story_id,object_story_spec},"
                "adset{id,name,status,effective_status,daily_budget,lifetime_budget,"
                "budget_remaining,bid_strategy,optimization_goal,campaign_id},"
                "campaign{id,name,status,effective_status,daily_budget,lifetime_budget,"
                "budget_remaining,bid_strategy,objective},"
                f"{insights_field}"
            )
            ads, err = _fb_ads_paginated(acc["act_id"], token, rich_fields)
            if err:
                fallback_fields = f'id,name,status,effective_status,adset_id,campaign_id,{insights_field}'
                ads, err = _fb_ads_paginated(acc["act_id"], token, fallback_fields)
            if err:
                note_account_read_failure(acc["act_id"], err)
                continue
            note_account_read_success(acc["act_id"])
            acc_caps = cap_map.get(acc["act_id"], {})
            automation_warnings = []
            team_guard = team_guard_map.get(int(acc.get("team_id") or 0), {})
            owner_guard = owner_guard_map.get(int(acc.get("owner_user_id") or 0), {})
            if settings_map.get("mirror_enabled") == "1" or int(acc.get("mirror_enabled") or 0) == 1 or int(team_guard.get("mirror_enabled") or 0) == 1 or int(owner_guard.get("mirror_enabled") or 0) == 1:
                automation_warnings.append("mirror_enabled")
            if settings_map.get("sentinel_enabled") == "1" or int(acc.get("sentinel_enabled") or 0) == 1 or int(team_guard.get("sentinel_enabled") or 0) == 1 or int(owner_guard.get("sentinel_enabled") or 0) == 1:
                automation_warnings.append("sentinel_enabled")
            if settings_map.get("heartbeat_enabled") == "1" or int(team_guard.get("heartbeat_enabled") or 0) == 1 or int(owner_guard.get("heartbeat_enabled") or 0) == 1:
                automation_warnings.append("heartbeat_enabled")
            for ad in ads:
                ins_data = ad.get('insights', {})
                ins = ins_data.get('data', []) if isinstance(ins_data, dict) else []
                spend_orig = 0.0
                impressions = 0
                reach = 0
                clicks = 0
                raw_actions = []
                roas = 0.0
                if ins:
                    spend_orig = float(ins[0].get('spend', 0) or 0)
                    impressions = int(ins[0].get('impressions', 0) or 0)
                    reach = int(ins[0].get('reach', 0) or 0)
                    clicks = int(ins[0].get('clicks', 0) or 0)
                    raw_actions = ins[0].get('actions', [])
                    revenue = _count_revenue(ins[0].get('action_values', []), None)
                    roas = round(revenue / spend_orig, 2) if spend_orig > 0 and revenue > 0 else 0
                spend_usd = round(spend_orig / rate, 2) if rate else spend_orig
                # v1.2.0: 根据kpi_field选择正确的转化字段
                kpi = kpi_map.get((acc['act_id'], ad['id']), {})
                kpi_field = kpi.get('kpi_field')
                conversions = _count_conversions(raw_actions, kpi_field)
                cpa = round(spend_usd / conversions, 2) if conversions > 0 else 0
                if kpi_field and "purchase" in str(kpi_field):
                    revenue = _count_revenue(ins[0].get('action_values', []), kpi_field) if ins else 0.0
                    roas = round(revenue / spend_orig, 2) if spend_orig > 0 and revenue > 0 else 0
                score_info = _score_ad_performance(
                    spend_usd, conversions, clicks, impressions, reach, cpa,
                    kpi.get('target_cpa'), roas, kpi_field
                )
                adset = ad.get("adset") if isinstance(ad.get("adset"), dict) else {}
                campaign = ad.get("campaign") if isinstance(ad.get("campaign"), dict) else {}
                creative = ad.get("creative") if isinstance(ad.get("creative"), dict) else {}
                story_id = str(creative.get("effective_object_story_id") or creative.get("object_story_id") or "").strip()
                post_id = story_id.split("_", 1)[1] if "_" in story_id else story_id
                fb_page_id = _extract_ad_page_id(ad) or str(acc.get("page_id") or "").strip()
                ad_status = ad.get("status", "")
                effective_status = ad.get("effective_status", "")
                adset_id = ad.get("adset_id") or adset.get("id") or ""
                campaign_id = ad.get("campaign_id") or campaign.get("id") or ""
                terminal = effective_status in ("DELETED", "ARCHIVED") or ad_status in ("DELETED", "ARCHIVED")
                paused_effective = effective_status in ("PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED")
                write_block_reason = ""
                if not acc_caps.get("account_writeable"):
                    write_block_reason = f"account_status={acc.get('account_status')}"
                elif not acc_caps.get("write_token_ok"):
                    write_block_reason = "no_system_user_operate_token"
                pause_block_reason = ""
                if not acc_caps.get("pause_token_ok"):
                    pause_block_reason = "no_pause_token"
                elif terminal:
                    pause_block_reason = "target_archived_or_deleted"
                elif paused_effective or ad_status == "PAUSED":
                    pause_block_reason = "already_paused"
                resume_block_reason = ""
                if not acc_caps.get("update_token_ok"):
                    resume_block_reason = write_block_reason or "no_update_token"
                elif terminal:
                    resume_block_reason = "target_archived_or_deleted"
                elif ad_status == "ACTIVE" and effective_status == "ACTIVE":
                    resume_block_reason = "already_active"
                adset_budget_amount = _minor_to_amount(adset.get("daily_budget"), currency)
                campaign_budget_amount = _minor_to_amount(campaign.get("daily_budget"), currency)
                adset_budget_block = ""
                if not acc_caps.get("update_token_ok"):
                    adset_budget_block = write_block_reason or "no_update_token"
                elif not adset_id:
                    adset_budget_block = "missing_adset_id"
                elif adset_budget_amount is None:
                    adset_budget_block = "adset_daily_budget_unavailable"
                all_ads.append({
                    'ad_id': ad['id'],
                    'ad_name': ad.get('name', ad['id']),
                    'act_id': acc['act_id'],
                    'account_name': acc.get('name', ''),
                    'currency': currency,
                    'timezone': acc.get('timezone', 'UTC'),
                    'date_from': date_from,
                    'date_to': date_to,
                    'status': ad_status,
                    'effective_status': effective_status,
                    'spend': spend_usd,
                    'impressions': impressions,
                    'reach': reach,
                    'clicks': clicks,
                    'ctr': score_info.get('ctr', 0),
                    'conversions': conversions,
                    'cpa': cpa,
                    'roas': roas,
                    'score': score_info.get('score'),
                    'score_label': score_info.get('label'),
                    'score_level': score_info.get('level'),
                    'score_reasons': score_info.get('reasons', []),
                    'adset_id': adset_id,
                    'adset_name': adset.get('name', ''),
                    'adset_status': adset.get('status', ''),
                    'adset_effective_status': adset.get('effective_status', ''),
                    'adset_daily_budget': adset_budget_amount,
                    'adset_lifetime_budget': _minor_to_amount(adset.get("lifetime_budget"), currency),
                    'adset_budget_remaining': _minor_to_amount(adset.get("budget_remaining"), currency),
                    'adset_bid_strategy': adset.get('bid_strategy', ''),
                    'adset_optimization_goal': adset.get('optimization_goal', ''),
                    'campaign_id': campaign_id,
                    'campaign_name': campaign.get('name', ''),
                    'campaign_status': campaign.get('status', ''),
                    'campaign_effective_status': campaign.get('effective_status', ''),
                    'campaign_daily_budget': campaign_budget_amount,
                    'campaign_lifetime_budget': _minor_to_amount(campaign.get("lifetime_budget"), currency),
                    'campaign_budget_remaining': _minor_to_amount(campaign.get("budget_remaining"), currency),
                    'campaign_bid_strategy': campaign.get('bid_strategy', ''),
                    'campaign_objective': campaign.get('objective', ''),
                    'creative_id': str(creative.get('id') or ''),
                    'story_id': story_id,
                    'post_id': post_id,
                    'fb_page_id': fb_page_id,
                    'account_page_id': str(acc.get('page_id') or '').strip(),
                    'target_cpa': kpi.get('target_cpa'),   # 已是USD
                    'kpi_field': kpi_field,
                    'kpi_label': kpi.get('kpi_label', ''),
                    'kpi_source': kpi.get('source', ''),
                    'manage_token_ok': bool(acc_caps.get('manage_token_ok')),
                    'write_token_ok': bool(acc_caps.get('write_token_ok')),
                    'pause_token_ok': bool(acc_caps.get('pause_token_ok')),
                    'update_token_ok': bool(acc_caps.get('update_token_ok')),
                    'account_writeable': bool(acc_caps.get('account_writeable')),
                    'automation_warnings': automation_warnings,
                    'can_pause': not bool(pause_block_reason),
                    'can_resume': not bool(resume_block_reason),
                    'can_edit_budget': not bool(adset_budget_block),
                    'pause_block_reason': pause_block_reason,
                    'resume_block_reason': resume_block_reason,
                    'budget_block_reason': adset_budget_block,
                })
        except Exception:
            continue

    if not served_from_cache:
        fresh_synced_at = _ads_live_now_text()
        for row in all_ads:
            row["ads_live_cached"] = False
            row["ads_live_synced_at"] = fresh_synced_at
            row["ads_live_cache_age_seconds"] = 0
        cache_save_conn = get_conn()
        try:
            grouped_rows = {}
            for row in all_ads:
                key = row.get("act_id")
                if not key:
                    continue
                grouped_rows.setdefault(key, []).append(row)
            for acc in _fetch_visible_accounts(cache_save_conn, user, act_id):
                acc_key = acc["act_id"]
                _ads_live_cache_set(cache_save_conn, acc_key, date_from, date_to, grouped_rows.get(acc_key, []))
        except Exception:
            pass
        finally:
            cache_save_conn.close()
    if cache_lock_acquired and cache_lock:
        try:
            cache_lock.release()
        except Exception:
            pass

    for row in all_ads:
        kpi = kpi_map.get((row.get("act_id"), row.get("ad_id")), {})
        row["target_cpa"] = kpi.get("target_cpa")
        row["kpi_field"] = kpi.get("kpi_field")
        row["kpi_label"] = kpi.get("kpi_label", "")
        row["kpi_source"] = kpi.get("source", "")

    if all_ads:
        try:
            conn3 = get_conn()
            try:
                spend_by_ad = {r.get("ad_id"): float(r.get("spend") or 0) for r in all_ads if r.get("ad_id")}
                landing_map = _landing_ad_metrics_for_ads(
                    conn3,
                    [r.get("ad_id") for r in all_ads],
                    date_from=date_from,
                    date_to=date_to,
                    spend_by_ad=spend_by_ad,
                )
                page_map = _page_brief_map(
                    conn3,
                    [r.get("fb_page_id") or r.get("account_page_id") for r in all_ads],
                )
                for row in all_ads:
                    row["landing"] = landing_map.get(row.get("ad_id"), {})
                    page_id = row.get("fb_page_id") or row.get("account_page_id") or ""
                    row["page_brief"] = page_map.get(page_id, {"page_id": page_id, "page_status": "unknown"} if page_id else {})
            finally:
                conn3.close()
        except Exception:
            for row in all_ads:
                row.setdefault("landing", {})
                row.setdefault("page_brief", {})

    all_ads.sort(key=lambda x: x['spend'], reverse=True)
    return all_ads


# ─── 消耗查询（自定义日期） ───────────────────────────────────
@router.post("/sync-spend")
def sync_spend_from_meta(body: dict, user=Depends(get_current_user)):
    _require_operator_user(user)
    body = body or {}
    act_id = str(body.get("act_id") or "").strip() or None
    date_from, date_to = _normalize_meta_sync_dates(
        body.get("date_from"),
        body.get("date_to"),
    )
    conn = get_conn()
    try:
        accounts = [dict(row) for row in _fetch_visible_accounts(conn, user, act_id)]
    finally:
        conn.close()
    if act_id and not accounts:
        raise HTTPException(status_code=404, detail="account not found or not accessible")
    outcome = _sync_meta_spend_for_accounts(
        accounts,
        date_from,
        date_to,
        source="manual_meta_sync",
    )
    outcome["status"] = "ok" if not outcome["failed_accounts"] else "partial"
    return outcome


@router.get("/spend-query")
def spend_query(
    date_from: str,
    date_to: str,
    account_id: Optional[str] = None,
    user=Depends(get_current_user)
):
    """自定义日期范围消耗查询。

    Dashboard uses local guard snapshots as the source of truth. This keeps the
    cards, trend chart, ad-stop records and copied spent IDs on the same data
    basis, and avoids repeated FB API calls while switching pages.
    """
    try:
        df = datetime.strptime(date_from, "%Y-%m-%d").date().isoformat()
        dt = datetime.strptime(date_to, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD") from exc
    if df > dt:
        df, dt = dt, df

    conn = get_conn()
    result_rows = []
    grouped = {}
    total_snapshot_rows = 0
    try:
        ensure_perf_snapshot_history_schema(conn)
        accs = [dict(a) for a in _fetch_visible_accounts(conn, user, account_id)]
        act_ids = [a["act_id"] for a in accs if a.get("act_id")]
        perf_cols = {r["name"] for r in conn.execute("PRAGMA table_info(perf_snapshots)").fetchall()}
        impressions_expr = "SUM(COALESCE(p.impressions,0))" if "impressions" in perf_cols else "0"
        clicks_expr = "SUM(COALESCE(p.clicks,0))" if "clicks" in perf_cols else "0"
        roas_expr = "AVG(CASE WHEN p.roas IS NOT NULL AND p.roas > 0 THEN p.roas END)" if "roas" in perf_cols else "NULL"

        if act_ids:
            placeholders = ",".join("?" for _ in act_ids)
            rows = conn.execute(
                f"""SELECT p.snapshot_date,
                           p.act_id,
                           COALESCE(a.name, p.act_id) AS account_name,
                           COALESCE(a.timezone, 'UTC') AS timezone,
                           COALESCE(a.currency, p.currency, 'USD') AS currency,
                           p.kpi_field,
                           COUNT(*) AS snapshot_rows,
                           SUM(COALESCE(p.spend,0)) AS spend_usd,
                           SUM(COALESCE(p.conversions,0)) AS conversions,
                           {impressions_expr} AS impressions,
                           {clicks_expr} AS clicks,
                           {roas_expr} AS roas
                    FROM perf_snapshots p
                    LEFT JOIN accounts a ON a.act_id=p.act_id
                    WHERE p.snapshot_date BETWEEN ? AND ?
                      AND p.act_id IN ({placeholders})
                    GROUP BY p.snapshot_date, p.act_id, a.name, a.timezone, a.currency, p.currency, p.kpi_field""",
                [df, dt] + act_ids,
            ).fetchall()
            for row in rows:
                total_snapshot_rows += int(row["snapshot_rows"] or 0)
                key = (row["snapshot_date"], row["act_id"])
                item = grouped.setdefault(key, {
                    "date": row["snapshot_date"],
                    "act_id": row["act_id"],
                    "account_name": row["account_name"] or row["act_id"],
                    "currency": (row["currency"] or "USD").upper(),
                    "timezone": row["timezone"] or "UTC",
                    "spend_usd": 0.0,
                    "conversions": 0.0,
                    "impressions": 0,
                    "clicks": 0,
                    "roas_values": [],
                    "source": "local_perf_snapshots",
                })
                item["spend_usd"] += float(row["spend_usd"] or 0)
                item["conversions"] += float(row["conversions"] or 0)
                item["impressions"] += int(row["impressions"] or 0)
                item["clicks"] += int(row["clicks"] or 0)
                if row["roas"]:
                    item["roas_values"].append(float(row["roas"] or 0))

        has_retention = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_spend_retention'"
        ).fetchone()
        if has_retention:
            retention_where = ["r.snapshot_date BETWEEN ? AND ?"]
            retention_params = [df, dt]
            if account_id:
                retention_where.append("r.act_id=?")
                retention_params.append(account_id)
            apply_team_scope(retention_where, retention_params, user, "r.team_id", include_unassigned=False)
            apply_account_owner_scope(retention_where, retention_params, user, "r.owner_user_id")
            retained = conn.execute(
                f"""SELECT r.snapshot_date,
                           r.act_id,
                           COALESCE(r.account_name, r.act_id) AS account_name,
                           COALESCE(r.currency, 'USD') AS currency,
                           SUM(COALESCE(r.spend,0)) AS spend_usd,
                           SUM(COALESCE(r.conversions,0)) AS conversions
                    FROM account_spend_retention r
                    WHERE {' AND '.join(retention_where)}
                    GROUP BY r.snapshot_date, r.act_id, r.account_name, r.currency""",
                retention_params,
            ).fetchall()
            for row in retained:
                key = (row["snapshot_date"], row["act_id"])
                if key in grouped:
                    continue
                grouped[key] = {
                    "date": row["snapshot_date"],
                    "act_id": row["act_id"],
                    "account_name": row["account_name"] or row["act_id"],
                    "currency": (row["currency"] or "USD").upper(),
                    "timezone": "archived",
                    "spend_usd": float(row["spend_usd"] or 0),
                    "conversions": float(row["conversions"] or 0),
                    "impressions": 0,
                    "clicks": 0,
                    "roas_values": [],
                    "source": "removed_account_retention",
                }
    finally:
        conn.close()

    total_usd = 0.0
    total_conversions = 0.0
    total_roas_list = []
    for item in grouped.values():
        spend_usd = float(item["spend_usd"] or 0)
        conversions = float(item["conversions"] or 0)
        currency = item["currency"] or "USD"
        rate = _get_rate(currency, None)
        spend_orig = round(spend_usd * rate, 2) if rate else spend_usd
        cpa_usd = round(spend_usd / conversions, 2) if conversions > 0 else 0
        cpa_orig = round(spend_orig / conversions, 2) if conversions > 0 else 0
        roas = round(sum(item["roas_values"]) / len(item["roas_values"]), 2) if item["roas_values"] else 0
        result_rows.append({
            "date": item["date"],
            "act_id": item["act_id"],
            "account_name": item["account_name"],
            "currency": currency,
            "timezone": item["timezone"],
            "spend_orig": spend_orig,
            "spend_usd": round(spend_usd, 2),
            "conversions": conversions,
            "cpa_orig": cpa_orig,
            "cpa_usd": cpa_usd,
            "roas": roas,
            "impressions": int(item["impressions"] or 0),
            "clicks": int(item["clicks"] or 0),
            "source": item["source"],
        })
        total_usd += spend_usd
        total_conversions += conversions
        if roas > 0:
            total_roas_list.append(roas)

    result_rows.sort(key=lambda x: (x["date"], x["spend_usd"]), reverse=True)
    avg_cpa = round(total_usd / total_conversions, 2) if total_conversions > 0 else 0
    avg_roas = round(sum(total_roas_list) / len(total_roas_list), 2) if total_roas_list else 0
    return {
        "total_usd": round(total_usd, 2),
        "total_conversions": total_conversions,
        "avg_cpa": avg_cpa,
        "avg_roas": avg_roas,
        "rows": result_rows,
        "source": "local_perf_snapshots",
        "snapshot_rows": total_snapshot_rows,
        "date_from": df,
        "date_to": dt,
    }


# ─── 其他接口（保持不变） ─────────────────────────────────────
@router.get("/ads")
def get_ads(
    act_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    kpi: Optional[str] = None,
    user=Depends(get_current_user)
):
    df, dt = _default_dates(date_from, date_to)
    conn = get_conn()
    accs = [dict(a) for a in _fetch_visible_accounts(conn, user, act_id)]
    kpi_filter = _normalize_dash_kpi_filter(kpi)
    result = []
    try:
        if not accs:
            return []
        act_ids = [a["act_id"] for a in accs if a.get("act_id")]
        if not act_ids:
            return []
        placeholders = ",".join("?" for _ in act_ids)
        kpi_rows = conn.execute(
            f"""SELECT act_id, target_id, kpi_field, kpi_label, target_cpa, source
                FROM kpi_configs
                WHERE level='ad' AND enabled=1 AND act_id IN ({placeholders})""",
            act_ids,
        ).fetchall()
        kpi_map = {(r["act_id"], r["target_id"]): dict(r) for r in kpi_rows}
        rows = conn.execute(
            f"""SELECT p.act_id,
                       COALESCE(a.name, p.act_id) AS account_name,
                       COALESCE(a.currency, p.currency, 'USD') AS currency,
                       COALESCE(a.timezone, 'UTC') AS timezone,
                       p.ad_id,
                       COALESCE(MAX(NULLIF(p.ad_name, '')), p.ad_id) AS ad_name,
                       COALESCE(MAX(NULLIF(p.adset_id, '')), '') AS adset_id,
                       COALESCE(MAX(NULLIF(p.campaign_id, '')), '') AS campaign_id,
                       p.kpi_field,
                       SUM(COALESCE(p.spend, 0)) AS spend,
                       SUM(COALESCE(p.impressions, 0)) AS impressions,
                       SUM(COALESCE(p.clicks, 0)) AS clicks,
                       SUM(COALESCE(p.conversions, 0)) AS conversions,
                       AVG(CASE WHEN p.roas IS NOT NULL AND p.roas > 0 THEN p.roas END) AS roas
                FROM perf_snapshots p
                LEFT JOIN accounts a ON a.act_id=p.act_id
                WHERE p.snapshot_date BETWEEN ? AND ?
                  AND p.act_id IN ({placeholders})
                  AND COALESCE(p.ad_id, '') <> ''
                GROUP BY p.act_id, a.name, a.currency, p.currency, a.timezone, p.ad_id, p.kpi_field""",
            [df, dt] + act_ids,
        ).fetchall()
        for row in rows:
            kpi_field = row["kpi_field"]
            if not _kpi_field_matches_filter(kpi_field, kpi_filter):
                continue
            ad_id_item = row["ad_id"]
            kpi_cfg = kpi_map.get((row["act_id"], ad_id_item), {})
            spend_usd = round(float(row["spend"] or 0), 2)
            conversions = float(row["conversions"] or 0)
            impressions = int(row["impressions"] or 0)
            clicks = int(row["clicks"] or 0)
            cpa = round(spend_usd / conversions, 2) if conversions > 0 else None
            roas = round(float(row["roas"] or 0), 2) if row["roas"] else None
            target_cpa = kpi_cfg.get("target_cpa")
            score_info = _score_ad_performance(
                spend_usd, int(conversions), clicks, impressions, 0, cpa or 0,
                target_cpa, roas or 0, kpi_field
            )
            result.append({
                "ad_id": ad_id_item,
                "ad_name": row["ad_name"] or ad_id_item,
                "act_id": row["act_id"],
                "account_name": row["account_name"] or row["act_id"],
                "currency": (row["currency"] or "USD").upper(),
                "timezone": row["timezone"] or "UTC",
                "date_from": df,
                "date_to": dt,
                "spend": spend_usd,
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "cpa": cpa,
                "roas": roas,
                "adset_id": row["adset_id"] or "",
                "adset_name": "",
                "campaign_id": row["campaign_id"] or "",
                "campaign_name": "",
                "target_cpa": target_cpa,
                "kpi_field": kpi_field,
                "kpi_label": kpi_cfg.get("kpi_label", ""),
                "kpi_source": kpi_cfg.get("source", ""),
                "source": kpi_cfg.get("source", ""),
                "data_source": "local_perf_snapshots",
                "score": score_info.get("score"),
                "score_label": score_info.get("label"),
                "score_level": score_info.get("level"),
                "score_reasons": score_info.get("reasons", []),
            })
        if result:
            spend_by_ad = {r.get("ad_id"): float(r.get("spend") or 0) for r in result if r.get("ad_id")}
            landing_map = _landing_ad_metrics_for_ads(
                conn,
                [r.get("ad_id") for r in result],
                date_from=df,
                date_to=dt,
                spend_by_ad=spend_by_ad,
            )
            for row in result:
                row["landing"] = landing_map.get(row.get("ad_id"), {})
    finally:
        conn.close()

    result.sort(key=lambda x: x.get("spend") or 0, reverse=True)
    return result


@router.post("/trigger-inspect")
def trigger_inspect(user=Depends(get_current_user)):
    import threading, time as _t
    from services.guard_engine import GuardEngine
    from core.tenancy import is_operator_user, user_id as _uid

    uid = _uid(user) if is_operator_user(user) else None
    team_id = None if is_superadmin(user) or uid else user.get("team_id")
    key = f"owner:{uid}" if uid else (f"team:{team_id}" if team_id else "all")

    def run():
        try:
            engine = GuardEngine()
            if uid:
                engine.run_all(operator_uid=uid)
            elif team_id:
                engine.run_all(team_id=team_id)
            else:
                engine.run_all()
            _LAST_INSPECT[key] = {"status": "done", "error": None, "ts": _t.time()}
        except Exception as e:
            _LAST_INSPECT[key] = {"status": "error", "error": str(e)[:200], "ts": _t.time()}

    _LAST_INSPECT[key] = {"status": "running", "error": None, "ts": _t.time()}
    threading.Thread(target=run, daemon=True).start()
    return {"message": "巡检已触发", "status": "running"}

_LAST_INSPECT = {}


@router.get("/scheduler/status")
def scheduler_status(user=Depends(get_current_user)):
    """Return real scheduler timing for frontend countdowns"""
    from core.scheduler import _job_state
    import time as _t
    now = _t.time()

    # Get last guard inspection time
    guard_state = _job_state.get("guard", {})
    last_finished = guard_state.get("last_finished_at", "")

    # Calculate next inspection
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key='inspect_interval'").fetchone()
    interval = int(row["value"]) if row and row["value"] else 10
    try: interval = int(interval)
    except: interval = 10

    next_inspect = None
    if last_finished:
        try:
            from datetime import datetime
            last_dt = datetime.strptime(last_finished, "%Y-%m-%d %H:%M:%S")
            next_inspect = (last_dt.timestamp() + interval * 60) * 1000  # ms for JS
        except:
            pass

    # Heartbeat info
    hb_enabled_row = conn.execute("SELECT value FROM settings WHERE key='heartbeat_enabled'").fetchone()
    hb_timeout_row = conn.execute("SELECT value FROM settings WHERE key='heartbeat_timeout'").fetchone()
    last_activity_row = conn.execute("SELECT value FROM settings WHERE key='last_admin_activity'").fetchone()
    conn.close()

    hb_enabled = (hb_enabled_row["value"] if hb_enabled_row else "0") == "1"
    hb_timeout_min = int(hb_timeout_row["value"]) if hb_timeout_row and hb_timeout_row["value"] else 30
    last_act_str = last_activity_row["value"] if last_activity_row else None

    last_activity_ts = None
    if last_act_str:
        try:
            from datetime import datetime
            last_activity_ts = datetime.strptime(last_act_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
        except:
            pass

    return {
        "next_inspect_at": next_inspect,
        "inspect_interval_min": interval,
        "heartbeat_enabled": hb_enabled,
        "heartbeat_timeout_min": hb_timeout_min,
        "last_activity_at": last_activity_ts,
    }

@router.get("/trigger-inspect/status")
def inspect_status(user=Depends(get_current_user)):
    from core.tenancy import is_operator_user, user_id as _uid
    if is_operator_user(user):
        key = f"owner:{_uid(user)}"
        return {key: _LAST_INSPECT.get(key)} if key in _LAST_INSPECT else {}
    if not is_superadmin(user):
        key = f"team:{user.get('team_id')}"
        return {key: _LAST_INSPECT.get(key)} if key in _LAST_INSPECT else {}
    return _LAST_INSPECT

@router.get("/stats")
def get_stats(user=Depends(get_current_user)):
    conn = get_conn()
    where, params = [], []
    apply_team_scope(where, params, user, "a.team_id", include_unassigned=False)
    apply_account_owner_scope(where, params, user, "a.owner_user_id")
    scope_sql = (" WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"""SELECT p.* FROM perf_snapshots p
            LEFT JOIN accounts a ON a.act_id = p.act_id
            {scope_sql}
            ORDER BY p.snapshot_date DESC LIMIT 100""",
        params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/system-logs")
def get_system_logs(lines: int = 100, user=Depends(get_current_user)):
    _require_superadmin_user(user)
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
    _require_superadmin_user(user)
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
    _require_operator_user(user)
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
            assert_row_access(conn, "accounts", item.act_id, user, id_column="act_id")
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
        assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
        rows = conn.execute(
            """SELECT * FROM kpi_configs WHERE level='account' AND act_id=? ORDER BY updated_at DESC""",
            (act_id,)
        ).fetchall()
    else:
        where, params = [], []
        apply_team_scope(where, params, user, "a.team_id", include_unassigned=False)
        apply_account_owner_scope(where, params, user, "a.owner_user_id")
        scope_sql = (" AND " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            f"""SELECT k.* FROM kpi_configs k
                LEFT JOIN accounts a ON a.act_id = k.act_id
                WHERE k.level='account'{scope_sql}
                ORDER BY k.act_id, k.updated_at DESC""",
            params,
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
    accs = _fetch_visible_accounts(conn, user)

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
           WHERE action_type='alert' AND trigger_type='budget_burn_fast'
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
    _require_operator_user(user)
    """
    设定默认精细化CPA（v1.2.0新增）
    act_id="*" 表示全局默认，否则为账户级默认
    同一账户+KPI字段组合唯一
    """
    from services.kpi_resolver import get_kpi_label
    conn = get_conn()
    if item.act_id == "*" and not is_superadmin(user):
        conn.close()
        raise HTTPException(status_code=403, detail="Global default CPA is superadmin only")
    if item.act_id != "*":
        assert_row_access(conn, "accounts", item.act_id, user, id_column="act_id")
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
