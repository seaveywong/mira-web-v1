"""
Mira v3.1 素材打分回写服务 (Asset Scorer)
────────────────────────────────────────
职责：
  1. 扫描 auto_campaign_ads 中已有 fb_ad_id 的广告，拉取其实际投放数据
  2. 将每条广告的数据 UPSERT 到 asset_spend_log 持久化表（不丢弃旧数据）
  3. 根据 asset_spend_log 中的汇总数据（含历史/已移除账户）计算素材得分
  4. 将得分和标签写回 ad_assets 表

持久化原则（v3.1 新增）：
  - 每次巡检时，能拉到数据的广告 → UPSERT 更新 asset_spend_log
  - 拉不到数据的广告（Token 失效/账户移除/API 限速）→ 保留旧数据，仅标记 is_active=0
  - 汇总评分时，读取 asset_spend_log 中所有记录（含 is_active=0 的历史数据）
  - 这样即使账户被移除、Token 过期、素材被替换，历史绩效永不丢失

转化字段识别规则（按优先级）：
  1. kpi_configs 中配置的 kpi_field 精确匹配，有数据则直接使用
  2. 若 kpi_field 无数据，则从 insights 中按优先级自动识别真实转化字段：
     purchase > lead/form > messaging_conversation > landing_page_view > link_click
  3. 不再使用"兜底累加所有 action"的方式，避免虚增转化数

评分规则（无目标 CPA 时）：
  - 有转化：基于转化量规模 + CPC 效率综合评分（不依赖 CPA 比率）
  - 无转化但消耗 < 止血线：基础分 30（数据不足，保留观察）
  - 无转化且消耗 ≥ 止血线：基础分 0（已止损关闭）

评分规则（有目标 CPA 时）：
  - 以实际 CPA / 目标 CPA 的比率作为主要评分依据

调用时机：
  - 由 guard_engine 的止损逻辑在关闭 AutoPilot 广告后触发
  - 由 scheduler 每天凌晨定时批量更新所有素材得分
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

import requests

from core.database import get_conn

logger = logging.getLogger("mira.asset_scorer")

FB_API_BASE = "https://graph.facebook.com/v25.0"

# 自动识别真实转化字段的优先级列表（从高到低）
# 每个元素是 (字段名, 是否为高价值转化)
AUTO_DETECT_PRIORITY = [
    # 电商购买类（最高价值）
    ("purchase",                                        True),
    ("offsite_conversion.fb_pixel_purchase",            True),
    ("web_in_store_purchase",                           True),
    ("onsite_web_purchase",                             True),
    ("omni_purchase",                                   True),
    # 表单/线索类
    ("lead",                                            True),
    ("onsite_conversion.lead_grouped",                  True),
    ("offsite_conversion.fb_pixel_lead",                True),
    ("contact_total",                                   True),
    # Messenger 对话类
    ("onsite_conversion.messaging_conversation_started_7d", True),
    ("onsite_conversion.messaging_first_reply",         True),
    ("onsite_conversion.messaging_welcome_message_view",True),
    # 加购类（次要）
    ("add_to_cart",                                     False),
    ("offsite_conversion.fb_pixel_add_to_cart",         False),
    ("omni_add_to_cart",                                False),
    # 落地页/点击类（兜底，仅在无上述转化时使用）
    ("landing_page_view",                               False),
    ("link_click",                                      False),
]


def _get_setting(key: str, default: str = "") -> str:
    try:
        conn = get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return default


def _get_kpi_field_for_ad(act_id: str) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """从 kpi_configs 获取该广告账户的 kpi_field、ad_type、target_cpa（去重取最新）"""
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT kpi_field, ad_type, target_cpa FROM kpi_configs WHERE act_id=? ORDER BY id DESC LIMIT 1",
            (act_id,)
        ).fetchone()
        conn.close()
        if row:
            return row["kpi_field"], row["ad_type"], row["target_cpa"]
        return None, None, None
    except Exception:
        return None, None, None


def _fb_get_ad_insights(fb_ad_id: str, token: str) -> Optional[dict]:
    """拉取单条广告的全时段历史数据"""
    try:
        resp = requests.get(
            f"{FB_API_BASE}/{fb_ad_id}",
            params={
                "access_token": token,
                "fields": "insights.date_preset(maximum, timeout=30){spend,actions,action_values,impressions,clicks}"
            },
            timeout=20
        )
        data = resp.json()
        if "error" in data:
            logger.warning(f"[Scorer] 广告 {fb_ad_id} FB API 错误: {data['error'].get('message', data['error'])}")
            return None
        insights = data.get("insights", {})
        if not insights or not insights.get("data"):
            return None
        return insights["data"][0]
    except Exception as e:
        logger.warning(f"[Scorer] 拉取广告 {fb_ad_id} 数据失败: {e}")
        return None


def _parse_conversions(insights: dict, kpi_field: Optional[str] = None) -> Tuple[float, int, float, str]:
    """
    从 insights 中提取消耗、转化数、转化价值、实际使用的转化字段名。

    优先级：
    1. kpi_field 精确匹配且有数据 → 直接使用
    2. 按 AUTO_DETECT_PRIORITY 顺序自动识别第一个有数据的字段
    3. 若全部为 0 → 返回 0 转化（不再兜底累加）

    返回: (spend, conv, conv_value, matched_field)
    """
    spend = float(insights.get("spend", 0) or 0)
    actions = insights.get("actions") or []
    action_values = insights.get("action_values") or []

    # 构建 action 快速查找字典
    action_map = {a["action_type"]: int(a.get("value", 0) or 0) for a in actions}
    av_map = {av["action_type"]: float(av.get("value", 0) or 0) for av in action_values}

    # 1. 优先：精确匹配 kpi_field
    if kpi_field and action_map.get(kpi_field, 0) > 0:
        conv = action_map[kpi_field]
        conv_value = av_map.get(kpi_field, 0.0)
        return spend, conv, conv_value, kpi_field

    # 2. 自动识别：按优先级找第一个有数据的字段
    for field, _ in AUTO_DETECT_PRIORITY:
        if action_map.get(field, 0) > 0:
            conv = action_map[field]
            conv_value = av_map.get(field, 0.0)
            return spend, conv, conv_value, field

    # 3. 无任何转化
    return spend, 0, 0.0, ""


def _score_label(score: float) -> str:
    if score >= 85:
        return "爆款"
    if score >= 72:
        return "可放量"
    if score >= 58:
        return "优质"
    if score >= 40:
        return "观察中"
    if score >= 20:
        return "待优化"
    return "淘汰"


def _traffic_adjustment(spend: float, impressions: int, clicks: int) -> Tuple[float, str]:
    """用点击信号做轻量修正，只影响边缘素材，不盖过转化结果。"""
    if impressions <= 0:
        return 0.0, ""
    ctr = (clicks / impressions) * 100 if impressions else 0.0
    cpc = spend / clicks if clicks > 0 else None
    adj = 0.0
    notes = [f"CTR {ctr:.2f}%"]
    if clicks > 0 and cpc is not None:
        notes.append(f"CPC ${cpc:.2f}")

    if ctr >= 2.0:
        adj += 4
    elif ctr >= 1.0:
        adj += 2
    elif spend >= 5 and ctr < 0.3:
        adj -= 5

    if cpc is not None:
        if cpc <= 0.5:
            adj += 3
        elif cpc >= 3 and spend >= 5:
            adj -= 3
    elif spend >= 3:
        adj -= 4

    return adj, "，".join(notes)


def _calc_score(spend: float, conv: int, conv_value: float,
                target_cpa: Optional[float], stop_loss: float,
                matched_field: str = "", impressions: int = 0,
                clicks: int = 0) -> Tuple[float, str, str]:
    """
    计算素材得分（0-100）和标签。

    有目标 CPA：以实际 CPA / 目标 CPA 比率评分
    无目标 CPA：以转化量规模 + 单次转化成本（绝对值）综合评分
    无转化：以消耗是否超止血线判断淘汰/观察

    返回：(score: float, label: str, reason: str)
    """
    spend = float(spend or 0)
    conv = int(conv or 0)
    conv_value = float(conv_value or 0)
    stop_loss = float(stop_loss or 15)
    traffic_adj, traffic_note = _traffic_adjustment(spend, impressions, clicks)

    if conv > 0:
        actual_cpa = spend / conv
        parts = [f"实投 ${spend:.2f}", f"转化 {conv}", f"CPA ${actual_cpa:.2f}"]

        if target_cpa and target_cpa > 0:
            cpa_ratio = actual_cpa / target_cpa
            parts.append(f"目标CPA ${target_cpa:.2f}")
            if cpa_ratio <= 0.5:
                score = 96
            elif cpa_ratio <= 0.8:
                score = 88
            elif cpa_ratio <= 1.0:
                score = 78
            elif cpa_ratio <= 1.3:
                score = 66
            elif cpa_ratio <= 1.5:
                score = 58
            elif cpa_ratio <= 2.0:
                score = 42
            elif cpa_ratio <= 2.8:
                score = 28
            else:
                score = 16
        else:
            # 没有目标 CPA 时，用止血线作为业务参照，不再用固定绝对 CPA 档位。
            ref_cpa = max(1.0, stop_loss)
            cpa_ratio = actual_cpa / ref_cpa
            if cpa_ratio <= 0.25:
                score = 90
            elif cpa_ratio <= 0.5:
                score = 78
            elif cpa_ratio <= 0.8:
                score = 66
            elif cpa_ratio <= 1.2:
                score = 54
            elif cpa_ratio <= 2.0:
                score = 38
            else:
                score = 24

        if conv >= 20:
            score += 10
        elif conv >= 10:
            score += 7
        elif conv >= 5:
            score += 4
        elif conv >= 2:
            score += 2

        if spend > 0 and conv_value > 0:
            roas = conv_value / spend
            parts.append(f"ROAS {roas:.2f}x")
            if roas >= 3:
                score += 10
            elif roas >= 2:
                score += 6
            elif roas >= 1:
                score += 2
            elif roas < 0.5:
                score -= 8

        score += traffic_adj
        metric_caps = {
            "link_click": (78, "点击类指标，仅作流量参考"),
            "landing_page_view": (82, "落地页浏览指标，低于表单/购买权重"),
            "add_to_cart": (88, "加购指标，未到最终购买"),
            "offsite_conversion.fb_pixel_add_to_cart": (88, "加购指标，未到最终购买"),
            "omni_add_to_cart": (88, "加购指标，未到最终购买"),
        }
        cap_info = metric_caps.get((matched_field or "").lower())
        if cap_info and score > cap_info[0]:
            score = cap_info[0]
            parts.append(cap_info[1])
        sample_cap = None
        if conv == 1 and spend < max(5.0, min(stop_loss, target_cpa or stop_loss) * 0.4):
            sample_cap = 68
        elif conv < 3 and spend < max(6.0, stop_loss * 0.75):
            sample_cap = 80
        if sample_cap is not None and score > sample_cap:
            score = sample_cap
            parts.append("样本偏少，限制评分上限")
    else:
        parts = [f"实投 ${spend:.2f}", "暂无转化"]
        if spend <= 0 and impressions <= 0:
            score = 0
        elif spend >= stop_loss:
            score = max(0, 8 + min(5, traffic_adj))
        elif spend >= stop_loss * 0.7:
            score = 22 + min(5, traffic_adj)
        else:
            score = 34 + max(-5, min(8, traffic_adj))

    if matched_field:
        parts.append(f"转化字段 {matched_field}")
    if traffic_note:
        parts.append(traffic_note)

    score = round(max(0, min(100, score)), 1)
    label = _score_label(score)
    reason = "；".join(parts)
    return score, label, reason


def _upsert_spend_log(asset_id: int, fb_ad_id: str, act_id: str,
                      act_name: str, target_countries: str, objective: str,
                      kpi_field: str, matched_field: str,
                      spend: float, conv: int, conv_value: float,
                      impressions: int, clicks: int):
    """
    将单条广告的数据 UPSERT 到 asset_spend_log 持久化表。
    使用 INSERT OR REPLACE 确保同一 (asset_id, fb_ad_id) 只保留最新数据。
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute(
        """INSERT INTO asset_spend_log
           (asset_id, fb_ad_id, act_id, act_name, target_countries, objective,
            kpi_field, matched_field, spend, conv, conv_value,
            impressions, clicks, is_active, last_synced_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)
           ON CONFLICT(asset_id, fb_ad_id) DO UPDATE SET
             act_name=excluded.act_name,
             target_countries=excluded.target_countries,
             objective=excluded.objective,
             kpi_field=excluded.kpi_field,
             matched_field=excluded.matched_field,
             spend=excluded.spend,
             conv=excluded.conv,
             conv_value=excluded.conv_value,
             impressions=excluded.impressions,
             clicks=excluded.clicks,
             is_active=1,
             last_synced_at=excluded.last_synced_at""",
        (asset_id, fb_ad_id, act_id, act_name, target_countries, objective,
         kpi_field or "", matched_field or "",
         spend, conv, conv_value, impressions, clicks, now)
    )
    conn.commit()
    conn.close()


def _mark_ad_inactive(asset_id: int, fb_ad_id: str):
    """
    将无法拉取数据的广告标记为 is_active=0，但保留旧数据。
    仅在该记录已存在时才更新（避免为从未成功拉取的广告创建空记录）。
    """
    conn = get_conn()
    conn.execute(
        """UPDATE asset_spend_log SET is_active=0
           WHERE asset_id=? AND fb_ad_id=?""",
        (asset_id, fb_ad_id)
    )
    conn.commit()
    conn.close()


def _get_account_name(act_id: str) -> str:
    """从 accounts 表获取账户名称"""
    try:
        conn = get_conn()
        row = conn.execute("SELECT name FROM accounts WHERE act_id=?", (act_id,)).fetchone()
        conn.close()
        return row["name"] if row and row["name"] else act_id
    except Exception:
        return act_id


def score_asset(asset_id: int, force_refresh: bool = False):
    """
    对单个素材进行打分并写回数据库。

    流程：
    1. 从 auto_campaign_ads 获取所有关联广告
    2. 对每条广告：
       a. 尝试从 FB API 拉取最新数据
       b. 成功 → UPSERT 到 asset_spend_log
       c. 失败（Token 失效/API 错误）→ 标记 is_active=0，保留旧数据
    3. 从 asset_spend_log 汇总所有历史数据（含 is_active=0）计算得分
    4. 写回 ad_assets 表

    参数：
      force_refresh: True 时强制从 FB API 拉取所有数据（忽略缓存）
    """
    conn = get_conn()
    ads = conn.execute(
        """SELECT aca.fb_ad_id, aca.act_id, ac.target_cpa,
                  ac.target_countries, ac.objective, ac.name as campaign_name
           FROM auto_campaign_ads aca
           JOIN auto_campaigns ac ON ac.id = aca.campaign_id
           WHERE aca.asset_id = ? AND aca.fb_ad_id IS NOT NULL AND aca.status = 'done'""",
        (asset_id,)
    ).fetchall()
    asset = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    conn.close()

    if not asset:
        return None

    stop_loss = float(_get_setting("autopilot_stop_loss", "15"))
    target_cpa = None

    # ── 步骤 1：从 FB API 拉取最新数据并写入持久化表 ──────────────────────────
    synced_count = 0
    failed_count = 0

    for ad in ads:
        if not ad["fb_ad_id"]:
            continue

        # 优先使用 auto_campaigns 里的 target_cpa
        if not target_cpa and ad["target_cpa"]:
            target_cpa = float(ad["target_cpa"])

        # 获取 Token
        try:
            from services.token_manager import get_exec_token, ACTION_READ
            token = get_exec_token(ad["act_id"], ACTION_READ)
        except Exception:
            token = None

        if not token:
            logger.warning(f"[Scorer] 广告 {ad['fb_ad_id']} 账户 {ad['act_id']} 无可用 Token，标记 inactive")
            _mark_ad_inactive(asset_id, ad["fb_ad_id"])
            failed_count += 1
            continue

        # 获取该账户的 KPI 配置
        kpi_field, ad_type, kpi_target_cpa = _get_kpi_field_for_ad(ad["act_id"])
        if not target_cpa and kpi_target_cpa:
            target_cpa = float(kpi_target_cpa)

        # 拉取 FB insights
        insights = _fb_get_ad_insights(ad["fb_ad_id"], token)
        if not insights:
            # 拉取失败：标记 inactive，保留旧数据
            _mark_ad_inactive(asset_id, ad["fb_ad_id"])
            failed_count += 1
            continue

        # 解析转化数据
        spend, conv, conv_value, matched_field = _parse_conversions(insights, kpi_field)
        impressions = int(insights.get("impressions", 0) or 0)
        clicks = int(insights.get("clicks", 0) or 0)

        # 获取账户名称
        act_name = _get_account_name(ad["act_id"])

        # UPSERT 到持久化表
        _upsert_spend_log(
            asset_id=asset_id,
            fb_ad_id=ad["fb_ad_id"],
            act_id=ad["act_id"],
            act_name=act_name,
            target_countries=ad["target_countries"] or "",
            objective=ad["objective"] or "",
            kpi_field=kpi_field or "",
            matched_field=matched_field,
            spend=spend,
            conv=conv,
            conv_value=conv_value,
            impressions=impressions,
            clicks=clicks
        )
        synced_count += 1

    logger.info(f"[Scorer] 素材 {asset_id} 数据同步: 成功={synced_count}, 失败/跳过={failed_count}")

    # ── 步骤 2：从持久化表汇总所有历史数据（含 inactive）────────────────────────
    conn = get_conn()
    logs = conn.execute(
        """SELECT spend, conv, conv_value, matched_field, act_id,
                  impressions, clicks, is_active, last_synced_at
           FROM asset_spend_log
           WHERE asset_id = ?""",
        (asset_id,)
    ).fetchall()

    # 如果持久化表没有数据（全新素材，从未成功同步），直接返回
    if not logs:
        conn.close()
        logger.info(f"[Scorer] 素材 {asset_id} 无历史数据，跳过评分")
        return None

    total_spend = sum(float(r["spend"] or 0) for r in logs)
    total_conv = sum(int(r["conv"] or 0) for r in logs)
    total_conv_value = sum(float(r["conv_value"] or 0) for r in logs)
    total_impressions = sum(int(r["impressions"] or 0) for r in logs)
    total_clicks = sum(int(r["clicks"] or 0) for r in logs)
    matched_fields = set(r["matched_field"] for r in logs if r["matched_field"])
    primary_matched_field = sorted(matched_fields)[0] if matched_fields else ""
    active_ads = sum(1 for r in logs if int(r["is_active"] or 0) == 1)
    synced_times = [r["last_synced_at"] for r in logs if r["last_synced_at"]]
    last_active_at = max(synced_times) if synced_times else None
    roas_values = [
        float(r["conv_value"] or 0) / float(r["spend"] or 0)
        for r in logs
        if float(r["spend"] or 0) > 0 and float(r["conv_value"] or 0) > 0
    ]
    best_roas = max(roas_values) if roas_values else None

    # 如果 target_cpa 还没从 auto_campaigns 里拿到，再从 kpi_configs 里找
    if not target_cpa:
        act_ids = list(set(r["act_id"] for r in logs))
        for act_id in act_ids:
            _, _, kpi_target_cpa = _get_kpi_field_for_ad(act_id)
            if kpi_target_cpa:
                target_cpa = float(kpi_target_cpa)
                break

    conn.close()

    # ── 步骤 3：计算评分 ────────────────────────────────────────────────────────
    score, label, reason = _calc_score(
        total_spend, total_conv, total_conv_value, target_cpa, stop_loss,
        primary_matched_field, total_impressions, total_clicks
    )

    avg_cpa = (total_spend / total_conv) if total_conv > 0 else None
    avg_roas = (total_conv_value / total_spend) if total_spend > 0 else None

    # ── 步骤 4：写回 ad_assets 表 ────────────────────────────────────────────────
    conn = get_conn()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(ad_assets)").fetchall()}
    updates = [
        "score=?", "score_label=?",
        "total_spend=?", "total_conv=?",
        "avg_cpa=?", "avg_roas=?",
    ]
    params = [score, label, total_spend, total_conv, avg_cpa, avg_roas]
    if "score_reason" in cols:
        updates.append("score_reason=?")
        params.append(reason)
    if "best_roas" in cols:
        updates.append("best_roas=?")
        params.append(best_roas)
    if "last_active_at" in cols:
        updates.append("last_active_at=?")
        params.append(last_active_at)
    updates.append("updated_at=?")
    params.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    params.append(asset_id)
    conn.execute(
        f"""UPDATE ad_assets SET
           {', '.join(updates)}
           WHERE id=?""",
        params
    )
    conn.commit()
    conn.close()

    logger.info(
        f"[Scorer] 素材 {asset_id} 打分完成: {score}分 ({label}) | "
        f"消耗={total_spend:.2f} 转化={total_conv} CPA={'%.2f' % avg_cpa if avg_cpa else 'N/A'} "
        f"转化字段={matched_fields} 活跃广告={active_ads}/{len(logs)} | {reason}"
    )
    return {
        "score": score, "label": label,
        "spend": total_spend, "conv": total_conv,
        "reason": reason,
        "matched_fields": list(matched_fields)
    }


def score_all_assets():
    """批量对所有有广告数据的素材进行打分（定时任务调用）"""
    conn = get_conn()
    asset_ids = [r[0] for r in conn.execute(
        "SELECT DISTINCT asset_id FROM auto_campaign_ads WHERE asset_id IS NOT NULL AND fb_ad_id IS NOT NULL"
    ).fetchall()]
    conn.close()

    logger.info(f"[Scorer] 开始批量打分，共 {len(asset_ids)} 个素材")
    results = []
    for asset_id in asset_ids:
        try:
            result = score_asset(asset_id)
            if result:
                results.append({"asset_id": asset_id, **result})
        except Exception as e:
            logger.error(f"[Scorer] 素材 {asset_id} 打分失败: {e}")
    return results
