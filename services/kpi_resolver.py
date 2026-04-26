"""
KPI 判定引擎 v1.2.0 - 基于 Mira v2.7.7 逻辑重构
多级降级策略: L0(手动) -> L1/L2(AI,可选) -> L3(经验库) -> L4(规则) -> L5(语义兜底)
AI分析为可选增强，未配置时静默跳过，不影响其他功能

修复记录:
v1.1.1 - effective_status 参数改用 http.client 直接发送，绕过 requests URL 编码
v1.2.0 - 修复推断优先级：L4规则（含messaging特判）优先于L3经验库；
         AI纠偏改为同步执行并写入ad级别；
         _ai_enhance_kpi 直接更新数据库中的错误推断；
         新增 destination_type 扩展支持 CONVERSATIONS/INSTAGRAM_DIRECT
"""
import re
import os
import json
import logging
import asyncio
import time
import urllib.parse
import http.client
import ssl
from typing import Optional, Tuple
import requests

from core.database import get_conn

logger = logging.getLogger("mira.kpi")

# ── L4 静态规则字典 ────────────────────────────────────────────────
_OBJECTIVE_RULES = {
    # Sales / Purchase
    "OUTCOME_SALES":                ("offsite_conversion.fb_pixel_purchase", "像素购买"),
    "OUTCOME_CONVERSIONS":          ("offsite_conversion.fb_pixel_purchase", "像素购买"),
    "CONVERSIONS":                  ("offsite_conversion.fb_pixel_purchase", "像素购买"),
    "OUTCOME_PRODUCT_CATALOG_SALES":("offsite_conversion.fb_pixel_purchase", "像素购买"),
    "OUTCOME_RETAIL":               ("offsite_conversion.fb_pixel_purchase", "像素购买"),
    # Leads
    "OUTCOME_LEADS":                ("onsite_conversion.lead_grouped",        "线索收集"),
    "LEAD_GENERATION":              ("onsite_conversion.lead_grouped",        "线索收集"),
    # Engagement
    "OUTCOME_ENGAGEMENT":           ("post_engagement",                       "帖子互动"),
    "OUTCOME_POST_ENGAGEMENT":      ("post_engagement",                       "帖子互动"),
    "PAGE_LIKES":                   ("page_likes",                            "主页获赞"),
    "OUTCOME_PAGE_LIKES":           ("page_likes",                            "主页获赞"),
    # Traffic
    "OUTCOME_TRAFFIC":              ("link_click",                             "链接点击"),
    "LINK_CLICKS":                  ("link_click",                             "链接点击"),
    # Awareness / Reach
    "OUTCOME_AWARENESS":            ("reach",                                  "触达人数"),
    "OUTCOME_BRAND_AWARENESS":      ("reach",                                  "触达人数"),
    "OUTCOME_REACH":                ("reach",                                  "触达人数"),
    "REACH":                        ("reach",                                  "触达人数"),
    # Video
    "OUTCOME_VIDEO_VIEWS":          ("video_view",                             "视频观看"),
    "VIDEO_VIEWS":                  ("video_view",                             "视频观看"),
    # App
    "OUTCOME_APP_PROMOTION":        ("app_install",                            "应用安装"),
    "APP_INSTALLS":                 ("app_install",                            "应用安装"),
    # Messages
    "MESSAGES":                     ("onsite_conversion.messaging_conversation_started_7d", "私信对话"),
    "OUTCOME_MESSAGES":             ("onsite_conversion.messaging_conversation_started_7d", "私信对话"),
}

_OPTGOAL_RULES = {
    # Purchase / Conversions
    "OFFSITE_CONVERSIONS":          ("offsite_conversion.fb_pixel_purchase", "像素购买"),
    "VALUE":                        ("offsite_conversion.fb_pixel_purchase", "像素购买"),
    # Leads
    "LEAD_GENERATION":              ("onsite_conversion.lead_grouped",       "线索收集"),
    # Messages
    "CONVERSATIONS":                ("onsite_conversion.messaging_conversation_started_7d", "私信对话"),
    # App
    "APP_INSTALLS":                 ("app_install",                          "应用安装"),
    # Traffic
    "LINK_CLICKS":                  ("link_click",                           "链接点击"),
    "LANDING_PAGE_VIEWS":           ("landing_page_view",                    "落地页浏览"),
    # Engagement
    "POST_ENGAGEMENT":              ("post_engagement",                      "帖子互动"),
    "PROFILE_AND_PAGE_ENGAGEMENT":  ("post_engagement",                      "帖子互动"),
    "PAGE_LIKES":                   ("page_likes",                           "主页获赞"),
    # Video
    "VIDEO_VIEWS":                  ("video_view",                           "视频观看"),
    # Awareness
    "REACH":                        ("reach",                                "触达人数"),
    "IMPRESSIONS":                  ("impressions",                          "展示次数"),
    "AD_RECALL_LIFT":              ("reach",                                "触达人数"),
}

_CUSTOM_EVENT_RULES = {
    "PURCHASE":             ("offsite_conversion.fb_pixel_purchase",  "像素购买"),
    "ADD_TO_CART":          ("offsite_conversion.fb_pixel_add_to_cart","加入购物车"),
    "INITIATE_CHECKOUT":    ("offsite_conversion.fb_pixel_initiate_checkout", "发起结账"),
    "LEAD":                 ("onsite_conversion.lead_grouped",         "线索收集"),
    "COMPLETE_REGISTRATION":("offsite_conversion.fb_pixel_complete_registration", "注册完成"),
    "SUBSCRIBE":            ("offsite_conversion.fb_pixel_subscribe",  "订阅"),
    "CONTACT":              ("offsite_conversion.fb_pixel_contact",    "像素联系"),
    "VIEW_CONTENT":         ("view_content",                           "浏览内容"),
    "SEARCH":               ("search",                                 "搜索"),
}

# 私信类 destination_type 集合（扩展）
_MESSAGING_DEST_TYPES = {
    "MESSENGER", "INSTAGRAM_DIRECT", "WHATSAPP",
    "CONVERSATIONS",  # 部分账户返回此值
}

# KPI字段完整映射表（用于前端展示）
KPI_FIELD_MAP = {
    # Purchase / Sales
    "offsite_conversion.fb_pixel_purchase":                 "像素购买",
    "offsite_conversion.fb_pixel_add_to_cart":              "像素加购",
    "offsite_conversion.fb_pixel_initiate_checkout":        "像素发起结账",
    "offsite_conversion.fb_pixel_lead":                     "像素潜在客户",
    "offsite_conversion.fb_pixel_complete_registration":    "像素注册完成",
    "offsite_conversion.fb_pixel_subscribe":                "像素订阅",
    "offsite_conversion.fb_pixel_contact":                  "像素联系",
    "offsite_conversion.fb_pixel_custom":                   "像素自定义事件",
    "purchase":                                             "购买",
    "omni_purchase":                                        "全渠道购买",
    "web_in_store_purchase":                                "线上到店购买",
    "onsite_web_purchase":                                  "网站内购买",
    "onsite_web_app_purchase":                              "网站应用购买",
    "web_app_in_store_purchase":                            "网站应用到店购买",
    # Lead / Contact
    "onsite_conversion.lead_grouped":                       "站内潜在客户",
    "lead":                                                 "潜在客户",
    "contact":                                              "联系",
    # Engagement
    "post_engagement":                                      "帖子互动",
    "page_engagement":                                      "主页互动",
    "page_likes":                                           "主页获赞",
    # Traffic
    "link_click":                                           "链接点击",
    "landing_page_view":                                    "落地页浏览",
    # Video
    "video_view":                                           "视频观看",
    # App
    "app_install":                                          "应用安装",
    # Awareness
    "reach":                                                "触达人数",
    "impressions":                                          "展示次数",
    # Messaging
    "onsite_conversion.messaging_conversation_started_7d":  "私信对话(7日)",
    "onsite_conversion.messaging_first_reply":              "私信首次回复",
    # Other
    "search":                                               "搜索",
    "view_content":                                         "浏览内容",
}

# 已知有效 KPI 字段集合（用于防 AI 幻觉 + 纠偏复审）
# 包含 KPI_FIELD_MAP 全部字段 + 常见 FB 标准 action_type
_KNOWN_KPI_FIELDS = set(KPI_FIELD_MAP.keys()) | {
    "page_like", "post_reaction", "post_save", "rate",
    "schedule", "start_trial", "apply_now",
    "donate", "get_quote", "request_time", "submit_application",
    # 归因变体: View Content 系列
    "omni_view_content", "onsite_web_view_content", "onsite_web_app_view_content",
    "offsite_conversion.fb_pixel_view_content", "offsite_content_view_add_meta_leads",
    # 归因变体: Add to Cart 系列
    "omni_add_to_cart", "onsite_web_add_to_cart", "onsite_web_app_add_to_cart",
    # 归因变体: Landing Page View 系列
    "omni_landing_page_view",
    # 其他常见标准事件
    "messaging_welcome_message_view",
}

# 高优先级字段（防 AI 幻觉用）
_HIGH_PRIORITY_FIELDS = [
    "onsite_conversion.messaging_conversation_started_7d",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.lead_grouped",
    "app_install",
]

# 辅助/上游字段（AI 应避免选择）
# 注意：post_engagement/page_engagement 是互动类广告的正确 KPI，不在此列
_AUXILIARY_FIELDS = {
    "messaging_welcome_message_view",
    "onsite_conversion.messaging_first_reply",
}

FIELD_RE = re.compile(r'^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_.]*)?$')

FB_API_BASE = "https://graph.facebook.com/v25.0"

# ── 所有未删除广告状态（KPI扫描范围）────────────────────────────────
_ALL_ACTIVE_STATUSES = '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED","PENDING_REVIEW","DISAPPROVED","WITH_ISSUES","IN_PROCESS","ARCHIVED"]'



def infer_ad_type(objective: str, optimization_goal: str, destination_type: str) -> str:
    """根据广告结构字段推断广告类型标签"""
    obj = (objective or "").upper()
    opt = (optimization_goal or "").upper()
    dst = (destination_type or "").upper()
    if dst == "MESSENGER" or opt == "CONVERSATIONS":
        return "messenger"
    if obj in ("OUTCOME_MESSAGES", "MESSAGES"):
        return "messenger"
    if opt in ("OFFSITE_CONVERSIONS", "VALUE") and obj in ("OUTCOME_SALES", "OUTCOME_CONVERSIONS", "OUTCOME_PRODUCT_CATALOG_SALES", "OUTCOME_RETAIL", "CONVERSIONS"):
        return "purchase"
    if opt in ("OFFSITE_CONVERSIONS", "LEAD_GENERATION") and obj in ("OUTCOME_LEADS", "LEAD_GENERATION"):
        return "leads"
    if obj in ("OUTCOME_TRAFFIC", "TRAFFIC", "LINK_CLICKS"):
        return "traffic"
    if opt in ("PAGE_LIKES", "POST_ENGAGEMENT", "PROFILE_AND_PAGE_ENGAGEMENT") or obj in ("PAGE_LIKES", "OUTCOME_ENGAGEMENT", "OUTCOME_POST_ENGAGEMENT", "OUTCOME_PAGE_LIKES"):
        return "engagement"
    if obj in ("OUTCOME_VIDEO_VIEWS", "VIDEO_VIEWS", "OUTCOME_AWARENESS", "OUTCOME_BRAND_AWARENESS", "OUTCOME_REACH", "REACH"):
        return "other"
    if obj or opt:
        return "other"
    return "other"


# ── DB 驱动规则缓存（300s TTL）────────────────────────────────────────
_KPI_RULES_CACHE = {}
_KPI_RULES_CACHE_TIME = 0
_KPI_RULES_CACHE_TTL = 300  # 5分钟

def _load_kpi_rules_cache():
    """从DB加载全量KPI规则到内存缓存，300s TTL"""
    global _KPI_RULES_CACHE, _KPI_RULES_CACHE_TIME
    now = time.time()
    if now - _KPI_RULES_CACHE_TIME < _KPI_RULES_CACHE_TTL and _KPI_RULES_CACHE:
        return
    try:
        conn = get_conn()
        _KPI_RULES_CACHE['composite'] = conn.execute(
            "SELECT objective, optimization_goal, custom_event_type, destination_type, kpi_field, kpi_label "
            "FROM kpi_composite_rules WHERE is_active=1 ORDER BY priority"
        ).fetchall()
        _KPI_RULES_CACHE['custom_event'] = conn.execute(
            "SELECT fb_custom_event_type AS custom_event, kpi_field, kpi_label FROM kpi_custom_event_rules WHERE is_active=1"
        ).fetchall()
        _KPI_RULES_CACHE['objective'] = conn.execute(
            "SELECT fb_objective AS objective, kpi_field, kpi_label FROM kpi_objective_rules WHERE is_active=1"
        ).fetchall()
        _KPI_RULES_CACHE['optgoal'] = conn.execute(
            "SELECT fb_optimization_goal AS optimization_goal, kpi_field, kpi_label FROM kpi_optgoal_rules WHERE is_active=1"
        ).fetchall()
        _KPI_RULES_CACHE['label_map'] = {
            r['kpi_field']: r['kpi_label']
            for r in conn.execute("SELECT kpi_field, kpi_label FROM kpi_label_map").fetchall()
            if r['kpi_label']
        }
        # known_fields: union of all kpi_field + fb_action_type
        known = set()
        for r in conn.execute("SELECT DISTINCT kpi_field FROM kpi_label_map").fetchall():
            known.add(r['kpi_field'])
        for r in conn.execute("SELECT DISTINCT fb_action_type FROM kpi_alias_map").fetchall():
            known.add(r['fb_action_type'])
        _KPI_RULES_CACHE['known_fields'] = known
        conn.close()
        _KPI_RULES_CACHE_TIME = now
        logger.info(f"KPI规则缓存已加载: composite={len(_KPI_RULES_CACHE.get('composite',[]))}, "
                    f"custom_event={len(_KPI_RULES_CACHE.get('custom_event',[]))}, "
                    f"objective={len(_KPI_RULES_CACHE.get('objective',[]))}, "
                    f"optgoal={len(_KPI_RULES_CACHE.get('optgoal',[]))}, "
                    f"known_fields={len(_KPI_RULES_CACHE.get('known_fields',set()))}")
    except Exception as e:
        logger.warning(f"KPI规则缓存加载失败（非致命）: {e}")


def _get_composite_rule(objective: str, opt_goal: str, custom_event: str, dest_type: str) -> Optional[Tuple[str, str]]:
    """组合矩阵：4字段渐进匹配（精确→3字段→2字段→1字段）"""
    try:
        _load_kpi_rules_cache()
        rules = _KPI_RULES_CACHE.get('composite', [])
    except Exception:
        return None
    obj, opt, ce, dst = objective or '', opt_goal or '', custom_event or '', dest_type or ''
    # 4字段全匹配
    for r in rules:
        if r['objective'] == obj and r['optimization_goal'] == opt and r['custom_event_type'] == ce and r['destination_type'] == dst:
            return r['kpi_field'], r['kpi_label']
    # 3字段 (obj+opt+ce)
    for r in rules:
        if r['objective'] == obj and r['optimization_goal'] == opt and r['custom_event_type'] == ce and r['destination_type'] == '':
            return r['kpi_field'], r['kpi_label']
    # 3字段 (obj+opt+dst)
    for r in rules:
        if r['objective'] == obj and r['optimization_goal'] == opt and r['custom_event_type'] == '' and r['destination_type'] == dst:
            return r['kpi_field'], r['kpi_label']
    # 2字段 (obj+opt)
    for r in rules:
        if r['objective'] == obj and r['optimization_goal'] == opt and r['custom_event_type'] == '' and r['destination_type'] == '':
            return r['kpi_field'], r['kpi_label']
    # 2字段 (obj+ce)
    for r in rules:
        if r['objective'] == obj and r['optimization_goal'] == '' and r['custom_event_type'] == ce and r['destination_type'] == '':
            return r['kpi_field'], r['kpi_label']
    # 1字段 (obj only)
    for r in rules:
        if r['objective'] == obj and r['optimization_goal'] == '' and r['custom_event_type'] == '' and r['destination_type'] == '':
            return r['kpi_field'], r['kpi_label']
    return None


def _get_custom_event_rule(custom_event: str) -> Optional[Tuple[str, str]]:
    """DB查询custom_event_type映射"""
    try:
        _load_kpi_rules_cache()
        for r in _KPI_RULES_CACHE.get('custom_event', []):
            if r['custom_event'] == custom_event:
                return r['kpi_field'], r['kpi_label']
    except Exception:
        pass
    return None


def _get_optgoal_rule(opt_goal: str) -> Optional[Tuple[str, str]]:
    """DB查询optimization_goal映射"""
    try:
        _load_kpi_rules_cache()
        for r in _KPI_RULES_CACHE.get('optgoal', []):
            if r['optimization_goal'] == opt_goal:
                return r['kpi_field'], r['kpi_label']
    except Exception:
        pass
    return None


def _get_objective_rule(objective: str) -> Optional[Tuple[str, str]]:
    """DB查询objective映射"""
    try:
        _load_kpi_rules_cache()
        for r in _KPI_RULES_CACHE.get('objective', []):
            if r['objective'] == objective:
                return r['kpi_field'], r['kpi_label']
    except Exception:
        pass
    return None


def _get_kpi_field_label(kpi_field: str) -> str:
    """DB查询kpi label，兜底到硬编码映射表"""
    if not kpi_field:
        return kpi_field
    try:
        _load_kpi_rules_cache()
        lbl = _KPI_RULES_CACHE.get('label_map', {}).get(kpi_field)
        if lbl:
            return lbl
    except Exception:
        pass
    return KPI_FIELD_MAP.get(kpi_field, kpi_field)


def _get_known_kpi_fields() -> set:
    """从DB获取已知KPI字段集合，与硬编码集合合并返回"""
    try:
        _load_kpi_rules_cache()
        db_fields = _KPI_RULES_CACHE.get('known_fields')
        if db_fields:
            return db_fields | _KNOWN_KPI_FIELDS
    except Exception:
        pass
    return _KNOWN_KPI_FIELDS


def _fb_get_with_status(path: str, token: str, fields: str,
                        effective_status: str = None, limit: int = 200) -> dict:
    """
    FB API GET 请求，支持 effective_status 参数（完全不做URL编码）。
    注意：不能使用 requests.get()，因为 requests 在 PreparedRequest 阶段会
    对 URL 中的方括号 [ ] 和引号 " 进行 URL 编码，导致 FB API 400 错误。
    改用 http.client 直接发送，完全控制 URL 格式。
    """
    base_params = urllib.parse.urlencode({
        "access_token": token,
        "fields": fields,
        "limit": limit,
    })
    query = base_params
    if effective_status:
        query += f"&effective_status={effective_status}"
    
    full_path = f"/v25.0/{path}?{query}"
    ctx = ssl.create_default_context()
    conn = http.client.HTTPSConnection("graph.facebook.com", context=ctx, timeout=30)
    try:
        conn.request("GET", full_path)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        import json as _json
        data = _json.loads(body)
        if resp.status >= 400:
            err_msg = data.get("error", {}).get("message", body[:200])
            raise Exception(f"{resp.status} Error: {err_msg} for url: https://graph.facebook.com{full_path[:100]}...")
        return data
    finally:
        conn.close()


def _fb_get_all_pages(path: str, token: str, fields: str,
                     effective_status: str = None,
                     limit: int = 200, max_total: int = 2000) -> list:
    """FB API 分页拉取，自动翻页直到无更多数据或达到 max_total 上限。"""
    import json as _json
    all_items = []
    after_cursor = None
    while len(all_items) < max_total:
        base_params = urllib.parse.urlencode({
            "access_token": token, "fields": fields, "limit": limit,
        })
        query = base_params
        if effective_status:
            query += f"&effective_status={effective_status}"
        if after_cursor:
            query += f"&after={urllib.parse.quote(after_cursor, safe='')}"
        full_path = f"/v25.0/{path}?{query}"
        ctx = ssl.create_default_context()
        hconn = http.client.HTTPSConnection("graph.facebook.com", context=ctx, timeout=30)
        try:
            hconn.request("GET", full_path)
            resp = hconn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            data = _json.loads(body)
            if resp.status >= 400:
                err_msg = data.get("error", {}).get("message", body[:200])
                raise Exception(f"{resp.status} Error: {err_msg}")
        finally:
            hconn.close()
        items = data.get("data", [])
        all_items.extend(items)
        paging = data.get("paging", {})
        cursors = paging.get("cursors", {})
        after_cursor = cursors.get("after") or paging.get("next", None)
        if after_cursor and after_cursor.startswith("http"):
            import urllib.parse as _up
            parsed = _up.urlparse(after_cursor)
            qs = _up.parse_qs(parsed.query)
            after_cursor = qs.get("after", [None])[0]
        if not after_cursor or len(items) < limit:
            break
    return all_items


def _get_setting(key: str, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def _is_ai_enabled() -> bool:
    """检查AI是否已配置且启用"""
    enabled = _get_setting("ai_enabled", "0")
    api_key = _get_setting("ai_api_key", "")
    return enabled == "1" and bool(api_key)


def get_kpi_label(kpi_field: str) -> str:
    return _get_kpi_field_label(kpi_field)


def _is_valid_kpi_field(field: str, actions: list = None) -> bool:
    """验证 KPI 字段名是否合法（DB驱动 + 硬编码兜底）"""
    if not field or not FIELD_RE.match(field):
        return False
    known = _get_known_kpi_fields()
    if field in known:
        return True
    if field in _AUXILIARY_FIELDS:
        return True
    if actions:
        action_types = {a.get("action_type", "") for a in actions}
        if field in action_types:
            return True
    return False


def _is_messaging_ad(campaign_meta: dict) -> bool:
    """
    判断是否为私信类广告。
    私信类广告的特征：
    1. objective == 'MESSAGES'
    2. destination_type 在 _MESSAGING_DEST_TYPES 中
    3. optimization_goal == 'CONVERSATIONS'
    4. ad_name 中包含 messaging 相关关键词（兜底）
    """
    objective = campaign_meta.get("objective", "")
    dest_type = campaign_meta.get("destination_type", "")
    opt_goal = campaign_meta.get("optimization_goal", "")

    if objective == "MESSAGES":
        return True
    if dest_type.upper() in _MESSAGING_DEST_TYPES:
        return True
    if opt_goal == "CONVERSATIONS":
        return True
    return False


class KpiResolver:
    """
    KPI 多级降级判定引擎 v1.2.0
    修复：L4规则（含messaging特判）优先于L3经验库，避免OUTCOME_ENGAGEMENT被错误推断
    """

    def __init__(self, act_id: str, campaign_id: str):
        self.act_id = act_id
        self.campaign_id = campaign_id

    def resolve(self, campaign_meta: dict, actions: list) -> Tuple[str, str, str]:
        """
        返回 (kpi_field, kpi_label, source)
        source: manual / ai / rule / history / fallback / unknown

        优先级调整（v1.2.0）：
        L0: 手动配置（最高优先级）
        L4: 规则引擎（先于L3，因为L4有私信特判，能覆盖L3的OUTCOME_ENGAGEMENT误判）
        L3: 经验库（L4无法判断时才用）
        L5: 语义候选匹配
        """
        # L0: 手动配置（最高优先级）
        result = self._l0_manual()
        if result:
            return result[0], result[1], "manual"

        # L4: 规则引擎（优先于L3，因为L4有更精细的私信/optimization_goal判断）
        result = self._l4_rule(campaign_meta, actions)
        if result:
            # 如果L4推断的是通用字段（如post_engagement），且AI可用，尝试AI纠偏
            if result[0] in _AUXILIARY_FIELDS and actions and _is_ai_enabled():
                ai_result = self._l1_ai_sync(campaign_meta, actions)
                if ai_result:
                    return ai_result[0], ai_result[1], "ai"
            return result[0], result[1], "rule"

        # L3: 经验库（L4无法判断时才用）
        result = self._l3_history(campaign_meta.get("objective", ""))
        if result:
            # 如果L3推断的是通用字段（如post_engagement），且AI可用，尝试AI纠偏
            if result[0] in _AUXILIARY_FIELDS and actions and _is_ai_enabled():
                ai_result = self._l1_ai_sync(campaign_meta, actions)
                if ai_result:
                    return ai_result[0], ai_result[1], "ai"
            return result[0], result[1], "history"

        # L5: 语义候选匹配
        result = self._l5_fallback(actions)
        if result:
            return result[0], result[1], "fallback"

        return "link_click", "链接点击", "default"

    def _l0_manual(self) -> Optional[Tuple[str, str]]:
        conn = get_conn()
        row = conn.execute(
            """SELECT kpi_field, kpi_label FROM kpi_configs
               WHERE act_id=? AND target_id=? AND source='manual' AND enabled=1
               ORDER BY level DESC LIMIT 1""",
            (self.act_id, self.campaign_id)
        ).fetchone()
        conn.close()
        if row and row["kpi_field"]:
            return row["kpi_field"], row["kpi_label"] or get_kpi_label(row["kpi_field"])
        return None

    def _l3_history(self, objective: str) -> Optional[Tuple[str, str]]:
        if not objective:
            return None
        try:
            from_cache = _get_objective_rule(objective)
            if from_cache:
                return from_cache
        except Exception:
            pass
        if objective in _OBJECTIVE_RULES:
            return _OBJECTIVE_RULES[objective]
        return None

    def _l4_rule(self, meta: dict, actions: list = None) -> Optional[Tuple[str, str]]:
        objective = meta.get("objective", "")
        opt_goal = meta.get("optimization_goal", "")
        custom_event = meta.get("custom_event_type", "")
        dest_type = meta.get("destination_type", "")

        # 私信类特判（最高优先级）：涵盖所有私信类型
        if _is_messaging_ad(meta):
            return ("onsite_conversion.messaging_conversation_started_7d", "私信对话")

        # L4: 组合矩阵（DB优先，4字段精确匹配→渐进降级）
        try:
            composite = _get_composite_rule(objective, opt_goal, custom_event, dest_type)
            if composite:
                return composite
        except Exception:
            pass

        # L4': 自定义事件（DB优先）
        if custom_event:
            try:
                ce_rule = _get_custom_event_rule(custom_event)
                if ce_rule:
                    return ce_rule
            except Exception:
                pass
            if custom_event in _CUSTOM_EVENT_RULES:
                return _CUSTOM_EVENT_RULES[custom_event]

        # 组合规则（硬编码兜底，覆盖DB缺失的边缘情况）
        if objective == "PAGE_LIKES":
            return ("page_likes", "主页获赞")
        if objective == "OUTCOME_LEADS" and opt_goal in ("OFFSITE_CONVERSIONS", "LEAD_GENERATION", ""):
            dst = (dest_type or "").upper()
            if dst == "WEBSITE":
                return ("offsite_conversion.fb_pixel_lead", "像素潜在客户")
            if opt_goal == "OFFSITE_CONVERSIONS":
                return ("offsite_conversion.fb_pixel_lead", "像素潜在客户")
            if actions:
                action_types = {a.get("action_type", "") for a in actions}
                if "offsite_conversion.fb_pixel_lead" in action_types:
                    return ("offsite_conversion.fb_pixel_lead", "像素潜在客户")
                if "onsite_conversion.lead_grouped" in action_types:
                    return ("onsite_conversion.lead_grouped", "线索收集")
            return ("onsite_conversion.lead_grouped", "线索收集")
        if objective == "OUTCOME_ENGAGEMENT" and opt_goal in (
            "PROFILE_AND_PAGE_ENGAGEMENT", "POST_ENGAGEMENT", ""
        ):
            return ("post_engagement", "帖子互动")
        if objective == "OUTCOME_ENGAGEMENT" and opt_goal == "VIDEO_VIEWS":
            return ("video_view", "视频观看")

        # opt_goal规则（DB优先）
        if opt_goal:
            try:
                og_rule = _get_optgoal_rule(opt_goal)
                if og_rule:
                    return og_rule
            except Exception:
                pass
            if opt_goal in _OPTGOAL_RULES:
                return _OPTGOAL_RULES[opt_goal]

        # objective规则（DB优先）
        if objective:
            try:
                obj_rule = _get_objective_rule(objective)
                if obj_rule:
                    return obj_rule
            except Exception:
                pass
            if objective in _OBJECTIVE_RULES:
                return _OBJECTIVE_RULES[objective]
        return None

    def _l5_fallback(self, actions: list) -> Optional[Tuple[str, str]]:
        if not actions:
            return None
        # 转化型动作优先于 engagement 类，确保 KPI 反映真实转化
        conversion_actions = []
        engagement_actions = []
        for a in actions:
            field = a.get("action_type", "")
            value = float(a.get("value", 0))
            if not field or field in _AUXILIARY_FIELDS or not FIELD_RE.match(field):
                continue
            if (field.startswith("offsite_conversion.") or
                field.startswith("onsite_conversion.") or
                field in ("purchase", "lead", "omni_purchase", "app_install")):
                conversion_actions.append((field, value))
            else:
                engagement_actions.append((field, value))
        # 优先选最高数值的转化动作
        if conversion_actions:
            conversion_actions.sort(key=lambda x: x[1], reverse=True)
            field = conversion_actions[0][0]
            return field, get_kpi_label(field)
        # 无转化数据时选 engagement 类中最高
        if engagement_actions:
            engagement_actions.sort(key=lambda x: x[1], reverse=True)
            field = engagement_actions[0][0]
            return field, get_kpi_label(field)
        return None

    def _l1_ai_sync(self, meta: dict, actions: list) -> Optional[Tuple[str, str]]:
        """
        同步AI推断（v1.2.0新增）
        当规则推断出辅助字段时，同步调用AI纠偏，返回更准确的字段
        """
        if not _is_ai_enabled():
            return None
        try:
            from openai import OpenAI
            api_key = _get_setting("ai_api_key", "")
            api_base = _get_setting("ai_api_base", "https://api.deepseek.com/v1")
            model = _get_setting("ai_model", "deepseek-chat")
            if not api_key:
                return None

            client = OpenAI(api_key=api_key, base_url=api_base)
            sorted_actions = sorted(actions, key=lambda x: float(x.get("value", 0)), reverse=True)[:15]
            actions_summary = "\n".join(
                f"  - {a.get('action_type')}: {a.get('value')} 次"
                for a in sorted_actions
            )

            prompt = f"""你是 Facebook 广告 KPI 分析专家。请根据以下广告配置，判断该广告的核心 KPI 字段。

广告配置：
- 活动目标 (objective): {meta.get('objective', '未知')}
- 优化目标 (optimization_goal): {meta.get('optimization_goal', '未知')}
- 自定义事件 (custom_event_type): {meta.get('custom_event_type', '无')}
- 目标类型 (destination_type): {meta.get('destination_type', '未知')}
- 近7日花费: ${meta.get('spend', 0):.2f}

近7日 Actions 数据（按数量降序）：
{actions_summary if actions_summary else '  暂无数据'}

判断规则：
1. 私信类广告（MESSENGER/INSTAGRAM_DIRECT/CONVERSATIONS/MESSAGES目标）优先选 onsite_conversion.messaging_conversation_started_7d
2. 电商/转化类优先选 offsite_conversion.fb_pixel_purchase
3. 线索类优先选 onsite_conversion.lead_grouped
4. 避免选择辅助/上游指标（如 messaging_welcome_message_view, post_engagement, page_engagement）
5. 优先选择数量最多的核心转化字段

请只返回 JSON 格式，不要有其他文字：
{{"field": "字段名", "label": "中文名称", "reason": "简短理由"}}"""

            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=200
            )
            content = response.choices[0].message.content.strip()
            if "```" in content:
                content = content.split("```")[1].replace("json", "").strip()
            data = json.loads(content)
            field = data.get("field", "").strip()
            label = data.get("label", field)
            if field and _is_valid_kpi_field(field, actions):
                logger.info(f"AI同步纠偏: {self.campaign_id} -> {field} ({label}) - {data.get('reason', '')}")
                return field, label
            return None
        except Exception as e:
            logger.warning(f"AI同步纠偏失败（非致命）: {e}")
            return None

    async def _l1_ai_async(self, meta: dict, actions: list):
        """异步 AI 推断（保留兼容性），结果写回数据库 ad 级别"""
        if not _is_ai_enabled():
            return
        try:
            from services.ai_advisor import ask_kpi
            field, label = await ask_kpi(meta, actions)
            if field and _is_valid_kpi_field(field, actions):
                field, label = self._anti_hallucination(field, label, actions)
                conn = get_conn()
                # 写入 ad 级别（而非 campaign 级别）
                conn.execute(
                    """UPDATE kpi_configs SET kpi_field=?, kpi_label=?, source='ai',
                       updated_at=datetime('now')
                       WHERE act_id=? AND target_id=? AND source!='manual'""",
                    (field, label, self.act_id, self.campaign_id)
                )
                conn.commit()
                conn.close()
                logger.info(f"AI异步纠偏完成: {self.campaign_id} -> {field}")
        except Exception as e:
            logger.warning(f"AI KPI 推断失败（非致命）: {e}")

    def _anti_hallucination(self, field: str, label: str, actions: list) -> Tuple[str, str]:
        if field in _AUXILIARY_FIELDS:
            action_fields = {a.get("action_type", "") for a in actions}
            for hp in _HIGH_PRIORITY_FIELDS:
                if hp in action_fields:
                    logger.info(f"防幻觉替换: {field} -> {hp}")
                    return hp, get_kpi_label(hp)
        return field, label


def get_kpi_for_ad(act_id: str, ad_id: str, campaign_id: str,
                   campaign_meta: dict, actions: list,
                   adset_id: str = "") -> Tuple[str, str, str]:
    """
    获取广告级别的 KPI 配置
    优先级：广告级 > 广告组级 > Campaign级 > 账户级 > 自动推断
    v3.3.6: 自愈逻辑——DB 中字段不合法时删除并重新推断，写回正确结果
    返回 (kpi_field, kpi_label, source)
    """
    conn = get_conn()
    found_invalid = False
    for level, tid in [("ad", ad_id), ("adset", adset_id), ("campaign", campaign_id), ("account", act_id)]:
        if not tid:
            continue
        row = conn.execute(
            """SELECT kpi_field, kpi_label, source FROM kpi_configs
               WHERE act_id=? AND target_id=? AND enabled=1 LIMIT 1""",
            (act_id, tid)
        ).fetchone()
        if row and row["kpi_field"]:
            if _is_valid_kpi_field(row["kpi_field"], actions):
                # v3.3.7: custom_event_type 覆盖——adset 的像素事件优先级高于历史存储
                # v3.3.12: DB驱动优先，硬编码兜底
                custom_event = (campaign_meta.get("custom_event_type") or "").upper()
                expected_field = None
                if custom_event:
                    try:
                        ce_rule = _get_custom_event_rule(custom_event)
                        if ce_rule:
                            expected_field = ce_rule[0]
                    except Exception:
                        pass
                    if not expected_field and custom_event in _CUSTOM_EVENT_RULES:
                        expected_field = _CUSTOM_EVENT_RULES[custom_event][0]
                if expected_field and row["kpi_field"] != expected_field:
                    logger.warning(f"KPI自愈(字段重写): {level}({tid}) custom_event={custom_event}, 存储={row['kpi_field']}->{expected_field}")
                    conn.execute(
                        "UPDATE kpi_configs SET kpi_field=?, kpi_label=?, source='auto', updated_at=datetime('now') WHERE act_id=? AND target_id=? AND enabled=1",
                        (expected_field, get_kpi_label(expected_field), act_id, tid)
                    )
                    conn.commit()
                    conn.close()
                    return expected_field, get_kpi_label(expected_field), "auto"
                conn.close()
                return row["kpi_field"], row["kpi_label"] or get_kpi_label(row["kpi_field"]), row["source"]
            else:
                logger.warning(f"KPI自愈: {level}({tid}) DB字段 '{row['kpi_field']}' (source={row['source']}) 不合法，删除并降级")
                conn.execute("DELETE FROM kpi_configs WHERE act_id=? AND target_id=? AND enabled=1", (act_id, tid))
                found_invalid = True
                break  # 有一级不合法就不再向上查找
    if found_invalid:
        conn.commit()
    conn.close()

    resolver = KpiResolver(act_id, campaign_id)
    field, label, source = resolver.resolve(campaign_meta, actions)

    # v3.3.7: 自愈写回时也尊重 custom_event_type (DB驱动优先)
    if not found_invalid:
        custom_event = (campaign_meta.get("custom_event_type") or "").upper()
        expected_field = None
        if custom_event:
            try:
                ce_rule = _get_custom_event_rule(custom_event)
                if ce_rule:
                    expected_field = ce_rule[0]
            except Exception:
                pass
            if not expected_field and custom_event in _CUSTOM_EVENT_RULES:
                expected_field = _CUSTOM_EVENT_RULES[custom_event][0]
        if expected_field and field != expected_field:
            field, label = expected_field, get_kpi_label(expected_field)
            source = "auto"

    # 自愈写回：如果删除了非法记录，把正确推断写回 ad 级别
    if found_invalid:
        try:
            conn2 = get_conn()
            conn2.execute(
                "INSERT OR REPLACE INTO kpi_configs "
                "(act_id, level, target_id, target_name, kpi_field, kpi_label, source, enabled, updated_at) "
                "VALUES (?, 'ad', ?, '', ?, ?, 'auto', 1, datetime('now'))",
                (act_id, ad_id, field, label)
            )
            conn2.commit()
            conn2.close()
            logger.info(f"KPI自愈写回: {ad_id} -> {field} ({label})")
        except Exception as e:
            logger.warning(f"KPI自愈写回失败（非致命）: {e}")

    return field, label, source


def scan_and_preset_kpi(act_id: str, token: str) -> dict:
    """
    扫描账户下所有广告（含已暂停），自动推断并写入KPI配置（广告级颗粒度）
    - 不覆盖人工配置（source='manual'）
    - AI已启用时，同步进行AI增强分析并纠正错误推断
    返回: {created, updated, skipped, total, details, ai_suggestions}

    注意：effective_status 必须手动拼接到URL，不能放入 requests params，
    否则 requests 会将方括号和引号 URL 编码，导致 FB API 400 错误。
    """
    created = 0
    updated = 0
    skipped = 0
    details = []

    try:
        # 扫描所有未删除状态（ACTIVE/PAUSED/ADSET_PAUSED/CAMPAIGN_PAUSED/
        # PENDING_REVIEW/DISAPPROVED/WITH_ISSUES/IN_PROCESS/ARCHIVED）
        # 注意：campaign 子字段只支持 objective（不支持 optimization_goal/destination_type）
        # adset 子字段支持 optimization_goal（不支持 destination_type）
        fields = (
            "id,name,adset_id,campaign_id,"
            "campaign{objective},"
            "adset{optimization_goal,destination_type,custom_event_type},"
            "insights.date_preset(last_7d){actions,spend}"
        )
        ads = _fb_get_all_pages(
            f"{act_id}/ads", token, fields,
            effective_status=_ALL_ACTIVE_STATUSES,
            limit=200, max_total=2000
        )
    except Exception as e:
        logger.error(f"扫描KPI失败 {act_id}: {e}")
        return {"created": 0, "updated": 0, "skipped": 0, "error": str(e)}

    conn = get_conn()

    # 自愈清理：删除所有非法非手动的 KPI 记录，使其重新推断
    invalid_rows = conn.execute(
        "SELECT id, target_id, kpi_field FROM kpi_configs "
        "WHERE act_id=? AND level='ad' AND enabled=1 AND source!='manual'",
        (act_id,)
    ).fetchall()
    cleaned = 0
    for ir in invalid_rows:
        if not _is_valid_kpi_field(ir["kpi_field"]):
            conn.execute("DELETE FROM kpi_configs WHERE id=?", (ir["id"],))
            cleaned += 1
            logger.warning(f"scan_and_preset_kpi清理: {act_id} {ir['target_id']} '{ir['kpi_field']}' 非法，删除")
    if cleaned:
        logger.info(f"scan_and_preset_kpi: {act_id} 清理了 {cleaned} 条非法 KPI 记录")
    conn.commit()

    for ad in ads:
        ad_id = ad["id"]
        ad_name = ad.get("name", ad_id)
        adset_id = ad.get("adset_id", "")
        campaign_id = ad.get("campaign_id", "")

        # 提取campaign元数据
        campaign_meta = {}
        camp_data = ad.get("campaign", {})
        if isinstance(camp_data, dict):
            campaign_meta["objective"] = camp_data.get("objective", "")

        # 从adset获取更精细的优化目标和目标类型
        adset_data = ad.get("adset", {})
        if isinstance(adset_data, dict):
            campaign_meta["optimization_goal"] = adset_data.get("optimization_goal", "")
            campaign_meta["destination_type"] = adset_data.get("destination_type", "")
            campaign_meta["custom_event_type"] = adset_data.get("custom_event_type", "")

        # 从近7天actions推断
        insights = ad.get("insights", {}).get("data", [])
        actions = []
        spend = 0.0
        if insights:
            actions = insights[0].get("actions", [])
            spend = float(insights[0].get("spend", 0))
        campaign_meta["spend"] = spend
        campaign_meta["name"] = ad_name

        # 检查是否已有人工配置（不覆盖）
        existing = conn.execute(
            "SELECT id, source, kpi_field FROM kpi_configs "
            "WHERE act_id=? AND target_id=? AND enabled=1 LIMIT 1",
            (act_id, ad_id)
        ).fetchone()

        if existing and existing["source"] == "manual":
            skipped += 1
            details.append({
                "ad_id": ad_id, "ad_name": ad_name,
                "action": "skipped", "reason": "已有人工配置",
                "kpi_field": existing["kpi_field"],
                "kpi_label": get_kpi_label(existing["kpi_field"])
            })
            continue

        # 使用 KpiResolver 推断（v1.2.0: L4优先于L3，含AI同步纠偏）
        resolver = KpiResolver(act_id, campaign_id)
        kpi_field, kpi_label, source = resolver.resolve(campaign_meta, actions)

        if existing:
            conn.execute(
                "UPDATE kpi_configs SET kpi_field=?, kpi_label=?, source=?, "
                "objective=?, optimization_goal=?, destination_type=?, ad_type=?, "
                "updated_at=datetime('now') WHERE act_id=? AND target_id=?",
                (kpi_field, kpi_label, source,
                 campaign_meta.get("objective",""),
                 campaign_meta.get("optimization_goal",""),
                 campaign_meta.get("destination_type",""),
                 infer_ad_type(campaign_meta.get("objective",""), campaign_meta.get("optimization_goal",""), campaign_meta.get("destination_type","")),
                 act_id, ad_id)
            )
            updated += 1
            action_str = "updated"
        else:
            conn.execute(
                "INSERT OR REPLACE INTO kpi_configs "
                "(act_id, level, target_id, target_name, kpi_field, kpi_label, source, enabled, "
                "objective, optimization_goal, destination_type, ad_type, updated_at) "
                "VALUES (?,?,?,?,?,?,?,1,?,?,?,?,datetime('now'))"
                ,
                (act_id, "ad", ad_id, ad_name, kpi_field, kpi_label, source,
                 campaign_meta.get("objective",""),
                 campaign_meta.get("optimization_goal",""),
                 campaign_meta.get("destination_type",""),
                 infer_ad_type(campaign_meta.get("objective",""), campaign_meta.get("optimization_goal",""), campaign_meta.get("destination_type","")))
            )
            created += 1
            action_str = "created"

        details.append({
            "ad_id": ad_id, "ad_name": ad_name,
            "kpi_field": kpi_field, "kpi_label": kpi_label,
            "source": source, "action": action_str,
            "objective": campaign_meta.get("objective", ""),
            "optimization_goal": campaign_meta.get("optimization_goal", ""),
            "destination_type": campaign_meta.get("destination_type", ""),
            "spend": spend
        })

    conn.commit()
    conn.close()

    # AI增强分析（可选，未配置则静默跳过）
    ai_suggestions = []
    if _is_ai_enabled():
        ai_suggestions = _ai_enhance_kpi(act_id, details)

    logger.info(f"KPI扫描完成 {act_id}: 新建{created}, 更新{updated}, 跳过{skipped}")
    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "total": len(ads),
        "details": details,
        "ai_suggestions": ai_suggestions
    }


def _ai_enhance_kpi(act_id: str, details: list) -> list:
    """
    AI增强KPI分析（v1.2.0改进）
    - 分析自动推断结果，对confidence=low的条目直接更新数据库
    - 特别关注被推断为辅助字段（post_engagement等）的广告
    """
    if not details or not _is_ai_enabled():
        return []

    try:
        from openai import OpenAI
        api_key = _get_setting("ai_api_key", "")
        api_base = _get_setting("ai_api_base", "https://api.deepseek.com/v1")
        model = _get_setting("ai_model", "deepseek-chat")

        if not api_key:
            return []

        client = OpenAI(api_key=api_key, base_url=api_base)

        auto_items = [d for d in details if d.get("action") in ("created", "updated")][:10]
        if not auto_items:
            return []

        prompt = (
            "你是Facebook广告KPI分析专家。以下是系统自动推断的广告KPI配置，"
            "请评估每条推断的合理性。\n"
            "特别注意：如果广告的 optimization_goal 或 destination_type 包含 CONVERSATIONS/MESSENGER/INSTAGRAM_DIRECT，"
            "则 kpi_field 应为 onsite_conversion.messaging_conversation_started_7d，而非 post_engagement。\n"
            "只返回JSON数组，每项包含: ad_id, confidence(high/medium/low), "
            "suggestion(如有更好建议则填写正确字段名，否则为null), reason(简短说明)。\n\n"
            f"推断结果：\n{json.dumps(auto_items, ensure_ascii=False, indent=2)}"
        )

        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1000
        )
        content = response.choices[0].message.content.strip()
        if "```" in content:
            content = content.split("```")[1].replace("json", "").strip()
        suggestions = json.loads(content)

        # AI 纠偏复审逻辑：
        # 1. 字段必须是已知有效 FB action_type（防幻觉）
        # 2. confidence 必须是 "high"（低置信度不覆盖）
        # 3. 所有 AI 建议都记录日志，便于审计
        conn = get_conn()
        for s in suggestions:
            suggestion = s.get("suggestion")
            ad_id = s.get("ad_id")
            confidence = s.get("confidence", "low")
            reason = s.get("reason", "")

            if not suggestion or not ad_id:
                continue

            # 复审检查 1：字段是否合法
            if not _is_valid_kpi_field(suggestion):
                logger.warning(f"AI纠偏复审拒绝(非法字段): {ad_id} -> {suggestion} (confidence={confidence}, reason={reason})")
                continue

            # 复审检查 2：置信度是否足够高
            if confidence != "high":
                logger.info(f"AI纠偏复审跳过(低置信度): {ad_id} -> {suggestion} (confidence={confidence}, reason={reason})")
                continue

            # 复审通过：只更新非手动配置的记录
            conn.execute(
                """UPDATE kpi_configs SET kpi_field=?, kpi_label=?, source='ai',
                   updated_at=datetime('now')
                   WHERE act_id=? AND target_id=? AND source!='manual'""",
                (suggestion, get_kpi_label(suggestion), act_id, ad_id)
            )
            logger.info(f"AI纠偏复审通过: {ad_id} -> {suggestion} (reason: {reason})")
        conn.commit()
        conn.close()

        return suggestions
    except Exception as e:
        logger.warning(f"AI KPI增强分析失败（非致命，不影响其他功能）: {e}")
        return []
