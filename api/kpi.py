"""
KPI 配置 API v1.2.0
支持: 广告级颗粒度、自动扫描预设、AI增强分析、批量操作
v1.2.0 新增:
  - POST /kpi/batch-cpa: 批量设置 CPA（按账户/按KPI字段类型/按ID列表）
  - POST /kpi/batch-kpi: 批量设置 KPI 字段（按账户/按层级）
  - GET  /kpi/summary: 获取 KPI 配置统计摘要（按账户/字段类型分组）
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List

from core.auth import get_current_user
from core.database import get_conn

router = APIRouter()

# KPI 字段选项（供前端下拉）
KPI_FIELD_OPTIONS = [
    # === 简化名称（推荐） ===
    {"value": "purchase",  "label": "网站购物", "category": "转化"},
    {"value": "lead",      "label": "线索",     "category": "转化"},
    {"value": "contact",   "label": "网站联系", "category": "转化"},
    # === 原始 FB action_type（兼容存量） ===
    {"value": "offsite_conversion.fb_pixel_purchase",              "label": "像素购买",     "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_add_to_cart",           "label": "加入购物车",   "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_initiate_checkout",     "label": "发起结账",     "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_complete_registration", "label": "注册完成",     "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_lead",                  "label": "像素潜在客户", "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_subscribe",             "label": "像素订阅",     "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_contact",               "label": "像素联系",     "category": "转化"},
    {"value": "onsite_conversion.lead_grouped",                    "label": "站内线索",     "category": "线索"},
    {"value": "onsite_conversion.messaging_conversation_started_7d","label": "私信对话(7日)", "category": "私信"},
    {"value": "onsite_conversion.messaging_first_reply",           "label": "私信首次回复",  "category": "私信"},
    {"value": "app_install",                                       "label": "应用安装",     "category": "App"},
    {"value": "link_click",                                        "label": "链接点击",     "category": "流量"},
    {"value": "landing_page_view",                                 "label": "落地页浏览",   "category": "流量"},
    {"value": "video_view",                                        "label": "视频观看",     "category": "互动"},
    {"value": "post_engagement",                                   "label": "帖子互动",     "category": "互动"},
    {"value": "page_engagement",                                   "label": "主页互动",     "category": "互动"},
    {"value": "reach",                                             "label": "触达人数",     "category": "曝光"},
    {"value": "impressions",                                       "label": "展示次数",     "category": "曝光"},
    {"value": "omni_purchase",                                     "label": "全渠道购买",   "category": "转化"},
]


class KpiConfigIn(BaseModel):
    act_id: str
    level: str = "ad"          # account / campaign / adset / ad
    target_id: str
    target_name: Optional[str] = None
    kpi_field: str
    kpi_label: Optional[str] = None
    target_cpa: Optional[float] = None
    enabled: int = 1
    note: Optional[str] = None


class KpiConfigUpdate(BaseModel):
    kpi_field: Optional[str] = None
    kpi_label: Optional[str] = None
    target_cpa: Optional[float] = None
    enabled: Optional[int] = None
    note: Optional[str] = None


class BatchCpaIn(BaseModel):
    """批量设置 CPA 请求体"""
    target_cpa: float                        # 目标 CPA 值（USD）
    act_ids: Optional[List[str]] = None      # 按账户过滤（空=全部）
    kpi_fields: Optional[List[str]] = None  # 按 KPI 字段类型过滤（空=全部）
    levels: Optional[List[str]] = None      # 按层级过滤（空=全部）
    ids: Optional[List[int]] = None         # 指定 config id 列表（优先级最高）
    overwrite_manual: bool = False           # 是否覆盖人工配置（默认不覆盖）


class BatchKpiIn(BaseModel):
    """批量设置 KPI 字段请求体"""
    kpi_field: str                           # 目标 KPI 字段
    kpi_label: Optional[str] = None
    act_ids: Optional[List[str]] = None      # 按账户过滤（空=全部）
    levels: Optional[List[str]] = None      # 按层级过滤（空=全部）
    ids: Optional[List[int]] = None         # 指定 config id 列表（优先级最高）
    overwrite_manual: bool = False           # 是否覆盖人工配置（默认不覆盖）


@router.get("/fields")
def get_kpi_fields():
    """获取 KPI 字段选项列表（DB驱动，硬编码兜底）"""
    try:
        conn = get_conn()
        rows = conn.execute(
            "SELECT value, label, category FROM kpi_field_options WHERE field_type='kpi_field' AND is_active=1 ORDER BY sort_order"
        ).fetchall()
        conn.close()
        if rows:
            return [{"value": r["value"], "label": r["label"], "category": r["category"] or ""} for r in rows]
    except Exception:
        pass
    return KPI_FIELD_OPTIONS


# ── 旧→新 kpi_field 名迁移映射 ──
_MIGRATION_MAP = {
    "offsite_conversion.fb_pixel_purchase": "purchase",
    "offsite_conversion.fb_pixel_lead": "lead",
    "onsite_conversion.lead_grouped": "lead",
    "offsite_conversion.fb_pixel_contact": "contact",
    "offsite_conversion.fb_pixel_view_content": "view_content",
    "offsite_conversion.fb_pixel_add_to_cart": "add_to_cart",
    "offsite_conversion.fb_pixel_initiate_checkout": "initiate_checkout",
    "offsite_conversion.fb_pixel_complete_registration": "complete_registration",
    "offsite_conversion.fb_pixel_subscribe": "subscribe",
    "offsite_conversion.purchase": "purchase",
    "offsite_conversion.lead": "lead",
    "offsite_conversion.lead_grouped": "lead",
}
_KPI_MIGRATION_LABELS = {
    "purchase": "购买", "lead": "线索", "contact": "联系",
    "view_content": "浏览内容", "add_to_cart": "加入购物车",
    "initiate_checkout": "发起结账", "complete_registration": "注册完成",
    "subscribe": "订阅",
}


@router.post("/migrate")
def migrate_kpi_fields(user=Depends(get_current_user)):
    """迁移 kpi_configs 中旧字段名为简化名称"""
    conn = get_conn()
    total = 0
    for old, new in _MIGRATION_MAP.items():
        label = _KPI_MIGRATION_LABELS.get(new, new)
        cur = conn.execute(
            "UPDATE kpi_configs SET kpi_field=?, kpi_label=? WHERE kpi_field=? AND source!='manual'",
            (new, label, old)
        )
        total += cur.rowcount
    conn.commit()
    conn.close()
    return {"success": True, "migrated": total, "message": f"已迁移 {total} 条 kpi_configs 记录"}


@router.get("/options")
def get_kpi_options(user=Depends(get_current_user)):
    """返回所有前端下拉框数据（来自 DB kpi_field_options）"""
    _FIELD_TYPE_MAP = {
        "objectives": "objective",
        "opt_goals": "opt_goal",
        "custom_events": "custom_event",
        "dest_types": "destination",
        "kpi_fields": "kpi_field",
        "ad_types": "ad_type",
    }
    result = {k: [] for k in _FIELD_TYPE_MAP}
    try:
        conn = get_conn()
        for key, db_type in _FIELD_TYPE_MAP.items():
            rows = conn.execute(
                "SELECT value, label, category FROM kpi_field_options WHERE field_type=? AND is_active=1 ORDER BY sort_order",
                (db_type,)
            ).fetchall()
            result[key] = [{"value": r["value"], "label": r["label"]} for r in rows]
        conn.close()
    except Exception:
        pass
    return result


# ── 转化目的（optimization_goal）按 objective 的分组映射 ──
# 与 autopilot_engine.py 的 VALID_GOALS 保持一致
_OPT_GOALS_BY_OBJECTIVE = {
    "OUTCOME_ENGAGEMENT": [
        {"value": "REACH", "label": "触达人数"},
        {"value": "LINK_CLICKS", "label": "链接点击"},
        {"value": "IMPRESSIONS", "label": "展示次数"},
        {"value": "CONVERSATIONS", "label": "聊天对话"},
        {"value": "LANDING_PAGE_VIEWS", "label": "落地页浏览"},
        {"value": "VIDEO_VIEWS", "label": "视频观看"},
        {"value": "THRUPLAY", "label": "ThruPlay"},
        {"value": "PAGE_LIKES", "label": "主页赞"},
        {"value": "MESSAGING_PURCHASE_CONVERSION", "label": "消息购买"},
        {"value": "MESSAGING_APPOINTMENT_CONVERSION", "label": "消息预约"},
    ],
    "OUTCOME_TRAFFIC": [
        {"value": "LINK_CLICKS", "label": "链接点击"},
        {"value": "LANDING_PAGE_VIEWS", "label": "落地页浏览"},
        {"value": "REACH", "label": "触达人数"},
        {"value": "IMPRESSIONS", "label": "展示次数"},
        {"value": "CONVERSATIONS", "label": "聊天对话"},
    ],
    "OUTCOME_AWARENESS": [
        {"value": "REACH", "label": "触达人数"},
        {"value": "IMPRESSIONS", "label": "展示次数"},
    ],
    "OUTCOME_MESSAGES": [
        {"value": "CONVERSATIONS", "label": "聊天对话"},
    ],
    "OUTCOME_LEADS": [
        {"value": "LEAD_GENERATION", "label": "即时表单"},
        {"value": "LINK_CLICKS", "label": "链接点击"},
        {"value": "LANDING_PAGE_VIEWS", "label": "落地页浏览"},
        {"value": "IMPRESSIONS", "label": "展示次数"},
        {"value": "OFFSITE_CONVERSIONS", "label": "网站转化线索"},
        {"value": "MESSAGING_PURCHASE_CONVERSION", "label": "消息线索"},
        {"value": "CONVERSATIONS", "label": "聊天对话"},
    ],
    "OUTCOME_SALES": [
        {"value": "LINK_CLICKS", "label": "链接点击"},
        {"value": "LANDING_PAGE_VIEWS", "label": "落地页浏览"},
        {"value": "REACH", "label": "触达人数"},
        {"value": "IMPRESSIONS", "label": "展示次数"},
        {"value": "CONVERSATIONS", "label": "聊天对话"},
        {"value": "OFFSITE_CONVERSIONS", "label": "网站转化"},
        {"value": "VALUE", "label": "转化价值(ROAS)"},
        {"value": "MESSAGING_PURCHASE_CONVERSION", "label": "消息购买"},
    ],
    "VIDEO_VIEWS": [
        {"value": "VIDEO_VIEWS", "label": "视频观看"},
        {"value": "THRUPLAY", "label": "ThruPlay"},
    ],
    "OUTCOME_VIDEO_VIEWS": [
        {"value": "VIDEO_VIEWS", "label": "视频观看"},
        {"value": "THRUPLAY", "label": "ThruPlay"},
    ],
    "OUTCOME_APP_PROMOTION": [
        {"value": "APP_INSTALLS", "label": "应用安装"},
    ],
}


@router.get("/opt-goals")
def get_opt_goals(objective: Optional[str] = None, user=Depends(get_current_user)):
    """
    返回转化目的（optimization_goal）选项。
    支持按 objective 过滤，返回 FB API UPPERCASE 值。
    """
    if objective:
        # DB kpi_field_options 兜底
        try:
            conn = get_conn()
            rows = conn.execute(
                "SELECT value, label FROM kpi_field_options WHERE field_type='opt_goal' AND is_active=1 ORDER BY sort_order",
            ).fetchall()
            conn.close()
            db_goals = {r["value"]: r["label"] for r in rows}
        except Exception:
            db_goals = {}

        result = _OPT_GOALS_BY_OBJECTIVE.get(objective, [])
        if not result and db_goals:
            # DB 兜底：返回所有 opt_goal
            result = [{"value": k, "label": v} for k, v in db_goals.items()]
        return result
    else:
        # 返回全部分组映射
        return _OPT_GOALS_BY_OBJECTIVE


@router.get("")
def list_kpi_configs(act_id: Optional[str] = None, level: Optional[str] = None,
                     user=Depends(get_current_user)):
    conn = get_conn()
    query = "SELECT * FROM kpi_configs WHERE 1=1"
    params = []
    if act_id:
        query += " AND act_id=?"
        params.append(act_id)
    if level:
        query += " AND level=?"
        params.append(level)
    query += " ORDER BY CASE level WHEN 'ad' THEN 1 WHEN 'adset' THEN 2 WHEN 'campaign' THEN 3 ELSE 4 END, created_at DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/summary")
def get_kpi_summary(act_id: Optional[str] = None, user=Depends(get_current_user)):
    """
    获取 KPI 配置统计摘要
    返回按账户和 KPI 字段类型分组的统计信息，用于批量操作的预览
    """
    conn = get_conn()
    params = []
    where = "WHERE 1=1"
    if act_id:
        where += " AND act_id=?"
        params.append(act_id)

    # 按账户分组统计
    by_account = conn.execute(
        f"SELECT act_id, COUNT(*) as count, "
        f"SUM(CASE WHEN target_cpa IS NOT NULL THEN 1 ELSE 0 END) as has_cpa, "
        f"SUM(CASE WHEN source='manual' THEN 1 ELSE 0 END) as manual_count "
        f"FROM kpi_configs {where} GROUP BY act_id",
        params
    ).fetchall()

    # 按 KPI 字段类型分组统计
    by_field = conn.execute(
        f"SELECT kpi_field, kpi_label, COUNT(*) as count, "
        f"SUM(CASE WHEN target_cpa IS NOT NULL THEN 1 ELSE 0 END) as has_cpa, "
        f"AVG(CASE WHEN target_cpa IS NOT NULL THEN target_cpa END) as avg_cpa "
        f"FROM kpi_configs {where} GROUP BY kpi_field ORDER BY count DESC",
        params
    ).fetchall()

    # 按层级分组统计
    by_level = conn.execute(
        f"SELECT level, COUNT(*) as count, "
        f"SUM(CASE WHEN target_cpa IS NOT NULL THEN 1 ELSE 0 END) as has_cpa "
        f"FROM kpi_configs {where} GROUP BY level",
        params
    ).fetchall()

    conn.close()
    return {
        "by_account": [dict(r) for r in by_account],
        "by_field": [dict(r) for r in by_field],
        "by_level": [dict(r) for r in by_level],
    }


@router.post("")
def add_kpi_config(body: KpiConfigIn, user=Depends(get_current_user)):
    label = body.kpi_label
    if not label:
        for opt in KPI_FIELD_OPTIONS:
            if opt["value"] == body.kpi_field:
                label = opt["label"]
                break
        label = label or body.kpi_field

    conn = get_conn()
    cur = conn.execute(
        """INSERT OR REPLACE INTO kpi_configs
           (act_id, level, target_id, target_name, kpi_field, kpi_label,
            target_cpa, source, enabled, note)
           VALUES (?,?,?,?,?,?,?,'manual',?,?)""",
        (body.act_id, body.level, body.target_id, body.target_name,
         body.kpi_field, label, body.target_cpa, body.enabled, body.note)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id, "message": "KPI 配置保存成功"}


@router.put("/{config_id}")
def update_kpi_config(config_id: int, body: KpiConfigUpdate, user=Depends(get_current_user)):
    conn = get_conn()
    updates = []
    params = []
    if body.kpi_field is not None:
        updates.append("kpi_field=?")
        params.append(body.kpi_field)
        # 自动更新label
        for opt in KPI_FIELD_OPTIONS:
            if opt["value"] == body.kpi_field:
                updates.append("kpi_label=?")
                params.append(opt["label"])
                break
    if body.kpi_label is not None:
        # 如果明确传了label则覆盖
        if "kpi_label=?" not in updates:
            updates.append("kpi_label=?")
            params.append(body.kpi_label)
    if body.target_cpa is not None:
        updates.append("target_cpa=?")
        params.append(body.target_cpa)
    if body.enabled is not None:
        updates.append("enabled=?")
        params.append(body.enabled)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if not updates:
        conn.close()
        raise HTTPException(400, "没有需要更新的字段")
    # 人工修改后标记为manual
    updates.append("source='manual'")
    updates.append("updated_at=datetime('now')")
    params.append(config_id)
    conn.execute(f"UPDATE kpi_configs SET {', '.join(updates)} WHERE id=?", params)
    conn.commit()
    conn.close()
    return {"success": True}


@router.delete("/{config_id}")
def delete_kpi_config(config_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute("DELETE FROM kpi_configs WHERE id=?", (config_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/batch-cpa")
def batch_set_cpa(body: BatchCpaIn, user=Depends(get_current_user)):
    """
    批量设置目标 CPA
    支持按账户、KPI字段类型、层级、或指定ID列表进行批量操作
    """
    conn = get_conn()

    if body.ids:
        # 指定 ID 列表（最高优先级）
        placeholders = ",".join("?" * len(body.ids))
        where = f"id IN ({placeholders})"
        params = list(body.ids)
        if not body.overwrite_manual:
            where += " AND source != 'manual'"
    else:
        # 按条件过滤
        conditions = ["1=1"]
        params = []
        if body.act_ids:
            placeholders = ",".join("?" * len(body.act_ids))
            conditions.append(f"act_id IN ({placeholders})")
            params.extend(body.act_ids)
        if body.kpi_fields:
            placeholders = ",".join("?" * len(body.kpi_fields))
            conditions.append(f"kpi_field IN ({placeholders})")
            params.extend(body.kpi_fields)
        if body.levels:
            placeholders = ",".join("?" * len(body.levels))
            conditions.append(f"level IN ({placeholders})")
            params.extend(body.levels)
        if not body.overwrite_manual:
            conditions.append("source != 'manual'")
        where = " AND ".join(conditions)

    # 先查询受影响的记录数
    count_row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM kpi_configs WHERE {where}", params
    ).fetchone()
    affected = count_row["cnt"] if count_row else 0

    if affected == 0:
        conn.close()
        return {"success": True, "updated": 0, "message": "没有符合条件的记录"}

    # 执行批量更新
    conn.execute(
        f"UPDATE kpi_configs SET target_cpa=?, updated_at=datetime('now') WHERE {where}",
        [body.target_cpa] + params
    )
    conn.commit()
    conn.close()

    return {
        "success": True,
        "updated": affected,
        "message": f"已批量更新 {affected} 条记录的目标 CPA 为 ${body.target_cpa:.2f}"
    }


@router.post("/batch-kpi")
def batch_set_kpi(body: BatchKpiIn, user=Depends(get_current_user)):
    """
    批量设置 KPI 字段
    支持按账户、层级、或指定ID列表进行批量操作
    """
    # 自动获取 label
    label = body.kpi_label
    if not label:
        for opt in KPI_FIELD_OPTIONS:
            if opt["value"] == body.kpi_field:
                label = opt["label"]
                break
        label = label or body.kpi_field

    conn = get_conn()

    if body.ids:
        placeholders = ",".join("?" * len(body.ids))
        where = f"id IN ({placeholders})"
        params = list(body.ids)
        if not body.overwrite_manual:
            where += " AND source != 'manual'"
    else:
        conditions = ["1=1"]
        params = []
        if body.act_ids:
            placeholders = ",".join("?" * len(body.act_ids))
            conditions.append(f"act_id IN ({placeholders})")
            params.extend(body.act_ids)
        if body.levels:
            placeholders = ",".join("?" * len(body.levels))
            conditions.append(f"level IN ({placeholders})")
            params.extend(body.levels)
        if not body.overwrite_manual:
            conditions.append("source != 'manual'")
        where = " AND ".join(conditions)

    count_row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM kpi_configs WHERE {where}", params
    ).fetchone()
    affected = count_row["cnt"] if count_row else 0

    if affected == 0:
        conn.close()
        return {"success": True, "updated": 0, "message": "没有符合条件的记录"}

    conn.execute(
        f"UPDATE kpi_configs SET kpi_field=?, kpi_label=?, source='manual', "
        f"updated_at=datetime('now') WHERE {where}",
        [body.kpi_field, label] + params
    )
    conn.commit()
    conn.close()

    return {
        "success": True,
        "updated": affected,
        "message": f"已批量更新 {affected} 条记录的 KPI 字段为 {label}"
    }



@router.post("/ai-analyze")
def ai_analyze_kpi(background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    """
    对所有 rule/default 来源的 KPI 配置重新进行 AI 分析和纠偏（后台异步执行）。
    AI 会评估每条推断的合理性，对置信度低的记录直接更新为 AI 建议的字段。
    """
    # 快速校验 AI 配置
    conn = get_conn()
    ai_enabled = conn.execute("SELECT value FROM settings WHERE key='ai_enabled'").fetchone()
    ai_api_key = conn.execute("SELECT value FROM settings WHERE key='ai_api_key'").fetchone()
    conn.close()
    if not ai_enabled or ai_enabled["value"] != "1":
        raise HTTPException(400, "AI 功能未启用，请在系统设置中启用 AI")
    if not ai_api_key or not ai_api_key["value"]:
        raise HTTPException(400, "AI API Key 未配置，请在系统设置中配置")
    # 后台异步执行（避免 nginx 60 秒超时）
    background_tasks.add_task(_run_ai_enhance, "all")
    return {"success": True, "message": "AI 分析已在后台启动，请稍后（约 1-2 分钟）刷新 KPI 列表查看结果"}

@router.post("/auto-scan/{act_id}")
def auto_scan_kpi(act_id: str, background_tasks: BackgroundTasks,
                  user=Depends(get_current_user)):
    """
    触发广告级KPI自动扫描预设
    - 不覆盖人工配置
    - AI已启用时同步进行AI增强分析
    - 后台执行，立即返回任务ID
    """
    conn = get_conn()
    row = conn.execute(
        "SELECT a.act_id, t.access_token_enc FROM accounts a "
        "LEFT JOIN fb_tokens t ON t.id=a.token_id "
        "WHERE a.act_id=? AND a.enabled=1 LIMIT 1",
        (act_id,)
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "账户不存在或未启用")
    if not row["access_token_enc"]:
        raise HTTPException(400, "账户未关联Token，请先配置Token")

    from core.database import decrypt_token
    token = decrypt_token(row["access_token_enc"])

    # 后台执行扫描
    background_tasks.add_task(_run_scan, act_id, token)
    return {"success": True, "message": "KPI自动扫描已启动，请稍后刷新查看结果"}


def _run_scan(act_id: str, token: str):
    """后台执行KPI扫描"""
    from services.kpi_resolver import scan_and_preset_kpi
    try:
        result = scan_and_preset_kpi(act_id, token)
        # 写入扫描结果到系统日志
        conn = get_conn()
        import json
        conn.execute(
            """INSERT INTO action_logs
                       (act_id, level, target_id, target_name, action_type, trigger_type, trigger_detail, operator,
                        created_at)
                       VALUES (?,?,?,?,?,?,?,?,datetime('now','+8 hours'))""",
            (act_id, "account", "__kpi_scan__", "KPI自动扫描",
             "kpi_scan", "system",
             f"扫描完成: 新建{result.get('created',0)}条, 更新{result.get('updated',0)}条, 跳过{result.get('skipped',0)}条",
             "system")
        )
        conn.commit()
        conn.close()
        # 扫描完成后自动触发 AI 增强分析（后台异步，不阻塞）
        try:
            _run_ai_enhance(act_id)
        except Exception as ai_err:
            import logging
            logging.getLogger("mira.kpi").warning(f"AI增强分析跳过 {act_id}: {ai_err}")
    except Exception as e:
        import logging
        logging.getLogger("mira.kpi").error(f"KPI扫描失败 {act_id}: {e}")


def _run_ai_enhance(act_id: str):
    """
    对指定账户（或所有账户）的 rule/default 来源 KPI 配置进行 AI 增强分析。
    - suggestion 不为 null：纠偏为 AI 建议字段，source='ai'
    - suggestion 为 null（AI 确认当前字段正确）：也将 source 更新为 'ai'
    """
    import json, re
    conn = get_conn()
    # 检查 AI 配置
    ai_enabled = conn.execute("SELECT value FROM settings WHERE key='ai_enabled'").fetchone()
    ai_api_key = conn.execute("SELECT value FROM settings WHERE key='ai_api_key'").fetchone()
    ai_api_base = conn.execute("SELECT value FROM settings WHERE key='ai_api_base'").fetchone()
    ai_model = conn.execute("SELECT value FROM settings WHERE key='ai_model'").fetchone()
    conn.close()

    if not ai_enabled or ai_enabled["value"] != "1":
        return  # AI 未启用，静默跳过
    if not ai_api_key or not ai_api_key["value"]:
        return  # API Key 未配置，静默跳过

    api_key  = ai_api_key["value"]
    api_base = (ai_api_base["value"] if ai_api_base and ai_api_base["value"] else "https://api.deepseek.com/v1")
    model    = (ai_model["value"] if ai_model and ai_model["value"] else "deepseek-chat")

    conn = get_conn()
    if act_id == "all":
        rows = conn.execute(
            """SELECT k.id, k.act_id, k.level, k.target_id, k.target_name,
                      k.kpi_field, k.kpi_label, k.source,
                      k.objective, k.optimization_goal, k.destination_type,
                      a.name as account_name
               FROM kpi_configs k
               LEFT JOIN accounts a ON a.act_id = k.act_id
               WHERE k.source IN ('rule', 'default', 'history')
               ORDER BY k.id"""
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT k.id, k.act_id, k.level, k.target_id, k.target_name,
                      k.kpi_field, k.kpi_label, k.source,
                      k.objective, k.optimization_goal, k.destination_type,
                      a.name as account_name
               FROM kpi_configs k
               LEFT JOIN accounts a ON a.act_id = k.act_id
               WHERE k.source IN ('rule', 'default', 'history') AND k.act_id=?
               ORDER BY k.id""",
            (act_id,)
        ).fetchall()
    conn.close()

    if not rows:
        return  # 没有需要分析的记录

    from openai import OpenAI
    client = OpenAI(api_key=api_key, base_url=api_base)
    BATCH_SIZE = 15
    FIELD_RE = re.compile(r'^[a-z][a-z0-9_.]{2,80}$')
    AUXILIARY_FIELDS = {"post_engagement", "page_engagement", "video_view", "reach", "impressions", "link_click"}
    batches = [rows[i:i+BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    all_suggestions = []

    for batch in batches:
        items_for_ai = []
        for r in batch:
            items_for_ai.append({
                "id": r["id"],
                "ad_id": r["target_id"],
                "ad_name": r["target_name"] or r["target_id"],
                "level": r["level"],
                "account": r["account_name"] or r["act_id"],
                "campaign_objective": r["objective"] or "",
                "adset_optimization_goal": r["optimization_goal"] or "",
                "adset_destination_type": r["destination_type"] or "",
                "current_kpi_field": r["kpi_field"],
                "current_source": r["source"]
            })
        prompt = (
            "你是 Facebook 广告 KPI 分析专家。以下是系统自动推断的广告 KPI 配置，"
            "请评估每条推断的合理性并给出建议。\n\n"
            "常见规则：\n"
            "每条记录包含 campaign_objective（活动目标）、adset_optimization_goal（优化目标）、"
            "adset_destination_type（投放目的地），这些是判断广告类型最重要的依据。\n\n"
            "关键判断规则：\n"
            "- optimization_goal=CONVERSATIONS 或 destination_type=MESSENGER/INSTAGRAM_DIRECT → onsite_conversion.messaging_conversation_started_7d\n"
            "- objective=OUTCOME_SALES 或 optimization_goal=OFFSITE_CONVERSIONS → purchase（或像素购买事件）\n"
            "- objective=OUTCOME_LEADS 或 optimization_goal=LEAD_GENERATION → lead\n"
            "- objective=OUTCOME_ENGAGEMENT 且无私信信号 → post_engagement\n"
            "- 广告名称含 Messenger/私信/对话/DM → 私信对话（优先级低于上述结构字段）\n"
            "- 广告名称含 购买、Purchase、Buy → kpi_field 应为 purchase\n"
            "- 广告名称含 注册、Register、Lead → kpi_field 应为 lead\n"
            "- 广告名称含 加购、AddToCart → kpi_field 应为 add_to_cart\n"
            "- 如果当前字段是 post_engagement 但广告明显是私信类型，应纠偏为 messaging_conversation_started_7d\n\n"
            "返回 JSON 数组，每项包含：\n"
            "  id（原始记录ID），confidence（high/medium/low），"
            "  suggestion（如有更好建议则填写正确字段名，否则为 null），reason（简短说明，中文）\n\n"
            f"待分析记录：\n{json.dumps(items_for_ai, ensure_ascii=False, indent=2)}"
        )
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=2000
            )
            content_str = response.choices[0].message.content.strip()
            if "```" in content_str:
                content_str = content_str.split("```")[1].replace("json", "").strip()
            batch_suggestions = json.loads(content_str)
            all_suggestions.extend(batch_suggestions)
        except Exception as e:
            import logging
            logging.getLogger("mira.kpi").error(f"AI增强分析批次失败: {e}")
            continue  # 跳过失败批次，继续处理其他批次

    if not all_suggestions:
        return

    # 将 AI 建议写入数据库
    conn = get_conn()
    try:
        for s in all_suggestions:
            suggestion = s.get("suggestion")
            record_id  = s.get("id")
            reason     = s.get("reason", "AI已确认")
            if not record_id:
                continue
            try:
                record_id = int(record_id)
            except (TypeError, ValueError):
                continue
            if suggestion and FIELD_RE.match(suggestion) and suggestion not in AUXILIARY_FIELDS:
                # AI 建议纠偏为新字段
                conn.execute(
                    """UPDATE kpi_configs
                       SET kpi_field=?, source='ai', updated_at=datetime('now','+8 hours'), note=?
                       WHERE id=? AND source NOT IN ('manual')""",
                    (suggestion, f"AI纠偏: {reason}", record_id)
                )
            else:
                # AI 确认当前字段正确，仅更新 source 为 'ai'
                conn.execute(
                    """UPDATE kpi_configs
                       SET source='ai', updated_at=datetime('now','+8 hours'), note=?
                       WHERE id=? AND source NOT IN ('manual')""",
                    (f"AI确认: {reason}", record_id)
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger("mira.kpi").error(f"AI增强分析写库失败: {e}")
    finally:
        conn.close()


@router.get("/auto-scan/{act_id}/result")
def get_scan_result(act_id: str, user=Depends(get_current_user)):
    """获取最近一次KPI扫描结果"""
    import json
    conn = get_conn()
    row = conn.execute(
        "SELECT trigger_detail as result, created_at FROM action_logs "
        "WHERE act_id=? AND target_id='__kpi_scan__' "
        "ORDER BY created_at DESC LIMIT 1",
        (act_id,)
    ).fetchone()
    conn.close()
    if not row:
        return {"scanned": False}
    return {"scanned": True, "scanned_at": row["created_at"], "result": row["result"]}


# ── 路由别名 ──
@router.get("/list")
def list_kpi_alias(act_id: str = None, user=Depends(get_current_user)):
    return list_kpi_configs(act_id=act_id, user=user)


# ─── KPI 目标 ID 联动下拉（从 FB API 实时拉取）─────────────────
@router.get("/targets")
def get_kpi_targets(act_id: str, level: str, user=Depends(get_current_user)):
    """根据账户和层级，从 FB API 实时拉取可选目标列表"""
    import requests as req
    from core.database import decrypt_token
    conn = get_conn()
    acc = conn.execute('SELECT * FROM accounts WHERE act_id=?', (act_id,)).fetchone()
    conn.close()
    if not acc:
        return []
    acc = dict(acc)
    # 获取 Token（多级 fallback）
    token = None
    if acc.get('token_id'):
        conn2 = get_conn()
        tk = conn2.execute('SELECT access_token_enc, status FROM fb_tokens WHERE id=?', (acc['token_id'],)).fetchone()
        conn2.close()
        if tk and tk['status'] == 'active':
            token = decrypt_token(tk['access_token_enc'])
    if not token:
        token = acc.get('access_token', '') or None
    if not token:
        conn3 = get_conn()
        any_tk = conn3.execute("SELECT access_token_enc FROM fb_tokens WHERE status='active' LIMIT 1").fetchone()
        conn3.close()
        if any_tk:
            token = decrypt_token(any_tk['access_token_enc'])
    if not token:
        return []

    def _fb_paginate(url: str, params: dict, max_total: int = 2000) -> list:
        all_items = []
        next_params = dict(params)
        while len(all_items) < max_total:
            r = req.get(url, params=next_params, timeout=15)
            d = r.json()
            if 'error' in d:
                break
            items = d.get('data', [])
            all_items.extend(items)
            after = d.get('paging', {}).get('cursors', {}).get('after')
            if after and len(items) == next_params.get('limit', 200):
                next_params = {k: v for k, v in next_params.items() if k != 'after'}
                next_params['after'] = after
            else:
                break
        return all_items


    try:
        if level == 'account':
            return [{'id': act_id, 'name': acc.get('name', act_id), 'status': 'ACTIVE'}]

        elif level == 'campaign':
            items = _fb_paginate(
                f'https://graph.facebook.com/v25.0/{act_id}/campaigns',
                {'access_token': token, 'fields': 'id,name,status,effective_status', 'limit': 200}
            )
            return [{'id': c['id'], 'name': c.get('name',''), 'status': c.get('effective_status','')} for c in items]

        elif level == 'adset':
            items = _fb_paginate(
                f'https://graph.facebook.com/v25.0/{act_id}/adsets',
                {'access_token': token, 'fields': 'id,name,status,effective_status,campaign_id', 'limit': 200}
            )
            return [{'id': s['id'], 'name': s.get('name',''), 'status': s.get('effective_status',''), 'campaign_id': s.get('campaign_id','')} for s in items]

        elif level == 'ad':
            items = _fb_paginate(
                f'https://graph.facebook.com/v25.0/{act_id}/ads',
                {'access_token': token, 'fields': 'id,name,status,effective_status,adset_id,campaign_id', 'limit': 200}
            )
            return [{'id': a['id'], 'name': a.get('name',''), 'status': a.get('effective_status',''), 'adset_id': a.get('adset_id',''), 'campaign_id': a.get('campaign_id','')} for a in items]
    except Exception as e:
        return []
    return []

# ============================================================
# KPI 审计 & 映射管理 API (v2)
# ============================================================

@router.get("/mapping")
def get_kpi_mapping(user=Depends(get_current_user)):
    """获取全量 kpi_alias_map（按 KPI 类型分组）"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT kpi_type, fb_action_type, label, sort_order, is_standard "
        "FROM kpi_alias_map ORDER BY kpi_type, sort_order"
    ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        kt = r["kpi_type"]
        if kt not in result:
            result[kt] = {"standard": [], "fallback": []}
        entry = {"action_type": r["fb_action_type"], "sort_order": r["sort_order"]}
        if r["label"]:
            entry["label"] = r["label"]
        key = "fallback" if r["is_standard"] == 0 else "standard"
        result[kt][key].append(entry)
    return result


class MappingUpdateIn(BaseModel):
    mapping_id: int
    kpi_type: Optional[str] = None
    is_standard: Optional[int] = None


class MappingAddIn(BaseModel):
    kpi_type: str
    fb_action_type: str
    label: Optional[str] = None
    is_standard: int = 1
    sort_order: int = 99


@router.post("/mapping/add")
def add_kpi_mapping(body: MappingAddIn, user=Depends(get_current_user)):
    """用户添加 action_type → KPI 类型映射"""
    conn = get_conn()
    existing = conn.execute(
        "SELECT id FROM kpi_alias_map WHERE kpi_type=? AND fb_action_type=?",
        (body.kpi_type, body.fb_action_type)
    ).fetchone()
    if existing:
        conn.close()
        return {"success": False, "message": f"映射已存在: {body.kpi_type} → {body.fb_action_type}"}

    conn.execute(
        "INSERT INTO kpi_alias_map (kpi_type, fb_action_type, label, is_standard, sort_order) VALUES (?,?,?,?,?)",
        (body.kpi_type, body.fb_action_type, body.label, body.is_standard, body.sort_order)
    )
    conn.commit()
    conn.close()

    # 清除 guard_engine 缓存
    _clear_guard_cache()

    return {"success": True, "message": f"{body.fb_action_type} 已加入 {body.kpi_type} 别名列表"}


@router.post("/mapping/remove")
def remove_kpi_mapping(body: MappingAddIn, user=Depends(get_current_user)):
    """用户移除 action_type → KPI 类型映射"""
    conn = get_conn()
    conn.execute(
        "DELETE FROM kpi_alias_map WHERE kpi_type=? AND fb_action_type=?",
        (body.kpi_type, body.fb_action_type)
    )
    conn.commit()
    conn.close()

    _clear_guard_cache()

    return {"success": True, "message": f"{body.fb_action_type} 已从 {body.kpi_type} 别名列表移除"}




@router.post("/mapping/update")
def update_kpi_mapping(body: MappingUpdateIn, user=Depends(get_current_user)):
    conn = get_conn()
    updates = []
    params = []
    if body.kpi_type is not None:
        updates.append("kpi_type=?")
        params.append(body.kpi_type)
    if body.is_standard is not None:
        updates.append("is_standard=?")
        params.append(body.is_standard)
    if not updates:
        conn.close()
        return {"success": False, "message": "no fields to update"}
    params.append(body.mapping_id)
    sql = "UPDATE kpi_alias_map SET " + ", ".join(updates) + " WHERE id=?"
    conn.execute(sql, params)
    conn.commit()
    conn.close()
    _clear_guard_cache()
    return {"success": True, "message": "mapping updated"}

@router.get("/unknown-types")
def get_unknown_types(user=Depends(get_current_user)):
    """返回系统自动发现的未知 action_types"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM kpi_unknown_types ORDER BY seen_count DESC, last_seen DESC"
    ).fetchall()
    conn.close()
    return {"unknown_types": [dict(r) for r in rows]}


class UnknownTypeMapIn(BaseModel):
    action_type: str
    kpi_type: str
    is_standard: int = 0
    sort_order: int = 99


@router.post("/unknown-types/map")
def map_unknown_type(body: UnknownTypeMapIn, user=Depends(get_current_user)):
    """用户确认未知 action_type 归属的 KPI 类型"""
    conn = get_conn()

    # 写入 kpi_alias_map
    conn.execute(
        "INSERT OR IGNORE INTO kpi_alias_map (kpi_type, fb_action_type, is_standard, sort_order) VALUES (?,?,?,?)",
        (body.kpi_type, body.action_type, body.is_standard, body.sort_order)
    )

    # 标记为已审核
    conn.execute(
        "UPDATE kpi_unknown_types SET reviewed=1, mapped_to_kpi_type=? WHERE action_type=?",
        (body.kpi_type, body.action_type)
    )
    conn.commit()
    conn.close()

    _clear_guard_cache()

    return {"success": True, "message": f"{body.action_type} 已映射到 {body.kpi_type}"}


def _clear_guard_cache():
    """清除 guard_engine 的 KPI 别名缓存，使下次查询生效"""
    import services.guard_engine as ge
    ge._KPI_ALIAS_MAP_DB = {}
    ge._KPI_ALIAS_MAP_DB_TIME = 0


@router.get("/trace/{ad_id}")
def get_kpi_trace(ad_id: str, user=Depends(get_current_user)):
    """返回单个广告的 KPI 决策链"""
    conn = get_conn()

    # 1. 广告 KPI 配置
    kpi_config = conn.execute(
        """SELECT k.*, a.name as account_name FROM kpi_configs k
           LEFT JOIN accounts a ON a.act_id=k.act_id
           WHERE k.target_id=? AND k.enabled=1
           ORDER BY k.level DESC LIMIT 1""",
        (ad_id,)
    ).fetchone()

    # 2. 最新快照
    snapshot = conn.execute(
        "SELECT * FROM perf_snapshots WHERE ad_id=? ORDER BY snapshot_at DESC LIMIT 1",
        (ad_id,)
    ).fetchone()

    # 3. 操作记录
    actions_log = conn.execute(
        """SELECT * FROM action_logs WHERE target_id=?
           ORDER BY created_at DESC LIMIT 20""",
        (ad_id,)
    ).fetchall()

    conn.close()

    result = {"ad_id": ad_id, "kpi_config": dict(kpi_config) if kpi_config else None}
    if snapshot:
        result["snapshot"] = dict(snapshot)
    if actions_log:
        result["recent_actions"] = [dict(a) for a in actions_log]

    # 4. 映射信息
    if kpi_config and kpi_config["kpi_field"]:
        kpi_type = kpi_config["kpi_field"]
        conn2 = get_conn()
        aliases = conn2.execute(
            "SELECT fb_action_type, is_standard, sort_order, label FROM kpi_alias_map WHERE kpi_type=? ORDER BY sort_order",
            (kpi_type,)
        ).fetchall()
        conn2.close()
        result["aliases"] = [dict(a) for a in aliases]

    return result

# ── AI 自动映射未知 action_type ──

def _ai_analyze_unknown_type(action_type: str, sample_ads: list) -> dict:
    """调用 AI 分析未知 action_type 应归属的 KPI 类型"""
    import json
    conn = get_conn()
    ai_enabled = conn.execute("SELECT value FROM settings WHERE key='ai_enabled'").fetchone()
    ai_api_key = conn.execute("SELECT value FROM settings WHERE key='ai_api_key'").fetchone()
    ai_api_base = conn.execute("SELECT value FROM settings WHERE key='ai_api_base'").fetchone()
    ai_model = conn.execute("SELECT value FROM settings WHERE key='ai_model'").fetchone()
    conn.close()

    if not ai_enabled or ai_enabled["value"] != "1" or not ai_api_key or not ai_api_key["value"]:
        return {"suggested_kpi_type": None, "confidence": None, "reason": "AI 未启用"}

    api_key = ai_api_key["value"]
    api_base = (ai_api_base["value"] if ai_api_base and ai_api_base["value"] else "https://api.deepseek.com/v1")
    model = (ai_model["value"] if ai_model and ai_model["value"] else "deepseek-chat")

    prompt = (
        "你是一个 Facebook 广告 KPI 映射专家。分析以下 FB action_type 应归属哪个 KPI 类型。\n\n"
        "已知 KPI 类型:\n"
        "- purchase: 购物/购买类转化 (标准事件: purchase, offsite_conversion.fb_pixel_purchase)\n"
        "- lead: 线索/潜在客户类转化 (标准事件: lead, offsite_conversion.fb_pixel_lead, onsite_conversion.lead_grouped)\n"
        "- contact: 联系类转化 (标准事件: contact, offsite_conversion.fb_pixel_contact)\n\n"
        "分析规则:\n"
        "1. 包含 purchase/buy/checkout/order/add_to_cart → purchase\n"
        "2. 包含 lead/signup/register/subscribe/form → lead\n"
        "3. 包含 contact/phone/email/message/call → contact\n"
        "4. 包含 custom/自定义 → 查看 sample_ads 的上下文推测\n"
        "5. 包含 view/like/share/engagement → 非主要转化事件，但可归类为 contact\n"
        "6. 无法判断 → 返回 null\n\n"
        f"待分析 action_type: {action_type}\n"
        f"示例广告 ID: {json.dumps(sample_ads[:5])}\n\n"
        "请返回 JSON (只返回 JSON，不要其他文字):\n"
        '{"suggested_kpi_type": "purchase"|"lead"|"contact"|null, '
        '"confidence": "high"|"medium"|"low", '
        '"reason": "简短原因"}'
    )

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=api_base)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=200
        )
        text = resp.choices[0].message.content.strip()
        if "```" in text:
            text = text.split("```")[1].replace("json", "").strip()
        result = json.loads(text)
        return {
            "suggested_kpi_type": result.get("suggested_kpi_type"),
            "confidence": result.get("confidence"),
            "reason": result.get("reason", ""),
        }
    except Exception as e:
        import logging
        logging.getLogger("mira.kpi").error(f"AI 分析失败 {action_type}: {e}")
        return {"suggested_kpi_type": None, "confidence": None, "reason": f"AI 调用失败: {e}"}


@router.post("/unknown-types/ai-auto-map")
def ai_auto_map_unknown_types(user=Depends(get_current_user)):
    """AI 自动分析所有未审核的未知 action_type，高置信度自动映射"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM kpi_unknown_types WHERE reviewed=0 ORDER BY seen_count DESC"
    ).fetchall()
    conn.close()

    if not rows:
        return {"success": True, "auto_mapped": 0, "message": "没有待分析的未知类型"}

    results = []
    for r in rows:
        sample_ads = []
        if r["sample_ads"]:
            import json as _j
            try:
                sample_ads = _j.loads(r["sample_ads"])
            except:
                sample_ads = []

        ai_result = _ai_analyze_unknown_type(r["action_type"], sample_ads)

        if ai_result["suggested_kpi_type"] and ai_result["confidence"] == "high":
            # 高置信度 → 自动加入 kpi_alias_map
            kt = ai_result["suggested_kpi_type"]
            conn2 = get_conn()
            existing = conn2.execute(
                "SELECT id FROM kpi_alias_map WHERE kpi_type=? AND fb_action_type=?",
                (kt, r["action_type"])
            ).fetchone()
            if not existing:
                conn2.execute(
                    "INSERT INTO kpi_alias_map (kpi_type, fb_action_type, is_standard, sort_order) VALUES (?,?,0,99)",
                    (kt, r["action_type"])
                )
            # 标记已审核
            conn2.execute(
                "UPDATE kpi_unknown_types SET reviewed=1, mapped_to_kpi_type=? WHERE action_type=?",
                (kt, r["action_type"])
            )
            conn2.commit()
            conn2.close()

            results.append({
                "action_type": r["action_type"],
                "auto_mapped": True,
                "kpi_type": kt,
                "confidence": "high",
                "reason": ai_result["reason"],
            })
        else:
            results.append({
                "action_type": r["action_type"],
                "auto_mapped": False,
                "kpi_type": ai_result["suggested_kpi_type"],
                "confidence": ai_result["confidence"],
                "reason": ai_result["reason"],
            })

    # 清除 guard cache（循环外只清一次）
    _clear_guard_cache()

    auto_count = sum(1 for x in results if x.get("auto_mapped"))
    return {
        "success": True,
        "auto_mapped": auto_count,
        "total": len(results),
        "results": results,
        "message": f"AI 分析完成: {auto_count} 个已自动映射, {len(results) - auto_count} 个待人工审核"
    }


@router.get("/diagnose/{act_id}/{ad_id}")
def diagnose_ad(act_id: str, ad_id: str, user=Depends(get_current_user)):
    """诊断广告：查看FB原始数据、KPI解析、别名匹配、成效归因、规则匹配"""
    import json
    from core.database import decrypt_token
    import requests as req

    from services.token_manager import get_exec_token
    token = get_exec_token(act_id, 'READ')
    if not token:
        raise HTTPException(400, "无法获取Token，请检查账户Token配置")
    conn = get_conn()
    row = conn.execute("SELECT currency FROM accounts WHERE act_id=? AND enabled=1", (act_id,)).fetchone()
    currency = (row["currency"] or "USD").upper().strip() if row else "USD"
    conn.close()

    # ── 1) 从FB API拉取广告数据 ──
    FB_API = "https://graph.facebook.com/v25.0"
    fields = ("id,name,status,effective_status,adset_id,campaign_id,"
              "campaign{objective},"
              "adset{optimization_goal,destination_type},"
              "insights.date_preset(today){spend,impressions,clicks,actions,action_values,cpc,cpm}")
    try:
        resp = req.get(f"{FB_API}/{ad_id}", params={"access_token": token, "fields": fields}, timeout=30)
        fb_data = resp.json()
    except Exception as e:
        raise HTTPException(502, f"FB API请求失败: {e}")
    if "error" in fb_data:
        raise HTTPException(502, f"FB API错误: {fb_data['error'].get('message', fb_data['error'])}")

    ad_name = fb_data.get("name", ad_id)
    eff_status = fb_data.get("effective_status", "")
    campaign_id = fb_data.get("campaign_id", "")
    adset_id = fb_data.get("adset_id", "")

    insights = fb_data.get("insights", {}).get("data", [])
    ins = insights[0] if insights else {}
    spend_raw = float(ins.get("spend", 0))
    impressions = int(ins.get("impressions", 0))
    clicks = int(ins.get("clicks", 0))
    actions_raw = ins.get("actions", [])
    action_values = ins.get("action_values", [])

    # USD化spend
    from services.guard_engine import _to_usd_guard
    spend = _to_usd_guard(spend_raw, currency)

    # ── 2) Campaign/Adset元数据 ──
    camp_data = fb_data.get("campaign", {})
    camp_obj = camp_data.get("objective", "") if isinstance(camp_data, dict) else ""
    adset_data = fb_data.get("adset", {})
    adset_opt_goal = ""
    adset_dest_type = ""
    adset_custom_event = ""
    if isinstance(adset_data, dict):
        adset_opt_goal = adset_data.get("optimization_goal", "")
        adset_dest_type = adset_data.get("destination_type", "")
        adset_custom_event = adset_data.get("custom_event_type", "")
    campaign_meta = {
        "objective": camp_obj,
        "optimization_goal": adset_opt_goal,
        "destination_type": adset_dest_type,
        "custom_event_type": adset_custom_event,
        "spend": spend,
    }

    # ── 3) KPI解析 ──
    from services.kpi_resolver import get_kpi_for_ad, infer_ad_type
    kpi_field, kpi_label, kpi_source = get_kpi_for_ad(
        act_id, ad_id, campaign_id,
        campaign_meta=campaign_meta,
        actions=actions_raw,
        adset_id=adset_id
    )
    ad_type = infer_ad_type(camp_obj, adset_opt_goal, adset_dest_type)

    # ── 4) 别名匹配 ──
    from services.guard_engine import (_calc_conversions_with_audit,
        _get_kpi_aliases, _get_kpi_fallback_aliases,
        _POOR_FALLBACK_TYPES,  _match_kpi_filter)
    standard_aliases = _get_kpi_aliases(kpi_field)
    fallback_aliases = _get_kpi_fallback_aliases(kpi_field)

    # ── 5) 转化计算（含审计） ──
    conv_audit = _calc_conversions_with_audit(actions_raw, kpi_field, spend, ad_id)
    conversions = conv_audit["conversions"]
    cpa = (spend / conversions) if conversions > 0 else None

    # ── 6) 所有FB返回的action_type ──
    all_actions = sorted(
        [{"action_type": a.get("action_type", ""), "value": float(a.get("value", 0))}
         for a in actions_raw],
        key=lambda x: -x["value"]
    )

    # ── 7) 规则匹配 ──
    conn2 = get_conn()
    all_rules = conn2.execute(
        "SELECT * FROM guard_rules WHERE enabled=1 ORDER BY rule_type, id"
    ).fetchall()
    conn2.close()

    matching_rules = []
    for r in all_rules:
        kr = dict(r)
        if _match_kpi_filter(kr.get("kpi_filter", ""), {"ad_type": ad_type}):
            rule_info = {
                "id": kr["id"],
                "rule_type": kr["rule_type"],
                "kpi_filter": kr.get("kpi_filter", ""),
                "param_value": kr.get("param_value"),
                "param_ratio": kr.get("param_ratio"),
                "action": kr.get("action"),
                "note": kr.get("note", ""),
            }
            # 计算触发状态
            if kr["rule_type"] == "cpa_exceed" and cpa is not None and kr.get("param_value"):
                effective_target = float(kr["param_value"])
                ratio = float(kr.get("param_ratio", 1.0))
                rule_info["threshold"] = round(effective_target * ratio, 2)
                rule_info["actual_cpa"] = round(cpa, 2)
                rule_info["would_trigger"] = cpa > effective_target * ratio
            elif kr["rule_type"] == "bleed_abs":
                rule_info["threshold_spend"] = kr.get("param_value")
                rule_info["actual_spend"] = round(spend, 2)
                rule_info["actual_conversions"] = conversions
                rule_info["would_trigger"] = spend >= (kr.get("param_value") or 0) and conversions == 0
            else:
                rule_info["would_trigger"] = None
            matching_rules.append(rule_info)

    # ── 8) 额外诊断：CPA与目标对比 ──
    target_cpa = None
    try:
        conn3 = get_conn()
        trow = conn3.execute(
            "SELECT param_value FROM kpi_configs WHERE level='account' AND target_id=? AND kpi_field=? AND is_active=1 LIMIT 1",
            (act_id, kpi_field)
        ).fetchone()
        if trow:
            target_cpa = float(trow["param_value"])
        conn3.close()
    except Exception:
        pass

    result = {
        "ad_id": ad_id,
        "ad_name": ad_name,
        "effective_status": eff_status,
        "campaign_id": campaign_id,
        "adset_id": adset_id,
        "ad_type": ad_type,
        "campaign_meta": {
            "objective": camp_obj or None,
            "optimization_goal": adset_opt_goal or None,
            "destination_type": adset_dest_type or None,
            "custom_event_type": adset_custom_event or None,
        },
        "kpi": {
            "field": kpi_field,
            "label": kpi_label,
            "source": kpi_source,
        },
        "conversions": {
            "count": conversions,
            "matched_action": conv_audit["matched_action"],
            "is_fallback": conv_audit["is_fallback"],
            "reason": conv_audit["reason"],
        },
        "spend": {
            "spend": round(spend, 2),
            "spend_raw": round(spend_raw, 2),
            "currency": currency,
            "impressions": impressions,
            "clicks": clicks,
        },
        "cpa": round(cpa, 2) if cpa else None,
        "target_cpa": round(target_cpa, 2) if target_cpa else None,
        "aliases": {
            "standard": standard_aliases,
            "fallback": fallback_aliases,
        },
        "all_actions": all_actions,
        "rules": {
            "matching": matching_rules,
        },
    }
    return result
