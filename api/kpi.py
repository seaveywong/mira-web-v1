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
    {"value": "offsite_conversion.fb_pixel_purchase",                  "label": "像素购买",         "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_add_to_cart",               "label": "加入购物车",        "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_initiate_checkout",         "label": "发起结账",          "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_complete_registration",     "label": "注册完成",          "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_lead",                      "label": "像素潜在客户",       "category": "转化"},
    {"value": "offsite_conversion.fb_pixel_subscribe",                 "label": "像素订阅",          "category": "转化"},
    {"value": "onsite_conversion.lead_grouped",                        "label": "站内线索",          "category": "线索"},
    {"value": "onsite_conversion.messaging_conversation_started_7d",   "label": "私信对话(7日)",      "category": "私信"},
    {"value": "onsite_conversion.messaging_first_reply",               "label": "私信首次回复",       "category": "私信"},
    {"value": "app_install",                                           "label": "应用安装",          "category": "App"},
    {"value": "link_click",                                            "label": "链接点击",          "category": "流量"},
    {"value": "landing_page_view",                                     "label": "落地页浏览",        "category": "流量"},
    {"value": "video_view",                                            "label": "视频观看",          "category": "互动"},
    {"value": "post_engagement",                                       "label": "帖子互动",          "category": "互动"},
    {"value": "reach",                                                 "label": "触达人数",          "category": "曝光"},
    {"value": "impressions",                                           "label": "展示次数",          "category": "曝光"},
    {"value": "omni_purchase",                                         "label": "全渠道购买",        "category": "转化"},
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
    """获取 KPI 字段选项列表（供前端下拉）"""
    return KPI_FIELD_OPTIONS


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
