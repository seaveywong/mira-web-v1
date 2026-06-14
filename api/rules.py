"""
规则引擎 API v1.1.0
支持: 扩展规则类型、地区字段、一键紧急暂停、连续恶化规则
"""
import json
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List

from core.auth import get_current_user, is_superadmin
from core.database import get_conn
from core.tenancy import apply_team_scope, assert_row_access, team_id_for_create

router = APIRouter()
logger = logging.getLogger("mira.api.rules")

GLOBAL_ACT_ID = "__global__"


def _ensure_rule_team_columns(conn) -> None:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(custom_rule_templates)").fetchall()}
    if cols and "team_id" not in cols:
        conn.execute("ALTER TABLE custom_rule_templates ADD COLUMN team_id INTEGER")
        conn.commit()


def _assert_rule_target_access(conn, act_id: str, user) -> None:
    target = (act_id or "").strip()
    if target == GLOBAL_ACT_ID:
        raise HTTPException(400, "Global account rules are disabled. Please choose a specific ad account.")
    if not target:
        raise HTTPException(400, "请指定账户")
    assert_row_access(conn, "accounts", target, user, id_column="act_id")


def _team_account_act_ids(conn, user) -> list[str]:
    where, params = [], []
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(f"SELECT act_id FROM accounts {clause}", params).fetchall()
    return [r["act_id"] for r in rows if r["act_id"]]


def _guard_rule_row_or_404(conn, rule_id: int):
    row = conn.execute("SELECT id, act_id FROM guard_rules WHERE id=?", (rule_id,)).fetchone()
    if not row:
        raise HTTPException(404, "规则不存在")
    return row


def _scale_rule_row_or_404(conn, rule_id: int):
    row = conn.execute("SELECT id, act_id FROM scale_rules WHERE id=?", (rule_id,)).fetchone()
    if not row:
        raise HTTPException(404, "拉量策略不存在")
    return row


class GuardRuleIn(BaseModel):
    act_id: str
    rule_name: Optional[str] = None
    level: str = "account"
    target_id: str = "__global__"
    rule_type: str
    # rule_type: bleed_abs / cpa_exceed / trend_drop / consecutive_bad / click_no_conv
    # / low_ctr_no_conv / reach_no_conv / budget_burn_fast / budget_cap
    param_value: Optional[float] = None
    param_ratio: Optional[float] = 1.2
    param_days: Optional[int] = 2
    action: str = "pause"
    # action: pause / reduce_budget / alert_only / pause_adset / pause_campaign
    action_value: Optional[float] = None
    enabled: int = 1
    silent_start: Optional[str] = None
    silent_end: Optional[str] = None
    note: Optional[str] = None
    kpi_filter: Optional[str] = None   # KPI类型筛选


class ScaleRuleIn(BaseModel):
    act_id: str
    rule_name: Optional[str] = None
    rule_type: str = "slow_scale"
    cpa_ratio: Optional[float] = 0.8
    min_conversions: Optional[int] = 3
    consecutive_days: Optional[int] = 2
    scale_pct: Optional[float] = 0.15
    max_budget: Optional[float] = None
    roas_threshold: Optional[float] = None
    target_regions: Optional[str] = None
    enabled: int = 1
    note: Optional[str] = None



class EmergencyPauseRequest(BaseModel):
    confirm: str
    level: str = "campaign"  # campaign | adset | ad  # 必须输入 "CONFIRM" 才能执行


# ── 止损规则 ──────────────────────────────────────────────────────────────

@router.get("/guard")
def list_guard_rules(act_id: Optional[str] = None, user=Depends(get_current_user)):
    conn = get_conn()
    if act_id and act_id != GLOBAL_ACT_ID:
        _assert_rule_target_access(conn, act_id, user)
        rows = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id=? ORDER BY id DESC", (act_id,)
        ).fetchall()
    elif act_id == GLOBAL_ACT_ID:
        rows = []
    else:
        account_ids = _team_account_act_ids(conn, user)
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            rows = conn.execute(
                f"SELECT * FROM guard_rules WHERE act_id!=? AND act_id IN ({placeholders}) ORDER BY id DESC",
                [GLOBAL_ACT_ID] + account_ids,
            ).fetchall()
        else:
            rows = []
    conn.close()
    return [dict(r) for r in rows]


@router.post("/guard")
def add_guard_rule(body: GuardRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    _assert_rule_target_access(conn, body.act_id, user)
    cur = conn.execute(
        """INSERT INTO guard_rules
           (act_id, rule_name, level, target_id, rule_type,
            param_value, param_ratio, param_days,
            action, action_value, enabled,
            silent_start, silent_end, note, kpi_filter)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (body.act_id, body.rule_name, body.level, body.target_id, body.rule_type,
         body.param_value, body.param_ratio, body.param_days,
         body.action, body.action_value, body.enabled,
         body.silent_start, body.silent_end, body.note, body.kpi_filter)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id, "message": "规则添加成功"}


@router.put("/guard/{rule_id}")
def update_guard_rule(rule_id: int, body: GuardRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    old = _guard_rule_row_or_404(conn, rule_id)
    _assert_rule_target_access(conn, old["act_id"], user)
    _assert_rule_target_access(conn, body.act_id, user)
    conn.execute(
        """UPDATE guard_rules SET
           act_id=?, rule_name=?, level=?, target_id=?, rule_type=?,
           param_value=?, param_ratio=?, param_days=?,
           action=?, action_value=?, enabled=?,
           silent_start=?, silent_end=?, note=?, kpi_filter=?,
           updated_at=datetime('now')
           WHERE id=?""",
        (body.act_id, body.rule_name, body.level, body.target_id, body.rule_type,
         body.param_value, body.param_ratio, body.param_days,
         body.action, body.action_value, body.enabled,
         body.silent_start, body.silent_end, body.note,
         getattr(body, "kpi_filter", None), rule_id)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "规则更新成功"}


@router.patch("/guard/{rule_id}/toggle")
def toggle_guard_rule(rule_id: int, user=Depends(get_current_user)):
    """快速启用/禁用规则"""
    conn = get_conn()
    row = _guard_rule_row_or_404(conn, rule_id)
    _assert_rule_target_access(conn, row["act_id"], user)
    conn.execute("UPDATE guard_rules SET enabled = 1 - enabled WHERE id=?", (rule_id,))
    row = conn.execute("SELECT enabled FROM guard_rules WHERE id=?", (rule_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "enabled": row["enabled"] if row else None}


@router.delete("/guard/{rule_id}")
def delete_guard_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _guard_rule_row_or_404(conn, rule_id)
    _assert_rule_target_access(conn, row["act_id"], user)
    conn.execute("DELETE FROM guard_rules WHERE id=?", (rule_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ── 一键紧急暂停 ──────────────────────────────────────────────────────────

@router.post("/emergency-pause")
def emergency_pause(body: EmergencyPauseRequest, user=Depends(get_current_user)):
    """
    一键紧急暂停所有账户的所有活跃系列
    需要输入确认词 "CONFIRM"
    """
    if body.confirm != "CONFIRM":
        raise HTTPException(400, "请输入确认词 CONFIRM 以执行紧急暂停")
    if not is_superadmin(user):
        raise HTTPException(403, "全局紧急暂停只能由超级管理员执行")

    from services.guard_engine import emergency_pause_all
    result = emergency_pause_all(operator="user", level=body.level)
    # 构建友好的返回消息
    level_label = {"campaign": "广告系列", "adset": "广告组", "ad": "广告"}.get(body.level, "广告系列")
    manual_count = len(result.get("manual_required", []))
    msg = f"紧急暂停完成：{result['total']} 个{level_label}，成功 {result['success']}，失败 {result['failed']}"
    if manual_count:
        msg += f"，{manual_count} 项需人工处理"
    result["message"] = msg
    return result


# ── 规则类型元数据（供前端下拉框使用）──────────────────────────────────

@router.get("/meta/types")
def get_rule_types(user=Depends(get_current_user)):
    """返回所有规则类型的元数据"""
    guard_types = [
        {
            "value": "bleed_abs",
            "label": "空成效止血",
            "desc": "消耗超过X元且KPI转化=0时，自动暂停广告。适合防止新广告乱烧钱。",
            "params": [
                {"key": "param_value", "label": "止血金额(USD)", "type": "number", "default": 20, "required": True}
            ]
        },
        {
            "value": "cpa_exceed",
            "label": "CPA超标止损",
            "desc": "当CPA超过设定阈值的N倍时，自动暂停或降预算。设置「目标CPA」后，规则会将实际CPA与「目标CPA×超标倍数」进行比较。",
            "params": [
                {"key": "param_value", "label": "目标CPA(USD)", "type": "number", "default": None, "required": True,
                 "hint": "设定一个绝对CPA阈值，如 40 表示目标CPA为$40"},
                {"key": "param_ratio", "label": "触发倍数", "type": "number", "default": 1.3, "required": True,
                 "hint": "实际CPA 超过 目标CPA×该倍数 时触发，如 1.5 表示超过150%时触发"}
            ]
        },
        {
            "value": "trend_drop",
            "label": "ROAS趋势熔断",
            "desc": "当ROAS相比昨日跌幅超过X%时，触发熔断暂停。",
            "params": [
                {"key": "param_value", "label": "跌幅阈值(%)", "type": "number", "default": 40, "required": True}
            ]
        },
        {
            "value": "consecutive_bad",
            "label": "连续恶化止损",
            "desc": "连续N天CPA超标，自动暂停。适合识别持续表现差的广告。",
            "params": [
                {"key": "param_value", "label": "目标CPA(USD)", "type": "number", "default": None, "required": True,
                 "hint": "设定一个绝对CPA阈值，如 40 表示目标CPA为$40"},
                {"key": "param_days", "label": "连续天数", "type": "number", "default": 2, "required": True},
                {"key": "param_ratio", "label": "CPA超标倍数", "type": "number", "default": 1.3, "required": True,
                 "hint": "实际CPA 超过 目标CPA×该倍数 时计为一天超标"}
            ]
        },
        {
            "value": "click_no_conv",
            "label": "高频点击无转化预警",
            "desc": "点击数超过X且KPI转化=0，发送预警通知（不自动暂停）。",
            "params": [
                {"key": "param_value", "label": "点击数阈值", "type": "number", "default": 100, "required": True}
            ]
        },
        {
            "value": "low_ctr_no_conv",
            "label": "低CTR空转止损",
            "desc": "消耗达到阈值后，如果CTR过低且KPI转化为0，则触发预警或暂停。适合识别素材/受众不匹配。",
            "params": [
                {"key": "param_value", "label": "最低消耗(USD)", "type": "number", "default": 10, "required": True},
                {"key": "param_ratio", "label": "最高CTR(%)", "type": "number", "default": 0.5, "required": True}
            ]
        },
        {
            "value": "reach_no_conv",
            "label": "高覆盖无转化止损",
            "desc": "覆盖人数达到阈值且已有一定消耗，但KPI转化仍为0时触发。适合识别放量后无反馈的广告。",
            "params": [
                {"key": "param_value", "label": "覆盖人数阈值", "type": "number", "default": 1000, "required": True},
                {"key": "param_ratio", "label": "最低消耗(USD)", "type": "number", "default": 10, "required": True}
            ]
        },
        {
            "value": "budget_burn_fast",
            "label": "瞬烧制止",
            "desc": "单次巡棄周期内消耗增量超过X元时，触发预警或暂停。适合防止瞬间大量烧预算。",
            "params": [
                {"key": "param_value", "label": "单周期最大允许消耗增量(USD)", "type": "number", "default": 20, "required": True,
                 "hint": "两次巡棄之间消耗增加超过此值则触发，如 20 表示单周期增加超过$20就触发"}
            ]
        },
    ]

    actions = [
        {"value": "pause", "label": "暂停广告"},
        {"value": "reduce_budget", "label": "降低预算"},
        {"value": "alert_only", "label": "仅发送预警"},
        {"value": "pause_adset", "label": "暂停广告组"},
        {"value": "pause_campaign", "label": "暂停广告系列"},
    ]

    scale_types = [
        {
            "value": "slow_scale",
            "label": "稳健拉量",
            "desc": "CPA 达标且有稳定转化后，按较小比例提升广告组日预算。",
            "defaults": {"cpa_ratio": 0.8, "min_conversions": 3, "consecutive_days": 2, "scale_pct": 0.15}
        },
        {
            "value": "fast_scale",
            "label": "快速拉量",
            "desc": "适合已验证素材和受众，转化充足时用更高比例加预算。",
            "defaults": {"cpa_ratio": 0.7, "min_conversions": 5, "consecutive_days": 1, "scale_pct": 0.25}
        },
        {
            "value": "roas_scale",
            "label": "ROAS 拉量",
            "desc": "ROAS 达到阈值且转化充足时加预算，适合购物类广告。",
            "defaults": {"cpa_ratio": 0.9, "min_conversions": 3, "consecutive_days": 1, "scale_pct": 0.2, "roas_threshold": 3.0}
        },
    ]

    return {"guard_types": guard_types, "scale_types": scale_types, "actions": actions}


@router.get("/scale")
def list_scale_rules(act_id: Optional[str] = None, user=Depends(get_current_user)):
    conn = get_conn()
    if act_id and act_id != GLOBAL_ACT_ID:
        _assert_rule_target_access(conn, act_id, user)
        rows = conn.execute(
            "SELECT * FROM scale_rules WHERE act_id=? ORDER BY id DESC", (act_id,)
        ).fetchall()
    elif act_id == GLOBAL_ACT_ID:
        rows = []
    else:
        account_ids = _team_account_act_ids(conn, user)
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            rows = conn.execute(
                f"SELECT * FROM scale_rules WHERE act_id!=? AND act_id IN ({placeholders}) ORDER BY id DESC",
                [GLOBAL_ACT_ID] + account_ids,
            ).fetchall()
        else:
            rows = []
    conn.close()
    return [dict(r) for r in rows]


@router.post("/scale")
def add_scale_rule(body: ScaleRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    _assert_rule_target_access(conn, body.act_id, user)
    cur = conn.execute(
        """INSERT INTO scale_rules
           (act_id, rule_name, rule_type, cpa_ratio, min_conversions,
            consecutive_days, scale_pct, max_budget, roas_threshold,
            target_regions, enabled, note)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (body.act_id, body.rule_name, body.rule_type, body.cpa_ratio,
         body.min_conversions, body.consecutive_days, body.scale_pct,
         body.max_budget, body.roas_threshold, body.target_regions,
         body.enabled, body.note)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id, "message": "拉量策略添加成功"}


@router.put("/scale/{rule_id}")
def update_scale_rule(rule_id: int, body: ScaleRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    old = _scale_rule_row_or_404(conn, rule_id)
    _assert_rule_target_access(conn, old["act_id"], user)
    _assert_rule_target_access(conn, body.act_id, user)
    conn.execute(
        """UPDATE scale_rules SET
           act_id=?, rule_name=?, rule_type=?, cpa_ratio=?,
           min_conversions=?, consecutive_days=?, scale_pct=?,
           max_budget=?, roas_threshold=?, target_regions=?,
           enabled=?, note=?
           WHERE id=?""",
        (body.act_id, body.rule_name, body.rule_type, body.cpa_ratio,
         body.min_conversions, body.consecutive_days, body.scale_pct,
         body.max_budget, body.roas_threshold, body.target_regions,
         body.enabled, body.note, rule_id)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "拉量策略更新成功"}


@router.patch("/scale/{rule_id}/toggle")
def toggle_scale_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _scale_rule_row_or_404(conn, rule_id)
    _assert_rule_target_access(conn, row["act_id"], user)
    conn.execute("UPDATE scale_rules SET enabled = 1 - enabled WHERE id=?", (rule_id,))
    row = conn.execute("SELECT enabled FROM scale_rules WHERE id=?", (rule_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "enabled": row["enabled"] if row else None}


@router.delete("/scale/{rule_id}")
def delete_scale_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _scale_rule_row_or_404(conn, rule_id)
    _assert_rule_target_access(conn, row["act_id"], user)
    conn.execute("DELETE FROM scale_rules WHERE id=?", (rule_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ─── 规则模板 API ────────────────────────────────────────────────────────
from pydantic import BaseModel

RULE_TEMPLATES = [
    {
        "id": "starter",
        "name": "🚀 新手入门套餐",
        "desc": "适合刚开始投放的账户，基础止损保护 + 赢家提示，防止新广告乱烧钱",
        "tags": ["推荐", "新手"],
        "guard_rules": [
            {
                "rule_name": "空成效止血 $20",
                "rule_type": "bleed_abs",
                "level": "account",
                "target_id": "__global__",
                "param_value": 20.0,
                "action": "pause",
                "note": "来自「新手入门」模板"
            },
            {
                "rule_name": "CPA超标1.5倍止损",
                "rule_type": "cpa_exceed",
                "level": "account",
                "target_id": "__global__",
                "param_ratio": 1.5,
                "action": "pause",
                "note": "来自「新手入门」模板"
            }
        ]
    },
    {
        "id": "aggressive",
        "name": "⚡ 激进拉量套餐",
        "desc": "适合已有稳定跑量广告的账户，严格止损 + 积极加量，最大化ROI",
        "tags": ["进阶", "拉量"],
        "guard_rules": [
            {
                "rule_name": "空成效止血 $15",
                "rule_type": "bleed_abs",
                "level": "account",
                "target_id": "__global__",
                "param_value": 15.0,
                "action": "pause",
                "note": "来自「激进拉量」模板"
            },
            {
                "rule_name": "CPA超标1.3倍止损",
                "rule_type": "cpa_exceed",
                "level": "account",
                "target_id": "__global__",
                "param_ratio": 1.3,
                "action": "pause",
                "note": "来自「激进拉量」模板"
            },
            {
                "rule_name": "ROAS跌幅40%熔断",
                "rule_type": "trend_drop",
                "level": "account",
                "target_id": "__global__",
                "param_value": 40.0,
                "action": "pause",
                "note": "来自「激进拉量」模板"
            },
            {
                "rule_name": "连续2天CPA超标",
                "rule_type": "consecutive_bad",
                "level": "account",
                "target_id": "__global__",
                "param_days": 2,
                "param_ratio": 1.3,
                "action": "pause",
                "note": "来自「激进拉量」模板"
            }
        ]
    },
    {
        "id": "conservative",
        "name": "🛡️ 保守防御套餐",
        "desc": "适合预算紧张或测试期账户，多重止损保护，优先保住本金",
        "tags": ["保守", "防御"],
        "guard_rules": [
            {
                "rule_name": "空成效止血 $10",
                "rule_type": "bleed_abs",
                "level": "account",
                "target_id": "__global__",
                "param_value": 10.0,
                "action": "pause",
                "note": "来自「保守防御」模板"
            },
            {
                "rule_name": "CPA超标1.2倍止损",
                "rule_type": "cpa_exceed",
                "level": "account",
                "target_id": "__global__",
                "param_ratio": 1.2,
                "action": "pause",
                "note": "来自「保守防御」模板"
            },
            {
                "rule_name": "ROAS跌幅30%熔断",
                "rule_type": "trend_drop",
                "level": "account",
                "target_id": "__global__",
                "param_value": 30.0,
                "action": "pause",
                "note": "来自「保守防御」模板"
            },
            {
                "rule_name": "连续2天CPA超标",
                "rule_type": "consecutive_bad",
                "level": "account",
                "target_id": "__global__",
                "param_days": 2,
                "param_ratio": 1.2,
                "action": "pause",
                "note": "来自「保守防御」模板"
            },
            {
                "rule_name": "高频点击无转化预警",
                "rule_type": "click_no_conv",
                "level": "account",
                "target_id": "__global__",
                "param_value": 80.0,
                "action": "alert_only",
                "note": "来自「保守防御」模板"
            }
        ]
    },
    {
        "id": "ecommerce",
        "name": "🛒 电商专属套餐",
        "desc": "针对电商广告优化，关注ROAS和CPA，兼顾止损与拉量",
        "tags": ["电商", "ROAS"],
        "guard_rules": [
            {
                "rule_name": "空成效止血 $20",
                "rule_type": "bleed_abs",
                "level": "account",
                "target_id": "__global__",
                "param_value": 20.0,
                "action": "pause",
                "note": "来自「电商专属」模板"
            },
            {
                "rule_name": "CPA超标1.3倍止损",
                "rule_type": "cpa_exceed",
                "level": "account",
                "target_id": "__global__",
                "param_ratio": 1.3,
                "action": "pause",
                "note": "来自「电商专属」模板"
            },
            {
                "rule_name": "ROAS跌幅35%熔断",
                "rule_type": "trend_drop",
                "level": "account",
                "target_id": "__global__",
                "param_value": 35.0,
                "action": "pause",
                "note": "来自「电商专属」模板"
            },
            {
                "rule_name": "预算消耗过快预警",
                "rule_type": "budget_burn_fast",
                "level": "account",
                "target_id": "__global__",
                "param_value": 70.0,
                "action": "alert_only",
                "note": "来自「电商专属」模板"
            }
        ]
    },
    {
        "id": "lead_gen",
        "name": "📋 线索收集套餐",
        "desc": "针对表单/线索类广告，重点控制CPA，防止无效点击消耗预算",
        "tags": ["线索", "表单"],
        "guard_rules": [
            {
                "rule_name": "空成效止血 $25",
                "rule_type": "bleed_abs",
                "level": "account",
                "target_id": "__global__",
                "param_value": 25.0,
                "action": "pause",
                "note": "来自「线索收集」模板"
            },
            {
                "rule_name": "CPA超标1.4倍止损",
                "rule_type": "cpa_exceed",
                "level": "account",
                "target_id": "__global__",
                "param_ratio": 1.4,
                "action": "pause",
                "note": "来自「线索收集」模板"
            },
            {
                "rule_name": "高频点击无转化预警",
                "rule_type": "click_no_conv",
                "level": "account",
                "target_id": "__global__",
                "param_value": 120.0,
                "action": "alert_only",
                "note": "来自「线索收集」模板"
            },
            {
                "rule_name": "连续3天CPA超标",
                "rule_type": "consecutive_bad",
                "level": "account",
                "target_id": "__global__",
                "param_days": 3,
                "param_ratio": 1.4,
                "action": "pause",
                "note": "来自「线索收集」模板"
            }
        ]
    }
]

class CustomTemplateCreate(BaseModel):
    name: str
    description: str = ""
    guard_rules: list = []
    tags: list = []

class SaveCurrentAsTemplateRequest(BaseModel):
    name: str
    description: str = ""
    act_id: Optional[str] = "__all__"

class ApplyTemplateRequest(BaseModel):
    template_id: str
    act_id: Optional[str] = None          # 单账户（向下兼容）
    act_ids: Optional[List[str]] = None   # 多账户批量应用
    override_existing: bool = False        # 是否覆盖已有规则
    global_mode: bool = False              # Legacy field; global account rules are disabled.

@router.get("/templates")
def list_rule_templates_v2(user=Depends(get_current_user)):
    """获取所有规则模板（内置 + 自定义）"""
    conn = get_conn()
    _ensure_rule_team_columns(conn)
    where, params = [], []
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    custom_rows = conn.execute(
        f"SELECT * FROM custom_rule_templates {clause} ORDER BY created_at DESC",
        params,
    ).fetchall()
    conn.close()

    custom = []
    for r in custom_rows:
        d = dict(r)
        d["guard_rules"] = json.loads(d.get("guard_rules") or "[]")
        d["tags"] = json.loads(d.get("tags") or "[]")
        custom.append(d)

    return {
        "builtin": RULE_TEMPLATES,
        "custom": custom
    }

@router.post("/templates/custom")
def create_custom_template(body: CustomTemplateCreate, user=Depends(get_current_user)):
    """手动创建自定义规则模板"""
    if not body.name.strip():
        raise HTTPException(400, "模板名称不能为空")
    conn = get_conn()
    _ensure_rule_team_columns(conn)
    resource_team_id = team_id_for_create(user)
    try:
        conn.execute(
            """INSERT INTO custom_rule_templates(name, description, guard_rules, tags, team_id)
               VALUES(?,?,?,?,?)""",
            (
                body.name.strip(),
                body.description,
                json.dumps(body.guard_rules, ensure_ascii=False),
                json.dumps(body.tags, ensure_ascii=False),
                resource_team_id,
            )
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        conn.close()
    return {"success": True, "id": new_id, "message": f"模板「{body.name}」已创建"}

@router.post("/templates/save-current")
def save_current_as_template(body: SaveCurrentAsTemplateRequest, user=Depends(get_current_user)):
    """将当前账户（或全部账户）的规则保存为自定义模板"""
    if not body.name.strip():
        raise HTTPException(400, "模板名称不能为空")

    conn = get_conn()
    _ensure_rule_team_columns(conn)
    try:
        # 查询止损规则
        if body.act_id and body.act_id not in ("__all__", ""):
            _assert_rule_target_access(conn, body.act_id, user)
            guard_rows = conn.execute(
                "SELECT * FROM guard_rules WHERE act_id=? AND enabled=1", (body.act_id,)
            ).fetchall()
        elif is_superadmin(user):
            guard_rows = conn.execute(
                "SELECT * FROM guard_rules WHERE enabled=1 AND act_id!=?", (GLOBAL_ACT_ID,)
            ).fetchall()
        else:
            account_ids = _team_account_act_ids(conn, user)
            if account_ids:
                placeholders = ",".join("?" for _ in account_ids)
                guard_rows = conn.execute(
                    f"SELECT * FROM guard_rules WHERE enabled=1 AND act_id IN ({placeholders})",
                    account_ids,
                ).fetchall()
            else:
                guard_rows = []

        guard_list = []
        for r in guard_rows:
            d = dict(r)
            for k in ["id", "act_id", "created_at", "updated_at", "last_triggered"]:
                d.pop(k, None)
            guard_list.append(d)

        conn.execute(
            """INSERT INTO custom_rule_templates(name, description, guard_rules, tags, team_id)
               VALUES(?,?,?,?,?)""",
            (
                body.name.strip(),
                body.description,
                json.dumps(guard_list, ensure_ascii=False),
                json.dumps([], ensure_ascii=False),
                team_id_for_create(user),
            )
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        conn.close()

    return {
        "success": True,
        "id": new_id,
        "guard_count": len(guard_list),
        "message": f"已保存为模板「{body.name}」（{len(guard_list)} 条止损规则）"
    }

@router.delete("/templates/{template_id}")
def delete_custom_template(template_id: int, user=Depends(get_current_user)):
    """删除自定义模板"""
    conn = get_conn()
    _ensure_rule_team_columns(conn)
    try:
        assert_row_access(conn, "custom_rule_templates", template_id, user, allow_unassigned=False)
        result = conn.execute(
            "DELETE FROM custom_rule_templates WHERE id=?", (template_id,)
        )
        conn.commit()
        if result.rowcount == 0:
            raise HTTPException(404, "模板不存在或已删除")
    finally:
        conn.close()
    return {"success": True, "message": "模板已删除"}

@router.post('/templates/apply')
def apply_rule_template(body: ApplyTemplateRequest, user=Depends(get_current_user)):
    """一键应用规则模板到指定账户（支持内置和自定义模板，支持单账户和多账户批量应用）"""
    # 确定目标账户列表：优先用 act_ids，如无则回退到 act_id
    target_act_ids = []
    if body.act_ids and len(body.act_ids) > 0:
        target_act_ids = [a for a in body.act_ids if a]
    elif body.act_id:
        target_act_ids = [body.act_id]
    global_mode = getattr(body, 'global_mode', False)
    if global_mode:
        raise HTTPException(400, "Global account rules are disabled. Please choose specific ad accounts.")
    if not target_act_ids:
        raise HTTPException(400, '请指定账户（act_id 或 act_ids）')

    access_conn = get_conn()
    try:
        for target_act_id in target_act_ids:
            _assert_rule_target_access(access_conn, target_act_id, user)
    finally:
        access_conn.close()

    # 先查内置模板
    tpl = next((t for t in RULE_TEMPLATES if t['id'] == str(body.template_id)), None)
    # 再查自定义模板
    if not tpl:
        conn = get_conn()
        try:
            _ensure_rule_team_columns(conn)
            try:
                template_db_id = int(body.template_id)
            except (TypeError, ValueError):
                raise HTTPException(404, f'模板 {body.template_id} 不存在')
            assert_row_access(conn, "custom_rule_templates", template_db_id, user, allow_unassigned=False)
            row = conn.execute('SELECT * FROM custom_rule_templates WHERE id=?', (template_db_id,)).fetchone()
        finally:
            conn.close()
        if row:
            d = dict(row)
            d['guard_rules'] = json.loads(d.get('guard_rules') or '[]')
            tpl = d
        else:
            raise HTTPException(404, f'模板 {body.template_id} 不存在')

    conn = get_conn()
    total_guard = 0
    results = []
    try:
        for act_id in target_act_ids:
            guard_added = 0
            if body.override_existing:
                conn.execute("DELETE FROM guard_rules WHERE act_id=? AND note LIKE '%模板%'", (act_id,))
            for r in tpl.get('guard_rules', []):
                conn.execute(
                    'INSERT INTO guard_rules (act_id, rule_name, level, target_id, rule_type, param_value, param_ratio, param_days, action, action_value, enabled, note) VALUES (?,?,?,?,?,?,?,?,?,?,1,?)',
                    (act_id, r.get('rule_name'), r.get('level','account'), r.get('target_id','__global__'), r.get('rule_type'), r.get('param_value'), r.get('param_ratio',1.2), r.get('param_days',2), r.get('action','pause'), r.get('action_value'), r.get('note', f'来自模板「{tpl.get("name", body.template_id)}」'))
                )
                guard_added += 1
            total_guard += guard_added
            results.append({'act_id': act_id, 'guard_added': guard_added})
        conn.commit()
    finally:
        conn.close()
    acc_count = len(target_act_ids)
    return {
        'success': True,
        'template_name': tpl.get('name'),
        'account_count': acc_count,
        'total_guard_added': total_guard,
        'results': results,
        'message': f'已应用「{tpl.get("name")}」到 {acc_count} 个账户：共添加 {total_guard} 条止损规则'
    }


# ---- cooling reset ------
@router.post("/guard/reset-cooldown")
def reset_cooldown(user=Depends(get_current_user)):
    if not is_superadmin(user):
        raise HTTPException(403, "Superadmin only")
    from services.guard_engine import _action_cooldown
    _action_cooldown.clear()
    logger.info("cooldown manually reset")
    return {"success": True, "message": "冷却状态已重置, 规则可立即重新触发"}
