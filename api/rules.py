"""
规则引擎 API v1.1.0
支持: 扩展规则类型、地区字段、一键紧急暂停、连续恶化规则
"""
import json
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List

from core.auth import get_current_user
from core.database import get_conn

router = APIRouter()
logger = logging.getLogger("mira.api.rules")


class GuardRuleIn(BaseModel):
    act_id: str
    rule_name: Optional[str] = None
    level: str = "account"
    target_id: str = "__global__"
    rule_type: str
    # rule_type: bleed_abs / cpa_exceed / trend_drop / consecutive_bad / click_no_conv / budget_burn_fast / budget_cap
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
    rule_type: str
    # rule_type: slow_scale / winner_alert / winner_copy
    target_regions: Optional[List[str]] = None  # 地区列表，如 ["US","TW"]，None=全部
    cpa_ratio: float = 0.8
    min_conversions: int = 3
    consecutive_days: int = 2
    scale_pct: float = 0.15
    max_budget: Optional[float] = None
    roas_threshold: float = 3.0
    enabled: int = 1
    note: Optional[str] = None


class EmergencyPauseRequest(BaseModel):
    confirm: str
    level: str = "campaign"  # campaign | adset | ad  # 必须输入 "CONFIRM" 才能执行


# ── 止损规则 ──────────────────────────────────────────────────────────────

@router.get("/guard")
def list_guard_rules(act_id: Optional[str] = None, user=Depends(get_current_user)):
    conn = get_conn()
    if act_id and act_id != "__global__":
        # 返回指定账户规则 + 全局规则（全局规则排前面）
        global_rows = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id='__global__' ORDER BY id DESC"
        ).fetchall()
        acc_rows = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id=? ORDER BY id DESC", (act_id,)
        ).fetchall()
        rows = list(global_rows) + list(acc_rows)
    elif act_id == "__global__":
        rows = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id='__global__' ORDER BY id DESC"
        ).fetchall()
    else:
        # 全部：全局规则排前面
        global_rows = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id='__global__' ORDER BY id DESC"
        ).fetchall()
        other_rows = conn.execute(
            "SELECT * FROM guard_rules WHERE act_id!='__global__' ORDER BY id DESC"
        ).fetchall()
        rows = list(global_rows) + list(other_rows)
    conn.close()
    return [dict(r) for r in rows]


@router.post("/guard")
def add_guard_rule(body: GuardRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
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
    conn.execute("UPDATE guard_rules SET enabled = 1 - enabled WHERE id=?", (rule_id,))
    row = conn.execute("SELECT enabled FROM guard_rules WHERE id=?", (rule_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "enabled": row["enabled"] if row else None}


@router.delete("/guard/{rule_id}")
def delete_guard_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute("DELETE FROM guard_rules WHERE id=?", (rule_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ── 拉量规则 ──────────────────────────────────────────────────────────────

@router.get("/scale")
def list_scale_rules(act_id: Optional[str] = None, user=Depends(get_current_user)):
    conn = get_conn()
    if act_id and act_id != "__global__":
        # 返回指定账户规则 + 全局规则（全局规则排前面，与止损规则逻辑一致）
        global_rows = conn.execute(
            "SELECT * FROM scale_rules WHERE act_id='__global__' ORDER BY id DESC"
        ).fetchall()
        acc_rows = conn.execute(
            "SELECT * FROM scale_rules WHERE act_id=? ORDER BY id DESC", (act_id,)
        ).fetchall()
        rows = list(global_rows) + list(acc_rows)
    elif act_id == "__global__":
        rows = conn.execute(
            "SELECT * FROM scale_rules WHERE act_id='__global__' ORDER BY id DESC"
        ).fetchall()
    else:
        # 全部：全局规则排前面
        global_rows = conn.execute(
            "SELECT * FROM scale_rules WHERE act_id='__global__' ORDER BY id DESC"
        ).fetchall()
        other_rows = conn.execute(
            "SELECT * FROM scale_rules WHERE act_id!='__global__' ORDER BY id DESC"
        ).fetchall()
        rows = list(global_rows) + list(other_rows)
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        # 反序列化地区字段
        if d.get("target_regions"):
            try:
                d["target_regions"] = json.loads(d["target_regions"])
            except Exception:
                d["target_regions"] = []
        result.append(d)
    return result


@router.post("/scale")
def add_scale_rule(body: ScaleRuleIn, user=Depends(get_current_user)):
    regions_json = json.dumps(body.target_regions) if body.target_regions else None
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO scale_rules
           (act_id, rule_name, rule_type, target_regions,
            cpa_ratio, min_conversions, consecutive_days,
            scale_pct, max_budget, roas_threshold, enabled, note)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (body.act_id, body.rule_name, body.rule_type, regions_json,
         body.cpa_ratio, body.min_conversions, body.consecutive_days,
         body.scale_pct, body.max_budget, body.roas_threshold, body.enabled, body.note)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id, "message": "规则添加成功"}


@router.put("/scale/{rule_id}")
def update_scale_rule(rule_id: int, body: ScaleRuleIn, user=Depends(get_current_user)):
    regions_json = json.dumps(body.target_regions) if body.target_regions else None
    conn = get_conn()
    conn.execute(
        """UPDATE scale_rules SET
           act_id=?, rule_name=?, rule_type=?, target_regions=?,
           cpa_ratio=?, min_conversions=?, consecutive_days=?,
           scale_pct=?, max_budget=?, roas_threshold=?, enabled=?, note=?,
           updated_at=datetime('now')
           WHERE id=?""",
        (body.act_id, body.rule_name, body.rule_type, regions_json,
         body.cpa_ratio, body.min_conversions, body.consecutive_days,
         body.scale_pct, body.max_budget, body.roas_threshold, body.enabled, body.note,
         rule_id)
    )
    conn.commit()
    conn.close()
    return {"success": True}


@router.patch("/scale/{rule_id}/toggle")
def toggle_scale_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute("UPDATE scale_rules SET enabled = 1 - enabled WHERE id=?", (rule_id,))
    row = conn.execute("SELECT enabled FROM scale_rules WHERE id=?", (rule_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "enabled": row["enabled"] if row else None}


@router.delete("/scale/{rule_id}")
def delete_scale_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute("DELETE FROM scale_rules WHERE id=?", (rule_id,))
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
            "value": "budget_burn_fast",
            "label": "瞬烧制止",
            "desc": "单次巡棄周期内消耗增量超过X元时，触发预警或暂停。适合防止瞬间大量烧预算。",
            "params": [
                {"key": "param_value", "label": "单周期最大允许消耗增量(USD)", "type": "number", "default": 20, "required": True,
                 "hint": "两次巡棄之间消耗增加超过此值则触发，如 20 表示单周期增加超过$20就触发"}
            ]
        },
    ]

    scale_types = [
        {
            "value": "slow_scale",
            "label": "平滑加量",
            "desc": "CPA低于目标的X%且连续N天，自动按比例提升预算。",
            "params": [
                {"key": "cpa_ratio", "label": "CPA优秀比例(如0.8=低于目标80%)", "type": "number", "default": 0.8},
                {"key": "min_conversions", "label": "最低转化数", "type": "number", "default": 3},
                {"key": "consecutive_days", "label": "连续天数", "type": "number", "default": 2},
                {"key": "scale_pct", "label": "加量比例(如0.15=+15%)", "type": "number", "default": 0.15},
                {"key": "max_budget", "label": "预算上限(USD)", "type": "number", "default": None},
                {"key": "target_regions", "label": "适用地区(空=全部)", "type": "regions", "default": []}
            ]
        },
        {
            "value": "winner_alert",
            "label": "赢家提示",
            "desc": "ROAS超过阈值时，发送TG通知提示复制该广告组。",
            "params": [
                {"key": "roas_threshold", "label": "ROAS阈值", "type": "number", "default": 3.0},
                {"key": "target_regions", "label": "适用地区(空=全部)", "type": "regions", "default": []}
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

    return {"guard_types": guard_types, "scale_types": scale_types, "actions": actions}


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
        ],
        "scale_rules": [
            {
                "rule_name": "赢家提示 ROAS≥3",
                "rule_type": "winner_alert",
                "roas_threshold": 3.0,
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
        ],
        "scale_rules": [
            {
                "rule_name": "平滑加量 +20%",
                "rule_type": "slow_scale",
                "cpa_ratio": 0.8,
                "min_conversions": 3,
                "consecutive_days": 2,
                "scale_pct": 0.20,
                "note": "来自「激进拉量」模板"
            },
            {
                "rule_name": "赢家提示 ROAS≥4",
                "rule_type": "winner_alert",
                "roas_threshold": 4.0,
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
        ],
        "scale_rules": []
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
        ],
        "scale_rules": [
            {
                "rule_name": "平滑加量 +15%",
                "rule_type": "slow_scale",
                "cpa_ratio": 0.75,
                "min_conversions": 5,
                "consecutive_days": 3,
                "scale_pct": 0.15,
                "note": "来自「电商专属」模板"
            },
            {
                "rule_name": "赢家提示 ROAS≥3.5",
                "rule_type": "winner_alert",
                "roas_threshold": 3.5,
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
        ],
        "scale_rules": [
            {
                "rule_name": "平滑加量 +10%",
                "rule_type": "slow_scale",
                "cpa_ratio": 0.8,
                "min_conversions": 10,
                "consecutive_days": 3,
                "scale_pct": 0.10,
                "note": "来自「线索收集」模板"
            }
        ]
    }
]

class CustomTemplateCreate(BaseModel):
    name: str
    description: str = ""
    guard_rules: list = []
    scale_rules: list = []
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
    global_mode: bool = False              # True 时规则写入 __global__，对所有账户生效

@router.get("/templates")
def list_rule_templates_v2(user=Depends(get_current_user)):
    """获取所有规则模板（内置 + 自定义）"""
    conn = get_conn()
    custom_rows = conn.execute(
        "SELECT * FROM custom_rule_templates ORDER BY created_at DESC"
    ).fetchall()
    conn.close()

    custom = []
    for r in custom_rows:
        d = dict(r)
        d["guard_rules"] = json.loads(d.get("guard_rules") or "[]")
        d["scale_rules"] = json.loads(d.get("scale_rules") or "[]")
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
    try:
        conn.execute(
            """INSERT INTO custom_rule_templates(name, description, guard_rules, scale_rules, tags)
               VALUES(?,?,?,?,?)""",
            (
                body.name.strip(),
                body.description,
                json.dumps(body.guard_rules, ensure_ascii=False),
                json.dumps(body.scale_rules, ensure_ascii=False),
                json.dumps(body.tags, ensure_ascii=False),
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
    try:
        # 查询止损规则
        if body.act_id:
            guard_rows = conn.execute(
                "SELECT * FROM guard_rules WHERE act_id=? AND enabled=1", (body.act_id,)
            ).fetchall()
            scale_rows = conn.execute(
                "SELECT * FROM scale_rules WHERE act_id=? AND enabled=1", (body.act_id,)
            ).fetchall()
        else:
            guard_rows = conn.execute("SELECT * FROM guard_rules WHERE enabled=1").fetchall()
            scale_rows = conn.execute("SELECT * FROM scale_rules WHERE enabled=1").fetchall()

        guard_list = []
        for r in guard_rows:
            d = dict(r)
            # 去掉数据库特有字段，保留规则逻辑字段
            for k in ["id", "act_id", "created_at", "updated_at", "last_triggered"]:
                d.pop(k, None)
            guard_list.append(d)

        scale_list = []
        for r in scale_rows:
            d = dict(r)
            for k in ["id", "act_id", "created_at", "updated_at", "last_triggered"]:
                d.pop(k, None)
            if d.get("target_regions") and isinstance(d["target_regions"], str):
                try:
                    d["target_regions"] = json.loads(d["target_regions"])
                except Exception:
                    pass
            scale_list.append(d)

        conn.execute(
            """INSERT INTO custom_rule_templates(name, description, guard_rules, scale_rules, tags)
               VALUES(?,?,?,?,?)""",
            (
                body.name.strip(),
                body.description,
                json.dumps(guard_list, ensure_ascii=False),
                json.dumps(scale_list, ensure_ascii=False),
                json.dumps([], ensure_ascii=False),
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
        "scale_count": len(scale_list),
        "message": f"已保存为模板「{body.name}」（{len(guard_list)} 条止损 + {len(scale_list)} 条拉量）"
    }

@router.delete("/templates/{template_id}")
def delete_custom_template(template_id: int, user=Depends(get_current_user)):
    """删除自定义模板"""
    conn = get_conn()
    try:
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
    # 全局模式：act_id = __global__，规则对所有账户生效
    global_mode = getattr(body, 'global_mode', False)
    if global_mode:
        target_act_ids = ['__global__']
    elif not target_act_ids:
        raise HTTPException(400, '请指定账户（act_id 或 act_ids）')

    # 先查内置模板
    tpl = next((t for t in RULE_TEMPLATES if t['id'] == str(body.template_id)), None)
    # 再查自定义模板
    if not tpl:
        conn = get_conn()
        row = conn.execute('SELECT * FROM custom_rule_templates WHERE id=?', (body.template_id,)).fetchone()
        conn.close()
        if row:
            d = dict(row)
            d['guard_rules'] = json.loads(d.get('guard_rules') or '[]')
            d['scale_rules'] = json.loads(d.get('scale_rules') or '[]')
            tpl = d
        else:
            raise HTTPException(404, f'模板 {body.template_id} 不存在')

    conn = get_conn()
    total_guard = 0
    total_scale = 0
    results = []
    try:
        for act_id in target_act_ids:
            guard_added = 0
            scale_added = 0
            if body.override_existing:
                conn.execute("DELETE FROM guard_rules WHERE act_id=? AND note LIKE '%模板%'", (act_id,))
                conn.execute("DELETE FROM scale_rules WHERE act_id=? AND note LIKE '%模板%'", (act_id,))
            for r in tpl.get('guard_rules', []):
                conn.execute(
                    'INSERT INTO guard_rules (act_id, rule_name, level, target_id, rule_type, param_value, param_ratio, param_days, action, action_value, enabled, note) VALUES (?,?,?,?,?,?,?,?,?,?,1,?)',
                    (act_id, r.get('rule_name'), r.get('level','account'), r.get('target_id','__global__'), r.get('rule_type'), r.get('param_value'), r.get('param_ratio',1.2), r.get('param_days',2), r.get('action','pause'), r.get('action_value'), r.get('note', f'来自模板「{tpl.get("name", body.template_id)}」'))
                )
                guard_added += 1
            for r in tpl.get('scale_rules', []):
                regions = r.get('target_regions')
                conn.execute(
                    'INSERT INTO scale_rules (act_id, rule_name, rule_type, target_regions, cpa_ratio, min_conversions, consecutive_days, scale_pct, max_budget, roas_threshold, enabled, note) VALUES (?,?,?,?,?,?,?,?,?,?,1,?)',
                    (act_id, r.get('rule_name'), r.get('rule_type','slow_scale'), json.dumps(regions) if isinstance(regions, list) else regions, r.get('cpa_ratio',0.8), r.get('min_conversions',3), r.get('consecutive_days',2), r.get('scale_pct',0.15), r.get('max_budget'), r.get('roas_threshold',3.0), r.get('note', f'来自模板「{tpl.get("name", body.template_id)}」'))
                )
                scale_added += 1
            total_guard += guard_added
            total_scale += scale_added
            results.append({'act_id': act_id, 'guard_added': guard_added, 'scale_added': scale_added})
        conn.commit()
    finally:
        conn.close()
    acc_count = len(target_act_ids)
    return {
        'success': True,
        'template_name': tpl.get('name'),
        'account_count': acc_count,
        'total_guard_added': total_guard,
        'total_scale_added': total_scale,
        'results': results,
        'message': f'已应用「{tpl.get("name")}」到 {acc_count} 个账户：共添加 {total_guard} 条止损规则、{total_scale} 条拉量策略'
    }
