from collections import Counter, defaultdict
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends
from typing import Optional
from core.auth import get_current_user, is_superadmin
from core.database import get_conn
from core.tenancy import apply_account_owner_scope, team_id_for_create

router = APIRouter()

GUARD_TRIGGER_TYPES = {
    "guard", "rule", "kpi", "system",
    "sentinel", "mirror_mode", "heartbeat",
    "bleed_abs", "cpa_exceed", "trend_drop", "consecutive_bad",
    "click_no_conv", "low_ctr_no_conv", "reach_no_conv", "budget_burn_fast",
}


def _scoped_log_where(user):
    where, params = ["1=1"], []
    if not is_superadmin(user):
        team_id = team_id_for_create(user)
        where.append(
            "((l.act_id IS NULL OR l.act_id='' OR l.act_id='__global__') "
            "OR (a.act_id IS NOT NULL AND a.team_id=?))"
        )
        params.append(team_id)
    apply_account_owner_scope(where, params, user, "a.owner_user_id")
    return where, params


def _guard_time_bounds(date_from: Optional[str], date_to: Optional[str]):
    if not date_from:
        date_from = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    elif len(date_from) == 10:
        date_from = f"{date_from} 00:00:00"
    if date_to and len(date_to) == 10:
        date_to = f"{date_to} 23:59:59"
    return date_from, date_to


def _classify_guard_error(text: str) -> str:
    raw = (text or "").strip()
    low = raw.lower()
    if not raw:
        return "无错误"
    if "reels" in low or "3498005" in low or "2446289" in low or "invalid parameter" in low and "升级" in raw:
        return "FB广告级限制"
    if "permission" in low or "permissions" in low or "权限" in raw or "code=200" in low or "(#200)" in raw:
        return "权限不足"
    if "token" in low or "code=190" in low or "oauth" in low:
        return "Token失效"
    if "核验" in raw or "verify" in low or "verification" in low:
        return "状态核验失败"
    if "缺少" in raw or "missing:" in low:
        return "层级ID缺失"
    if "限额" in raw or "spend cap" in low or "billing" in low:
        return "账户/账单限制"
    return "其他"


def _safe_job_health():
    try:
        from core.scheduler import get_scheduler_health
        data = get_scheduler_health() or {}
    except Exception as e:
        return {"running": False, "jobs": [], "error": str(e)}
    wanted = {"guard", "sentinel_patrol", "heartbeat_check", "warmup_check", "op_heartbeat", "account_sync"}
    jobs = [j for j in data.get("jobs", []) if j.get("id") in wanted]
    return {"running": bool(data.get("running")), "jobs": jobs}


@router.get("/guard-observability")
def get_guard_observability(
    act_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 160,
    user=Depends(get_current_user)
):
    date_from, date_to = _guard_time_bounds(date_from, date_to)
    limit = max(20, min(int(limit or 160), 500))
    conn = get_conn()
    where, params = _scoped_log_where(user)
    where.append("l.created_at>=?")
    params.append(date_from)
    if date_to:
        where.append("l.created_at<=?")
        params.append(date_to)
    if act_id:
        where.append("l.act_id=?")
        params.append(act_id)

    trigger_placeholders = ",".join("?" for _ in GUARD_TRIGGER_TYPES)
    where.append(
        "(l.action_type IN ('pause','alert','reduce_budget','increase_budget','bleed_abort') "
        f"OR l.trigger_type IN ({trigger_placeholders}))"
    )
    params.extend(sorted(GUARD_TRIGGER_TYPES))

    rows = conn.execute(
        f"""SELECT l.*,
                  COALESCE(a.name, l.act_id) as account_name,
                  a.currency as account_currency,
                  a.owner_user_id as account_owner_user_id
             FROM action_logs l
             LEFT JOIN accounts a ON a.act_id = l.act_id
            WHERE {' AND '.join(where)}
            ORDER BY l.created_at DESC
            LIMIT ?""",
        [*params, limit]
    ).fetchall()
    conn.close()

    items = [dict(r) for r in rows]
    pause_items = [r for r in items if r.get("action_type") == "pause"]
    levels = Counter((r.get("level") or "unknown") for r in pause_items)
    statuses = Counter((r.get("status") or "unknown") for r in pause_items)
    triggers = Counter((r.get("trigger_type") or "unknown") for r in pause_items)
    failure_classes = Counter()
    for r in pause_items:
        if (r.get("status") or "") in ("failed", "error"):
            failure_classes[_classify_guard_error((r.get("error_msg") or "") + " " + (r.get("trigger_detail") or ""))] += 1

    by_ad = defaultdict(list)
    for r in pause_items:
        detail = r.get("trigger_detail") or ""
        key = (r.get("act_id") or "", r.get("target_id") or "", detail[:120])
        if r.get("level") in ("adset", "campaign") and "missing:" not in str(r.get("target_id") or ""):
            key = (r.get("act_id") or "", detail[:120], r.get("trigger_type") or "")
        by_ad[key].append(r)

    replay = []
    for group in by_ad.values():
        group = sorted(group, key=lambda x: x.get("created_at") or "")
        latest = group[-1]
        if len(replay) >= 40:
            break
        replay.append({
            "account_name": latest.get("account_name") or latest.get("act_id"),
            "act_id": latest.get("act_id"),
            "target_name": latest.get("target_name") or latest.get("target_id"),
            "target_id": latest.get("target_id"),
            "trigger_type": latest.get("trigger_type"),
            "trigger_detail": latest.get("trigger_detail"),
            "created_at": latest.get("created_at"),
            "final_level": latest.get("level"),
            "final_status": latest.get("status"),
            "classification": _classify_guard_error((latest.get("error_msg") or "") + " " + (latest.get("trigger_detail") or "")),
            "steps": [{
                "level": s.get("level"),
                "target_id": s.get("target_id"),
                "status": s.get("status"),
                "error_msg": s.get("error_msg"),
                "created_at": s.get("created_at"),
            } for s in group],
        })

    return {
        "date_from": date_from,
        "date_to": date_to,
        "summary": {
            "pause_attempts": len(pause_items),
            "success": statuses.get("success", 0),
            "failed": statuses.get("failed", 0) + statuses.get("error", 0),
            "escalated": statuses.get("escalated", 0),
            "ad_level_success": sum(1 for r in pause_items if r.get("level") == "ad" and r.get("status") == "success"),
            "ad_level_failed": sum(1 for r in pause_items if r.get("level") == "ad" and r.get("status") == "failed"),
            "level_counts": dict(levels),
            "status_counts": dict(statuses),
            "trigger_counts": dict(triggers),
            "failure_classes": dict(failure_classes),
        },
        "health": _safe_job_health(),
        "recent": items[:80],
        "replay": replay,
    }

@router.get("")
def get_logs(
    act_id: Optional[str] = None,
    action_type: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    keyword: Optional[str] = None,
    trigger_type: Optional[str] = None,
    owner_user_id: Optional[int] = None,
    limit: int = 200,
    offset: int = 0,
    user=Depends(get_current_user)
):
    limit = max(1, min(int(limit or 50), 500))
    offset = max(0, int(offset or 0))
    conn = get_conn()
    where, params = ["1=1"], []
    if not is_superadmin(user):
        team_id = team_id_for_create(user)
        where.append(
            "((l.act_id IS NULL OR l.act_id='' OR l.act_id='__global__') "
            "OR (a.act_id IS NOT NULL AND a.team_id=?))"
        )
        params.append(team_id)
    apply_account_owner_scope(where, params, user, "a.owner_user_id")
    if act_id:
        where.append("l.act_id=?"); params.append(act_id)
    if action_type:
        where.append("l.action_type=?"); params.append(action_type)
    if trigger_type:
        where.append("l.trigger_type=?"); params.append(trigger_type)
    if status:
        where.append("l.status=?"); params.append(status)
    if owner_user_id:
        where.append("a.owner_user_id=?"); params.append(owner_user_id)
    if date_from:
        where.append("date(l.created_at)>=?"); params.append(date_from)
    if date_to:
        where.append("date(l.created_at)<=?"); params.append(date_to)
    if keyword:
        where.append("(l.target_name LIKE ? OR l.trigger_detail LIKE ? OR l.act_id LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw])

    count_params = list(params)
    params.extend([limit, offset])

    rows = conn.execute(
        f"""SELECT l.*,
               COALESCE(a.name, l.act_id) as account_name,
               a.currency as account_currency,
               a.timezone as account_timezone,
               a.owner_user_id as account_owner_user_id
            FROM action_logs l
            LEFT JOIN accounts a ON a.act_id = l.act_id
            WHERE {' AND '.join(where)}
            ORDER BY l.created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()

    total = conn.execute(
        f"""SELECT COUNT(*) as c FROM action_logs l
            LEFT JOIN accounts a ON a.act_id = l.act_id
            WHERE {' AND '.join(where)}""",
        count_params
    ).fetchone()["c"]

    conn.close()
    return {"total": total, "items": [dict(r) for r in rows]}
