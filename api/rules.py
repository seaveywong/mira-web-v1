"""
规则引擎 API v1.1.0
支持: 扩展规则类型、地区字段、一键紧急暂停、连续恶化规则
"""
import json
import logging
import re
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None

from core.auth import get_current_user, is_superadmin
from core.database import get_conn
from core.tenancy import apply_team_scope, assert_row_access, team_id_for_create

router = APIRouter()
logger = logging.getLogger("mira.api.rules")

GLOBAL_ACT_ID = "__global__"
OWNER_SCOPE_ACT_ID = "__owner__"
RULE_SCOPE_ACCOUNT = "account"
RULE_SCOPE_OWNER = "owner"
DEFAULT_OWNER_RULE_NOTE = "owner_default_stoploss_v2"


def _ensure_rule_team_columns(conn) -> None:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(custom_rule_templates)").fetchall()}
    if cols and "team_id" not in cols:
        conn.execute("ALTER TABLE custom_rule_templates ADD COLUMN team_id INTEGER")
        conn.commit()


def _ensure_rule_scope_columns(conn) -> None:
    guard_cols = {r["name"] for r in conn.execute("PRAGMA table_info(guard_rules)").fetchall()}
    if guard_cols:
        if "scope" not in guard_cols:
            conn.execute("ALTER TABLE guard_rules ADD COLUMN scope TEXT DEFAULT 'account'")
        if "owner_user_id" not in guard_cols:
            conn.execute("ALTER TABLE guard_rules ADD COLUMN owner_user_id INTEGER")
        if "team_id" not in guard_cols:
            conn.execute("ALTER TABLE guard_rules ADD COLUMN team_id INTEGER")
        if "created_by" not in guard_cols:
            conn.execute("ALTER TABLE guard_rules ADD COLUMN created_by TEXT")
        conn.execute("UPDATE guard_rules SET scope='account' WHERE scope IS NULL OR scope=''")
        conn.execute(
            """UPDATE guard_rules
               SET team_id=(SELECT a.team_id FROM accounts a WHERE a.act_id=guard_rules.act_id)
               WHERE team_id IS NULL AND act_id NOT IN (?, ?)""",
            (GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID),
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_guard_rules_scope_owner ON guard_rules(scope, owner_user_id, enabled)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_guard_rules_scope_account ON guard_rules(act_id, scope, enabled)")
    conn.execute(
        """UPDATE guard_rules
           SET level='ad', target_id='__global__'
           WHERE scope=? AND act_id=? AND COALESCE(level,'')='account'""",
        (RULE_SCOPE_OWNER, OWNER_SCOPE_ACT_ID),
    )

    scale_cols = {r["name"] for r in conn.execute("PRAGMA table_info(scale_rules)").fetchall()}
    if scale_cols:
        if "scope" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN scope TEXT DEFAULT 'account'")
        if "owner_user_id" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN owner_user_id INTEGER")
        if "team_id" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN team_id INTEGER")
        if "created_by" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN created_by TEXT")
        conn.execute("UPDATE scale_rules SET scope='account' WHERE scope IS NULL OR scope=''")
        conn.execute(
            """UPDATE scale_rules
               SET team_id=(SELECT a.team_id FROM accounts a WHERE a.act_id=scale_rules.act_id)
               WHERE team_id IS NULL AND act_id NOT IN (?, ?)""",
            (GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID),
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scale_rules_scope_owner ON scale_rules(scope, owner_user_id, enabled)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scale_rules_scope_account ON scale_rules(act_id, scope, enabled)")
    conn.commit()


def _assert_rule_target_access(conn, act_id: str, user) -> None:
    target = (act_id or "").strip()
    if target in (GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID):
        return
    if not target:
        return
    assert_row_access(conn, "accounts", target, user, id_column="act_id")


def _actor_uid(user) -> int | None:
    try:
        uid = int((user or {}).get("uid") or 0)
    except (TypeError, ValueError):
        return None
    return uid if uid > 0 else None


def _actor_can_manage_team(user) -> bool:
    return bool(is_superadmin(user) or (user or {}).get("role") == "admin")


def _account_row(conn, act_id: str):
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    select_cols = ["act_id", "name", "team_id", "owner_user_id"]
    for col in ("currency", "timezone", "timezone_name", "timezone_offset_hours_utc"):
        if col in cols:
            select_cols.append(col)
    return conn.execute(
        f"SELECT {', '.join(select_cols)} FROM accounts WHERE act_id=?",
        (act_id,),
    ).fetchone()


def _allowance_norm_act_id(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    return raw if raw.lower().startswith("act_") else f"act_{raw}"


def _allowance_plain_id(value: str | None) -> str:
    raw = str(value or "").strip()
    return raw[4:] if raw.lower().startswith("act_") else raw


def _normalize_ad_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = re.findall(r"\d{6,}", raw)
    if parts:
        return max(parts, key=len)
    return raw


def _account_local_date(account) -> str:
    if not account:
        return date.today().isoformat()
    keys = account.keys() if hasattr(account, "keys") else account
    tz_name = ""
    if "timezone_name" in keys:
        tz_name = account["timezone_name"] or ""
    if not tz_name and "timezone" in keys:
        tz_name = account["timezone"] or ""
    if tz_name and ZoneInfo:
        try:
            return datetime.now(ZoneInfo(str(tz_name))).date().isoformat()
        except Exception:
            pass
    try:
        if "timezone_offset_hours_utc" in keys and account["timezone_offset_hours_utc"] not in (None, ""):
            return (datetime.utcnow() + timedelta(hours=float(account["timezone_offset_hours_utc"]))).date().isoformat()
    except Exception:
        pass
    return date.today().isoformat()


def _ensure_guard_allowance_schema(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS guard_ad_allowances (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           act_id TEXT NOT NULL,
           ad_id TEXT NOT NULL,
           allowance_date TEXT NOT NULL,
           reason TEXT,
           status TEXT DEFAULT 'active',
           team_id INTEGER,
           owner_user_id INTEGER,
           created_by TEXT,
           created_at TEXT DEFAULT (datetime('now','+8 hours')),
           updated_at TEXT DEFAULT (datetime('now','+8 hours')),
           UNIQUE(act_id, ad_id, allowance_date)
        )"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_guard_ad_allowances_lookup
           ON guard_ad_allowances(act_id, ad_id, allowance_date, status)"""
    )
    conn.commit()


def _active_guard_allowance(conn, account, ad_id: str, allowance_date: str | None = None):
    _ensure_guard_allowance_schema(conn)
    if not account or not ad_id:
        return None
    act_id = _allowance_norm_act_id(account["act_id"])
    plain_act = _allowance_plain_id(act_id)
    day = allowance_date or _account_local_date(account)
    row = conn.execute(
        """SELECT * FROM guard_ad_allowances
           WHERE status='active'
             AND allowance_date=?
             AND ad_id=?
             AND (act_id=? OR act_id=?)
           ORDER BY updated_at DESC, id DESC
           LIMIT 1""",
        (day, _normalize_ad_id(ad_id), act_id, plain_act),
    ).fetchone()
    return row


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _table_columns(conn, table: str) -> set[str]:
    try:
        return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _guard_ad_lookup_candidates(conn, ad_id: str, user) -> list[dict]:
    ad_id = _normalize_ad_id(ad_id)
    if not ad_id:
        return []
    candidates: dict[str, dict] = {}

    def add_candidate(act_id, source, ad_name="", seen_at="", account_name=""):
        act = _allowance_norm_act_id(act_id)
        if not act:
            return
        item = candidates.setdefault(act, {
            "act_id": act,
            "account_name": account_name or act,
            "ad_id": ad_id,
            "ad_name": ad_name or "",
            "sources": [],
            "seen_at": "",
            "account_exists": False,
            "accessible": False,
        })
        if source and source not in item["sources"]:
            item["sources"].append(source)
        if ad_name and not item["ad_name"]:
            item["ad_name"] = ad_name
        if account_name and item["account_name"] == act:
            item["account_name"] = account_name
        if seen_at and (not item["seen_at"] or str(seen_at) > str(item["seen_at"])):
            item["seen_at"] = str(seen_at)

    if _table_exists(conn, "perf_snapshots"):
        cols = _table_columns(conn, "perf_snapshots")
        seen_expr = "p.snapshot_at" if "snapshot_at" in cols else ("p.updated_at" if "updated_at" in cols else "p.snapshot_date")
        rows = conn.execute(
            f"""SELECT p.act_id, p.ad_name, p.snapshot_date, {seen_expr} AS seen_at,
                       COALESCE(a.name, p.act_id) AS account_name
                FROM perf_snapshots p
                LEFT JOIN accounts a ON a.act_id=p.act_id
                WHERE p.ad_id=?
                ORDER BY p.snapshot_date DESC, seen_at DESC
                LIMIT 20""",
            (ad_id,),
        ).fetchall()
        for r in rows:
            add_candidate(r["act_id"], "巡检快照", r["ad_name"], r["seen_at"] or r["snapshot_date"], r["account_name"])

    if _table_exists(conn, "perf_snapshot_history"):
        rows = conn.execute(
            """SELECT h.act_id, h.ad_name, h.snapshot_at AS seen_at,
                      COALESCE(a.name, h.act_id) AS account_name
               FROM perf_snapshot_history h
               LEFT JOIN accounts a ON a.act_id=h.act_id
               WHERE h.ad_id=?
               ORDER BY h.snapshot_at DESC
               LIMIT 20""",
            (ad_id,),
        ).fetchall()
        for r in rows:
            add_candidate(r["act_id"], "小时历史", r["ad_name"], r["seen_at"], r["account_name"])

    if _table_exists(conn, "inspect_cache"):
        rows = conn.execute(
            """SELECT c.act_id, c.data, c.updated_at AS seen_at,
                      COALESCE(a.name, c.act_id) AS account_name
               FROM inspect_cache c
               LEFT JOIN accounts a ON a.act_id=c.act_id
               WHERE c.ad_id=?
               ORDER BY c.updated_at DESC
               LIMIT 20""",
            (ad_id,),
        ).fetchall()
        for r in rows:
            ad_name = ""
            try:
                payload = json.loads(r["data"] or "{}")
                ad_name = payload.get("ad_name") or payload.get("name") or ""
            except Exception:
                pass
            add_candidate(r["act_id"], "巡检缓存", ad_name, r["seen_at"], r["account_name"])

    if _table_exists(conn, "kpi_configs"):
        cols = _table_columns(conn, "kpi_configs")
        seen_expr = "k.updated_at" if "updated_at" in cols else "k.created_at"
        target_name_expr = "k.target_name" if "target_name" in cols else "k.target_id"
        rows = conn.execute(
            f"""SELECT k.act_id, {target_name_expr} AS ad_name, {seen_expr} AS seen_at,
                      COALESCE(a.name, k.act_id) AS account_name
               FROM kpi_configs k
               LEFT JOIN accounts a ON a.act_id=k.act_id
               WHERE k.target_id=? AND COALESCE(k.level,'ad')='ad'
               ORDER BY seen_at DESC
               LIMIT 20""",
            (ad_id,),
        ).fetchall()
        for r in rows:
            add_candidate(r["act_id"], "KPI配置", r["ad_name"], r["seen_at"], r["account_name"])

    if _table_exists(conn, "action_logs"):
        rows = conn.execute(
            """SELECT l.act_id, l.target_name AS ad_name, l.created_at AS seen_at,
                      COALESCE(a.name, l.act_id) AS account_name
               FROM action_logs l
               LEFT JOIN accounts a ON a.act_id=l.act_id
               WHERE l.target_id=? OR l.trigger_detail LIKE ?
               ORDER BY l.created_at DESC
               LIMIT 30""",
            (ad_id, f"%{ad_id}%"),
        ).fetchall()
        for r in rows:
            add_candidate(r["act_id"], "操作日志", r["ad_name"], r["seen_at"], r["account_name"])

    if _table_exists(conn, "landing_ad_links"):
        cols = _table_columns(conn, "landing_ad_links")
        seen_expr = "l.updated_at" if "updated_at" in cols else ("l.created_at" if "created_at" in cols else "''")
        ad_name_expr = "l.ad_name" if "ad_name" in cols else "l.ad_id"
        rows = conn.execute(
            f"""SELECT l.act_id, {ad_name_expr} AS ad_name, {seen_expr} AS seen_at,
                      COALESCE(a.name, l.act_id) AS account_name
               FROM landing_ad_links l
               LEFT JOIN accounts a ON a.act_id=l.act_id
               WHERE l.ad_id=?
               ORDER BY seen_at DESC
               LIMIT 20""",
            (ad_id,),
        ).fetchall()
        for r in rows:
            add_candidate(r["act_id"], "广告链接", r["ad_name"], r["seen_at"], r["account_name"])

    if _table_exists(conn, "auto_campaign_ads"):
        cols = _table_columns(conn, "auto_campaign_ads")
        if "fb_ad_id" in cols:
            seen_expr = "aca.updated_at" if "updated_at" in cols else ("aca.created_at" if "created_at" in cols else "''")
            ad_name_expr = "aca.ad_name" if "ad_name" in cols else "aca.fb_ad_id"
            rows = conn.execute(
                f"""SELECT aca.act_id, {ad_name_expr} AS ad_name, {seen_expr} AS seen_at,
                          COALESCE(a.name, aca.act_id) AS account_name
                   FROM auto_campaign_ads aca
                   LEFT JOIN accounts a ON a.act_id=aca.act_id
                   WHERE aca.fb_ad_id=?
                   ORDER BY seen_at DESC
                   LIMIT 20""",
                (ad_id,),
            ).fetchall()
            for r in rows:
                add_candidate(r["act_id"], "创建记录", r["ad_name"], r["seen_at"], r["account_name"])

    items = []
    for item in candidates.values():
        row = _account_row(conn, item["act_id"])
        item["account_exists"] = bool(row)
        if row:
            item["account_name"] = row["name"] if "name" in row.keys() and row["name"] else item["account_name"]
            try:
                _assert_rule_target_access(conn, item["act_id"], user)
                item["accessible"] = True
            except HTTPException:
                item["accessible"] = False
        else:
            item["accessible"] = bool(is_superadmin(user))
        items.append(item)
    items.sort(key=lambda x: (bool(x["accessible"]), bool(x["account_exists"]), str(x["seen_at"] or "")), reverse=True)
    return items


def _rule_scope_for_body(conn, act_id: str | None, user) -> tuple[str, str, int | None, int | None]:
    _ensure_rule_scope_columns(conn)
    target = (act_id or "").strip()
    if target in ("", GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID, "__all__"):
        if is_superadmin(user):
            raise HTTPException(400, "超级管理员不创建默认规则；请由运营在自己的账户范围内配置。")
        uid = _actor_uid(user)
        if not uid:
            raise HTTPException(403, "当前用户不能创建运营级规则")
        return OWNER_SCOPE_ACT_ID, RULE_SCOPE_OWNER, uid, team_id_for_create(user)

    _assert_rule_target_access(conn, target, user)
    account = _account_row(conn, target)
    if not account:
        raise HTTPException(404, "账户不存在")
    if not _actor_can_manage_team(user):
        uid = _actor_uid(user)
        owner_id = account["owner_user_id"] if "owner_user_id" in account.keys() else None
        if owner_id and uid and int(owner_id) != uid:
            raise HTTPException(403, "只能为自己负责的账户配置规则")
    return target, RULE_SCOPE_ACCOUNT, _actor_uid(user), account["team_id"]


def _rule_row_accessible(conn, row, user) -> bool:
    if is_superadmin(user):
        return True
    team_id = team_id_for_create(user)
    row_team = row["team_id"] if "team_id" in row.keys() else None
    if row_team is not None and int(row_team) != int(team_id):
        return False
    scope = (row["scope"] if "scope" in row.keys() else None) or RULE_SCOPE_ACCOUNT
    uid = _actor_uid(user)
    if scope == RULE_SCOPE_OWNER:
        owner = row["owner_user_id"] if "owner_user_id" in row.keys() else None
        return bool(owner and uid and (int(owner) == int(uid) or _actor_can_manage_team(user)))
    if not _actor_can_manage_team(user):
        owner = row["owner_user_id"] if "owner_user_id" in row.keys() else None
        if owner and uid and int(owner) != int(uid):
            return False
    return True


def _assert_rule_row_access(conn, row, user) -> None:
    if not _rule_row_accessible(conn, row, user):
        raise HTTPException(403, "规则不属于当前用户或团队")


def _team_account_act_ids(conn, user) -> list[str]:
    if not is_superadmin(user) and (user or {}).get("role") not in ("admin", "superadmin"):
        uid = _actor_uid(user)
        team_id = team_id_for_create(user)
        if not uid:
            return []
        rows = conn.execute(
            "SELECT act_id FROM accounts WHERE team_id=? AND owner_user_id=?",
            (team_id, uid),
        ).fetchall()
        return [r["act_id"] for r in rows if r["act_id"]]
    where, params = [], []
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(f"SELECT act_id FROM accounts {clause}", params).fetchall()
    return [r["act_id"] for r in rows if r["act_id"]]


def _target_cpa_preview(conn, act_id: str) -> list[dict]:
    if not act_id or act_id in (GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID):
        return []
    try:
        rows = conn.execute(
            """SELECT level, target_id, kpi_field, kpi_label, target_cpa
               FROM kpi_configs
               WHERE act_id=? AND enabled=1
                 AND target_cpa IS NOT NULL AND target_cpa > 0
               ORDER BY CASE level
                   WHEN 'ad' THEN 1
                   WHEN 'adset' THEN 2
                   WHEN 'campaign' THEN 3
                   WHEN 'account' THEN 4
                   ELSE 5 END,
                   updated_at DESC
               LIMIT 5""",
            (act_id,),
        ).fetchall()
    except Exception:
        return []

    result = []
    seen = set()
    for row in rows:
        key = (row["kpi_field"], row["target_cpa"])
        if key in seen:
            continue
        seen.add(key)
        result.append(dict(row))
        if len(result) >= 3:
            break
    return result


def _guard_rule_row_or_404(conn, rule_id: int):
    _ensure_rule_scope_columns(conn)
    row = conn.execute("SELECT * FROM guard_rules WHERE id=?", (rule_id,)).fetchone()
    if not row:
        raise HTTPException(404, "规则不存在")
    return row


def _scale_rule_row_or_404(conn, rule_id: int):
    _ensure_rule_scope_columns(conn)
    row = conn.execute("SELECT * FROM scale_rules WHERE id=?", (rule_id,)).fetchone()
    if not row:
        raise HTTPException(404, "拉量策略不存在")
    return row


class GuardRuleIn(BaseModel):
    act_id: Optional[str] = None
    rule_name: Optional[str] = None
    level: str = "ad"
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


class GuardAdAllowanceIn(BaseModel):
    act_id: str
    ad_id: Optional[str] = None
    ad_ids: Optional[List[str]] = None
    allowance_date: Optional[str] = None
    reason: Optional[str] = None

class ScaleRuleIn(BaseModel):
    act_id: Optional[str] = None
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
    _ensure_rule_scope_columns(conn)
    if act_id and act_id not in (GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID):
        _assert_rule_target_access(conn, act_id, user)
        account = _account_row(conn, act_id)
        owner_id = account["owner_user_id"] if account and "owner_user_id" in account.keys() else None
        params = [act_id]
        owner_sql = ""
        if owner_id:
            owner_sql = " OR (scope=? AND owner_user_id=?)"
            params.extend([RULE_SCOPE_OWNER, owner_id])
        rows = conn.execute(
            f"""SELECT * FROM guard_rules
                WHERE ((COALESCE(scope,'account')=? AND act_id=?){owner_sql})
                ORDER BY id DESC""",
            [RULE_SCOPE_ACCOUNT] + params,
        ).fetchall()
    else:
        if is_superadmin(user):
            rows = []
        elif (user or {}).get("role") == "admin":
            team_id = team_id_for_create(user)
            rows = conn.execute(
                "SELECT * FROM guard_rules WHERE team_id=? ORDER BY id DESC",
                (team_id,),
            ).fetchall()
        else:
            uid = _actor_uid(user)
            account_ids = _team_account_act_ids(conn, user)
            rows = []
            if account_ids:
                placeholders = ",".join("?" for _ in account_ids)
                rows.extend(conn.execute(
                    f"""SELECT * FROM guard_rules
                        WHERE COALESCE(scope,'account')=? AND act_id IN ({placeholders})
                        ORDER BY id DESC""",
                    [RULE_SCOPE_ACCOUNT] + account_ids,
                ).fetchall())
            if uid:
                rows.extend(conn.execute(
                    "SELECT * FROM guard_rules WHERE scope=? AND owner_user_id=? ORDER BY id DESC",
                    (RULE_SCOPE_OWNER, uid),
                ).fetchall())
    result = []
    seen = set()
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        d = dict(r)
        d["scope"] = d.get("scope") or RULE_SCOPE_ACCOUNT
        d["scope_label"] = "名下全部账户" if d["scope"] == RULE_SCOPE_OWNER else "指定账户"
        d["target_cpa_preview"] = _target_cpa_preview(conn, d.get("act_id", ""))
        result.append(d)
    conn.close()
    return result


@router.post("/guard")
def add_guard_rule(body: GuardRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    act_id, scope, owner_user_id, team_id = _rule_scope_for_body(conn, body.act_id, user)
    cur = conn.execute(
        """INSERT INTO guard_rules
           (act_id, rule_name, level, target_id, rule_type,
            param_value, param_ratio, param_days,
            action, action_value, enabled,
            silent_start, silent_end, note, kpi_filter,
            scope, owner_user_id, team_id, created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (act_id, body.rule_name, body.level, body.target_id, body.rule_type,
         body.param_value, body.param_ratio, body.param_days,
         body.action, body.action_value, body.enabled,
         body.silent_start, body.silent_end, body.note, body.kpi_filter,
         scope, owner_user_id, team_id, (user or {}).get("username"))
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id, "message": "规则添加成功"}


@router.get("/guard/ad-account-lookup")
def lookup_guard_ad_account(ad_id: str, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        target = _normalize_ad_id(ad_id)
        if not target:
            raise HTTPException(400, "请填写广告 ID")
        matches = _guard_ad_lookup_candidates(conn, target, user)
        selectable = [m for m in matches if m.get("accessible") and m.get("account_exists")]
        unique_acts = []
        seen = set()
        for item in selectable:
            act = item.get("act_id")
            if act and act not in seen:
                seen.add(act)
                unique_acts.append(act)
        selected = None
        if len(unique_acts) == 1:
            selected = next((m for m in selectable if m.get("act_id") == unique_acts[0]), None)
        return {
            "success": True,
            "ad_id": target,
            "selected_act_id": selected.get("act_id") if selected else "",
            "selected_account_name": selected.get("account_name") if selected else "",
            "ambiguous": len(unique_acts) > 1,
            "matches": matches,
            "message": (
                "已匹配账户" if selected else
                ("广告 ID 匹配到多个账户，请手动选择" if len(unique_acts) > 1 else "未从本地缓存找到可用账户")
            ),
        }
    finally:
        conn.close()


@router.get("/guard/allowances")
def list_guard_ad_allowances(
    act_id: Optional[str] = None,
    allowance_date: Optional[str] = None,
    user=Depends(get_current_user),
):
    conn = get_conn()
    try:
        _ensure_guard_allowance_schema(conn)
        target = _allowance_norm_act_id(act_id)
        if not target:
            return {"success": True, "items": [], "message": "请选择账户后查看当日放行。"}
        _assert_rule_target_access(conn, target, user)
        account = _account_row(conn, target)
        if not account:
            raise HTTPException(404, "账户不存在")
        day = (allowance_date or "").strip() or _account_local_date(account)
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
            raise HTTPException(400, "日期格式必须为 YYYY-MM-DD")
        rows = conn.execute(
            """SELECT * FROM guard_ad_allowances
               WHERE status='active' AND allowance_date=?
                 AND (act_id=? OR act_id=?)
               ORDER BY updated_at DESC, id DESC""",
            (day, target, _allowance_plain_id(target)),
        ).fetchall()
        items = [dict(r) for r in rows]
        for item in items:
            item["account_name"] = account["name"] if "name" in account.keys() else target
        return {"success": True, "act_id": target, "allowance_date": day, "items": items}
    finally:
        conn.close()


@router.post("/guard/allowances")
def save_guard_ad_allowances(body: GuardAdAllowanceIn, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _ensure_guard_allowance_schema(conn)
        act_id = _allowance_norm_act_id(body.act_id)
        if not act_id:
            raise HTTPException(400, "请选择账户")
        _assert_rule_target_access(conn, act_id, user)
        account = _account_row(conn, act_id)
        if not account:
            raise HTTPException(404, "账户不存在")
        day = (body.allowance_date or "").strip() or _account_local_date(account)
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
            raise HTTPException(400, "日期格式必须为 YYYY-MM-DD")
        raw_ids = []
        if body.ad_id:
            raw_ids.append(body.ad_id)
        if body.ad_ids:
            raw_ids.extend(body.ad_ids)
        ad_ids = []
        seen = set()
        for raw in raw_ids:
            ad_id = _normalize_ad_id(raw)
            if ad_id and ad_id not in seen:
                seen.add(ad_id)
                ad_ids.append(ad_id)
        if not ad_ids:
            raise HTTPException(400, "请填写广告 ID")
        team_id = account["team_id"] if "team_id" in account.keys() else None
        owner_user_id = account["owner_user_id"] if "owner_user_id" in account.keys() else None
        created_by = (user or {}).get("username") or ""
        reason = (body.reason or "").strip()
        saved = []
        for ad_id in ad_ids:
            old = conn.execute(
                "SELECT id FROM guard_ad_allowances WHERE act_id=? AND ad_id=? AND allowance_date=?",
                (act_id, ad_id, day),
            ).fetchone()
            if old:
                conn.execute(
                    """UPDATE guard_ad_allowances
                       SET status='active', reason=?, team_id=?, owner_user_id=?,
                           created_by=?, updated_at=datetime('now','+8 hours')
                       WHERE id=?""",
                    (reason, team_id, owner_user_id, created_by, old["id"]),
                )
                saved.append(old["id"])
            else:
                cur = conn.execute(
                    """INSERT INTO guard_ad_allowances
                       (act_id, ad_id, allowance_date, reason, status, team_id, owner_user_id, created_by)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (act_id, ad_id, day, reason, "active", team_id, owner_user_id, created_by),
                )
                saved.append(cur.lastrowid)
        conn.commit()
        return {
            "success": True,
            "act_id": act_id,
            "allowance_date": day,
            "count": len(saved),
            "ids": saved,
            "message": f"已放行 {len(saved)} 条广告，周期为账户日期 {day}",
        }
    finally:
        conn.close()


@router.delete("/guard/allowances/{allowance_id}")
def revoke_guard_ad_allowance(allowance_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        _ensure_guard_allowance_schema(conn)
        row = conn.execute("SELECT * FROM guard_ad_allowances WHERE id=?", (allowance_id,)).fetchone()
        if not row:
            raise HTTPException(404, "放行记录不存在")
        act_id = _allowance_norm_act_id(row["act_id"])
        _assert_rule_target_access(conn, act_id, user)
        conn.execute(
            "UPDATE guard_ad_allowances SET status='revoked', updated_at=datetime('now','+8 hours') WHERE id=?",
            (allowance_id,),
        )
        conn.commit()
        return {"success": True}
    finally:
        conn.close()


@router.put("/guard/{rule_id}")
def update_guard_rule(rule_id: int, body: GuardRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    old = _guard_rule_row_or_404(conn, rule_id)
    _assert_rule_row_access(conn, old, user)
    act_id, scope, owner_user_id, team_id = _rule_scope_for_body(conn, body.act_id, user)
    conn.execute(
        """UPDATE guard_rules SET
           act_id=?, rule_name=?, level=?, target_id=?, rule_type=?,
           param_value=?, param_ratio=?, param_days=?,
           action=?, action_value=?, enabled=?,
           silent_start=?, silent_end=?, note=?, kpi_filter=?,
           scope=?, owner_user_id=?, team_id=?, created_by=?,
           updated_at=datetime('now')
           WHERE id=?""",
        (act_id, body.rule_name, body.level, body.target_id, body.rule_type,
         body.param_value, body.param_ratio, body.param_days,
         body.action, body.action_value, body.enabled,
         body.silent_start, body.silent_end, body.note,
         getattr(body, "kpi_filter", None),
         scope, owner_user_id, team_id, (user or {}).get("username"), rule_id)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "规则更新成功"}


@router.patch("/guard/{rule_id}/toggle")
def toggle_guard_rule(rule_id: int, user=Depends(get_current_user)):
    """快速启用/禁用规则"""
    conn = get_conn()
    row = _guard_rule_row_or_404(conn, rule_id)
    _assert_rule_row_access(conn, row, user)
    conn.execute("UPDATE guard_rules SET enabled = 1 - enabled WHERE id=?", (rule_id,))
    row = conn.execute("SELECT enabled FROM guard_rules WHERE id=?", (rule_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "enabled": row["enabled"] if row else None}


@router.delete("/guard/{rule_id}")
def delete_guard_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _guard_rule_row_or_404(conn, rule_id)
    _assert_rule_row_access(conn, row, user)
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
            "desc": "消耗 ≥ $止血线 且 零转化 → 暂停。例如设置$20，当广告消费≥$20且无转化时触发。适合快速阻断无效消耗。",
            "params": [
                {"key": "param_value", "label": "止血金额(USD)", "type": "number", "default": 20, "required": True}
            ]
        },
        {
            "value": "cpa_exceed",
            "label": "CPA超标止损",
            "desc": "CPA > 目标×超标倍数 → 暂停。例如目标$10、倍数1.3，当CPA>$13时触发。设置「目标CPA」后按此逻辑判断。",
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
            "desc": "ROAS跌幅 > X% → 熔断暂停。例如设置40%，当ROAS比昨天下跌超过40%时触发。趋势逆转时自动保护。",
            "params": [
                {"key": "param_value", "label": "跌幅阈值(%)", "type": "number", "default": 40, "required": True}
            ]
        },
        {
            "value": "consecutive_bad",
            "label": "连续恶化止损",
            "desc": "连续N天CPA > 目标×倍数 → 暂停。例如2天、1.3倍，则连续2天实际CPA超过目标×1.3时触发。防止持续恶化。",
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
            "desc": "点击 ≥ X 且 零转化 → 预警。例如设置100，点击≥100次但无转化时发送通知。过滤无效点击流量。",
            "params": [
                {"key": "param_value", "label": "点击数阈值", "type": "number", "default": 100, "required": True}
            ]
        },
        {
            "value": "low_ctr_no_conv",
            "label": "低CTR空转止损",
            "desc": "消耗 ≥ $X + CTR ≤ Y% + 零转化 → 暂停。例如$10+0.5%，消费≥$10且CTR≤0.5%且无转化时触发。剔除低质流量。",
            "params": [
                {"key": "param_value", "label": "最低消耗(USD)", "type": "number", "default": 10, "required": True},
                {"key": "param_ratio", "label": "最高CTR(%)", "type": "number", "default": 0.5, "required": True}
            ]
        },
        {
            "value": "reach_no_conv",
            "label": "高覆盖无转化止损",
            "desc": "覆盖 ≥ X + 消耗 ≥ $Y + 零转化 → 暂停。例如1000+$10，覆盖≥1000、消费≥$10且无转化时触发。大曝光零转化止损。",
            "params": [
                {"key": "param_value", "label": "覆盖人数阈值", "type": "number", "default": 1000, "required": True},
                {"key": "param_ratio", "label": "最低消耗(USD)", "type": "number", "default": 10, "required": True}
            ]
        },
        {
            "value": "budget_burn_fast",
            "label": "瞬烧制止",
            "desc": "单次巡检消耗增量 > $X → 暂停。例如$20，两次巡检间消耗增加超过$20时触发。防止预算异常快速消耗。",
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
            "desc": "CPA≤目标80%+连续2天+≥3转化 → 每次+15%预算",
            "defaults": {"cpa_ratio": 0.8, "min_conversions": 3, "consecutive_days": 2, "scale_pct": 0.15}
        },
        {
            "value": "fast_scale",
            "label": "快速拉量",
            "desc": "CPA≤目标70%+连续1天+≥5转化 → 每次+25%预算",
            "defaults": {"cpa_ratio": 0.7, "min_conversions": 5, "consecutive_days": 1, "scale_pct": 0.25}
        },
        {
            "value": "roas_scale",
            "label": "ROAS 拉量",
            "desc": "CPA≤目标90%+ROAS≥3.0+≥3转化 → 每次+20%预算",
            "defaults": {"cpa_ratio": 0.9, "min_conversions": 3, "consecutive_days": 1, "scale_pct": 0.2, "roas_threshold": 3.0}
        },
    ]

    return {"guard_types": guard_types, "scale_types": scale_types, "actions": actions}


@router.get("/scale")
def list_scale_rules(act_id: Optional[str] = None, user=Depends(get_current_user)):
    conn = get_conn()
    _ensure_rule_scope_columns(conn)
    if act_id and act_id not in (GLOBAL_ACT_ID, OWNER_SCOPE_ACT_ID):
        _assert_rule_target_access(conn, act_id, user)
        account = _account_row(conn, act_id)
        owner_id = account["owner_user_id"] if account and "owner_user_id" in account.keys() else None
        params = [RULE_SCOPE_ACCOUNT, act_id]
        owner_sql = ""
        if owner_id:
            owner_sql = " OR (scope=? AND owner_user_id=?)"
            params.extend([RULE_SCOPE_OWNER, owner_id])
        rows = conn.execute(
            f"""SELECT * FROM scale_rules
                WHERE ((COALESCE(scope,'account')=? AND act_id=?){owner_sql})
                ORDER BY id DESC""",
            params,
        ).fetchall()
    else:
        if is_superadmin(user):
            rows = []
        elif (user or {}).get("role") == "admin":
            team_id = team_id_for_create(user)
            rows = conn.execute(
                "SELECT * FROM scale_rules WHERE team_id=? ORDER BY id DESC",
                (team_id,),
            ).fetchall()
        else:
            uid = _actor_uid(user)
            account_ids = _team_account_act_ids(conn, user)
            rows = []
            if account_ids:
                placeholders = ",".join("?" for _ in account_ids)
                rows.extend(conn.execute(
                    f"""SELECT * FROM scale_rules
                        WHERE COALESCE(scope,'account')=? AND act_id IN ({placeholders})
                        ORDER BY id DESC""",
                    [RULE_SCOPE_ACCOUNT] + account_ids,
                ).fetchall())
            if uid:
                rows.extend(conn.execute(
                    "SELECT * FROM scale_rules WHERE scope=? AND owner_user_id=? ORDER BY id DESC",
                    (RULE_SCOPE_OWNER, uid),
                ).fetchall())
    result = []
    seen = set()
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        d = dict(r)
        d["scope"] = d.get("scope") or RULE_SCOPE_ACCOUNT
        d["scope_label"] = "名下全部账户" if d["scope"] == RULE_SCOPE_OWNER else "指定账户"
        d["target_cpa_preview"] = _target_cpa_preview(conn, d.get("act_id", ""))
        result.append(d)
    conn.close()
    return result


@router.post("/scale")
def add_scale_rule(body: ScaleRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    act_id, scope, owner_user_id, team_id = _rule_scope_for_body(conn, body.act_id, user)
    cur = conn.execute(
        """INSERT INTO scale_rules
           (act_id, rule_name, rule_type, cpa_ratio, min_conversions,
            consecutive_days, scale_pct, max_budget, roas_threshold,
            target_regions, enabled, note, scope, owner_user_id, team_id, created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (act_id, body.rule_name, body.rule_type, body.cpa_ratio,
         body.min_conversions, body.consecutive_days, body.scale_pct,
         body.max_budget, body.roas_threshold, body.target_regions,
         body.enabled, body.note, scope, owner_user_id, team_id, (user or {}).get("username"))
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id, "message": "拉量策略添加成功"}


@router.put("/scale/{rule_id}")
def update_scale_rule(rule_id: int, body: ScaleRuleIn, user=Depends(get_current_user)):
    conn = get_conn()
    old = _scale_rule_row_or_404(conn, rule_id)
    _assert_rule_row_access(conn, old, user)
    act_id, scope, owner_user_id, team_id = _rule_scope_for_body(conn, body.act_id, user)
    conn.execute(
        """UPDATE scale_rules SET
           act_id=?, rule_name=?, rule_type=?, cpa_ratio=?,
           min_conversions=?, consecutive_days=?, scale_pct=?,
           max_budget=?, roas_threshold=?, target_regions=?,
           enabled=?, note=?, scope=?, owner_user_id=?, team_id=?, created_by=?
           WHERE id=?""",
        (act_id, body.rule_name, body.rule_type, body.cpa_ratio,
         body.min_conversions, body.consecutive_days, body.scale_pct,
         body.max_budget, body.roas_threshold, body.target_regions,
         body.enabled, body.note, scope, owner_user_id, team_id, (user or {}).get("username"), rule_id)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "拉量策略更新成功"}


@router.patch("/scale/{rule_id}/toggle")
def toggle_scale_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _scale_rule_row_or_404(conn, rule_id)
    _assert_rule_row_access(conn, row, user)
    conn.execute("UPDATE scale_rules SET enabled = 1 - enabled WHERE id=?", (rule_id,))
    row = conn.execute("SELECT enabled FROM scale_rules WHERE id=?", (rule_id,)).fetchone()
    conn.commit()
    conn.close()
    return {"success": True, "enabled": row["enabled"] if row else None}


@router.delete("/scale/{rule_id}")
def delete_scale_rule(rule_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = _scale_rule_row_or_404(conn, rule_id)
    _assert_rule_row_access(conn, row, user)
    conn.execute("DELETE FROM scale_rules WHERE id=?", (rule_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ─── 规则模板 API ────────────────────────────────────────────────────────
from pydantic import BaseModel

RULE_TEMPLATES = [
    {
        "id": "default_stoploss",
        "name": "默认止损规则",
        "desc": "一条主要成效空成效止损：每个广告按自己的 KPI 字段判断，消耗达到 $20 且主要成效为 0 时关闭广告。",
        "tags": ["止损", "默认"],
        "guard_rules": [
            {"rule_name": "主要成效 $20 空成效止损", "rule_type": "bleed_abs", "level": "ad", "target_id": "__global__", "param_value": 20.0, "action": "pause", "kpi_filter": "primary", "note": "来自默认止损规则"},
        ],
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

class RuleDryRunRequest(BaseModel):
    act_id: Optional[str] = None
    target_id: Optional[str] = None
    target_level: Optional[str] = "auto"
    max_ads: Optional[int] = 80

class ApplyTemplateRequest(BaseModel):
    template_id: str
    act_id: Optional[str] = None          # 单账户（向下兼容）
    act_ids: Optional[List[str]] = None   # 多账户批量应用
    override_existing: bool = False        # 是否覆盖已有规则
    global_mode: bool = False              # Legacy field; global account rules are disabled.

def _plain_id(value: str | None) -> str:
    raw = (value or "").strip()
    return raw[4:] if raw.startswith("act_") else raw


def _normalize_account_id(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("act_") else f"act_{raw}"


def _same_fb_id(left: str | None, right: str | None) -> bool:
    return bool(left and right and _plain_id(str(left)) == _plain_id(str(right)))


def _visible_dry_run_accounts(conn, user, act_id: str | None) -> list[dict]:
    target = _normalize_account_id(act_id)
    if target:
        assert_row_access(conn, "accounts", target, user, id_column="act_id")
        row = _account_row(conn, target)
        return [dict(row)] if row else []
    ids = _team_account_act_ids(conn, user)
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    select_cols = ["act_id", "name", "currency"]
    for col in ("timezone", "timezone_name", "timezone_offset_hours_utc"):
        if col in cols:
            select_cols.append(col)
    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM accounts WHERE act_id IN ({placeholders}) ORDER BY id",
        ids,
    ).fetchall()
    return [dict(r) for r in rows]


def _dry_run_target_matches(ad: dict, account: dict, level: str, target_id: str) -> bool:
    if not target_id:
        return True
    level = (level or "auto").lower()
    if level == "account":
        return _same_fb_id(account.get("act_id"), target_id)
    if level == "campaign":
        return _same_fb_id(ad.get("campaign_id"), target_id)
    if level == "adset":
        return _same_fb_id(ad.get("adset_id"), target_id)
    if level == "ad":
        return _same_fb_id(ad.get("ad_id"), target_id)
    return (
        _same_fb_id(account.get("act_id"), target_id)
        or _same_fb_id(ad.get("campaign_id"), target_id)
        or _same_fb_id(ad.get("adset_id"), target_id)
        or _same_fb_id(ad.get("ad_id"), target_id)
    )


def _rule_action_label(rule: dict) -> str:
    action = rule.get("action") or "pause"
    if action == "pause":
        return "暂停广告；失败时自动升级到广告组/系列"
    if action == "pause_adset":
        return "暂停广告组"
    if action == "pause_campaign":
        return "暂停系列"
    if action == "alert_only":
        return "仅预警"
    if action == "reduce_budget":
        return "降低预算"
    return str(action)


def _dry_run_rule_summary(rule: dict) -> dict:
    out = {
        "id": rule.get("id"),
        "rule_type": rule.get("rule_type"),
        "kpi_filter": rule.get("kpi_filter"),
        "param_value": rule.get("param_value"),
        "param_ratio": rule.get("param_ratio"),
        "action": rule.get("action"),
        "action_label": _rule_action_label(rule),
        "scope": rule.get("scope"),
        "note": rule.get("note"),
        "would_trigger": bool(rule.get("would_trigger")),
        "in_cooldown": bool(rule.get("in_cooldown")),
        "cooldown_remaining_sec": int(rule.get("cooldown_remaining_sec") or 0),
    }
    for key in ("threshold", "actual_cpa", "threshold_spend", "actual_spend", "actual_conversions"):
        if key in rule:
            out[key] = rule.get(key)
    return out


@router.post("/dry-run")
def dry_run_rules(body: RuleDryRunRequest, user=Depends(get_current_user)):
    """Preview rule decisions for a selected account/campaign/adset/ad without writing to Meta."""
    from datetime import date
    from api.dashboard import get_ads_live
    from api.kpi import diagnose_ad

    target_id = (body.target_id or "").strip()
    target_level = (body.target_level or "auto").strip().lower()
    if target_level not in ("auto", "account", "campaign", "adset", "ad"):
        raise HTTPException(400, "target_level must be auto/account/campaign/adset/ad")
    if not (body.act_id or target_id):
        raise HTTPException(400, "请至少填写账户ID或目标ID")
    max_ads = max(1, min(int(body.max_ads or 80), 300))

    act_id = _normalize_account_id(body.act_id)
    if not act_id and target_level == "account" and target_id:
        act_id = _normalize_account_id(target_id)

    conn = get_conn()
    try:
        accounts = _visible_dry_run_accounts(conn, user, act_id)
    finally:
        conn.close()
    if not accounts:
        raise HTTPException(404, "没有可访问的账户")

    today = date.today().isoformat()
    candidates = []
    scan_errors = []
    for account in accounts:
        try:
            rows = get_ads_live(act_id=account["act_id"], date_from=today, date_to=today, user=user)
        except Exception as exc:
            scan_errors.append({
                "act_id": account["act_id"],
                "account_name": account.get("name") or account["act_id"],
                "error": str(exc)[:300],
            })
            continue
        for ad in rows or []:
            if _dry_run_target_matches(ad, account, target_level, target_id):
                candidates.append({"account": account, "ad": ad})
                if len(candidates) >= max_ads:
                    break
        if len(candidates) >= max_ads:
            break

    if not candidates:
        return {
            "success": True,
            "target": {"act_id": act_id, "target_id": target_id, "target_level": target_level},
            "scanned_accounts": len(accounts),
            "candidate_count": 0,
            "trigger_count": 0,
            "execute_count": 0,
            "items": [],
            "errors": scan_errors,
            "message": "没有找到匹配的广告。请确认 ID 是否存在、未删除，或补充账户 ID 后重试。",
        }

    cannot_spend = {"PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED", "DELETED", "ARCHIVED", "DISAPPROVED", "WITH_ISSUES"}
    items = []
    for item in candidates:
        account = item["account"]
        ad = item["ad"]
        try:
            diag = diagnose_ad(account["act_id"], ad["ad_id"], user=user)
        except Exception as exc:
            items.append({
                "act_id": account["act_id"],
                "account_name": account.get("name") or account["act_id"],
                "ad_id": ad.get("ad_id"),
                "ad_name": ad.get("ad_name") or ad.get("ad_id"),
                "status": "diagnose_failed",
                "error": str(exc)[:300],
            })
            continue
        rules = ((diag.get("rules") or {}).get("matching") or [])
        matched = [_dry_run_rule_summary(r) for r in rules]
        triggered = [r for r in matched if r.get("would_trigger")]
        effective_status = diag.get("effective_status") or ad.get("effective_status") or ""
        block_reasons = []
        if effective_status in cannot_spend:
            block_reasons.append("广告当前已停止投放，正式巡检会跳过")
        if any(r.get("in_cooldown") for r in triggered):
            block_reasons.append("命中规则仍在冷却中")
        allowance = None
        lookup_conn = get_conn()
        try:
            allowance = _active_guard_allowance(lookup_conn, account, diag.get("ad_id") or ad.get("ad_id"))
        finally:
            lookup_conn.close()
        if allowance:
            note = f"该广告已做当日放行，账户日期 {allowance['allowance_date']} 内正式巡检会跳过"
            if allowance["reason"]:
                note += f"：{allowance['reason']}"
            block_reasons.append(note)
        would_execute = bool(triggered) and not block_reasons
        items.append({
            "act_id": account["act_id"],
            "account_name": account.get("name") or account["act_id"],
            "currency": account.get("currency") or ad.get("currency") or "USD",
            "ad_id": diag.get("ad_id") or ad.get("ad_id"),
            "ad_name": diag.get("ad_name") or ad.get("ad_name") or ad.get("ad_id"),
            "adset_id": diag.get("adset_id") or ad.get("adset_id"),
            "campaign_id": diag.get("campaign_id") or ad.get("campaign_id"),
            "effective_status": effective_status,
            "spend": (diag.get("spend") or {}).get("spend"),
            "spend_raw": (diag.get("spend") or {}).get("spend_raw"),
            "conversions": (diag.get("conversions") or {}).get("count"),
            "matched_action": (diag.get("conversions") or {}).get("matched_action"),
            "cpa": diag.get("cpa"),
            "kpi": diag.get("kpi") or {},
            "matched_rules": matched,
            "triggered_rules": triggered,
            "would_execute": would_execute,
            "block_reasons": block_reasons,
            "allowance": dict(allowance) if allowance else None,
            "planned_actions": [_rule_action_label(r) for r in triggered],
        })

    trigger_count = sum(1 for item in items if item.get("triggered_rules"))
    execute_count = sum(1 for item in items if item.get("would_execute"))
    return {
        "success": True,
        "target": {"act_id": act_id, "target_id": target_id, "target_level": target_level},
        "scanned_accounts": len(accounts),
        "candidate_count": len(candidates),
        "trigger_count": trigger_count,
        "execute_count": execute_count,
        "items": items,
        "errors": scan_errors,
        "message": f"预演完成：匹配 {len(candidates)} 条广告，命中 {trigger_count} 条规则，正式巡检会处理 {execute_count} 条广告。",
    }


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
    # 确定目标账户列表：未选择账户时，创建“当前运营名下全部账户”的 owner 级规则。
    target_act_ids = []
    if body.act_ids and len(body.act_ids) > 0:
        target_act_ids = [a for a in body.act_ids if a]
    elif body.act_id:
        target_act_ids = [body.act_id]
    global_mode = getattr(body, 'global_mode', False)
    if global_mode:
        target_act_ids = []

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
    _ensure_rule_scope_columns(conn)
    total_guard = 0
    results = []
    try:
        targets = target_act_ids or [None]
        for target_act_id in targets:
            act_id, scope, owner_user_id, team_id = _rule_scope_for_body(conn, target_act_id, user)
            guard_added = 0
            if body.override_existing:
                if scope == RULE_SCOPE_OWNER:
                    conn.execute(
                        "DELETE FROM guard_rules WHERE scope=? AND owner_user_id=? AND (note LIKE '%默认止损规则%' OR note=?)",
                        (RULE_SCOPE_OWNER, owner_user_id, DEFAULT_OWNER_RULE_NOTE),
                    )
                else:
                    conn.execute(
                        "DELETE FROM guard_rules WHERE scope=? AND act_id=? AND (note LIKE '%默认止损规则%' OR note=?)",
                        (RULE_SCOPE_ACCOUNT, act_id, DEFAULT_OWNER_RULE_NOTE),
                    )
            for r in tpl.get('guard_rules', []):
                note = r.get('note', f'来自默认止损规则「{tpl.get("name", body.template_id)}」')
                if scope == RULE_SCOPE_OWNER and tpl.get("id") == "default_stoploss":
                    existing = conn.execute(
                        """SELECT id FROM guard_rules
                           WHERE scope=? AND owner_user_id=?
                             AND rule_type=? AND kpi_filter=?
                             AND (note LIKE '%默认止损规则%' OR note=?)
                           LIMIT 1""",
                        (RULE_SCOPE_OWNER, owner_user_id, r.get('rule_type'), r.get('kpi_filter'), DEFAULT_OWNER_RULE_NOTE),
                    ).fetchone()
                    if existing:
                        continue
                    note = DEFAULT_OWNER_RULE_NOTE
                conn.execute(
                    """INSERT INTO guard_rules
                       (act_id, rule_name, level, target_id, rule_type, param_value,
                        param_ratio, param_days, action, action_value, enabled, note,
                        kpi_filter, scope, owner_user_id, team_id, created_by)
                       VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?)""",
                    (act_id, r.get('rule_name'), r.get('level','ad'), r.get('target_id','__global__'),
                     r.get('rule_type'), r.get('param_value'), r.get('param_ratio',1.2),
                     r.get('param_days',2), r.get('action','pause'), r.get('action_value'),
                     note,
                     r.get('kpi_filter'), scope, owner_user_id, team_id, (user or {}).get("username"))
                )
                guard_added += 1
            total_guard += guard_added
            results.append({'act_id': act_id, 'scope': scope, 'guard_added': guard_added})
        conn.commit()
    finally:
        conn.close()
    acc_count = len(target_act_ids) if target_act_ids else 0
    scope_text = f"{acc_count} 个账户" if target_act_ids else "当前运营名下全部账户"
    return {
        'success': True,
        'template_name': tpl.get('name'),
        'account_count': acc_count,
        'total_guard_added': total_guard,
        'results': results,
        'message': f'已应用「{tpl.get("name")}」到 {scope_text}：共添加 {total_guard} 条止损规则'
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
