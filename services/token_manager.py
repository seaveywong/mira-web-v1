"""
Mira v3.0 Token 调度引擎 (TokenManager)
────────────────────────────────────────
职责：
  - 操作号优先：所有操作（包括 PAUSE）优先使用绑定在账户上的操作号 Token。
  - 多操作号轮询：按 priority 降序依次尝试，第一个有效的立即返回。
  - 管理号兜底：当所有操作号均失效时，仅允许 PAUSE 操作回退到管理号；
                CREATE / UPDATE 操作直接拒绝，绝不动用管理号。
  - 心跳缓存：每次验证结果缓存 10 分钟，避免频繁调用 FB API。
  - 自动降级告警：操作号全灭时通过 TG 推送告警。

使用方式：
  from services.token_manager import get_exec_token, ACTION_PAUSE, ACTION_CREATE, ACTION_UPDATE

  token = get_exec_token(act_id, ACTION_PAUSE)
  if not token:
      # 无可用 Token，操作被拒绝
      ...
"""

import time
import logging
import threading
import requests
from typing import Optional, Tuple

from core.database import get_conn, decrypt_token
from services.notifier import notify_account

logger = logging.getLogger("mira.token_manager")

TOKEN_SOURCE_SYSTEM_USER = "system_user"
TOKEN_SOURCE_PERSONAL = "personal"
TOKEN_SOURCE_PAGE = "page"
TOKEN_SOURCE_UNKNOWN = "unknown"
ALLOWED_TOKEN_SOURCES = {
    TOKEN_SOURCE_SYSTEM_USER,
    TOKEN_SOURCE_PERSONAL,
    TOKEN_SOURCE_PAGE,
    TOKEN_SOURCE_UNKNOWN,
}


def normalize_token_source(value, default: str = TOKEN_SOURCE_UNKNOWN) -> str:
    raw = str(value or "").strip().lower()
    alias_map = {
        "system": TOKEN_SOURCE_SYSTEM_USER,
        "systemuser": TOKEN_SOURCE_SYSTEM_USER,
        "system-user": TOKEN_SOURCE_SYSTEM_USER,
        "system_user": TOKEN_SOURCE_SYSTEM_USER,
        "su": TOKEN_SOURCE_SYSTEM_USER,
        "personal": TOKEN_SOURCE_PERSONAL,
        "person": TOKEN_SOURCE_PERSONAL,
        "human": TOKEN_SOURCE_PERSONAL,
        "user": TOKEN_SOURCE_PERSONAL,
        "page": TOKEN_SOURCE_PAGE,
        "page_token": TOKEN_SOURCE_PAGE,
        "unknown": TOKEN_SOURCE_UNKNOWN,
        "legacy": TOKEN_SOURCE_UNKNOWN,
    }
    normalized = alias_map.get(raw, raw or default)
    return normalized if normalized in ALLOWED_TOKEN_SOURCES else default


def default_token_source_for_type(token_type: Optional[str]) -> str:
    kind = str(token_type or "").strip().lower()
    if kind == "operate":
        return TOKEN_SOURCE_SYSTEM_USER
    if kind == "page":
        return TOKEN_SOURCE_PAGE
    return TOKEN_SOURCE_PERSONAL


def is_operate_token_eligible(token_type: Optional[str], token_source: Optional[str]) -> bool:
    return str(token_type or "").strip().lower() == "operate" and normalize_token_source(
        token_source,
        default_token_source_for_type(token_type),
    ) == TOKEN_SOURCE_SYSTEM_USER


def ensure_token_source_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(fb_tokens)").fetchall()}
    changed = False
    if "token_source" not in cols:
        conn.execute("ALTER TABLE fb_tokens ADD COLUMN token_source TEXT")
        changed = True
    if "team_id" not in cols:
        conn.execute("ALTER TABLE fb_tokens ADD COLUMN team_id INTEGER")
        changed = True
    if changed:
        conn.commit()
    conn.execute(
        """
        UPDATE fb_tokens
        SET token_source = CASE
            WHEN token_source IS NOT NULL AND TRIM(token_source) != '' THEN LOWER(TRIM(token_source))
            WHEN token_type = 'operate' THEN ?
            WHEN token_type = 'page' THEN ?
            ELSE ?
        END
        WHERE token_source IS NULL OR TRIM(token_source) = ''
        """,
        (TOKEN_SOURCE_SYSTEM_USER, TOKEN_SOURCE_PAGE, TOKEN_SOURCE_PERSONAL),
    )
    conn.execute(
        """
        UPDATE fb_tokens
        SET token_source = ?
        WHERE LOWER(TRIM(COALESCE(token_source, ''))) NOT IN ('system_user','personal','page','unknown')
        """,
        (TOKEN_SOURCE_UNKNOWN,),
    )
    conn.commit()


def _account_team_id(conn, act_id: str) -> Optional[int]:
    try:
        row = conn.execute("SELECT team_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
        return row["team_id"] if row and row["team_id"] is not None else None
    except Exception:
        return None


def _token_team_clause(account_team_id: Optional[int], alias: str = "t") -> tuple[str, list]:
    if account_team_id is None:
        return f" AND {alias}.team_id IS NULL", []
    return f" AND {alias}.team_id=?", [account_team_id]

# ── 操作类型常量 ──────────────────────────────────────────────────────────────
ACTION_PAUSE  = "PAUSE"   # 关闭广告（管理号可兜底）
ACTION_CREATE = "CREATE"  # 新建广告（仅操作号）
ACTION_UPDATE = "UPDATE"  # 修改预算/出价（仅操作号）
ACTION_READ   = "READ"    # 只读拉取（管理号即可）

FB_API_BASE = "https://graph.facebook.com/v25.0"

# ── 心跳缓存（内存级，重启清空）────────────────────────────────────────────
# key: token_id  value: (is_valid: bool, checked_at: float)
_heartbeat_cache: dict[int, Tuple[bool, float]] = {}
CACHE_TTL = 600  # 10 分钟
_selection_lock = threading.Lock()
_matrix_rr_state: dict[int, int] = {}
_token_runtime_state: dict[int, dict] = {}
_op_exhaust_alert_state: dict[tuple[str, str], float] = {}
TOKEN_MIN_GAP_SECONDS = 1.5
TOKEN_REQUEST_GAP_SECONDS = 0.8
OP_EXHAUST_ALERT_COOLDOWN_SECONDS = 1800

TRANSIENT_ERROR_COOLDOWNS = {
    1: 12.0,
    2: 15.0,
    4: 90.0,
    17: 120.0,
    32: 120.0,
    341: 120.0,
    613: 180.0,
}
DEFAULT_TRANSIENT_COOLDOWN = 15.0


def _is_token_alive(token_id: int, token_plain: str) -> bool:
    """
    检查 Token 是否有效，带缓存。
    缓存 TTL 内直接返回缓存结果，过期则重新验证。
    """
    now = time.time()
    cached = _heartbeat_cache.get(token_id)
    if cached:
        is_valid, checked_at = cached
        if now - checked_at < CACHE_TTL:
            return is_valid

    # 实际调用 FB API 验证
    try:
        resp = requests.get(
            f"{FB_API_BASE}/me",
            params={"access_token": token_plain, "fields": "id"},
            timeout=8
        )
        data = resp.json()
        err = data.get("error") if isinstance(data, dict) else None
        if err:
            err_code = err.get("code")
            if err_code in TRANSIENT_ERROR_COOLDOWNS or err.get("is_transient"):
                logger.warning(
                    f"[TokenManager] token_id={token_id} 心跳遇到 Meta 临时错误 code={err_code}，"
                    "不标记失效"
                )
                valid = True
            else:
                valid = False
        else:
            valid = "id" in data
    except Exception:
        valid = False

    _heartbeat_cache[token_id] = (valid, now)
    return valid


def invalidate_token_cache(token_id: int):
    """强制清除某个 Token 的心跳缓存（Token 被手动标记失效时调用）"""
    _heartbeat_cache.pop(token_id, None)


def _get_manage_token(act_id: str) -> Optional[str]:
    """
    获取账户的管理号 Token。READ/PAUSE 兜底允许使用 active 管理号，
    不要求该管理号绑定处于 active，避免操作号失效后巡检/关闭被误阻断。
    """
    conn = get_conn()
    ensure_token_source_columns(conn)
    account_team_id = _account_team_id(conn, act_id)
    token_team_sql, token_team_params = _token_team_clause(account_team_id, "t")
    row = conn.execute(
        f"""
        SELECT t.id, t.access_token_enc, t.status, aot.status as bind_status
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id = ?
          AND t.status = 'active'
          AND t.token_type = 'manage'
          {token_team_sql}
        ORDER BY CASE WHEN aot.status='active' THEN 0 ELSE 1 END,
                 aot.priority ASC, aot.id ASC
        LIMIT 1
        """,
        [act_id] + token_team_params,
    ).fetchone()
    if not row:
        row = conn.execute(
            """
            SELECT t.id, t.access_token_enc, t.status
            FROM accounts a
            JOIN fb_tokens t ON t.id = a.token_id
            WHERE a.act_id = ?
              AND t.status = 'active'
              AND (
                (a.team_id IS NULL AND t.team_id IS NULL)
                OR (a.team_id IS NOT NULL AND t.team_id=a.team_id)
              )
            LIMIT 1
            """,
            (act_id,),
        ).fetchone()
    if not row:
        fallback_team_sql, fallback_team_params = _token_team_clause(account_team_id, "fb_tokens")
        row = conn.execute(
            f"""
            SELECT id, access_token_enc, status
            FROM fb_tokens
            WHERE status='active' AND token_type='manage'
              {fallback_team_sql}
            ORDER BY id ASC
            LIMIT 1
            """,
            fallback_team_params,
        ).fetchone()
    conn.close()

    if not row or row["status"] != "active":
        return None
    token = decrypt_token(row["access_token_enc"])
    return token if token else None


def _get_op_tokens(act_id: str) -> list[dict]:
    """
    获取账户绑定的所有有效操作号 Token（仅 operate）。
    返回列表：[{"token_id": int, "token_plain": str, "alias": str, "matrix_id": int|None}, ...]
    """
    conn = get_conn()
    ensure_token_source_columns(conn)
    account_team_id = _account_team_id(conn, act_id)
    token_team_sql, token_team_params = _token_team_clause(account_team_id, "t")
    rows = conn.execute(f"""
        SELECT t.id as token_id, t.access_token_enc, t.status as token_status,
               aot.status as bind_status, aot.priority,
               t.token_alias, t.matrix_id, t.token_source
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id = ?
          AND aot.status = 'active'
          AND t.status = 'active'
          AND t.token_type = 'operate'
          AND t.token_source = ?
          {token_team_sql}
        ORDER BY aot.priority DESC, t.id ASC
    """, [act_id, TOKEN_SOURCE_SYSTEM_USER] + token_team_params).fetchall()
    conn.close()

    result = []
    for row in rows:
        plain = decrypt_token(row["access_token_enc"])
        if plain:
            result.append({
                "token_id": row["token_id"],
                "token_plain": plain,
                "alias": row["token_alias"] or f"token_{row['token_id']}",
                "matrix_id": row["matrix_id"],
                "priority": row["priority"],
                "token_source": row["token_source"],
            })

    return result


def _update_rr_state(act_id: str, used_token_id: int):
    """更新账户的轮询状态，记录本次使用的 token_id"""
    try:
        conn = get_conn()
        conn.execute("""
            INSERT INTO token_rr_state (act_id, last_token_id, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(act_id) DO UPDATE SET
                last_token_id=excluded.last_token_id,
                updated_at=excluded.updated_at
        """, (act_id, used_token_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[TokenManager] 更新 RR 状态失败: {e}")


def _token_runtime_meta(token_id: int) -> dict:
    return _token_runtime_state.setdefault(
        token_id,
        {
            "last_selected_at": 0.0,
            "next_request_at": 0.0,
            "cooldown_until": 0.0,
            "last_error_code": None,
            "last_reason": "",
        },
    )


def _sort_token_candidates_locked(tokens: list[dict]) -> list[dict]:
    if not tokens:
        return []

    matrix_id = next((t.get("matrix_id") for t in tokens if t.get("matrix_id")), None)
    ordered = list(tokens)
    if matrix_id:
        last_token_id = _matrix_rr_state.get(int(matrix_id))
        ids = [t["token_id"] for t in ordered]
        if last_token_id in ids:
            idx = ids.index(last_token_id)
            ordered = ordered[idx + 1:] + ordered[:idx + 1]

    now = time.time()
    ranked = []
    for idx, token in enumerate(ordered):
        state = _token_runtime_meta(token["token_id"])
        cooldown_until = float(state.get("cooldown_until") or 0.0)
        last_selected_at = float(state.get("last_selected_at") or 0.0)

        if cooldown_until > now:
            rank = (2, cooldown_until, idx)
        elif last_selected_at and (now - last_selected_at) < TOKEN_MIN_GAP_SECONDS:
            rank = (1, last_selected_at, idx)
        else:
            rank = (0, last_selected_at, idx)
        ranked.append((rank, token))

    ranked.sort(key=lambda item: item[0])
    return [item[1] for item in ranked]


def _reserve_token_locked(token_row: dict):
    token_id = token_row["token_id"]
    state = _token_runtime_meta(token_id)
    state["last_selected_at"] = time.time()
    matrix_id = token_row.get("matrix_id")
    if matrix_id:
        _matrix_rr_state[int(matrix_id)] = token_id


def _cooldown_token_locked(token_id: int, seconds: float, reason: str = "", error_code: Optional[int] = None):
    state = _token_runtime_meta(token_id)
    now = time.time()
    state["cooldown_until"] = max(float(state.get("cooldown_until") or 0.0), now + max(0.0, seconds))
    state["last_error_code"] = error_code
    state["last_reason"] = reason or state.get("last_reason") or ""


def _find_token_id_by_plain_locked(token_plain: str) -> Optional[int]:
    if not token_plain:
        return None
    conn = get_conn()
    ensure_token_source_columns(conn)
    rows = conn.execute(
        "SELECT id, access_token_enc FROM fb_tokens WHERE status IN ('active', 'invalid', 'suspended')"
    ).fetchall()
    conn.close()
    for row in rows:
        try:
            if decrypt_token(row["access_token_enc"]) == token_plain:
                return row["id"]
        except Exception:
            continue
    return None


def _reserve_request_slot_locked(token_id: int, min_gap_seconds: float) -> float:
    state = _token_runtime_meta(token_id)
    now = time.time()
    next_request_at = float(state.get("next_request_at") or 0.0)
    wait_seconds = max(0.0, next_request_at - now)
    state["next_request_at"] = max(now, next_request_at) + max(0.1, float(min_gap_seconds or TOKEN_REQUEST_GAP_SECONDS))
    return wait_seconds


def cooldown_token_by_plain(
    plain_token: str,
    seconds: float,
    reason: str = "",
    error_code: Optional[int] = None,
):
    """对指定明文 Token 设置短时冷却，供批量下发/限流重试时避开热点 Token。"""
    if not plain_token or seconds <= 0:
        return
    with _selection_lock:
        token_id = _find_token_id_by_plain_locked(plain_token)
        if token_id:
            _cooldown_token_locked(token_id, seconds, reason=reason, error_code=error_code)


def wait_for_token_slot_by_plain(
    plain_token: str,
    min_gap_seconds: float = TOKEN_REQUEST_GAP_SECONDS,
) -> float:
    """
    为同一个 token 的真实 FB 请求做轻量串行化，避免多个账户并发时瞬间打爆同一 token。
    返回实际等待秒数，便于调用方记录日志。
    """
    if not plain_token:
        return 0.0

    with _selection_lock:
        token_id = _find_token_id_by_plain_locked(plain_token)
        if not token_id:
            return 0.0
        wait_seconds = _reserve_request_slot_locked(token_id, min_gap_seconds)

    if wait_seconds > 0:
        time.sleep(wait_seconds)
    return wait_seconds



def _token_has_ads_management(token_id: Optional[int]) -> bool:
    """Check if the token's cached permission_snapshot includes ads_management."""
    if token_id is None:
        return False
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT permission_snapshot FROM fb_tokens WHERE id=?",
            (token_id,),
        ).fetchone()
        conn.close()
        if row and row["permission_snapshot"]:
            import json as _json2
            snap = _json2.loads(row["permission_snapshot"])
            status = snap.get("permission_status", {}).get("ads_management", "")
            return status == "granted"
    except Exception:
        pass
    return False

def get_exec_token_candidates(
    act_id: str,
    action_type: str = ACTION_CREATE,
    notify_exhausted: bool = True,
    reserve: bool = True,
) -> list[dict]:
    """
    返回当前账户可用的 Token 候选池，按“矩阵内全局轮询 + 冷却避让”排序。
    第一项会被视为本次优先使用的 Token，并立即占位，避免并发账户扎堆打到同一颗 Token。
    """
    with _selection_lock:
        raw_candidates = _get_op_tokens(act_id)
        ordered_candidates = _sort_token_candidates_locked(raw_candidates)
        alive_candidates = []

        for candidate in ordered_candidates:
            if _is_token_alive(candidate["token_id"], candidate["token_plain"]):
                label = candidate.get("alias") or f"token_{candidate['token_id']}"
                matrix_id = candidate.get("matrix_id")
                alive_candidates.append({
                    "token_id": candidate["token_id"],
                    "token_plain": candidate["token_plain"],
                    "token": candidate["token_plain"],
                    "alias": label,
                    "label": f"矩阵{matrix_id}·{label}" if matrix_id else label,
                    "matrix_id": matrix_id,
                    "token_source": candidate.get("token_source") or TOKEN_SOURCE_SYSTEM_USER,
                    "source": "operate",
                })
            else:
                logger.warning(
                    f"[TokenManager] 账户 {act_id} 操作号 token_id={candidate['token_id']} 心跳失败，跳过"
                )

        if alive_candidates and action_type not in (ACTION_PAUSE, ACTION_READ):
            if reserve:
                _reserve_token_locked(alive_candidates[0])
                _update_rr_state(act_id, alive_candidates[0]["token_id"])
            return alive_candidates

    if action_type in (ACTION_PAUSE, ACTION_READ):
        manage = _get_manage_token(act_id)
        manage_candidate = None
        manage_token_id = None
        if manage:
            # Fetch token_id for permission check
            try:
                conn2 = get_conn()
                trow = conn2.execute("SELECT id FROM fb_tokens WHERE access_token_enc=? LIMIT 1", (manage,)).fetchone()
                conn2.close()
                manage_token_id = trow["id"] if trow else None
            except Exception:
                manage_token_id = None
            manage_candidate = {
                "token_id": None,
                "token_plain": manage,
                "token": manage,
                "alias": "manage_fallback",
                "label": "管理号兜底",
                "matrix_id": None,
                "source": "manage",
            }
        if action_type == ACTION_PAUSE:
            candidates = list(alive_candidates)
            if manage_candidate:
                if _token_has_ads_management(manage_token_id):
                    candidates.append(manage_candidate)
                else:
                    logger.warning(
                        f"[TokenManager] 账户 {act_id} 管理号缺少 ads_management 权限，"
                        f"无法用于 PAUSE 操作。请在 Token 管理页点击验证更新权限后重试。"
                    )
                    # Send TG alert (deduped per account per hour)
                    _alert_no_pause_token(act_id, "管理号缺少 ads_management 权限")
            if candidates and alive_candidates and reserve:
                with _selection_lock:
                    _reserve_token_locked(alive_candidates[0])
                    _update_rr_state(act_id, alive_candidates[0]["token_id"])
            return candidates
        if manage_candidate:
            return [manage_candidate] + alive_candidates
        if alive_candidates:
            if reserve:
                with _selection_lock:
                    _reserve_token_locked(alive_candidates[0])
                    _update_rr_state(act_id, alive_candidates[0]["token_id"])
            return alive_candidates
        return []

    logger.error(f"[TokenManager] 账户 {act_id} 所有操作号均失效！action={action_type}")
    if notify_exhausted:
        _send_op_pool_exhausted_alert(act_id, action_type)
    return []


def get_exec_token(
    act_id: str,
    action_type: str = ACTION_PAUSE,
    notify_exhausted: bool = True,
) -> Optional[str]:
    """
    核心调度函数：根据 act_id 和操作类型，返回最合适的 Token。

    调度逻辑：
    1. 拉取该账户绑定的所有操作号（按优先级排序）。
    2. 依次验证心跳，返回第一个有效的操作号 Token。
    3. 如果所有操作号均失效：
       - PAUSE：回退到管理号 Token（兜底），并触发告警。
       - CREATE / UPDATE：直接返回 None，拒绝执行，并触发告警。
    4. READ 操作：直接使用管理号 Token。
    """
    candidates = get_exec_token_candidates(act_id, action_type, notify_exhausted=notify_exhausted)
    if candidates:
        return candidates[0]["token_plain"]
    if action_type in (ACTION_PAUSE, ACTION_READ):
        logger.warning(f"[TokenManager] 账户 {act_id} 无可用 Token，{action_type} 操作无法执行")
        if action_type == ACTION_PAUSE:
            _alert_no_pause_token(act_id, "无可用 Token（操作号+管理号均不可用）")
    else:
        logger.error(
            f"[TokenManager] 账户 {act_id} 操作号耗尽，{action_type} 操作已被拦截，保护管理号安全"
        )
    return None


def get_op_token_status(act_id: str) -> dict:
    """
    获取账户操作号池的状态摘要，用于前端展示。
    返回：{total, active, invalid, using_fallback,
              operate_total, operate_active, operate_invalid,
              manage_total, manage_active, manage_invalid}
    """
    conn = get_conn()
    ensure_token_source_columns(conn)
    account_team_id = _account_team_id(conn, act_id)
    token_team_sql, token_team_params = _token_team_clause(account_team_id, "t")

    # 1. 操作号：来自 account_op_tokens 表手动绑定的 Token
    op_rows = conn.execute(f"""
        SELECT t.id as token_id, t.access_token_enc, t.status as token_status,
               aot.status as bind_status, aot.priority, t.token_source
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id = ?
          AND t.token_type = 'operate'
          AND t.token_source = ?
          {token_team_sql}
        ORDER BY aot.priority DESC
    """, [act_id, TOKEN_SOURCE_SYSTEM_USER] + token_team_params).fetchall()

    legacy_rows = conn.execute(f"""
        SELECT t.id as token_id, t.status as token_status,
               aot.status as bind_status, aot.priority, t.token_source
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id = ?
          AND t.token_type = 'operate'
          AND COALESCE(t.token_source, '') != ?
          {token_team_sql}
        ORDER BY aot.priority DESC
    """, [act_id, TOKEN_SOURCE_SYSTEM_USER] + token_team_params).fetchall()

    # 2. 管理号：从 account_op_tokens 查（按 token_type=manage 过滤，与动态发现机制一致）
    mg_rows = conn.execute(f"""
        SELECT t.id as token_id, t.access_token_enc, t.status as token_status,
               aot.status as bind_status, aot.priority, t.token_alias
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id = ? AND t.token_type = 'manage'
          {token_team_sql}
        ORDER BY aot.priority DESC
    """, [act_id] + token_team_params).fetchall()
    conn.close()

    def _count_rows(rows, has_bind_status=True):
        total = len(rows)
        active_cnt = 0
        invalid_cnt = 0
        for row in rows:
            ts = row["token_status"]
            bs = row["bind_status"] if has_bind_status else "active"
            if bs == "active" and ts == "active":
                plain = decrypt_token(row["access_token_enc"])
                if plain:
                    cached = _heartbeat_cache.get(row["token_id"])
                    # 心跳缓存存在且明确失效才计入 invalid
                    if cached and not cached[0]:
                        invalid_cnt += 1
                    else:
                        active_cnt += 1  # 未检测过的默认视为 active
                else:
                    invalid_cnt += 1
            else:
                invalid_cnt += 1
        return total, active_cnt, invalid_cnt

    op_total, op_active, op_invalid = _count_rows(op_rows, has_bind_status=True)
    mg_total, mg_active, mg_invalid = _count_rows(mg_rows, has_bind_status=True)

    # 兼容旧字段： total/active/invalid 指操作号
    return {
        "total": op_total,
        "active": op_active,
        "invalid": op_invalid,
        "using_fallback": op_total > 0 and op_active == 0,
        "operate_total": op_total,
        "operate_active": op_active,
        "operate_invalid": op_invalid,
        "manage_total": mg_total,
        "manage_active": mg_active,
        "manage_invalid": mg_invalid,
        "legacy_operate_total": len(legacy_rows),
        "legacy_operate_active": sum(
            1 for row in legacy_rows
            if row["token_status"] == "active" and row["bind_status"] == "active"
        ),
    }


def run_heartbeat_check(act_id: str) -> dict:
    """
    主动对某账户所有操作号执行一次心跳检测，更新缓存。
    返回：{checked, alive, dead}
    """
    op_tokens = _get_op_tokens(act_id)
    alive = 0
    dead = 0
    for op in op_tokens:
        # 强制清除缓存，重新检测
        invalidate_token_cache(op["token_id"])
        if _is_token_alive(op["token_id"], op["token_plain"]):
            alive += 1
        else:
            # 重试一次（3秒后），避免网络抖动导致 Token 被误杀
            import time as _ht
            _ht.sleep(3)
            if _is_token_alive(op["token_id"], op["token_plain"]):
                alive += 1
                logger.warning(f"[TokenManager] token_id={op['token_id']} 首次心跳失败但重试成功，未标记失效")
                continue
            dead += 1
            # 将 fb_tokens 状态标记为 invalid
            _mark_token_invalid(op["token_id"])

    return {"checked": len(op_tokens), "alive": alive, "dead": dead}


def _mark_token_invalid(token_id: int):
    """将 fb_tokens 中某个 Token 标记为 invalid"""
    try:
        conn = get_conn()
        conn.execute(
            "UPDATE fb_tokens SET status='invalid' WHERE id=?",
            (token_id,)
        )
        conn.commit()
        conn.close()
        logger.warning(f"[TokenManager] token_id={token_id} 已被标记为 invalid")
    except Exception as e:
        logger.error(f"[TokenManager] 标记 Token 失效时出错: {e}")


def _send_op_pool_exhausted_alert(act_id: str, action_type: str):
    """操作号全灭时发送 TG 告警"""
    try:
        now = time.time()
        alert_key = (act_id, action_type)
        last_sent = _op_exhaust_alert_state.get(alert_key, 0.0)
        if now - last_sent < OP_EXHAUST_ALERT_COOLDOWN_SECONDS:
            return
        _op_exhaust_alert_state[alert_key] = now

        conn = get_conn()
        acc = conn.execute(
            "SELECT name FROM accounts WHERE act_id=?", (act_id,)
        ).fetchone()
        conn.close()

        acc_name = (acc["name"] if acc else "") or act_id
        account_label = f"{acc_name} (<code>{act_id}</code>)" if acc_name != act_id else f"<code>{act_id}</code>"

        fallback_note = "已回退到管理号执行 PAUSE 兜底" if action_type == ACTION_PAUSE \
            else f"⛔ {action_type} 操作已被系统拦截，保护管理号安全"

        msg = (
            f"🚨 <b>Mira 操作号池耗尽告警</b>\n\n"
            f"账户：{account_label}\n"
            f"触发操作：<code>{action_type}</code>\n"
            f"状态：{fallback_note}\n\n"
            f"⚠️ 自动铺广告和加预算功能已暂停，请尽快在后台补充操作号！"
        )

        notify_account(act_id, msg, event_type="token", dedup_key=f"op_exhaust:{act_id}:{action_type}")
    except Exception as e:
        logger.warning(f"[TokenManager] TG 告警发送失败: {e}")


# ── 矩阵内 Token 轮询（兜底用）────────────────────────────────────────────────

def get_matrix_tokens(act_id: str) -> list[dict]:
    """
    获取与指定账户同矩阵的所有 active operate Token（排除账户自身绑定的 token）。
    用于矩阵内兜底轮询：当账户绑定的 Token 失败时，尝试同矩阵内其他 Token。
    返回：[{"token_id": int, "token_plain": str, "alias": str}, ...]
    """
    try:
        conn = get_conn()
        ensure_token_source_columns(conn)
        # 获取该账户绑定的 token_id 和 matrix_id
        acc_row = conn.execute(
            "SELECT token_id, team_id FROM accounts WHERE act_id=?", (act_id,)
        ).fetchone()
        if not acc_row or not acc_row["token_id"]:
            conn.close()
            return []
        current_token_id = acc_row["token_id"]
        account_team_id = acc_row["team_id"] if acc_row["team_id"] is not None else None

        # 获取该 token 所属的 matrix_id
        token_row = conn.execute(
            "SELECT matrix_id, team_id FROM fb_tokens WHERE id=?", (current_token_id,)
        ).fetchone()
        if not token_row or not token_row["matrix_id"]:
            conn.close()
            return []
        if token_row["team_id"] != account_team_id:
            conn.close()
            return []
        matrix_id = token_row["matrix_id"]

        # 获取同矩阵内所有 active operate Token（排除当前账户绑定的）
        token_team_sql, token_team_params = _token_team_clause(account_team_id, "t")
        rows = conn.execute(
            f"""SELECT id as token_id, token_alias, access_token_enc
               FROM fb_tokens t
               WHERE matrix_id=? AND token_type='operate' AND token_source=? AND status='active'
               AND id != ?
               {token_team_sql}
               ORDER BY id ASC""",
            [matrix_id, TOKEN_SOURCE_SYSTEM_USER, current_token_id] + token_team_params,
        ).fetchall()
        conn.close()

        result = []
        for row in rows:
            plain = decrypt_token(row["access_token_enc"])
            if plain:
                result.append({
                    "token_id": row["token_id"],
                    "token_plain": plain,
                    "alias": row["token_alias"] or str(row["token_id"])
                })
        return result
    except Exception as e:
        logger.warning(f"[TokenManager] get_matrix_tokens 失败: {e}")
        return []


def get_matrix_id_for_account(act_id: str) -> Optional[int]:
    """获取账户所属矩阵 ID"""
    try:
        conn = get_conn()
        ensure_token_source_columns(conn)
        account_team_id = _account_team_id(conn, act_id)
        token_team_sql, token_team_params = _token_team_clause(account_team_id, "ft")
        op_row = conn.execute(
            f"""
            SELECT ft.matrix_id
            FROM account_op_tokens aot
            JOIN fb_tokens ft ON ft.id = aot.token_id
            WHERE aot.act_id=?
              AND aot.status='active'
              AND ft.status='active'
              AND ft.token_type='operate'
              AND ft.token_source=?
              AND ft.matrix_id IS NOT NULL
              {token_team_sql}
            ORDER BY aot.priority ASC, aot.id ASC
            LIMIT 1
            """,
            [act_id, TOKEN_SOURCE_SYSTEM_USER] + token_team_params,
        ).fetchone()
        if op_row and op_row["matrix_id"] is not None:
            conn.close()
            return op_row["matrix_id"]

        acc_row = conn.execute(
            "SELECT token_id FROM accounts WHERE act_id=?", (act_id,)
        ).fetchone()
        if not acc_row or not acc_row["token_id"]:
            conn.close()
            return None
        fallback_team_sql, fallback_team_params = _token_team_clause(account_team_id, "fb_tokens")
        token_row = conn.execute(
            f"SELECT matrix_id FROM fb_tokens WHERE id=? {fallback_team_sql}",
            [acc_row["token_id"]] + fallback_team_params,
        ).fetchone()
        conn.close()
        return token_row["matrix_id"] if token_row else None
    except Exception as e:
        logger.warning(f"[TokenManager] get_matrix_id_for_account 失败: {e}")
        return None

def suspend_token_by_plain(plain_token: str, reason: str = 'certification_required'):
    """通过明文 Token 值找到对应记录并将其标记为 suspended（需要认证）"""
    try:
        conn = get_conn()
        rows = conn.execute("SELECT id, token_alias, access_token_enc FROM fb_tokens WHERE status='active'").fetchall()
        for row in rows:
            try:
                if decrypt_token(row['access_token_enc']) == plain_token:
                    conn.execute("UPDATE fb_tokens SET status='suspended' WHERE id=?", (row['id'],))
                    conn.commit()
                    logger.warning(f"[TokenManager] token_id={row['id']} alias={row['token_alias']} 因 {reason} 已自动标记为 suspended")
                    conn.close()
                    return row['id']
            except Exception:
                continue
        conn.close()
        logger.warning(f"[TokenManager] 未找到匹配的 active token 进行 suspend（reason={reason}）")
    except Exception as e:
        logger.error(f"[TokenManager] suspend_token_by_plain 出错: {e}")
    return None
