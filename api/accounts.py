"""
账户管理 API v1.2.0
修复: 导入时数据库锁死问题（先批量调用FB API，再一次性写入DB）
"""
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, File, HTTPException, Depends, UploadFile
from pydantic import BaseModel
from typing import Optional, List

from core.auth import get_current_user, is_superadmin, normalize_user_claims
from core.database import get_conn, encrypt_token, decrypt_token
from core.account_access import (
    classify_read_failure,
    ensure_account_access_columns,
    is_read_blocking_status,
    mark_account_read_failure,
    mark_account_read_success,
)
from core.tenancy import (
    apply_team_scope,
    assert_row_access,
    claim_row_for_team,
    is_operator_user,
    team_id_for_create,
    user_id,
)
from services.token_manager import (
    ALLOWED_TOKEN_SOURCES,
    TOKEN_SOURCE_OAUTH_USER,
    TOKEN_SOURCE_SYSTEM_USER,
    default_token_source_for_type,
    ensure_token_source_columns,
    get_account_token_summary,
    is_operate_token_eligible,
    normalize_token_source,
)
from services.landing_link_resolver import resolve_account_form_link, resolve_account_landing_link
from services.notifier import ensure_notification_schema
from services.guard_engine import _local_per_usd_rate, _to_usd_guard

router = APIRouter()


def _require_operator_user(user) -> None:
    if not isinstance(user, dict) or user.get("role") not in ("superadmin", "admin", "operator"):
        raise HTTPException(status_code=403, detail="Operator permission required")


def _apply_account_owner_scope(where: list[str], params: list, user: dict, alias: str = "a") -> None:
    if is_operator_user(user):
        column = f"{alias}.owner_user_id" if alias else "owner_user_id"
        where.append(f"{column}=?")
        params.append(user_id(user))


def _owner_user_id_for_import(user: dict) -> Optional[int]:
    if is_operator_user(user):
        return user_id(user)
    return None


def _owner_id_for_token(user: dict) -> Optional[int]:
    if is_operator_user(user):
        return user_id(user)
    return None

_NO_DECIMAL_CURRENCIES = {"JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS"}
_UNLIMITED_SPEND_CAP_USD = 1_000_000.0

# ── 余额计算辅助函数 ──────────────────────────────────────────
# 汇率缓存（从数据库读取，不存在则用 1.0）
def _to_usd(amount, currency):
    """将金额转换为 USD（从数据库读取汇率）"""
    if amount is None:
        return None
    if currency in (None, '', 'USD'):
        return float(amount)
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT rate FROM currency_rates WHERE currency=?", (currency.upper(),)
        ).fetchone()
        conn.close()
        if row and row["rate"]:
            return round(float(amount) / float(row["rate"]), 2)
    except Exception:
        pass
    return float(amount)  # 无汇率时原值返回


def _money_factor(currency: str) -> int:
    return 1 if (currency or "USD").upper() in _NO_DECIMAL_CURRENCIES else 100


def _from_minor_units(value, currency: str):
    if value is None:
        return None
    try:
        return float(value) / _money_factor(currency)
    except (TypeError, ValueError):
        return None


def _to_minor_units(value, currency: str) -> int:
    factor = _money_factor(currency)
    return int(round(float(value) * factor))


def _calc_available_balance(balance, spend_cap, amount_spent, spending_limit, currency):
    """
    计算账户可用投放额度
    返回: (available_balance, balance_type, amount_spent_usd)
      - available_balance: spend_cap 减 amount_spent 后的可用额度（USD），None 表示无上限、超高上限或无法计算
      - balance_type: 'spending_limit' | 'very_high_limit' | 'unlimited'
      - amount_spent_usd: 已消费金额（USD）

    注意：FB balance 在后付费账户里通常是账单余额/欠款，不等于还能花多少钱。
    因此可用额度只由 spend_cap / spending_limit 与 amount_spent 推导。
    """
    # FB API 金额是 minor units：大多数币种为分，JPY/KRW 等零小数位币种为本币整数。
    sl = _from_minor_units(spending_limit, currency)
    spent = _from_minor_units(amount_spent, currency)
    cap = _from_minor_units(spend_cap, currency)

    # 已消费金额（USD）
    spent_usd = _to_usd(spent, currency) if spent is not None else None

    if spend_cap is None and spending_limit in (None, ""):
        return (None, 'unlimited', spent_usd)

    # 优先使用 spending_limit（消费上限型）
    if sl and sl > 0:
        if _to_usd(sl, currency) >= _UNLIMITED_SPEND_CAP_USD:
            return (None, 'very_high_limit', spent_usd)
        avail = max(0.0, sl - (spent or 0))
        avail_usd = _to_usd(avail, currency)
        return (round(avail_usd, 2) if avail_usd is not None else None,
                'spending_limit', spent_usd)

    # 其次使用 spend_cap（账户总上限），极高值（>= $1M 或 sentinel）视为超高上限。
    if cap and cap > 0:
        if _to_usd(cap, currency) >= _UNLIMITED_SPEND_CAP_USD:
            return (None, 'very_high_limit', spent_usd)
        avail = max(0.0, cap - (spent or 0))
        avail_usd = _to_usd(avail, currency)
        return (round(avail_usd, 2) if avail_usd is not None else None,
                'spending_limit', spent_usd)

    # spend_cap=0 / spending_limit=0 即无账户级消费上限；balance 不参与可用额度计算。
    return (None, 'unlimited', spent_usd)
# ── 余额计算辅助函数 END ──────────────────────────────────────


logger = logging.getLogger("mira.api.accounts")

FB_API_BASE = "https://graph.facebook.com/v25.0"
_SPEND_API_CACHE = {}


def _ensure_spend_retention_schema(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS account_spend_retention (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           snapshot_date TEXT NOT NULL,
           act_id TEXT NOT NULL,
           account_id TEXT,
           account_name TEXT,
           currency TEXT DEFAULT 'USD',
           team_id INTEGER,
           owner_user_id INTEGER,
           spend REAL DEFAULT 0,
           conversions REAL DEFAULT 0,
           removed_at TEXT DEFAULT (datetime('now','+8 hours')),
           source TEXT DEFAULT 'account_delete',
           UNIQUE(snapshot_date, act_id)
        )"""
    )
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(account_spend_retention)").fetchall()}
    if "owner_user_id" not in cols:
        conn.execute("ALTER TABLE account_spend_retention ADD COLUMN owner_user_id INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spend_retention_date ON account_spend_retention(snapshot_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spend_retention_team_date ON account_spend_retention(team_id, snapshot_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spend_retention_team_owner_date ON account_spend_retention(team_id, owner_user_id, snapshot_date)")
    conn.execute(
        """INSERT OR IGNORE INTO settings(key,value,label,description,category,sort_order)
           VALUES ('spend_retention_days','180','消耗历史留存天数',
                   '账户移除后保留指定天数的本地消耗归档，用于复制有消耗账户 ID 和追溯账单',
                   'storage',30)"""
    )
    conn.commit()


def _archive_account_spend_history(conn, account_id: int) -> None:
    _ensure_spend_retention_schema(conn)
    row = conn.execute(
        "SELECT id, act_id, name, currency, team_id, owner_user_id FROM accounts WHERE id=?",
        (account_id,),
    ).fetchone()
    if not row:
        return
    act_id = row["act_id"]
    plain_id = act_id[4:] if str(act_id or "").startswith("act_") else act_id
    conn.execute(
        """INSERT INTO account_spend_retention
           (snapshot_date, act_id, account_id, account_name, currency, team_id, owner_user_id,
            spend, conversions, removed_at, source)
           SELECT p.snapshot_date, p.act_id, ?, ?, ?, ?, ?,
                  SUM(COALESCE(p.spend, 0)),
                  SUM(COALESCE(p.conversions, 0)),
                  datetime('now','+8 hours'),
                  'account_delete'
           FROM perf_snapshots p
           WHERE p.act_id=?
           GROUP BY p.snapshot_date, p.act_id
           ON CONFLICT(snapshot_date, act_id) DO UPDATE SET
             account_id=excluded.account_id,
             account_name=excluded.account_name,
             currency=excluded.currency,
             team_id=excluded.team_id,
             owner_user_id=excluded.owner_user_id,
             spend=excluded.spend,
             conversions=excluded.conversions,
             removed_at=excluded.removed_at,
             source=excluded.source""",
        (plain_id, row["name"] or act_id, row["currency"] or "USD", row["team_id"], row["owner_user_id"], act_id),
    )
_SPEND_API_CACHE_TTL_SECONDS = 120


def _fetch_all_fb_adaccounts(
    access_token: str,
    fields: str,
    *,
    limit: int = 200,
    timeout: int = 30,
    max_pages: int = 100,
) -> List[dict]:
    """Follow Graph paging.next so tokens with >200 accounts are not truncated."""
    url = f"{FB_API_BASE}/me/adaccounts"
    params = {
        "access_token": access_token,
        "fields": fields,
        "limit": limit,
    }
    items: List[dict] = []
    seen_ids = set()
    seen_next_urls = set()

    for _ in range(max_pages):
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            err = data["error"]
            raise RuntimeError(err.get("message", "Facebook API 未知错误"))

        for item in data.get("data", []):
            item_id = item.get("id")
            if item_id and item_id in seen_ids:
                continue
            if item_id:
                seen_ids.add(item_id)
            items.append(item)

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        if next_url in seen_next_urls:
            logger.warning("[FBAdAccounts] duplicate paging.next detected, stop following pagination: %s", next_url)
            break
        seen_next_urls.add(next_url)
        url = next_url
        params = {}
    else:
        logger.warning("[FBAdAccounts] reached max_pages=%s while scanning token ad accounts", max_pages)

    return items


def _fetch_all_fb_adaccount_ids(access_token: str, *, timeout: int = 20) -> List[str]:
    return [
        item["id"]
        for item in _fetch_all_fb_adaccounts(access_token, "id", timeout=timeout)
        if item.get("id")
    ]


ACCOUNT_DETAIL_FIELDS = (
    "id,name,currency,timezone_name,timezone_offset_hours_utc,"
    "balance,account_status,spend_cap,amount_spent"
)


def _normalize_act_id(act_id: str) -> str:
    raw = str(act_id or "").strip()
    if not raw:
        return raw
    return raw if raw.startswith("act_") else f"act_{raw}"


def _coerce_float_or_none(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_account_read_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    changed = False
    if "timezone_name" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN timezone_name TEXT")
        changed = True
    if "timezone_offset_hours_utc" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN timezone_offset_hours_utc REAL")
        changed = True
    if "spending_limit" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN spending_limit TEXT")
        changed = True
    if "sentinel_enabled" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN sentinel_enabled INTEGER DEFAULT 0")
        changed = True
    if "owner_user_id" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN owner_user_id INTEGER")
        changed = True
    if changed:
        conn.commit()
    ensure_notification_schema(conn)
    ensure_account_access_columns(conn)


def _validate_account_owner(conn, owner_user_id: Optional[int], account_id: int, user: dict) -> Optional[int]:
    if owner_user_id in (None, 0):
        return None
    acc = conn.execute("SELECT team_id FROM accounts WHERE id=?", (account_id,)).fetchone()
    if not acc:
        raise HTTPException(status_code=404, detail="账户不存在")
    owner = conn.execute(
        "SELECT id, role, team_id, is_active FROM users WHERE id=?",
        (owner_user_id,),
    ).fetchone()
    if not owner or not owner["is_active"]:
        raise HTTPException(status_code=400, detail="负责人不存在或已禁用")
    if owner["role"] not in ("admin", "operator"):
        raise HTTPException(status_code=400, detail="负责人必须是团队管理员或运营")
    if owner["team_id"] != acc["team_id"]:
        raise HTTPException(status_code=400, detail="负责人必须属于该账户所在团队")
    if not normalize_user_claims(user).get("is_superadmin") and owner["team_id"] != user.get("team_id"):
        raise HTTPException(status_code=403, detail="不能设置其他团队的负责人")
    return int(owner_user_id)


def _validate_token_owner(conn, owner_user_id: Optional[int], token_id: int, user: dict) -> Optional[int]:
    if owner_user_id in (None, 0, ""):
        return None
    try:
        owner_id = int(owner_user_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid token owner")

    token = conn.execute("SELECT team_id FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    if token["team_id"] is None:
        raise HTTPException(status_code=400, detail="Assign token to a team before setting owner")

    owner = conn.execute(
        "SELECT id, role, team_id, is_active FROM users WHERE id=?",
        (owner_id,),
    ).fetchone()
    if not owner or not owner["is_active"]:
        raise HTTPException(status_code=400, detail="Owner does not exist or is disabled")
    if owner["role"] not in ("admin", "operator"):
        raise HTTPException(status_code=400, detail="Owner must be a team admin or operator")
    if owner["team_id"] != token["team_id"]:
        raise HTTPException(status_code=400, detail="Owner must belong to the token team")
    if not normalize_user_claims(user).get("is_superadmin") and owner["team_id"] != user.get("team_id"):
        raise HTTPException(status_code=403, detail="Cannot assign owner from another team")
    return owner_id


def _normalize_account_info(act_id: str, raw: dict) -> dict:
    timezone_name = raw.get("timezone_name") or raw.get("timezone") or "UTC"
    return {
        "act_id": _normalize_act_id(raw.get("id") or act_id),
        "name": raw.get("name") or act_id,
        "currency": raw.get("currency") or "USD",
        "timezone": timezone_name,
        "timezone_name": timezone_name,
        "timezone_offset_hours_utc": _coerce_float_or_none(raw.get("timezone_offset_hours_utc")),
        "balance": raw.get("balance"),
        "account_status": raw.get("account_status", 1),
        "spend_cap": raw.get("spend_cap"),
        "amount_spent": raw.get("amount_spent"),
        "spending_limit": raw.get("spend_cap"),
    }


def _read_token_candidates_for_account(
    act_id: str,
    *,
    preferred_token_id: Optional[int] = None,
    preferred_token_plain: Optional[str] = None,
    prefer_manage: bool = False,
    team_id: Optional[int] = None,
) -> List[dict]:
    candidates = []
    seen = set()

    def add_row(row, source: str):
        if not row:
            return
        token_id = row["id"]
        marker = f"id:{token_id}"
        if marker in seen:
            return
        plain = decrypt_token(row["access_token_enc"]) if row["access_token_enc"] else None
        if not plain:
            return
        seen.add(marker)
        candidates.append({
            "token_id": token_id,
            "token_plain": plain,
            "token_type": row["token_type"],
            "token_source": row["token_source"],
            "alias": row["token_alias"] or f"token_{token_id}",
            "source": source,
        })

    def add_plain(token_plain: Optional[str], source: str):
        if not token_plain:
            return
        marker = f"plain:{token_plain[:12]}:{token_plain[-8:]}"
        if marker in seen:
            return
        seen.add(marker)
        candidates.append({
            "token_id": None,
            "token_plain": token_plain,
            "token_type": "",
            "token_source": "",
            "alias": source,
            "source": source,
        })

    conn = get_conn()
    try:
        ensure_token_source_columns(conn)
        account_team_id = team_id
        if account_team_id is None:
            account_row = conn.execute("SELECT team_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
            if account_row:
                account_team_id = account_row["team_id"]

        def token_team_sql(alias: str = "t") -> tuple[str, list]:
            if account_team_id is None:
                return f" AND {alias}.team_id IS NULL", []
            return f" AND {alias}.team_id=?", [account_team_id]

        if prefer_manage:
            team_sql, team_params = token_team_sql("fb_tokens")
            for row in conn.execute(
                f"""
                SELECT id, access_token_enc, token_alias, token_type, token_source
                FROM fb_tokens
                WHERE status='active'
                  AND token_type='manage'
                  AND access_token_enc IS NOT NULL
                  {team_sql}
                ORDER BY id ASC
                """,
                team_params,
            ).fetchall():
                add_row(row, "manage_pool")

        if preferred_token_id:
            team_sql, team_params = token_team_sql("fb_tokens")
            row = conn.execute(
                f"""
                SELECT id, access_token_enc, token_alias, token_type, token_source
                FROM fb_tokens
                WHERE id=? AND status='active' AND access_token_enc IS NOT NULL
                  {team_sql}
                """,
                [preferred_token_id] + team_params,
            ).fetchone()
            add_row(row, "preferred")
        add_plain(preferred_token_plain, "preferred_plain")

        team_sql, team_params = token_team_sql("t")
        for row in conn.execute(
            f"""
            SELECT t.id, t.access_token_enc, t.token_alias, t.token_type, t.token_source,
                   aot.priority
            FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id = aot.token_id
            JOIN accounts a ON a.act_id = aot.act_id
            WHERE aot.act_id = ?
              AND aot.status = 'active'
              AND t.status = 'active'
              AND t.access_token_enc IS NOT NULL
              {team_sql}
              AND (
                (a.team_id IS NULL AND t.team_id IS NULL)
                OR (a.team_id IS NOT NULL AND t.team_id=a.team_id)
              )
            ORDER BY CASE t.token_type WHEN 'operate' THEN 0 WHEN 'manage' THEN 1 ELSE 2 END,
                     aot.priority DESC,
                     t.id ASC
            """,
            [act_id] + team_params,
        ).fetchall():
            add_row(row, "linked")

        row = conn.execute(
            """
            SELECT t.id, t.access_token_enc, t.token_alias, t.token_type, t.token_source
            FROM accounts a
            JOIN fb_tokens t ON t.id = a.token_id
            WHERE a.act_id = ?
              AND t.status = 'active'
              AND t.access_token_enc IS NOT NULL
              AND (
                (a.team_id IS NULL AND t.team_id IS NULL)
                OR (a.team_id IS NOT NULL AND t.team_id=a.team_id)
              )
            LIMIT 1
            """,
            (act_id,),
        ).fetchone()
        add_row(row, "primary")

        if not prefer_manage:
            team_sql, team_params = token_team_sql("fb_tokens")
            for row in conn.execute(
                f"""
                SELECT id, access_token_enc, token_alias, token_type, token_source
                FROM fb_tokens
                WHERE status='active'
                  AND token_type='manage'
                  AND access_token_enc IS NOT NULL
                  {team_sql}
                ORDER BY id ASC
                """,
                team_params,
            ).fetchall():
                add_row(row, "manage_pool")
    finally:
        conn.close()

    return candidates


def _resolve_account_info(
    act_id: str,
    *,
    preferred_token_id: Optional[int] = None,
    preferred_token_plain: Optional[str] = None,
    require_manage_read: bool = False,
    team_id: Optional[int] = None,
) -> dict:
    normalized_act_id = _normalize_act_id(act_id)
    candidates = _read_token_candidates_for_account(
        normalized_act_id,
        preferred_token_id=preferred_token_id,
        preferred_token_plain=preferred_token_plain,
        prefer_manage=require_manage_read,
        team_id=team_id,
    )
    errors = []
    for candidate in candidates:
        if require_manage_read and candidate.get("token_type") != "manage":
            continue
        info = _fetch_single_account(normalized_act_id, candidate["token_plain"])
        if "error" not in info:
            return {
                "ok": True,
                "info": info,
                "read_token_id": candidate.get("token_id"),
                "read_token_type": candidate.get("token_type"),
                "read_source": candidate.get("source"),
                "read_alias": candidate.get("alias"),
            }
        errors.append({
            "token_id": candidate.get("token_id"),
            "type": candidate.get("token_type"),
            "source": candidate.get("source"),
            "error": info.get("error"),
        })
    reason = "no_manage_read_token" if require_manage_read else "no_read_token"
    if errors:
        reason = errors[-1].get("error") or reason
    return {"ok": False, "act_id": normalized_act_id, "error": reason, "errors": errors}


def _resolve_account_info_for_smart_import(act_id: str, user: dict) -> dict:
    normalized_act_id = _normalize_act_id(act_id)
    claims = normalize_user_claims(user)
    if not claims.get("is_superadmin"):
        team_id = team_id_for_create(user)
        result = _resolve_account_info(
            normalized_act_id,
            require_manage_read=True,
            team_id=team_id,
        )
        if result.get("ok"):
            result["read_team_id"] = team_id
        return result

    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT id, access_token_enc, token_alias, token_type, token_source, team_id
            FROM fb_tokens
            WHERE status='active'
              AND token_type='manage'
              AND access_token_enc IS NOT NULL
            ORDER BY CASE WHEN team_id IS NULL THEN 1 ELSE 0 END, id ASC
            """
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"ok": False, "act_id": normalized_act_id, "error": "no_manage_read_token", "errors": []}

    def _try_row(row):
        try:
            plain = decrypt_token(row["access_token_enc"])
            info = _fetch_single_account(normalized_act_id, plain)
            if "error" not in info:
                return {
                    "ok": True,
                    "info": info,
                    "read_token_id": row["id"],
                    "read_token_type": row["token_type"],
                    "read_source": "smart_manage_pool",
                    "read_alias": row["token_alias"],
                    "read_team_id": row["team_id"],
                }
            return {
                "ok": False,
                "token_id": row["id"],
                "type": row["token_type"],
                "source": "smart_manage_pool",
                "error": info.get("error"),
            }
        except Exception as exc:
            return {
                "ok": False,
                "token_id": row["id"],
                "type": row["token_type"],
                "source": "smart_manage_pool",
                "error": str(exc),
            }

    errors = []
    with ThreadPoolExecutor(max_workers=min(8, len(rows))) as executor:
        futures = [executor.submit(_try_row, row) for row in rows]
        for future in as_completed(futures):
            result = future.result()
            if result.get("ok"):
                return result
            errors.append(result)

    reason = errors[-1].get("error") if errors else "no_manage_read_token"
    return {"ok": False, "act_id": normalized_act_id, "error": reason or "no_manage_read_token", "errors": errors}


def _extract_graph_error(data: dict, fallback: str = "FB API error") -> str:
    if not isinstance(data, dict):
        return fallback
    err = data.get("error")
    if isinstance(err, dict):
        msg = err.get("message") or fallback
        code = err.get("code")
        subcode = err.get("error_subcode") or err.get("subcode")
        parts = [msg]
        if code is not None:
            parts.append(f"code={code}")
        if subcode is not None:
            parts.append(f"subcode={subcode}")
        return " | ".join(parts)
    return fallback


def _sum_account_insights_spend(act_id: str, currency: str, date_from: str, date_to: str, team_id=None) -> dict:
    """Fetch account-level spend from FB Insights for a date range, short cached."""
    normalized_act_id = _normalize_act_id(act_id)
    currency = (currency or "USD").upper()
    cache_key = (normalized_act_id, currency, date_from, date_to, team_id)
    now = time.time()
    cached = _SPEND_API_CACHE.get(cache_key)
    if cached and now - cached.get("ts", 0) < _SPEND_API_CACHE_TTL_SECONDS:
        return dict(cached["data"], cached=True)

    candidates = _read_token_candidates_for_account(normalized_act_id, team_id=team_id)
    errors = []
    for candidate in candidates:
        try:
            resp = requests.get(
                f"{FB_API_BASE}/{normalized_act_id}/insights",
                params={
                    "access_token": candidate["token_plain"],
                    "fields": "date_start,date_stop,spend,actions",
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "time_increment": 1,
                    "limit": 100,
                },
                timeout=25,
            )
            try:
                data = resp.json()
            except Exception:
                data = {}
            if resp.status_code >= 400 or data.get("error"):
                errors.append({
                    "token_id": candidate.get("token_id"),
                    "alias": candidate.get("alias"),
                    "type": candidate.get("token_type"),
                    "error": _extract_graph_error(data, f"HTTP {resp.status_code}"),
                })
                continue

            spend_orig = 0.0
            conversions = 0.0
            for item in data.get("data", []) or []:
                try:
                    spend_orig += float(item.get("spend") or 0)
                except (TypeError, ValueError):
                    pass
                # Only used as a secondary detail for this endpoint. Do not let
                # action parsing affect spend detection.
                for action in item.get("actions") or []:
                    try:
                        if action.get("action_type") in ("purchase", "lead", "onsite_conversion.messaging_conversation_started_7d"):
                            conversions += float(action.get("value") or 0)
                    except (TypeError, ValueError):
                        pass

            spend_usd = _to_usd(spend_orig, currency)
            result = {
                "ok": True,
                "act_id": normalized_act_id,
                "currency": currency,
                "spend": float(spend_usd or 0),
                "spend_orig": float(spend_orig or 0),
                "conversions": float(conversions or 0),
                "source": "fb_insights_api",
                "token_id": candidate.get("token_id"),
                "token_alias": candidate.get("alias"),
            }
            _SPEND_API_CACHE[cache_key] = {"ts": now, "data": result}
            return result
        except Exception as exc:
            errors.append({
                "token_id": candidate.get("token_id"),
                "alias": candidate.get("alias"),
                "type": candidate.get("token_type"),
                "error": str(exc),
            })

    return {
        "ok": False,
        "act_id": normalized_act_id,
        "currency": currency,
        "spend": 0.0,
        "spend_orig": 0.0,
        "conversions": 0.0,
        "source": "fb_insights_api",
        "error": errors[-1]["error"] if errors else "no_read_token",
        "errors": errors,
    }


def _fetch_spend_for_accounts(accounts: list, date_from: str, date_to: str, *, max_workers: int = 6) -> dict:
    if not accounts:
        return {}
    results = {}
    workers = max(1, min(max_workers, len(accounts)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {}
        for acc in accounts:
            act_id = _normalize_act_id(acc.get("act_id"))
            if not act_id:
                continue
            future = executor.submit(
                _sum_account_insights_spend,
                act_id,
                acc.get("currency") or "USD",
                date_from,
                date_to,
                acc.get("team_id"),
            )
            future_map[future] = act_id
        for future in as_completed(future_map):
            act_id = future_map[future]
            try:
                results[act_id] = future.result()
            except Exception as exc:
                results[act_id] = {
                    "ok": False,
                    "act_id": act_id,
                    "spend": 0.0,
                    "spend_orig": 0.0,
                    "conversions": 0.0,
                    "source": "fb_insights_api",
                    "error": str(exc),
                }
    return results


def _matching_team_id(user: dict, account_team_id: Optional[int]) -> Optional[int]:
    if account_team_id is not None:
        return account_team_id
    claims = normalize_user_claims(user)
    if claims.get("is_superadmin"):
        return None
    return claims.get("team_id")


def _auto_link_tokens_for_accounts(
    act_ids: List[str],
    user: dict,
    *,
    team_id: Optional[int] = None,
    note: str = "auto_match",
) -> dict:
    normalized_act_ids = []
    seen_acts = set()
    for raw in act_ids or []:
        act_id = _normalize_act_id(raw)
        if act_id and act_id not in seen_acts:
            seen_acts.add(act_id)
            normalized_act_ids.append(act_id)
    if not normalized_act_ids:
        return {"matched": 0, "restored": 0, "already_linked": 0, "token_checked": 0, "token_failed": 0, "accounts": []}

    conn = get_conn()
    try:
        ensure_token_source_columns(conn)
        _ensure_account_read_columns(conn)
        token_where = [
            "status='active'",
            "access_token_enc IS NOT NULL",
            "token_type IN ('manage','operate','user')",
            "(token_type!='operate' OR token_source IN (?, ?))",
        ]
        token_params = [TOKEN_SOURCE_SYSTEM_USER, TOKEN_SOURCE_OAUTH_USER]
        if team_id is not None:
            if is_superadmin(user):
                token_where.append("(team_id=? OR (team_id IS NULL AND token_type='operate' AND token_source=?))")
                token_params.extend([team_id, TOKEN_SOURCE_OAUTH_USER])
            else:
                token_where.append("team_id=?")
                token_params.append(team_id)
        elif not normalize_user_claims(user).get("is_superadmin"):
            user_team_id = team_id_for_create(user)
            token_where.append("team_id=?")
            token_params.append(user_team_id)
        else:
            token_where.append("team_id IS NULL")
        if is_operator_user(user):
            token_where.append("(owner_user_id=? OR owner_user_id IS NULL)")
            token_params.append(user_id(user))
        token_rows = conn.execute(
            f"""
            SELECT id, token_alias, token_type, token_source, matrix_id, access_token_enc
            FROM fb_tokens
            WHERE {' AND '.join(token_where)}
            ORDER BY CASE token_type WHEN 'manage' THEN 0 WHEN 'operate' THEN 1 ELSE 2 END,
                     id ASC
            """,
            token_params,
        ).fetchall()
    finally:
        conn.close()

    token_results = []

    def _scan_token(row):
        token_id = row["id"]
        alias = row["token_alias"] or f"token_{token_id}"
        try:
            plain = decrypt_token(row["access_token_enc"])
            if not plain:
                return {"token_id": token_id, "alias": alias, "ok": False, "error": "empty token", "act_ids": []}
            fb_ids = set(_fetch_all_fb_adaccount_ids(plain, timeout=20))
            matched_ids = [act_id for act_id in normalized_act_ids if act_id in fb_ids]
            return {
                "token_id": token_id,
                "alias": alias,
                "token_type": row["token_type"],
                "token_source": row["token_source"],
                "matrix_id": row["matrix_id"],
                "ok": True,
                "act_ids": matched_ids,
            }
        except Exception as exc:
            return {
                "token_id": token_id,
                "alias": alias,
                "token_type": row["token_type"],
                "token_source": row["token_source"],
                "matrix_id": row["matrix_id"],
                "ok": False,
                "error": _compact_diag_error(exc),
                "act_ids": [],
            }

    if token_rows:
        with ThreadPoolExecutor(max_workers=min(6, len(token_rows))) as executor:
            futures = [executor.submit(_scan_token, row) for row in token_rows]
            for future in as_completed(futures):
                token_results.append(future.result())

    matched = 0
    restored = 0
    already_linked = 0
    account_hits = {act_id: [] for act_id in normalized_act_ids}
    conn = get_conn()
    try:
        ensure_token_source_columns(conn)
        _ensure_account_read_columns(conn)
        for result in token_results:
            if not result.get("ok"):
                continue
            token_id = result["token_id"]
            token_type = result.get("token_type") or "user"
            for act_id in result.get("act_ids") or []:
                existing = conn.execute(
                    "SELECT id, status FROM account_op_tokens WHERE act_id=? AND token_id=?",
                    (act_id, token_id),
                ).fetchone()
                if not existing:
                    max_pri = conn.execute(
                        "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?",
                        (act_id,),
                    ).fetchone()[0] or 0
                    conn.execute(
                        """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                           VALUES (?, ?, ?, 'active', ?, ?, datetime('now'))""",
                        (act_id, token_id, max_pri + 1, note, token_type),
                    )
                    matched += 1
                elif existing["status"] != "active":
                    conn.execute(
                        "UPDATE account_op_tokens SET status='active', note=? WHERE id=?",
                        (note, existing["id"]),
                    )
                    restored += 1
                else:
                    already_linked += 1
                account_hits.setdefault(act_id, []).append({
                    "token_id": token_id,
                    "alias": result.get("alias"),
                    "token_type": token_type,
                    "token_source": result.get("token_source"),
                    "matrix_id": result.get("matrix_id"),
                })
        for act_id, hits in account_hits.items():
            if hits:
                mark_account_read_success(conn, act_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    failed_tokens = [r for r in token_results if not r.get("ok")]
    return {
        "matched": matched,
        "restored": restored,
        "already_linked": already_linked,
        "token_checked": len(token_results),
        "token_failed": len(failed_tokens),
        "accounts": [
            {
                "act_id": act_id,
                "matched_tokens": account_hits.get(act_id, []),
                "matched_count": len(account_hits.get(act_id, [])),
            }
            for act_id in normalized_act_ids
        ],
        "failed_tokens": failed_tokens[:10],
    }


# ── Pydantic 模型 ──────────────────────────────────────────────────────────

class TokenCreate(BaseModel):
    token_alias: str
    access_token: str
    token_type: str = "user"
    token_source: Optional[str] = None
    note: Optional[str] = ""
    matrix_id: Optional[int] = None  # 操作号所属矩阵编号，管理号不填


class TokenUpdate(BaseModel):
    access_token: str
    token_alias: Optional[str] = None
    token_type: Optional[str] = None
    token_source: Optional[str] = None
    note: Optional[str] = None
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None


class TokenTypeUpdate(BaseModel):
    token_type: str  # manage / operate / user


class TokenSourceUpdate(BaseModel):
    token_source: str


class AccountImport(BaseModel):
    act_ids: List[str]
    token_id: Optional[int] = None  # 路由参数已包含 token_id，body 中可选
    page_id: Optional[str] = None    # 批量导入时统一设置主页ID
    pixel_id: Optional[str] = None   # 批量导入时统一设置像素ID


def _normalize_import_act_ids(raw_act_ids: List[str]) -> List[str]:
    act_ids = []
    seen_act_ids = set()
    for raw_act_id in raw_act_ids or []:
        act_id = _normalize_act_id(raw_act_id)
        if act_id and act_id not in seen_act_ids:
            seen_act_ids.add(act_id)
            act_ids.append(act_id)
    return act_ids


class AccountUpdate(BaseModel):
    name: Optional[str] = None
    enabled: Optional[int] = None
    note: Optional[str] = None
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None
    beneficiary: Optional[str] = None
    payer: Optional[str] = None
    tw_advertiser_id: Optional[int] = None
    # 智能铺放目标配置
    target_countries: Optional[str] = None      # JSON 字符串，如 '["TW","HK"]'
    target_age_min: Optional[int] = None        # 最小年龄，默认 25
    target_age_max: Optional[int] = None        # 最大年龄，默认 65
    target_gender: Optional[int] = None         # 0=不限 1=男 2=女
    target_placements: Optional[str] = None     # JSON 字符串，如 '["feed","reels"]'
    target_objective: Optional[str] = None      # 真实广告目标，如 OUTCOME_SALES
    warmup_days: Optional[int] = None           # 预热天数，默认 1
    warmup_budget: Optional[float] = None       # 预热消耗阈值（美元），默认 5
    landing_url: Optional[str] = None           # 账户级默认落地页链接
    form_link: Optional[str] = None             # 账户级表单链接（潜在客户广告用）
    target_objective_type: Optional[str] = None  # sales/website/leads/engagement
    sentinel_enabled: Optional[int] = None     # Account-level sentinel switch 0/1
    mirror_enabled: Optional[int] = None       # 镜像模式开关 0/1
    owner_user_id: Optional[int] = None        # 账户负责人（同团队用户）


# ── Token 管理 ─────────────────────────────────────────────────────────────


def _auto_detect_token_type(access_token: str) -> str:
    """通过 FB API 自动检测 Token 类型：manage（管理号）或 operate（操作号）"""
    import requests
    try:
        r = requests.get(
            "https://graph.facebook.com/v25.0/me",
            params={"fields": "id,name,type", "access_token": access_token},
            timeout=8
        )
        data = r.json()
        if "error" in data:
            return "manage"
        # 系统用户 type 为 "application"
        if data.get("type") == "application":
            return "operate"
        # 检查是否有 BM 关联的广告账户权限（操作号特征）
        r2 = requests.get(
            "https://graph.facebook.com/v25.0/me/adaccounts",
            params={"access_token": access_token, "limit": 1},
            timeout=8
        )
        d2 = r2.json()
        # 如果有广告账户权限，默认为管理号（个人号通常有 adaccounts）
        return "manage"
    except Exception:
        return "manage"


TOKEN_PERMISSION_CAPABILITY_DEFS = [
    ("ads_management", "广告管理", ("ads_management",)),
    ("ads_read", "广告读取", ("ads_read", "ads_management")),
    ("business_management", "BM管理", ("business_management",)),
    ("pages_show_list", "主页列表", ("pages_show_list",)),
    ("pages_manage_ads", "Lead表单", ("pages_manage_ads",)),
    ("pages_messaging", "消息", ("pages_messaging",)),
]


def _ensure_fb_token_permission_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(fb_tokens)").fetchall()}
    changed = False
    if "permission_snapshot" not in cols:
        conn.execute("ALTER TABLE fb_tokens ADD COLUMN permission_snapshot TEXT")
        changed = True
    if "permission_checked_at" not in cols:
        conn.execute("ALTER TABLE fb_tokens ADD COLUMN permission_checked_at TEXT")
        changed = True
    if changed:
        conn.commit()


def _resolve_token_source(raw_source: Optional[str], token_type: Optional[str]) -> str:
    return normalize_token_source(raw_source, default_token_source_for_type(token_type))


def _validate_token_role_source(token_type: Optional[str], token_source: Optional[str]) -> str:
    resolved_source = _resolve_token_source(token_source, token_type)
    if resolved_source not in ALLOWED_TOKEN_SOURCES:
        raise HTTPException(400, f"token_source 必须是 {sorted(ALLOWED_TOKEN_SOURCES)} 之一")
    if str(token_type or "").strip().lower() == "operate" and not is_operate_token_eligible(token_type, resolved_source):
        raise HTTPException(400, "操作号必须使用 System User 或 Meta 官方授权来源；如需保留个人号，请改为管理号或旧版个人号类型")
    return resolved_source


def _format_fb_graph_error(payload) -> str:
    err = payload.get("error") if isinstance(payload, dict) else None
    if not err:
        return "Facebook API 未知错误"
    parts = [str(err.get("message") or "Facebook API 未知错误").strip()]
    if err.get("code") not in (None, ""):
        parts.append(f"code={err.get('code')}")
    if err.get("error_subcode") not in (None, ""):
        parts.append(f"subcode={err.get('error_subcode')}")
    return " | ".join(parts)


def _graph_get_json(path: str, access_token: str, *, params: Optional[dict] = None, timeout: int = 10) -> dict:
    request_params = {"access_token": access_token}
    if params:
        request_params.update(params)
    resp = requests.get(f"{FB_API_BASE}{path}", params=request_params, timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RuntimeError("Facebook API 返回了无法解析的数据")
    if resp.status_code >= 400:
        raise RuntimeError(_format_fb_graph_error(data))
    if "error" in data:
        raise RuntimeError(_format_fb_graph_error(data))
    return data


def _build_token_permission_capabilities(permission_status: dict, probes: dict, permission_loaded: bool) -> List[dict]:
    capabilities = []
    for key, label, scopes in TOKEN_PERMISSION_CAPABILITY_DEFS:
        granted_scope = next((scope for scope in scopes if permission_status.get(scope) == "granted"), None)
        if granted_scope:
            capabilities.append({
                "key": key,
                "label": label,
                "status": "granted",
                "detail": f"已授权 {granted_scope}",
            })
            continue

        if key == "ads_read" and probes.get("adaccounts", {}).get("status") == "ok":
            capabilities.append({
                "key": key,
                "label": label,
                "status": "granted",
                "detail": "广告账户接口可读取",
            })
            continue

        if key == "pages_show_list" and probes.get("pages", {}).get("status") == "ok":
            capabilities.append({
                "key": key,
                "label": label,
                "status": "granted",
                "detail": "主页列表接口可读取",
            })
            continue

        primary_scope = scopes[0]
        if permission_loaded:
            raw_state = permission_status.get(primary_scope) or "missing"
            detail = f"未授权 {primary_scope}"
            if raw_state in {"declined", "expired"}:
                detail = f"{primary_scope} 为 {raw_state}"
            capabilities.append({
                "key": key,
                "label": label,
                "status": "missing",
                "detail": detail,
            })
            continue

        capabilities.append({
            "key": key,
            "label": label,
            "status": "unknown",
            "detail": "尚未完成权限检测",
        })
    return capabilities


def _build_failed_token_permission_snapshot(error_message: str) -> dict:
    return {
        "user": {},
        "granted_permissions": [],
        "declined_permissions": [],
        "expired_permissions": [],
        "permission_status": {},
        "granted_count": 0,
        "declined_count": 0,
        "expired_count": 0,
        "total_permissions": 0,
        "permission_loaded": False,
        "probe_error": f"Token 已失效：{error_message}" if error_message else "Token 已失效",
        "probes": {},
        "capabilities": [
            {"key": key, "label": label, "status": "unknown", "detail": "Token 已失效"}
            for key, label, _ in TOKEN_PERMISSION_CAPABILITY_DEFS
        ],
    }


def _build_token_permission_snapshot(access_token: str, base_info: Optional[dict] = None) -> dict:
    snapshot = {
        "user": {},
        "granted_permissions": [],
        "declined_permissions": [],
        "expired_permissions": [],
        "permission_status": {},
        "granted_count": 0,
        "declined_count": 0,
        "expired_count": 0,
        "total_permissions": 0,
        "permission_loaded": False,
        "probe_error": None,
        "probes": {},
        "capabilities": [],
    }
    if isinstance(base_info, dict):
        snapshot["user"] = {
            k: base_info.get(k)
            for k in ("id", "name", "type")
            if base_info.get(k) not in (None, "")
        }

    try:
        me_data = _graph_get_json("/me", access_token, params={"fields": "id,name,type"}, timeout=10)
        if isinstance(me_data, dict):
            snapshot["user"].update({
                k: me_data.get(k)
                for k in ("id", "name", "type")
                if me_data.get(k) not in (None, "")
            })
    except Exception as exc:
        logger.warning("[TokenPermission] failed to load /me: %s", exc)
        if not snapshot["probe_error"]:
            snapshot["probe_error"] = f"基本信息读取失败：{exc}"

    permission_status = {}
    permission_loaded = False
    try:
        perm_data = _graph_get_json("/me/permissions", access_token, timeout=10)
        for item in perm_data.get("data", []) or []:
            scope = str(item.get("permission") or "").strip()
            status = str(item.get("status") or "").strip().lower()
            if scope:
                permission_status[scope] = status or "unknown"
        permission_loaded = True
    except Exception as exc:
        logger.warning("[TokenPermission] failed to load /me/permissions: %s", exc)
        if not snapshot["probe_error"]:
            snapshot["probe_error"] = f"权限列表读取失败：{exc}"

    def _run_probe(path: str, params: Optional[dict] = None) -> dict:
        try:
            data = _graph_get_json(path, access_token, params=params, timeout=10)
            count = len(data.get("data", [])) if isinstance(data.get("data"), list) else 0
            return {"status": "ok", "count": count}
        except Exception as exc:
            return {"status": "error", "message": str(exc)}

    probes = {
        "adaccounts": _run_probe("/me/adaccounts", params={"fields": "id", "limit": 1}),
        "pages": _run_probe("/me/accounts", params={"fields": "id", "limit": 1}),
    }

    granted_permissions = sorted([scope for scope, status in permission_status.items() if status == "granted"])
    declined_permissions = sorted([scope for scope, status in permission_status.items() if status == "declined"])
    expired_permissions = sorted([scope for scope, status in permission_status.items() if status == "expired"])

    snapshot["permission_status"] = permission_status
    snapshot["granted_permissions"] = granted_permissions
    snapshot["declined_permissions"] = declined_permissions
    snapshot["expired_permissions"] = expired_permissions
    snapshot["granted_count"] = len(granted_permissions)
    snapshot["declined_count"] = len(declined_permissions)
    snapshot["expired_count"] = len(expired_permissions)
    snapshot["total_permissions"] = len(permission_status)
    snapshot["permission_loaded"] = permission_loaded
    snapshot["probes"] = probes
    snapshot["capabilities"] = _build_token_permission_capabilities(permission_status, probes, permission_loaded)
    return snapshot

@router.get("/tokens")
def list_tokens(user=Depends(get_current_user)):
    """获取所有Token列表（脱敏）"""
    conn = get_conn()
    ensure_token_source_columns(conn)
    _ensure_fb_token_permission_columns(conn)
    where, params = ["1=1"], []
    apply_team_scope(where, params, user, "t.team_id", include_unassigned=False)
    from core.tenancy import apply_account_owner_scope as _apply_token_owner
    _apply_token_owner(where, params, user, "t.owner_user_id")
    rows = conn.execute(f"""
        SELECT t.id, t.token_alias, t.token_type, t.token_source, t.status,
               t.last_verified_at, t.note, t.created_at, t.matrix_id,
               t.team_id, tm.name AS team_name,
               t.owner_user_id,
               COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name,
               t.permission_snapshot, t.permission_checked_at,
               0 as account_count
        FROM fb_tokens t
        LEFT JOIN accounts a ON a.token_id = t.id
        LEFT JOIN teams tm ON tm.id = t.team_id
        LEFT JOIN users ou ON ou.id = t.owner_user_id
        WHERE {' AND '.join(where)}
        GROUP BY t.id
        ORDER BY t.created_at DESC
    """, params).fetchall()
    matrix_where, matrix_params = ["1=1"], []
    apply_team_scope(matrix_where, matrix_params, user, "a.team_id", include_unassigned=False)
    _apply_token_owner(matrix_where, matrix_params, user, "a.owner_user_id")
    matrix_scope_sql = " AND ".join(matrix_where)
    token_account_count_map = {}
    for cr in conn.execute(f"""
        WITH rel AS (
            SELECT token_id, act_id
              FROM account_op_tokens
             WHERE status = 'active'
            UNION
            SELECT token_id, act_id
              FROM accounts
             WHERE token_id IS NOT NULL
        )
        SELECT rel.token_id, COUNT(DISTINCT rel.act_id) AS account_count
          FROM rel
          JOIN accounts a ON a.act_id = rel.act_id
         WHERE {matrix_scope_sql}
         GROUP BY rel.token_id
    """, matrix_params).fetchall():
        try:
            token_account_count_map[int(cr["token_id"])] = int(cr["account_count"] or 0)
        except (TypeError, ValueError):
            continue
    token_matrix_map = {}
    for mr in conn.execute(f"""
        WITH rel AS (
            SELECT token_id, act_id
              FROM account_op_tokens
             WHERE status = 'active'
            UNION
            SELECT token_id, act_id
              FROM accounts
             WHERE token_id IS NOT NULL
        )
        SELECT rel.token_id, mt.matrix_id
          FROM rel
          JOIN accounts a ON a.act_id = rel.act_id
          JOIN account_op_tokens aotm ON aotm.act_id = a.act_id AND aotm.status = 'active'
          JOIN fb_tokens mt ON mt.id = aotm.token_id
         WHERE {matrix_scope_sql}
           AND mt.matrix_id IS NOT NULL
        UNION
        SELECT rel.token_id, pt.matrix_id
          FROM rel
          JOIN accounts a ON a.act_id = rel.act_id
          JOIN fb_tokens pt ON pt.id = a.token_id
         WHERE {matrix_scope_sql}
           AND pt.matrix_id IS NOT NULL
    """, matrix_params + matrix_params).fetchall():
        try:
            tid = int(mr["token_id"])
            mid = int(mr["matrix_id"])
        except (TypeError, ValueError):
            continue
        token_matrix_map.setdefault(tid, set()).add(mid)
    conn.close()
    data = []
    for row in rows:
        item = dict(row)
        raw_snapshot = item.get("permission_snapshot")
        if raw_snapshot:
            try:
                item["permission_snapshot"] = json.loads(raw_snapshot)
            except Exception:
                item["permission_snapshot"] = None
        else:
            item["permission_snapshot"] = None
        token_id_int = int(item["id"])
        item["account_count"] = token_account_count_map.get(token_id_int, 0)
        item["linked_matrix_ids"] = sorted(token_matrix_map.get(token_id_int, set()))
        data.append(item)
    return data


@router.post("/tokens")
def add_token(body: TokenCreate, user=Depends(get_current_user)):
    """添加新Token（加密存储）"""
    if not body.access_token.strip():
        raise HTTPException(400, "Access Token 不能为空")

    ok, info = _verify_fb_token(body.access_token)
    if not ok:
        raise HTTPException(400, f"Token验证失败: {info}")

    enc = encrypt_token(body.access_token.strip())
    permission_snapshot = _build_token_permission_snapshot(body.access_token.strip(), info if isinstance(info, dict) else None)
    permission_snapshot_json = json.dumps(permission_snapshot, ensure_ascii=False)
    conn = get_conn()
    ensure_token_source_columns(conn)
    _ensure_fb_token_permission_columns(conn)
    resource_team_id = team_id_for_create(user)
    actual_type_for_insert = _auto_detect_token_type(body.access_token) if body.token_type == "auto" else body.token_type
    resolved_source = _validate_token_role_source(actual_type_for_insert, body.token_source)
    cursor = conn.execute(
        """INSERT INTO fb_tokens (
               token_alias, access_token_enc, token_type, token_source, status,
               last_verified_at, note, matrix_id, permission_snapshot, permission_checked_at, team_id, owner_user_id
           ) VALUES (?,?,?,?,?,datetime('now','+8 hours'),?,?,?,datetime('now','+8 hours'),?,?)""",
        (body.token_alias, enc,
         actual_type_for_insert,
         resolved_source,
         "active", body.note or "",
         body.matrix_id if actual_type_for_insert == "operate" else None,
         permission_snapshot_json,
         resource_team_id,
         _owner_id_for_token(user))
    )
    token_id = cursor.lastrowid
    conn.commit()
    conn.close()
    # v4.1: 新增可用 Token 后自动匹配已导入账户并加入账户 Token 池（后台线程，不阻塞响应）
    actual_type = actual_type_for_insert
    if actual_type in ("operate", "manage"):  # 管理号和操作号都触发自动匹配
        import threading
        _access_token_copy = body.access_token.strip()
        _token_id_copy = token_id
        def _auto_match_op_bg():
            """
            Token 自动匹配逻辑：
            1. 调用 FB API 获取该 Token 有权限的所有广告账户
            2. 与系统已导入账户做交集匹配
            3. 匹配到的账户自动将该 Token 加入账户 Token 池
            4. 同步更新账户状态（回收/禁用等）
            """
            try:
                fb_accounts = _fetch_all_fb_adaccounts(
                    _access_token_copy,
                    "id,name,account_status,balance,amount_spent,spend_cap",
                    timeout=30,
                )
                if not fb_accounts:
                    logger.info(f"[TokenAutoMatch] token {_token_id_copy} 无可匹配账户")
                    return
                # 构建 FB 账户字典 {act_id: data}
                fb_map = {a["id"]: a for a in fb_accounts}
                c = get_conn()
                try:
                    # 获取系统已导入的所有账户
                    import_where = ["1=1"]
                    import_params = []
                    if resource_team_id is not None:
                        import_where.append("team_id=?")
                        import_params.append(resource_team_id)
                    if is_operator_user(user):
                        import_where.append("owner_user_id=?")
                        import_params.append(user_id(user))
                    imported = c.execute(
                        f"SELECT id, act_id, account_status FROM accounts WHERE {' AND '.join(import_where)}",
                        import_params,
                    ).fetchall()
                    matched = 0
                    status_updated = 0
                    for acc in imported:
                        act_id = acc["act_id"]
                        fb_info = fb_map.get(act_id)
                        if fb_info:
                            # 匹配成功：将 Token 加入该账户 Token 池（如未已存在）
                            existing_op = c.execute(
                                "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
                                (act_id, _token_id_copy)
                            ).fetchone()
                            if not existing_op:
                                # 获取当前最大优先级
                                max_pri = c.execute(
                                    "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (act_id,)
                                ).fetchone()[0] or 0
                                c.execute(
                                    """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                                       VALUES (?, ?, ?, 'active', '自动匹配导入', (SELECT token_type FROM fb_tokens WHERE id=?), datetime('now'))""",
                                    (act_id, _token_id_copy, max_pri + 1, _token_id_copy)
                                )
                                matched += 1
                            # 同步更新账户状态（FB返回的最新状态）
                            new_status = fb_info.get("account_status", acc["account_status"])
                            if new_status != acc["account_status"]:
                                c.execute(
                                    "UPDATE accounts SET account_status=?, updated_at=datetime('now') WHERE id=?",
                                    (new_status, acc["id"])
                                )
                                status_updated += 1
                        else:
                            # 该账户不在操作号权限范围内，可能已被回收/禁用
                            # 不强制更新状态（可能只是操作号权限不够，不代表账户真的被禁）
                            pass
                    c.commit()
                    logger.info(f"[TokenAutoMatch] token {_token_id_copy} 自动匹配 {matched} 个账户加入账户 Token 池，更新 {status_updated} 个账户状态")
                except Exception as e:
                    c.rollback()
                    logger.error(f"[TokenAutoMatch] 写入失败: {e}")
                finally:
                    c.close()
            except Exception as e:
                logger.error(f"[TokenAutoMatch] Token 自动匹配失败: {e}")
        threading.Thread(target=_auto_match_op_bg, daemon=True).start()
    return {
        "success": True,
        "token_id": token_id,
        "user_info": info,
        "auto_match_started": actual_type in ("operate", "manage"),
        "auto_match_token_type": actual_type,
    }


@router.put("/tokens/{token_id}")
def update_token(token_id: int, body: TokenUpdate, user=Depends(get_current_user)):
    """更新Token（用于Token失效后重新授权）"""
    if not body.access_token.strip():
        raise HTTPException(400, "Access Token 不能为空")

    ok, info = _verify_fb_token(body.access_token)
    if not ok:
        raise HTTPException(400, f"Token验证失败: {info}")

    enc = encrypt_token(body.access_token.strip())
    permission_snapshot = _build_token_permission_snapshot(body.access_token.strip(), info if isinstance(info, dict) else None)
    permission_snapshot_json = json.dumps(permission_snapshot, ensure_ascii=False)
    conn = get_conn()
    ensure_token_source_columns(conn)
    _ensure_fb_token_permission_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    token_row = conn.execute(
        "SELECT id, token_type, token_source FROM fb_tokens WHERE id=?",
        (token_id,),
    ).fetchone()
    if not token_row:
        conn.close()
        raise HTTPException(404, "Token 不存在")
    actual_type = body.token_type or token_row["token_type"]
    actual_source = _validate_token_role_source(
        actual_type,
        body.token_source if body.token_source is not None else token_row["token_source"],
    )
    updates = [
        "access_token_enc=?",
        "status='active'",
        "last_verified_at=datetime('now','+8 hours')",
        "permission_snapshot=?",
        "permission_checked_at=datetime('now','+8 hours')",
        "token_type=?",
        "token_source=?",
    ]
    params = [enc, permission_snapshot_json, actual_type, actual_source]
    if body.token_alias:
        updates.append("token_alias=?")
        params.append(body.token_alias)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if actual_type != "operate":
        updates.append("matrix_id=NULL")
    params.append(token_id)
    conn.execute(f"UPDATE fb_tokens SET {', '.join(updates)} WHERE id=?", params)
    claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
    conn.execute("UPDATE accounts SET enabled=1 WHERE token_id=?", (token_id,))
    conn.commit()
    conn.close()
    # Token 更新后触发自动发现（后台线程）
    import threading as _th_upd
    def _trigger_discovery_bg():
        try:
            from core.scheduler import run_token_account_discovery
            run_token_account_discovery()
        except Exception as _e:
            logger.warning(f"[TokenUpdate] 触发自动发现失败: {_e}")
    _th_upd.Thread(target=_trigger_discovery_bg, daemon=True).start()
    return {"success": True, "user_info": info}


@router.patch("/tokens/{token_id}/type")
def update_token_type(token_id: int, body: TokenTypeUpdate, user=Depends(get_current_user)):
    """单独修改 Token 类型，无需重新录入 Token。"""
    allowed = {"manage", "operate", "user"}
    if body.token_type not in allowed:
        raise HTTPException(400, f"token_type 必须是 {sorted(allowed)} 之一")
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute(
        "SELECT id, token_source FROM fb_tokens WHERE id=?",
        (token_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Token 不存在")
    resolved_source = _validate_token_role_source(body.token_type, row["token_source"])
    if body.token_type == "operate":
        conn.execute(
            "UPDATE fb_tokens SET token_type=?, token_source=? WHERE id=?",
            (body.token_type, resolved_source, token_id),
        )
    else:
        conn.execute(
            "UPDATE fb_tokens SET token_type=?, token_source=?, matrix_id=NULL WHERE id=?",
            (body.token_type, resolved_source, token_id),
        )
    claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
    conn.commit()
    conn.close()
    return {"success": True, "token_type": body.token_type, "token_source": resolved_source}


@router.patch("/tokens/{token_id}/source")
def update_token_source(token_id: int, body: TokenSourceUpdate, user=Depends(get_current_user)):
    """单独修改 Token 来源。"""
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute(
        "SELECT id, token_type FROM fb_tokens WHERE id=?",
        (token_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Token 不存在")
    resolved_source = _validate_token_role_source(row["token_type"], body.token_source)
    conn.execute("UPDATE fb_tokens SET token_source=? WHERE id=?", (resolved_source, token_id))
    claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
    conn.commit()
    conn.close()
    return {"success": True, "token_source": resolved_source}



@router.patch("/tokens/{token_id}/matrix")
def update_token_matrix(token_id: int, body: dict, user=Depends(get_current_user)):
    """修改 Token 的矩阵归属。"""
    matrix_id = body.get("matrix_id")
    if matrix_id is not None and matrix_id != 0:
        try:
            matrix_id = int(matrix_id)
        except (ValueError, TypeError):
            raise HTTPException(400, "matrix_id 必须是整数或 null")
    else:
        matrix_id = None
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute(
        "SELECT id, token_type, token_source FROM fb_tokens WHERE id=?",
        (token_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Token 不存在")
    if row["token_type"] != "operate":
        if matrix_id is not None:
            conn.close()
            raise HTTPException(400, "只有操作号 Token 才能分配矩阵")
        conn.execute("UPDATE fb_tokens SET matrix_id=NULL WHERE id=?", (token_id,))
    else:
        _validate_token_role_source(row["token_type"], row["token_source"])
        conn.execute("UPDATE fb_tokens SET matrix_id=? WHERE id=?", (matrix_id, token_id))
    claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
    conn.commit()
    conn.close()
    return {"success": True, "matrix_id": matrix_id}
@router.delete("/tokens/{token_id}")
def delete_token(token_id: int, user=Depends(get_current_user)):
    """删除Token（仅检查启用状态账户，disabled账户不阻止删除）"""
    conn = get_conn()
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    # 只统计 enabled=1 的活跃账户，disabled 账户不阻止删除
    count = conn.execute(
        "SELECT COUNT(*) as c FROM accounts WHERE token_id=? AND enabled=1",
        (token_id,)
    ).fetchone()["c"]
    if count > 0:
        conn.close()
        raise HTTPException(400, f"该Token下还有 {count} 个启用账户，请先在账户管理中更换或删除这些账户")
    # 清理残留的 disabled 账户的 token_id 引用（防止幽灵关联）
    conn.execute("UPDATE accounts SET token_id=NULL WHERE token_id=? AND enabled=0", (token_id,))
    # 同步清理操作号池关联记录（防止幽灵token导致巡检失败）
    conn.execute("DELETE FROM account_op_tokens WHERE token_id=?", (token_id,))
    conn.execute("DELETE FROM fb_tokens WHERE id=?", (token_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/tokens/{token_id}/verify")
def verify_token_now(token_id: int, user=Depends(get_current_user)):
    """立即验证Token有效性"""
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute("SELECT access_token_enc, token_type FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Token不存在")

    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]
    ok, info = _verify_fb_token(token)
    permission_snapshot = (
        _build_token_permission_snapshot(token, info if isinstance(info, dict) else None)
        if ok else
        _build_failed_token_permission_snapshot(str(info))
    )
    permission_snapshot_json = json.dumps(permission_snapshot, ensure_ascii=False)

    conn = get_conn()
    _ensure_fb_token_permission_columns(conn)
    status = "active" if ok else "expired"
    conn.execute(
        """UPDATE fb_tokens
           SET status=?,
               last_verified_at=datetime('now','+8 hours'),
               permission_snapshot=?,
               permission_checked_at=datetime('now','+8 hours')
           WHERE id=?""",
        (status, permission_snapshot_json, token_id)
    )
    claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
    account_where, account_params = ["token_id=?"], [token_id]
    apply_team_scope(account_where, account_params, user, "team_id", include_unassigned=False)
    _apply_account_owner_scope(account_where, account_params, user, "")
    op_scope_where, op_scope_params = ["a.act_id=account_op_tokens.act_id"], []
    apply_team_scope(op_scope_where, op_scope_params, user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(op_scope_where, op_scope_params, user, "a")
    op_scope_sql = (
        "token_id=? AND EXISTS ("
        f"SELECT 1 FROM accounts a WHERE {' AND '.join(op_scope_where)}"
        ")"
    )
    if not ok:
        conn.execute(
            f"UPDATE accounts SET enabled=0 WHERE {' AND '.join(account_where)}",
            account_params,
        )
        # Token 失效时实时标记 account_op_tokens 为 disabled
        conn.execute(
            f"UPDATE account_op_tokens SET status='disabled' WHERE {op_scope_sql}",
            [token_id] + op_scope_params,
        )
    else:
        # Token 验证成功时恢复 account_op_tokens 为 active
        conn.execute(
            f"UPDATE account_op_tokens SET status='active' WHERE {op_scope_sql}",
            [token_id] + op_scope_params,
        )
    conn.commit()
    conn.close()
    if ok:
        name = info.get('name', '') if isinstance(info, dict) else ''
        msg = f'Token 验证成功' + (f'（{name}）' if name else '')
    else:
        msg = f'Token 已失效：{info}'
        raise HTTPException(400, msg)
    return {
        "success": ok,
        "status": status,
        "info": info,
        "permissions": permission_snapshot,
        "message": msg,
    }


@router.post("/tokens/{token_id}/rematch-accounts")
def rematch_op_token_accounts(token_id: int, user=Depends(get_current_user)):
    """手动触发操作号重新匹配已导入账户（用于操作号添加后匹配失败的情况）"""
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute(
        "SELECT id, token_type, token_source, access_token_enc, status, team_id FROM fb_tokens WHERE id=?",
        (token_id,)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Token不存在")
    resource_team_id = _matching_team_id(user, row["team_id"])
    claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
    conn.commit()
    conn.close()
    if row["token_type"] == "operate":
        _validate_token_role_source(row["token_type"], row["token_source"])
    token = decrypt_token(row["access_token_enc"])
    # 调用 FB API 获取该操作号有权限的广告账户
    try:
        fb_accounts = _fetch_all_fb_adaccounts(
            token,
            "id,name,account_status",
            timeout=20,
        )
    except Exception as e:
        raise HTTPException(500, f"调用 Facebook API 失败: {str(e)}")
    fb_map = {a["id"]: a for a in fb_accounts}
    # 与系统已导入账户做交集匹配
    conn = get_conn()
    try:
        if resource_team_id is None:
            imported = conn.execute("SELECT id, act_id, account_status FROM accounts").fetchall()
        else:
            imported = conn.execute(
                "SELECT id, act_id, account_status FROM accounts WHERE team_id=?",
                (resource_team_id,),
            ).fetchall()
        matched = 0
        already = 0
        for acc in imported:
            act_id = acc["act_id"]
            if act_id in fb_map:
                existing = conn.execute(
                    "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
                    (act_id, token_id)
                ).fetchone()
                if not existing:
                    max_pri = conn.execute(
                        "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (act_id,)
                    ).fetchone()[0] or 0
                    conn.execute(
                        """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                           VALUES (?, ?, ?, 'active', '手动重匹配', (SELECT token_type FROM fb_tokens WHERE id=?), datetime('now'))""",
                        (act_id, token_id, max_pri + 1, token_id)
                    )
                    matched += 1
                else:
                    already += 1
        conn.commit()
        logger.info(f"[Rematch] 操作号 {token_id} 手动重匹配: 新增 {matched} 个，已存在 {already} 个，FB账户总数 {len(fb_accounts)}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"写入数据库失败: {str(e)}")
    finally:
        conn.close()
    return {
        "success": True,
        "matched": matched,
        "already_linked": already,
        "fb_total": len(fb_accounts),
        "imported_total": len(imported),
        "message": f"匹配完成：新增关联 {matched} 个账户，已有 {already} 个已关联"
    }


@router.patch("/tokens/{token_id}/owner")
def update_token_owner(token_id: int, body: dict, user=Depends(get_current_user)):
    """Assign token to a specific operator (admin+ only)"""
    if user.get("role") not in ("superadmin", "admin"):
        raise HTTPException(status_code=403, detail="Only admin can assign token ownership")
    owner_user_id = body.get("owner_user_id")
    conn = get_conn()
    row = conn.execute("SELECT id, team_id FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Token not found")
    if not user.get("is_superadmin") and row["team_id"] != user.get("team_id"):
        conn.close()
        raise HTTPException(status_code=403, detail="Token belongs to another team")
    owner_user_id = _validate_token_owner(conn, owner_user_id, token_id, user)
    conn.execute("UPDATE fb_tokens SET owner_user_id=? WHERE id=?", (owner_user_id, token_id))
    conn.commit()
    conn.close()
    return {"success": True, "token_id": token_id, "owner_user_id": owner_user_id}

@router.get("/tokens/{token_id}/fetch-accounts")
def fetch_token_accounts(token_id: int, user=Depends(get_current_user)):
    """拉取Token授权的所有广告账户列表（供用户勾选导入）"""
    # 先读取token，立即关闭连接
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute(
        "SELECT access_token_enc, status, token_type, token_source, team_id FROM fb_tokens WHERE id=?",
        (token_id,),
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "Token不存在")
    if row["status"] != "active":
        raise HTTPException(400, "Token已失效，请先更新Token")

    token_type = row["token_type"]
    if token_type == "operate":
        _validate_token_role_source(token_type, row["token_source"])
    token = decrypt_token(row["access_token_enc"])
    resource_team_id = row["team_id"]
    if resource_team_id is None and not normalize_user_claims(user).get("is_superadmin"):
        resource_team_id = team_id_for_create(user)

    # 调用FB API（不持有数据库连接）
    try:
        fb_accounts = _fetch_all_fb_adaccounts(
            token,
            ACCOUNT_DETAIL_FIELDS,
            timeout=30,
        )
    except Exception as e:
        raise HTTPException(400, f"拉取账户失败: {e}")

    # FB API调用完毕后，再开数据库连接
    conn = get_conn()
    _ensure_account_read_columns(conn)
    imported_where, imported_params = [], []
    apply_team_scope(imported_where, imported_params, user, "team_id", include_unassigned=False)
    _apply_account_owner_scope(imported_where, imported_params, user, "")
    imported_clause = ("WHERE " + " AND ".join(imported_where)) if imported_where else ""
    imported = {
        r["act_id"]
        for r in conn.execute(f"SELECT act_id FROM accounts {imported_clause}", imported_params).fetchall()
    }
    conn.close()
    # 操作号导入时：遍历所有管理号Token，拉取其覆盖的账户集合，判断每个账户是否有管理号兜底
    manage_token_status = {}  # act_id -> {"status": "active"|None, "alias": str}
    if token_type == "operate":
        _conn_mgr = get_conn()
        mgr_where = ["token_type='manage'", "status='active'"]
        mgr_params = []
        if resource_team_id is None:
            mgr_where.append("team_id IS NULL")
        else:
            mgr_where.append("team_id=?")
            mgr_params.append(resource_team_id)
        _mgr_tokens = _conn_mgr.execute(
            f"SELECT id, access_token_enc, status, token_alias FROM fb_tokens WHERE {' AND '.join(mgr_where)}",
            mgr_params,
        ).fetchall()
        _conn_mgr.close()
        # 用每个管理号Token调用FB API，获取其覆盖的账户列表
        import concurrent.futures
        def _fetch_mgr_accounts(mgr_row):
            try:
                _tk = decrypt_token(mgr_row["access_token_enc"])
                _ids = _fetch_all_fb_adaccount_ids(_tk, timeout=15)
                return [(act_id, {"status": mgr_row["status"], "alias": mgr_row["token_alias"]}) for act_id in _ids]
            except Exception:
                return []
        if _mgr_tokens:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as _pool:
                _futures = [_pool.submit(_fetch_mgr_accounts, t) for t in _mgr_tokens]
                for _f in concurrent.futures.as_completed(_futures):
                    for _act_id, _info in _f.result():
                        # 已有active管理号则不覆盖
                        if _act_id not in manage_token_status or manage_token_status[_act_id]["status"] != "active":
                            manage_token_status[_act_id] = _info

    result = []
    for acc in fb_accounts:
        act_id = acc["id"]
        info = _normalize_account_info(act_id, acc)
        result.append({
            "act_id": act_id,
            "name": info.get("name", ""),
            "currency": info.get("currency", "USD"),
            "timezone": info.get("timezone", ""),
            "timezone_name": info.get("timezone_name", ""),
            "timezone_offset_hours_utc": info.get("timezone_offset_hours_utc"),
            "account_status": info.get("account_status", 1),
            "balance": info.get("balance"),
            "spend_cap": info.get("spend_cap"),
            "amount_spent": info.get("amount_spent"),
            "spending_limit": info.get("spending_limit"),
            "already_imported": act_id in imported
        })
        # 操作号：附带管理号状态（管理号未覆盖的账户展示为不可导入）
        if token_type == "operate":
            _mgr = manage_token_status.get(act_id, {})
            result[-1]["mgr_status"] = _mgr.get("status")
            result[-1]["mgr_alias"] = _mgr.get("alias")
            result[-1]["mgr_ok"] = _mgr.get("status") == "active"

    return {"accounts": result, "total": len(result), "token_type": token_type}


def _fetch_single_account(act_id: str, token: str) -> dict:
    """Fetch one ad account. Errors are explicit so callers do not persist fake defaults."""
    normalized_act_id = _normalize_act_id(act_id)
    try:
        resp = requests.get(
            f"{FB_API_BASE}/{normalized_act_id}",
            params={"access_token": token, "fields": ACCOUNT_DETAIL_FIELDS},
            timeout=10
        )
        info = resp.json()
        if resp.status_code >= 400 or "error" in info:
            return {"act_id": normalized_act_id, "error": _format_fb_graph_error(info)}
        return _normalize_account_info(normalized_act_id, info)
    except Exception as e:
        return {"act_id": normalized_act_id, "error": str(e)}


def _resolve_read_token(act_id: str) -> Optional[str]:
    try:
        from services.token_manager import ACTION_READ, get_exec_token

        token = get_exec_token(act_id, ACTION_READ)
        if token:
            return token
    except Exception:
        pass

    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT t.access_token_enc
            FROM accounts a
            JOIN fb_tokens t ON t.id = a.token_id
            WHERE a.act_id = ?
              AND t.status = 'active'
              AND t.access_token_enc IS NOT NULL
            LIMIT 1
            """,
            (act_id,),
        ).fetchone()
        if row:
            token = decrypt_token(row["access_token_enc"])
            if token:
                return token

        row = conn.execute(
            """
            SELECT access_token_enc
            FROM fb_tokens
            WHERE status = 'active'
              AND access_token_enc IS NOT NULL
            LIMIT 1
            """
        ).fetchone()
        if row:
            token = decrypt_token(row["access_token_enc"])
            if token:
                return token
    finally:
        conn.close()

    return None


@router.post("/tokens/{token_id}/import-accounts")
def import_accounts(token_id: int, body: AccountImport, user=Depends(get_current_user)):
    """批量导入选中的广告账户
    
    修复: 先批量并发调用FB API获取所有账户信息，再一次性写入数据库
    避免在持有数据库连接时进行网络请求导致的数据库锁死
    """
    if not body.act_ids:
        return {"success": True, "imported": [], "skipped": []}
    act_ids = _normalize_import_act_ids(body.act_ids)
    if not act_ids:
        return {"success": True, "imported": [], "skipped": []}

    # Step 1: 读取token，立即关闭连接
    conn = get_conn()
    ensure_token_source_columns(conn)
    assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
    row = conn.execute(
        "SELECT access_token_enc, status, token_type, token_source, team_id FROM fb_tokens WHERE id=?",
        (token_id,),
    ).fetchone()

    if not row:
        conn.close()
        raise HTTPException(404, "Token不存在")
    if row["status"] != "active":
        conn.close()
        raise HTTPException(400, "Token已失效，请先更新 Token")
    claimed_token_team_id = None
    if row["team_id"] is None:
        claimed_token_team_id = claim_row_for_team(conn, "fb_tokens", "id", token_id, user)
        if claimed_token_team_id is not None:
            conn.commit()
    conn.close()

    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]
    resource_team_id = (
        row["team_id"]
        if row["team_id"] is not None
        else (claimed_token_team_id if claimed_token_team_id is not None else team_id_for_create(user))
    )
    if token_type_import == "operate":
        _validate_token_role_source(token_type_import, row["token_source"])

    # Step 2: 并发读取账户详情。操作号导入仍要求管理号可读取，保证后续巡检/关停兜底。
    account_infos = {}
    read_meta = {}
    failed = []
    max_workers = min(10, len(act_ids))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _resolve_account_info,
                act_id,
                preferred_token_id=token_id,
                preferred_token_plain=token,
                require_manage_read=(token_type_import == "operate"),
                team_id=resource_team_id,
            ): act_id
            for act_id in act_ids
        }
        for future in as_completed(futures):
            act_id = futures[future]
            resolved = future.result()
            if resolved.get("ok"):
                account_infos[act_id] = resolved["info"]
                read_meta[act_id] = resolved
            else:
                err = resolved.get("error") or "no_read_token"
                if err == "no_manage_read_token":
                    err = "无可读取该账户的管理号；操作号导入前需先导入覆盖该账户的管理号"
                elif err == "no_read_token":
                    err = "没有任何可读取该账户的有效 Token"
                failed.append({"act_id": act_id, "error": err})
    successful_act_ids = [act_id for act_id in act_ids if act_id in account_infos]

    # Step 3: 所有FB API调用完毕后，一次性写入数据库
    imported = []
    skipped = []

    conn = get_conn()
    team_blocked_act_ids = set()
    import_owner_user_id = _owner_user_id_for_import(user)
    try:
        _ensure_account_read_columns(conn)
        # 获取已存在的账户
        existing_rows = {
            r["act_id"]: dict(r)
            for r in conn.execute("SELECT id, act_id, team_id, owner_user_id FROM accounts").fetchall()
        }

        for act_id in act_ids:
            existing = existing_rows.get(act_id)
            if existing:
                existing_team_id = existing.get("team_id")
                if (
                    resource_team_id is not None
                    and existing_team_id is not None
                    and existing_team_id != resource_team_id
                ):
                    failed.append({"act_id": act_id, "error": "该账户已归属其他团队，请联系超级管理员处理"})
                    team_blocked_act_ids.add(act_id)
                    continue
                existing_owner_id = existing.get("owner_user_id")
                if (
                    import_owner_user_id is not None
                    and existing_owner_id is not None
                    and int(existing_owner_id) != import_owner_user_id
                ):
                    failed.append({"act_id": act_id, "error": "Account belongs to another operator"})
                    continue
                updates = []
                update_params = []
                if resource_team_id is not None and existing_team_id is None:
                    updates.append("team_id=?")
                    update_params.append(resource_team_id)
                    updates.append("token_id=COALESCE(token_id,?)")
                    update_params.append(token_id)
                if (
                    import_owner_user_id is not None
                    and existing_owner_id is None
                    and act_id in successful_act_ids
                ):
                    updates.append("owner_user_id=?")
                    update_params.append(import_owner_user_id)
                if updates:
                    updates.append("updated_at=datetime('now')")
                    update_params.append(existing["id"])
                    conn.execute(
                        f"UPDATE accounts SET {', '.join(updates)} WHERE id=?",
                        update_params,
                    )
                skipped.append(act_id)
                continue

            info = account_infos.get(act_id)
            if not info:
                continue
            conn.execute(
                """INSERT INTO accounts (
                       act_id, name, currency, timezone, timezone_name, timezone_offset_hours_utc,
                       token_id, enabled, balance, account_status, spend_cap, page_id, pixel_id,
                       amount_spent, spending_limit, team_id, owner_user_id
                   )
                   VALUES (?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?)""",
                (
                    act_id,
                    info.get("name", act_id),
                    info.get("currency", "USD"),
                    info.get("timezone", "UTC"),
                    info.get("timezone_name") or info.get("timezone", "UTC"),
                    info.get("timezone_offset_hours_utc"),
                    token_id,
                    info.get("balance"),
                    info.get("account_status", 1),
                    info.get("spend_cap"),
                    body.page_id or info.get("page_id"),
                    body.pixel_id or info.get("pixel_id"),
                    info.get("amount_spent"),
                    info.get("spending_limit"),
                    resource_team_id,
                    import_owner_user_id,
                )
            )
            imported.append({
                "act_id": act_id,
                "name": info.get("name", act_id),
                "read_token_id": read_meta.get(act_id, {}).get("read_token_id"),
                "read_token_type": read_meta.get(act_id, {}).get("read_token_type"),
                "read_source": read_meta.get(act_id, {}).get("read_source"),
            })
        conn.commit()
        if team_blocked_act_ids:
            successful_act_ids = [act_id for act_id in successful_act_ids if act_id not in team_blocked_act_ids]
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"导入失败: {str(e)}")
    finally:
        conn.close()

    # v4.3: 导入/重复导入账户后，立即将当前 token 与读取兜底 token 写入 account_op_tokens。
    # 注意：已存在账户会进入 skipped，但当前 token 仍应补齐关联，否则必须手点“重新匹配”。
    if successful_act_ids:
        _c_link = get_conn()
        try:
            _linked = 0
            _restored = 0
            for _act_id in successful_act_ids:
                _link_token_ids = [(token_id, token_type_import, "导入时绑定")]
                _read_token_id = read_meta.get(_act_id, {}).get("read_token_id")
                if _read_token_id and _read_token_id != token_id:
                    _rt = _c_link.execute(
                        "SELECT token_type FROM fb_tokens WHERE id=?",
                        (_read_token_id,)
                    ).fetchone()
                    if _rt:
                        _link_token_ids.append((_read_token_id, _rt["token_type"], "导入读取兜底"))
                _seen_link_ids = set()
                for _link_token_id, _link_token_type, _link_note in _link_token_ids:
                    if not _link_token_id or _link_token_id in _seen_link_ids:
                        continue
                    _seen_link_ids.add(_link_token_id)
                    _existing_link = _c_link.execute(
                        "SELECT id, status FROM account_op_tokens WHERE act_id=? AND token_id=?",
                        (_act_id, _link_token_id)
                    ).fetchone()
                    if not _existing_link:
                        _max_pri = _c_link.execute(
                            "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (_act_id,)
                        ).fetchone()[0] or 0
                        _c_link.execute(
                            """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                               VALUES (?, ?, ?, 'active', ?, ?, datetime('now'))""",
                            (_act_id, _link_token_id, _max_pri + 1, _link_note, _link_token_type)
                        )
                        _linked += 1
                    elif _existing_link["status"] != "active":
                        _c_link.execute(
                            "UPDATE account_op_tokens SET status='active', note=? WHERE id=?",
                            (_link_note, _existing_link["id"])
                        )
                        _restored += 1
            _c_link.commit()
            logger.info(
                f"[ImportLink] 导入 token_id={token_id} 处理 {len(successful_act_ids)} 个账户，"
                f"新增关联 {_linked} 条，恢复 {_restored} 条"
            )
        except Exception as _le:
            _c_link.rollback()
            logger.error(f"[ImportLink] 写入关联失败: {_le}")
        finally:
            _c_link.close()

    auto_match_result = {"matched": 0, "restored": 0, "already_linked": 0, "token_checked": 0, "token_failed": 0}
    if successful_act_ids:
        try:
            auto_match_result = _auto_link_tokens_for_accounts(
                successful_act_ids,
                user,
                team_id=resource_team_id,
                note="import_auto_match",
            )
            logger.info(
                "[ImportLink] auto matched accounts=%s tokens=%s matched=%s restored=%s already=%s failed_tokens=%s",
                len(successful_act_ids),
                auto_match_result.get("token_checked"),
                auto_match_result.get("matched"),
                auto_match_result.get("restored"),
                auto_match_result.get("already_linked"),
                auto_match_result.get("token_failed"),
            )
        except Exception as _match_exc:
            logger.warning("[ImportLink] auto match failed: %s", _match_exc)

    if failed and not imported and not skipped:
        first = failed[0]
        raise HTTPException(400, f"导入失败：无法读取账户 {first.get('act_id')}（{first.get('error')}）")
    return {"success": True, "imported": imported, "skipped": skipped, "failed": failed, "auto_match": auto_match_result}


@router.post("/import-accounts-auto")
def smart_import_accounts(body: AccountImport, user=Depends(get_current_user)):
    """Import accounts by ad account id without forcing the operator to pick a token first."""
    if not body.act_ids:
        return {"success": True, "imported": [], "skipped": [], "failed": [], "auto_match": {}}
    act_ids = _normalize_import_act_ids(body.act_ids)
    if not act_ids:
        return {"success": True, "imported": [], "skipped": [], "failed": [], "auto_match": {}}

    account_infos = {}
    read_meta = {}
    failed = []
    with ThreadPoolExecutor(max_workers=min(10, len(act_ids))) as executor:
        futures = {
            executor.submit(_resolve_account_info_for_smart_import, act_id, user): act_id
            for act_id in act_ids
        }
        for future in as_completed(futures):
            act_id = futures[future]
            resolved = future.result()
            if resolved.get("ok"):
                account_infos[act_id] = resolved["info"]
                read_meta[act_id] = resolved
            else:
                failed.append({"act_id": act_id, "error": resolved.get("error") or "no_manage_read_token"})

    imported = []
    skipped = []
    import_owner_user_id = _owner_user_id_for_import(user)
    claims = normalize_user_claims(user)
    fixed_team_id = None if claims.get("is_superadmin") else team_id_for_create(user)
    match_groups: dict[Optional[int], list[str]] = {}

    conn = get_conn()
    try:
        _ensure_account_read_columns(conn)
        existing_rows = {
            r["act_id"]: dict(r)
            for r in conn.execute("SELECT id, act_id, team_id, owner_user_id FROM accounts").fetchall()
        }

        for act_id in act_ids:
            info = account_infos.get(act_id)
            if not info:
                continue
            meta = read_meta.get(act_id, {})
            resolved_team_id = fixed_team_id if fixed_team_id is not None else meta.get("read_team_id")
            read_token_id = meta.get("read_token_id")
            existing = existing_rows.get(act_id)
            if existing:
                existing_team_id = existing.get("team_id")
                if (
                    fixed_team_id is not None
                    and existing_team_id is not None
                    and existing_team_id != fixed_team_id
                ):
                    failed.append({"act_id": act_id, "error": "account_belongs_to_another_team"})
                    continue
                existing_owner_id = existing.get("owner_user_id")
                if (
                    import_owner_user_id is not None
                    and existing_owner_id is not None
                    and int(existing_owner_id) != import_owner_user_id
                ):
                    failed.append({"act_id": act_id, "error": "account_belongs_to_another_operator"})
                    continue
                updates = []
                update_params = []
                if resolved_team_id is not None and existing_team_id is None:
                    updates.append("team_id=?")
                    update_params.append(resolved_team_id)
                if read_token_id:
                    updates.append("token_id=COALESCE(token_id,?)")
                    update_params.append(read_token_id)
                if import_owner_user_id is not None and existing_owner_id is None:
                    updates.append("owner_user_id=?")
                    update_params.append(import_owner_user_id)
                if updates:
                    updates.append("updated_at=datetime('now')")
                    update_params.append(existing["id"])
                    conn.execute(
                        f"UPDATE accounts SET {', '.join(updates)} WHERE id=?",
                        update_params,
                    )
                skipped.append(act_id)
                match_team_id = existing_team_id if existing_team_id is not None else resolved_team_id
                match_groups.setdefault(match_team_id, []).append(act_id)
                continue

            conn.execute(
                """INSERT INTO accounts (
                       act_id, name, currency, timezone, timezone_name, timezone_offset_hours_utc,
                       token_id, enabled, balance, account_status, spend_cap, page_id, pixel_id,
                       amount_spent, spending_limit, team_id, owner_user_id
                   )
                   VALUES (?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?)""",
                (
                    act_id,
                    info.get("name", act_id),
                    info.get("currency", "USD"),
                    info.get("timezone", "UTC"),
                    info.get("timezone_name") or info.get("timezone", "UTC"),
                    info.get("timezone_offset_hours_utc"),
                    read_token_id,
                    info.get("balance"),
                    info.get("account_status", 1),
                    info.get("spend_cap"),
                    body.page_id or info.get("page_id"),
                    body.pixel_id or info.get("pixel_id"),
                    info.get("amount_spent"),
                    info.get("spending_limit"),
                    resolved_team_id,
                    import_owner_user_id,
                ),
            )
            imported.append({
                "act_id": act_id,
                "name": info.get("name", act_id),
                "read_token_id": read_token_id,
                "read_token_type": meta.get("read_token_type"),
                "read_source": meta.get("read_source"),
            })
            match_groups.setdefault(resolved_team_id, []).append(act_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"smart import failed: {str(e)}")
    finally:
        conn.close()

    auto_match_result = {
        "matched": 0,
        "restored": 0,
        "already_linked": 0,
        "token_checked": 0,
        "token_failed": 0,
        "accounts": [],
        "failed_tokens": [],
    }
    for team_id, group_act_ids in match_groups.items():
        try:
            result = _auto_link_tokens_for_accounts(
                group_act_ids,
                user,
                team_id=team_id,
                note="smart_import_auto_match",
            )
            for key in ("matched", "restored", "already_linked", "token_checked", "token_failed"):
                auto_match_result[key] += int(result.get(key) or 0)
            auto_match_result["accounts"].extend(result.get("accounts") or [])
            auto_match_result["failed_tokens"].extend(result.get("failed_tokens") or [])
        except Exception as exc:
            logger.warning("[SmartImport] auto match failed team_id=%s: %s", team_id, exc)
    auto_match_result["failed_tokens"] = auto_match_result["failed_tokens"][:20]

    if failed and not imported and not skipped:
        first = failed[0]
        raise HTTPException(400, f"smart import failed: cannot read account {first.get('act_id')} ({first.get('error')})")
    return {"success": True, "imported": imported, "skipped": skipped, "failed": failed, "auto_match": auto_match_result}


# ── 账户管理 ──────────────────────────────────────────────────────────────

# 默认汇率表（1单位外币 = X USD），用于无法获取实时汇率时的备用
_DEFAULT_RATES = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
    "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74,
    "AUD": 0.65, "CAD": 0.74, "BRL": 0.20, "MXN": 0.058,
    "CLP": 0.0011, "COP": 0.00025, "PEN": 0.27, "ARS": 0.001,
    "THB": 0.028, "VND": 0.000040, "IDR": 0.000063, "PHP": 0.017,
    "MYR": 0.21, "INR": 0.012, "TRY": 0.031, "ZAR": 0.053,
    # 补充常见货币
    "BDT": 0.0091, "PKR": 0.0036, "LKR": 0.0031, "NPR": 0.0075,
    "KRW": 0.00072, "CHF": 1.12, "NZD": 0.60, "SEK": 0.096,
    "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "CZK": 0.044,
    "HUF": 0.0028, "RON": 0.22, "BGN": 0.55, "HRK": 0.14,
    "AED": 0.272, "SAR": 0.267, "QAR": 0.275, "KWD": 3.26,
    "BHD": 2.65, "OMR": 2.60, "JOD": 1.41, "EGP": 0.021,
    "MAD": 0.099, "TND": 0.32, "GHS": 0.067, "NGN": 0.00065,
    "KES": 0.0077, "TZS": 0.00038, "UGX": 0.00027, "ETB": 0.0088,
    "UAH": 0.027, "KZT": 0.0022, "UZS": 0.000079, "GEL": 0.37,
    "AMD": 0.0026, "AZN": 0.59, "BYN": 0.31, "MDL": 0.056,
    "RSD": 0.0093, "MKD": 0.018, "ALL": 0.011, "BAM": 0.55,
    "CRC": 0.0019, "GTQ": 0.13, "HNL": 0.040, "NIO": 0.027,
    "PAB": 1.0, "DOP": 0.017, "JMD": 0.0064, "TTD": 0.15,
    "BBD": 0.50, "BSD": 1.0, "BZD": 0.50, "GYD": 0.0048,
    "SRD": 0.029, "UYU": 0.026, "PYG": 0.000135, "BOB": 0.145,
    "VES": 0.000027, "CUP": 0.042,
}

def _to_usd(amount, currency: str) -> float:
    """将任意货币金额转换为 USD。currency_rates 存的是 1 USD = X 本币。"""
    if amount is None:
        return 0.0
    cur = (currency or "USD").upper().strip()
    if cur == "USD":
        return round(float(amount), 2)
    try:
        conn = get_conn()
        row = conn.execute("SELECT rate FROM currency_rates WHERE currency=?", (cur,)).fetchone()
        conn.close()
        if row and row["rate"]:
            db_rate = float(row["rate"])
            if db_rate > 0:
                return round(float(amount) / db_rate, 2)
    except Exception:
        pass
    rate = _DEFAULT_RATES.get(cur, 1.0)
    return round(float(amount) * rate, 2)


def _to_usd(amount, currency: str) -> float:
    if amount is None:
        return 0.0
    return round(_to_usd_guard(amount, currency), 2)


@router.post("/rematch-tokens")
def rematch_visible_account_tokens(user=Depends(get_current_user)):
    """Scan visible accounts and bind same-team tokens that can see them."""
    _require_operator_user(user)
    conn = get_conn()
    try:
        where, params = ["1=1"], []
        apply_team_scope(where, params, user, "team_id", include_unassigned=False)
        _apply_account_owner_scope(where, params, user, "")
        rows = conn.execute(
            f"SELECT act_id, team_id FROM accounts WHERE {' AND '.join(where)} ORDER BY team_id, id",
            params,
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return {
            "success": True,
            "scanned_accounts": 0,
            "matched": 0,
            "restored": 0,
            "already_linked": 0,
            "token_checked": 0,
            "token_failed": 0,
            "accounts": [],
            "failed_tokens": [],
        }

    grouped: dict[Optional[int], list[str]] = {}
    for row in rows:
        grouped.setdefault(row["team_id"], []).append(row["act_id"])

    summary = {
        "matched": 0,
        "restored": 0,
        "already_linked": 0,
        "token_checked": 0,
        "token_failed": 0,
        "accounts": [],
        "failed_tokens": [],
    }
    for team_id, act_ids in grouped.items():
        result = _auto_link_tokens_for_accounts(
            act_ids,
            user,
            team_id=team_id,
            note="manual_bulk_token_rematch",
        )
        for key in ("matched", "restored", "already_linked", "token_checked", "token_failed"):
            summary[key] += int(result.get(key) or 0)
        summary["accounts"].extend(result.get("accounts") or [])
        summary["failed_tokens"].extend(result.get("failed_tokens") or [])

    return {
        "success": True,
        "scanned_accounts": len(rows),
        **summary,
        "failed_tokens": summary["failed_tokens"][:20],
        "message": (
            f"checked {len(rows)} accounts, matched {summary['matched']}, "
            f"restored {summary['restored']}, already linked {summary['already_linked']}"
        ),
    }


@router.get("")
def list_accounts(user=Depends(get_current_user)):
    """获取所有账户列表"""
    conn = get_conn()
    ensure_token_source_columns(conn)
    _ensure_account_read_columns(conn)
    where, params = ["1=1"], []
    apply_team_scope(where, params, user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(where, params, user, "a")
    rows = conn.execute(f"""
        SELECT a.id, a.act_id, a.name, a.currency, a.timezone, a.timezone_name, a.timezone_offset_hours_utc,
               a.enabled, a.note, a.page_id, a.pixel_id, a.beneficiary, a.payer, a.tw_advertiser_id, a.created_at,
               a.team_id, tm.name AS team_name, a.owner_user_id,
               COALESCE(NULLIF(ou.display_name,''), ou.username) AS owner_user_name,
               a.balance, a.account_status, a.spend_cap, a.amount_spent, a.spending_limit,
               a.read_permission_status, a.read_permission_error, a.read_permission_checked_at,
               COALESCE(a.mirror_enabled, 0) as mirror_enabled,
               COALESCE(a.sentinel_enabled, 0) as sentinel_enabled,
               COALESCE(a.warmup_state, '') as warmup_state,
               a.warmup_triggered_at, a.warmup_campaign_id,
               a.warmup_last_spend, a.warmup_last_checked_at,
               a.target_countries, a.target_age_min, a.target_age_max,
               a.target_gender, a.target_placements, a.target_objective_type, a.landing_url, a.form_link,
               t.token_alias, t.status as token_status, t.matrix_id,
               tp.page_name,
               (SELECT MAX(created_at) FROM action_logs WHERE act_id=a.act_id) as last_inspect_at
        FROM accounts a
        LEFT JOIN fb_tokens t ON t.id = a.token_id
        LEFT JOIN tw_certified_pages tp ON tp.page_id = a.page_id
        LEFT JOIN teams tm ON tm.id = a.team_id
        LEFT JOIN users ou ON ou.id = a.owner_user_id
        WHERE {' AND '.join(where)}
        ORDER BY a.created_at DESC
    """, params).fetchall()
    # 查询每个账户关联的所有 Token（来自 account_op_tokens，管理号+操作号，动态发现）
    recent_spend_map = {}
    recent_where, recent_params = [], []
    apply_team_scope(recent_where, recent_params, user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(recent_where, recent_params, user, "a")
    recent_scope_sql = (" AND " + " AND ".join(recent_where)) if recent_where else ""
    for sr in conn.execute(f"""
        SELECT p.act_id, SUM(COALESCE(p.spend, 0)) AS recent_spend
        FROM perf_snapshots p
        JOIN accounts a ON a.act_id = p.act_id
        WHERE p.snapshot_date >= date('now','+8 hours','-3 days'){recent_scope_sql}
        GROUP BY p.act_id
    """, recent_params).fetchall():
        recent_spend_map[sr["act_id"]] = float(sr["recent_spend"] or 0)
    recent_spend_source_map = {act_id: "local_perf_snapshots" for act_id in recent_spend_map.keys()}
    recent_spend_error_map = {}
    try:
        from datetime import datetime as _datetime, timedelta as _timedelta
        today_cst_date = (_datetime.utcnow() + _timedelta(hours=8)).date()
        today_cst = today_cst_date.isoformat()
        recent_since = (today_cst_date - _timedelta(days=3)).isoformat()
        recent_api_accounts = []
        for acc_row in rows:
            act_id = acc_row["act_id"]
            if float(recent_spend_map.get(act_id, 0) or 0) > 0:
                continue
            recent_api_accounts.append({
                "act_id": act_id,
                "currency": acc_row["currency"] or "USD",
                "team_id": acc_row["team_id"],
            })
        for act_id, api_spend in _fetch_spend_for_accounts(
            recent_api_accounts, recent_since, today_cst, max_workers=5
        ).items():
            if api_spend.get("ok") and float(api_spend.get("spend") or 0) > 0:
                recent_spend_map[act_id] = float(api_spend.get("spend") or 0)
                recent_spend_source_map[act_id] = "fb_insights_api"
            elif not api_spend.get("ok"):
                recent_spend_error_map[act_id] = api_spend.get("error") or "FB API 拉取失败"
    except Exception as exc:
        logger.warning("[Accounts] recent spend API supplement failed: %s", exc)
    effective_account_links = {}
    for acc_row in rows:
        try:
            acc_dict = dict(acc_row)
            landing_url_effective = resolve_account_landing_link(conn, acc_dict.get("act_id"), acc_dict)
            form_link_effective = resolve_account_form_link(
                conn,
                acc_dict.get("act_id"),
                acc_dict,
                landing_url_effective,
            )
            effective_account_links[acc_dict.get("act_id")] = {
                "landing_url_effective": landing_url_effective,
                "form_link_effective": form_link_effective,
            }
        except Exception as exc:
            logger.warning("[Accounts] effective landing link lookup failed for %s: %s", acc_row["act_id"], exc)
    conn.close()
    try:
        from services.local_token_bridge import get_local_token_candidates_for_account
    except Exception:
        get_local_token_candidates_for_account = None
    result = []
    for r in rows:
        d = dict(r)
        d.update(effective_account_links.get(d.get("act_id"), {}))
        cur = (d.get('currency') or 'USD').upper()
        bal = d.get('balance')
        amount_spent = d.get('amount_spent')
        spending_limit = d.get('spending_limit')
        cur = d.get('currency', 'USD')
        # 计算可用余额（剩余可投）
        available, bal_type, spent_usd = _calc_available_balance(
            bal, d.get('spend_cap'), amount_spent, spending_limit, cur
        )
        d['available_balance'] = available
        d['balance_type'] = bal_type
        d['amount_spent_usd'] = spent_usd
        cap_units = _from_minor_units(d.get('spend_cap'), cur)
        limit_units = _from_minor_units(spending_limit, cur)
        d['spend_cap_usd'] = _to_usd(cap_units, cur) if cap_units is not None else None
        d['spending_limit_usd'] = _to_usd(limit_units, cur) if limit_units is not None else None
        cap_usd_candidates = [d.get('spending_limit_usd'), d.get('spend_cap_usd')]
        d['balance_cap_usd'] = next(
            (float(v) for v in cap_usd_candidates if v is not None and float(v) > 0),
            None,
        )
        d['balance_over_cap'] = bool(
            d.get('balance_type') == 'spending_limit'
            and d.get('balance_cap_usd') is not None
            and spent_usd is not None
            and float(spent_usd) >= float(d['balance_cap_usd'])
        )
        # 附带 balance_usd：balance 原始值来自 FB minor units，需先转换为账户货币金额。
        if bal is not None:
            bal_units = _from_minor_units(bal, cur)
            d['balance_usd'] = _to_usd(bal_units, cur) if bal_units is not None else None
        else:
            d['balance_usd'] = None
        # 附带 timezone_name（兼容旧字段名 timezone）
        if not d.get('timezone_name'):
            d['timezone_name'] = d.get('timezone', '')
        # 附带关联的所有 Token（来自 account_op_tokens，动态发现，管理号+操作号）
        d['recent_3d_snapshot_spend'] = recent_spend_map.get(d.get('act_id'), 0.0)
        d['recent_3d_spend_source'] = recent_spend_source_map.get(d.get('act_id'), 'none')
        d['recent_3d_spend_error'] = recent_spend_error_map.get(d.get('act_id'))
        local_write_tokens = []
        if get_local_token_candidates_for_account:
            try:
                local_candidates = get_local_token_candidates_for_account(d.get('act_id'), "CREATE")
            except Exception as exc:
                logger.warning("[Accounts] local token candidates failed for %s: %s", d.get('act_id'), exc)
                local_candidates = []
            for cand in local_candidates or []:
                local_write_tokens.append({
                    "token_id": cand.get("token_id"),
                    "alias": cand.get("alias") or cand.get("label") or "本地执行器",
                    "type": "operate",
                    "source": "local_token",
                    "matrix_id": None,
                    "token_status": "active",
                    "bind_status": "active",
                    "active": True,
                    "local_token": True,
                    "node_id": cand.get("node_id"),
                })
        d['primary_token_invalid'] = bool(
            d.get("token_alias") and d.get("token_status") and d.get("token_status") != "active"
        )
        primary_token_active = bool(d.get("token_alias") and d.get("token_status") == "active")
        read_permission_status = d.get("read_permission_status") or ""
        d.update(
            get_account_token_summary(
                d.get('act_id'),
                local_write_tokens=local_write_tokens,
                primary_token_active=primary_token_active,
                read_permission_blocked=is_read_blocking_status(read_permission_status),
                read_permission_status=read_permission_status,
                read_permission_error=d.get("read_permission_error") or "",
                read_permission_checked_at=d.get("read_permission_checked_at") or "",
            )
        )
        result.append(d)
    return result


@router.get("/spent-ids")
def list_spent_account_ids(date: str, user=Depends(get_current_user)):
    """Return visible account IDs that have spend on the specified date.

    Prefer local perf_snapshots, then supplement missing accounts through FB Insights.
    """
    from datetime import date as _date
    try:
        target_date = _date.fromisoformat(str(date or "").strip()).isoformat()
    except Exception:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    conn = get_conn()
    _ensure_spend_retention_schema(conn)
    where, params = ["p.snapshot_date = ?"], [target_date]
    apply_team_scope(where, params, user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(where, params, user, "a")
    snapshot_meta = conn.execute(f"""
        SELECT COUNT(*) AS snapshot_rows,
               COUNT(DISTINCT p.act_id) AS snapshot_accounts,
               SUM(COALESCE(p.spend, 0)) AS snapshot_total_spend
        FROM perf_snapshots p
        JOIN accounts a ON a.act_id = p.act_id
        WHERE {' AND '.join(where)}
    """, params).fetchone()
    rows = conn.execute(f"""
        SELECT a.act_id, a.name, a.currency,
               SUM(COALESCE(p.spend, 0)) AS spend,
               SUM(COALESCE(p.conversions, 0)) AS conversions
        FROM perf_snapshots p
        JOIN accounts a ON a.act_id = p.act_id
        WHERE {' AND '.join(where)}
        GROUP BY a.act_id, a.name, a.currency
        HAVING SUM(COALESCE(p.spend, 0)) > 0
        ORDER BY spend DESC, a.name ASC
    """, params).fetchall()
    orphan_rows = []
    if is_superadmin(user):
        orphan_rows = conn.execute(
            """SELECT p.act_id,
                      SUM(COALESCE(p.spend, 0)) AS spend,
                      SUM(COALESCE(p.conversions, 0)) AS conversions
               FROM perf_snapshots p
               LEFT JOIN accounts a ON a.act_id = p.act_id
               WHERE p.snapshot_date=? AND a.act_id IS NULL
               GROUP BY p.act_id
               HAVING SUM(COALESCE(p.spend, 0)) > 0
               ORDER BY spend DESC, p.act_id ASC""",
            (target_date,),
        ).fetchall()
    retention_where, retention_params = ["r.snapshot_date = ?"], [target_date]
    if not is_superadmin(user):
        retention_where.append("r.team_id=?")
        retention_params.append(team_id_for_create(user))
    _apply_account_owner_scope(retention_where, retention_params, user, "r")
    retention_meta = conn.execute(f"""
        SELECT COUNT(*) AS retention_rows,
               COUNT(DISTINCT r.act_id) AS retention_accounts,
               SUM(COALESCE(r.spend, 0)) AS retention_total_spend
        FROM account_spend_retention r
        WHERE {' AND '.join(retention_where)}
    """, retention_params).fetchone()
    retention_rows = conn.execute(f"""
        SELECT r.act_id, r.account_id, r.account_name AS name, r.currency,
               SUM(COALESCE(r.spend, 0)) AS spend,
               SUM(COALESCE(r.conversions, 0)) AS conversions
        FROM account_spend_retention r
        WHERE {' AND '.join(retention_where)}
        GROUP BY r.act_id, r.account_id, r.account_name, r.currency
        HAVING SUM(COALESCE(r.spend, 0)) > 0
        ORDER BY spend DESC, r.account_name ASC
    """, retention_params).fetchall()
    account_where, account_params = ["1=1"], []
    apply_team_scope(account_where, account_params, user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(account_where, account_params, user, "a")
    visible_accounts = conn.execute(f"""
        SELECT a.act_id, a.name, a.currency, a.team_id
        FROM accounts a
        WHERE {' AND '.join(account_where)}
        ORDER BY a.name ASC
    """, account_params).fetchall()
    conn.close()

    items = []
    for r in rows:
        act_id = r["act_id"] or ""
        account_id = act_id[4:] if act_id.startswith("act_") else act_id
        items.append({
            "act_id": act_id,
            "account_id": account_id,
            "name": r["name"] or act_id,
            "currency": r["currency"] or "USD",
            "spend": float(r["spend"] or 0),
            "conversions": float(r["conversions"] or 0),
            "source": "local_perf_snapshots",
        })
    local_positive = {i["act_id"] for i in items}
    for r in retention_rows:
        act_id = r["act_id"] or ""
        if not act_id or act_id in local_positive:
            continue
        account_id = r["account_id"] or (act_id[4:] if act_id.startswith("act_") else act_id)
        items.append({
            "act_id": act_id,
            "account_id": account_id,
            "name": r["name"] or act_id,
            "currency": r["currency"] or "USD",
            "spend": float(r["spend"] or 0),
            "conversions": float(r["conversions"] or 0),
            "source": "removed_account_retention",
        })
        local_positive.add(act_id)
    for r in orphan_rows:
        act_id = r["act_id"] or ""
        if not act_id or act_id in local_positive:
            continue
        account_id = act_id[4:] if act_id.startswith("act_") else act_id
        items.append({
            "act_id": act_id,
            "account_id": account_id,
            "name": act_id,
            "currency": "USD",
            "spend": float(r["spend"] or 0),
            "conversions": float(r["conversions"] or 0),
            "source": "orphan_perf_snapshots",
        })
        local_positive.add(act_id)
    local_source_count = len(items)
    api_candidates = [
        dict(acc)
        for acc in visible_accounts
        if acc["act_id"] not in local_positive
    ]
    api_results = _fetch_spend_for_accounts(api_candidates, target_date, target_date, max_workers=6)
    api_errors = []
    for act_id, api_spend in api_results.items():
        if api_spend.get("ok") and float(api_spend.get("spend") or 0) > 0:
            account_id = act_id[4:] if act_id.startswith("act_") else act_id
            match = next((acc for acc in api_candidates if acc.get("act_id") == act_id), None) or {}
            items.append({
                "act_id": act_id,
                "account_id": account_id,
                "name": match.get("name") or act_id,
                "currency": api_spend.get("currency") or match.get("currency") or "USD",
                "spend": float(api_spend.get("spend") or 0),
                "spend_orig": float(api_spend.get("spend_orig") or 0),
                "conversions": float(api_spend.get("conversions") or 0),
                "source": "fb_insights_api",
            })
        elif not api_spend.get("ok"):
            api_errors.append({
                "act_id": act_id,
                "error": api_spend.get("error") or "FB API 拉取失败",
            })

    items.sort(key=lambda x: (-float(x.get("spend") or 0), x.get("name") or ""))
    if api_results and local_source_count:
        source = "mixed"
    elif api_results:
        source = "fb_insights_api"
    elif (retention_rows or orphan_rows) and rows:
        source = "local_perf_snapshots+retention"
    elif retention_rows:
        source = "removed_account_retention"
    elif orphan_rows:
        source = "orphan_perf_snapshots"
    else:
        source = "local_perf_snapshots"
    return {
        "date": target_date,
        "count": len(items),
        "source": source,
        "snapshot_rows": int(snapshot_meta["snapshot_rows"] or 0) if snapshot_meta else 0,
        "snapshot_accounts": int(snapshot_meta["snapshot_accounts"] or 0) if snapshot_meta else 0,
        "snapshot_total_spend": float(snapshot_meta["snapshot_total_spend"] or 0) if snapshot_meta else 0,
        "retention_rows": int(retention_meta["retention_rows"] or 0) if retention_meta else 0,
        "retention_accounts": int(retention_meta["retention_accounts"] or 0) if retention_meta else 0,
        "retention_total_spend": float(retention_meta["retention_total_spend"] or 0) if retention_meta else 0,
        "orphan_snapshot_accounts": len(orphan_rows),
        "visible_accounts": len(visible_accounts),
        "api_checked": len(api_results),
        "api_error_count": len(api_errors),
        "api_errors": api_errors[:20],
        "account_ids": [i["account_id"] for i in items],
        "act_ids": [i["act_id"] for i in items],
        "items": items,
    }


@router.post("/spent-retention/cleanup")
def cleanup_spend_retention(days: int = 180, user=Depends(get_current_user)):
    """Prune archived spend records for accounts that were removed from the account list."""
    if not is_superadmin(user):
        raise HTTPException(status_code=403, detail="Superadmin required")
    days = max(30, min(int(days or 180), 1095))
    conn = get_conn()
    _ensure_spend_retention_schema(conn)
    row = conn.execute("SELECT COUNT(*) AS c FROM account_spend_retention").fetchone()
    before = int(row["c"] or 0)
    cur = conn.execute(
        "DELETE FROM account_spend_retention WHERE date(snapshot_date) < date('now','+8 hours', ?)",
        (f"-{days} days",),
    )
    conn.execute(
        """INSERT OR REPLACE INTO settings(key,value,label,description,category,sort_order)
           VALUES ('spend_retention_days', ?, '消耗历史留存天数',
                   '账户移除后保留指定天数的本地消耗归档，用于复制有消耗账户 ID 和追溯账单',
                   'storage',30)""",
        (str(days),),
    )
    conn.commit()
    row = conn.execute("SELECT COUNT(*) AS c FROM account_spend_retention").fetchone()
    after = int(row["c"] or 0)
    conn.close()
    return {
        "success": True,
        "days": days,
        "deleted": int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else max(0, before - after)),
        "remaining": after,
    }


@router.put("/{account_id}")
def update_account(account_id: int, body: AccountUpdate, user=Depends(get_current_user)):
    """更新账户信息"""
    conn = get_conn()
    _ensure_account_read_columns(conn)
    assert_row_access(conn, "accounts", account_id, user, allow_unassigned=False)
    updates = []
    params = []
    if body.name is not None:
        updates.append("name=?")
        params.append(body.name)
    if body.enabled is not None:
        updates.append("enabled=?")
        params.append(body.enabled)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if body.page_id is not None:
        updates.append("page_id=?")
        params.append(body.page_id)
    if body.pixel_id is not None:
        updates.append("pixel_id=?")
        params.append(body.pixel_id)
    if body.beneficiary is not None:
        updates.append("beneficiary=?")
        params.append(body.beneficiary)
    if body.payer is not None:
        updates.append("payer=?")
        params.append(body.payer)
    if body.tw_advertiser_id is not None:
        # 0 表示清除关联，其他值表示设置关联
        updates.append("tw_advertiser_id=?")
        params.append(None if body.tw_advertiser_id == 0 else body.tw_advertiser_id)
    # 智能铺放目标配置字段
    if body.target_countries is not None:
        updates.append("target_countries=?")
        params.append(body.target_countries)
    if body.target_age_min is not None:
        updates.append("target_age_min=?")
        params.append(body.target_age_min)
    if body.target_age_max is not None:
        updates.append("target_age_max=?")
        params.append(body.target_age_max)
    if body.target_gender is not None:
        updates.append("target_gender=?")
        params.append(body.target_gender)
    if body.target_placements is not None:
        updates.append("target_placements=?")
        params.append(body.target_placements)
    if body.target_objective is not None:
        updates.append("target_objective=?")
        params.append(body.target_objective)
    if body.warmup_days is not None:
        updates.append("warmup_days=?")
        params.append(body.warmup_days)
    if body.warmup_budget is not None:
        updates.append("warmup_budget=?")
        params.append(body.warmup_budget)
    # 账户级默认落地页和目标类型（之前遗漏处理）
    if body.landing_url is not None:
        updates.append("landing_url=?")
        params.append(body.landing_url)
    if body.target_objective_type is not None:
        updates.append("target_objective_type=?")
        params.append(body.target_objective_type)
    if getattr(body, 'mirror_enabled', None) is not None:
        updates.append("mirror_enabled=?")
        params.append(1 if body.mirror_enabled else 0)
    if getattr(body, 'sentinel_enabled', None) is not None:
        updates.append("sentinel_enabled=?")
        params.append(1 if body.sentinel_enabled else 0)
    if getattr(body, 'owner_user_id', None) is not None:
        updates.append("owner_user_id=?")
        params.append(_validate_account_owner(conn, body.owner_user_id, account_id, user))
    if not updates:
        conn.close()
        raise HTTPException(400, "没有需要更新的字段")
    updates.append("updated_at=datetime('now')")
    params.append(account_id)
    conn.execute(f"UPDATE accounts SET {', '.join(updates)} WHERE id=?", params)
    claim_row_for_team(conn, "accounts", "id", account_id, user)
    conn.commit()
    conn.close()
    return {"success": True}



@router.patch("/by-act-id/{act_id_str}")
def patch_account_by_act_id(act_id_str: str, body: AccountUpdate, user=Depends(get_current_user)):
    """通过 act_id 字符串更新账户配置（用于前端批量链接管理等场景）"""
    conn = get_conn()
    _ensure_account_read_columns(conn)
    row = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id_str,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"账户 {act_id_str} 不存在")
    account_id = row[0]
    assert_row_access(conn, "accounts", account_id, user, allow_unassigned=False)
    updates = []
    params = []
    if body.name is not None:
        updates.append("name=?"); params.append(body.name)
    if body.enabled is not None:
        updates.append("enabled=?"); params.append(body.enabled)
    if body.note is not None:
        updates.append("note=?"); params.append(body.note)
    if body.page_id is not None:
        updates.append("page_id=?"); params.append(body.page_id)
    if body.pixel_id is not None:
        updates.append("pixel_id=?"); params.append(body.pixel_id)
    if body.landing_url is not None:
        updates.append("landing_url=?"); params.append(body.landing_url)
    if getattr(body, "form_link", None) is not None:
        updates.append("form_link=?"); params.append(body.form_link)
    if body.target_countries is not None:
        updates.append("target_countries=?"); params.append(body.target_countries)
    if body.target_age_min is not None:
        updates.append("target_age_min=?"); params.append(body.target_age_min)
    if body.target_age_max is not None:
        updates.append("target_age_max=?"); params.append(body.target_age_max)
    if body.target_gender is not None:
        updates.append("target_gender=?"); params.append(body.target_gender)
    if body.target_placements is not None:
        updates.append("target_placements=?"); params.append(body.target_placements)
    if body.target_objective_type is not None:
        updates.append("target_objective_type=?"); params.append(body.target_objective_type)
    if getattr(body, 'mirror_enabled', None) is not None:
        updates.append("mirror_enabled=?"); params.append(1 if body.mirror_enabled else 0)
    if getattr(body, 'sentinel_enabled', None) is not None:
        updates.append("sentinel_enabled=?"); params.append(1 if body.sentinel_enabled else 0)
    if getattr(body, 'owner_user_id', None) is not None:
        updates.append("owner_user_id=?"); params.append(_validate_account_owner(conn, body.owner_user_id, account_id, user))
    if not updates:
        conn.close()
        raise HTTPException(400, "没有需要更新的字段")
    updates.append("updated_at=datetime('now')")
    params.append(account_id)
    conn.execute(f"UPDATE accounts SET {', '.join(updates)} WHERE id=?", params)
    claim_row_for_team(conn, "accounts", "id", account_id, user)
    conn.commit()
    conn.close()
    return {"success": True, "act_id": act_id_str}

@router.delete("/{account_id}")
def delete_account(account_id: int, user=Depends(get_current_user)):
    """删除账户（不删除Token）"""
    conn = get_conn()
    assert_row_access(conn, "accounts", account_id, user, allow_unassigned=False)
    _archive_account_spend_history(conn, account_id)
    conn.execute("DELETE FROM accounts WHERE id=?", (account_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/{account_id}/sync-status")
def sync_account_status(account_id: int, user=Depends(get_current_user)):
    """从 FB API 同步单个账户的真实状态（account_status、balance、spend_cap）"""
    conn = get_conn()
    assert_row_access(conn, "accounts", account_id, user, allow_unassigned=False)
    row = conn.execute("""
        SELECT a.act_id, a.name
        FROM accounts a
        WHERE a.id=?
    """, (account_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "账户不存在")
    resolved = _resolve_account_info(row["act_id"])
    if not resolved.get("ok"):
        raise HTTPException(400, f"FB API 返回错误: {resolved.get('error') or 'no_read_token'}")
    info = resolved["info"]
    new_name = info.get("name")
    if not new_name or new_name == row["act_id"]:
        new_name = row["name"]

    conn = get_conn()
    try:
        _ensure_account_read_columns(conn)
        conn.execute("""
            UPDATE accounts
            SET account_status=?, balance=?, spend_cap=?, amount_spent=?, spending_limit=?,
                name=?, currency=?, timezone=?, timezone_name=?, timezone_offset_hours_utc=?,
                updated_at=datetime('now')
            WHERE id=?
        """, (
            info.get("account_status", 1),
            info.get("balance"),
            info.get("spend_cap"),
            info.get("amount_spent"),
            info.get("spending_limit"),
            new_name,
            info.get("currency", "USD"),
            info.get("timezone", "UTC"),
            info.get("timezone_name") or info.get("timezone", "UTC"),
            info.get("timezone_offset_hours_utc"),
            account_id
        ))
        conn.commit()
    finally:
        conn.close()

    return {
        "success": True,
        "account_status": info.get("account_status", 1),
        "balance": info.get("balance"),
        "name": new_name,
        "currency": info.get("currency"),
        "timezone": info.get("timezone"),
        "timezone_offset_hours_utc": info.get("timezone_offset_hours_utc"),
    }


@router.post("/sync-all-status")
def sync_all_accounts_status(user=Depends(get_current_user)):
    """批量从 FB API 同步所有账户的真实状态（并发执行）"""
    conn = get_conn()
    _ensure_account_read_columns(conn)
    where, params = [], []
    apply_team_scope(where, params, user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(where, params, user, "a")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(f"""
        SELECT a.id, a.act_id, a.name
        FROM accounts a
        {clause}
    """, params).fetchall()
    # 对 name=act_id 的账户，尝试从 account_op_tokens 中找到可用的操作号 token 补充
    conn.close()

    if not rows:
        return {"success": True, "updated": 0, "failed": 0, "message": "没有可同步的账户"}

    # 并发拉取所有账户信息
    results = {}
    with ThreadPoolExecutor(max_workers=min(10, len(rows))) as executor:
        futures = {}
        for row in rows:
            fut = executor.submit(_resolve_account_info, row["act_id"])
            futures[fut] = row
        for fut in as_completed(futures):
            row = futures[fut]
            resolved = fut.result()
            results[row["id"]] = resolved["info"] if resolved.get("ok") else {
                "act_id": row["act_id"],
                "error": resolved.get("error") or "no_read_token",
            }

    # 批量更新数据库
    updated = 0
    failed = 0
    conn = get_conn()
    try:
        _ensure_account_read_columns(conn)
        for row in rows:
            info = results.get(row["id"], {})
            if "error" in info:
                failed += 1
                continue
            # 如果返回的 name 仍是 act_id 或为空，保留原有名称不覆盖
            _new_name = info.get("name")
            if not _new_name or _new_name == row["act_id"]:
                _new_name = row["name"]  # 保留原有名称
            conn.execute("""
                UPDATE accounts
                SET account_status=?, balance=?, spend_cap=?, amount_spent=?, spending_limit=?,
                    name=?, currency=?, timezone=?, timezone_name=?, timezone_offset_hours_utc=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, (
                info.get("account_status", 1),
                info.get("balance"),
                info.get("spend_cap"),
                info.get("amount_spent"),
                info.get("spending_limit"),
                _new_name,
                info.get("currency", "USD"),
                info.get("timezone", "UTC"),
                info.get("timezone_name") or info.get("timezone", "UTC"),
                info.get("timezone_offset_hours_utc"),
                row["id"]
            ))
            updated += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"批量更新失败: {str(e)}")
    finally:
        conn.close()

    return {"success": True, "updated": updated, "failed": failed}


# ── 工具函数 ──────────────────────────────────────────────────────────────

def _verify_fb_token(token: str):
    """验证FB Token有效性，返回 (ok, info_or_error)"""
    try:
        resp = requests.get(
            f"{FB_API_BASE}/me",
            params={"access_token": token, "fields": "id,name"},
            timeout=10
        )
        data = resp.json()
        if "error" in data:
            err = data["error"]
            return False, f"{err.get('message', '未知错误')} (code={err.get('code')})"
        return True, {"id": data.get("id"), "name": data.get("name")}
    except Exception as e:
        return False, str(e)

def _compact_diag_error(error: object) -> str:
    text = str(error or "").strip().replace("\n", " ")
    if not text:
        return ""
    return text[:500]


@router.get("/{act_id}/permission-diagnostic")
def diagnose_account_permission(act_id: str, user=Depends(get_current_user)):
    """Probe each relevant token for an account without creating or updating ads."""
    from services.token_manager import TOKEN_SOURCE_OAUTH_USER, TOKEN_SOURCE_SYSTEM_USER

    conn = get_conn()
    ensure_token_source_columns(conn)
    _ensure_account_read_columns(conn)
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    acc = conn.execute(
        """
        SELECT a.id, a.act_id, a.name, a.token_id, a.page_id, a.team_id,
               a.read_permission_status, a.read_permission_error, a.read_permission_checked_at
        FROM accounts a
        WHERE a.act_id=?
        """,
        (act_id,),
    ).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, "Account not found")

    account_team_id = acc["team_id"]
    if account_team_id is None and isinstance(user, dict) and user.get("role") != "superadmin":
        account_team_id = user.get("team_id")

    def _team_sql(alias: str) -> tuple[str, list]:
        if account_team_id is None:
            return f" AND {alias}.team_id IS NULL", []
        return f" AND {alias}.team_id=?", [account_team_id]

    def _token_public(row: dict, origin: str, bind_status: str = "", priority=None) -> dict:
        token_type = str(row.get("token_type") or "").strip().lower()
        token_source = normalize_token_source(
            row.get("token_source"),
            default_token_source_for_type(token_type),
        )
        token_status = row.get("token_status") or row.get("status") or ""
        return {
            "token_id": row.get("token_id") or row.get("id"),
            "alias": row.get("token_alias") or row.get("alias") or "",
            "type": token_type or "unknown",
            "source": token_source,
            "matrix_id": row.get("matrix_id"),
            "token_status": token_status,
            "bind_status": bind_status,
            "priority": priority,
            "origins": [origin],
            "_access_token_enc": row.get("access_token_enc"),
        }

    candidates = []
    by_token_id = {}

    def _add_candidate(item: dict):
        token_id = item.get("token_id")
        if token_id in by_token_id:
            existing = by_token_id[token_id]
            for origin in item.get("origins") or []:
                if origin not in existing["origins"]:
                    existing["origins"].append(origin)
            if not existing.get("bind_status") and item.get("bind_status"):
                existing["bind_status"] = item["bind_status"]
            if existing.get("priority") is None and item.get("priority") is not None:
                existing["priority"] = item["priority"]
            return
        by_token_id[token_id] = item
        candidates.append(item)

    token_team_sql, token_team_params = _team_sql("t")
    bound_rows = conn.execute(
        f"""
        SELECT t.id as token_id, t.token_alias, t.token_type, t.token_source,
               t.status as token_status, t.matrix_id, t.access_token_enc,
               aot.status as bind_status, aot.priority
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.act_id=?
          {token_team_sql}
        ORDER BY
          CASE WHEN aot.status='active' AND t.status='active' THEN 0 ELSE 1 END,
          CASE t.token_type WHEN 'manage' THEN 0 WHEN 'operate' THEN 1 ELSE 2 END,
          aot.priority DESC,
          t.id ASC
        """,
        [act_id] + token_team_params,
    ).fetchall()
    for row in bound_rows:
        _add_candidate(_token_public(dict(row), "bound", row["bind_status"], row["priority"]))

    if acc["token_id"]:
        primary_team_sql, primary_team_params = _team_sql("t")
        primary = conn.execute(
            f"""
            SELECT t.id as token_id, t.token_alias, t.token_type, t.token_source,
                   t.status as token_status, t.matrix_id, t.access_token_enc
            FROM fb_tokens t
            WHERE t.id=?
              {primary_team_sql}
            """,
            [acc["token_id"]] + primary_team_params,
        ).fetchone()
        if primary:
            _add_candidate(_token_public(dict(primary), "primary"))

    manage_team_sql, manage_team_params = _team_sql("t")
    manage_rows = conn.execute(
        f"""
        SELECT t.id as token_id, t.token_alias, t.token_type, t.token_source,
               t.status as token_status, t.matrix_id, t.access_token_enc
        FROM fb_tokens t
        WHERE t.status='active'
          AND t.token_type='manage'
          {manage_team_sql}
        ORDER BY t.id ASC
        LIMIT 10
        """,
        manage_team_params,
    ).fetchall()
    for row in manage_rows:
        _add_candidate(_token_public(dict(row), "team_manage"))

    matrix_ids = sorted(
        {
            int(c["matrix_id"])
            for c in candidates
            if c.get("matrix_id") not in (None, "", 0)
        }
    )
    if matrix_ids:
        matrix_team_sql, matrix_team_params = _team_sql("t")
        matrix_placeholders = ",".join("?" for _ in matrix_ids)
        matrix_rows = conn.execute(
            f"""
            SELECT t.id as token_id, t.token_alias, t.token_type, t.token_source,
                   t.status as token_status, t.matrix_id, t.access_token_enc
            FROM fb_tokens t
            WHERE t.status='active'
              AND t.token_type='operate'
              AND t.token_source IN (?, ?)
              AND t.matrix_id IN ({matrix_placeholders})
              {matrix_team_sql}
            ORDER BY t.matrix_id ASC, t.id ASC
            LIMIT 20
            """,
            [TOKEN_SOURCE_SYSTEM_USER, TOKEN_SOURCE_OAUTH_USER] + matrix_ids + matrix_team_params,
        ).fetchall()
        for row in matrix_rows:
            if row["token_id"] not in by_token_id:
                _add_candidate(_token_public(dict(row), "matrix_peer"))

    def _is_active_candidate(item: dict) -> bool:
        token_ok = item.get("token_status") == "active"
        bind_status = item.get("bind_status")
        bind_ok = bind_status in ("", None, "active")
        return bool(token_ok and bind_ok)

    def _participation(item: dict) -> dict:
        active = _is_active_candidate(item)
        origins = set(item.get("origins") or [])
        is_bound = "bound" in origins
        is_primary = "primary" in origins
        is_manage = item.get("type") == "manage"
        is_team_manage = "team_manage" in origins and is_manage
        is_system_operate = (
            item.get("type") == "operate"
            and item.get("source") in (TOKEN_SOURCE_SYSTEM_USER, TOKEN_SOURCE_OAUTH_USER)
        )
        read = active and (is_bound or is_primary or is_manage or is_team_manage)
        pause = active and ((is_bound and is_system_operate) or is_manage or is_team_manage or is_primary)
        create_update = active and is_bound and is_system_operate
        return {
            "read": bool(read),
            "pause": bool(pause),
            "create": bool(create_update),
            "update": bool(create_update),
        }

    def _ok_probe(data: dict | None = None) -> dict:
        return {"ok": True, "data": data or {}}

    def _error_probe(error: object) -> dict:
        text = _compact_diag_error(error)
        return {"ok": False, "error": text, "failure_type": classify_read_failure(text)}

    def _probe_account(token_plain: str) -> dict:
        try:
            data = _graph_get_json(
                f"/{act_id}",
                token_plain,
                params={"fields": "id,name,account_status,currency,timezone_name"},
                timeout=12,
            )
            return _ok_probe({
                "id": data.get("id"),
                "name": data.get("name"),
                "account_status": data.get("account_status"),
                "currency": data.get("currency"),
                "timezone_name": data.get("timezone_name"),
            })
        except Exception as exc:
            return _error_probe(exc)

    def _probe_campaigns(token_plain: str) -> dict:
        try:
            data = _graph_get_json(
                f"/{act_id}/campaigns",
                token_plain,
                params={"fields": "id,name,status,effective_status", "limit": 1},
                timeout=12,
            )
            return _ok_probe({"count_sampled": len(data.get("data") or [])})
        except Exception as exc:
            return _error_probe(exc)

    def _find_page(token_plain: str, page_id: str):
        if not page_id:
            return None, None
        url = f"{FB_API_BASE}/me/accounts"
        params = {
            "access_token": token_plain,
            "fields": "id,name,tasks,is_published,access_token",
            "limit": 200,
        }
        seen = set()
        for _ in range(5):
            try:
                resp = requests.get(url, params=params, timeout=12)
                data = resp.json()
            except Exception as exc:
                return None, exc
            if "error" in data:
                return None, data["error"].get("message") or data["error"]
            for page in data.get("data") or []:
                if str(page.get("id")) == str(page_id):
                    return page, None
            next_url = data.get("paging", {}).get("next")
            if not next_url or next_url in seen:
                break
            seen.add(next_url)
            url = next_url
            params = {}
        return None, "Token 的 /me/accounts 中未找到该主页"

    def _probe_lead_forms(page_id: str, page_token: str) -> dict:
        try:
            data = _graph_get_json(
                f"/{page_id}/leadgen_forms",
                page_token,
                params={"limit": 1},
                timeout=12,
            )
            return _ok_probe({"count_sampled": len(data.get("data") or [])})
        except Exception as exc:
            return _error_probe(exc)

    results = []
    for item in candidates:
        enc = item.pop("_access_token_enc", None)
        public = dict(item)
        public["participates"] = _participation(public)
        public["active"] = _is_active_candidate(public)
        public["probes"] = {}
        public["notes"] = []
        if "matrix_peer" in public["origins"]:
            public["notes"].append("同矩阵可见但未绑定，当前不会参与自动创建/改预算")
        if not public["active"]:
            public["notes"].append("Token 或绑定状态不是 active")
        token_plain = decrypt_token(enc) if enc else ""
        if not token_plain:
            public["probes"]["account_read"] = _error_probe("Token 解密失败或为空")
            results.append(public)
            continue

        account_probe = _probe_account(token_plain)
        public["probes"]["account_read"] = account_probe
        if account_probe.get("ok"):
            campaigns_probe = _probe_campaigns(token_plain)
            public["probes"]["campaigns_read"] = campaigns_probe
        else:
            public["probes"]["campaigns_read"] = {"ok": False, "skipped": True, "error": "账户读取失败，跳过广告列表探测"}

        if acc["page_id"] and account_probe.get("ok"):
            page, page_err = _find_page(token_plain, acc["page_id"])
            if page:
                tasks = page.get("tasks") or []
                page_token = page.get("access_token") or ""
                can_advertise = (not tasks) or ("ADVERTISE" in tasks)
                public["probes"]["page"] = _ok_probe({
                    "id": page.get("id"),
                    "name": page.get("name"),
                    "is_published": page.get("is_published"),
                    "tasks": tasks,
                    "can_advertise": bool(can_advertise),
                    "has_page_token": bool(page_token),
                })
                if page_token:
                    public["probes"]["lead_forms"] = _probe_lead_forms(acc["page_id"], page_token)
                else:
                    public["probes"]["lead_forms"] = _error_probe("可见主页，但拿不到 Page Access Token")
            else:
                public["probes"]["page"] = _error_probe(page_err)
                public["probes"]["lead_forms"] = {"ok": False, "skipped": True, "error": "主页不可见，跳过 Lead Form 探测"}
        else:
            public["probes"]["page"] = {"ok": False, "skipped": True, "error": "账户未绑定主页或账户读取失败"}
            public["probes"]["lead_forms"] = {"ok": False, "skipped": True, "error": "账户未绑定主页或账户读取失败"}

        campaign_ok = bool(public["probes"].get("campaigns_read", {}).get("ok"))
        page_ok = bool(public["probes"].get("page", {}).get("ok"))
        lead_ok = bool(public["probes"].get("lead_forms", {}).get("ok"))
        page_can_advertise = bool(
            public["probes"].get("page", {}).get("data", {}).get("can_advertise")
        )
        public["can_read"] = bool(public["participates"]["read"] and account_probe.get("ok"))
        public["can_pause_likely"] = bool(public["participates"]["pause"] and campaign_ok)
        public["can_create_likely"] = bool(public["participates"]["create"] and account_probe.get("ok"))
        public["can_update_likely"] = bool(public["participates"]["update"] and account_probe.get("ok"))
        public["lead_form_likely"] = bool(lead_ok and page_can_advertise)
        if acc["page_id"] and page_ok and not page_can_advertise:
            public["notes"].append("主页可见，但缺少 ADVERTISE 任务或主页不可投放")
        if acc["page_id"] and public["can_create_likely"] and not public["lead_form_likely"]:
            public["notes"].append("广告账户可写，但当前主页/Lead Form 条件不足")
        results.append(public)

    read_ok = any(r.get("can_read") for r in results)
    campaign_read_ok = any(
        r.get("participates", {}).get("read") and r.get("probes", {}).get("campaigns_read", {}).get("ok")
        for r in results
    )
    pause_ok = any(r.get("can_pause_likely") for r in results)
    create_ok = any(r.get("can_create_likely") for r in results)
    update_ok = any(r.get("can_update_likely") for r in results)
    lead_ok = any(r.get("can_create_likely") and r.get("lead_form_likely") for r in results)
    best_read = next((r for r in results if r.get("can_read")), None)
    best_pause = next((r for r in results if r.get("can_pause_likely")), None)
    best_write = next((r for r in results if r.get("can_create_likely")), None)

    def _public_best(item):
        if not item:
            return None
        return {
            "token_id": item.get("token_id"),
            "alias": item.get("alias"),
            "type": item.get("type"),
            "source": item.get("source"),
            "origins": item.get("origins"),
        }

    all_read_errors = [
        r.get("probes", {}).get("account_read", {}).get("error")
        for r in results
        if not r.get("probes", {}).get("account_read", {}).get("ok")
    ]
    all_read_errors = [e for e in all_read_errors if e]

    restored_read_status = False
    if read_ok:
        mark_account_read_success(conn, act_id)
        restored_read_status = True
    else:
        err_text = "；".join(all_read_errors[:3]) if all_read_errors else "no_read_token"
        if not results:
            mark_account_read_failure(conn, act_id, "no_read_token", status="no_read_token")
        else:
            status = classify_read_failure(err_text)
            mark_account_read_failure(conn, act_id, err_text, status=status)
    conn.commit()
    conn.close()

    if not results:
        recommended = "没有找到可用于该账户的候选 Token；请先重新匹配或绑定同团队/同矩阵 Token。"
    elif not read_ok:
        recommended = "当前系统实际可用候选 Token 都读不到该广告账户；优先检查 BM 授权、账户归属和 Token 权限。"
    elif not create_ok:
        recommended = "读取和巡检链路可用，但没有绑定可写的 System User 或 Meta 官方授权操作号；自动铺广告、预热、改预算会失败。"
    elif acc["page_id"] and not lead_ok:
        recommended = "广告账户写入候选可用，但主页或 Lead Form 条件不足；创建线索广告前请检查主页权限和表单权限。"
    else:
        recommended = "权限链路正常：账户可读，且存在可写操作号。"

    return {
        "success": True,
        "act_id": act_id,
        "account_name": acc["name"],
        "page_id": acc["page_id"],
        "previous_read_status": acc["read_permission_status"],
        "previous_read_error": acc["read_permission_error"],
        "restored_read_status": restored_read_status,
        "summary": {
            "read_ok": read_ok,
            "campaign_read_ok": campaign_read_ok,
            "pause_likely": pause_ok,
            "create_likely": create_ok,
            "update_likely": update_ok,
            "lead_form_likely": lead_ok if acc["page_id"] else None,
            "candidate_count": len(results),
            "best_read_token": _public_best(best_read),
            "best_pause_token": _public_best(best_pause),
            "best_write_token": _public_best(best_write),
            "recommended_action": recommended,
        },
        "tokens": results,
    }


@router.post("/{act_id}/permission-diagnostic/repair")
def repair_account_permission_links(act_id: str, user=Depends(get_current_user)):
    """Re-scan same-team tokens and bind every token that can see this account."""
    _require_operator_user(user)
    conn = get_conn()
    try:
        acc = assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
        team_id = _matching_team_id(user, acc["team_id"])
    finally:
        conn.close()
    try:
        result = _auto_link_tokens_for_accounts(
            [act_id],
            user,
            team_id=team_id,
            note="permission_diagnostic_repair",
        )
    except Exception as exc:
        raise HTTPException(500, f"token rematch failed: {exc}")
    return {
        "success": True,
        "act_id": _normalize_act_id(act_id),
        "result": result,
        "message": (
            f"checked {result.get('token_checked', 0)} tokens, "
            f"matched {result.get('matched', 0)}, "
            f"restored {result.get('restored', 0)}"
        ),
    }


@router.get("/{act_id}/fb-pages")
def get_fb_pages(act_id: str, user=Depends(get_current_user)):
    """
    拉取自动铺广告可用主页列表。

    注意：自动铺广告只会使用 ACTION_CREATE 的可写操作号（System User 或 Meta 官方授权），不会使用管理号或浏览器里的个人号。
    这里必须和创建广告的 Token 池保持一致，否则会出现“页面能选，AdSet 创建时报主页权限不足”。
    返回格式：[{id, name, category, can_use}]
    """
    import requests as _req
    from services.token_manager import ACTION_CREATE, get_exec_token_candidates
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    conn.close()

    def _probe_lead_form_capability(token_: str, page_id_: str):
        try:
            resp = _req.get(
                f"{FB_API_BASE}/{page_id_}/leadgen_forms",
                params={"access_token": token_, "limit": 1},
                timeout=12
            )
            data = resp.json()
        except Exception as exc:
            return None, f"暂时无法确认 Lead 表单权限：{exc}"

        if "error" not in data:
            return True, "可自动创建 Lead 表单"

        err = data.get("error") or {}
        err_msg = str(err.get("message") or "leadgen_forms probe failed")
        err_code = err.get("code", 0)
        lower_msg = err_msg.lower()
        permission_blocked = err_code in {10, 200} or "permission" in lower_msg or "pages_manage_ads" in lower_msg
        if permission_blocked:
            return False, "缺少 pages_manage_ads，无法自动创建 Lead 表单"
        return None, f"暂时无法确认 Lead 表单权限：{err_msg}"

    def _fetch_pages_with_token(token_: str, token_label_: str) -> list:
        """用给定 Token 通过 /me/accounts 拉取主页列表"""
        pages_map = {}
        try:
            url = f"{FB_API_BASE}/me/accounts"
            params = {"access_token": token_, "fields": "id,name,category,is_published,tasks", "limit": 200}
            seen_next = set()
            for _ in range(20):
                r = _req.get(url, params=params, timeout=15)
                d = r.json()
                if "error" in d:
                    break
                for p in d.get("data", []):
                    pid = p.get("id")
                    if not pid:
                        continue
                    tasks = p.get("tasks") or []
                    is_published = p.get("is_published", True)
                    can_adv = is_published is not False and "ADVERTISE" in tasks
                    if can_adv:
                        lead_form_can_create, lead_form_hint = _probe_lead_form_capability(token_, pid)
                    elif is_published is False:
                        lead_form_can_create, lead_form_hint = False, "主页未发布，不能自动铺广告"
                    else:
                        lead_form_can_create, lead_form_hint = False, "当前自动铺广告操作号缺少该主页 ADVERTISE 权限"
                    pages_map[pid] = {
                        "id": pid,
                        "name": p.get("name", ""),
                        "category": p.get("category", ""),
                        "is_published": is_published,
                        "tasks": tasks,
                        "can_use": can_adv,
                        "lead_form_can_create": lead_form_can_create,
                        "lead_form_status": (
                            "ok" if lead_form_can_create is True else
                            "fail" if lead_form_can_create is False else
                            "warn"
                        ),
                        "lead_form_hint": lead_form_hint,
                        "source": "create_token",
                        "token_label": token_label_,
                    }
                next_url = d.get("paging", {}).get("next")
                if not next_url or next_url in seen_next:
                    break
                seen_next.add(next_url)
                url = next_url
                params = {}
        except Exception:
            pass
        return list(pages_map.values())

    def _page_capability_rank(page_: dict):
        lead_state = page_.get("lead_form_can_create")
        lead_rank = 2 if lead_state is True else 1 if lead_state is False else 0
        return (
            1 if page_.get("can_use") else 0,
            lead_rank,
        )

    create_candidates = get_exec_token_candidates(
        act_id,
        ACTION_CREATE,
        notify_exhausted=False,
        reserve=False,
    )
    if not create_candidates:
        raise HTTPException(400, "该账户没有可用于自动铺广告的可写操作号，无法拉取可投主页")

    # 只用自动铺广告实际可用的 CREATE token 拉取，合并去重。
    merged = {}
    for cand in create_candidates:
        tok = cand.get("token_plain") or cand.get("token")
        if not tok:
            continue
        label = cand.get("label") or cand.get("alias") or "操作号"
        for p in _fetch_pages_with_token(tok, label):
            pid = p["id"]
            if pid not in merged:
                merged[pid] = p
            elif _page_capability_rank(p) > _page_capability_rank(merged[pid]):
                # 优先保留“既能投放又能自动创建 Lead 表单”的 CREATE token 记录
                merged[pid] = p

    pages = list(merged.values())
    pages.sort(key=lambda x: (
        0 if x["can_use"] else 1,
        0 if x.get("lead_form_can_create") is True else 1 if x.get("lead_form_can_create") is False else 2,
        x.get("name", "")
    ))
    return {"success": True, "pages": pages, "total": len(pages)}


@router.get("/{act_id}/lead-form-diagnostic")
def diagnose_lead_form(
    act_id: str,
    page_id: str = "",
    countries: str = "",
    user=Depends(get_current_user),
):
    """无副作用诊断 Lead Form 权限和区域声明，不创建真实表单。"""
    import requests as _req
    from services.token_manager import ACTION_CREATE, ACTION_READ, get_exec_token_candidates
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    conn.close()

    page_id = str(page_id or "").strip()
    if not page_id:
        raise HTTPException(400, "请先选择主页")

    def _token_pool():
        seen = set()
        pool = []
        for action, purpose in ((ACTION_CREATE, "CREATE"), (ACTION_READ, "READ")):
            try:
                candidates = get_exec_token_candidates(act_id, action, notify_exhausted=False, reserve=False)
            except Exception:
                candidates = []
            for c in candidates or []:
                token_plain = c.get("token_plain") or c.get("token")
                if not token_plain or token_plain in seen:
                    continue
                seen.add(token_plain)
                pool.append({
                    "token": token_plain,
                    "alias": c.get("alias") or c.get("label") or purpose,
                    "source": c.get("source") or purpose.lower(),
                    "purpose": purpose,
                })
        return pool

    def _find_page(token: str):
        resp = _req.get(
            f"{FB_API_BASE}/me/accounts",
            params={"access_token": token, "fields": "id,name,access_token,tasks,is_published", "limit": 200},
            timeout=15,
        )
        data = resp.json()
        if "error" in data:
            return None, data["error"]
        for page in data.get("data", []) or []:
            if str(page.get("id")) == page_id:
                return page, None
        return None, {"message": "该 Token 的 /me/accounts 中未找到此主页"}

    def _probe_forms(page_token: str):
        resp = _req.get(
            f"{FB_API_BASE}/{page_id}/leadgen_forms",
            params={"access_token": page_token, "limit": 1},
            timeout=15,
        )
        data = resp.json()
        if "error" in data:
            return False, data["error"]
        return True, None

    token_results = []
    best = None
    for item in _token_pool():
        page, page_err = _find_page(item["token"])
        row = {
            "alias": item["alias"],
            "source": item["source"],
            "purpose": item["purpose"],
            "page_visible": bool(page),
            "page_name": page.get("name") if page else "",
            "tasks": page.get("tasks", []) if page else [],
            "is_published": page.get("is_published") if page else None,
            "has_page_token": bool(page and page.get("access_token")),
            "lead_forms_readable": False,
            "can_create_likely": False,
            "error": "",
        }
        if not page:
            row["error"] = (page_err or {}).get("message", "主页不可见")
            token_results.append(row)
            continue
        tasks = set(page.get("tasks") or [])
        row["can_advertise"] = (not tasks) or ("ADVERTISE" in tasks)
        if not page.get("access_token"):
            row["error"] = "可见主页但拿不到 Page Access Token"
            token_results.append(row)
            continue
        ok, form_err = _probe_forms(page["access_token"])
        row["lead_forms_readable"] = ok
        row["can_create_likely"] = bool(ok and row["can_advertise"])
        if form_err:
            err_msg = str(form_err.get("message") or form_err)
            row["error"] = err_msg
            row["error_code"] = form_err.get("code")
            row["error_subcode"] = form_err.get("error_subcode")
        if row["can_create_likely"] and best is None:
            best = row
        token_results.append(row)

    country_list = [c.strip().upper() for c in str(countries or "").split(",") if c.strip()]
    regulated = [c for c in country_list if c in {"TW", "HK", "SG"}]
    regional = {
        "countries": regulated,
        "required": bool(regulated),
        "verified_identity_id": "",
        "page_in_cert_library": False,
        "message": "无需区域声明",
    }
    if regulated:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT page_name, verified_identity_id FROM tw_certified_pages WHERE page_id=?",
                (page_id,),
            ).fetchone()
        except Exception:
            row = None
        finally:
            conn.close()
        if row:
            regional["page_in_cert_library"] = True
            regional["verified_identity_id"] = str(row["verified_identity_id"] or "").strip()
            if regional["verified_identity_id"]:
                regional["message"] = "已找到主页库 Verified ID"
            else:
                regional["message"] = "主页库有该主页，但缺少 Verified ID"
        else:
            regional["message"] = "该主页不在认证主页库中"

    if best:
        overall = "pass"
        message = "当前主页可读取 Lead Form，具备自动建表单的必要条件"
    elif any(r.get("page_visible") for r in token_results):
        overall = "fail"
        message = "主页可见，但 Lead Form/Page Token 权限不足"
    else:
        overall = "fail"
        message = "所有可用 Token 都看不到该主页"

    if regional["required"] and not regional["verified_identity_id"]:
        overall = "fail"
        message += "；目标国家需要区域声明，但该主页缺少 Verified ID"

    return {
        "success": True,
        "act_id": act_id,
        "page_id": page_id,
        "overall": overall,
        "message": message,
        "tokens": token_results,
        "regional": regional,
    }


@router.get("/{act_id}/fb-pixels")
def get_fb_pixels(act_id: str, user=Depends(get_current_user)):
    """
    从 Facebook API 拉取该广告账户下的像素列表。
    返回格式：[{id, name, last_fired_time, can_use}]
    """
    import requests as _req
    from services.token_manager import get_exec_token, ACTION_READ
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    conn.close()
    token = get_exec_token(act_id, ACTION_READ)
    if not token:
        raise HTTPException(400, "该账户无可用 Token，无法拉取像素列表")
    try:
        resp = _req.get(
            f"{FB_API_BASE}/{act_id}/adspixels",
            params={
                "access_token": token,
                "fields": "id,name,last_fired_time,is_unavailable",
                "limit": 50
            },
            timeout=12
        )
        data = resp.json()
    except Exception as e:
        raise HTTPException(502, f"FB API 请求失败: {e}")
    if "error" in data:
        err = data["error"]
        raise HTTPException(400, f"FB API 错误: {err.get('message','未知')} (code={err.get('code')})")
    pixels = []
    for px in data.get("data", []):
        is_unavailable = px.get("is_unavailable", False)
        pixels.append({
            "id": px.get("id"),
            "name": px.get("name", f"Pixel {px.get('id','')}"),
            "last_fired_time": px.get("last_fired_time"),
            "can_use": not is_unavailable
        })
    pixels.sort(key=lambda x: (0 if x["can_use"] else 1, x.get("name", "")))
    return {"success": True, "pixels": pixels}

@router.get("/currency-rates")
async def get_currency_rates(user=Depends(get_current_user)):
    """获取汇率信息（用于非USD货币换算）"""
    try:
        from services.currency import get_rates
        rates = await get_rates()
        return {"success": True, "rates": rates, "base": "USD"}
    except Exception:
        # 返回默认汇率
        return {"success": True, "rates": {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067, "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74, "AUD": 0.65, "CAD": 0.74}, "base": "USD"}


# ── 台湾广告认证身份管理 ──────────────────────────────────────────────────────

class TwAdvertiserCreate(BaseModel):
    name: str
    fb_user_id: Optional[str] = None
    beneficiary: str
    payer: str
    note: Optional[str] = None

class TwAdvertiserUpdate(BaseModel):
    name: Optional[str] = None
    fb_user_id: Optional[str] = None
    beneficiary: Optional[str] = None
    payer: Optional[str] = None
    note: Optional[str] = None
    verified: Optional[int] = None

@router.get("/tw-advertisers")
def list_tw_advertisers(user=Depends(get_current_user)):
    """列出所有台湾广告认证身份"""
    conn = get_conn()
    where, params = ["1=1"], []
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    rows = conn.execute(
        f"SELECT id, name, fb_user_id, beneficiary, payer, note, verified, created_at, team_id FROM tw_advertisers WHERE {' AND '.join(where)} ORDER BY id",
        params,
    ).fetchall()
    conn.close()
    return {"success": True, "advertisers": [dict(r) for r in rows]}

@router.post("/tw-advertisers")
def create_tw_advertiser(body: TwAdvertiserCreate, user=Depends(get_current_user)):
    """新增台湾广告认证身份"""
    conn = get_conn()
    resource_team_id = team_id_for_create(user)
    cur = conn.execute(
        "INSERT INTO tw_advertisers (name, fb_user_id, beneficiary, payer, note, verified, team_id) VALUES (?,?,?,?,?,1,?)",
        (body.name, body.fb_user_id, body.beneficiary, body.payer, body.note, resource_team_id)
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"success": True, "id": new_id}

@router.put("/tw-advertisers/{adv_id}")
def update_tw_advertiser(adv_id: int, body: TwAdvertiserUpdate, user=Depends(get_current_user)):
    """更新台湾广告认证身份"""
    conn = get_conn()
    assert_row_access(conn, "tw_advertisers", adv_id, user, allow_unassigned=False)
    updates, params = [], []
    if body.name is not None:
        updates.append("name=?"); params.append(body.name)
    if body.fb_user_id is not None:
        updates.append("fb_user_id=?"); params.append(body.fb_user_id)
    if body.beneficiary is not None:
        updates.append("beneficiary=?"); params.append(body.beneficiary)
    if body.payer is not None:
        updates.append("payer=?"); params.append(body.payer)
    if body.note is not None:
        updates.append("note=?"); params.append(body.note)
    if body.verified is not None:
        updates.append("verified=?"); params.append(body.verified)
    if updates:
        params.append(adv_id)
        conn.execute(f"UPDATE tw_advertisers SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"success": True}

@router.delete("/tw-advertisers/{adv_id}")
def delete_tw_advertiser(adv_id: int, user=Depends(get_current_user)):
    """删除台湾广告认证身份"""
    conn = get_conn()
    assert_row_access(conn, "tw_advertisers", adv_id, user, allow_unassigned=False)
    conn.execute("DELETE FROM tw_advertisers WHERE id=?", (adv_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ─────────────────────────────────────────────────────────────────────────────
# 台湾认证主页库（tw_certified_pages）
# 用户手动录入已完成台湾广告认证的主页，铺广告时自动匹配 Token 有权限的认证主页
# ─────────────────────────────────────────────────────────────────────────────

def _extract_verified_identity_id(value) -> Optional[str]:
    """Extract a Taiwan verified identity number from plain text like '丁玉香（编号：1311102860475960）'."""
    if value is None:
        return None
    import html as _html
    import re as _re

    text = _html.unescape(str(value or "").strip())
    if not text or text.lower() in {"none", "null", "undefined"}:
        return None

    def _decode_unicode_escapes(match):
        try:
            return chr(int(match.group(1), 16))
        except Exception:
            return match.group(0)

    text = _re.sub(r"\\u([0-9a-fA-F]{4})", _decode_unicode_escapes, text)
    if _re.fullmatch(r"\d{10,20}", text):
        return text

    keyword_patterns = [
        r"(?:編號|编号|編碼|编码|認證編號|认证编号|Verified\s*ID|Identity\s*ID|ID)\s*[：:\s]*([0-9]{10,20})",
        r"[（(]\s*(?:編號|编号|編碼|编码)\s*[：:\s]*([0-9]{10,20})\s*[）)]",
    ]
    for pattern in keyword_patterns:
        match = _re.search(pattern, text, flags=_re.IGNORECASE)
        if match:
            return match.group(1)

    digit_runs = _re.findall(r"(?<!\d)([0-9]{10,20})(?!\d)", text)
    return digit_runs[0] if len(digit_runs) == 1 else None


def _normalize_verified_identity_input(value) -> Optional[str]:
    raw = "" if value is None else str(value).strip()
    if not raw or raw.lower() in {"none", "null", "undefined"}:
        return None
    extracted = _extract_verified_identity_id(raw)
    if not extracted:
        raise HTTPException(400, "Verified ID 必须是数字，或包含「编号：数字」这类可识别文本")
    return extracted


def _extract_verified_identity_from_payload(payload) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, dict):
        trusted_keys = (
            "verified_identity_id",
            "regional_regulation_identity",
            "beneficiary",
            "payer",
            "bylines",
            "beneficiary_payers",
            "disclaimer",
            "about",
            "description",
            "name",
        )
        for key in trusted_keys:
            value = payload.get(key)
            extracted = _extract_verified_identity_from_payload(value)
            if extracted:
                return extracted
        for key, value in payload.items():
            if key in trusted_keys or key in {"id", "page_id", "business", "owner"}:
                continue
            if isinstance(value, (dict, list, tuple)):
                extracted = _extract_verified_identity_from_payload(value)
                if extracted:
                    return extracted
        return None
    if isinstance(payload, (list, tuple)):
        for item in payload:
            extracted = _extract_verified_identity_from_payload(item)
            if extracted:
                return extracted
        return None
    return _extract_verified_identity_id(payload)


def _probe_page_verified_identity(page_id: str, page_token: str):
    """Probe public/page-token payloads for a Taiwan verified identity number."""
    import requests as _req
    if not page_id:
        return None, None
    if page_token:
        field_sets = [
            "id,name,verification_status,about,description,business",
            "id,name,category,link,username,fan_count",
        ]
        for fields in field_sets:
            try:
                r = _req.get(
                    f"https://graph.facebook.com/v25.0/{page_id}",
                    params={"fields": fields, "access_token": page_token},
                    timeout=8,
                )
                d = r.json()
                extracted = _extract_verified_identity_from_payload(d)
                if extracted and extracted != str(page_id):
                    return extracted, "page_graph"
            except Exception:
                pass

    for url in (
        f"https://www.facebook.com/{page_id}",
        f"https://m.facebook.com/{page_id}",
    ):
        try:
            r = _req.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                },
                timeout=10,
            )
            extracted = _extract_verified_identity_id(r.text)
            if extracted and extracted != str(page_id):
                return extracted, "public_page_text"
        except Exception:
            pass
    return None, None


def _cleanup_bad_verified_identity_rows(conn):
    """Clear legacy auto IDs that were accidentally set to the page ID."""
    try:
        conn.execute(
            """
            UPDATE tw_certified_pages
            SET verified_identity_id=NULL, verified_source=NULL
            WHERE verified_source IN ('auto_badge','auto_bm')
              AND verified_identity_id=page_id
            """
        )
    except Exception:
        pass


def _ensure_tw_page_status_columns(conn):
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(tw_certified_pages)").fetchall()}
    columns = {
        "page_category": "TEXT DEFAULT NULL",
        "page_is_published": "INTEGER DEFAULT NULL",
        "page_verification_status": "TEXT DEFAULT NULL",
        "page_tasks": "TEXT DEFAULT NULL",
        "page_can_advertise": "INTEGER DEFAULT NULL",
        "page_lead_form_status": "TEXT DEFAULT NULL",
        "page_status": "TEXT DEFAULT NULL",
        "page_status_hint": "TEXT DEFAULT NULL",
        "page_status_checked_at": "TEXT DEFAULT NULL",
    }
    for name, ddl in columns.items():
        if name not in cols:
            try:
                conn.execute(f"ALTER TABLE tw_certified_pages ADD COLUMN {name} {ddl}")
            except Exception:
                pass
    try:
        conn.commit()
    except Exception:
        pass


def _tw_page_status_checked_at():
    from datetime import datetime, timezone, timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


def _probe_tw_page_lead_form_status(page_id: str, page_token: str):
    if not page_id or not page_token:
        return "unknown", "缺少 page token，无法确认 Lead 表单权限"
    try:
        r = requests.get(
            f"{FB_API_BASE}/{page_id}/leadgen_forms",
            params={"access_token": page_token, "limit": 1},
            timeout=10,
        )
        d = r.json()
    except Exception as exc:
        return "unknown", f"暂时无法确认 Lead 表单权限：{exc}"
    if "error" not in d:
        return "ok", "可自动创建 Lead 表单"
    err = d.get("error") or {}
    msg = str(err.get("message") or "")
    code = err.get("code")
    lower = msg.lower()
    if code in {10, 200} or "permission" in lower or "pages_manage_ads" in lower:
        return "fail", "缺少 pages_manage_ads，无法自动创建 Lead 表单"
    return "warn", f"Lead 表单权限未确认：{msg or err}"


def _probe_tw_page_status(page_id: str, page_token: str, page_payload: dict = None) -> dict:
    payload = dict(page_payload or {})
    if page_id and page_token:
        try:
            r = requests.get(
                f"{FB_API_BASE}/{page_id}",
                params={
                    "fields": "id,name,category,is_published,tasks,verification_status",
                    "access_token": page_token,
                },
                timeout=10,
            )
            d = r.json()
            if "error" not in d:
                payload.update(d)
        except Exception:
            pass

    tasks = payload.get("tasks") or []
    if isinstance(tasks, str):
        try:
            tasks = json.loads(tasks)
        except Exception:
            tasks = [tasks]
    tasks = [str(t).upper() for t in tasks if str(t or "").strip()]

    raw_published = payload.get("is_published")
    if isinstance(raw_published, str):
        is_published = raw_published.strip().lower() not in {"0", "false", "no"}
    elif raw_published is None:
        is_published = None
    else:
        is_published = bool(raw_published)

    task_can_advertise = True if not tasks else ("ADVERTISE" in tasks)
    can_advertise = task_can_advertise and is_published is not False
    if is_published is False:
        lead_status, lead_hint = "fail", "主页未发布，无法自动创建 Lead 表单"
    elif not can_advertise:
        lead_status, lead_hint = "fail", "该主页当前没有广告投放权限"
    else:
        lead_status, lead_hint = _probe_tw_page_lead_form_status(page_id, page_token)

    hints = []
    if is_published is False:
        hints.append("主页未发布")
    if not task_can_advertise:
        hints.append("缺少 ADVERTISE 广告权限")
    if can_advertise and lead_status in {"fail", "warn", "unknown"}:
        hints.append(lead_hint)
    if not hints:
        hints.append("主页可投放" if can_advertise else lead_hint)

    if is_published is False or not can_advertise:
        page_status = "restricted"
    elif lead_status == "ok":
        page_status = "ok"
    else:
        page_status = "warn"

    return {
        "page_category": payload.get("category") or "",
        "page_is_published": None if is_published is None else (1 if is_published else 0),
        "page_verification_status": payload.get("verification_status") or "",
        "page_tasks": json.dumps(tasks, ensure_ascii=False),
        "page_can_advertise": 1 if can_advertise else 0,
        "page_lead_form_status": lead_status,
        "page_status": page_status,
        "page_status_hint": "；".join(hints),
        "page_status_checked_at": _tw_page_status_checked_at(),
    }


@router.get("/tw-certified-pages/resolve-name")
def resolve_tw_page_name(page_id: str, token_id: int = None, user=Depends(get_current_user)):
    """
    根据主页 ID 自动获取主页名称。
    同时尽力从主页公开文本中识别 Verified ID，识别不到时仍允许用户手动填写。
    """
    import requests as _req
    conn = get_conn()
    token_plain = None
    token_alias = None

    if token_id:
        assert_row_access(conn, "fb_tokens", token_id, user, allow_unassigned=False)
        # 使用指定 Token
        row = conn.execute(
            "SELECT access_token_enc, token_alias FROM fb_tokens WHERE id=? AND status='active'",
            (token_id,)
        ).fetchone()
        if row:
            token_plain = decrypt_token(row["access_token_enc"])
            token_alias = row["token_alias"]

    if not token_plain:
        # 回退：用任意有效 Token
        token_where, token_params = ["status='active'"], []
        apply_team_scope(token_where, token_params, user, "team_id", include_unassigned=False)
        op_rows = conn.execute(
            "SELECT access_token_enc, token_alias FROM fb_tokens WHERE " + " AND ".join(token_where) + " ORDER BY id LIMIT 10",
            token_params,
        ).fetchall()
        for row in op_rows:
            plain = decrypt_token(row["access_token_enc"])
            if plain:
                token_plain = plain
                token_alias = row["token_alias"]
                break
    conn.close()

    if not token_plain:
        raise HTTPException(400, "系统中无有效 Token，无法自动获取主页名称")

    import concurrent.futures as _cf
    try:
        page_name = None
        page_token = None
        found_token_alias = token_alias

        conn2 = get_conn()
        all_where, all_params = ["status='active'"], []
        apply_team_scope(all_where, all_params, user, "team_id", include_unassigned=False)
        all_tokens = conn2.execute(
            "SELECT id, access_token_enc, token_alias FROM fb_tokens WHERE " + " AND ".join(all_where) + " ORDER BY id",
            all_params,
        ).fetchall()
        conn2.close()

        # 优先用指定 token_id 的 Token，放在最前面
        token_rows = []
        if token_id:
            for r in all_tokens:
                if r["id"] == token_id:
                    token_rows.insert(0, r)
                else:
                    token_rows.append(r)
        else:
            token_rows = list(all_tokens)

        # 将所有 Token 解密，过滤掉无效的
        plain_tokens = []
        for t_row in token_rows:
            plain = decrypt_token(t_row["access_token_enc"])
            if plain:
                plain_tokens.append((t_row["id"], plain, t_row["token_alias"]))

        def try_me_accounts(args):
            tid, plain, alias = args
            try:
                r = _req.get(
                    "https://graph.facebook.com/v25.0/me/accounts",
                    params={"fields": "id,name,access_token", "access_token": plain, "limit": 200},
                    timeout=5
                )
                d = r.json()
                for pg in d.get("data", []):
                    if str(pg.get("id", "")) == str(page_id):
                        return (pg.get("name", ""), pg.get("access_token", ""), alias)
            except Exception:
                pass
            return None

        # 并发查询所有 Token 的 /me/accounts，最多等 8 秒
        with _cf.ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(try_me_accounts, args): args for args in plain_tokens}
            for fut in _cf.as_completed(futures, timeout=8):
                result = fut.result()
                if result:
                    page_name, page_token, found_token_alias = result
                    break

        # 如果 /me/accounts 找不到，尝试直接查公开主页信息（只用前3个 Token）
        if not page_name:
            for _, plain, alias in plain_tokens[:3]:
                try:
                    r_pub = _req.get(
                        f"https://graph.facebook.com/v25.0/{page_id}",
                        params={"fields": "id,name", "access_token": plain},
                        timeout=5
                    )
                    d_pub = r_pub.json()
                    if d_pub.get("name") and "error" not in d_pub:
                        page_name = d_pub["name"]
                        found_token_alias = alias
                        break
                except Exception:
                    continue

        if not page_name:
            raise HTTPException(400, "无法获取主页名称，请检查主页 ID 是否正确，或手动填写主页名称")

        verified_identity_id, verified_identity_source = _probe_page_verified_identity(page_id, page_token)

        return {
            "success": True,
            "page_id": page_id,
            "page_name": page_name,
            "verified_identity_id": verified_identity_id,
            "verified_identity_source": verified_identity_source,
            "me_user_id": None,
            "owner_hint": (
                f"已自动识别 Verified ID：{verified_identity_id}"
                if verified_identity_id
                else "未从主页公开信息识别到 Verified ID，请以广告后台已通过审核的受益者 ID 为准手动填写"
            ),
            "token_used": found_token_alias
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"获取主页名称失败：{e}")


@router.get("/tw-certified-pages")
def list_tw_certified_pages(user=Depends(get_current_user)):
    """列出所有台湾认证主页（含归属矩阵和Token信息）"""
    conn = get_conn()
    # 确保新字段存在（兼容旧数据库）
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN matrix_id INTEGER DEFAULT NULL")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN token_id INTEGER DEFAULT NULL REFERENCES fb_tokens(id)")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN verified_source TEXT DEFAULT NULL")
        conn.commit()
    except Exception:
        pass
    _ensure_tw_page_status_columns(conn)
    _cleanup_bad_verified_identity_rows(conn)
    conn.commit()
    where, params = ["1=1"], []
    apply_team_scope(where, params, user, "p.team_id", include_unassigned=False)
    rows = conn.execute(
        f"""
        SELECT p.id, p.page_id, p.page_name, p.verified_identity_id, p.verified_source, p.note, p.created_at,
               p.matrix_id, p.token_id, p.team_id, tm.name AS team_name,
               p.page_category, p.page_is_published, p.page_verification_status, p.page_tasks,
               p.page_can_advertise, p.page_lead_form_status, p.page_status, p.page_status_hint,
               p.page_status_checked_at,
               ft.token_alias, ft.matrix_id AS token_matrix_id
        FROM tw_certified_pages p
        LEFT JOIN fb_tokens ft ON ft.id = p.token_id
        LEFT JOIN teams tm ON tm.id = p.team_id
        WHERE {' AND '.join(where)}
        ORDER BY p.id
        """,
        params,
    ).fetchall()
    pages = []
    for r in rows:
        item = dict(r)
        linked_matrix_ids = set()
        for mid in (item.get("matrix_id"), item.get("token_matrix_id")):
            try:
                parsed = int(mid)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                linked_matrix_ids.add(parsed)
        item["linked_matrix_ids"] = sorted(linked_matrix_ids)
        pages.append(item)
    conn.close()
    return {"success": True, "pages": pages}


@router.post("/tw-certified-pages")
def create_tw_certified_page(body: dict, user=Depends(get_current_user)):
    """
    新增主页库记录。
    - page_id: 必填
    - page_name: 可选（留空则自动获取）
    - verified_identity_id: 台湾广告认证人编号（FB User ID），可选（仅支持手动填写）
    - matrix_id: 归属矩阵 ID（1=矩阵1, 2=矩阵2 等）
    - token_id: 对该主页有管理权限的 Token ID
    - note: 备注
    """
    import requests as _req
    page_id = str(body.get("page_id", "")).strip()
    page_name = str(body.get("page_name", "")).strip()
    note = body.get("note", "")
    verified_identity_id = _normalize_verified_identity_input(body.get("verified_identity_id"))
    matrix_id = body.get("matrix_id") or None
    token_id = body.get("token_id") or None
    if not page_id:
        raise HTTPException(400, "page_id 不能为空")
    resource_team_id = team_id_for_create(user)

    # 自动获取主页名称（优先用指定 token_id，否则用任意有效 Token）
    conn_tmp = get_conn()
    if token_id:
        assert_row_access(conn_tmp, "fb_tokens", int(token_id), user, allow_unassigned=False)
        token_rows = conn_tmp.execute(
            "SELECT access_token_enc FROM fb_tokens WHERE id=? AND status='active'",
            (token_id,)
        ).fetchall()
        if not matrix_id:
            token_matrix = conn_tmp.execute(
                "SELECT matrix_id FROM fb_tokens WHERE id=?",
                (token_id,),
            ).fetchone()
            if token_matrix and token_matrix["matrix_id"] not in (None, "", 0):
                matrix_id = token_matrix["matrix_id"]
    else:
        token_where, token_params = ["status='active'"], []
        apply_team_scope(token_where, token_params, user, "team_id", include_unassigned=False)
        token_rows = conn_tmp.execute(
            f"SELECT access_token_enc FROM fb_tokens WHERE {' AND '.join(token_where)} ORDER BY id LIMIT 10",
            token_params,
        ).fetchall()
    conn_tmp.close()

    for row_tmp in token_rows:
        plain_tmp = decrypt_token(row_tmp["access_token_enc"])
        if plain_tmp:
            try:
                resp_tmp = _req.get(
                    f"https://graph.facebook.com/v25.0/{page_id}",
                    params={"fields": "id,name", "access_token": plain_tmp},
                    timeout=10
                )
                data_tmp = resp_tmp.json()
                if "name" in data_tmp and not page_name:
                    page_name = data_tmp["name"]
            except Exception:
                pass
            break

    if not page_name:
        raise HTTPException(400, "主页名称自动获取失败，请手动填写 page_name")

    conn = get_conn()
    # 确保新字段存在
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN matrix_id INTEGER DEFAULT NULL")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN token_id INTEGER DEFAULT NULL REFERENCES fb_tokens(id)")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN verified_source TEXT DEFAULT NULL")
        conn.commit()
    except Exception:
        pass
    _ensure_tw_page_status_columns(conn)
    try:
        cur = conn.execute(
            "INSERT INTO tw_certified_pages (page_id, page_name, verified_identity_id, verified_source, note, matrix_id, token_id, team_id) VALUES (?,?,?,?,?,?,?,?)",
            (page_id, page_name, verified_identity_id, "manual" if verified_identity_id else None, note, matrix_id, token_id, resource_team_id)
        )
        conn.commit()
        new_id = cur.lastrowid
    except Exception as e:
        conn.close()
        raise HTTPException(400, f"添加失败（主页ID可能已存在）: {e}")
    conn.close()
    return {"success": True, "id": new_id, "page_name": page_name, "verified_identity_id": verified_identity_id, "matrix_id": matrix_id, "token_id": token_id}


@router.put("/tw-certified-pages/{page_db_id}")
def update_tw_certified_page(page_db_id: int, body: dict, user=Depends(get_current_user)):
    """更新台湾认证主页（支持更新 matrix_id、token_id、verified_identity_id 等字段）"""
    conn = get_conn()
    assert_row_access(conn, "tw_certified_pages", page_db_id, user, allow_unassigned=False)
    updates, params = [], []
    if "page_name" in body:
        updates.append("page_name=?"); params.append(body["page_name"])
    if "note" in body:
        updates.append("note=?"); params.append(body["note"])
    if "verified_identity_id" in body:
        verified_identity_id = _normalize_verified_identity_input(body.get("verified_identity_id"))
        updates.append("verified_identity_id=?")
        params.append(verified_identity_id)
        updates.append("verified_source=?")
        params.append("manual" if verified_identity_id else None)
    if "matrix_id" in body:
        updates.append("matrix_id=?")
        params.append(body["matrix_id"] or None)
    if "token_id" in body:
        updates.append("token_id=?")
        params.append(body["token_id"] or None)
    if updates:
        params.append(page_db_id)
        conn.execute(f"UPDATE tw_certified_pages SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"success": True}


@router.delete("/tw-certified-pages/{page_db_id}")
def delete_tw_certified_page(page_db_id: int, user=Depends(get_current_user)):
    """删除台湾认证主页"""
    conn = get_conn()
    assert_row_access(conn, "tw_certified_pages", page_db_id, user, allow_unassigned=False)
    conn.execute("DELETE FROM tw_certified_pages WHERE id=?", (page_db_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/tw-certified-pages/scan")
def scan_tw_certified_pages(user=Depends(get_current_user)):
    """
    自动扫描：遍历所有 active Token 的 /me/accounts，
    将所有主页入库 tw_certified_pages（已存在则跳过），
    同时尝试通过 page_token 获取 owner.id 作为更可信的候选值。
    返回：新增数量、已存在数量、扫描到的主页总数。
    """
    import requests as _req
    import concurrent.futures as _cf
    resource_team_id = team_id_for_create(user)

    conn = get_conn()
    # 确保字段存在
    for col_sql in [
        "ALTER TABLE tw_certified_pages ADD COLUMN matrix_id INTEGER DEFAULT NULL",
        "ALTER TABLE tw_certified_pages ADD COLUMN token_id INTEGER DEFAULT NULL REFERENCES fb_tokens(id)",
        "ALTER TABLE tw_certified_pages ADD COLUMN verified_source TEXT DEFAULT NULL",
    ]:
        try:
            conn.execute(col_sql); conn.commit()
        except Exception:
            pass
    _ensure_tw_page_status_columns(conn)
    _cleanup_bad_verified_identity_rows(conn)
    conn.commit()

    # 获取所有 active Token（含矩阵归属）
    token_where, token_params = ["ft.status = 'active'"], []
    apply_team_scope(token_where, token_params, user, "ft.team_id", include_unassigned=False)
    token_rows = conn.execute(
        f"""
        SELECT ft.id, ft.access_token_enc, ft.token_alias, ft.matrix_id
        FROM fb_tokens ft
        WHERE {' AND '.join(token_where)}
        ORDER BY ft.id
        """,
        token_params,
    ).fetchall()

    # 获取已存在的主页记录，重扫时刷新主页名/矩阵/Token 绑定
    existing_where, existing_params = ["1=1"], []
    apply_team_scope(existing_where, existing_params, user, "team_id", include_unassigned=False)
    existing_rows = {
        r["page_id"]: dict(r)
        for r in conn.execute(
            f"SELECT * FROM tw_certified_pages WHERE {' AND '.join(existing_where)}",
            existing_params,
        ).fetchall()
    }
    existing = set(existing_rows.keys())
    conn.close()

    # 解密所有 Token
    plain_tokens = []
    def _get_token_user_id(t):
        plain = decrypt_token(t["access_token_enc"])
        if not plain:
            return None
        try:
            return {
                "id": t["id"],
                "plain": plain,
                "alias": t["token_alias"],
                "matrix_id": t["matrix_id"],
            }
        except Exception:
            return {
                "id": t["id"],
                "plain": plain,
                "alias": t["token_alias"],
                "matrix_id": t["matrix_id"],
            }

    import concurrent.futures as _cf2
    with _cf2.ThreadPoolExecutor(max_workers=8) as ex2:
        results2 = list(ex2.map(_get_token_user_id, token_rows, timeout=20))
    plain_tokens = [r for r in results2 if r is not None]

    def fetch_pages_for_token(t_info):
        """获取单个 Token 管理的所有主页"""
        results = []
        try:
            r = _req.get(
                "https://graph.facebook.com/v25.0/me/accounts",
                params={"fields": "id,name,access_token,category,is_published,tasks,verification_status", "access_token": t_info["plain"], "limit": 200},
                timeout=8
            )
            d = r.json()
            if "error" in d:
                r = _req.get(
                    "https://graph.facebook.com/v25.0/me/accounts",
                    params={"fields": "id,name,access_token", "access_token": t_info["plain"], "limit": 200},
                    timeout=8
                )
                d = r.json()
            for pg in d.get("data", []):
                if pg.get("id") and pg.get("name"):
                    results.append({
                        "page_id": str(pg["id"]),
                        "page_name": pg["name"],
                        "page_token": pg.get("access_token"),
                        "category": pg.get("category"),
                        "is_published": pg.get("is_published"),
                        "tasks": pg.get("tasks"),
                        "verification_status": pg.get("verification_status"),
                        "token_id": t_info["id"],
                        "token_alias": t_info["alias"],
                        "matrix_id": t_info["matrix_id"],
                    })
        except Exception:
            pass
        return results

    # 并发扫描所有 Token
    all_pages = []
    with _cf.ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_pages_for_token, t) for t in plain_tokens]
        for fut in _cf.as_completed(futures, timeout=20):
            try:
                all_pages.extend(fut.result())
            except Exception:
                pass

    # 去重（同一主页可能被多个 Token 管理，保留第一个）
    seen_pages = {}
    for pg in all_pages:
        current = seen_pages.get(pg["page_id"])
        if not current:
            seen_pages[pg["page_id"]] = pg
            continue
        current_score = (
            1 if current.get("matrix_id") is not None else 0,
            1 if current.get("token_id") is not None else 0,
        )
        candidate_score = (
            1 if pg.get("matrix_id") is not None else 0,
            1 if pg.get("token_id") is not None else 0,
        )
        if candidate_score > current_score:
            seen_pages[pg["page_id"]] = pg

    all_page_list = list(seen_pages.values())

    # ── 自动探测每个主页的 verified_identity_id 候选 ──
    auto_identified = 0
    auto_probe_failed = 0
    for pg in all_page_list:
        page_token = pg.get("page_token")
        if not page_token:
            pg["verified_candidate"] = None
            pg["verified_source"] = None
            continue
        try:
            candidate, source = _probe_page_verified_identity(pg["page_id"], page_token)
            if candidate:
                pg["verified_candidate"] = candidate
                pg["verified_source"] = source or "auto_detected"
                auto_identified += 1
            else:
                pg["verified_candidate"] = None
                pg["verified_source"] = None
        except Exception:
            auto_probe_failed += 1
            pg["verified_candidate"] = None
            pg["verified_source"] = None
        try:
            pg.update(_probe_tw_page_status(pg["page_id"], page_token, pg))
        except Exception as status_exc:
            pg.update({
                "page_category": pg.get("category") or "",
                "page_is_published": None,
                "page_verification_status": pg.get("verification_status") or "",
                "page_tasks": json.dumps(pg.get("tasks") or [], ensure_ascii=False),
                "page_can_advertise": None,
                "page_lead_form_status": "unknown",
                "page_status": "unknown",
                "page_status_hint": f"主页状态检查失败：{status_exc}",
                "page_status_checked_at": _tw_page_status_checked_at(),
            })

    # 分为新增和已存在两组
    new_pages = [pg for pg in all_page_list if pg["page_id"] not in existing]

    # 写入数据库
    added = 0
    refreshed_existing = 0
    skipped = 0
    conn2 = get_conn()
    for pg in all_page_list:
        if pg["page_id"] not in existing:
            # 新增
            try:
                conn2.execute(
                    "INSERT OR IGNORE INTO tw_certified_pages "
                    "(page_id, page_name, verified_identity_id, verified_source, note, matrix_id, token_id, "
                    "page_category, page_is_published, page_verification_status, page_tasks, "
                    "page_can_advertise, page_lead_form_status, page_status, page_status_hint, page_status_checked_at, team_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        pg["page_id"],
                        pg["page_name"],
                        pg.get("verified_candidate") or None,
                        pg.get("verified_source") or None,
                        f"自动扫描（Token: {pg['token_alias']}）",
                        pg.get("matrix_id") or None,
                        pg.get("token_id") or None,
                        pg.get("page_category") or None,
                        pg.get("page_is_published"),
                        pg.get("page_verification_status") or None,
                        pg.get("page_tasks") or None,
                        pg.get("page_can_advertise"),
                        pg.get("page_lead_form_status") or None,
                        pg.get("page_status") or None,
                        pg.get("page_status_hint") or None,
                        pg.get("page_status_checked_at") or None,
                        resource_team_id,
                    )
                )
                added += 1
            except Exception:
                pass
        else:
            try:
                conn2.execute(
                    """
                    UPDATE tw_certified_pages
                    SET page_name=?,
                        matrix_id=?,
                        token_id=?,
                        verified_identity_id=CASE WHEN verified_source='manual' THEN verified_identity_id ELSE COALESCE(?, verified_identity_id) END,
                        verified_source=CASE WHEN verified_source='manual' THEN verified_source ELSE COALESCE(?, verified_source) END,
                        page_category=?,
                        page_is_published=?,
                        page_verification_status=?,
                        page_tasks=?,
                        page_can_advertise=?,
                        page_lead_form_status=?,
                        page_status=?,
                        page_status_hint=?,
                        page_status_checked_at=?,
                        team_id=COALESCE(team_id, ?)
                    WHERE page_id=?
                    """,
                    (
                        pg["page_name"],
                        pg.get("matrix_id") or None,
                        pg.get("token_id") or None,
                        pg.get("verified_candidate") or None,
                        pg.get("verified_source") or None,
                        pg.get("page_category") or None,
                        pg.get("page_is_published"),
                        pg.get("page_verification_status") or None,
                        pg.get("page_tasks") or None,
                        pg.get("page_can_advertise"),
                        pg.get("page_lead_form_status") or None,
                        pg.get("page_status") or None,
                        pg.get("page_status_hint") or None,
                        pg.get("page_status_checked_at") or None,
                        resource_team_id,
                        pg["page_id"],
                    )
                )
                refreshed_existing += conn2.execute("SELECT changes()").fetchone()[0]
            except Exception:
                pass
            skipped += 1
    conn2.commit()
    conn2.close()

    return {
        "success": True,
        "scanned_tokens": len(plain_tokens),
        "total_pages_found": len(seen_pages),
        "added": added,
        "refreshed_existing": refreshed_existing,
        "auto_identified": auto_identified,
        "auto_probe_failed": auto_probe_failed,
        "skipped_existing": skipped,
        "pages": [
            {
                "page_id": pg["page_id"],
                "page_name": pg["page_name"],
                "verified_identity_id": (existing_rows.get(pg["page_id"]) or {}).get("verified_identity_id"),
                "verified_source": (existing_rows.get(pg["page_id"]) or {}).get("verified_source"),
                "page_status": pg.get("page_status"),
                "page_status_hint": pg.get("page_status_hint"),
                "token_alias": pg["token_alias"],
                "matrix_id": pg.get("matrix_id"),
                "is_new": pg["page_id"] not in existing
            }
            for pg in seen_pages.values()
        ]
    }


@router.get("/{act_id}/tw-matched-pages")
def get_tw_matched_pages(act_id: str, user=Depends(get_current_user)):
    """
    自动匹配：用该账户绑定的 Token 查询 FB 主页列表，
    与主页库中已填写 Verified ID 的记录对比，返回匹配到的可投认证主页
    """
    import requests as _req
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    _ensure_tw_page_status_columns(conn)
    try:
        from services.token_manager import get_matrix_id_for_account
        account_matrix_id = get_matrix_id_for_account(act_id)
    except Exception:
        account_matrix_id = None

    count_where, count_params = [], []
    if account_matrix_id is not None:
        count_where.append("matrix_id=?")
        count_params.append(account_matrix_id)
    apply_team_scope(count_where, count_params, user, "team_id", include_unassigned=False)
    count_clause = ("WHERE " + " AND ".join(count_where)) if count_where else ""
    all_pages_count = conn.execute(
        f"SELECT COUNT(*) FROM tw_certified_pages {count_clause}",
        count_params,
    ).fetchone()[0]

    # 仅返回已填写 Verified ID 的主页库记录
    certified_sql = """
        SELECT p.page_id, p.page_name, p.verified_identity_id, p.matrix_id, p.token_id, ft.token_alias,
               p.page_status, p.page_status_hint, p.page_is_published, p.page_can_advertise,
               p.page_lead_form_status
        FROM tw_certified_pages p
        LEFT JOIN fb_tokens ft ON ft.id = p.token_id
        WHERE p.verified_identity_id IS NOT NULL
          AND TRIM(p.verified_identity_id) != ''
          AND LOWER(TRIM(p.verified_identity_id)) NOT IN ('none','null','undefined')
          AND COALESCE(p.page_is_published, 1) != 0
          AND COALESCE(p.page_can_advertise, 1) != 0
          AND COALESCE(p.page_status, 'ok') NOT IN ('restricted','unpublished')
    """
    certified_params = []
    if account_matrix_id is not None:
        certified_sql += " AND p.matrix_id=?"
        certified_params.append(account_matrix_id)
    cert_scope_where, cert_scope_params = [], []
    apply_team_scope(cert_scope_where, cert_scope_params, user, "p.team_id", include_unassigned=False)
    if cert_scope_where:
        certified_sql += " AND " + " AND ".join(cert_scope_where)
        certified_params.extend(cert_scope_params)
    certified = conn.execute(certified_sql, tuple(certified_params)).fetchall()
    certified_ids = {
        r["page_id"]: {
            "page_id": r["page_id"],
            "page_name": r["page_name"],
            "verified_identity_id": r["verified_identity_id"],
            "matrix_id": r["matrix_id"],
            "token_id": r["token_id"],
            "token_alias": r["token_alias"],
            "page_status": r["page_status"],
            "page_status_hint": r["page_status_hint"],
            "page_is_published": r["page_is_published"],
            "page_can_advertise": r["page_can_advertise"],
            "page_lead_form_status": r["page_lead_form_status"],
        }
        for r in certified
    }

    if not certified_ids:
        conn.close()
        return {"success": True, "matched": [], "all_certified": [], "all_pages_count": all_pages_count, "matrix_id": account_matrix_id}

    # 获取该账户的操作号 Token（优先）或管理号 Token
    token_row = None
    # 先找操作号池
    op_rows = conn.execute(
        """SELECT ft.access_token_enc FROM account_op_tokens aot
           JOIN fb_tokens ft ON ft.id = aot.token_id
           WHERE aot.act_id=? AND ft.status='active'
           ORDER BY aot.priority LIMIT 1""",
        (act_id,)
    ).fetchone()
    if op_rows:
        token_row = op_rows
    else:
        # 用管理号 Token
        mgr = conn.execute(
            """SELECT ft.access_token_enc FROM accounts a
               JOIN fb_tokens ft ON ft.id = a.token_id
               WHERE a.act_id=? AND ft.status='active' LIMIT 1""",
            (act_id,)
        ).fetchone()
        if not mgr:
            # 回退：用 fb_tokens 表中任意 active 的 user token
            fallback_where, fallback_params = ["status='active'"], []
            apply_team_scope(fallback_where, fallback_params, user, "team_id", include_unassigned=False)
            mgr = conn.execute(
                "SELECT access_token_enc FROM fb_tokens WHERE " + " AND ".join(fallback_where) + " LIMIT 1",
                fallback_params,
            ).fetchone()
        if mgr:
            token_row = mgr

    conn.close()

    if not token_row:
        return {"success": True, "matched": [], "all_certified": list(certified_ids.values()), "all_pages_count": all_pages_count, "matrix_id": account_matrix_id, "error": "无可用Token"}

    try:
        raw = decrypt_token(token_row["access_token_enc"])
    except Exception as e:
        return {"success": True, "matched": [], "all_certified": list(certified_ids.values()), "all_pages_count": all_pages_count, "matrix_id": account_matrix_id, "error": f"Token解密失败: {e}"}

    # 拉取该 Token 有管理权限的主页列表
    matched = []
    try:
        r = _req.get("https://graph.facebook.com/v25.0/me/accounts", params={
            "fields": "id,name,access_token",
            "access_token": raw,
            "limit": 100
        }, timeout=10)
        data = r.json()
        pages = data.get("data", [])
        for p in pages:
            pid = str(p.get("id", ""))
            if pid in certified_ids:
                cert_info = certified_ids[pid]
                matched.append({
                    "page_id": pid,
                    "page_name": cert_info["page_name"],
                    "verified_identity_id": cert_info["verified_identity_id"],
                    "matrix_id": cert_info["matrix_id"],
                    "token_id": cert_info["token_id"],
                    "token_alias": cert_info["token_alias"],
                    "page_status": cert_info.get("page_status"),
                    "page_status_hint": cert_info.get("page_status_hint"),
                    "page_is_published": cert_info.get("page_is_published"),
                    "page_can_advertise": cert_info.get("page_can_advertise"),
                    "page_lead_form_status": cert_info.get("page_lead_form_status"),
                    "fb_name": p.get("name", "")
                })
    except Exception as e:
        return {"success": True, "matched": [], "all_certified": list(certified_ids.values()), "all_pages_count": all_pages_count, "matrix_id": account_matrix_id, "error": f"FB API 调用失败: {e}"}

    return {
        "success": True,
        "matched": matched,
        "all_pages_count": all_pages_count,
        "matrix_id": account_matrix_id,
        "all_certified": [
            {
                "page_id": k,
                "page_name": v["page_name"],
                "verified_identity_id": v["verified_identity_id"],
                "matrix_id": v["matrix_id"],
                "token_id": v["token_id"],
                "token_alias": v["token_alias"],
                "page_status": v.get("page_status"),
                "page_status_hint": v.get("page_status_hint"),
                "page_is_published": v.get("page_is_published"),
                "page_can_advertise": v.get("page_can_advertise"),
                "page_lead_form_status": v.get("page_lead_form_status"),
            }
            for k, v in certified_ids.items()
        ]
    }


# ─────────────────────────────────────────────────────────────────────────────
# 主页消息功能检查（铺广告前置检查）
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/{act_id}/check-page-messaging")
def check_page_messaging(act_id: str, page_id: str, user=Depends(get_current_user)):
    """
    检查指定主页是否开启了消息功能（messaging_feature_status）。
    用于铺消息广告前的前置检查，避免因主页消息关闭导致 Ad 创建失败。
    返回：{messaging_enabled: bool, page_name: str, error: str|null}
    """
    import requests as _req
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    conn.close()

    def _get_probe_tokens(act_id_: str, page_id_: str) -> list[str]:
        """获取消息能力探测用 Token，优先 page token，再回退用户 token。"""
        conn = get_conn()
        rows = conn.execute("""
            SELECT t.access_token_enc FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id = aot.token_id
            WHERE aot.act_id = ? AND aot.status = 'active' AND t.status = 'active'
        """, (act_id_,)).fetchall()
        tokens = []
        if rows:
            for row in rows:
                plain = decrypt_token(row["access_token_enc"])
                if plain:
                    tokens.append(plain)
        if not tokens:
            row = conn.execute("""
                SELECT t.access_token_enc FROM accounts a
                JOIN fb_tokens t ON t.id = a.token_id
                WHERE a.act_id = ? AND t.status = 'active'
                LIMIT 1
            """, (act_id_,)).fetchone()
            if row:
                plain = decrypt_token(row["access_token_enc"])
                if plain:
                    tokens.append(plain)
        conn.close()
        probe_tokens = []
        for plain in tokens:
            probe_tokens.append(plain)
            try:
                page_resp = _req.get(
                    f"{FB_API_BASE}/me/accounts",
                    params={"access_token": plain, "fields": "id,access_token", "limit": 200},
                    timeout=10,
                )
                page_data = page_resp.json()
                for page in page_data.get("data", []) or []:
                    if str(page.get("id")) == str(page_id_) and page.get("access_token"):
                        probe_tokens.insert(0, str(page["access_token"]))
                        break
            except Exception:
                continue
        # 去重并保持顺序
        deduped = []
        seen = set()
        for tk in probe_tokens:
            if tk and tk not in seen:
                deduped.append(tk)
                seen.add(tk)
        return deduped

    probe_tokens = _get_probe_tokens(act_id, page_id)
    if not probe_tokens:
        return {"messaging_enabled": None, "page_name": "", "error": "无可用 Token，无法检查主页状态"}

    try:
        data = None
        for token in probe_tokens:
            r = _req.get(
                f"{FB_API_BASE}/{page_id}",
                params={
                    "access_token": token,
                    "fields": "id,name,messaging_feature_status,features"
                },
                timeout=10
            )
            data = r.json()
            if "error" not in data:
                break

        if "error" in data:
            err = data["error"]
            return {
                "messaging_enabled": None,
                "page_name": "",
                "error": f"FB API 错误: {err.get('message', str(err))}"
            }

        page_name = data.get("name", page_id)
        messaging_feature_status = data.get("messaging_feature_status", {})
        features = data.get("features", [])

        # messaging_feature_status 是一个 dict，key 为功能名，value 为状态
        # 主要检查 "MESSENGER_PLATFORM" 或 "WHATSAPP_PLATFORM" 是否为 "ENABLED"
        # 如果字段不存在，尝试通过 features 列表判断
        messaging_enabled = None

        if messaging_feature_status:
            # 检查 MESSENGER_PLATFORM 状态
            messenger_status = messaging_feature_status.get("MESSENGER_PLATFORM", "")
            if messenger_status:
                messaging_enabled = (messenger_status.upper() == "ENABLED")
            else:
                # 任意消息平台启用即可
                for k, v in messaging_feature_status.items():
                    if "MESSAG" in k.upper() or "WHATSAPP" in k.upper():
                        if str(v).upper() == "ENABLED":
                            messaging_enabled = True
                            break
                if messaging_enabled is None:
                    messaging_enabled = False
        elif features:
            # features 是字符串列表
            messaging_enabled = any(
                "messag" in str(f).lower() or "whatsapp" in str(f).lower()
                for f in features
            )
        else:
            # 无法确定，返回 None（不阻止铺广告，只警告）
            messaging_enabled = None

        return {
            "messaging_enabled": messaging_enabled,
            "page_name": page_name,
            "error": None
        }

    except Exception as e:
        return {
            "messaging_enabled": None,
            "page_name": "",
            "error": f"检查失败: {str(e)}"
        }



# ══════════════════════════════════════════════════════════════
# 批量账户配置 / CSV 导入导出 / 重新扫描
# ══════════════════════════════════════════════════════════════

class BatchConfigPayload(BaseModel):
    act_ids: List[str]
    target_countries: Optional[str] = None
    target_age_min: Optional[int] = None
    target_age_max: Optional[int] = None
    target_gender: Optional[int] = None
    target_placements: Optional[str] = None
    target_objective_type: Optional[str] = None
    landing_url: Optional[str] = None


@router.post("/batch-config")
async def batch_config_accounts(
    payload: BatchConfigPayload,
    current_user=Depends(get_current_user)
):
    _require_operator_user(current_user)
    """批量修改多个账户的投放配置"""
    conn = get_conn()
    try:
        updated = 0
        for act_id in payload.act_ids:
            row = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
            if not row:
                continue
            acc_id = row[0]
            assert_row_access(conn, "accounts", acc_id, current_user, allow_unassigned=False)
            fields = []
            vals = []
            if payload.target_countries is not None:
                fields.append("target_countries=?"); vals.append(payload.target_countries)
            if payload.target_age_min is not None:
                fields.append("target_age_min=?"); vals.append(payload.target_age_min)
            if payload.target_age_max is not None:
                fields.append("target_age_max=?"); vals.append(payload.target_age_max)
            if payload.target_gender is not None:
                fields.append("target_gender=?"); vals.append(payload.target_gender)
            if payload.target_placements is not None:
                fields.append("target_placements=?"); vals.append(payload.target_placements)
            if payload.target_objective_type is not None:
                fields.append("target_objective_type=?"); vals.append(payload.target_objective_type)
            if payload.landing_url is not None:
                fields.append("landing_url=?"); vals.append(payload.landing_url)
            if fields:
                vals.append(acc_id)
                conn.execute(f"UPDATE accounts SET {', '.join(fields)} WHERE id=?", vals)
                updated += 1
        conn.commit()
        return {"updated": updated, "total": len(payload.act_ids)}
    finally:
        conn.close()


@router.get("/export-config")
async def export_account_config(
    token: str = None,
    current_user=None
):
    # Support ?token= query param for direct browser download
    from fastapi import HTTPException
    from core.auth import decode_token
    if current_user is None:
        if not token:
            raise HTTPException(status_code=401, detail="Not authenticated")
        try:
            current_user = decode_token(token)
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")
    current_user = normalize_user_claims(current_user)
    """导出所有账户配置为 CSV"""
    import csv, io
    from fastapi.responses import StreamingResponse

    conn = get_conn()
    try:
        where, params = [], []
        apply_team_scope(where, params, current_user, "team_id", include_unassigned=False)
        _apply_account_owner_scope(where, params, current_user, "")
        where_clause = (" WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            "SELECT act_id, name, target_countries, target_age_min, target_age_max, "
            "target_gender, target_placements, target_objective_type, landing_url FROM accounts" + where_clause,
            params,
        ).fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["act_id", "name", "target_countries", "target_age_min", "target_age_max",
                     "target_gender", "target_placements", "target_objective_type", "landing_url"])
    for r in rows:
        writer.writerow([r[0] or "", r[1] or "", r[2] or "", r[3] or 25, r[4] or 65,
                         r[5] or 0, r[6] or "", r[7] or "", r[8] or ""])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=account_config.csv"}
    )


@router.post("/import-config")
async def import_account_config(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user)
):
    _require_operator_user(current_user)
    """从 CSV 文件批量导入账户配置"""
    import csv, io
    raw = await file.read()
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    conn = get_conn()
    try:
        updated = 0
        errors = []
        for row in reader:
            act_id = row.get("act_id", "").strip()
            if not act_id:
                continue
            acc = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
            if not acc:
                errors.append(f"账户 {act_id} 不存在")
                continue
            try:
                assert_row_access(conn, "accounts", acc[0], current_user, allow_unassigned=False)
                fields = []
                vals = []
                if row.get("target_countries"):
                    fields.append("target_countries=?"); vals.append(row["target_countries"].strip())
                if row.get("target_age_min"):
                    fields.append("target_age_min=?"); vals.append(int(row["target_age_min"]))
                if row.get("target_age_max"):
                    fields.append("target_age_max=?"); vals.append(int(row["target_age_max"]))
                if row.get("target_gender") not in (None, ""):
                    fields.append("target_gender=?"); vals.append(int(row.get("target_gender", 0)))
                if row.get("target_placements"):
                    fields.append("target_placements=?"); vals.append(row["target_placements"].strip())
                if row.get("target_objective_type"):
                    _obj_alias = {"sales":"OUTCOME_SALES","website":"OUTCOME_TRAFFIC","leads":"OUTCOME_LEADS","engagement":"OUTCOME_ENGAGEMENT","messages":"OUTCOME_MESSAGES"}
                    _obj_val = row["target_objective_type"].strip()
                    _obj_val = _obj_alias.get(_obj_val, _obj_val)
                    fields.append("target_objective_type=?"); vals.append(_obj_val)
                if row.get("landing_url"):
                    fields.append("landing_url=?"); vals.append(row["landing_url"].strip())
                if fields:
                    vals.append(acc[0])
                    conn.execute(f"UPDATE accounts SET {', '.join(fields)} WHERE id=?", vals)
                    updated += 1
            except Exception as e:
                errors.append(f"账户 {act_id} 更新失败: {str(e)}")
        conn.commit()
        return {"updated": updated, "errors": errors}
    finally:
        conn.close()


@router.post("/{act_id}/rescan")
async def rescan_account_assets(
    act_id: str,
    current_user=Depends(get_current_user)
):
    _require_operator_user(current_user)
    """重新扫描账户素材（将所有素材状态重置为待扫描）"""
    conn = get_conn()
    try:
        assert_row_access(conn, "accounts", act_id, current_user, id_column="act_id")
        try:
            result = conn.execute(
                "UPDATE assets SET scan_status='pending', last_scanned_at=NULL WHERE act_id=?",
                (act_id,)
            )
            conn.commit()
            reset_count = result.rowcount
        except Exception:
            # assets 表可能字段不同
            try:
                row = conn.execute("SELECT COUNT(*) FROM assets WHERE act_id=?", (act_id,)).fetchone()
                reset_count = row[0] if row else 0
            except Exception:
                reset_count = 0
        return {"act_id": act_id, "assets_reset": reset_count, "message": "重新扫描已触发"}
    finally:
        conn.close()

@router.patch("/{token_id}/value")
async def update_token_value(token_id: int, body: dict, current_user=Depends(get_current_user)):
    """Update the access token value and re-verify with Facebook"""
    _require_operator_user(current_user)
    import requests as req_lib
    conn = get_conn()
    assert_row_access(conn, "fb_tokens", token_id, current_user, allow_unassigned=False)
    new_token = (body.get("access_token") or "").strip()
    if not new_token or len(new_token) < 20:
        conn.close()
        raise HTTPException(status_code=400, detail="Token value is invalid")
    try:
        row = conn.execute(
            "SELECT id, token_alias FROM fb_tokens WHERE id=?", (token_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Token not found")
        # Verify with Facebook
        try:
            resp = req_lib.get(
                "https://graph.facebook.com/v19.0/me",
                params={"access_token": new_token, "fields": "id,name"},
                timeout=10
            )
            fb_data = resp.json()
            if "error" in fb_data:
                raise HTTPException(status_code=400, detail=f"FB verification failed: {fb_data['error'].get('message','unknown')}")
            fb_name = fb_data.get("name", "")
            fb_id = fb_data.get("id", "")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"FB API error: {str(e)}")
        # Encrypt and store
        enc = encrypt_token(new_token)
        conn.execute(
            "UPDATE fb_tokens SET access_token_enc=?, status='active', last_verified_at=datetime('now','+8 hours') WHERE id=?",
            (enc, token_id)
        )
        claim_row_for_team(conn, "fb_tokens", "id", token_id, current_user)
        conn.commit()
        return {
            "success": True,
            "token_id": token_id,
            "fb_id": fb_id,
            "fb_name": fb_name,
            "message": f"Token updated and verified: {fb_name} ({fb_id})"
        }
    finally:
        conn.close()


# ── AI 决策日志 API ──────────────────────────────────────────────────────────
@router.get("/ai-decisions")
def get_ai_decisions(act_id: str = None, limit: int = 100, current_user=Depends(get_current_user)):
    conn = get_conn()
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_decisions'"
    ).fetchone()
    if not exists:
        conn.close()
        return []
    try:
        if act_id:
            assert_row_access(conn, "accounts", act_id, current_user, id_column="act_id")
            rows = conn.execute(
                "SELECT d.*, a.name as account_name FROM ai_decisions d "
                "LEFT JOIN accounts a ON d.act_id = a.act_id "
                "WHERE d.act_id=? ORDER BY d.created_at DESC LIMIT ?",
                (act_id, limit)
            ).fetchall()
        else:
            where, params = [], []
            apply_team_scope(where, params, current_user, "a.team_id", include_unassigned=False)
            where_clause = ("WHERE " + " AND ".join(where)) if where else ""
            rows = conn.execute(
                "SELECT d.*, a.name as account_name FROM ai_decisions d "
                "LEFT JOIN accounts a ON d.act_id = a.act_id "
                f"{where_clause} ORDER BY d.created_at DESC LIMIT ?",
                params + [limit]
            ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ── 矩阵列表 API ──────────────────────────────────────────────────────────────
@router.get("/matrices")
def get_matrices(current_user=Depends(get_current_user)):
    """获取所有已使用的矩阵ID列表（动态读取，不限数量）"""
    conn = get_conn()
    has_tw_pages = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tw_certified_pages'"
    ).fetchone()
    has_assets = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ad_assets'"
    ).fetchone()
    matrix_ids = set()
    def add_matrix_id(value):
        try:
            mid = int(value)
        except (TypeError, ValueError):
            return
        if mid > 0:
            matrix_ids.add(mid)
    token_where, token_params = ["matrix_id IS NOT NULL"], []
    apply_team_scope(token_where, token_params, current_user, "team_id", include_unassigned=False)
    token_rows = conn.execute(
        "SELECT DISTINCT matrix_id FROM fb_tokens WHERE " + " AND ".join(token_where),
        token_params,
    ).fetchall()
    for r in token_rows:
        add_matrix_id(r["matrix_id"])
    if has_tw_pages:
        page_where, page_params = ["matrix_id IS NOT NULL"], []
        apply_team_scope(page_where, page_params, current_user, "team_id", include_unassigned=False)
        page_rows = conn.execute(
            "SELECT DISTINCT matrix_id FROM tw_certified_pages WHERE " + " AND ".join(page_where),
            page_params,
        ).fetchall()
        for r in page_rows:
            add_matrix_id(r["matrix_id"])
    if has_assets:
        try:
            asset_cols = {row["name"] for row in conn.execute("PRAGMA table_info(ad_assets)").fetchall()}
            if "matrix_id" in asset_cols:
                asset_where, asset_params = ["matrix_id IS NOT NULL"], []
                apply_team_scope(asset_where, asset_params, current_user, "team_id", include_unassigned=False)
                asset_rows = conn.execute(
                    "SELECT DISTINCT matrix_id FROM ad_assets WHERE " + " AND ".join(asset_where),
                    asset_params,
                ).fetchall()
                for r in asset_rows:
                    add_matrix_id(r["matrix_id"])
        except Exception:
            logging.exception("Failed to load asset matrix ids")
    conn.close()
    return {"matrices": sorted(matrix_ids)}


@router.get("/matrix-diagnostic")
def matrix_diagnostic(current_user=Depends(get_current_user)):
    """诊断当前可见账户的矩阵归属覆盖情况。"""
    conn = get_conn()
    ensure_token_source_columns(conn)
    account_where, account_params = ["1=1"], []
    apply_team_scope(account_where, account_params, current_user, "a.team_id", include_unassigned=False)
    _apply_account_owner_scope(account_where, account_params, current_user, "a")
    account_scope_sql = " AND ".join(account_where)

    accounts = [
        dict(r)
        for r in conn.execute(
            f"""
            SELECT a.id, a.act_id, a.name, a.token_id, a.team_id, a.owner_user_id,
                   COALESCE(NULLIF(u.display_name,''), u.username) AS owner_name,
                   pt.token_alias AS primary_token_alias,
                   pt.status AS primary_token_status,
                   pt.token_type AS primary_token_type,
                   pt.token_source AS primary_token_source,
                   pt.matrix_id AS primary_matrix_id
              FROM accounts a
              LEFT JOIN users u ON u.id=a.owner_user_id
              LEFT JOIN fb_tokens pt ON pt.id=a.token_id
             WHERE {account_scope_sql}
             ORDER BY a.id DESC
            """,
            account_params,
        ).fetchall()
    ]
    act_ids = [str(a.get("act_id") or "").strip() for a in accounts if str(a.get("act_id") or "").strip()]
    linked_by_act: dict[str, list[dict]] = {act: [] for act in act_ids}
    if act_ids:
        placeholders = ",".join("?" for _ in act_ids)
        for row in conn.execute(
            f"""
            SELECT aot.act_id, aot.status AS bind_status, aot.priority,
                   t.id AS token_id, t.token_alias, t.token_type, t.token_source,
                   t.status AS token_status, t.matrix_id
              FROM account_op_tokens aot
              JOIN fb_tokens t ON t.id=aot.token_id
             WHERE aot.act_id IN ({placeholders})
             ORDER BY aot.act_id ASC, aot.priority DESC, t.id ASC
            """,
            act_ids,
        ).fetchall():
            item = dict(row)
            linked_by_act.setdefault(item["act_id"], []).append(item)

    token_where, token_params = ["1=1"], []
    apply_team_scope(token_where, token_params, current_user, "t.team_id", include_unassigned=False)
    from core.tenancy import apply_account_owner_scope as _apply_token_owner
    _apply_token_owner(token_where, token_params, current_user, "t.owner_user_id")
    token_scope_sql = " AND ".join(token_where)
    matrix_tokens = []
    for row in conn.execute(
        f"""
        SELECT t.id, t.token_alias, t.token_type, t.token_source, t.status,
               t.matrix_id, t.team_id, t.owner_user_id,
               COALESCE(NULLIF(u.display_name,''), u.username) AS owner_name
          FROM fb_tokens t
          LEFT JOIN users u ON u.id=t.owner_user_id
         WHERE {token_scope_sql}
           AND t.matrix_id IS NOT NULL
         ORDER BY t.matrix_id ASC, t.status ASC, t.id DESC
        """,
        token_params,
    ).fetchall():
        matrix_tokens.append(dict(row))

    unassigned = []
    assigned = []
    no_operate = 0
    operate_without_matrix = 0
    for acc in accounts:
        matrix_ids = set()
        if acc.get("primary_matrix_id") not in (None, "", 0):
            try:
                matrix_ids.add(int(acc["primary_matrix_id"]))
            except (TypeError, ValueError):
                pass
        linked = linked_by_act.get(acc.get("act_id") or "", [])
        active_operate = []
        for token in linked:
            if token.get("bind_status") == "active" and token.get("token_type") == "operate":
                active_operate.append(token)
            if token.get("bind_status") == "active" and token.get("matrix_id") not in (None, "", 0):
                try:
                    matrix_ids.add(int(token["matrix_id"]))
                except (TypeError, ValueError):
                    pass
        if not active_operate:
            no_operate += 1
        elif not any(t.get("matrix_id") not in (None, "", 0) for t in active_operate):
            operate_without_matrix += 1
        public = {
            "id": acc.get("id"),
            "act_id": acc.get("act_id"),
            "name": acc.get("name"),
            "owner_name": acc.get("owner_name"),
            "team_id": acc.get("team_id"),
            "primary_token_alias": acc.get("primary_token_alias"),
            "primary_token_status": acc.get("primary_token_status"),
            "linked_matrix_ids": sorted(matrix_ids),
            "active_operate_count": len(active_operate),
            "operate_token_count": len([t for t in linked if t.get("token_type") == "operate"]),
            "linked_token_count": len(linked),
        }
        if matrix_ids:
            assigned.append(public)
        else:
            reason = "没有绑定带矩阵的有效操作号"
            if not linked and not acc.get("primary_token_alias"):
                reason = "没有主 Token 或操作号绑定"
            elif not active_operate:
                reason = "没有 active 操作号绑定"
            elif active_operate:
                reason = "操作号已绑定，但没有设置矩阵"
            public["reason"] = reason
            unassigned.append(public)

    active_matrix_operate = [
        t for t in matrix_tokens
        if t.get("status") == "active" and t.get("token_type") == "operate"
    ]
    inactive_matrix_tokens = [t for t in matrix_tokens if t.get("status") != "active"]
    certified_pages_with_matrix = 0
    certified_pages_without_matrix = 0
    try:
        page_counts = conn.execute(
            """
            SELECT
              SUM(CASE WHEN matrix_id IS NOT NULL THEN 1 ELSE 0 END) AS with_matrix,
              SUM(CASE WHEN matrix_id IS NULL THEN 1 ELSE 0 END) AS without_matrix
            FROM tw_certified_pages
            """
        ).fetchone()
        certified_pages_with_matrix = int(page_counts["with_matrix"] or 0) if page_counts else 0
        certified_pages_without_matrix = int(page_counts["without_matrix"] or 0) if page_counts else 0
    except Exception:
        certified_pages_with_matrix = 0
        certified_pages_without_matrix = 0
    suggestions = []
    if unassigned and not active_matrix_operate:
        suggestions.append("当前没有 active 的带矩阵操作号。请先在 Token 页把 System User / Meta 官方授权操作号分配到矩阵，或重新走 Meta 官方授权并选择矩阵。")
    if certified_pages_with_matrix and unassigned:
        suggestions.append("主页库已有矩阵记录，但账户矩阵仍由账户主 Token 或已绑定操作号推导；只给主页设置矩阵不会让账户自动归属矩阵。")
    if operate_without_matrix:
        suggestions.append("有账户已经绑定操作号，但操作号未设置矩阵。去 Token 页点击“矩阵”即可补齐，之后账户、素材、主页库会自动显示矩阵标签。")
    if no_operate:
        suggestions.append("有账户没有 active 操作号绑定。可在账户页点击“智能导入并关联”，或在 Token 页重新匹配 Token。")
    if not suggestions:
        suggestions.append("矩阵归属覆盖正常；若某个页面仍显示未识别，请刷新页面或检查该资源是否绑定到账户。")

    conn.close()
    return {
        "success": True,
        "summary": {
            "accounts_total": len(accounts),
            "accounts_with_matrix": len(assigned),
            "accounts_without_matrix": len(unassigned),
            "active_matrix_operate_tokens": len(active_matrix_operate),
            "inactive_matrix_tokens": len(inactive_matrix_tokens),
            "accounts_without_active_operate": no_operate,
            "accounts_with_operate_without_matrix": operate_without_matrix,
            "certified_pages_with_matrix": certified_pages_with_matrix,
            "certified_pages_without_matrix": certified_pages_without_matrix,
        },
        "matrix_tokens": matrix_tokens[:60],
        "unassigned_accounts": unassigned[:80],
        "suggestions": suggestions,
    }


# ── 消费上限设置 ──────────────────────────────────────────────────────────────

class SetSpendCapBody(BaseModel):
    spend_cap_usd: Optional[float] = None  # USD金额；必须 > 0，移除上限请在 Meta UI 人工操作


@router.post("/{act_id}/set-spend-cap")
def set_spend_cap(act_id: str, body: SetSpendCapBody, current_user=Depends(get_current_user)):
    _require_operator_user(current_user)
    """设置账户消费上限 - API 只负责设置明确限额；移除上限请在 Meta UI 人工操作"""
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, current_user, id_column="act_id")
    acc = conn.execute(
        "SELECT act_id, name, currency FROM accounts WHERE act_id=?",
        (act_id,)
    ).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, f"账户 {act_id} 不存在")

    currency = (acc["currency"] or "USD").upper()
    cap_usd = body.spend_cap_usd
    if cap_usd is None or cap_usd <= 0:
        conn.close()
        raise HTTPException(
            400,
            "Mira API 只支持设置大于 0 的消费上限；移除上限请到 Meta 后台 Billing/Payment settings 人工移除，完成后回到 Mira 点击“同步状态”。"
        )

    # 获取操作号 Token（需要写权限）
    from services.token_manager import get_exec_token, ACTION_UPDATE
    token = get_exec_token(act_id, ACTION_UPDATE)
    if not token:
        conn.close()
        raise HTTPException(400, f"账户 {act_id} 没有可用的操作号Token，无法设置消费上限")

    # 静态兜底汇率表（1 单位本币 = X USD）；数据库 currency_rates 为 1 USD = X 本币
    _DEFAULT_RATES = {
        "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
        "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74,
        "AUD": 0.65, "CAD": 0.74, "BRL": 0.20, "MXN": 0.058,
        "CLP": 0.0011, "COP": 0.00025, "PEN": 0.27, "ARS": 0.001,
        "THB": 0.028, "VND": 0.000040, "IDR": 0.000063, "PHP": 0.017,
        "MYR": 0.21, "INR": 0.012, "TRY": 0.031, "ZAR": 0.053,
        "BDT": 0.0091, "PKR": 0.0036, "LKR": 0.0031, "NPR": 0.0075,
        "KRW": 0.00072, "CHF": 1.12, "NZD": 0.60, "SEK": 0.096,
        "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "CZK": 0.044,
        "HUF": 0.0028, "RON": 0.22, "BGN": 0.55, "AED": 0.272,
        "SAR": 0.267, "QAR": 0.275, "KWD": 3.26, "BHD": 2.65,
        "OMR": 2.60, "JOD": 1.41, "EGP": 0.021, "MAD": 0.099,
        "TND": 0.32, "GHS": 0.067, "NGN": 0.00065, "KES": 0.0077,
        "UAH": 0.027, "KZT": 0.0022, "GEL": 0.37,
    }

    local_amount = round(cap_usd * _local_per_usd_rate(currency), 2)

    fb_value = _to_minor_units(local_amount, currency)
    db_value = fb_value

    fb_error = None
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v25.0/{act_id}",
            data={"spend_cap": fb_value, "access_token": token},
            timeout=30
        )
        result = resp.json()
        if not resp.ok:
            err_info = result.get("error", {})
            error_msg = err_info.get("message", str(result))
            fb_error = error_msg
    except requests.RequestException as e:
        fb_error = str(e)

    if fb_error:
        conn.close()
        raise HTTPException(400, f"FB API 设置失败: {fb_error}")

    # 更新本地数据库
    conn.execute(
        "UPDATE accounts SET spend_cap=?, spending_limit=?, updated_at=datetime('now') WHERE act_id=?",
        (db_value, db_value, act_id)
    )
    conn.commit()
    conn.close()

    return {
        "ok": True,
        "act_id": act_id,
        "spend_cap": fb_value,
        "spend_cap_usd": cap_usd,
        "message": f"消费上限已设置为 ${cap_usd:,.2f} USD"
    }
