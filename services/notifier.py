"""Central Telegram notification routing for teams and account owners."""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from core.database import get_conn


logger = logging.getLogger("mira.notifier")

_SCHEMA_READY = False
_SEND_CACHE: dict[tuple[str, str, str], float] = {}
_DEDUP_SECONDS = 20


def _columns(conn, table: str) -> set[str]:
    try:
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def ensure_notification_schema(conn=None) -> None:
    """Create notification/team ownership columns lazily and idempotently."""
    global _SCHEMA_READY
    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True
    try:
        team_cols = _columns(conn, "teams")
        if team_cols:
            if "tg_chat_ids" not in team_cols:
                conn.execute("ALTER TABLE teams ADD COLUMN tg_chat_ids TEXT")
            if "notify_enabled" not in team_cols:
                conn.execute("ALTER TABLE teams ADD COLUMN notify_enabled INTEGER DEFAULT 1")

        user_cols = _columns(conn, "users")
        if user_cols:
            if "tg_chat_id" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN tg_chat_id TEXT")
            if "notify_enabled" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN notify_enabled INTEGER DEFAULT 0")
            if "notify_scope" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN notify_scope TEXT DEFAULT 'owned'")
            if "notify_types" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN notify_types TEXT DEFAULT 'all'")

        account_cols = _columns(conn, "accounts")
        if account_cols:
            if "owner_user_id" not in account_cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN owner_user_id INTEGER")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_accounts_owner_user ON accounts(owner_user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_accounts_team_owner ON accounts(team_id, owner_user_id)")

        conn.commit()
        _SCHEMA_READY = True
    finally:
        if close_conn:
            conn.close()


def _setting(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row and row["value"] is not None else default


def split_chat_ids(value: str | None) -> list[str]:
    out: list[str] = []
    seen = set()
    for raw in (value or "").split(","):
        chat_id = raw.strip()
        if not chat_id or chat_id in seen:
            continue
        seen.add(chat_id)
        out.append(chat_id)
    return out


def _chat_ids_from_row(row, key: str = "tg_chat_ids") -> list[str]:
    if not row:
        return []
    try:
        return split_chat_ids(row[key])
    except Exception:
        return []


def _event_allowed(notify_types: str | None, event_type: str) -> bool:
    raw = (notify_types or "all").strip().lower()
    if not raw or raw == "all":
        return True
    values = {x.strip() for x in raw.split(",") if x.strip()}
    return event_type in values


def _add_unique(target: list[str], seen: set[str], chat_ids: list[str]) -> None:
    for chat_id in chat_ids:
        if chat_id and chat_id not in seen:
            seen.add(chat_id)
            target.append(chat_id)


def _resolve_account(conn, act_id: str | None) -> dict[str, Any]:
    if not act_id:
        return {}
    ensure_notification_schema(conn)
    cols = _columns(conn, "accounts")
    select_owner = "owner_user_id" if "owner_user_id" in cols else "NULL AS owner_user_id"
    row = conn.execute(
        f"""SELECT id, act_id, name, team_id, {select_owner}
            FROM accounts WHERE act_id=? LIMIT 1""",
        (act_id,),
    ).fetchone()
    return dict(row) if row else {"act_id": act_id, "name": act_id}


def resolve_recipients(
    act_id: str | None = None,
    team_id: int | None = None,
    event_type: str = "guard",
    include_owner: bool = True,
    fallback_global: bool = True,
) -> dict[str, Any]:
    """Resolve chat IDs without leaking one team's account alerts to another team."""
    conn = get_conn()
    try:
        ensure_notification_schema(conn)
        recipients: list[str] = []
        seen: set[str] = set()
        account = _resolve_account(conn, act_id)
        resolved_team_id = team_id if team_id is not None else account.get("team_id")

        if resolved_team_id is not None:
            team = conn.execute(
                "SELECT id, name, tg_chat_ids, COALESCE(notify_enabled, 1) AS notify_enabled FROM teams WHERE id=?",
                (resolved_team_id,),
            ).fetchone()
            if team and int(team["notify_enabled"] if team["notify_enabled"] is not None else 1) == 1:
                _add_unique(recipients, seen, _chat_ids_from_row(team, "tg_chat_ids"))

        if include_owner and account.get("owner_user_id"):
            owner = conn.execute(
                """SELECT id, team_id, username, tg_chat_id, COALESCE(notify_enabled, 0) AS notify_enabled,
                          COALESCE(notify_scope, 'owned') AS notify_scope,
                          COALESCE(notify_types, 'all') AS notify_types,
                          COALESCE(is_active, 1) AS is_active
                   FROM users WHERE id=?""",
                (account["owner_user_id"],),
            ).fetchone()
            if owner and owner["team_id"] == resolved_team_id and owner["is_active"] and owner["notify_enabled"]:
                if _event_allowed(owner["notify_types"], event_type):
                    _add_unique(recipients, seen, split_chat_ids(owner["tg_chat_id"]))

        global_chat_ids = split_chat_ids(_setting(conn, "tg_chat_ids", ""))
        if fallback_global and not recipients:
            _add_unique(recipients, seen, global_chat_ids)

        return {
            "chat_ids": recipients,
            "account": account,
            "team_id": resolved_team_id,
            "global_chat_ids": global_chat_ids,
        }
    finally:
        conn.close()


def _send_to_chat_ids(
    chat_ids: list[str],
    msg: str,
    parse_mode: str = "HTML",
    dedup_key: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    conn = get_conn()
    try:
        token = _setting(conn, "tg_bot_token", "")
        enabled = _setting(conn, "tg_enabled", "0") == "1"
    finally:
        conn.close()

    if not enabled or not token or not chat_ids:
        return {"sent": 0, "skipped": True, "chat_ids": chat_ids}

    sent = 0
    errors = []
    now = time.time()
    for chat_id in chat_ids:
        cache_key = (chat_id, dedup_key or "", msg[:500])
        last = _SEND_CACHE.get(cache_key, 0.0)
        if dedup_key and now - last < _DEDUP_SECONDS:
            continue
        _SEND_CACHE[cache_key] = now
        try:
            payload = {"chat_id": chat_id, "text": msg, "parse_mode": parse_mode}
            if reply_markup:
                payload["reply_markup"] = reply_markup
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json=payload,
                timeout=10,
            )
            sent += 1
        except Exception as exc:
            errors.append({"chat_id": chat_id, "error": str(exc)})
            logger.warning("TG send failed chat_id=%s: %s", chat_id, exc)
    return {"sent": sent, "errors": errors, "chat_ids": chat_ids}


def notify_account(
    act_id: str,
    msg: str,
    event_type: str = "guard",
    parse_mode: str = "HTML",
    include_owner: bool = True,
    fallback_global: bool = True,
    dedup_key: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route = resolve_recipients(
        act_id=act_id,
        event_type=event_type,
        include_owner=include_owner,
        fallback_global=fallback_global,
    )
    result = _send_to_chat_ids(
        route["chat_ids"],
        msg,
        parse_mode=parse_mode,
        dedup_key=dedup_key or f"{event_type}:{act_id}",
        reply_markup=reply_markup,
    )
    result["route"] = {"team_id": route.get("team_id"), "act_id": act_id}
    return result


def notify_team(
    team_id: int | None,
    msg: str,
    event_type: str = "team",
    parse_mode: str = "HTML",
    fallback_global: bool = True,
    dedup_key: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route = resolve_recipients(
        team_id=team_id,
        event_type=event_type,
        include_owner=False,
        fallback_global=fallback_global,
    )
    result = _send_to_chat_ids(
        route["chat_ids"],
        msg,
        parse_mode=parse_mode,
        dedup_key=dedup_key or f"{event_type}:team:{team_id}",
        reply_markup=reply_markup,
    )
    result["route"] = {"team_id": team_id}
    return result


def notify_global(
    msg: str,
    parse_mode: str = "HTML",
    dedup_key: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    conn = get_conn()
    try:
        chat_ids = split_chat_ids(_setting(conn, "tg_chat_ids", ""))
    finally:
        conn.close()
    return _send_to_chat_ids(
        chat_ids,
        msg,
        parse_mode=parse_mode,
        dedup_key=dedup_key or "global",
        reply_markup=reply_markup,
    )
