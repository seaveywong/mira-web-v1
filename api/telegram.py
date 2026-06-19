from __future__ import annotations

import hashlib
import hmac
import html
import os
import re
from datetime import date, datetime, timedelta
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

from core.auth import require_superadmin
from core.database import get_conn
from services.notifier import ensure_notification_schema, split_chat_ids


router = APIRouter()


class TelegramWebhookSetupIn(BaseModel):
    public_base_url: str | None = None


def _columns(conn, table: str) -> set[str]:
    try:
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _setting(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row and row["value"] is not None else default


def _webhook_secret(token: str) -> str:
    salt = os.environ.get("MIRA_TG_WEBHOOK_SALT") or os.environ.get("SECRET_KEY") or "mira-tg"
    return hashlib.sha256(f"{token}:{salt}".encode("utf-8")).hexdigest()[:32]


def _normalize_base_url(raw: str | None) -> str:
    value = (raw or "").strip().rstrip("/")
    if not value:
        value = os.environ.get("MIRA_PUBLIC_BASE_URL", "https://shouhu.asia").strip().rstrip("/")
    if value.startswith("http://"):
        value = "https://" + value[7:]
    if not value.startswith("https://"):
        value = "https://" + value
    return value.rstrip("/")


def telegram_webhook_url(conn, public_base_url: str | None = None) -> str:
    token = _setting(conn, "tg_bot_token", "")
    if not token:
        raise HTTPException(status_code=400, detail="Telegram Bot Token is not configured")
    return f"{_normalize_base_url(public_base_url)}/api/telegram/webhook/{_webhook_secret(token)}"


def _answer_callback(token: str, callback_id: str | None, text: str, alert: bool = False) -> None:
    if not token or not callback_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text[:180], "show_alert": alert},
            timeout=8,
        )
    except Exception:
        pass


def _send_tg_message(token: str, chat_id: str | None, text: str) -> None:
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


def _edit_reply_markup(token: str, chat_id: str | None, message_id: int | None) -> None:
    if not token or not chat_id or not message_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
            json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}},
            timeout=8,
        )
    except Exception:
        pass


def _allowed_chat_ids(conn) -> set[str]:
    ensure_notification_schema(conn)
    allowed: set[str] = set(split_chat_ids(_setting(conn, "tg_chat_ids", "")))
    for row in conn.execute("SELECT tg_chat_ids FROM teams WHERE COALESCE(notify_enabled, 1)=1").fetchall():
        allowed.update(split_chat_ids(row["tg_chat_ids"]))
    for row in conn.execute("SELECT tg_chat_id FROM users WHERE COALESCE(notify_enabled, 0)=1").fetchall():
        allowed.update(split_chat_ids(row["tg_chat_id"]))
    return {str(x).strip() for x in allowed if str(x).strip()}


def _norm_act_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw if raw.lower().startswith("act_") else f"act_{raw}"


def _plain_id(value: str | None) -> str:
    raw = str(value or "").strip()
    return raw[4:] if raw.lower().startswith("act_") else raw


def _normalize_ad_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = re.findall(r"\d{6,}", raw)
    return max(parts, key=len) if parts else raw


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


def _account_row(conn, act_id: str):
    cols = _columns(conn, "accounts")
    select_cols = ["act_id", "name"]
    for col in ("team_id", "owner_user_id", "timezone_name", "timezone", "timezone_offset_hours_utc"):
        if col in cols:
            select_cols.append(col)
    act_norm = _norm_act_id(act_id)
    act_plain = _plain_id(act_norm)
    return conn.execute(
        f"SELECT {', '.join(select_cols)} FROM accounts WHERE act_id IN (?, ?) LIMIT 1",
        (act_norm, act_plain),
    ).fetchone()


def _account_local_date(account) -> str:
    if not account:
        return date.today().isoformat()
    keys = account.keys() if hasattr(account, "keys") else []
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


def _save_guard_allowance(conn, account, ad_id: str, created_by: str, reason: str) -> dict[str, Any]:
    _ensure_guard_allowance_schema(conn)
    act_id = _norm_act_id(account["act_id"])
    act_plain = _plain_id(act_id)
    ad_plain = _normalize_ad_id(ad_id)
    allowance_date = _account_local_date(account)
    team_id = account["team_id"] if "team_id" in account.keys() else None
    owner_user_id = account["owner_user_id"] if "owner_user_id" in account.keys() else None
    old = conn.execute(
        """SELECT id FROM guard_ad_allowances
           WHERE ad_id=? AND allowance_date=? AND (act_id=? OR act_id=?)
           ORDER BY id DESC LIMIT 1""",
        (ad_plain, allowance_date, act_id, act_plain),
    ).fetchone()
    if old:
        conn.execute(
            """UPDATE guard_ad_allowances
               SET act_id=?, status='active', reason=?, team_id=?, owner_user_id=?,
                   created_by=?, updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (act_id, reason, team_id, owner_user_id, created_by, old["id"]),
        )
        allowance_id = old["id"]
        action = "updated"
    else:
        cur = conn.execute(
            """INSERT INTO guard_ad_allowances
               (act_id, ad_id, allowance_date, reason, status, team_id, owner_user_id, created_by)
               VALUES (?,?,?,?,?,?,?,?)""",
            (act_id, ad_plain, allowance_date, reason, "active", team_id, owner_user_id, created_by),
        )
        allowance_id = cur.lastrowid
        action = "created"
    conn.commit()
    return {
        "id": allowance_id,
        "act_id": act_id,
        "ad_id": ad_plain,
        "allowance_date": allowance_date,
        "action": action,
    }


def _callback_actor(callback: dict[str, Any]) -> str:
    user = callback.get("from") or {}
    return str(user.get("username") or user.get("id") or "telegram")


@router.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    conn = get_conn()
    try:
        token = _setting(conn, "tg_bot_token", "")
        if not token or not hmac.compare_digest(secret, _webhook_secret(token)):
            raise HTTPException(status_code=404, detail="Not found")
        update = await request.json()
        callback = update.get("callback_query") or {}
        if not callback:
            return {"ok": True}

        callback_id = callback.get("id")
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id") or "")
        from_id = str((callback.get("from") or {}).get("id") or "")
        if chat_id not in _allowed_chat_ids(conn) and from_id not in _allowed_chat_ids(conn):
            _answer_callback(token, callback_id, "这个 Telegram 未配置到 Mira 通知，不能加白。", alert=True)
            return {"ok": True, "ignored": "unauthorized_chat"}

        data = str(callback.get("data") or "")
        parts = data.split("|", 3)
        if len(parts) != 4 or parts[0] != "ga" or parts[1] != "d":
            _answer_callback(token, callback_id, "未知操作。", alert=True)
            return {"ok": True, "ignored": "unknown_callback"}

        act_id = _norm_act_id(parts[2])
        ad_id = _normalize_ad_id(parts[3])
        account = _account_row(conn, act_id)
        if not account:
            _answer_callback(token, callback_id, "账户不在 Mira 资产库，无法加白。", alert=True)
            return {"ok": True, "ignored": "account_not_found"}
        if not ad_id:
            _answer_callback(token, callback_id, "广告 ID 为空，无法加白。", alert=True)
            return {"ok": True, "ignored": "empty_ad_id"}

        actor = _callback_actor(callback)
        result = _save_guard_allowance(
            conn,
            account,
            ad_id,
            created_by=f"tg:{actor}",
            reason=f"TG 快捷加白 by {actor}",
        )
        account_name = account["name"] if "name" in account.keys() and account["name"] else result["act_id"]
        _answer_callback(token, callback_id, f"已加白至账户日期 {result['allowance_date']}")
        _edit_reply_markup(token, chat_id, message.get("message_id"))
        _send_tg_message(
            token,
            chat_id,
            "✅ <b>Mira 已加白广告</b>\n"
            f"账户：{html.escape(str(account_name), quote=False)} ({html.escape(result['act_id'], quote=False)})\n"
            f"广告ID：<code>{html.escape(result['ad_id'], quote=False)}</code>\n"
            f"有效期：账户本地日期 {html.escape(result['allowance_date'], quote=False)}\n"
            "说明：只跳过当日规则止损，不会自动重新开启已经关闭的广告。",
        )
        return {"ok": True, "result": result}
    finally:
        conn.close()


@router.post("/webhook/setup")
def setup_telegram_webhook(body: TelegramWebhookSetupIn | None = None, user=Depends(require_superadmin)):
    conn = get_conn()
    try:
        token = _setting(conn, "tg_bot_token", "")
        if not token:
            raise HTTPException(status_code=400, detail="Telegram Bot Token is not configured")
        url = telegram_webhook_url(conn, (body.public_base_url if body else None))
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            json={"url": url, "allowed_updates": ["callback_query"]},
            timeout=15,
        )
        data = resp.json()
        return {"success": bool(data.get("ok")), "webhook_url": url, "telegram": data}
    finally:
        conn.close()


@router.get("/webhook/info")
def get_telegram_webhook_info(user=Depends(require_superadmin)):
    conn = get_conn()
    try:
        token = _setting(conn, "tg_bot_token", "")
        if not token:
            raise HTTPException(status_code=400, detail="Telegram Bot Token is not configured")
        resp = requests.get(f"https://api.telegram.org/bot{token}/getWebhookInfo", timeout=15)
        return {"success": True, "telegram": resp.json()}
    finally:
        conn.close()
