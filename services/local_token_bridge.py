from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from core.auth import ROLE_LEVELS, is_superadmin, normalize_user_claims
from core.database import get_conn
from core.tenancy import is_operator_user, user_id


FB_API_BASE = "https://graph.facebook.com/v25.0"
REGISTRATION_TTL_SECONDS = 10 * 60
NODE_STALE_SECONDS = 90
TOKEN_PROBE_TTL_SECONDS = 5 * 60
MAX_ACCOUNT_PROBE = 250
LOCAL_TOKEN_REQUEST_GAP_SECONDS = 3.0

_lock = threading.RLock()
_registration_codes: dict[str, dict] = {}
_nodes: dict[str, dict] = {}
_nodes_loaded = False
_NODE_STORE_PATH = os.environ.get("MIRA_LOCAL_TOKEN_NODES_FILE", "/opt/mira/data/local_token_nodes.json")


def _persistable_node(node: dict) -> dict:
    safe = dict(node or {})
    safe["token_plain"] = ""
    safe["token_fp"] = ""
    return safe


def _load_persisted_nodes_locked() -> None:
    global _nodes_loaded
    if _nodes_loaded:
        return
    _nodes_loaded = True
    try:
        if not os.path.exists(_NODE_STORE_PATH):
            return
        with open(_NODE_STORE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        raw_nodes = data.get("nodes") if isinstance(data, dict) else data
        if not isinstance(raw_nodes, dict):
            return
        for node_id, node in raw_nodes.items():
            if not isinstance(node, dict):
                continue
            restored = dict(node)
            restored["token_plain"] = ""
            restored["token_fp"] = ""
            _nodes[str(node_id)] = restored
    except Exception:
        return


def _save_persisted_nodes_locked() -> None:
    try:
        os.makedirs(os.path.dirname(_NODE_STORE_PATH), exist_ok=True)
        tmp_path = _NODE_STORE_PATH + ".tmp"
        data = {"nodes": {node_id: _persistable_node(node) for node_id, node in _nodes.items()}}
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp_path, _NODE_STORE_PATH)
    except Exception:
        return


def _now_ts() -> float:
    return time.time()


def _now_cst() -> str:
    return datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _parse_time_to_ts(value: str) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
        return parsed.timestamp()
    except Exception:
        pass
    try:
        parsed = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
        return parsed.timestamp()
    except Exception:
        return 0.0


def _clean_expired() -> None:
    now = _now_ts()
    for code, meta in list(_registration_codes.items()):
        if float(meta.get("expires_at_ts") or 0) <= now:
            _registration_codes.pop(code, None)


def _token_fp(token: str) -> str:
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _mask_token(token: str) -> str:
    token = str(token or "").strip()
    if len(token) <= 12:
        return "****"
    return token[:6] + "****" + token[-4:]


def _graph_get(path: str, token: str, params: Optional[dict] = None, timeout: int = 10) -> dict:
    params = dict(params or {})
    params["access_token"] = token
    resp = requests.get(f"{FB_API_BASE}/{path.lstrip('/')}", params=params, timeout=timeout)
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Meta API returned non-json response")
    err = data.get("error")
    if err:
        msg = err.get("message") or str(err)
        code = err.get("code")
        subcode = err.get("error_subcode") or err.get("subcode")
        detail = msg
        if code:
            detail += f" | code={code}"
        if subcode:
            detail += f" | subcode={subcode}"
        raise RuntimeError(detail)
    return data


def _normalize_act_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("act_"):
        return raw
    return f"act_{raw}"


def _visible_accounts_for_node(node: dict) -> list[dict]:
    conn = get_conn()
    try:
        where = ["1=1"]
        params: list = []
        role = str(node.get("role") or "").strip()
        team_id = node.get("team_id")
        if role != "superadmin":
            where.append("team_id=?")
            params.append(team_id)
            if role == "operator":
                where.append("owner_user_id=?")
                params.append(node.get("user_id"))
        rows = conn.execute(
            f"""
            SELECT act_id, name, team_id, owner_user_id
            FROM accounts
            WHERE {' AND '.join(where)}
            ORDER BY id ASC
            LIMIT ?
            """,
            params + [MAX_ACCOUNT_PROBE],
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _account_allowed_for_node(act_id: str, node: dict) -> bool:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT act_id, team_id, owner_user_id FROM accounts WHERE act_id=? LIMIT 1",
            (_normalize_act_id(act_id),),
        ).fetchone()
        if not row:
            return False
        role = str(node.get("role") or "").strip()
        if role == "superadmin":
            return True
        if row["team_id"] != node.get("team_id"):
            return False
        if role == "operator" and row["owner_user_id"] != node.get("user_id"):
            return False
        return True
    finally:
        conn.close()


def _account_summaries_for_ids(account_ids: list[str]) -> list[dict]:
    normalized = []
    seen = set()
    for item in account_ids or []:
        act_id = _normalize_act_id(item)
        if act_id and act_id not in seen:
            seen.add(act_id)
            normalized.append(act_id)
    if not normalized:
        return []
    conn = get_conn()
    try:
        placeholders = ",".join("?" for _ in normalized)
        rows = conn.execute(
            f"""
            SELECT act_id, name, team_id, owner_user_id
            FROM accounts
            WHERE act_id IN ({placeholders})
            """,
            normalized,
        ).fetchall()
        by_id = {row["act_id"]: dict(row) for row in rows}
        return [
            {
                "act_id": act_id,
                "name": (by_id.get(act_id) or {}).get("name") or act_id,
                "team_id": (by_id.get(act_id) or {}).get("team_id"),
                "owner_user_id": (by_id.get(act_id) or {}).get("owner_user_id"),
            }
            for act_id in normalized
        ]
    finally:
        conn.close()


def _fetch_token_permissions(token: str) -> dict:
    try:
        data = _graph_get("/me/permissions", token, timeout=10)
        rows = data.get("data") or []
        granted = sorted(
            {
                str(item.get("permission") or "").strip()
                for item in rows
                if str(item.get("status") or "").lower() == "granted"
            }
        )
        declined = sorted(
            {
                str(item.get("permission") or "").strip()
                for item in rows
                if str(item.get("status") or "").lower() != "granted"
            }
        )
        return {"granted": [p for p in granted if p], "declined": [p for p in declined if p]}
    except Exception as exc:
        return {"granted": [], "declined": [], "error": str(exc)}


def _fetch_adaccounts_from_me(token: str) -> set[str]:
    out: set[str] = set()
    url = f"{FB_API_BASE}/me/adaccounts"
    params = {"fields": "id,name,account_status", "limit": 200, "access_token": token}
    seen_next: set[str] = set()
    for _ in range(20):
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        if not isinstance(data, dict):
            break
        if data.get("error"):
            raise RuntimeError((data.get("error") or {}).get("message") or str(data.get("error")))
        for item in data.get("data") or []:
            act_id = _normalize_act_id(item.get("id"))
            if act_id:
                out.add(act_id)
        next_url = (data.get("paging") or {}).get("next")
        if not next_url or next_url in seen_next:
            break
        seen_next.add(next_url)
        url = next_url
        params = {}
    return out


def _probe_token_for_node(node: dict, token: str) -> dict:
    me = _graph_get("/me", token, params={"fields": "id,name"}, timeout=10)
    permissions = _fetch_token_permissions(token)
    granted = set(permissions.get("granted") or [])
    has_ads_management = "ads_management" in granted
    has_ads_read = "ads_read" in granted or has_ads_management
    listed_accounts: set[str] = set()
    list_error = ""
    try:
        listed_accounts = _fetch_adaccounts_from_me(token)
    except Exception as exc:
        list_error = str(exc)

    visible = _visible_accounts_for_node(node)
    visible_ids = {_normalize_act_id(r["act_id"]) for r in visible if r.get("act_id")}
    matched = sorted(visible_ids.intersection(listed_accounts))

    # Some Meta tokens can read an account directly even when /me/adaccounts is incomplete.
    if not matched and visible:
        direct_matched = []
        for row in visible[:MAX_ACCOUNT_PROBE]:
            act_id = _normalize_act_id(row.get("act_id"))
            if not act_id:
                continue
            try:
                _graph_get(f"/{act_id}", token, params={"fields": "id,name,account_status"}, timeout=8)
                direct_matched.append(act_id)
            except Exception:
                continue
        matched = sorted(set(direct_matched))

    status = "online"
    last_error = ""
    if not has_ads_management:
        status = "permission_limited"
        last_error = "缺少 ads_management，不能作为铺广告优先 Token"
    elif not matched:
        status = "no_accounts"
        last_error = list_error or "当前 Token 没有匹配到此用户可见账户"

    return {
        "status": status,
        "fb_user_id": str(me.get("id") or ""),
        "fb_user_name": str(me.get("name") or ""),
        "permissions": permissions,
        "has_ads_management": has_ads_management,
        "has_ads_read": has_ads_read,
        "account_ids": matched,
        "listed_account_count": len(listed_accounts),
        "last_error": last_error,
        "verified_at_ts": _now_ts(),
        "verified_at": _now_cst(),
    }


def create_registration(user: dict, node_name: str = "") -> dict:
    user = normalize_user_claims(user)
    if ROLE_LEVELS.get(user.get("role", "viewer"), 0) < ROLE_LEVELS["operator"]:
        raise PermissionError("Operator permission required")
    with _lock:
        _load_persisted_nodes_locked()
        _clean_expired()
        code = "MLT-" + secrets.token_urlsafe(10).replace("-", "").replace("_", "")[:12].upper()
        expires_at_ts = _now_ts() + REGISTRATION_TTL_SECONDS
        _registration_codes[code] = {
            "user_id": int(user.get("uid") or 0),
            "username": user.get("username") or "",
            "role": user.get("role") or "viewer",
            "team_id": user.get("team_id"),
            "team_name": user.get("team_name"),
            "node_name": (node_name or "").strip(),
            "created_at_ts": _now_ts(),
            "expires_at_ts": expires_at_ts,
        }
        return {
            "code": code,
            "expires_at": _iso_from_ts(expires_at_ts),
            "ttl_seconds": REGISTRATION_TTL_SECONDS,
        }


def register_node(
    code: str,
    node_name: str,
    browser: str = "",
    user_agent: str = "",
    install_id: str = "",
    extension_version: str = "",
    capabilities: Optional[list] = None,
) -> dict:
    code = str(code or "").strip().upper()
    with _lock:
        _load_persisted_nodes_locked()
        _clean_expired()
        meta = _registration_codes.pop(code, None)
        if not meta:
            raise ValueError("绑定码无效或已过期，请在 Mira 重新生成")
        node_id = uuid.uuid4().hex
        node_secret = secrets.token_urlsafe(32)
        name = (node_name or "").strip() or meta.get("node_name") or f"Chrome-{node_id[:6]}"
        now = _now_ts()
        _nodes[node_id] = {
            "node_id": node_id,
            "node_secret": node_secret,
            "node_name": name,
            "browser": browser or "Chrome",
            "user_agent": user_agent or "",
            "install_id": str(install_id or "").strip(),
            "extension_version": str(extension_version or "").strip(),
            "capabilities": list(capabilities or []),
            "user_id": meta.get("user_id"),
            "username": meta.get("username"),
            "role": meta.get("role"),
            "team_id": meta.get("team_id"),
            "team_name": meta.get("team_name"),
            "created_at_ts": now,
            "created_at": _now_cst(),
            "last_seen_ts": now,
            "last_seen_at": _now_cst(),
            "status": "registered",
            "last_error": "",
            "token_plain": "",
            "token_fp": "",
            "token_mask": "",
            "token_expires_at_ts": 0,
            "token_expires_at": "",
            "permissions": {"granted": [], "declined": []},
            "account_ids": [],
            "fb_user_id": "",
            "fb_user_name": "",
            "has_ads_management": False,
            "has_ads_read": False,
            "verified_at_ts": 0,
            "verified_at": "",
            "last_selected_at_ts": 0,
            "next_request_at_ts": 0,
            "cooldown_until_ts": 0,
            "cooldown_reason": "",
            "last_error_code": None,
            "reported_accounts": [],
            "queue": {"running": 0, "waiting": 0},
        }
        _save_persisted_nodes_locked()
        return {
            "node_id": node_id,
            "node_secret": node_secret,
            "node_name": name,
            "heartbeat_interval_seconds": 30,
        }


def heartbeat_node(
    node_id: str,
    node_secret: str,
    access_token: str = "",
    expires_at: str = "",
    expires_in_minutes: Optional[int] = None,
    token_summary: Optional[dict] = None,
    node_name: str = "",
    browser: str = "",
    user_agent: str = "",
    install_id: str = "",
    extension_version: str = "",
    capabilities: Optional[list] = None,
    runtime_status: str = "",
    reported_accounts: Optional[list] = None,
    queue: Optional[dict] = None,
) -> dict:
    # Local executors must keep credentials local.  The server stores only a
    # summary reported by the extension and dispatches structured tasks back to
    # the browser node; it never stores nor probes local executor access tokens.
    access_token_present = bool(str(access_token or "").strip())
    access_token = ""
    token_summary = dict(token_summary or {})
    with _lock:
        _load_persisted_nodes_locked()
        node = _nodes.get(str(node_id or "").strip())
        if not node or not secrets.compare_digest(str(node.get("node_secret") or ""), str(node_secret or "")):
            raise PermissionError("本地 Token 节点不存在或密钥无效，请重新绑定")
        now = _now_ts()
        if node_name:
            node["node_name"] = node_name.strip()
        if browser:
            node["browser"] = browser
        if user_agent:
            node["user_agent"] = user_agent
        if install_id:
            node["install_id"] = str(install_id or "").strip()
        if extension_version:
            node["extension_version"] = str(extension_version or "").strip()
        if capabilities is not None:
            node["capabilities"] = list(capabilities or [])
        node["last_seen_ts"] = now
        node["last_seen_at"] = _now_cst()
        node["token_plain"] = ""
        node["token_fp"] = ""

    if token_summary:
        present = bool(token_summary.get("present"))
        with _lock:
            node = _nodes[node_id]
            node["token_plain"] = ""
            node["token_fp"] = ""
            node["token_mask"] = str(token_summary.get("token_mask") or "")
            node["token_expires_at"] = str(token_summary.get("token_expires_at") or expires_at or "")
            node["token_expires_at_ts"] = _parse_time_to_ts(node["token_expires_at"])
            node["fb_user_id"] = str(token_summary.get("fb_user_id") or "")
            node["fb_user_name"] = str(token_summary.get("fb_user_name") or "")
            perms = token_summary.get("permissions") if isinstance(token_summary.get("permissions"), dict) else {}
            node["permissions"] = {
                "granted": list(perms.get("granted") or []),
                "declined": list(perms.get("declined") or []),
            }
            raw_account_ids = {
                _normalize_act_id(item)
                for item in (token_summary.get("account_ids") or [])
                if _normalize_act_id(item)
            }
            normalized_reported_accounts = []
            for item in (reported_accounts or token_summary.get("accounts") or []):
                if not isinstance(item, dict):
                    continue
                act_id = _normalize_act_id(item.get("act_id") or item.get("account_id") or item.get("id"))
                if not act_id:
                    continue
                normalized_reported_accounts.append({
                    "act_id": act_id,
                    "name": str(item.get("name") or item.get("account_name") or act_id),
                    "currency": str(item.get("currency") or ""),
                    "timezone": str(item.get("timezone") or item.get("timezone_name") or ""),
                    "write_status": str(item.get("write_status") or item.get("status") or ""),
                })
                raw_account_ids.add(act_id)
            visible_ids = {
                _normalize_act_id(item.get("act_id"))
                for item in _visible_accounts_for_node(node)
                if _normalize_act_id(item.get("act_id"))
            }
            node["account_ids"] = sorted(raw_account_ids.intersection(visible_ids))[:MAX_ACCOUNT_PROBE]
            node["reported_accounts"] = [
                item for item in normalized_reported_accounts
                if item.get("act_id") in set(node["account_ids"])
            ][:MAX_ACCOUNT_PROBE]
            if queue is not None:
                node["queue"] = {
                    "running": int((queue or {}).get("running") or 0),
                    "waiting": int((queue or {}).get("waiting") or 0),
                }
            node["has_ads_management"] = bool(token_summary.get("has_ads_management"))
            node["has_ads_read"] = bool(token_summary.get("has_ads_read"))
            node["verified_at_ts"] = _now_ts()
            node["verified_at"] = _now_cst()
            node["last_error"] = str(token_summary.get("last_error") or "")
            node["local_runtime_ready"] = bool(
                present and node["has_ads_management"] and node["account_ids"]
            )
            if runtime_status:
                node["runtime_status"] = str(runtime_status or "")
            if present and node["last_error"]:
                node["status"] = "token_error"
            elif present and node["has_ads_management"]:
                node["status"] = "online" if node["account_ids"] else "no_accounts"
                if not node["account_ids"] and not node["last_error"]:
                    node["last_error"] = "本地执行器没有匹配到当前团队/运营名下的账户"
            elif present:
                node["status"] = "permission_limited"
                if not node["last_error"]:
                    node["last_error"] = "本地执行器缺少 ads_management，无法执行广告创建/关闭/预算操作"
            else:
                node["status"] = "online_no_token"
                node["last_error"] = "本地执行器在线，但尚未汇报可执行摘要"
    elif not token_summary:
        with _lock:
            node = _nodes[node_id]
            node["token_plain"] = ""
            node["token_fp"] = ""
            node["local_runtime_ready"] = False
            node["status"] = "online_no_summary" if access_token_present else "no_summary"
            node["last_error"] = (
                "服务器已忽略本地 access_token；请让插件通过 token_summary 汇报账户/权限摘要，"
                "实际 API 请求必须由本地执行器完成。"
                if access_token_present
                else "插件尚未汇报本地执行器摘要"
            )

    with _lock:
        _save_persisted_nodes_locked()
    return node_public_view(node_id)


def _node_visible_to_user(node: dict, user: dict) -> bool:
    user = normalize_user_claims(user)
    role = user.get("role")
    if is_superadmin(user):
        return True
    if role == "admin":
        return node.get("team_id") == user.get("team_id")
    return int(node.get("user_id") or -1) == int(user.get("uid") or -2)


def node_public_view(node_id: str) -> dict:
    with _lock:
        node = dict(_nodes.get(node_id) or {})
    if not node:
        raise KeyError("node not found")
    account_ids = list(node.get("account_ids") or [])[:80]
    account_summaries = _account_summaries_for_ids(account_ids)
    reported_by_id = {
        item.get("act_id"): item
        for item in (node.get("reported_accounts") or [])
        if isinstance(item, dict) and item.get("act_id")
    }
    for item in account_summaries:
        reported = reported_by_id.get(item.get("act_id")) or {}
        if reported.get("name") and (not item.get("name") or item.get("name") == item.get("act_id")):
            item["name"] = reported.get("name")
        if reported.get("currency"):
            item["currency"] = reported.get("currency")
        if reported.get("timezone"):
            item["timezone"] = reported.get("timezone")
        if reported.get("write_status"):
            item["write_status"] = reported.get("write_status")
    now = _now_ts()
    last_seen = float(node.get("last_seen_ts") or 0)
    online = bool(last_seen and now - last_seen <= NODE_STALE_SECONDS)
    status = node.get("status") or "offline"
    if not online:
        status = "offline"
    exp_ts = float(node.get("token_expires_at_ts") or 0)
    expires_in_seconds = int(exp_ts - now) if exp_ts else None
    if expires_in_seconds is not None and expires_in_seconds <= 0 and status != "offline":
        status = "expired"
    cooldown_until = float(node.get("cooldown_until_ts") or 0)
    cooldown_remaining = int(cooldown_until - now) if cooldown_until > now else 0
    if cooldown_remaining > 0 and status == "online":
        status = "cooldown"
    has_runtime_token = bool(node.get("local_runtime_ready"))
    executable = bool(
        online
        and status == "online"
        and has_runtime_token
        and bool(node.get("has_ads_management"))
        and len(node.get("account_ids") or []) > 0
    )
    return {
        "node_id": node.get("node_id"),
        "node_name": node.get("node_name"),
        "browser": node.get("browser"),
        "install_id": node.get("install_id") or "",
        "extension_version": node.get("extension_version") or "",
        "capabilities": node.get("capabilities") or [],
        "username": node.get("username"),
        "operator_id": node.get("user_id"),
        "team_id": node.get("team_id"),
        "team_name": node.get("team_name"),
        "status": status,
        "online": online,
        "last_seen_at": node.get("last_seen_at"),
        "created_at": node.get("created_at"),
        "fb_user_id": node.get("fb_user_id"),
        "fb_user_name": node.get("fb_user_name"),
        "token_mask": node.get("token_mask"),
        "token_fingerprint": node.get("token_fp"),
        "token_expires_at": node.get("token_expires_at"),
        "expires_in_seconds": expires_in_seconds,
        "cooldown_remaining_seconds": cooldown_remaining,
        "cooldown_reason": node.get("cooldown_reason") or "",
        "has_runtime_token": has_runtime_token,
        "local_runtime_ready": has_runtime_token,
        "executable": executable,
        "activation_mode": "one_time_code",
        "binding_permanent": True,
        "heartbeat_interval_seconds": 30,
        "verified_at": node.get("verified_at"),
        "account_count": len(node.get("account_ids") or []),
        "account_ids": account_ids,
        "accounts": account_summaries,
        "has_ads_management": bool(node.get("has_ads_management")),
        "has_ads_read": bool(node.get("has_ads_read")),
        "permissions": node.get("permissions") or {"granted": [], "declined": []},
        "last_error": node.get("last_error") or "",
        "queue": node.get("queue") or {"running": 0, "waiting": 0},
        "runtime_status": node.get("runtime_status") or "",
        "source": "local_token",
    }


def list_nodes(user: dict) -> list[dict]:
    with _lock:
        _load_persisted_nodes_locked()
        _clean_expired()
        ids = [node_id for node_id, node in _nodes.items() if _node_visible_to_user(node, user)]
    return sorted(
        [node_public_view(node_id) for node_id in ids],
        key=lambda item: (not item.get("online"), item.get("node_name") or ""),
    )


def remove_node(node_id: str, user: dict) -> bool:
    with _lock:
        _load_persisted_nodes_locked()
        node = _nodes.get(node_id)
        if not node:
            return False
        if not _node_visible_to_user(node, user):
            raise PermissionError("无权移除此本地 Token 节点")
        _nodes.pop(node_id, None)
        _save_persisted_nodes_locked()
        return True


def authenticate_node(node_id: str, node_secret: str) -> dict:
    """Validate a bound local browser node and return its private metadata."""
    raw_id = str(node_id or "").strip()
    with _lock:
        _load_persisted_nodes_locked()
        node = _nodes.get(raw_id)
        if not node or not secrets.compare_digest(str(node.get("node_secret") or ""), str(node_secret or "")):
            raise PermissionError("本地执行器不存在或密钥无效，请重新绑定")
        return dict(node)


def mark_local_token_selected(node_id: str) -> None:
    with _lock:
        _load_persisted_nodes_locked()
        node = _nodes.get(node_id)
        if node:
            node["last_selected_at_ts"] = _now_ts()
            _save_persisted_nodes_locked()


def _node_by_token_plain_locked(plain_token: str) -> Optional[dict]:
    token_fp = _token_fp(plain_token)
    if not token_fp:
        return None
    for node in _nodes.values():
        if node.get("token_fp") == token_fp:
            return node
    return None


def wait_for_local_token_slot_by_plain(
    plain_token: str,
    min_gap_seconds: float = LOCAL_TOKEN_REQUEST_GAP_SECONDS,
) -> float:
    plain_token = str(plain_token or "").strip()
    if not plain_token:
        return 0.0
    with _lock:
        _load_persisted_nodes_locked()
        node = _node_by_token_plain_locked(plain_token)
        if not node:
            return 0.0
        now = _now_ts()
        cooldown_until = float(node.get("cooldown_until_ts") or 0)
        if cooldown_until > now:
            remain = int(cooldown_until - now)
            reason = node.get("cooldown_reason") or "local token cooldown"
            raise RuntimeError(f"Local token cooldown {remain}s: {reason}")
        next_at = float(node.get("next_request_at_ts") or 0)
        wait_seconds = max(0.0, next_at - now)
        node["next_request_at_ts"] = max(now, next_at) + max(
            0.2,
            float(min_gap_seconds or LOCAL_TOKEN_REQUEST_GAP_SECONDS),
        )
        node["last_selected_at_ts"] = now
        _save_persisted_nodes_locked()
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    return wait_seconds


def cooldown_local_token_by_plain(
    plain_token: str,
    seconds: float,
    reason: str = "",
    error_code: Optional[int] = None,
) -> bool:
    plain_token = str(plain_token or "").strip()
    if not plain_token or seconds <= 0:
        return False
    changed = False
    with _lock:
        _load_persisted_nodes_locked()
        node = _node_by_token_plain_locked(plain_token)
        if not node:
            return False
        now = _now_ts()
        node["cooldown_until_ts"] = max(float(node.get("cooldown_until_ts") or 0), now + float(seconds))
        node["cooldown_reason"] = str(reason or "")[:500]
        node["last_error_code"] = error_code
        node["last_error"] = str(reason or node.get("last_error") or "")[:500]
        changed = True
        _save_persisted_nodes_locked()
    return changed


def get_local_token_candidates_for_account(act_id: str, action_type: str = "CREATE") -> list[dict]:
    action = str(action_type or "").upper()
    if action not in {"CREATE", "UPDATE", "PAUSE"}:
        return []
    target = _normalize_act_id(act_id)
    now = _now_ts()
    candidates = []
    with _lock:
        _load_persisted_nodes_locked()
        for node_id, node in _nodes.items():
            last_seen = float(node.get("last_seen_ts") or 0)
            if not last_seen or now - last_seen > NODE_STALE_SECONDS:
                continue
            cooldown_until = float(node.get("cooldown_until_ts") or 0)
            if cooldown_until > now:
                continue
            exp_ts = float(node.get("token_expires_at_ts") or 0)
            if exp_ts and exp_ts <= now + 90:
                continue
            if not node.get("local_runtime_ready"):
                continue
            if target not in set(node.get("account_ids") or []):
                continue
            if not _account_allowed_for_node(target, node):
                continue
            candidates.append({
                "token_id": f"local:{node_id[:8]}",
                "alias": node.get("node_name") or f"local_{node_id[:6]}",
                "label": f"本地执行器·{node.get('node_name') or node_id[:6]}",
                "matrix_id": None,
                "token_source": "local_token",
                "source": "local_token",
                "node_id": node_id,
                "local_token": True,
                "local_executor": True,
                "last_selected_at_ts": float(node.get("last_selected_at_ts") or 0),
                "account_count": len(node.get("account_ids") or []),
            })
    candidates.sort(key=lambda item: (item.get("last_selected_at_ts") or 0, -int(item.get("account_count") or 0)))
    return candidates
