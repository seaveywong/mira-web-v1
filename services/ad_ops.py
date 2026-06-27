import json
import logging
import threading
import time
from contextlib import contextmanager
from typing import Dict, Iterable, Optional, Tuple

import requests

from core.database import get_conn
from services.guard_engine import _local_per_usd_rate
from services.execution_safety import (
    account_write_guard,
    classify_fb_write_error,
    note_write_failure,
    wait_for_write_slot,
)
from services.local_executor import run_local_graph_task
from services.token_manager import (
    ACTION_PAUSE,
    ACTION_UPDATE,
    get_exec_token_candidates,
)


logger = logging.getLogger("mira.ad_ops")

FB_API_BASE = "https://graph.facebook.com/v25.0"
ZERO_DECIMAL_CURRENCIES = {
    "JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS",
}
LEVEL_FIELDS = {
    "ad": "id,name,status,effective_status,adset_id,campaign_id",
    "adset": (
        "id,name,status,effective_status,daily_budget,lifetime_budget,"
        "budget_remaining,bid_strategy,optimization_goal,campaign_id"
    ),
    "campaign": (
        "id,name,status,effective_status,daily_budget,lifetime_budget,"
        "budget_remaining,bid_strategy,objective"
    ),
}
ALLOWED_LEVELS = set(LEVEL_FIELDS)
ALLOWED_STATUS = {"ACTIVE", "PAUSED"}

_target_locks: Dict[str, threading.Lock] = {}
_target_locks_guard = threading.Lock()


class AdOpsError(RuntimeError):
    pass


def _sanitize(text) -> str:
    if text is None:
        return ""
    s = str(text)
    s = s.replace("\n", " ").replace("\r", " ")
    if len(s) > 900:
        s = s[:900] + "..."
    return s


def _fb_error(result: dict) -> str:
    err = result.get("error") if isinstance(result, dict) else None
    if not isinstance(err, dict):
        return _sanitize(result)
    bits = []
    if err.get("message"):
        bits.append(str(err.get("message")))
    if err.get("type"):
        bits.append(str(err.get("type")))
    if err.get("code") is not None:
        bits.append(f"code={err.get('code')}")
    if err.get("error_subcode") is not None:
        bits.append(f"subcode={err.get('error_subcode')}")
    return _sanitize(" | ".join(bits) or err)


def _fb_get(path: str, token: str, params: Optional[dict] = None) -> dict:
    payload = dict(params or {})
    payload["access_token"] = token
    try:
        resp = requests.get(f"{FB_API_BASE}/{path}", params=payload, timeout=25)
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        raise AdOpsError(f"Network error: {_sanitize(exc)}") from exc
    except ValueError as exc:
        raise AdOpsError("FB API returned non-json response") from exc
    if resp.status_code >= 400 or (isinstance(data, dict) and data.get("error")):
        raise AdOpsError(_fb_error(data if isinstance(data, dict) else {}))
    return data


def _fb_post(path: str, token: str, data: dict, source: str = "", operation: str = "ad_ops") -> dict:
    payload = dict(data or {})
    payload["access_token"] = token
    try:
        wait_for_write_slot(token, source=source, operation=operation)
        resp = requests.post(f"{FB_API_BASE}/{path}", data=payload, timeout=25)
        result = resp.json()
    except requests.exceptions.RequestException as exc:
        raise AdOpsError(f"Network error: {_sanitize(exc)}") from exc
    except ValueError as exc:
        raise AdOpsError("FB API returned non-json response") from exc
    if resp.status_code >= 400 or result.get("error") or result.get("success") is False:
        note_write_failure(token, result, operation=operation)
        # v3.11.154 §17.1：权限错误 → 清晰中文提示（原始 FB 错误作为次要信息保留）
        perm = classify_fb_write_error(result)
        if perm["is_permission"]:
            raise AdOpsError(f"{perm['user_message']}（原始错误：{_fb_error(result)}）")
        raise AdOpsError(_fb_error(result))
    return result


def _get_account(act_id: str) -> dict:
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise AdOpsError(f"Account not found: {act_id}")
    return dict(row)


def _get_rate(currency: str) -> float:
    return _local_per_usd_rate(currency)


def _amount_to_minor(amount: float, currency: str) -> int:
    currency = (currency or "USD").upper()
    amount = float(amount)
    if amount <= 0:
        raise AdOpsError("Budget must be greater than 0")
    if currency in ZERO_DECIMAL_CURRENCIES:
        return int(round(amount))
    return int(round(amount * 100))


def _minor_to_amount(value, currency: str) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if (currency or "USD").upper() in ZERO_DECIMAL_CURRENCIES:
        return round(raw, 2)
    return round(raw / 100, 2)


def _pick_candidates(act_id: str, action_type: str) -> list:
    candidates = get_exec_token_candidates(
        act_id,
        action_type,
        notify_exhausted=False,
        reserve=True,
    )
    if not candidates:
        raise AdOpsError("No available token for this operation")
    return candidates


@contextmanager
def _target_lock(key: str):
    with _target_locks_guard:
        lock = _target_locks.setdefault(key, threading.Lock())
    acquired = lock.acquire(timeout=3)
    if not acquired:
        raise AdOpsError("Another operation is running for this target")
    try:
        yield
    finally:
        lock.release()


def _read_target(level: str, target_id: str, token: str) -> dict:
    fields = LEVEL_FIELDS.get(level)
    if not fields:
        raise AdOpsError(f"Unsupported level: {level}")
    return _fb_get(target_id, token, {"fields": fields})


def _is_local_candidate(candidate: dict) -> bool:
    return bool(candidate and (candidate.get("local_executor") or candidate.get("source") == "local_token"))


def _read_target_with_candidate(act_id: str, level: str, target_id: str, token: str, candidate: dict) -> dict:
    fields = LEVEL_FIELDS.get(level)
    if not fields:
        raise AdOpsError(f"Unsupported level: {level}")
    if _is_local_candidate(candidate):
        return run_local_graph_task(
            candidate,
            "graph_get",
            act_id,
            {
                "path": target_id,
                "params": {"fields": fields},
                "_progress": f"本地读取 {level} 状态",
            },
            timeout_seconds=45,
            created_by_name="ad_ops",
        )
    return _read_target(level, target_id, token)


def _post_with_candidate(act_id: str, path: str, data: dict, token: str, candidate: dict, operation: str) -> dict:
    if _is_local_candidate(candidate):
        return run_local_graph_task(
            candidate,
            "graph_post",
            act_id,
            {
                "path": path,
                "data": dict(data or {}),
                "_progress": f"本地执行 {operation}",
            },
            timeout_seconds=60,
            created_by_name="ad_ops",
        )
    return _fb_post(
        path,
        token,
        data,
        source=candidate.get("source") or candidate.get("token_source") or "",
        operation=operation,
    )


def _log_action(
    act_id: str,
    level: str,
    target_id: str,
    target_name: str,
    action_type: str,
    detail: str,
    old_value: Optional[dict] = None,
    new_value: Optional[dict] = None,
    status: str = "success",
    error_msg: Optional[str] = None,
    operator: str = "manual",
):
    conn = get_conn()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(action_logs)").fetchall()}
        data = {
            "act_id": act_id,
            "level": level,
            "target_id": target_id,
            "target_name": target_name,
            "action_type": action_type,
            "trigger_type": "manual",
            "trigger_detail": _sanitize(detail),
            "old_value": json.dumps(old_value, ensure_ascii=False) if old_value else None,
            "new_value": json.dumps(new_value, ensure_ascii=False) if new_value else None,
            "status": status,
            "error_msg": _sanitize(error_msg) if error_msg else None,
            "operator": operator,
        }
        filtered = {k: v for k, v in data.items() if k in cols}
        keys = list(filtered)
        placeholders = ",".join("?" for _ in keys)
        conn.execute(
            f"INSERT INTO action_logs ({','.join(keys)}) VALUES ({placeholders})",
            [filtered[k] for k in keys],
        )
        conn.commit()
    finally:
        conn.close()


def _try_candidates(candidates: Iterable[dict], func) -> Tuple[dict, dict]:
    errors = []
    last_candidate = None
    for candidate in candidates:
        last_candidate = candidate
        token = candidate.get("token_plain") or candidate.get("token")
        try:
            result = func(token, candidate)
            return result, candidate
        except Exception as exc:
            label = candidate.get("label") or candidate.get("alias") or candidate.get("source") or "token"
            errors.append(f"{label}: {_sanitize(exc)}")
            logger.warning("[AdOps] candidate failed: %s", errors[-1])
            # v3.11.155 §1: 写权限错误是账户/主页级问题，与 token 无关。
            # account_write / page_ads 命中 → 立即停止轮换其它 token（轮换也救不回来），
            # 直接抛出清晰的中文 user_message。非权限错误（限流/瞬时）继续轮换。
            cls = classify_fb_write_error(exc)
            if cls["is_permission"] and cls["kind"] in (
                "account_write",
                "page_ads",
            ):
                raise AdOpsError(cls["user_message"]) from exc
    raise AdOpsError("; ".join(errors) or "All token candidates failed")


def set_status(
    act_id: str,
    level: str,
    target_id: str,
    desired_status: str,
    target_name: str = "",
    operator: str = "manual",
) -> dict:
    level = (level or "").lower().strip()
    desired_status = (desired_status or "").upper().strip()
    if level not in ALLOWED_LEVELS:
        raise AdOpsError("Unsupported target level")
    if desired_status not in ALLOWED_STATUS:
        raise AdOpsError("Unsupported status")

    account = _get_account(act_id)
    account_status = int(account.get("account_status") or 1)
    if desired_status == "ACTIVE" and account_status not in (1,):
        raise AdOpsError(f"Account is not writable: status={account_status}")

    action_type = ACTION_PAUSE if desired_status == "PAUSED" else ACTION_UPDATE
    candidates = _pick_candidates(act_id, action_type)
    lock_key = f"status:{level}:{target_id}"
    with _target_lock(lock_key), account_write_guard(act_id, f"status:{level}:{desired_status}"):
        def _do(token, candidate):
            before = _read_target_with_candidate(act_id, level, target_id, token, candidate)
            _post_with_candidate(
                act_id,
                target_id,
                {"status": desired_status},
                token,
                candidate,
                f"status:{level}:{desired_status}",
            )
            time.sleep(0.8)
            after = _read_target_with_candidate(act_id, level, target_id, token, candidate)
            return {"before": before, "after": after}

        result, used = _try_candidates(candidates, _do)
        before = result["before"]
        after = result["after"]

    actual = (after.get("status") or "").upper()
    effective = (after.get("effective_status") or "").upper()
    verified = actual == desired_status
    warning = ""
    if desired_status == "ACTIVE" and verified and effective != "ACTIVE":
        warning = (
            "Target status is ACTIVE, but effective_status is "
            f"{effective or 'unknown'}. Parent level or account state may still block delivery."
        )
    if not verified:
        warning = f"FB readback status is {actual or 'unknown'}"

    action_name = "manual_pause" if desired_status == "PAUSED" else "manual_resume"
    log_status = "success" if verified else "failed"
    _log_action(
        act_id,
        level,
        target_id,
        target_name or before.get("name") or after.get("name") or target_id,
        action_name,
        f"Manual {desired_status} via {used.get('label') or used.get('alias') or used.get('source')}",
        old_value={"status": before.get("status"), "effective_status": before.get("effective_status")},
        new_value={"status": after.get("status"), "effective_status": after.get("effective_status")},
        status=log_status,
        error_msg=warning if not verified else None,
        operator=operator,
    )
    if not verified:
        raise AdOpsError(warning or "Status readback verification failed")
    return {
        "status": "ok",
        "act_id": act_id,
        "level": level,
        "target_id": target_id,
        "desired_status": desired_status,
        "actual_status": actual,
        "effective_status": effective,
        "verified": verified,
        "warning": warning,
        "token_source": used.get("source"),
        "token_label": used.get("label") or used.get("alias"),
        "before": before,
        "after": after,
    }


def set_daily_budget(
    act_id: str,
    level: str,
    target_id: str,
    daily_budget: Optional[float] = None,
    daily_budget_usd: Optional[float] = None,
    target_name: str = "",
    operator: str = "manual",
) -> dict:
    level = (level or "").lower().strip()
    if level not in {"adset", "campaign"}:
        raise AdOpsError("Budget update only supports campaign or adset")
    account = _get_account(act_id)
    if int(account.get("account_status") or 1) != 1:
        raise AdOpsError(f"Account is not writable: status={account.get('account_status')}")
    currency = (account.get("currency") or "USD").upper()
    if daily_budget is None and daily_budget_usd is None:
        raise AdOpsError("daily_budget is required")
    if daily_budget is None:
        daily_budget = float(daily_budget_usd) * _get_rate(currency)
    minor = _amount_to_minor(float(daily_budget), currency)
    if minor <= 0:
        raise AdOpsError("Budget is too small")

    candidates = _pick_candidates(act_id, ACTION_UPDATE)
    lock_key = f"budget:{level}:{target_id}"
    with _target_lock(lock_key), account_write_guard(act_id, f"budget:{level}"):
        def _do(token, candidate):
            before = _read_target_with_candidate(act_id, level, target_id, token, candidate)
            if before.get("lifetime_budget") and not before.get("daily_budget"):
                raise AdOpsError("Target uses lifetime_budget and has no daily_budget to update")
            _post_with_candidate(
                act_id,
                target_id,
                {"daily_budget": str(minor)},
                token,
                candidate,
                f"budget:{level}",
            )
            time.sleep(0.8)
            after = _read_target_with_candidate(act_id, level, target_id, token, candidate)
            return {"before": before, "after": after}

        result, used = _try_candidates(candidates, _do)
        before = result["before"]
        after = result["after"]

    actual_minor = after.get("daily_budget")
    verified = str(actual_minor or "") == str(minor)
    old_amount = _minor_to_amount(before.get("daily_budget"), currency)
    new_amount = _minor_to_amount(actual_minor, currency)
    warning = "" if verified else f"FB readback daily_budget is {actual_minor or 'empty'}"
    _log_action(
        act_id,
        level,
        target_id,
        target_name or before.get("name") or after.get("name") or target_id,
        "manual_budget",
        f"Manual daily_budget update via {used.get('label') or used.get('alias') or used.get('source')}",
        old_value={"daily_budget": before.get("daily_budget"), "amount": old_amount, "currency": currency},
        new_value={"daily_budget": actual_minor, "amount": new_amount, "currency": currency},
        status="success" if verified else "failed",
        error_msg=warning if warning else None,
        operator=operator,
    )
    if not verified:
        raise AdOpsError(warning or "Budget readback verification failed")
    return {
        "status": "ok",
        "act_id": act_id,
        "level": level,
        "target_id": target_id,
        "currency": currency,
        "daily_budget_minor": minor,
        "daily_budget": new_amount,
        "old_daily_budget": old_amount,
        "verified": verified,
        "token_source": used.get("source"),
        "token_label": used.get("label") or used.get("alias"),
        "before": before,
        "after": after,
    }


def set_adset_budget(
    act_id: str,
    adset_id: str,
    daily_budget: Optional[float] = None,
    daily_budget_usd: Optional[float] = None,
    target_name: str = "",
    operator: str = "manual",
) -> dict:
    return set_daily_budget(
        act_id=act_id,
        level="adset",
        target_id=adset_id,
        daily_budget=daily_budget,
        daily_budget_usd=daily_budget_usd,
        target_name=target_name,
        operator=operator,
    )
