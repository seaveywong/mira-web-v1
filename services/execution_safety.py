from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Optional

logger = logging.getLogger("mira.execution_safety")

ACCOUNT_WRITE_GAP_SECONDS = 2.5
TOKEN_WRITE_GAP_SECONDS = 1.5
LOCAL_TOKEN_WRITE_GAP_SECONDS = 3.0
ACCOUNT_LOCK_TIMEOUT_SECONDS = 15.0

RATE_LIMIT_ERROR_CODES = {4, 17, 32, 341, 613}
TRANSIENT_ERROR_CODES = {1, 2}
AUTH_ERROR_CODES = {190}
PERMISSION_ERROR_CODES = {10, 100, 200, 294}

_account_locks: dict[str, threading.Lock] = {}
_account_next_at: dict[str, float] = {}
_guard = threading.RLock()


def _safe_text(value: Any, limit: int = 500) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\n", " ").replace("\r", " ").strip()
    return text[:limit] + "..." if len(text) > limit else text


def _extract_error(value: Any) -> dict:
    if isinstance(value, dict):
        err = value.get("error")
        if isinstance(err, dict):
            return err
        if value.get("code") is not None or value.get("message"):
            return value
    return {"message": _safe_text(value)}


def fb_error_code(value: Any) -> Optional[int]:
    err = _extract_error(value)
    for key in ("code", "error_code"):
        raw = err.get(key)
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def fb_error_subcode(value: Any) -> Optional[int]:
    err = _extract_error(value)
    for key in ("error_subcode", "subcode"):
        raw = err.get(key)
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def fb_error_message(value: Any) -> str:
    err = _extract_error(value)
    parts = []
    for key in ("error_user_title", "error_user_msg", "message", "type"):
        v = err.get(key)
        if v and str(v) not in parts:
            parts.append(str(v))
    code = fb_error_code(value)
    subcode = fb_error_subcode(value)
    if code is not None:
        parts.append(f"code={code}")
    if subcode is not None:
        parts.append(f"subcode={subcode}")
    return _safe_text(" | ".join(parts) or err)


def is_transient_fb_error(value: Any) -> bool:
    code = fb_error_code(value)
    msg = fb_error_message(value).lower()
    return (
        code in RATE_LIMIT_ERROR_CODES
        or code in TRANSIENT_ERROR_CODES
        or "rate limit" in msg
        or "request limit" in msg
        or "temporarily unavailable" in msg
        or "retry your request later" in msg
        or "unexpected error" in msg
    )


def cooldown_seconds_for_fb_error(value: Any) -> float:
    code = fb_error_code(value)
    msg = fb_error_message(value).lower()
    if code in AUTH_ERROR_CODES:
        return 30 * 60.0
    if code in RATE_LIMIT_ERROR_CODES or "rate limit" in msg or "request limit" in msg:
        return 5 * 60.0
    if code in PERMISSION_ERROR_CODES:
        return 90.0
    if is_transient_fb_error(value):
        return 30.0
    return 0.0


@contextmanager
def account_write_guard(act_id: str, operation: str = "write", min_gap_seconds: float = ACCOUNT_WRITE_GAP_SECONDS):
    key = str(act_id or "").strip() or "global"
    with _guard:
        lock = _account_locks.setdefault(key, threading.Lock())
    acquired = lock.acquire(timeout=ACCOUNT_LOCK_TIMEOUT_SECONDS)
    if not acquired:
        raise RuntimeError(f"Account write queue busy: {key}")
    try:
        with _guard:
            wait_seconds = max(0.0, float(_account_next_at.get(key) or 0.0) - time.time())
        if wait_seconds > 0:
            logger.info(
                "[ExecutionSafety] account write delayed %.2fs act_id=%s operation=%s",
                wait_seconds,
                key,
                operation,
            )
            time.sleep(wait_seconds)
        yield
    finally:
        with _guard:
            _account_next_at[key] = max(time.time(), float(_account_next_at.get(key) or 0.0)) + max(
                0.1,
                float(min_gap_seconds or ACCOUNT_WRITE_GAP_SECONDS),
            )
        lock.release()


def wait_for_write_slot(
    token: str,
    source: str = "",
    operation: str = "write",
    min_gap_seconds: Optional[float] = None,
) -> float:
    token = str(token or "").strip()
    if not token:
        return 0.0
    wait_total = 0.0
    gap = LOCAL_TOKEN_WRITE_GAP_SECONDS if source == "local_token" else TOKEN_WRITE_GAP_SECONDS
    if min_gap_seconds is not None:
        gap = float(min_gap_seconds)

    try:
        from services.local_token_bridge import wait_for_local_token_slot_by_plain

        wait_total += float(wait_for_local_token_slot_by_plain(token, gap) or 0.0)
    except Exception as exc:
        # Local cooldown should be surfaced so the caller can try another token.
        if "cooldown" in str(exc).lower() or "cooling" in str(exc).lower():
            raise
        logger.debug("[ExecutionSafety] local token slot ignored: %s", exc)

    try:
        from services.token_manager import wait_for_token_slot_by_plain

        wait_total += float(wait_for_token_slot_by_plain(token, gap) or 0.0)
    except Exception as exc:
        logger.debug("[ExecutionSafety] db token slot ignored: %s", exc)

    if wait_total > 0.2:
        logger.info(
            "[ExecutionSafety] token write delayed %.2fs source=%s operation=%s",
            wait_total,
            source or "token",
            operation,
        )
    return wait_total


def note_write_failure(token: str, error: Any, operation: str = "write") -> float:
    seconds = cooldown_seconds_for_fb_error(error)
    if not token or seconds <= 0:
        return 0.0
    code = fb_error_code(error)
    reason = f"{operation}: {fb_error_message(error)}"
    try:
        from services.token_manager import cooldown_token_by_plain

        cooldown_token_by_plain(token, seconds, reason=reason, error_code=code)
    except Exception as exc:
        logger.debug("[ExecutionSafety] db token cooldown ignored: %s", exc)
    try:
        from services.local_token_bridge import cooldown_local_token_by_plain

        cooldown_local_token_by_plain(token, seconds, reason=reason, error_code=code)
    except Exception as exc:
        logger.debug("[ExecutionSafety] local token cooldown ignored: %s", exc)
    logger.warning(
        "[ExecutionSafety] token cooldown %.0fs operation=%s code=%s reason=%s",
        seconds,
        operation,
        code,
        _safe_text(reason, 220),
    )
    return seconds

