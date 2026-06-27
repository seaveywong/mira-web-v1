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


# ── FB 写权限错误分类（v3.11.154 §17.1）─────────────────────────────────────
# 共享分类器：把 FB 写操作的权限错误翻译成清晰的中文 user_message。
# 创建/预热类操作无 manage-token 兜底，遇到权限错误应明确告知用户去 BM 授权，
# 而不是把原始 FB JSON 透出 + 每次扫描重试刷屏。
# 覆盖三类：账户写权限（code=100/subcode=33）、主页广告权限（subcode=1487202）、
# 其它 100/200 类权限错误。非权限错误返回 is_permission=False，由调用方按原逻辑处理。

PERM_KIND_ACCOUNT_WRITE = "account_write"
PERM_KIND_PAGE_ADS = "page_ads"
PERM_KIND_GENERIC = "generic"

# 主页广告权限（subcode 1487202）的英文/中文字符特征 —— FB 有时只放在 message/user_msg 里
_PAGE_ADS_MARKERS = (
    "insufficient page permission",
    "create ads for this page",
    "permission to create ads",
    "page permission",
    "advertising permission",
    "无法为该主页创建广告",
    "主页广告权限",
    "主页无广告权限",
)

# code=100 但并非权限错误的已知 subcode（避免误判）：
# 1487067 = 广告组预算过低/被拒（warmup 的 _fb_error_is_invalid_adset_budget）
# 1487072 = 预算相关校验
# 1885183 = Meta App 处于 Development mode（launch_engine 单独处理）
_NON_PERM_CODE100_SUBCODES = {1487067, 1487072, 1885183}


def _classify_fb_write_error_value(value: Any) -> dict:
    """内部分类：接受 dict / 异常 / 字符串，返回 {is_permission, kind, user_message}。"""
    # 归一化为 (code, subcode, text)
    code = fb_error_code(value)
    subcode = fb_error_subcode(value)
    text = fb_error_message(value)
    raw = str(value or "")
    lower = text.lower() if text else ""

    # 1) 账户写权限：code=100 + subcode=33（操作号对该账户只读，写被拒）
    if code == 100 and subcode == 33:
        return {
            "is_permission": True,
            "kind": PERM_KIND_ACCOUNT_WRITE,
            "user_message": (
                "操作号对该广告账户无写权限——请在 BM 给系统用户授予 Advertiser/管理权限"
            ),
        }

    # 2) 主页广告权限：subcode=1487202，或 message/user_msg 里出现主页广告权限特征
    if subcode == 1487202 or any(marker in lower for marker in _PAGE_ADS_MARKERS):
        return {
            "is_permission": True,
            "kind": PERM_KIND_PAGE_ADS,
            "user_message": (
                "操作号对该主页无广告权限——请在 BM 主页设置里给系统用户授予 "
                "Create Ads/Manage Campaigns 权限"
            ),
        }

    # 3) 账户写权限（字符串降级路径）：格式化错误里出现 subcode=33 但没有 dict
    if code is None and subcode is None and "subcode=33" in raw:
        return {
            "is_permission": True,
            "kind": PERM_KIND_ACCOUNT_WRITE,
            "user_message": (
                "操作号对该广告账户无写权限——请在 BM 给系统用户授予 Advertiser/管理权限"
            ),
        }

    # 4) 其它 100/200/294 类权限错误（含 authorization/permission 字样）。
    #    排除 code=100 但实为预算/配置类错误的已知 subcode（1487067 预算被拒、1885183 Dev mode 等）。
    is_code100_non_perm = code == 100 and subcode in _NON_PERM_CODE100_SUBCODES
    if (not is_code100_non_perm) and (
        code in PERMISSION_ERROR_CODES
        or any(kw in lower for kw in ("authorization", "permission", "权限不足", "权限拒绝", "无写权限"))
    ):
        return {
            "is_permission": True,
            "kind": PERM_KIND_GENERIC,
            "user_message": "操作号权限不足，请在 BM 授权后重试",
        }

    return {"is_permission": False, "kind": "", "user_message": ""}


def classify_fb_write_error(exc_or_msg: Any) -> dict:
    """把 FB 写操作的错误分类为权限错误或非权限错误。

    接受：异常对象 / FB 错误 dict（含 ``error`` 嵌套）/ 已格式化的字符串。

    返回::
        {"is_permission": bool,
         "kind": "" | "account_write" | "page_ads" | "generic",
         "user_message": str}
    ``is_permission=False`` 时 kind/user_message 为空串，由调用方按原逻辑处理。
    """
    if isinstance(exc_or_msg, BaseException):
        # 异常：优先尝试 FB 错误 dict（很多地方把 result 当异常抛）；否则用字符串
        result = getattr(exc_or_msg, "args", None)
        if result and isinstance(result[0], dict):
            return _classify_fb_write_error_value(result[0])
        return _classify_fb_write_error_value(str(exc_or_msg))
    return _classify_fb_write_error_value(exc_or_msg)


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

