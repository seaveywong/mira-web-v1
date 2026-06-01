from __future__ import annotations

from typing import Optional

from core.database import get_conn


READ_BLOCKING_STATUSES = {"permission_error", "no_read_token"}


def ensure_account_access_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    changed = False
    if "read_permission_status" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN read_permission_status TEXT")
        changed = True
    if "read_permission_error" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN read_permission_error TEXT")
        changed = True
    if "read_permission_checked_at" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN read_permission_checked_at TEXT")
        changed = True
    if changed:
        conn.commit()


def classify_read_failure(error: object) -> str:
    text = str(error or "")
    lower = text.lower()
    if "no_read_token" in lower or "no readable token" in lower or "无有效token" in lower:
        return "no_read_token"
    if (
        "ad account owner has not grant" in lower
        or "ad account owner has not granted" in lower
        or ("ads_management" in lower and "ads_read" in lower)
        or ("permission" in lower and "ad account" in lower)
    ):
        return "permission_error"
    if "code=200" in lower and "permission" in lower:
        return "permission_error"
    return "api_error"


def is_read_blocking_status(status: Optional[str]) -> bool:
    return (status or "") in READ_BLOCKING_STATUSES


def mark_account_read_success(conn, act_id: str) -> None:
    ensure_account_access_columns(conn)
    conn.execute(
        """
        UPDATE accounts
        SET read_permission_status='ok',
            read_permission_error=NULL,
            read_permission_checked_at=datetime('now','+8 hours')
        WHERE act_id=?
        """,
        (act_id,),
    )


def mark_account_read_failure(conn, act_id: str, error: object, status: Optional[str] = None) -> str:
    ensure_account_access_columns(conn)
    resolved = status or classify_read_failure(error)
    message = str(error or "")[:500]
    conn.execute(
        """
        UPDATE accounts
        SET read_permission_status=?,
            read_permission_error=?,
            read_permission_checked_at=datetime('now','+8 hours')
        WHERE act_id=?
        """,
        (resolved, message, act_id),
    )
    return resolved


def note_account_read_success(act_id: str) -> None:
    conn = get_conn()
    try:
        mark_account_read_success(conn, act_id)
        conn.commit()
    finally:
        conn.close()


def note_account_read_failure(act_id: str, error: object, status: Optional[str] = None) -> str:
    conn = get_conn()
    try:
        resolved = mark_account_read_failure(conn, act_id, error, status=status)
        conn.commit()
        return resolved
    finally:
        conn.close()
