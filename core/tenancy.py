from fastapi import HTTPException

from core.auth import is_superadmin, normalize_user_claims


def _row_get(row, key: str, index: int = 0):
    try:
        return row[key]
    except Exception:
        return row[index]


def team_write_block_reason(conn, user: dict) -> str | None:
    """Return a user-facing reason when this user's team cannot write."""
    if is_superadmin(user):
        return None
    team_id = user.get("team_id") if isinstance(user, dict) else None
    if not team_id:
        return None
    try:
        team_id = int(team_id)
    except (TypeError, ValueError):
        return "当前用户团队信息异常，写入操作已禁用，请重新登录或联系超级管理员"

    row = conn.execute("SELECT status FROM teams WHERE id=?", (team_id,)).fetchone()
    if not row:
        return "当前用户所属团队不存在，写入操作已禁用，请联系超级管理员"

    status = (_row_get(row, "status") or "active").strip().lower()
    if status != "active":
        return "当前团队已暂停，写入操作已禁用，请联系超级管理员"
    return None


def team_id_for_claim(user: dict) -> int | None:
    """Return the team id used when a team user writes an unassigned row."""
    if is_superadmin(user):
        return None
    team_id = user.get("team_id") if isinstance(user, dict) else None
    if not team_id:
        return None
    try:
        return int(team_id)
    except (TypeError, ValueError):
        return None


def claim_row_for_team(conn, table: str, id_column: str, row_id, user: dict, team_column: str = "team_id") -> int | None:
    """Stamp team_id on legacy unassigned rows when a team user writes them."""
    team_id = team_id_for_claim(user)
    if team_id is None:
        return None
    conn.execute(
        f"UPDATE {table} SET {team_column}=COALESCE({team_column}, ?) WHERE {id_column}=?",
        (team_id, row_id),
    )
    return team_id


def team_id_for_create(user: dict) -> int | None:
    """Return the team id to stamp on newly-created resources."""
    if is_superadmin(user):
        return None
    team_id = user.get("team_id") if isinstance(user, dict) else None
    if not team_id:
        raise HTTPException(status_code=403, detail="Current user is not assigned to a team")
    return int(team_id)


def team_scope_condition(user: dict, column: str = "team_id", include_unassigned: bool = True):
    """SQL condition/params for resources visible to a team user."""
    if is_superadmin(user):
        return "", []
    team_id = team_id_for_create(user)
    if include_unassigned:
        return f"({column}=? OR {column} IS NULL)", [team_id]
    return f"{column}=?", [team_id]


def apply_team_scope(where: list[str], params: list, user: dict, column: str = "team_id", include_unassigned: bool = True):
    condition, scope_params = team_scope_condition(user, column, include_unassigned)
    if condition:
        where.append(condition)
        params.extend(scope_params)


def is_operator_user(user: dict) -> bool:
    return normalize_user_claims(user).get("role") == "operator"


def user_id(user: dict) -> int:
    try:
        return int(normalize_user_claims(user).get("uid"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=403, detail="Current user id is invalid")


def apply_account_owner_scope(where: list[str], params: list, user: dict, column: str = "owner_user_id"):
    """Restrict account-backed resources to the current operator's own accounts."""
    if is_operator_user(user):
        where.append(f"{column}=?")
        params.append(user_id(user))


def assert_row_access(conn, table: str, row_id: int, user: dict, id_column: str = "id", allow_unassigned: bool = True):
    """Raise 404/403 unless the current user can access a row with team_id."""
    has_owner_column = False
    select_cols = f"{id_column}, team_id"
    if table == "accounts":
        try:
            account_cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
            has_owner_column = "owner_user_id" in account_cols
        except Exception:
            has_owner_column = False
        if has_owner_column:
            select_cols += ", owner_user_id"
    row = conn.execute(
        f"SELECT {select_cols} FROM {table} WHERE {id_column}=?",
        (row_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Resource not found")
    if is_superadmin(user):
        return row
    team_id = team_id_for_create(user)
    row_team_id = row["team_id"]
    team_allowed = row_team_id == team_id or (allow_unassigned and row_team_id is None)
    if not team_allowed:
        raise HTTPException(status_code=403, detail="Resource belongs to another team")
    if table == "accounts" and has_owner_column and is_operator_user(user):
        owner_user_id = row["owner_user_id"]
        if owner_user_id is None:
            raise HTTPException(status_code=403, detail="Account is not assigned to current operator")
        if int(owner_user_id) != user_id(user):
            raise HTTPException(status_code=403, detail="Account belongs to another operator")
    return row
