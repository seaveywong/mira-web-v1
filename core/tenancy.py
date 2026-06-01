from fastapi import HTTPException

from core.auth import is_superadmin


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


def assert_row_access(conn, table: str, row_id: int, user: dict, id_column: str = "id", allow_unassigned: bool = True):
    """Raise 404/403 unless the current user can access a row with team_id."""
    row = conn.execute(
        f"SELECT {id_column}, team_id FROM {table} WHERE {id_column}=?",
        (row_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Resource not found")
    if is_superadmin(user):
        return row
    team_id = team_id_for_create(user)
    row_team_id = row["team_id"]
    if row_team_id == team_id or (allow_unassigned and row_team_id is None):
        return row
    raise HTTPException(status_code=403, detail="Resource belongs to another team")

