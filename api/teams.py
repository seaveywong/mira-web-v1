from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from core.auth import is_superadmin, require_admin, require_superadmin
from core.database import get_db


router = APIRouter()


class TeamBody(BaseModel):
    name: str
    note: Optional[str] = None
    status: Optional[str] = "active"


def _count(conn, table: str, team_id: int) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE team_id=?", (team_id,)).fetchone()
        return int(row["c"] if row else 0)
    except Exception:
        return 0


def _team_payload(conn, row) -> dict:
    team_id = int(row["id"])
    return {
        "id": team_id,
        "name": row["name"],
        "status": row["status"],
        "note": row["note"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "user_count": _count(conn, "users", team_id),
        "account_count": _count(conn, "accounts", team_id),
        "token_count": _count(conn, "fb_tokens", team_id),
        "asset_count": _count(conn, "ad_assets", team_id),
    }


@router.get("")
def list_teams(user=Depends(require_admin)):
    conn = get_db()
    if is_superadmin(user):
        rows = conn.execute("SELECT * FROM teams ORDER BY status, id").fetchall()
    else:
        team_id = user.get("team_id")
        rows = conn.execute("SELECT * FROM teams WHERE id=? ORDER BY id", (team_id,)).fetchall() if team_id else []
    teams = [_team_payload(conn, row) for row in rows]
    conn.close()
    return {"teams": teams}


@router.post("")
def create_team(body: TeamBody, user=Depends(require_superadmin)):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Team name is required")
    status = body.status if body.status in ("active", "paused") else "active"
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO teams (name, status, note, created_at, updated_at)
               VALUES (?, ?, ?, datetime('now','+8 hours'), datetime('now','+8 hours'))""",
            (name, status, body.note),
        )
        conn.commit()
        team_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    except Exception as exc:
        conn.close()
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=400, detail="Team name already exists")
        raise HTTPException(status_code=500, detail=str(exc))
    row = conn.execute("SELECT * FROM teams WHERE id=?", (team_id,)).fetchone()
    payload = _team_payload(conn, row)
    conn.close()
    return {"success": True, "team": payload}


@router.patch("/{team_id}")
def update_team(team_id: int, body: TeamBody, user=Depends(require_superadmin)):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Team name is required")
    status = body.status if body.status in ("active", "paused") else "active"
    conn = get_db()
    row = conn.execute("SELECT id FROM teams WHERE id=?", (team_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Team not found")
    try:
        conn.execute(
            """UPDATE teams
               SET name=?, status=?, note=?, updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (name, status, body.note, team_id),
        )
        conn.execute(
            """UPDATE users
               SET group_name=?
               WHERE team_id=? AND (group_name IS NULL OR group_name='' OR group_name!=?)""",
            (name, team_id, name),
        )
        conn.commit()
    except Exception as exc:
        conn.close()
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=400, detail="Team name already exists")
        raise HTTPException(status_code=500, detail=str(exc))
    row = conn.execute("SELECT * FROM teams WHERE id=?", (team_id,)).fetchone()
    payload = _team_payload(conn, row)
    conn.close()
    return {"success": True, "team": payload}


@router.get("/{team_id}/users")
def list_team_users(team_id: int, user=Depends(require_admin)):
    if not is_superadmin(user) and user.get("team_id") != team_id:
        raise HTTPException(status_code=403, detail="Permission denied")
    conn = get_db()
    rows = conn.execute(
        """SELECT id, username, role, display_name, note, is_active,
                  last_login_at, last_active_at, last_ip, created_at
           FROM users WHERE team_id=? ORDER BY role, id""",
        (team_id,),
    ).fetchall()
    conn.close()
    return {"users": [dict(row) for row in rows]}
