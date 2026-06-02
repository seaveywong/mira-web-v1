from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any, Optional

from core.auth import is_superadmin, require_admin, require_superadmin
from core.database import get_db


router = APIRouter()


class TeamBody(BaseModel):
    name: str
    note: Optional[str] = None
    status: Optional[str] = "active"


class ResourceAssignBody(BaseModel):
    kind: str
    ids: list[int]
    team_id: Optional[int] = None


RESOURCE_CONFIG: dict[str, dict[str, Any]] = {
    "accounts": {
        "table": "accounts",
        "alias": "r",
        "label": "Ad accounts",
        "select": """r.id, r.act_id, COALESCE(NULLIF(r.name,''), r.act_id) AS name,
                       r.currency, r.account_status, r.enabled, r.created_at,
                       r.team_id, t.name AS team_name""",
        "search": ("r.act_id", "r.name", "r.currency"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "tokens": {
        "table": "fb_tokens",
        "alias": "r",
        "label": "Tokens",
        "select": """r.id, r.token_alias AS name, r.token_type, r.token_source,
                       r.status, r.account_count, r.last_verified_at, r.created_at,
                       r.team_id, t.name AS team_name""",
        "search": ("r.token_alias", "r.token_type", "r.token_source", "r.status"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "assets": {
        "table": "ad_assets",
        "alias": "r",
        "label": "Assets",
        "select": """r.id, COALESCE(NULLIF(r.display_name,''), NULLIF(r.file_name,''), r.asset_code) AS name,
                       r.file_name, r.asset_code, r.file_type, r.source, r.upload_status,
                       r.score_label, r.created_at, r.team_id, t.name AS team_name""",
        "search": ("r.display_name", "r.file_name", "r.asset_code", "r.tags", "r.source"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "pages": {
        "table": "tw_certified_pages",
        "alias": "r",
        "label": "Certified pages",
        "select": """r.id, r.page_id, COALESCE(NULLIF(r.page_name,''), r.page_id) AS name,
                       r.page_status, r.page_can_advertise, r.page_is_published,
                       r.matrix_id, r.created_at, r.team_id, t.name AS team_name""",
        "search": ("r.page_id", "r.page_name", "r.page_status", "r.note"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "msg_templates": {
        "table": "msg_templates",
        "alias": "r",
        "label": "Message templates",
        "select": """r.id, r.name, r.destination, r.note, r.created_at, r.updated_at,
                       r.team_id, t.name AS team_name""",
        "search": ("r.name", "r.destination", "r.note"),
        "order": "r.updated_at DESC, r.id DESC",
    },
    "lead_forms": {
        "table": "lead_form_templates",
        "alias": "r",
        "label": "Lead form templates",
        "select": """r.id, r.name, r.locale, r.note, r.created_at, r.updated_at,
                       r.team_id, t.name AS team_name""",
        "search": ("r.name", "r.locale", "r.note"),
        "order": "r.updated_at DESC, r.id DESC",
    },
}


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
        "page_count": _count(conn, "tw_certified_pages", team_id),
        "msg_template_count": _count(conn, "msg_templates", team_id),
        "lead_form_count": _count(conn, "lead_form_templates", team_id),
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


@router.get("/resources/kinds")
def list_resource_kinds(user=Depends(require_superadmin)):
    return {
        "kinds": [
            {"kind": kind, "label": cfg["label"]}
            for kind, cfg in RESOURCE_CONFIG.items()
        ]
    }


@router.get("/resources")
def list_resources(
    kind: str = "accounts",
    team_id: Optional[str] = None,
    keyword: str = "",
    limit: int = 200,
    offset: int = 0,
    user=Depends(require_superadmin),
):
    cfg = RESOURCE_CONFIG.get(kind)
    if not cfg:
        raise HTTPException(status_code=400, detail="Unsupported resource kind")
    limit = max(1, min(int(limit or 200), 500))
    offset = max(0, int(offset or 0))
    alias = cfg["alias"]
    where = ["1=1"]
    params: list[Any] = []
    if team_id not in (None, "", "all"):
        if team_id == "unassigned":
            where.append(f"{alias}.team_id IS NULL")
        else:
            try:
                team_id_int = int(team_id)
            except Exception as exc:
                raise HTTPException(status_code=400, detail="Invalid team_id") from exc
            where.append(f"{alias}.team_id=?")
            params.append(team_id_int)
    keyword = (keyword or "").strip()
    if keyword:
        like = f"%{keyword}%"
        search_clause = " OR ".join([f"{col} LIKE ?" for col in cfg["search"]])
        where.append(f"({search_clause})")
        params.extend([like] * len(cfg["search"]))

    sql_from = f"{cfg['table']} {alias} LEFT JOIN teams t ON t.id = {alias}.team_id"
    where_sql = " AND ".join(where)
    conn = get_db()
    total = conn.execute(
        f"SELECT COUNT(*) AS c FROM {sql_from} WHERE {where_sql}",
        params,
    ).fetchone()["c"]
    rows = conn.execute(
        f"""SELECT {cfg['select']}
            FROM {sql_from}
            WHERE {where_sql}
            ORDER BY {cfg['order']}
            LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()
    teams = conn.execute("SELECT id, name, status FROM teams ORDER BY status, name").fetchall()
    conn.close()
    return {
        "kind": kind,
        "label": cfg["label"],
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [dict(row) for row in rows],
        "teams": [dict(row) for row in teams],
    }


@router.patch("/resources/assign")
def assign_resources(body: ResourceAssignBody, user=Depends(require_superadmin)):
    cfg = RESOURCE_CONFIG.get(body.kind)
    if not cfg:
        raise HTTPException(status_code=400, detail="Unsupported resource kind")
    ids = sorted({int(x) for x in (body.ids or []) if int(x) > 0})
    if not ids:
        raise HTTPException(status_code=400, detail="No resources selected")
    if len(ids) > 500:
        raise HTTPException(status_code=400, detail="At most 500 resources can be assigned at once")

    conn = get_db()
    team_name = None
    if body.team_id is not None:
        team = conn.execute("SELECT id, name FROM teams WHERE id=?", (body.team_id,)).fetchone()
        if not team:
            conn.close()
            raise HTTPException(status_code=400, detail="Team not found")
        team_name = team["name"]

    placeholders = ",".join(["?"] * len(ids))
    conn.execute(
        f"UPDATE {cfg['table']} SET team_id=? WHERE id IN ({placeholders})",
        [body.team_id] + ids,
    )
    changed = conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    conn.close()
    return {
        "success": True,
        "kind": body.kind,
        "updated": changed,
        "team_id": body.team_id,
        "team_name": team_name,
    }
