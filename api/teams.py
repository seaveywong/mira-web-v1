from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any, Optional

from core.auth import is_superadmin, require_admin, require_superadmin
from core.database import get_db
from services.notifier import ensure_notification_schema


router = APIRouter()


class TeamBody(BaseModel):
    name: str
    note: Optional[str] = None
    status: Optional[str] = "active"
    tg_chat_ids: Optional[str] = None
    notify_enabled: Optional[bool] = None


class TeamNotifyBody(BaseModel):
    tg_chat_ids: Optional[str] = None
    notify_enabled: Optional[bool] = True


class TeamGuardBody(BaseModel):
    sentinel_enabled: Optional[bool] = None
    mirror_enabled: Optional[bool] = None
    heartbeat_enabled: Optional[bool] = None
    warmup_enabled: Optional[bool] = None


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
                       r.team_id, t.name AS team_name, r.owner_user_id,
                       COALESCE(NULLIF(u.display_name,''), u.username) AS owner_user_name""",
        "joins": " LEFT JOIN users u ON u.id = r.owner_user_id",
        "search": ("r.act_id", "r.name", "r.currency"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "tokens": {
        "table": "fb_tokens",
        "alias": "r",
        "label": "Tokens",
        "select": """r.id, r.token_alias AS name, r.token_type, r.token_source,
                       r.status, r.account_count, r.matrix_id, r.last_verified_at, r.created_at,
                       r.team_id, t.name AS team_name, r.owner_user_id,
                       COALESCE(NULLIF(u.display_name,''), u.username) AS owner_user_name""",
        "joins": " LEFT JOIN users u ON u.id = r.owner_user_id",
        "search": ("r.token_alias", "r.token_type", "r.token_source", "r.status", "r.matrix_id"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "assets": {
        "table": "ad_assets",
        "alias": "r",
        "label": "Assets",
        "select": """r.id, COALESCE(NULLIF(r.display_name,''), NULLIF(r.file_name,''), r.asset_code) AS name,
                       r.file_name, r.asset_code, r.act_id, r.file_type, r.source, r.upload_status,
                       r.score_label, r.matrix_id, r.created_at, r.team_id, t.name AS team_name""",
        "search": ("r.display_name", "r.file_name", "r.asset_code", "r.act_id", "r.tags", "r.source"),
        "order": "r.created_at DESC, r.id DESC",
    },
    "pages": {
        "table": "tw_certified_pages",
        "alias": "r",
        "label": "Certified pages",
        "select": """r.id, r.page_id, COALESCE(NULLIF(r.page_name,''), r.page_id) AS name,
                       r.page_status, r.page_can_advertise, r.page_is_published,
                       r.matrix_id, r.token_id, ft.matrix_id AS token_matrix_id,
                       r.created_at, r.team_id, t.name AS team_name""",
        "joins": " LEFT JOIN fb_tokens ft ON ft.id = r.token_id",
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


TEAM_GUARD_KEYS = ("sentinel_enabled", "mirror_enabled", "heartbeat_enabled", "warmup_enabled")


def _act_id_variants(act_id: str) -> list[str]:
    raw = str(act_id or "").strip()
    if not raw:
        return []
    num = raw[4:] if raw.startswith("act_") else raw
    variants = [raw]
    if num and num not in variants:
        variants.append(num)
    prefixed = f"act_{num}" if num else ""
    if prefixed and prefixed not in variants:
        variants.append(prefixed)
    return variants


def _matrix_map_for_acts(conn, act_ids: list[str]) -> dict[str, list[int]]:
    clean = sorted({str(x or "").strip() for x in act_ids if str(x or "").strip()})
    if not clean:
        return {}
    variant_to_assets: dict[str, set[str]] = {}
    for act in clean:
        for variant in _act_id_variants(act):
            variant_to_assets.setdefault(variant, set()).add(act)
    variants = sorted(variant_to_assets)
    placeholders = ",".join("?" for _ in variants)
    rows = conn.execute(
        f"""
        SELECT a.act_id AS matched_act_id, t.matrix_id
        FROM accounts a
        JOIN fb_tokens t ON t.id=a.token_id
        WHERE a.act_id IN ({placeholders}) AND t.matrix_id IS NOT NULL
        UNION
        SELECT aot.act_id AS matched_act_id, t.matrix_id
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id=aot.token_id
        WHERE aot.act_id IN ({placeholders})
          AND aot.status='active'
          AND t.matrix_id IS NOT NULL
        """,
        variants + variants,
    ).fetchall()
    out: dict[str, set[int]] = {act: set() for act in clean}
    for row in rows:
        try:
            mid = int(row["matrix_id"])
        except (TypeError, ValueError):
            continue
        for source_act in variant_to_assets.get(str(row["matched_act_id"]), set()):
            out.setdefault(source_act, set()).add(mid)
    return {act: sorted(ids) for act, ids in out.items()}


def ensure_team_guard_schema(conn) -> None:
    ensure_notification_schema(conn)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(teams)").fetchall()}
    for key in TEAM_GUARD_KEYS:
        if key not in cols:
            conn.execute(f"ALTER TABLE teams ADD COLUMN {key} INTEGER DEFAULT 0")
    conn.commit()


def _assert_team_admin(team_id: int, user) -> None:
    if not is_superadmin(user) and user.get("team_id") != team_id:
        raise HTTPException(status_code=403, detail="Permission denied")


def _count(conn, table: str, team_id: int) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE team_id=?", (team_id,)).fetchone()
        return int(row["c"] if row else 0)
    except Exception:
        return 0


def _team_resource_counts(conn, team_id: int) -> dict[str, int]:
    return {
        "users": _count(conn, "users", team_id),
        "accounts": _count(conn, "accounts", team_id),
        "tokens": _count(conn, "fb_tokens", team_id),
        "assets": _count(conn, "ad_assets", team_id),
        "pages": _count(conn, "tw_certified_pages", team_id),
        "msg_templates": _count(conn, "msg_templates", team_id),
        "lead_forms": _count(conn, "lead_form_templates", team_id),
    }


def _team_payload(conn, row) -> dict:
    team_id = int(row["id"])
    counts = _team_resource_counts(conn, team_id)
    return {
        "id": team_id,
        "name": row["name"],
        "status": row["status"],
        "note": row["note"],
        "tg_chat_ids": row["tg_chat_ids"] if "tg_chat_ids" in row.keys() else "",
        "notify_enabled": bool(row["notify_enabled"]) if "notify_enabled" in row.keys() and row["notify_enabled"] is not None else True,
        "sentinel_enabled": bool(row["sentinel_enabled"]) if "sentinel_enabled" in row.keys() else False,
        "mirror_enabled": bool(row["mirror_enabled"]) if "mirror_enabled" in row.keys() else False,
        "heartbeat_enabled": bool(row["heartbeat_enabled"]) if "heartbeat_enabled" in row.keys() else False,
        "warmup_enabled": bool(row["warmup_enabled"]) if "warmup_enabled" in row.keys() else False,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "user_count": counts["users"],
        "account_count": counts["accounts"],
        "token_count": counts["tokens"],
        "asset_count": counts["assets"],
        "page_count": counts["pages"],
        "msg_template_count": counts["msg_templates"],
        "lead_form_count": counts["lead_forms"],
    }


@router.get("")
def list_teams(user=Depends(require_admin)):
    conn = get_db()
    ensure_team_guard_schema(conn)
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
    ensure_team_guard_schema(conn)
    try:
        conn.execute(
            """INSERT INTO teams (name, status, note, tg_chat_ids, notify_enabled, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, datetime('now','+8 hours'), datetime('now','+8 hours'))""",
            (name, status, body.note, body.tg_chat_ids or "",
             1 if body.notify_enabled is not False else 0),
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
    ensure_team_guard_schema(conn)
    row = conn.execute("SELECT id FROM teams WHERE id=?", (team_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Team not found")
    try:
        conn.execute(
            """UPDATE teams
               SET name=?, status=?, note=?,
                   tg_chat_ids=COALESCE(?, tg_chat_ids),
                   notify_enabled=COALESCE(?, notify_enabled),
                   updated_at=datetime('now','+8 hours')
               WHERE id=?""",
            (name, status, body.note, body.tg_chat_ids,
             (1 if body.notify_enabled else 0) if body.notify_enabled is not None else None,
             team_id),
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


@router.delete("/{team_id}")
def delete_team(team_id: int, user=Depends(require_superadmin)):
    conn = get_db()
    ensure_team_guard_schema(conn)
    row = conn.execute("SELECT id, name FROM teams WHERE id=?", (team_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Team not found")
    counts = _team_resource_counts(conn, team_id)
    if sum(counts.values()) > 0:
        conn.close()
        raise HTTPException(status_code=400, detail="Team still has users or resources. Transfer or clear them first.")
    conn.execute("DELETE FROM teams WHERE id=?", (team_id,))
    conn.commit()
    conn.close()
    return {"success": True, "team_id": team_id, "message": "Team deleted"}


@router.get("/{team_id}/notify-settings")
def get_team_notify_settings(team_id: int, user=Depends(require_admin)):
    _assert_team_admin(team_id, user)
    conn = get_db()
    ensure_team_guard_schema(conn)
    row = conn.execute(
        "SELECT id, name, tg_chat_ids, COALESCE(notify_enabled, 1) AS notify_enabled FROM teams WHERE id=?",
        (team_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Team not found")
    return {
        "team_id": row["id"],
        "team_name": row["name"],
        "tg_chat_ids": row["tg_chat_ids"] or "",
        "notify_enabled": bool(row["notify_enabled"]),
    }


@router.patch("/{team_id}/notify-settings")
def update_team_notify_settings(team_id: int, body: TeamNotifyBody, user=Depends(require_admin)):
    _assert_team_admin(team_id, user)
    conn = get_db()
    ensure_team_guard_schema(conn)
    row = conn.execute("SELECT id FROM teams WHERE id=?", (team_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Team not found")
    conn.execute(
        """UPDATE teams
           SET tg_chat_ids=?, notify_enabled=?, updated_at=datetime('now','+8 hours')
           WHERE id=?""",
        (body.tg_chat_ids or "", 1 if body.notify_enabled else 0, team_id),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@router.get("/{team_id}/guard-settings")
def get_team_guard_settings(team_id: int, user=Depends(require_admin)):
    _assert_team_admin(team_id, user)
    conn = get_db()
    ensure_team_guard_schema(conn)
    row = conn.execute(
        f"SELECT id, name, {', '.join(TEAM_GUARD_KEYS)} FROM teams WHERE id=?",
        (team_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Team not found")
    return {
        "team_id": row["id"],
        "team_name": row["name"],
        **{key: bool(row[key]) for key in TEAM_GUARD_KEYS},
    }


@router.patch("/{team_id}/guard-settings")
def update_team_guard_settings(team_id: int, body: TeamGuardBody, user=Depends(require_admin)):
    _assert_team_admin(team_id, user)
    updates: list[str] = []
    params: list[int] = []
    dump = getattr(body, "model_dump", None)
    data = dump(exclude_unset=True) if dump else body.dict(exclude_unset=True)
    for key in TEAM_GUARD_KEYS:
        if key in data:
            updates.append(f"{key}=?")
            params.append(1 if data[key] else 0)
    if not updates:
        return {"success": True, "updated": 0}
    conn = get_db()
    ensure_team_guard_schema(conn)
    row = conn.execute("SELECT id FROM teams WHERE id=?", (team_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Team not found")
    conn.execute(
        f"UPDATE teams SET {', '.join(updates)}, updated_at=datetime('now','+8 hours') WHERE id=?",
        params + [team_id],
    )
    conn.commit()
    conn.close()
    return {"success": True, "updated": len(updates)}


@router.get("/{team_id}/users")
def list_team_users(team_id: int, user=Depends(require_admin)):
    _assert_team_admin(team_id, user)
    conn = get_db()
    ensure_team_guard_schema(conn)
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

    sql_from = f"{cfg['table']} {alias} LEFT JOIN teams t ON t.id = {alias}.team_id{cfg.get('joins', '')}"
    where_sql = " AND ".join(where)
    conn = get_db()
    ensure_team_guard_schema(conn)
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
    items = [dict(row) for row in rows]
    if kind in ("accounts", "assets"):
        matrix_map = _matrix_map_for_acts(conn, [item.get("act_id") for item in items])
        for item in items:
            item["linked_matrix_ids"] = matrix_map.get(str(item.get("act_id") or "").strip(), [])
    elif kind == "pages":
        for item in items:
            ids: set[int] = set()
            for mid in (item.get("matrix_id"), item.get("token_matrix_id")):
                try:
                    parsed = int(mid)
                except (TypeError, ValueError):
                    continue
                if parsed > 0:
                    ids.add(parsed)
            item["linked_matrix_ids"] = sorted(ids)
    teams = conn.execute("SELECT id, name, status FROM teams ORDER BY status, name").fetchall()
    conn.close()
    return {
        "kind": kind,
        "label": cfg["label"],
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
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
    if body.kind == "accounts":
        account_rows = conn.execute(
            f"SELECT act_id FROM accounts WHERE id IN ({placeholders})",
            ids,
        ).fetchall()
        conn.execute(
            f"UPDATE {cfg['table']} SET team_id=?, owner_user_id=NULL WHERE id IN ({placeholders})",
            [body.team_id] + ids,
        )
        changed = conn.execute("SELECT changes()").fetchone()[0]
        act_ids = [r["act_id"] for r in account_rows if r["act_id"]]
        if act_ids:
            act_placeholders = ",".join(["?"] * len(act_ids))
            conn.execute(
                f"""UPDATE guard_rules
                    SET team_id=?, owner_user_id=NULL
                    WHERE COALESCE(scope,'account')='account'
                      AND act_id IN ({act_placeholders})""",
                [body.team_id] + act_ids,
            )
            conn.execute(
                f"""UPDATE scale_rules
                    SET team_id=?, owner_user_id=NULL
                    WHERE COALESCE(scope,'account')='account'
                      AND act_id IN ({act_placeholders})""",
                [body.team_id] + act_ids,
            )
    else:
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
