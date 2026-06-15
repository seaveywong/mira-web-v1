"""v6 migration: first-class teams and non-enforcing resource ownership columns."""
import logging

logger = logging.getLogger("mira.migrate_v6")


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def _columns(conn, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_column(conn, table: str, column: str, ddl: str) -> None:
    if not _table_exists(conn, table):
        return
    if column in _columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
    logger.info("Added %s.%s", table, column)


def _find_team(conn, name: str) -> int | None:
    name = (name or "").strip()
    if not name:
        return None
    row = conn.execute("SELECT id FROM teams WHERE name=?", (name,)).fetchone()
    return int(row["id"]) if row else None


def run():
    from core.database import get_db

    conn = get_db()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            note TEXT,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at TEXT DEFAULT (datetime('now','+8 hours'))
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS user_team_memberships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            is_primary INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now','+8 hours')),
            UNIQUE(user_id, team_id)
        )"""
    )

    _add_column(conn, "users", "group_name", "group_name TEXT")
    _add_column(conn, "users", "team_id", "team_id INTEGER")
    _add_column(conn, "users", "team_role", "team_role TEXT DEFAULT 'member'")
    _add_column(conn, "user_activity_log", "team_id", "team_id INTEGER")
    _add_column(conn, "user_activity_log", "team_name", "team_name TEXT")
    _add_column(conn, "action_logs", "operator_user_id", "operator_user_id INTEGER")
    _add_column(conn, "action_logs", "operator_username", "operator_username TEXT")
    _add_column(conn, "action_logs", "team_id", "team_id INTEGER")
    _add_column(conn, "action_logs", "team_name", "team_name TEXT")

    for table in (
        "accounts",
        "fb_tokens",
        "ad_assets",
        "tw_certified_pages",
        "tw_advertisers",
        "msg_templates",
        "lead_form_templates",
        "page_lead_forms",
    ):
        _add_column(conn, table, "team_id", "team_id INTEGER")

    if _table_exists(conn, "users"):
        rows = conn.execute(
            "SELECT id, role, group_name, team_id FROM users ORDER BY id"
        ).fetchall()
        for row in rows:
            team_id = row["team_id"]
            team_name = (row["group_name"] or "").strip()
            if not team_id and team_name:
                team_id = _find_team(conn, team_name)
                if team_id:
                    conn.execute("UPDATE users SET team_id=? WHERE id=?", (team_id, row["id"]))
            if not team_id:
                continue
            membership_role = "admin" if row["role"] == "admin" else "member"
            conn.execute(
                """INSERT OR IGNORE INTO user_team_memberships
                   (user_id, team_id, role, is_primary)
                   VALUES (?, ?, ?, 1)""",
                (row["id"], team_id, membership_role),
            )

    conn.execute(
        """INSERT OR IGNORE INTO settings
           (key, value, label, description, placeholder, category, sort_order)
           VALUES
           ('team_access_enforced','0','Team access enforcement',
            'Keep disabled until accounts/assets are explicitly assigned to teams.',
            '', 'security', 10)"""
    )
    conn.commit()
    conn.close()
    logger.info("v6 team migration complete")


if __name__ == "__main__":
    run()
