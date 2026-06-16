"""Owner-scoped default stoploss rules.

These defaults are intentionally user-scoped, not account-scoped:
one visible rule set per active operator, covering that operator's accounts.
Defaults are created only once per operator. If an operator edits or deletes
them later, this helper must not recreate missing rules.
"""

from core.database import get_conn


OWNER_SCOPE_ACT_ID = "__owner__"
RULE_SCOPE_OWNER = "owner"
DEFAULT_OWNER_RULE_NOTE = "owner_default_stoploss_v2"
DEFAULT_OWNER_INIT_SETTING_PREFIX = "default_owner_rules_initialized:"

DEFAULT_OWNER_STOPLOSS_RULES = [
    ("购买目标 $20 空成效止损", "purchase", 20.0),
    ("线索目标 $20 空成效止损", "leads", 20.0),
    ("私信目标 $20 空成效止损", "messenger", 20.0),
    ("联系目标 $20 空成效止损", "contact", 20.0),
    ("流量目标 $20 空成效止损", "traffic", 20.0),
    ("互动目标 $20 空成效止损", "engagement", 20.0),
]
DEFAULT_OWNER_KPI_FILTERS = tuple(rule[1] for rule in DEFAULT_OWNER_STOPLOSS_RULES)


def _set_setting(conn, key: str, value: str) -> None:
    conn.execute("UPDATE settings SET value=? WHERE key=?", (value, key))
    if conn.execute("SELECT changes()").fetchone()[0] == 0:
        conn.execute("INSERT INTO settings(key, value) VALUES(?, ?)", (key, value))


def _has_setting(conn, key: str) -> bool:
    return conn.execute("SELECT 1 FROM settings WHERE key=? LIMIT 1", (key,)).fetchone() is not None


def ensure_rule_scope_schema(conn) -> None:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(guard_rules)").fetchall()}
    if "scope" not in cols:
        conn.execute("ALTER TABLE guard_rules ADD COLUMN scope TEXT DEFAULT 'account'")
    if "owner_user_id" not in cols:
        conn.execute("ALTER TABLE guard_rules ADD COLUMN owner_user_id INTEGER")
    if "team_id" not in cols:
        conn.execute("ALTER TABLE guard_rules ADD COLUMN team_id INTEGER")
    if "created_by" not in cols:
        conn.execute("ALTER TABLE guard_rules ADD COLUMN created_by TEXT")
    conn.execute("UPDATE guard_rules SET scope='account' WHERE scope IS NULL OR scope=''")
    conn.execute("DELETE FROM guard_rules WHERE act_id='__global__'")
    conn.execute(
        """UPDATE guard_rules
           SET team_id=(SELECT a.team_id FROM accounts a WHERE a.act_id=guard_rules.act_id)
           WHERE team_id IS NULL AND act_id NOT IN ('__global__', ?)""",
        (OWNER_SCOPE_ACT_ID,),
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_guard_rules_scope_owner ON guard_rules(scope, owner_user_id, enabled)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_guard_rules_scope_account ON guard_rules(act_id, scope, enabled)")

    scale_cols = {r["name"] for r in conn.execute("PRAGMA table_info(scale_rules)").fetchall()}
    if scale_cols:
        if "scope" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN scope TEXT DEFAULT 'account'")
        if "owner_user_id" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN owner_user_id INTEGER")
        if "team_id" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN team_id INTEGER")
        if "created_by" not in scale_cols:
            conn.execute("ALTER TABLE scale_rules ADD COLUMN created_by TEXT")
        conn.execute("UPDATE scale_rules SET scope='account' WHERE scope IS NULL OR scope=''")
        conn.execute("DELETE FROM scale_rules WHERE act_id='__global__'")
        conn.execute(
            """UPDATE scale_rules
               SET team_id=(SELECT a.team_id FROM accounts a WHERE a.act_id=scale_rules.act_id)
               WHERE team_id IS NULL AND act_id NOT IN ('__global__', ?)""",
            (OWNER_SCOPE_ACT_ID,),
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scale_rules_scope_owner ON scale_rules(scope, owner_user_id, enabled)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scale_rules_scope_account ON scale_rules(act_id, scope, enabled)")


def ensure_operator_default_stoploss_rules() -> dict:
    conn = get_conn()
    created = 0
    initialized = 0
    skipped_initialized = 0
    skipped_existing = 0
    skipped_disabled = 0
    try:
        row = conn.execute("SELECT value FROM settings WHERE key='default_owner_rules_enabled'").fetchone()
        disabled = bool(row and row["value"] == "0")
        ensure_rule_scope_schema(conn)
        conn.execute(
            """UPDATE guard_rules
               SET level='ad', target_id='__global__'
               WHERE scope=? AND act_id=? AND COALESCE(level,'')='account'""",
            (RULE_SCOPE_OWNER, OWNER_SCOPE_ACT_ID),
        )
        users = conn.execute(
            """SELECT id, username, team_id
               FROM users
               WHERE role='operator'
                 AND COALESCE(is_active, 1)=1
                 AND team_id IS NOT NULL"""
        ).fetchall()
        placeholders = ",".join("?" for _ in DEFAULT_OWNER_KPI_FILTERS)
        for user in users:
            uid = int(user["id"])
            init_key = f"{DEFAULT_OWNER_INIT_SETTING_PREFIX}{uid}"

            # Remove only system-owned legacy defaults. Operator edits must survive this helper.
            conn.execute(
                f"""DELETE FROM guard_rules
                    WHERE scope=? AND owner_user_id=?
                      AND (note=? OR note LIKE '%默认止损规则%')
                      AND (
                        rule_type!='bleed_abs'
                        OR kpi_filter IS NULL
                        OR kpi_filter NOT IN ({placeholders})
                      )""",
                (RULE_SCOPE_OWNER, uid, DEFAULT_OWNER_RULE_NOTE, *DEFAULT_OWNER_KPI_FILTERS),
            )

            # Normalize duplicate old defaults, but do not create anything once initialized.
            for rule_name, kpi_filter, amount in DEFAULT_OWNER_STOPLOSS_RULES:
                existing_rows = conn.execute(
                    """SELECT id FROM guard_rules
                       WHERE scope=? AND owner_user_id=?
                         AND rule_type='bleed_abs' AND kpi_filter=?
                         AND (note=? OR note LIKE '%默认止损规则%')
                       ORDER BY id ASC""",
                    (RULE_SCOPE_OWNER, uid, kpi_filter, DEFAULT_OWNER_RULE_NOTE),
                ).fetchall()
                if not existing_rows:
                    continue
                keep_id = existing_rows[0]["id"]
                conn.execute(
                    """UPDATE guard_rules
                       SET act_id=?, level='ad', target_id='__global__',
                           rule_name=COALESCE(NULLIF(rule_name, ''), ?),
                           param_value=COALESCE(param_value, ?),
                           action=COALESCE(NULLIF(action, ''), 'pause'),
                           note=?,
                           team_id=COALESCE(team_id, ?)
                       WHERE id=?""",
                    (OWNER_SCOPE_ACT_ID, rule_name, amount, DEFAULT_OWNER_RULE_NOTE, user["team_id"], keep_id),
                )
                if len(existing_rows) > 1:
                    stale_ids = [r["id"] for r in existing_rows[1:]]
                    stale_placeholders = ",".join("?" for _ in stale_ids)
                    conn.execute(f"DELETE FROM guard_rules WHERE id IN ({stale_placeholders})", stale_ids)

            if _has_setting(conn, init_key):
                skipped_initialized += 1
                continue

            any_owner_rule = conn.execute(
                "SELECT 1 FROM guard_rules WHERE scope=? AND owner_user_id=? LIMIT 1",
                (RULE_SCOPE_OWNER, uid),
            ).fetchone()
            if any_owner_rule:
                _set_setting(conn, init_key, "existing")
                skipped_existing += 1
                continue

            if disabled:
                skipped_disabled += 1
                continue

            for rule_name, kpi_filter, amount in DEFAULT_OWNER_STOPLOSS_RULES:
                conn.execute(
                    """INSERT INTO guard_rules
                       (act_id, rule_name, level, target_id, rule_type, param_value,
                        param_ratio, param_days, action, action_value, enabled, note,
                        kpi_filter, scope, owner_user_id, team_id, created_by)
                       VALUES (?, ?, 'ad', '__global__', 'bleed_abs', ?,
                               NULL, NULL, 'pause', NULL, 1, ?,
                               ?, ?, ?, ?, ?)""",
                    (
                        OWNER_SCOPE_ACT_ID,
                        rule_name,
                        amount,
                        DEFAULT_OWNER_RULE_NOTE,
                        kpi_filter,
                        RULE_SCOPE_OWNER,
                        uid,
                        user["team_id"],
                        "system",
                    ),
                )
                created += 1
            _set_setting(conn, init_key, "created")
            initialized += 1
        conn.commit()
    finally:
        conn.close()
    return {
        "created": created,
        "initialized": initialized,
        "skipped_initialized": skipped_initialized,
        "skipped_existing": skipped_existing,
        "skipped_disabled": skipped_disabled,
        "disabled": disabled,
    }
