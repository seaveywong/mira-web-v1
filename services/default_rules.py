from __future__ import annotations

import logging
from typing import Iterable

from core.database import get_conn


logger = logging.getLogger("mira")

DEFAULT_STOPLOSS_MARKER = "auto_default_stoploss_v1"

DEFAULT_STOPLOSS_RULES = [
    {
        "rule_name": "购买空成效止损 $20",
        "rule_type": "bleed_abs",
        "level": "account",
        "target_id": "__global__",
        "param_value": 20.0,
        "param_ratio": None,
        "param_days": None,
        "action": "pause",
        "action_value": None,
        "kpi_filter": "purchase",
        "note": DEFAULT_STOPLOSS_MARKER,
    },
    {
        "rule_name": "线索空成效止损 $20",
        "rule_type": "bleed_abs",
        "level": "account",
        "target_id": "__global__",
        "param_value": 20.0,
        "param_ratio": None,
        "param_days": None,
        "action": "pause",
        "action_value": None,
        "kpi_filter": "lead",
        "note": DEFAULT_STOPLOSS_MARKER,
    },
    {
        "rule_name": "私信空成效止损 $20",
        "rule_type": "bleed_abs",
        "level": "account",
        "target_id": "__global__",
        "param_value": 20.0,
        "param_ratio": None,
        "param_days": None,
        "action": "pause",
        "action_value": None,
        "kpi_filter": "messaging",
        "note": DEFAULT_STOPLOSS_MARKER,
    },
]


def _guard_rule_columns(conn) -> set[str]:
    return {row["name"] for row in conn.execute("PRAGMA table_info(guard_rules)").fetchall()}


def _insert_rule(conn, act_id: str, rule: dict, columns: set[str]) -> None:
    data = {
        "act_id": act_id,
        "rule_name": rule.get("rule_name"),
        "level": rule.get("level", "account"),
        "target_id": rule.get("target_id", "__global__"),
        "rule_type": rule.get("rule_type"),
        "param_value": rule.get("param_value"),
        "param_ratio": rule.get("param_ratio"),
        "param_days": rule.get("param_days"),
        "action": rule.get("action", "pause"),
        "action_value": rule.get("action_value"),
        "enabled": 1,
        "note": rule.get("note"),
        "kpi_filter": rule.get("kpi_filter"),
    }
    data = {key: value for key, value in data.items() if key in columns}
    names = list(data.keys())
    placeholders = ",".join("?" for _ in names)
    conn.execute(
        f"INSERT INTO guard_rules ({','.join(names)}) VALUES ({placeholders})",
        [data[name] for name in names],
    )


def ensure_default_stoploss_rules_for_accounts(act_ids: Iterable[str]) -> int:
    act_ids = [act_id for act_id in dict.fromkeys(act_ids or []) if act_id]
    if not act_ids:
        return 0
    conn = get_conn()
    added = 0
    try:
        columns = _guard_rule_columns(conn)
        for act_id in act_ids:
            existing = conn.execute(
                "SELECT 1 FROM guard_rules WHERE act_id=? AND note=? LIMIT 1",
                (act_id, DEFAULT_STOPLOSS_MARKER),
            ).fetchone()
            if existing:
                continue
            for rule in DEFAULT_STOPLOSS_RULES:
                _insert_rule(conn, act_id, rule, columns)
                added += 1
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("default stoploss rule seed failed")
    finally:
        conn.close()
    return added


def backfill_default_stoploss_rules_once() -> int:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key='default_stoploss_backfill_done_v1'"
        ).fetchone()
        if row and str(row["value"]) == "1":
            return 0
        act_ids = [r["act_id"] for r in conn.execute("SELECT act_id FROM accounts WHERE COALESCE(enabled,1)=1").fetchall()]
    finally:
        conn.close()

    added = ensure_default_stoploss_rules_for_accounts(act_ids)

    conn = get_conn()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO settings(key,value,label,description,category,sort_order)
               VALUES('default_stoploss_backfill_done_v1','1','默认止损模板已补齐','系统初始化标记','guard',99)"""
        )
        conn.commit()
    finally:
        conn.close()
    return added
