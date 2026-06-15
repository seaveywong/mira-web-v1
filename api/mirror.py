"""
镜像模式 API v1.0.0
账户级安全功能：开启后自动暂停不在快照白名单中的活跃广告
"""
import json
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from core.auth import get_current_user
from core.database import get_conn
from core.tenancy import apply_account_owner_scope, apply_team_scope, assert_row_access

router = APIRouter()
logger = logging.getLogger("mira.api.mirror")


class MirrorToggleRequest(BaseModel):
    act_id: str


def _ensure_mirror_schema():
    conn = get_conn()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        if "mirror_enabled" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN mirror_enabled INTEGER DEFAULT 0")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS mirror_snapshots (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               act_id TEXT NOT NULL,
               ad_id TEXT NOT NULL,
               ad_name TEXT,
               captured_at TEXT DEFAULT (datetime('now','+8 hours')),
               UNIQUE(act_id, ad_id)
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mirror_snapshots_act ON mirror_snapshots(act_id)")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS mirror_snapshot_meta (
               act_id TEXT PRIMARY KEY,
               captured_at TEXT DEFAULT (datetime('now','+8 hours')),
               verified_at TEXT,
               source TEXT,
               note TEXT,
               expected_count INTEGER DEFAULT 0,
               captured_count INTEGER DEFAULT 0,
               visible_count INTEGER DEFAULT 0,
               paging_complete INTEGER DEFAULT 1,
               is_partial INTEGER DEFAULT 0,
               last_error TEXT,
               last_checked_at TEXT
            )"""
        )
        meta_cols = {r["name"] for r in conn.execute("PRAGMA table_info(mirror_snapshot_meta)").fetchall()}
        for name, ddl in {
            "expected_count": "INTEGER DEFAULT 0",
            "captured_count": "INTEGER DEFAULT 0",
            "visible_count": "INTEGER DEFAULT 0",
            "paging_complete": "INTEGER DEFAULT 1",
            "is_partial": "INTEGER DEFAULT 0",
            "last_error": "TEXT",
            "last_checked_at": "TEXT",
        }.items():
            if name not in meta_cols:
                conn.execute(f"ALTER TABLE mirror_snapshot_meta ADD COLUMN {name} {ddl}")
        conn.execute(
            """INSERT OR IGNORE INTO settings(key,value,label,description,category,sort_order)
               VALUES ('mirror_enabled','0','镜像模式','开启后暂停所有不在快照白名单中的活跃广告','guard',5)"""
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_mirror_ads_payload(act_id: str, token: str) -> dict:
    from services.guard_engine import _fb_get, MIRROR_AD_FIELDS

    return _fb_get(
        f"{act_id}/ads", token,
        {"fields": MIRROR_AD_FIELDS, "limit": 200},
        paginate=True
    )


def _fetch_mirror_ads(act_id: str, token: str) -> list:
    data = _fetch_mirror_ads_payload(act_id, token)
    return data.get("data", [])


def _capture_verified_seed_snapshot(act_id: str, token: str) -> dict:
    from services.guard_engine import _capture_mirror_snapshot, _mirror_snapshotable_ads

    first = _fetch_mirror_ads_payload(act_id, token)
    second = _fetch_mirror_ads_payload(act_id, token)
    first_ads = _mirror_snapshotable_ads(first.get("data", []))
    second_ads = _mirror_snapshotable_ads(second.get("data", []))
    by_id = {}
    for ad in first_ads + second_ads:
        by_id[ad.get("id")] = ad
    by_id.pop(None, None)
    first_ids = {ad.get("id") for ad in first_ads if ad.get("id")}
    second_ids = {ad.get("id") for ad in second_ads if ad.get("id")}
    paging_complete = bool(first.get("_paging_complete", True)) and bool(second.get("_paging_complete", True))
    consistent = paging_complete and first_ids == second_ids
    count = _capture_mirror_snapshot(
        act_id,
        list(by_id.values()),
        source="manual_capture",
        note=("manual double-scan consistent; waiting first patrol verify" if consistent else "manual double-scan merged; waiting first patrol verify"),
        paging_complete=paging_complete,
        expected_count=max(len(first_ids), len(second_ids), len(by_id)),
        verified=False,
    )
    return {
        "captured": count,
        "first_count": len(first_ids),
        "second_count": len(second_ids),
        "consistent": consistent,
        "paging_complete": paging_complete,
        "verified": False,
    }


# ── 获取镜像状态 ──────────────────────────────────────────────────────────────

@router.get("/status")
def mirror_status(act_id: str = "", user=Depends(get_current_user)):
    """获取单个账户或所有账户的镜像状态"""
    _ensure_mirror_schema()
    conn = get_conn()
    if act_id:
        assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
        row = conn.execute(
            "SELECT act_id, name, mirror_enabled FROM accounts WHERE act_id=?", (act_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "账户不存在")
        snap_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM mirror_snapshots WHERE act_id=?", (act_id,)
        ).fetchone()["cnt"]
        captured = conn.execute(
            "SELECT MAX(captured_at) as ts FROM mirror_snapshots WHERE act_id=?", (act_id,)
        ).fetchone()["ts"]
        meta = conn.execute(
            """SELECT captured_at, verified_at, source, note, expected_count, captured_count,
                      visible_count, paging_complete, is_partial, last_error, last_checked_at
               FROM mirror_snapshot_meta WHERE act_id=?""",
            (act_id,)
        ).fetchone()
        conn.close()
        return {
            "act_id": act_id,
            "name": row["name"],
            "mirror_enabled": row["mirror_enabled"],
            "snapshot_count": snap_count,
            "captured_at": captured,
            "meta": dict(meta) if meta else None,
        }
    else:
        where, params = ["enabled=1"], []
        apply_team_scope(where, params, user, "team_id", include_unassigned=False)
        apply_account_owner_scope(where, params, user, "owner_user_id")
        rows = conn.execute(
            f"SELECT act_id, name, mirror_enabled FROM accounts WHERE {' AND '.join(where)} ORDER BY name",
            params,
        ).fetchall()
        result = []
        for r in rows:
            snap_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM mirror_snapshots WHERE act_id=?", (r["act_id"],)
            ).fetchone()["cnt"]
            meta = conn.execute(
                "SELECT verified_at, is_partial, last_error, last_checked_at FROM mirror_snapshot_meta WHERE act_id=?",
                (r["act_id"],)
            ).fetchone()
            result.append({
                "act_id": r["act_id"],
                "name": r["name"],
                "mirror_enabled": r["mirror_enabled"],
                "snapshot_count": snap_count,
                "verified": bool(meta and meta["verified_at"]),
                "is_partial": bool(meta and meta["is_partial"]),
                "last_error": meta["last_error"] if meta else "",
                "last_checked_at": meta["last_checked_at"] if meta else None,
            })
        conn.close()
        return {"accounts": result, "total": len(result)}


# ── 获取快照广告列表 ──────────────────────────────────────────────────────────

@router.get("/snapshot")
def mirror_snapshot(act_id: str, user=Depends(get_current_user)):
    """获取账户的镜像快照中的广告ID列表"""
    _ensure_mirror_schema()
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    rows = conn.execute(
        "SELECT ad_id, ad_name, captured_at FROM mirror_snapshots WHERE act_id=? ORDER BY captured_at",
        (act_id,)
    ).fetchall()
    conn.close()
    return {
        "act_id": act_id,
        "snapshot_count": len(rows),
        "ads": [{"ad_id": r["ad_id"], "ad_name": r["ad_name"], "captured_at": r["captured_at"]} for r in rows]
    }


# ── 开启镜像模式 ──────────────────────────────────────────────────────────────

@router.post("/enable")
def mirror_enable(body: MirrorToggleRequest, user=Depends(get_current_user)):
    """开启镜像模式：抓取当前活跃广告作为快照白名单"""
    _ensure_mirror_schema()
    act_id = body.act_id
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    acc = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, "账户不存在")

    # 获取当前活跃广告
    from services.guard_engine import _get_token_for_account
    token = _get_token_for_account(dict(acc))
    if not token:
        conn.close()
        raise HTTPException(400, "该账户无可用Token，无法抓取快照")

    try:
        capture = _capture_verified_seed_snapshot(act_id, token)
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"FB API调用失败: {e}")

    # 捕获快照
    count = capture["captured"]

    # 启用镜像模式
    conn.execute("UPDATE accounts SET mirror_enabled=1, updated_at=datetime('now','+8 hours') WHERE act_id=?", (act_id,))
    conn.commit()
    conn.close()

    logger.info(f"镜像模式已开启: {act_id}, 快照 {count} 条广告, 操作者: {user.get('username','unknown')}")
    return {"status": "ok", "act_id": act_id, "mirror_enabled": 1, "snapshot_count": count, "capture": capture}


# ── 关闭镜像模式 ──────────────────────────────────────────────────────────────

@router.post("/disable")
def mirror_disable(body: MirrorToggleRequest, user=Depends(get_current_user)):
    """关闭镜像模式"""
    _ensure_mirror_schema()
    act_id = body.act_id
    conn = get_conn()
    assert_row_access(conn, "accounts", act_id, user, id_column="act_id")
    acc = conn.execute("SELECT act_id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if not acc:
        conn.close()
        raise HTTPException(404, "账户不存在")
    conn.execute("UPDATE accounts SET mirror_enabled=0, updated_at=datetime('now','+8 hours') WHERE act_id=?", (act_id,))
    conn.commit()
    conn.close()
    logger.info(f"镜像模式已关闭: {act_id}, 操作者: {user.get('username','unknown')}")
    return {"status": "ok", "act_id": act_id, "mirror_enabled": 0}


# ── 全局开启：批量捕获所有账户快照 ─────────────────────────────────────────────

@router.post("/enable-all")
def mirror_enable_all(user=Depends(get_current_user)):
    """全局开启镜像模式：为所有活跃账户批量捕获快照"""
    _ensure_mirror_schema()
    from services.guard_engine import _get_token_for_account
    conn = get_conn()
    where, params = ["account_status NOT IN (3,7,9,100)"], []
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    apply_account_owner_scope(where, params, user, "owner_user_id")
    accounts = conn.execute(
        f"SELECT * FROM accounts WHERE {' AND '.join(where)}",
        params,
    ).fetchall()
    conn.close()

    total_captured = 0
    results = []
    for acc in accounts:
        a = dict(acc)
        act_id = a["act_id"]
        try:
            token = _get_token_for_account(a)
            if not token:
                results.append({"act_id": act_id, "name": a["name"], "status": "skipped", "reason": "无可用Token"})
                continue

            capture = _capture_verified_seed_snapshot(act_id, token)
            count = capture["captured"]
            total_captured += count
            results.append({"act_id": act_id, "name": a["name"], "status": "ok", "captured": count, "capture": capture})
        except Exception as e:
            results.append({"act_id": act_id, "name": a.get("name", act_id), "status": "error", "reason": str(e)})

    logger.info(f"全局镜像快照已捕获: {total_captured} 条广告, {len(results)} 个账户, 操作者: {user.get('username','unknown')}")
    return {"status": "ok", "total_captured": total_captured, "accounts": len(results), "details": results}
