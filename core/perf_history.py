import time


_SCHEMA_READY = False
_LAST_CLEANUP_TS = 0.0


def ensure_perf_snapshot_history_schema(conn) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    conn.execute(
        """CREATE TABLE IF NOT EXISTS perf_snapshot_history (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           act_id TEXT NOT NULL,
           ad_id TEXT,
           adset_id TEXT,
           campaign_id TEXT,
           ad_name TEXT,
           snapshot_date TEXT NOT NULL,
           snapshot_at TEXT NOT NULL DEFAULT (datetime('now','+8 hours')),
           spend REAL DEFAULT 0,
           impressions INTEGER DEFAULT 0,
           clicks INTEGER DEFAULT 0,
           conversions REAL DEFAULT 0,
           cpa REAL,
           roas REAL,
           kpi_field TEXT,
           raw_actions TEXT,
           currency TEXT DEFAULT 'USD'
        )"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_perf_history_date_hour ON perf_snapshot_history(snapshot_date, snapshot_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_perf_history_act_date ON perf_snapshot_history(act_id, snapshot_date, snapshot_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_perf_history_ad_date ON perf_snapshot_history(ad_id, snapshot_date, snapshot_at)")
    conn.commit()
    _SCHEMA_READY = True


def cleanup_perf_snapshot_history(conn, retention_days: int = 180) -> None:
    global _LAST_CLEANUP_TS
    now = time.time()
    if now - _LAST_CLEANUP_TS < 86400:
        return
    retention_days = max(30, int(retention_days or 180))
    conn.execute(
        "DELETE FROM perf_snapshot_history WHERE date(snapshot_date) < date('now','+8 hours', ?)",
        (f"-{retention_days} days",),
    )
    _LAST_CLEANUP_TS = now


def append_perf_snapshot_history(
    conn,
    *,
    act_id,
    ad_id,
    adset_id,
    campaign_id,
    ad_name,
    snapshot_date,
    spend,
    impressions,
    clicks,
    conversions,
    cpa,
    roas,
    kpi_field,
    raw_actions,
    currency="USD",
) -> None:
    ensure_perf_snapshot_history_schema(conn)
    cleanup_perf_snapshot_history(conn)
    conn.execute(
        """INSERT INTO perf_snapshot_history
           (act_id, ad_id, adset_id, campaign_id, ad_name,
            snapshot_date, snapshot_at, spend, impressions, clicks,
            conversions, cpa, roas, kpi_field, raw_actions, currency)
           VALUES (?,?,?,?,?,?,datetime('now','+8 hours'),?,?,?,?,?,?,?,?,?)""",
        (
            act_id,
            ad_id,
            adset_id,
            campaign_id,
            ad_name,
            snapshot_date,
            spend,
            impressions,
            clicks,
            conversions,
            cpa,
            roas,
            kpi_field,
            raw_actions,
            currency,
        ),
    )
