#!/usr/bin/env python3
"""Focused checks for ad-level landing link performance attribution."""

import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import api.landing_pages as landing_pages  # noqa: E402
from api.landing_pages import (  # noqa: E402
    LandingRouteNextReq,
    _ad_link_stats,
    _landing_ad_link_create_count,
    next_landing_route_target,
)
from core import perf_history  # noqa: E402


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE landing_events (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           page_id INTEGER,
           event_type TEXT,
           path TEXT,
           target_url TEXT,
           ip_hash TEXT,
           user_agent_hash TEXT,
           metadata TEXT,
           created_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE landing_ad_links (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           page_id INTEGER,
           slug TEXT,
           ad_id TEXT,
           target_url TEXT,
           status TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE landing_ad_link_results (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           link_id INTEGER,
           result_date TEXT,
           confirmed_actions INTEGER DEFAULT 0,
           confirmed_sales INTEGER DEFAULT 0,
           confirmed_revenue REAL DEFAULT 0,
           source TEXT,
           note TEXT,
           updated_at TEXT,
           UNIQUE(link_id,result_date)
        )"""
    )
    conn.execute(
        """CREATE TABLE asset_spend_log (
           fb_ad_id TEXT,
           spend REAL DEFAULT 0,
           conv REAL DEFAULT 0,
           clicks REAL DEFAULT 0,
           impressions REAL DEFAULT 0,
           last_synced_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE perf_snapshots (
           act_id TEXT,
           ad_id TEXT,
           snapshot_date TEXT,
           snapshot_at TEXT,
           spend REAL DEFAULT 0,
           conversions REAL DEFAULT 0,
           clicks REAL DEFAULT 0,
           impressions REAL DEFAULT 0
        )"""
    )
    perf_history._SCHEMA_READY = False
    perf_history.ensure_perf_snapshot_history_schema(conn)
    return conn


def test_date_range_uses_daily_history_without_double_counting():
    conn = make_conn()
    conn.execute(
        "INSERT INTO landing_ad_links (page_id, slug, ad_id, status) VALUES (1,'abc123','1201','active')"
    )
    conn.execute(
        """INSERT INTO perf_snapshot_history
           (act_id, ad_id, snapshot_date, snapshot_at, spend, conversions, clicks, impressions)
           VALUES
           ('act_1','1201','2026-06-17','2026-06-17 10:00:00',5,0,10,100),
           ('act_1','1201','2026-06-17','2026-06-17 12:00:00',7,1,12,120),
           ('act_1','1201','2026-06-18','2026-06-18 09:00:00',2,0,4,40)"""
    )
    stats = _ad_link_stats(conn, 1, "abc123", date_from="2026-06-17", date_to="2026-06-17")
    assert_equal(stats["spend"], 7.0, "single-day spend should use max daily snapshot, not sum samples")
    assert_equal(stats["fb_conversions"], 1.0, "single-day conversions should use max daily snapshot")
    assert_equal(stats["spend_source"], "perf_snapshot_history", "date stats should prefer history")

    stats = _ad_link_stats(conn, 1, "abc123", date_from="2026-06-17", date_to="2026-06-18")
    assert_equal(stats["spend"], 9.0, "range spend should sum one daily total per day")
    assert_equal(stats["fb_clicks"], 16.0, "range clicks should sum one daily total per day")
    conn.close()


def test_spend_log_fallback_when_no_history_exists():
    conn = make_conn()
    conn.execute(
        "INSERT INTO landing_ad_links (page_id, slug, ad_id, status) VALUES (1,'def456','1202','active')"
    )
    conn.execute(
        """INSERT INTO asset_spend_log
           (fb_ad_id, spend, conv, clicks, impressions, last_synced_at)
           VALUES ('1202', 12.5, 2, 8, 88, '2026-06-17 13:00:00')"""
    )
    stats = _ad_link_stats(conn, 1, "def456", date_from="2026-06-17", date_to="2026-06-17")
    assert_equal(stats["spend"], 12.5, "missing history should fall back to cached spend")
    assert_equal(stats["spend_source"], "asset_spend_log", "fallback source should be explicit")
    conn.close()


def test_redirect_events_with_ad_slug_metadata_are_attributed_to_ad_link():
    conn = make_conn()
    conn.execute(
        "INSERT INTO landing_ad_links (page_id, slug, ad_id, status) VALUES (1,'abc123','1203','active')"
    )
    conn.execute(
        """INSERT INTO landing_events
           (page_id, event_type, path, target_url, ip_hash, user_agent_hash, metadata, created_at)
           VALUES
           (1, 'visit', '/a/abc123', '', 'ip1', 'ua1', '{"ad_slug": "abc123"}', '2026-06-19 10:00:00'),
           (1, 'redirect', '/__mira/redirect', 'https://wa.me/111', 'ip1', 'ua1', '{"ad_slug": "abc123"}', '2026-06-19 10:01:00'),
           (1, 'redirect', '/__mira/redirect', 'https://wa.me/222', 'ip2', 'ua2', '{"ad_slug": "other"}', '2026-06-19 10:02:00')"""
    )
    stats = _ad_link_stats(conn, 1, "abc123", date_from="2026-06-19", date_to="2026-06-19")
    assert_equal(stats["visit"], 1, "visit on /a/slug should be counted")
    assert_equal(stats["redirect"], 1, "redirect with matching metadata ad_slug should be counted")
    assert_equal(stats["whatsapp_redirect"], 1, "matching redirect should count as WhatsApp redirect")
    assert_equal(stats["true_contact"], 1, "metadata-attributed redirect should count as a true action")
    assert_equal(stats["unique_true_contact"], 1, "unique true action should use the same visitor fingerprint")
    assert_equal(stats["effective_true_contact"], 1, "effective true action should include metadata-attributed redirect")
    conn.close()


def test_router_prefers_ad_link_target_then_rotates_fallback():
    tmp = tempfile.NamedTemporaryFile(prefix="mira_route_test_", suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()

    def open_conn():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        return c

    conn = open_conn()
    try:
        conn.execute(
            """CREATE TABLE landing_pages (
               id INTEGER PRIMARY KEY,
               ingest_secret TEXT,
               target_urls TEXT,
               rotation_mode TEXT,
               status TEXT,
               pages_url TEXT,
               custom_domain TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE landing_ad_links (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               page_id INTEGER,
               slug TEXT,
               target_url TEXT,
               status TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE landing_route_state (
               page_id INTEGER PRIMARY KEY,
               cursor INTEGER DEFAULT 0,
               updated_at TEXT
            )"""
        )
        conn.execute(
            """INSERT INTO landing_pages
               (id, ingest_secret, target_urls, rotation_mode, status, pages_url, custom_domain)
               VALUES (7, 'secret', '["https://wa.me/a","https://wa.me/b"]', 'sequential', 'published', 'https://go.example.com', '')"""
        )
        conn.execute(
            """INSERT INTO landing_ad_links (page_id, slug, target_url, status)
               VALUES (7, 'abc123', 'https://wa.me/specific', 'active')"""
        )
        conn.commit()
    finally:
        conn.close()

    old_get_conn = landing_pages.get_conn
    landing_pages.get_conn = open_conn
    try:
        direct = next_landing_route_target(
            LandingRouteNextReq(page_id=7, secret="secret", path="/__mira/redirect", metadata={"ad_slug": "abc123"}),
            None,
        )
        assert_equal(direct["mode"], "ad_link", "ad slug should prefer the ad-specific target")
        assert_equal(direct["target_url"], "https://wa.me/specific", "ad-specific target should be returned")

        first = next_landing_route_target(
            LandingRouteNextReq(page_id=7, secret="secret", path="/__mira/redirect"),
            None,
        )
        second = next_landing_route_target(
            LandingRouteNextReq(page_id=7, secret="secret", path="/__mira/redirect"),
            None,
        )
        assert_equal(first["target_url"], "https://wa.me/a", "fallback rotation should return first target")
        assert_equal(second["target_url"], "https://wa.me/b", "fallback rotation should advance cursor")
    finally:
        landing_pages.get_conn = old_get_conn
        try:
            Path(db_path).unlink()
        except OSError:
            pass


def test_explicit_targets_define_ad_link_count():
    assert_equal(
        _landing_ad_link_create_count(5, ["https://wa.me/a", "https://wa.me/b"]),
        2,
        "explicit targets should create one ad link per target, not repeat targets up to count",
    )
    assert_equal(
        _landing_ad_link_create_count(3, []),
        3,
        "without explicit targets the requested count should still be honored",
    )


if __name__ == "__main__":
    test_date_range_uses_daily_history_without_double_counting()
    test_spend_log_fallback_when_no_history_exists()
    test_redirect_events_with_ad_slug_metadata_are_attributed_to_ad_link()
    test_router_prefers_ad_link_target_then_rotates_fallback()
    test_explicit_targets_define_ad_link_count()
    print("landing ad link stats tests passed")
