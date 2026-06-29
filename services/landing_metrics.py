"""Landing-page CTA click metrics for guard rule evaluation.

Provides ``landing_click_conversions`` — a per-ad count of distinct-visitor
CTA clicks on the account's *dedicated* (non-shared) landing links for the
account-local "today" window. Used by the guard engine as an alternative /
additive conversion signal (rule.conversion_source = landing | either).

Design constraints (user decisions 2026-06-27):
- Guard thresholds are USD-normalized and spend lives in the FB
  "account-local today" window. To make spend <-> click apples-to-apples,
  clicks are counted in the SAME account-local-today window — even though
  landing_events.created_at is stored as CST (+8h). We shift the stored
  timestamp by (account_utc_offset - 8) hours before the date compare.
- Shared sub-codes (one slug bound to >1 ad) are EXCLUDED from click rules.
- Fault-tolerant by design: ANY failure returns {} so FB-based eval is never
  affected (补充1: click-counter failure must not break FB-feedback thresholds).
- One visitor clicking N times on the same ad counts as 1; multiple ads each
  count independently (dedup = COUNT(DISTINCT fingerprint) per ad).
"""
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# visitor fingerprint — mirrors api/landing_pages.py:2212
_FP_EXPR = (
    "NULLIF(COALESCE(NULLIF(ip_hash,''),'') || '|' || "
    "COALESCE(NULLIF(user_agent_hash,''),''), '|')"
)


def _account_shift_modifier(account: dict) -> str:
    """SQLite datetime modifier converting landing_events.created_at (CST,+8h)
    into account-local time. shift = account_utc_offset - 8 hours.
    Returns '+0 hours' if the offset is unknown (safe CST fallback)."""
    try:
        off = account.get("timezone_offset_hours_utc")
        if off is None or off == "":
            return "+0 hours"
        shift = float(off) - 8.0
        if shift == 0:
            return "+0 hours"
        return ("+%g hours" % shift) if shift > 0 else ("%g hours" % shift)
    except Exception:
        return "+0 hours"


def landing_click_conversions(conn, account: dict, today: str) -> Dict[str, int]:
    """Return {ad_id(str): distinct_visitor_clicks} for this account's dedicated
    landing links, counting event_type='click' rows whose account-local date == today.

    Shared links (>=2 distinct bound ad_ids in landing_ad_link_bindings) are skipped.
    ``today`` is a 'YYYY-MM-DD' string in the account's local timezone (caller
    computes it via guard_engine._account_local_date). The CST-stored created_at
    is shifted to account-local before the date comparison.

    Fault-tolerant: returns {} on ANY error.
    """
    try:
        act_id = account.get("act_id")
        if not act_id or not today:
            return {}
        # landing_ad_links stores the bare numeric act id (no 'act_' prefix);
        # accounts.act_id carries the prefix. Normalize so they join.
        act_plain = act_id[4:] if str(act_id).lower().startswith("act_") else str(act_id)
        mod = _account_shift_modifier(account)
        links = conn.execute(
            "SELECT id, ad_id, slug, page_id FROM landing_ad_links "
            "WHERE (act_id=? OR COALESCE(act_id,'')='') AND ad_id IS NOT NULL AND ad_id!='' "
            "AND slug IS NOT NULL AND slug!=''",
            (act_plain,),
        ).fetchall()
        out: Dict[str, int] = {}
        for link_id, ad_id, slug, page_id in links:
            try:
                n = conn.execute(
                    "SELECT COUNT(DISTINCT ad_id) FROM landing_ad_link_bindings WHERE link_id=?",
                    (link_id,),
                ).fetchone()[0]
            except Exception:
                n = 1
            if n and n > 1:
                continue  # 共享子码不计入点击规则
            pat = "/a/" + slug
            row = conn.execute(
                "SELECT COUNT(*) FROM ("
                " SELECT DISTINCT " + _FP_EXPR + " AS fp FROM landing_events"
                " WHERE page_id=? AND event_type='click'"
                "   AND date(datetime(created_at, ?))=?"
                "   AND (" + _FP_EXPR + ") IS NOT NULL"
                "   AND (path LIKE ? OR json_extract(metadata,'$.ad_slug')=?"
                "        OR json_extract(metadata,'$.ad_id')=?)"
                ")",
                (page_id, mod, today, pat + "%", slug, str(ad_id)),
            ).fetchone()
            cnt = int(row[0]) if row and row[0] else 0
            if cnt:
                key = str(ad_id)
                out[key] = out.get(key, 0) + cnt
        return out
    except Exception as e:
        logger.warning(
            "landing_click_conversions failed (act=%s): %s",
            account.get("act_id"),
            e,
        )
        return {}
