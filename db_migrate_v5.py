"""v5 迁移：users 表增加 last_active_at / last_ip 列"""
import logging
logger = logging.getLogger("mira.migrate_v5")

def run():
    from core.database import get_db
    conn = get_db()
    try:
        conn.execute("ALTER TABLE users ADD COLUMN last_active_at TEXT")
        logger.info("Added users.last_active_at column")
    except Exception:
        pass  # column already exists
    try:
        conn.execute("ALTER TABLE users ADD COLUMN last_ip TEXT")
        logger.info("Added users.last_ip column")
    except Exception:
        pass  # column already exists
    conn.commit()
    conn.close()
