"""
数据库迁移：添加 users 多用户表
"""
import os
import sys
sys.path.insert(0, '/opt/mira')
os.chdir('/opt/mira')

# 加载环境变量
from dotenv import load_dotenv
load_dotenv('/opt/mira/.env')

from core.database import get_db

def run():
    conn = get_db()
    c = conn.cursor()

    # 创建 users 表
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'operator',
        display_name TEXT,
        note TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        last_login_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """)

    conn.commit()
    conn.close()
    print("[migrate_users] ✅ users 表已创建/确认")

if __name__ == "__main__":
    run()
