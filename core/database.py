import sqlite3, os, json
from datetime import datetime
from cryptography.fernet import Fernet

DB_PATH = os.environ.get("DB_PATH", "/opt/mira/data/mira.db")
KEY_PATH = os.environ.get("KEY_PATH", "/opt/mira/data/.enc_key")

def _get_fernet():
    if not os.path.exists(KEY_PATH):
        os.makedirs(os.path.dirname(KEY_PATH), exist_ok=True)
        key = Fernet.generate_key()
        with open(KEY_PATH, 'wb') as f:
            f.write(key)
        os.chmod(KEY_PATH, 0o600)
    with open(KEY_PATH, 'rb') as f:
        return Fernet(f.read())

def encrypt_token(plain: str) -> str:
    return _get_fernet().encrypt(plain.encode()).decode()

def decrypt_token(cipher: str) -> str:
    return _get_fernet().decrypt(cipher.encode()).decode()

def mask_token(plain: str) -> str:
    return plain[:6] + "****" + plain[-4:] if len(plain) > 10 else "****"

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS fb_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_alias TEXT NOT NULL,
        access_token_enc TEXT NOT NULL,
        token_type TEXT DEFAULT 'user',
        status TEXT DEFAULT 'active',
        note TEXT,
        account_count INTEGER DEFAULT 0,
        last_verified_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        act_id TEXT UNIQUE NOT NULL,
        name TEXT,
        currency TEXT DEFAULT 'USD',
        timezone TEXT,
        token_id INTEGER REFERENCES fb_tokens(id),
        enabled INTEGER DEFAULT 1,
        last_inspect_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS kpi_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        act_id TEXT NOT NULL,
        level TEXT NOT NULL DEFAULT 'ad',
        target_id TEXT NOT NULL,
        target_name TEXT,
        kpi_field TEXT NOT NULL,
        kpi_label TEXT,
        target_cpa REAL,
        source TEXT DEFAULT 'manual',
        enabled INTEGER DEFAULT 1,
        note TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(act_id, level, target_id)
    );
    CREATE TABLE IF NOT EXISTS guard_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        act_id TEXT NOT NULL,
        rule_name TEXT,
        rule_type TEXT NOT NULL,
        action TEXT NOT NULL DEFAULT 'pause',
        param_value REAL,
        param_ratio REAL,
        param_days INTEGER,
        silent_start TEXT,
        silent_end TEXT,
        enabled INTEGER DEFAULT 1,
        note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS scale_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        act_id TEXT NOT NULL,
        rule_name TEXT,
        rule_type TEXT NOT NULL DEFAULT 'slow_scale',
        cpa_ratio REAL DEFAULT 0.8,
        min_conversions INTEGER DEFAULT 3,
        consecutive_days INTEGER DEFAULT 2,
        scale_pct REAL DEFAULT 0.15,
        max_budget REAL,
        roas_threshold REAL DEFAULT 3.0,
        target_regions TEXT,
        enabled INTEGER DEFAULT 1,
        note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS action_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        act_id TEXT,
        target_id TEXT,
        target_name TEXT,
        action_type TEXT,
        trigger_detail TEXT,
        status TEXT DEFAULT 'success',
        error_msg TEXT,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now', '+8 hours'))
    );
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        label TEXT,
        description TEXT,
        placeholder TEXT,
        category TEXT DEFAULT 'general',
        sort_order INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS inspect_cache (
        act_id TEXT,
        ad_id TEXT,
        data TEXT,
        updated_at TEXT,
        PRIMARY KEY(act_id, ad_id)
    );
    CREATE TABLE IF NOT EXISTS login_fails (
        ip TEXT PRIMARY KEY,
        fail_count INTEGER DEFAULT 0,
        locked_until INTEGER DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now'))
    );
    """)
    # Default settings
    defaults = [
        ('inspect_interval','30','巡检间隔（分钟）','每隔多少分钟自动巡检一次','30','scheduler',1),
        ('inspect_enabled','1','启用自动巡检','关闭后不自动巡检，仍可手动触发','','scheduler',2),
        ('ai_enabled','0','启用 AI 分析','需配置 AI API Key 后生效','','ai',1),
        ('ai_provider','deepseek','AI 厂商','选择 AI 服务商','','ai',2),
        ('ai_api_key','','AI API Key','','sk-...','ai',3),
        ('ai_api_base','https://api.deepseek.com/v1','AI API Base URL','','','ai',4),
        ('ai_model','deepseek-chat','AI 模型','','','ai',5),
        ('tg_bot_token','','Telegram Bot Token','','','notify',1),
        ('tg_chat_ids','','TG 接收 ID（多个用逗号分隔，支持群组）','支持个人ID和群组ID，用逗号分隔','','notify',2),
        ('tg_enabled','0','启用 TG 通知','','','notify',3),
        ('default_bleed_amount','20','默认止血金额（USD）','空成效止血默认阈值','20','guard',1),
        ('default_cpa_ratio','1.5','默认CPA超标倍数','超过目标CPA的倍数触发止损','1.5','guard',2),
        ('learning_phase_protect','1','保护学习期广告','学习期广告不触发止损','','guard',3),
        ('escalate_on_fail','1','关闭失败自动向上升级','广告关闭失败时自动尝试关闭广告组/系列','','guard',4),
        ('dry_run','0','模拟模式（Dry Run）','开启后不真实执行操作，仅记录日志','','general',1),
        ('op_cooldown_min','60','操作冷却时间（分钟）','同一广告触发同一规则的最小间隔','60','general',2),
    ]
    for row in defaults:
        c.execute("INSERT OR IGNORE INTO settings(key,value,label,description,placeholder,category,sort_order) VALUES(?,?,?,?,?,?,?)", row)
    conn.commit()
    conn.close()

# 向后兼容别名
get_conn = get_db
