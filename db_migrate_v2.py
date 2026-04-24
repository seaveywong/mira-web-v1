#!/usr/bin/env python3
"""
Mira v2.0 数据库迁移脚本
- 添加 currency_rates 表（货币汇率缓存）
- 添加 custom_rule_templates 表（自定义规则模板）
- 添加 accounts.currency 字段（如不存在）
- 添加 guard_rules.auto_reopen 字段（广告自动恢复）
- 添加 settings.MIRA_USERNAME 到 .env（如不存在）
- 修复 settings 中 label 为 NULL 的记录
"""
import sqlite3
import os
import sys

DB_PATH = os.environ.get("DB_PATH", "/opt/mira/data/mira.db")
ENV_PATH = "/opt/mira/.env"

def run():
    print(f"[migrate] 连接数据库: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # 1. 添加 currency_rates 表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS currency_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency TEXT NOT NULL UNIQUE,
            rate REAL NOT NULL DEFAULT 1.0,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # 预填常用汇率（近似值，会被自动更新覆盖）
    default_rates = [
        ("EUR", 1.08), ("GBP", 1.27), ("JPY", 0.0067), ("CAD", 0.74),
        ("AUD", 0.65), ("SGD", 0.74), ("HKD", 0.128), ("MXN", 0.058),
        ("BRL", 0.20), ("INR", 0.012), ("THB", 0.028), ("TWD", 0.031),
        ("KRW", 0.00075), ("PHP", 0.017), ("IDR", 0.000063), ("VND", 0.000040),
        ("MYR", 0.22), ("TRY", 0.031), ("AED", 0.272), ("SAR", 0.267),
        ("ZAR", 0.055), ("NGN", 0.00065), ("EGP", 0.021), ("COP", 0.00025),
        ("CLP", 0.0011), ("PEN", 0.27), ("ARS", 0.0011), ("CZK", 0.044),
        ("PLN", 0.25), ("HUF", 0.0028), ("RON", 0.22), ("DKK", 0.145),
        ("SEK", 0.096), ("NOK", 0.095), ("CHF", 1.12), ("NZD", 0.61),
    ]
    for currency, rate in default_rates:
        conn.execute(
            "INSERT OR IGNORE INTO currency_rates(currency, rate) VALUES(?,?)",
            (currency, rate)
        )
    print(f"[migrate] ✅ currency_rates 表已创建，预填 {len(default_rates)} 条汇率")

    # 2. 添加 custom_rule_templates 表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_rule_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            guard_rules TEXT DEFAULT '[]',
            scale_rules TEXT DEFAULT '[]',
            tags TEXT DEFAULT '[]',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    print("[migrate] ✅ custom_rule_templates 表已创建")

    # 3. accounts 表添加 currency 字段
    cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
    if "currency" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN currency TEXT DEFAULT 'USD'")
        print("[migrate] ✅ accounts.currency 字段已添加")
    else:
        print("[migrate] ℹ️  accounts.currency 字段已存在，跳过")

    # 4. guard_rules 表添加 auto_reopen 字段
    cols = [r[1] for r in conn.execute("PRAGMA table_info(guard_rules)").fetchall()]
    if "auto_reopen" not in cols:
        conn.execute("ALTER TABLE guard_rules ADD COLUMN auto_reopen INTEGER DEFAULT 0")
        print("[migrate] ✅ guard_rules.auto_reopen 字段已添加")
    else:
        print("[migrate] ℹ️  guard_rules.auto_reopen 字段已存在，跳过")

    # 5. perf_snapshots 表添加 currency 字段
    cols = [r[1] for r in conn.execute("PRAGMA table_info(perf_snapshots)").fetchall()]
    if "currency" not in cols:
        conn.execute("ALTER TABLE perf_snapshots ADD COLUMN currency TEXT DEFAULT 'USD'")
        print("[migrate] ✅ perf_snapshots.currency 字段已添加")
    else:
        print("[migrate] ℹ️  perf_snapshots.currency 字段已存在，跳过")

    # 6. 修复 settings 中 label 为 NULL 的记录
    label_map = {
        # AI配置
        "ai_enabled": "启用 AI 功能",
        "ai_provider": "AI 厂商",
        "ai_api_base": "API 基础 URL",
        "ai_api_key": "API Key",
        "ai_model": "模型名称",
        "ai_max_tokens": "最大 Token 数",
        "ai_temperature": "Temperature",
        "dry_run": "演练模式（不执行真实操作）",
        "learning_phase_protect": "学习期保护",
        "escalate_on_fail": "失败时升级通知",
        # 通知配置
        "tg_bot_token": "Telegram Bot Token",
        "tg_chat_id": "TG Chat ID（旧）",
        "tg_chat_ids": "TG 接收 ID",
        "tg_enabled": "启用 TG 通知",
        # 巡检调度
        "inspect_enabled": "启用自动巡检",
        "inspect_interval": "巡检间隔（分钟）",
        "inspect_interval_min": "巡检间隔（分钟）",
        # 系统
        "system_version": "系统版本",
        "max_daily_budget_increase": "单日最大加预算比例(%)",
        "max_budget_increase_pct": "单次加预算比例(%)",
        "max_daily_actions": "单日最大操作次数",
    }
    category_map = {
        "ai_enabled": "ai", "ai_provider": "ai", "ai_api_base": "ai",
        "ai_api_key": "ai", "ai_model": "ai", "ai_max_tokens": "ai",
        "ai_temperature": "ai", "dry_run": "general", "learning_phase_protect": "general",
        "escalate_on_fail": "general",
        "tg_bot_token": "notify", "tg_chat_id": "notify", "tg_chat_ids": "notify",
        "tg_enabled": "notify",
        "inspect_enabled": "scheduler", "inspect_interval": "scheduler",
        "inspect_interval_min": "scheduler",
        "system_version": "system", "max_daily_budget_increase": "general",
        "max_budget_increase_pct": "general", "max_daily_actions": "general",
    }
    fixed = 0
    for key, label in label_map.items():
        result = conn.execute(
            "UPDATE settings SET label=? WHERE key=? AND (label IS NULL OR label='')",
            (label, key)
        )
        if result.rowcount > 0:
            fixed += result.rowcount
        # 同时修复 category
        cat = category_map.get(key)
        if cat:
            conn.execute(
                "UPDATE settings SET category=? WHERE key=? AND (category IS NULL OR category='')",
                (cat, key)
            )
    print(f"[migrate] ✅ 修复 {fixed} 条 settings label 为 NULL 的记录")

    # 7. 确保 settings 中有 tg_chat_ids 的 description
    conn.execute(
        "UPDATE settings SET description='多个ID用英文逗号分隔，群组ID前有负号，如：6671042868,-1001234567890' WHERE key='tg_chat_ids' AND (description IS NULL OR description='')"
    )

    conn.commit()
    conn.close()
    print("[migrate] ✅ 数据库迁移完成")

    # 8. 确保 .env 中有 MIRA_USERNAME
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, "r") as f:
            content = f.read()
        if "MIRA_USERNAME=" not in content:
            with open(ENV_PATH, "a") as f:
                f.write("\nMIRA_USERNAME=admin\n")
            print("[migrate] ✅ .env 中已添加 MIRA_USERNAME=admin")
        else:
            print("[migrate] ℹ️  .env 中已有 MIRA_USERNAME，跳过")
    else:
        print(f"[migrate] ⚠️  .env 文件不存在: {ENV_PATH}")


if __name__ == "__main__":
    run()
